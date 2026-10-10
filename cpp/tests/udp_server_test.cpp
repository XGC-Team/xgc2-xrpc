// udp.v1 server and client over real loopback sockets.
//
// Usage: udp_server_test [host]   (a numeric loopback address, default 127.0.0.1)
//
// For ::1 on a machine without IPv6 loopback (containers often disable it) the
// test first tries to build a private one in a new user and network namespace,
// and exits 77, which ctest reports as skipped, if the system refuses that too.
#include "../src/udp_net.hpp"
#include "../src/udp_wire.hpp"
#include <net/if.h>
#include <poll.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
using namespace xgc2::xrpc::udp;
using namespace xgc2::xrpc::udp::detail;
using namespace std::chrono;

namespace {
using Bytes = std::vector<std::uint8_t>;
std::string host = "127.0.0.1";

Key filled_key(bool sequential) {
  Key key;
  for (std::size_t i = 0; i < key.size(); ++i) key[i] = sequential ? static_cast<std::uint8_t>(i) : 0x88;
  return key;
}
const Key key7 = filled_key(true), key8 = filled_key(false);
KeyRing keys() {
  KeyRing ring;
  ring.add(7, std::string(key7.begin(), key7.end()));
  ring.add(8, std::string(key8.begin(), key8.end()));
  return ring;
}

template <class Predicate> bool await(Predicate predicate, milliseconds limit = seconds(3)) {
  const auto deadline = steady_clock::now() + limit;
  while (!predicate()) {
    if (steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(milliseconds(1));
  }
  return true;
}
RequestId fresh_id() {
  RequestId id;
  random_bytes(id.data(), id.size());
  return id;
}
std::string endpoint_of(std::uint16_t port) {
  return (host.find(':') == std::string::npos ? host : "[" + host + "]") + ":" + std::to_string(port);
}
Bytes resign(Bytes datagram, const Key &key) {
  hmac_sha256(key.data(), key.size(), datagram.data(), datagram.size() - tag_bytes,
              datagram.data() + datagram.size() - tag_bytes);
  return datagram;
}
Datagram parse_reply(const Bytes &bytes, const Key &key = key7) {
  Datagram d;
  assert(parse(bytes.data(), bytes.size(), d) == Parse::Ok && verify(d, key));
  assert(d.type == Type::Reply);
  return d;
}
template <class Exception, class F> void throws(F &&f) {
  bool caught = false;
  try {
    f();
  } catch (const Exception &) {
    caught = true;
  }
  assert(caught);
}

// A raw socket that speaks to a server (or plays one).
class Peer {
public:
  explicit Peer(const std::string &bind_host = host) {
    const auto address = make_address(bind_host, 0);
    assert(address);
    fd_ = ::socket(address->family(), SOCK_DGRAM | SOCK_CLOEXEC, 0);
    assert(fd_ >= 0 && ::bind(fd_, address->get(), address->size) == 0);
    local_.size = sizeof local_.storage;
    assert(::getsockname(fd_, local_.get(), &local_.size) == 0);
  }
  ~Peer() { ::close(fd_); }
  Peer(const Peer &) = delete;
  Peer &operator=(const Peer &) = delete;
  int fd() const { return fd_; }
  std::uint16_t port() const { return port_of(local_); }
  void send_to(std::uint16_t port, const Bytes &bytes) { send_to(*make_address(host, port), bytes); }
  void send_to(const Address &to, const Bytes &bytes) {
    assert(::sendto(fd_, bytes.data(), bytes.size(), 0, to.get(), to.size) == static_cast<ssize_t>(bytes.size()));
  }
  std::optional<Bytes> receive(milliseconds timeout, Address *from = nullptr) {
    pollfd ready{fd_, POLLIN, 0};
    if (::poll(&ready, 1, static_cast<int>(timeout.count())) <= 0) return std::nullopt;
    Bytes buffer(4096);
    Address source;
    source.size = sizeof source.storage;
    const auto n = ::recvfrom(fd_, buffer.data(), buffer.size(), 0, source.get(), &source.size);
    assert(n >= 0);
    buffer.resize(static_cast<std::size_t>(n));
    if (from) *from = source;
    return buffer;
  }
  // Everything that arrives until the socket has been quiet for `quiet`.
  std::vector<Bytes> receive_all(milliseconds quiet) {
    std::vector<Bytes> all;
    while (auto bytes = receive(quiet)) all.push_back(std::move(*bytes));
    return all;
  }

private:
  int fd_ = -1;
  Address local_;
};

// The handlers that the scenarios share.
struct Probe {
  std::atomic<std::uint64_t> counter{0}, echoes{0}, holds{0};
  std::atomic<int> late_completions{0}, on_time_completions{0}, sleeps{0};
  std::mutex mutex;
  std::vector<Reply> held;
  std::vector<std::thread> threads;
  ~Probe() {
    for (auto &thread : threads) thread.join();
  }
  void install(Server &server) {
    server.add_method("test/Echo", [this](Request request, Reply reply) {
      ++echoes;
      reply.complete(Status::Ok, request.body);
    });
    server.add_method("test/Count", [this](Request, Reply reply) {
      reply.complete(Status::Ok, std::to_string(++counter));
    });
    server.add_method("test/Hold", [this](Request, Reply reply) {
      std::lock_guard<std::mutex> lock(mutex);
      held.push_back(std::move(reply));
      ++holds;
    });
    server.add_method("test/Drop", [](Request, Reply) {});
    server.add_method("test/Throw", [](Request, Reply) { throw std::runtime_error("handler failure"); });
    server.add_method("test/ThrowAfterReply", [](Request, Reply reply) {
      reply.complete(Status::Ok, "done");
      throw std::runtime_error("late failure");
    });
    server.add_method("test/Fail", [](Request request, Reply reply) {
      reply.error(static_cast<Status>(std::stoi(request.body)), "requested failure");
    });
    server.add_method("test/Big", [](Request request, Reply reply) {
      reply.complete(Status::Ok, std::string(static_cast<std::size_t>(std::stoul(request.body)), 'x'));
    });
    // Replies from another thread after request.body milliseconds.
    server.add_method("test/Sleep", [this](Request request, Reply reply) {
      const auto wait = milliseconds(std::stoi(request.body));
      std::lock_guard<std::mutex> lock(mutex);
      ++sleeps;
      threads.emplace_back([this, wait, reply = std::move(reply)]() mutable {
        std::this_thread::sleep_for(wait);
        if (reply.complete(Status::Ok, "slept")) ++on_time_completions;
        else ++late_completions;
      });
    });
  }
  void complete_held(Status status, const std::string &body) {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto &reply : held) assert(reply.complete(status, body));
    held.clear();
  }
};

struct Env {
  Probe probe;
  std::unique_ptr<Server> server;
  Client client;
  explicit Env(const std::function<void(ServerOptions &)> &configure = {}) : client(keys()) {
    ServerOptions options;
    options.bind_address = host;
    if (configure) configure(options);
    server.reset(new Server(options, keys()));
    probe.install(*server);
    server->start();
  }
  std::uint16_t port() const { return server->port(); }
  std::string endpoint() const { return endpoint_of(port()); }
  Response call(const std::string &method, const std::string &body, milliseconds timeout = seconds(2),
                std::optional<InstanceId> pin = std::nullopt, std::uint32_t key_id = 7) {
    return client.call(endpoint(), key_id, method, body, steady_clock::now() + timeout, pin);
  }
  static Bytes request(const std::string &method, const std::string &body, const RequestId &id,
                       std::uint32_t timeout_ms = 2000, std::uint32_t key_id = 7) {
    return encode_request(key_id == 7 ? key7 : key8, key_id, id, std::nullopt, timeout_ms, method, body);
  }
};

// ---- calls and replies -----------------------------------------------------

void basic_calls() {
  Env env;
  const auto first = env.call("test/Echo", "{\"hello\":\"world\"}");
  assert(first.delivery == Delivery::ResponseReceived && first.status == Status::Ok);
  assert(first.body == "{\"hello\":\"world\"}" && first.instance == env.server->instance());
  assert(first.attempts >= 1 && first.message.empty());
  assert(env.server->instance() != InstanceId{});
  // Empty and maximum-size bodies.
  assert(env.call("test/Echo", "").body.empty());
  const std::string biggest(max_body_bytes(std::strlen("test/Echo")), 'b');
  assert(env.call("test/Echo", biggest).body == biggest);
  // An unknown method is an authenticated error reply, not silence.
  const auto missing = env.call("test/Missing", "{}");
  assert(missing.delivery == Delivery::ResponseReceived && missing.status == Status::NotFound);
  assert(missing.body.find("\"code\":\"not_found\"") != std::string::npos);
  assert(missing.instance == env.server->instance());
  // Every status code a handler may send travels with its error body.
  for (std::uint32_t code : {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u, 10u}) {
    const auto failure = env.call("test/Fail", std::to_string(code));
    assert(failure.delivery == Delivery::ResponseReceived && static_cast<std::uint32_t>(failure.status) == code);
    assert(failure.body.find("\"code\":\"" + std::string(status_name(failure.status)) + "\"") != std::string::npos);
    assert(failure.body.find("\"message\":\"requested failure\"") != std::string::npos);
  }
  // The server never sends unauthenticated; the handler API refuses it.
  Reply empty;
  throws<std::invalid_argument>([&] { empty.complete(Status::Unauthenticated, "{}"); });
  throws<std::invalid_argument>([&] { empty.complete(static_cast<Status>(11), "{}"); });
  throws<std::invalid_argument>([&] { empty.error(Status::Ok, "no"); });
  assert(!empty.complete(Status::Ok, "{}"));
  assert(env.call("test/Echo", "{}", seconds(2), std::nullopt, 8).status == Status::Ok); // the second key
  // The server counts a reply just after writing it, so allow for that race.
  assert(await([&] {
    const auto stats = env.server->stats();
    return stats.replies_sent == stats.received && stats.inflight == 0;
  }));
  const auto stats = env.server->stats();
  assert(stats.dropped_auth == 0 && stats.dropped_malformed == 0 && stats.dropped_rate == 0);
  Env other;
  assert(other.server->instance() != env.server->instance()); // fresh per server
  assert(env.server->instance_hex() == to_hex(env.server->instance()));
  assert(instance_from_hex(env.server->instance_hex()) == env.server->instance());
}

void configured_instance() {
  // A host that already has a domain instance makes the transport use it.
  const InstanceId domain{{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}};
  Env env([&](ServerOptions &options) { options.instance = domain; });
  assert(env.server->instance() == domain);
  assert(env.server->instance_hex() == "deadbeef0102030405060708090a0b0c");
  const auto response = env.call("test/Echo", "{}", seconds(2), domain); // pinned to it
  assert(response.status == Status::Ok && response.instance == domain);
  InstanceId stale = domain;
  stale[15] ^= 1;
  const auto refused = env.call("test/Echo", "{}", seconds(2), stale);
  assert(refused.status == Status::Conflict && refused.instance == domain);
  // Two servers may deliberately share an identity; zero is not an identity.
  Env twin([&](ServerOptions &options) { options.instance = domain; });
  assert(twin.server->instance() == env.server->instance());
  throws<std::invalid_argument>([] {
    ServerOptions options;
    options.instance = InstanceId{};
    Server server(options, keys());
  });
}

void at_most_once_under_duplicates() {
  Env env;
  Peer peer;
  const auto id = fresh_id();
  const auto datagram = Env::request("test/Count", "", id);
  peer.send_to(env.port(), datagram);
  const auto first = peer.receive(seconds(2));
  assert(first && parse_reply(*first).body == "1");
  for (int i = 0; i < 5; ++i) {
    peer.send_to(env.port(), datagram);
    const auto again = peer.receive(seconds(2));
    assert(again && *again == *first); // the cached bytes, verbatim
  }
  assert(env.probe.counter == 1);
  assert(await([&] { return env.server->stats().replies_sent == 6; }));
  assert(env.server->stats().cache_hits == 5);
  // The same request id under another key is another request.
  peer.send_to(env.port(), Env::request("test/Count", "", id, 2000, 8));
  assert(peer.receive(seconds(2)) && env.probe.counter == 2);
  // A burst of identical datagrams still runs the handler once.
  const auto burst = Env::request("test/Count", "", fresh_id());
  for (int i = 0; i < 20; ++i) peer.send_to(env.port(), burst);
  const auto replies = peer.receive_all(milliseconds(300));
  assert(env.probe.counter == 3 && !replies.empty() && replies.size() <= 20);
  for (const auto &reply : replies) assert(parse_reply(reply).body == "3");
}

void retransmissions_of_a_running_call_are_ignored() {
  Env env;
  Peer peer;
  const auto datagram = Env::request("test/Hold", "", fresh_id());
  peer.send_to(env.port(), datagram);
  assert(await([&] { return env.probe.holds == 1; }));
  for (int i = 0; i < 4; ++i) peer.send_to(env.port(), datagram);
  assert(await([&] { return env.server->stats().inflight_ignored == 4; }));
  assert(env.probe.holds == 1 && !peer.receive(milliseconds(100)));
  assert(env.server->stats().inflight == 1);
  env.probe.complete_held(Status::Ok, "late result");
  const auto reply = peer.receive(seconds(2));
  assert(reply && parse_reply(*reply).body == "late result");
  assert(!peer.receive(milliseconds(100))); // exactly one reply for five datagrams
  peer.send_to(env.port(), datagram);
  assert(peer.receive(seconds(2)) == reply); // and now the cache answers
  assert(env.probe.holds == 1 && env.server->stats().inflight == 0);
}

void retransmitting_client_runs_the_handler_once() {
  Env env;
  // The handler answers after 250 ms: the client sends at 0, 30, 90 and 210 ms.
  const auto response = env.call("test/Sleep", "250");
  assert(response.delivery == Delivery::ResponseReceived && response.body == "slept");
  assert(response.attempts >= 4 && response.attempts <= 6);
  assert(env.probe.sleeps == 1 && await([&] { return env.probe.on_time_completions == 1; }));
  assert(await([&] { return env.server->stats().inflight == 0; }));
  assert(env.server->stats().inflight_ignored >= 3);
}

void async_completion_keeps_the_io_thread_free() {
  Env env;
  std::atomic<bool> slow_done{false};
  Response slow;
  std::thread caller([&] {
    slow = env.call("test/Sleep", "400");
    slow_done = true;
  });
  assert(await([&] { return env.server->stats().inflight == 1; }));
  const auto start = steady_clock::now();
  for (int i = 0; i < 20; ++i) assert(env.call("test/Echo", "{}").status == Status::Ok);
  assert(steady_clock::now() - start < milliseconds(300) && !slow_done);
  caller.join();
  assert(slow.status == Status::Ok && slow.body == "slept");
}

void missed_deadlines_send_nothing() {
  Env env([](ServerOptions &options) { options.call_budget = milliseconds(150); });
  Peer peer;
  // The server's call budget caps a client that asks for more.
  peer.send_to(env.port(), Env::request("test/Sleep", "400", fresh_id(), 30000));
  assert(!peer.receive(milliseconds(700)));
  assert(await([&] { return env.probe.late_completions == 1; }));
  assert(env.probe.on_time_completions == 0 && env.server->stats().unanswered == 1);
  assert(env.server->stats().inflight == 0);
  // So does the client's own, shorter timeout_ms.
  const auto short_timeout = Env::request("test/Sleep", "300", fresh_id(), 60);
  peer.send_to(env.port(), short_timeout);
  assert(!peer.receive(milliseconds(500)));
  assert(await([&] { return env.probe.late_completions == 2; }));
  // A retransmission after the missed deadline neither replies nor re-runs the handler.
  const auto before = env.probe.sleeps.load();
  peer.send_to(env.port(), short_timeout);
  assert(!peer.receive(milliseconds(300)));
  assert(env.probe.sleeps == before && env.server->stats().inflight_ignored >= 1);
  // The blocking client reports the unknown outcome.
  const auto response = env.call("test/Sleep", "600", milliseconds(200));
  assert(response.delivery == Delivery::OutcomeUnknown && response.status == Status::DeadlineExceeded);
  assert(response.attempts >= 2 && response.body.empty() && response.instance == InstanceId{});
  // A Reply that nobody completes frees its slot when the deadline passes.
  Peer holder;
  holder.send_to(env.port(), Env::request("test/Hold", "", fresh_id()));
  assert(await([&] { return env.probe.holds == 1 && env.server->stats().inflight == 1; }));
  assert(await([&] { return env.server->stats().inflight == 0; }));
  {
    std::lock_guard<std::mutex> lock(env.probe.mutex);
    assert(env.probe.held.size() == 1 && !env.probe.held[0].complete(Status::Ok, "too late"));
    env.probe.held.clear();
  }
  assert(!holder.receive(milliseconds(100)));
}

void handlers_that_drop_or_fail() {
  Env env;
  Peer peer;
  peer.send_to(env.port(), Env::request("test/Drop", "", fresh_id()));
  assert(!peer.receive(milliseconds(300)));
  assert(await([&] { return env.server->stats().unanswered == 1 && env.server->stats().inflight == 0; }));
  const auto thrown = env.call("test/Throw", "");
  assert(thrown.delivery == Delivery::ResponseReceived && thrown.status == Status::Internal);
  assert(thrown.body.find("\"code\":\"internal\"") != std::string::npos);
  const auto after_reply = env.call("test/ThrowAfterReply", "");
  assert(after_reply.status == Status::Ok && after_reply.body == "done");
  assert(env.server->stats().inflight == 0);
}

void oversized_messages() {
  Env env;
  // A reply that fits exactly, and one a byte too long becomes resource_exhausted.
  assert(env.call("test/Big", std::to_string(max_body_bytes(0))).body.size() == max_body_bytes(0));
  for (const char *size : {"1117", "5000", "100000"}) {
    const auto response = env.call("test/Big", size);
    assert(response.delivery == Delivery::ResponseReceived && response.status == Status::ResourceExhausted);
    assert(response.body.find("\"code\":\"resource_exhausted\"") != std::string::npos);
    assert(response.body.size() < 200 && response.instance == env.server->instance());
  }
  // An oversized request never leaves the client.
  const auto before = env.server->stats().received;
  const auto too_big = env.call("test/Echo", std::string(max_body_bytes(std::strlen("test/Echo")) + 1, 'x'));
  assert(too_big.delivery == Delivery::NotSent && too_big.status == Status::ResourceExhausted);
  assert(too_big.attempts == 0 && !too_big.message.empty());
  assert(env.server->stats().received == before);
  // A datagram above 1200 bytes is dropped as malformed, whatever it holds.
  Peer peer;
  peer.send_to(env.port(), Bytes(max_datagram_bytes + 1, 0));
  assert(!peer.receive(milliseconds(200)));
  assert(await([&] { return env.server->stats().dropped_malformed == 1; }));
}

// ---- authentication, limits, fencing ---------------------------------------

void unauthenticated_and_malformed_datagrams_get_silence() {
  Env env;
  Peer peer;
  const auto id = fresh_id();
  const auto good = Env::request("test/Echo", "{}", id);
  const auto flipped = [&](std::size_t index) {
    auto copy = good;
    copy[index] ^= 1;
    return copy;
  };
  const std::vector<Bytes> bad_auth = {
      encode_request(key8, 7, id, std::nullopt, 2000, "test/Echo", "{}"),  // wrong key for the id
      encode_request(key7, 99, id, std::nullopt, 2000, "test/Echo", "{}"), // unknown key id
      flipped(good.size() - 1), flipped(good.size() - tag_bytes - 1), flipped(8), flipped(12), flipped(44)};
  std::vector<Bytes> malformed = {
      Bytes(10, 1), Bytes(83, 0), Bytes(good.begin(), good.end() - 1),
      [&] { auto c = good; c.push_back(0); return c; }(), flipped(0), flipped(4), flipped(5), flipped(48),
      flipped(51), [&] { auto c = good; c[5] = 2; return c; }(),
      // A reply is not a request, even with a valid tag.
      encode_reply(key7, 7, id, env.server->instance(), 0, "{}")};
  for (const auto &datagram : bad_auth) peer.send_to(env.port(), datagram);
  for (const auto &datagram : malformed) peer.send_to(env.port(), datagram);
  assert(!peer.receive(milliseconds(300)));
  assert(await([&] { return env.server->stats().received == bad_auth.size() + malformed.size(); }));
  const auto stats = env.server->stats();
  assert(stats.dropped_auth == bad_auth.size() && stats.dropped_malformed == malformed.size());
  assert(stats.replies_sent == 0 && env.probe.echoes == 0);
  // Authenticated but unusable requests get an error reply.
  const auto semantic = [&](const Bytes &datagram) {
    peer.send_to(env.port(), datagram);
    const auto reply = peer.receive(seconds(2));
    assert(reply);
    return static_cast<Status>(parse_reply(*reply).word);
  };
  auto reserved_flag = good;
  reserved_flag[7] |= 2;
  assert(semantic(resign(reserved_flag, key7)) == Status::InvalidArgument);
  assert(semantic(Env::request("test/Echo", "{}", fresh_id(), 0)) == Status::InvalidArgument);
  assert(semantic(Env::request("test/Echo", "{}", fresh_id(), 60001)) == Status::InvalidArgument);
  assert(semantic(Env::request("test/Echo", "{}", fresh_id(), 60000)) == Status::Ok);
  assert(semantic(Env::request("test/Echo", "{}", fresh_id(), 1000)) == Status::Ok);
  assert(env.probe.echoes == 2);
}

void rate_limit_per_source() {
  Env env([](ServerOptions &options) { options.requests_per_second = 1; options.burst = 3; });
  Peer peer;
  // Garbage never spends tokens.
  for (int i = 0; i < 50; ++i) peer.send_to(env.port(), Bytes(100, static_cast<std::uint8_t>(i)));
  for (int i = 0; i < 10; ++i) peer.send_to(env.port(), Env::request("test/Echo", "{}", fresh_id()));
  assert(peer.receive_all(milliseconds(300)).size() == 3);
  auto stats = env.server->stats();
  assert(stats.dropped_rate == 7 && stats.dropped_malformed == 50 && env.probe.echoes == 3);
  // The bucket refills at one token per second.
  std::this_thread::sleep_for(milliseconds(1200));
  peer.send_to(env.port(), Env::request("test/Echo", "{}", fresh_id()));
  assert(peer.receive(seconds(1)));
  peer.send_to(env.port(), Env::request("test/Echo", "{}", fresh_id()));
  assert(!peer.receive(milliseconds(200)));
  // Another source address has its own budget (127.0.0.0/8 is all loopback).
  if (host == "127.0.0.1") {
    Peer other("127.0.0.2");
    for (int i = 0; i < 3; ++i) other.send_to(env.port(), Env::request("test/Echo", "{}", fresh_id()));
    assert(other.receive_all(milliseconds(300)).size() == 3);
  }
  // The defaults, 50 per second with a burst of 100, let a modest burst through.
  Env defaults;
  Peer burst;
  for (int i = 0; i < 100; ++i) burst.send_to(defaults.port(), Env::request("test/Echo", "{}", fresh_id()));
  assert(burst.receive_all(milliseconds(400)).size() == 100);
  assert(defaults.server->stats().dropped_rate == 0);
  // Tracked sources are bounded: with room for one, a second source is dropped.
  Env tight([](ServerOptions &options) {
    options.max_rate_sources = 1;
    options.requests_per_second = 1;
    options.burst = 3;
  });
  Peer first;
  first.send_to(tight.port(), Env::request("test/Echo", "{}", fresh_id()));
  assert(first.receive(seconds(1)));
  if (host == "127.0.0.1") {
    Peer second("127.0.0.2");
    second.send_to(tight.port(), Env::request("test/Echo", "{}", fresh_id()));
    assert(!second.receive(milliseconds(200)) && tight.server->stats().dropped_rate == 1);
  }
}

void instance_pinning() {
  Env env;
  const auto pinned = env.call("test/Count", "", seconds(2), env.server->instance());
  assert(pinned.delivery == Delivery::ResponseReceived && pinned.status == Status::Ok);
  InstanceId stale = env.server->instance();
  stale[0] ^= 0xff;
  const auto refused = env.call("test/Count", "", seconds(2), stale);
  // The fence answers conflict, naming the instance that is really there.
  assert(refused.delivery == Delivery::ResponseReceived && refused.status == Status::Conflict);
  assert(refused.instance == env.server->instance() && refused.attempts >= 1);
  assert(refused.body.find("\"code\":\"conflict\"") != std::string::npos);
  assert(env.probe.counter == 1); // the fenced call never ran
  assert(env.call("test/Count", "", seconds(2), InstanceId{}).status == Status::Conflict); // zero is a pin
  assert(env.probe.counter == 1);
}

void limits_and_the_reply_cache() {
  {
    Env env([](ServerOptions &options) { options.max_inflight = 2; });
    Peer peer;
    peer.send_to(env.port(), Env::request("test/Hold", "", fresh_id()));
    peer.send_to(env.port(), Env::request("test/Hold", "", fresh_id()));
    assert(await([&] { return env.probe.holds == 2; }));
    const auto refused = env.call("test/Echo", "{}");
    assert(refused.delivery == Delivery::ResponseReceived && refused.status == Status::ResourceExhausted);
    assert(refused.body.find("\"code\":\"resource_exhausted\"") != std::string::npos);
    env.probe.complete_held(Status::Ok, "{}");
    assert(peer.receive_all(milliseconds(200)).size() == 2);
    assert(env.call("test/Echo", "{}").status == Status::Ok);
  }
  {
    // TTL: inside it a duplicate is answered from the cache, after it the id is new.
    Env env([](ServerOptions &options) {
      options.call_budget = milliseconds(100);
      options.reply_cache_ttl = seconds(1);
    });
    Peer peer;
    const auto datagram = Env::request("test/Count", "", fresh_id(), 100);
    peer.send_to(env.port(), datagram);
    assert(peer.receive(seconds(1)));
    std::this_thread::sleep_for(milliseconds(300));
    peer.send_to(env.port(), datagram);
    assert(peer.receive(seconds(1)) && env.probe.counter == 1);
    std::this_thread::sleep_for(milliseconds(900));
    peer.send_to(env.port(), datagram);
    assert(peer.receive(seconds(1)) && env.probe.counter == 2);
  }
  {
    // Capacity: only the newest replies are kept.
    Env env([](ServerOptions &options) {
      options.max_inflight = 2;
      options.reply_cache_capacity = 4;
    });
    Peer peer;
    std::vector<Bytes> datagrams;
    for (int i = 0; i < 6; ++i) {
      datagrams.push_back(Env::request("test/Count", "", fresh_id()));
      peer.send_to(env.port(), datagrams.back());
      assert(peer.receive(seconds(1)));
    }
    assert(env.probe.counter == 6);
    peer.send_to(env.port(), datagrams.back()); // newest: cached
    assert(peer.receive(seconds(1)) && env.probe.counter == 6);
    peer.send_to(env.port(), datagrams.front()); // oldest: evicted, runs again
    assert(peer.receive(seconds(1)) && env.probe.counter == 7);
  }
}

// ---- the client against hand-made peers ------------------------------------

void client_input_validation() {
  Env env;
  const auto now = steady_clock::now();
  const auto call = [&](const std::string &endpoint, std::uint32_t key_id, const std::string &method,
                        steady_clock::time_point deadline) {
    return env.client.call(endpoint, key_id, method, "{}", deadline);
  };
  const auto expect_not_sent = [](const Response &response, Status status) {
    assert(response.delivery == Delivery::NotSent && response.status == status);
    assert(response.attempts == 0 && !response.message.empty() && response.body.empty());
  };
  const auto good = env.endpoint();
  expect_not_sent(call(good, 9, "test/Echo", now + seconds(1)), Status::InvalidArgument); // no such key
  for (const char *endpoint : {"", "localhost:1", "127.0.0.1", "127.0.0.1:", ":80", "127.0.0.1:0",
                               "127.0.0.1:65536", "127.0.0.1:99999", "127.0.0.1:12ab", "::1:80",
                               "[::1]", "[::1]80", "[::1", "::1", "127.0.0.1:80:80", "example.org:80"})
    expect_not_sent(call(endpoint, 7, "test/Echo", now + seconds(1)), Status::InvalidArgument);
  expect_not_sent(call(good, 7, "", now + seconds(1)), Status::InvalidArgument);
  expect_not_sent(call(good, 7, std::string(129, 'm'), now + seconds(1)), Status::InvalidArgument);
  expect_not_sent(call(good, 7, "test/Echo", steady_clock::time_point::max()), Status::InvalidArgument);
  expect_not_sent(call(good, 7, "test/Echo", now - seconds(1)), Status::DeadlineExceeded);
  assert(env.server->stats().received == 0);
  assert(env.call(std::string(128, 'm'), "{}").status == Status::NotFound); // the longest method
  ClientOptions bad;
  bad.steady_interval = milliseconds(0);
  throws<std::invalid_argument>([&] { Client client(keys(), bad); });
  bad = {};
  bad.backoff = {milliseconds(10), milliseconds(-1)};
  throws<std::invalid_argument>([&] { Client client(keys(), bad); });
  // Nobody answers: the call ends at its deadline, after retransmitting.
  Peer silent;
  const auto start = steady_clock::now();
  const auto lost = env.client.call(endpoint_of(silent.port()), 7, "test/Echo", "{}", start + milliseconds(400));
  assert(lost.delivery == Delivery::OutcomeUnknown && lost.status == Status::DeadlineExceeded);
  assert(steady_clock::now() - start >= milliseconds(395) && steady_clock::now() - start < milliseconds(900));
  assert(lost.attempts >= 4 && lost.attempts <= 6 && !lost.message.empty());
  assert(silent.receive_all(milliseconds(50)).size() == lost.attempts);
  // No route at all: nothing could be sent.
  if (host == "127.0.0.1") {
    const auto unreachable = env.client.call("[2001:db8::1]:9", 7, "test/Echo", "{}", steady_clock::now() + milliseconds(200));
    assert(unreachable.delivery == Delivery::NotSent || unreachable.delivery == Delivery::OutcomeUnknown);
    if (unreachable.delivery == Delivery::NotSent) assert(unreachable.status == Status::Unavailable);
  }
}

void retransmission_schedule_on_the_wire() {
  Peer fake;
  Response response;
  const auto start = steady_clock::now();
  std::thread caller([&] {
    Client client(keys());
    response = client.call(endpoint_of(fake.port()), 7, "test/Echo", "{}", start + milliseconds(1100));
  });
  std::vector<std::pair<milliseconds, Bytes>> arrivals;
  while (auto bytes = fake.receive(milliseconds(400)))
    arrivals.emplace_back(duration_cast<milliseconds>(steady_clock::now() - start), *bytes);
  caller.join();
  assert(response.delivery == Delivery::OutcomeUnknown && response.attempts == arrivals.size());
  // 0, 30, 90, 210, 450, 700, 950 ms: waits of 30, 60, 120 and 240 ms, then every 250 ms.
  const long expected[] = {0, 30, 90, 210, 450, 700, 950};
  assert(arrivals.size() >= 6 && arrivals.size() <= 8);
  for (std::size_t i = 0; i < 7 && i < arrivals.size(); ++i) {
    const long at = static_cast<long>(arrivals[i].first.count());
    assert(at >= expected[i] - 5 && at <= expected[i] + 120);
  }
  // Every retransmission is the same datagram: one request id, one tag.
  for (const auto &arrival : arrivals) assert(arrival.second == arrivals.front().second);
  Datagram d;
  assert(parse(arrivals[0].second.data(), arrivals[0].second.size(), d) == Parse::Ok && verify(d, key7));
  assert(d.method == "test/Echo" && d.flags == 0);
  assert(d.word >= 1090 && d.word <= 1100); // the budget that remained when it was first sent
  // A custom schedule is honored.
  ClientOptions fast;
  fast.backoff = {milliseconds(5), milliseconds(5)};
  fast.steady_interval = milliseconds(50);
  Client client(keys(), fast);
  const auto began = steady_clock::now();
  const auto quick = client.call(endpoint_of(fake.port()), 7, "test/Echo", "{}", began + milliseconds(180));
  assert(quick.attempts >= 4 && quick.attempts <= 6);
}

// A server written by hand, to see what the client accepts.
void client_ignores_what_is_not_its_reply() {
  Peer fake;
  const InstanceId instance{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}};
  const InstanceId other_instance{{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}};
  const auto endpoint = endpoint_of(fake.port());
  const auto run = [&](std::optional<InstanceId> pin, steady_clock::duration timeout,
                       const std::function<std::vector<Bytes>(const Datagram &)> &answer) {
    Response response;
    std::thread caller([&] {
      Client client(keys());
      response = client.call(endpoint, 7, "test/Echo", "{}", steady_clock::now() + timeout, pin);
    });
    Address from;
    const auto request = fake.receive(seconds(2), &from);
    assert(request);
    Datagram d;
    assert(parse(request->data(), request->size(), d) == Parse::Ok && verify(d, key7));
    for (const auto &reply : answer(d)) fake.send_to(from, reply);
    caller.join();
    return response;
  };
  const auto reply = [](const Key &key, std::uint32_t key_id, const RequestId &id, const InstanceId &from,
                        std::uint32_t status, const std::string &body) {
    return encode_reply(key, key_id, id, from, status, body);
  };
  // Forgeries and strays first, the real reply last: only the last counts.
  const auto response = run(std::nullopt, seconds(3), [&](const Datagram &d) {
    std::vector<Bytes> replies;
    RequestId other_id = d.request_id;
    other_id[0] ^= 1;
    replies.push_back(reply(key7, 7, other_id, instance, 0, "wrong request id"));
    replies.push_back(reply(key8, 7, d.request_id, instance, 0, "wrong key"));
    replies.push_back(reply(key8, 8, d.request_id, instance, 0, "other key id"));
    auto bad_tag = reply(key7, 7, d.request_id, instance, 0, "bad tag");
    bad_tag.back() ^= 1;
    replies.push_back(bad_tag);
    replies.push_back(encode_request(key7, 7, d.request_id, std::nullopt, 1000, "test/Echo", "request type"));
    replies.push_back(reply(key7, 7, d.request_id, instance, 99, "unknown status"));
    auto flags = reply(key7, 7, d.request_id, instance, 0, "reply flags");
    flags[7] = 1;
    replies.push_back(resign(flags, key7));
    replies.push_back(Bytes(40, 7));
    replies.push_back(reply(key7, 7, d.request_id, instance, 0, "real reply"));
    return replies;
  });
  assert(response.delivery == Delivery::ResponseReceived && response.body == "real reply");
  assert(response.status == Status::Ok && response.instance == instance && response.attempts >= 1);
  // A pinned client skips replies of other instances, except the fence's conflict.
  const auto pinned = run(instance, seconds(3), [&](const Datagram &d) {
    return std::vector<Bytes>{reply(key7, 7, d.request_id, other_instance, 0, "impostor"),
                              reply(key7, 7, d.request_id, other_instance, 8, "impostor error"),
                              reply(key7, 7, d.request_id, instance, 0, "pinned reply")};
  });
  assert(pinned.body == "pinned reply" && pinned.instance == instance);
  const auto fenced = run(instance, seconds(3), [&](const Datagram &d) {
    return std::vector<Bytes>{reply(key7, 7, d.request_id, other_instance, 3, "{\"code\":\"conflict\"}")};
  });
  assert(fenced.delivery == Delivery::ResponseReceived && fenced.status == Status::Conflict);
  assert(fenced.instance == other_instance);
  // A deadline further away than udp.v1's 60 s is shortened on the wire.
  std::uint32_t advertised = 0;
  run(std::nullopt, hours(1), [&](const Datagram &d) {
    advertised = d.word;
    return std::vector<Bytes>{reply(key7, 7, d.request_id, instance, 0, "ok")};
  });
  assert(advertised == max_timeout_ms);
  // Replies may come from any address: the tag decides, not the source.
  Peer elsewhere;
  Response from_elsewhere;
  std::thread caller([&] {
    Client client(keys());
    from_elsewhere = client.call(endpoint, 7, "test/Echo", "{}", steady_clock::now() + seconds(3));
  });
  Address from;
  const auto request = fake.receive(seconds(2), &from);
  assert(request);
  Datagram d;
  assert(parse(request->data(), request->size(), d) == Parse::Ok);
  elsewhere.send_to(from, reply(key7, 7, d.request_id, instance, 0, "from another socket"));
  caller.join();
  assert(from_elsewhere.body == "from another socket");
}

// A proxy that loses chosen datagrams between a client and a server.
class LossyProxy {
public:
  LossyProxy(std::uint16_t server_port, int drop_requests, int drop_replies)
      : server_port_(server_port), drop_requests_(drop_requests), drop_replies_(drop_replies),
        thread_([this] { run(); }) {}
  ~LossyProxy() {
    stop_ = true;
    thread_.join();
  }
  std::uint16_t port() const { return front_.port(); }
  int dropped_requests() const { return dropped_requests_; }
  int dropped_replies() const { return dropped_replies_; }

private:
  void run() {
    Address client_address;
    bool known = false;
    while (!stop_) {
      pollfd fds[2] = {{front_.fd(), POLLIN, 0}, {back_.fd(), POLLIN, 0}};
      if (::poll(fds, 2, 20) <= 0) continue;
      if (fds[0].revents & POLLIN) {
        if (auto bytes = front_.receive(milliseconds(0), &client_address)) {
          known = true;
          if (dropped_requests_ < drop_requests_) ++dropped_requests_;
          else back_.send_to(server_port_, *bytes);
        }
      }
      if ((fds[1].revents & POLLIN) && known) {
        if (auto bytes = back_.receive(milliseconds(0))) {
          if (dropped_replies_ < drop_replies_) ++dropped_replies_;
          else front_.send_to(client_address, *bytes);
        }
      }
    }
  }
  const std::uint16_t server_port_;
  const int drop_requests_, drop_replies_;
  std::atomic<bool> stop_{false};
  std::atomic<int> dropped_requests_{0}, dropped_replies_{0};
  Peer front_, back_;
  std::thread thread_; // last: starts after everything it uses exists
};

void loss_never_repeats_an_execution() {
  struct Case { int lost_requests, lost_replies; };
  for (const Case loss : {Case{2, 0}, Case{0, 2}, Case{1, 1}, Case{3, 3}}) {
    Env env;
    LossyProxy proxy(env.port(), loss.lost_requests, loss.lost_replies);
    Client client(keys());
    const auto response = client.call(endpoint_of(proxy.port()), 7, "test/Count", "", steady_clock::now() + seconds(3));
    assert(response.delivery == Delivery::ResponseReceived && response.status == Status::Ok);
    assert(response.body == "1" && env.probe.counter == 1);
    assert(proxy.dropped_requests() == loss.lost_requests && proxy.dropped_replies() == loss.lost_replies);
    if (loss.lost_replies) assert(env.server->stats().cache_hits >= static_cast<std::uint64_t>(loss.lost_replies));
    assert(response.attempts >= static_cast<unsigned>(loss.lost_requests + loss.lost_replies + 1));
  }
}

// ---- lifecycle ---------------------------------------------------------------

void shutdown_with_work_in_flight() {
  {
    // Work that finishes inside the drain budget still delivers its reply.
    Env env;
    Response response;
    std::thread caller([&] { response = env.call("test/Sleep", "200"); });
    assert(await([&] { return env.server->stats().inflight == 1; }));
    const auto start = steady_clock::now();
    assert(env.server->shutdown(seconds(2)));
    assert(steady_clock::now() - start >= milliseconds(100) && steady_clock::now() - start < seconds(1));
    caller.join();
    assert(response.delivery == Delivery::ResponseReceived && response.body == "slept");
    assert(env.server->shutdown(seconds(2))); // idempotent
    throws<std::logic_error>([&] { env.server->start(); });
    // The port is released.
    ServerOptions options;
    options.bind_address = host;
    options.port = env.port();
    Server again(options, keys());
  }
  {
    // Work that does not finish is cut off: no reply, and the Reply knows.
    Env env;
    Peer peer;
    peer.send_to(env.port(), Env::request("test/Hold", "", fresh_id()));
    assert(await([&] { return env.probe.holds == 1; }));
    const auto start = steady_clock::now();
    assert(!env.server->shutdown(milliseconds(80)));
    assert(steady_clock::now() - start >= milliseconds(70) && steady_clock::now() - start < seconds(1));
    assert(!env.server->shutdown(seconds(2))); // the first result stands
    {
      std::lock_guard<std::mutex> lock(env.probe.mutex);
      assert(!env.probe.held[0].complete(Status::Ok, "{}"));
      env.probe.held.clear();
    }
    assert(!peer.receive(milliseconds(150)) && env.server->stats().unanswered == 1);
  }
  {
    // While draining: new requests are refused, duplicates of running ones ignored.
    Env env;
    Peer peer;
    const auto running = Env::request("test/Hold", "", fresh_id());
    peer.send_to(env.port(), running);
    assert(await([&] { return env.probe.holds == 1; }));
    std::atomic<bool> drained{false};
    std::thread closer([&] { drained = env.server->shutdown(seconds(5)); });
    std::this_thread::sleep_for(milliseconds(100));
    peer.send_to(env.port(), Env::request("test/Echo", "{}", fresh_id()));
    const auto refusal = peer.receive(seconds(2));
    assert(refusal && static_cast<Status>(parse_reply(*refusal).word) == Status::Unavailable);
    peer.send_to(env.port(), running);
    assert(!peer.receive(milliseconds(150)) && !drained);
    env.probe.complete_held(Status::Ok, "finished while draining");
    const auto reply = peer.receive(seconds(2));
    assert(reply && parse_reply(*reply).body == "finished while draining");
    closer.join();
    assert(drained && env.probe.echoes == 0);
    peer.send_to(env.port(), running); // the cache keeps answering until the socket closes
  }
  {
    // A Reply may outlive its Server.
    auto env = std::make_unique<Env>();
    Peer peer;
    peer.send_to(env->port(), Env::request("test/Hold", "", fresh_id()));
    assert(await([&] { return env->probe.holds == 1; }));
    Reply survivor = std::move(env->probe.held[0]);
    env->probe.held.clear();
    env.reset();
    assert(!survivor.complete(Status::Ok, "{}"));
    assert(!peer.receive(milliseconds(100)));
  }
}

void construction_and_registration_rules() {
  const auto build = [](const std::function<void(ServerOptions &)> &configure, KeyRing ring = keys()) {
    ServerOptions options;
    options.bind_address = host;
    configure(options);
    Server server(options, std::move(ring));
  };
  throws<std::invalid_argument>([&] { build([](ServerOptions &) {}, KeyRing{}); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.call_budget = milliseconds(0); }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.call_budget = seconds(61); o.reply_cache_ttl = seconds(500); }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.max_inflight = 0; }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.max_inflight = 64; o.reply_cache_capacity = 64; }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.reply_cache_ttl = seconds(2); }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.requests_per_second = 0; }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.burst = 0; }); });
  throws<std::invalid_argument>([&] { build([](ServerOptions &o) { o.max_rate_sources = 0; }); });
  for (const char *name : {"", "localhost", "256.0.0.1", "1.2.3", "::g", "[::1]", "example.org"})
    throws<std::invalid_argument>([&] { build([&](ServerOptions &o) { o.bind_address = name; }); });
  {
    Env first;
    throws<std::system_error>([&] { build([&](ServerOptions &o) { o.port = first.port(); }); });
  }
  Server server(ServerOptions{}, keys());
  const Server::Handler handler = [](Request, Reply) {};
  throws<std::logic_error>([&] { server.add_method("", handler); });
  throws<std::logic_error>([&] { server.add_method(std::string(129, 'm'), handler); });
  throws<std::logic_error>([&] { server.add_method("test/Null", nullptr); });
  server.add_method(std::string(128, 'm'), handler);
  server.add_method("test/Once", handler);
  throws<std::logic_error>([&] { server.add_method("test/Once", handler); });
  server.start();
  throws<std::logic_error>([&] { server.start(); });
  throws<std::logic_error>([&] { server.add_method("test/Late", handler); });
  assert(server.port() != 0);
}

void concurrent_callers() {
  // The default budget (50 requests per second per source) would throttle this.
  Env env([](ServerOptions &options) { options.requests_per_second = 100000; options.burst = 100000; });
  constexpr int threads = 8, calls = 40;
  std::atomic<int> ok{0};
  std::vector<std::thread> callers;
  for (int t = 0; t < threads; ++t)
    callers.emplace_back([&, t] {
      for (int i = 0; i < calls; ++i) {
        const std::string body = "{\"t\":" + std::to_string(t) + ",\"i\":" + std::to_string(i) + "}";
        const auto echo = env.call("test/Echo", body);
        if (echo.status == Status::Ok && echo.body == body && env.call("test/Count", "").status == Status::Ok) ++ok;
      }
    });
  for (auto &caller : callers) caller.join();
  assert(ok == threads * calls && env.probe.counter == static_cast<std::uint64_t>(threads * calls));
}

// ---- IPv6 without IPv6 -------------------------------------------------------

bool can_bind(const std::string &address) {
  const auto target = make_address(address, 0);
  if (!target) return false;
  const int fd = ::socket(target->family(), SOCK_DGRAM, 0);
  if (fd < 0) return false;
  const bool bound = ::bind(fd, target->get(), target->size) == 0;
  ::close(fd);
  return bound;
}
void write_file(const char *path, const std::string &text) { std::ofstream(path) << text; }
// A new network namespace has a loopback interface that is down; bringing it up
// also gives it ::1. Needs an unprivileged user namespace to own it.
bool enter_private_loopback() {
  const auto uid = ::geteuid();
  const auto gid = ::getegid();
  if (::unshare(CLONE_NEWUSER | CLONE_NEWNET) != 0) return false;
  write_file("/proc/self/setgroups", "deny");
  write_file("/proc/self/uid_map", "0 " + std::to_string(uid) + " 1");
  write_file("/proc/self/gid_map", "0 " + std::to_string(gid) + " 1");
  const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return false;
  ifreq request{};
  std::strncpy(request.ifr_name, "lo", IFNAMSIZ - 1);
  const bool up = ::ioctl(fd, SIOCGIFFLAGS, &request) == 0 &&
                  (request.ifr_flags |= IFF_UP, ::ioctl(fd, SIOCSIFFLAGS, &request) == 0);
  ::close(fd);
  return up;
}
} // namespace

int main(int argc, char **argv) {
  if (argc > 1) host = argv[1];
  if (!can_bind(host)) {
    // Only before any thread exists may a process unshare its namespaces.
    if (!(enter_private_loopback() && can_bind(host))) {
      std::cout << "SKIP: " << host << " is not available here\n";
      return 77;
    }
    std::cout << "using a private network namespace for " << host << '\n';
  }
  basic_calls();
  configured_instance();
  at_most_once_under_duplicates();
  retransmissions_of_a_running_call_are_ignored();
  retransmitting_client_runs_the_handler_once();
  async_completion_keeps_the_io_thread_free();
  missed_deadlines_send_nothing();
  handlers_that_drop_or_fail();
  oversized_messages();
  unauthenticated_and_malformed_datagrams_get_silence();
  rate_limit_per_source();
  instance_pinning();
  limits_and_the_reply_cache();
  client_input_validation();
  retransmission_schedule_on_the_wire();
  client_ignores_what_is_not_its_reply();
  loss_never_repeats_an_execution();
  shutdown_with_work_in_flight();
  construction_and_registration_rules();
  concurrent_callers();
  std::cout << "udp.v1 over " << host << ": calls, at-most-once under duplicates and loss, async replies, "
               "deadlines, authentication drops, rate limits, fencing, limits, cache window, client "
               "validation and shutdown passed\n";
}
