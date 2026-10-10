#include "udp_net.hpp"
#include "udp_wire.hpp"
#include <netinet/in.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <list>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

namespace xgc2::xrpc::udp {
namespace detail {
using SteadyClock = std::chrono::steady_clock;
using TimePoint = SteadyClock::time_point;

struct CacheKey {
  std::uint32_t key_id = 0;
  RequestId request_id{};
  bool operator==(const CacheKey &other) const {
    return key_id == other.key_id && request_id == other.request_id;
  }
};
struct CacheKeyHash {
  std::size_t operator()(const CacheKey &key) const {
    char bytes[sizeof key.key_id + 16];
    std::memcpy(bytes, &key.key_id, sizeof key.key_id);
    std::memcpy(bytes + sizeof key.key_id, key.request_id.data(), 16);
    return std::hash<std::string_view>()(std::string_view(bytes, sizeof bytes));
  }
};

enum class CallState {
  InFlight,  // admitted; its handler or Reply may still complete it
  Replied,   // `reply` holds the bytes to resend for a retransmission
  Abandoned  // no reply exists and none will be sent (missed deadline, dropped Reply)
};
// One request id seen inside the cache window. Guarded by ServerCore::mutex_.
struct Entry {
  CacheKey key;
  CallState state = CallState::InFlight;
  TimePoint deadline, expires;
  Address peer; // where a completion is sent
  std::vector<std::uint8_t> reply;
  bool reply_alive = true;       // the Reply handed to the handler still exists
  bool handler_returned = false; // the I/O thread's handler call has returned
  std::list<std::shared_ptr<Entry>>::iterator position;
};

struct Fd {
  int value = -1;
  Fd() = default;
  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;
  ~Fd() { reset(); }
  void reset() noexcept {
    if (value >= 0) ::close(value);
    value = -1;
  }
};

struct Bucket {
  double tokens;
  TimePoint last;
};
} // namespace detail

struct Reply::State {
  std::shared_ptr<detail::ServerCore> core;
  std::shared_ptr<detail::Entry> entry;
  ~State(); // tells the core that the Reply is gone
};

namespace detail {
class ServerCore : public std::enable_shared_from_this<ServerCore> {
public:
  ServerCore(ServerOptions options, KeyRing keys)
      : options_(std::move(options)), keys_(std::move(keys)) {
    if (keys_.empty()) throw std::invalid_argument("udp server needs at least one key");
    if (options_.call_budget <= std::chrono::milliseconds::zero() ||
        options_.call_budget > std::chrono::milliseconds(max_timeout_ms))
      throw std::invalid_argument("call_budget must be within 1 ms .. 60 s");
    if (options_.max_inflight == 0 || options_.reply_cache_capacity <= options_.max_inflight)
      throw std::invalid_argument("reply_cache_capacity must exceed max_inflight, which must be positive");
    if (options_.reply_cache_ttl <= options_.call_budget)
      throw std::invalid_argument("reply_cache_ttl must exceed call_budget");
    if (options_.requests_per_second == 0 || options_.burst == 0 || options_.max_rate_sources == 0)
      throw std::invalid_argument("rate limit values must be positive");
    if (options_.instance) {
      if (*options_.instance == InstanceId{}) throw std::invalid_argument("the instance must not be all zero");
      instance_ = *options_.instance;
    } else {
      do random_bytes(instance_.data(), instance_.size());
      while (instance_ == InstanceId{});
    }
    const auto address = make_address(options_.bind_address, options_.port);
    if (!address) throw std::invalid_argument("bind_address must be a numeric IPv4 or IPv6 address");
    socket_.value = ::socket(address->family(), SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (socket_.value < 0) throw std::system_error(errno, std::generic_category(), "socket");
    if (address->family() == AF_INET6) {
      // "::" serves IPv4 senders too; a specific IPv6 address serves IPv6 only.
      const int v6_only = is_unspecified_v6(*address) ? 0 : 1;
      ::setsockopt(socket_.value, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof v6_only);
    }
    if (::bind(socket_.value, address->get(), address->size) != 0)
      throw std::system_error(errno, std::generic_category(), "bind " + options_.bind_address);
    Address bound;
    bound.size = sizeof bound.storage;
    if (::getsockname(socket_.value, bound.get(), &bound.size) != 0)
      throw std::system_error(errno, std::generic_category(), "getsockname");
    port_ = port_of(bound);
    wake_.value = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_.value < 0) throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  ~ServerCore() { shutdown(std::chrono::milliseconds::zero()); }

  void add_method(std::string name, Server::Handler handler) {
    if (!valid_method(name) || !handler)
      throw std::logic_error("udp method needs a name of 1..128 bytes of UTF-8 without spaces and a handler");
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) throw std::logic_error("udp methods are fixed once the server has started");
    if (!methods_.emplace(std::move(name), std::move(handler)).second)
      throw std::logic_error("udp method is already registered");
  }

  void start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_ || closed_) throw std::logic_error("udp server already started or shut down");
    started_ = true;
    thread_ = std::thread([this] { run(); });
  }

  bool shutdown(std::chrono::milliseconds drain_budget) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) return drained_;
    if (thread_.joinable() && thread_.get_id() == std::this_thread::get_id())
      throw std::logic_error("udp server cannot be shut down from its own handler");
    admitting_ = false;
    drained_ = idle_.wait_for(lock, drain_budget, [this] { return inflight_ == 0; });
    while (!pending_.empty()) abandon_locked(*pending_.back());
    closed_ = true; // completions made from here on return false
    lock.unlock();
    stop_.store(true);
    signal_wake();
    if (thread_.joinable()) thread_.join();
    lock.lock();
    socket_.reset();
    wake_.reset();
    methods_.clear(); // release whatever the handlers captured
    return drained_;
  }

  std::uint16_t port() const noexcept { return port_; }
  const InstanceId &instance() const noexcept { return instance_; }

  ServerStats stats() const noexcept {
    ServerStats stats;
    stats.received = received_;
    stats.dropped_malformed = dropped_malformed_;
    stats.dropped_auth = dropped_auth_;
    stats.dropped_rate = dropped_rate_;
    stats.cache_hits = cache_hits_;
    stats.inflight_ignored = inflight_ignored_;
    stats.unanswered = unanswered_;
    stats.replies_sent = replies_sent_;
    std::lock_guard<std::mutex> lock(mutex_);
    stats.inflight = inflight_;
    return stats;
  }

  // ---- Reply support
  bool complete(Entry &entry, Status status, std::string_view body) {
    std::lock_guard<std::mutex> lock(mutex_);
    return complete_locked(entry, status, body);
  }
  void reply_destroyed(Entry &entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    entry.reply_alive = false;
    if (entry.state == CallState::InFlight && entry.handler_returned) abandon_locked(entry);
  }

private:
  void signal_wake() noexcept {
    const std::uint64_t one = 1;
    ssize_t written;
    do written = ::write(wake_.value, &one, sizeof one);
    while (written < 0 && errno == EINTR);
  }

  void run() {
    std::array<std::uint8_t, 2048> buffer;
    pollfd fds[2] = {{socket_.value, POLLIN, 0}, {wake_.value, POLLIN, 0}};
    while (!stop_.load()) {
      const int ready = ::poll(fds, 2, poll_timeout_ms());
      if (ready < 0 && errno != EINTR) break;
      if (ready > 0 && (fds[1].revents & POLLIN)) {
        std::uint64_t ignored;
        const auto n = ::read(wake_.value, &ignored, sizeof ignored);
        (void)n;
      }
      if (ready > 0 && (fds[0].revents & (POLLIN | POLLERR))) receive(buffer);
      sweep(SteadyClock::now());
    }
  }

  // A bounded batch per wake-up, so timers and shutdown stay responsive.
  void receive(std::array<std::uint8_t, 2048> &buffer) {
    for (unsigned count = 0; count < 64 && !stop_.load(); ++count) {
      Address from;
      from.size = sizeof from.storage;
      const auto n = ::recvfrom(socket_.value, buffer.data(), buffer.size(), 0, from.get(), &from.size);
      if (n < 0) {
        if (errno == EINTR) continue;
        return; // EAGAIN: drained; anything else is not about a datagram
      }
      handle(buffer.data(), static_cast<std::size_t>(n), from, SteadyClock::now());
    }
  }

  void handle(const std::uint8_t *data, std::size_t size, const Address &from, TimePoint now) {
    ++received_;
    Datagram d;
    if (parse(data, size, d) != Parse::Ok || d.type != Type::Request) {
      ++dropped_malformed_;
      return;
    }
    // Everything below is cheap or needs an authenticated sender.
    const Key *key = keys_.find(d.key_id);
    if (!key || !verify(d, *key)) {
      ++dropped_auth_;
      return;
    }
    if (!allow(source_key(from), now)) {
      ++dropped_rate_;
      return;
    }
    if ((d.flags & ~flag_expected_instance) != 0 || d.word < 1 || d.word > max_timeout_ms) {
      reject(from, d, Status::InvalidArgument, "reserved flags set or timeout_ms outside 1..60000");
      return;
    }
    if ((d.flags & flag_expected_instance) && d.instance != instance_) {
      reject(from, d, Status::Conflict, "service instance mismatch");
      return;
    }
    const CacheKey cache_key{d.key_id, d.request_id};
    std::unique_lock<std::mutex> lock(mutex_);
    auto found = cache_.find(cache_key);
    if (found != cache_.end() && found->second->state != CallState::InFlight &&
        found->second->expires <= now) {
      erase_locked(found->second);
      found = cache_.end();
    }
    if (found != cache_.end()) {
      const Entry &entry = *found->second;
      if (entry.state == CallState::Replied) {
        ++cache_hits_;
        send(entry.reply, from);
      } else {
        ++inflight_ignored_; // running, or ended without a reply: nothing to send
      }
      return;
    }
    if (!admitting_) {
      lock.unlock();
      reject(from, d, Status::Unavailable, "server is shutting down");
      return;
    }
    const auto method = methods_.find(std::string(d.method));
    if (method == methods_.end()) {
      lock.unlock();
      reject(from, d, Status::NotFound, "unknown method");
      return;
    }
    if (inflight_ >= options_.max_inflight) {
      lock.unlock();
      reject(from, d, Status::ResourceExhausted, "too many calls in flight");
      return;
    }
    auto entry = std::make_shared<Entry>();
    entry->key = cache_key;
    entry->peer = from;
    entry->deadline = now + std::min<SteadyClock::duration>(std::chrono::milliseconds(d.word),
                                                           options_.call_budget);
    entry->expires = now + options_.reply_cache_ttl;
    make_room_locked(now);
    order_.push_back(entry);
    entry->position = std::prev(order_.end());
    cache_.emplace(cache_key, entry);
    pending_.push_back(entry);
    ++inflight_;
    lock.unlock();

    Request request;
    request.method.assign(d.method);
    request.body.assign(d.body);
    request.key_id = d.key_id;
    request.request_id = d.request_id;
    request.peer = format_address(from.get(), from.size);
    request.deadline = entry->deadline;
    bool threw = false;
    try {
      method->second(std::move(request),
                     Reply(std::unique_ptr<Reply::State>(new Reply::State{shared_from_this(), entry})));
    } catch (...) {
      threw = true;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    entry->handler_returned = true;
    if (entry->state != CallState::InFlight || entry->reply_alive) return;
    // No Reply survives: a handler that failed answers internal, one that
    // simply dropped it has chosen to send nothing.
    if (threw)
      complete_locked(*entry, Status::Internal, error_body(Status::Internal, "handler failed"));
    else
      abandon_locked(*entry);
  }

  // An error reply for an authenticated request that was not admitted. Not
  // cached: the condition may pass before the client retransmits.
  void reject(const Address &to, const Datagram &request, Status status, const char *message) {
    send(build_reply(request.key_id, request.request_id, status, error_body(status, message)), to);
  }

  std::vector<std::uint8_t> build_reply(std::uint32_t key_id, const RequestId &request_id,
                                        Status status, std::string_view body) const {
    const Key &key = *keys_.find(key_id);
    if (body.size() > max_body_bytes(0)) {
      status = Status::ResourceExhausted;
      static const std::string too_big = error_body(
          Status::ResourceExhausted, "reply exceeds the 1200-byte udp.v1 datagram limit");
      return encode_reply(key, key_id, request_id, instance_, static_cast<std::uint32_t>(status), too_big);
    }
    return encode_reply(key, key_id, request_id, instance_, static_cast<std::uint32_t>(status), body);
  }

  bool send(const std::vector<std::uint8_t> &bytes, const Address &to) {
    const auto n = ::sendto(socket_.value, bytes.data(), bytes.size(), MSG_NOSIGNAL, to.get(), to.size);
    if (n != static_cast<ssize_t>(bytes.size())) return false;
    ++replies_sent_;
    return true;
  }

  // Under mutex_. The socket is open until the I/O thread has been joined.
  bool complete_locked(Entry &entry, Status status, std::string_view body) {
    if (closed_ || entry.state != CallState::InFlight) return false;
    if (SteadyClock::now() >= entry.deadline) {
      abandon_locked(entry);
      return false;
    }
    entry.reply = build_reply(entry.key.key_id, entry.key.request_id, status, body);
    entry.state = CallState::Replied;
    finish_locked(entry);
    send(entry.reply, entry.peer); // a lost datagram is recovered by a retransmission
    return true;
  }

  void abandon_locked(Entry &entry) {
    if (entry.state != CallState::InFlight) return;
    entry.state = CallState::Abandoned;
    ++unanswered_;
    finish_locked(entry);
  }

  void finish_locked(Entry &entry) {
    pending_.erase(std::find_if(pending_.begin(), pending_.end(),
                                [&entry](const std::shared_ptr<Entry> &p) { return p.get() == &entry; }));
    if (--inflight_ == 0) idle_.notify_all();
  }

  // By value: the argument may be the very list element that is erased.
  void erase_locked(std::shared_ptr<Entry> entry) {
    order_.erase(entry->position);
    cache_.erase(entry->key);
  }

  // Drops entries past their TTL, then the oldest finished ones until there is
  // room for one more. Running calls are never dropped: max_inflight is below
  // the capacity, so a finished entry always exists.
  void make_room_locked(TimePoint now) {
    while (!order_.empty() && order_.front()->state != CallState::InFlight &&
           order_.front()->expires <= now)
      erase_locked(order_.front());
    while (cache_.size() >= options_.reply_cache_capacity) {
      const auto oldest = std::find_if(order_.begin(), order_.end(), [](const std::shared_ptr<Entry> &e) {
        return e->state != CallState::InFlight;
      });
      if (oldest == order_.end()) break;
      erase_locked(*oldest);
    }
  }

  // Calls that outlive their deadline without a completion end here, so a lost
  // Reply cannot hold an in-flight slot or block a drain.
  void sweep(TimePoint now) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (std::size_t i = 0; i < pending_.size();) {
        if (pending_[i]->deadline <= now) abandon_locked(*pending_[i]);
        else ++i;
      }
      while (!order_.empty() && order_.front()->state != CallState::InFlight &&
             order_.front()->expires <= now)
        erase_locked(order_.front());
    }
    if (now >= next_purge_) {
      next_purge_ = now + std::chrono::seconds(1);
      purge_idle_sources(now);
    }
  }

  int poll_timeout_ms() {
    const auto now = SteadyClock::now();
    auto next = now + std::chrono::milliseconds(250);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto &entry : pending_) next = std::min(next, entry->deadline);
    }
    const auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(next - now) +
                      std::chrono::milliseconds(1);
    return static_cast<int>(std::max<std::int64_t>(1, wait.count()));
  }

  // Token bucket per source address, touched by the I/O thread only.
  double refilled(const Bucket &bucket, TimePoint now) const {
    const double elapsed = std::chrono::duration<double>(now - bucket.last).count();
    return std::min<double>(options_.burst, bucket.tokens + elapsed * options_.requests_per_second);
  }
  bool allow(const SourceKey &source, TimePoint now) {
    auto found = buckets_.find(source);
    if (found == buckets_.end()) {
      if (buckets_.size() >= options_.max_rate_sources) {
        purge_idle_sources(now);
        if (buckets_.size() >= options_.max_rate_sources) return false;
      }
      found = buckets_.emplace(source, Bucket{static_cast<double>(options_.burst), now}).first;
    }
    Bucket &bucket = found->second;
    bucket.tokens = refilled(bucket, now);
    bucket.last = now;
    if (bucket.tokens < 1.0) return false;
    bucket.tokens -= 1.0;
    return true;
  }
  void purge_idle_sources(TimePoint now) {
    for (auto it = buckets_.begin(); it != buckets_.end();) {
      // A full bucket is indistinguishable from a source never seen.
      if (refilled(it->second, now) >= options_.burst) it = buckets_.erase(it);
      else ++it;
    }
  }

  const ServerOptions options_;
  const KeyRing keys_;
  InstanceId instance_{};
  Fd socket_, wake_;
  std::uint16_t port_ = 0;
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::unordered_map<std::string, Server::Handler> methods_;
  // I/O thread only.
  std::unordered_map<SourceKey, Bucket, SourceKeyHash> buckets_;
  TimePoint next_purge_{};
  // Shared with completing threads.
  mutable std::mutex mutex_;
  std::condition_variable idle_;
  std::unordered_map<CacheKey, std::shared_ptr<Entry>, CacheKeyHash> cache_;
  std::list<std::shared_ptr<Entry>> order_; // by arrival, oldest first
  std::vector<std::shared_ptr<Entry>> pending_;
  std::size_t inflight_ = 0;
  bool started_ = false, admitting_ = true, closed_ = false, drained_ = true;
  std::atomic<std::uint64_t> received_{0}, dropped_malformed_{0}, dropped_auth_{0},
      dropped_rate_{0}, cache_hits_{0}, inflight_ignored_{0}, unanswered_{0}, replies_sent_{0};
};
} // namespace detail

Reply::State::~State() { core->reply_destroyed(*entry); }

Reply::Reply() noexcept = default;
Reply::Reply(std::unique_ptr<State> state) noexcept : state_(std::move(state)) {}
Reply::Reply(Reply &&) noexcept = default;
Reply &Reply::operator=(Reply &&) noexcept = default;
Reply::~Reply() = default;

bool Reply::complete(Status status, std::string_view body) {
  if (status == Status::Unauthenticated || static_cast<std::uint32_t>(status) > 10)
    throw std::invalid_argument("udp.v1 never sends this status");
  return state_ && state_->core->complete(*state_->entry, status, body);
}

bool Reply::error(Status status, std::string_view message, std::string_view details_json) {
  if (status == Status::Ok) throw std::invalid_argument("error() needs a status other than ok");
  return complete(status, error_body(status, message, details_json));
}

Server::Server(ServerOptions options, KeyRing keys)
    : core_(std::make_shared<detail::ServerCore>(std::move(options), std::move(keys))) {}
Server::~Server() {
  if (core_) core_->shutdown(std::chrono::milliseconds::zero());
}
void Server::add_method(std::string name, Handler handler) {
  core_->add_method(std::move(name), std::move(handler));
}
void Server::start() { core_->start(); }
bool Server::shutdown(std::chrono::milliseconds drain_budget) { return core_->shutdown(drain_budget); }
std::uint16_t Server::port() const noexcept { return core_->port(); }
const InstanceId &Server::instance() const noexcept { return core_->instance(); }
ServerStats Server::stats() const noexcept { return core_->stats(); }
} // namespace xgc2::xrpc::udp
