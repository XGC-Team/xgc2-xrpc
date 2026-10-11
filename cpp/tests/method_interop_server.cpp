// xgc2-xrpc-method-interop-server: one MethodRouter served on http.v1 (a Unix
// socket) and on udp.v1, for cross-language tests of method addressing.
//
//   xgc2-xrpc-method-interop-server --unix <socket path> --bind <address> --port <port|0>
//                                   --key-file <path> --key-id <id>
//
// The socket must lie in a private directory (mode 0700) that exists. The udp.v1
// server serves only the key `--key-id` of the key file (`<key_id> <base64 key>`
// lines) on a numeric IPv4 or IPv6 address; port 0 picks an ephemeral port. Both
// transports share one instance: once they are up the server prints
// `READY <udp port> <instance hex>` on stdout and flushes, and runs until SIGINT
// or SIGTERM, then exits 0 after a short drain.
//
// Methods, the same on both transports (http.v1: POST /v1/call/<name>):
//   xgc2.fixture/Echo      returns the request body
//   xgc2.fixture/Count     adds one to a counter of this process and returns it as a
//                          decimal number: a retransmitted udp.v1 request never counts twice
//   xgc2.fixture/Sleep     body {"ms":N}: completes from another thread after N
//                          milliseconds (at most 60000) and returns the body
//   xgc2.fixture/Fail      body {"code":N}: answers error code N (1..10) with the details
//                          {"requested":N}; any other N is invalid_argument
//   xgc2.fixture/Describe  returns the readiness envelope, which lists the capability
//                          xgc2.fixture for the entities fixture-1 and fixture-2
// http.v1 also answers GET /v1/describe with that envelope.
#include "xgc2/xrpc/method_http.hpp"
#include "xgc2/xrpc/method_udp.hpp"
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>
using namespace xgc2::xrpc;

namespace {
// {"<name>": <unsigned integer>} and nothing else, whitespace allowed.
std::optional<std::uint32_t> integer_field(std::string_view body, std::string_view name) {
  std::size_t i = 0;
  const auto space = [&] {
    while (i < body.size() && (body[i] == ' ' || body[i] == '\t' || body[i] == '\n' || body[i] == '\r')) ++i;
  };
  const auto expect = [&](std::string_view text) {
    space();
    if (body.substr(i, text.size()) != text) return false;
    i += text.size();
    return true;
  };
  if (!expect("{") || !expect("\"") || !expect(name) || !expect("\"") || !expect(":")) return std::nullopt;
  space();
  std::uint64_t value = 0;
  std::size_t digits = 0;
  for (; i < body.size() && body[i] >= '0' && body[i] <= '9'; ++i, ++digits) value = value * 10 + (body[i] - '0');
  if (digits == 0 || digits > 9 || !expect("}")) return std::nullopt;
  space();
  if (i != body.size()) return std::nullopt;
  return static_cast<std::uint32_t>(value);
}

[[noreturn]] void usage(const std::string &problem) {
  std::cerr << problem << "\nusage: xgc2-xrpc-method-interop-server --unix <socket path> --bind <address> "
                          "--port <port> --key-file <path> --key-id <id>\n";
  std::exit(2);
}
} // namespace

int main(int argc, char **argv) {
  std::string socket_path, bind = "127.0.0.1", key_file;
  long port = 0, key_id = -1;
  for (int i = 1; i < argc; i += 2) {
    const std::string flag = argv[i];
    if (i + 1 >= argc) usage("missing value for " + flag);
    const std::string value = argv[i + 1];
    char *end = nullptr;
    if (flag == "--unix") socket_path = value;
    else if (flag == "--bind") bind = value;
    else if (flag == "--key-file") key_file = value;
    else if (flag == "--port") port = std::strtol(value.c_str(), &end, 10);
    else if (flag == "--key-id") key_id = std::strtol(value.c_str(), &end, 10);
    else usage("unknown option " + flag);
    if (end && (*end != '\0' || value.empty())) usage("not a number: " + value);
  }
  if (socket_path.empty() || key_file.empty() || key_id < 0 || key_id > 0xffffffffL || port < 0 || port > 65535)
    usage("--unix, --key-file, --key-id and a --port of 0..65535 are required");

  // Every thread inherits this mask; the main thread waits for the signals.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  try {
    const auto file = udp::KeyRing::load_file(key_file);
    const auto *key = file.find(static_cast<std::uint32_t>(key_id));
    if (!key) usage("the key file has no key " + std::to_string(key_id));
    udp::KeyRing keys;
    keys.add(static_cast<std::uint32_t>(key_id), std::string(key->begin(), key->end()));

    // The handlers use these, so they are declared before (and outlive) both servers.
    std::atomic<std::uint64_t> counter{0};
    std::mutex sleepers_mutex;
    std::condition_variable wake_sleepers;
    bool stopping = false;
    std::vector<std::thread> sleepers;
    const auto instance = new_instance_id();
    const auto describe = [&](bool ready) {
      const std::string facts = "{\"capabilities\":" + capabilities_json({{"xgc2.fixture", {"fixture-1", "fixture-2"}}}) + "}";
      return describe_json("xgc2-fixture-host", "v1", instance, ready, facts);
    };

    MethodRouter router;
    router.add("xgc2.fixture/Echo", [](MethodRequest request, MethodReply reply) { reply.complete(request.body); });
    router.add("xgc2.fixture/Count", [&counter](MethodRequest, MethodReply reply) { reply.complete(std::to_string(++counter)); });
    router.add("xgc2.fixture/Describe", [&describe](MethodRequest, MethodReply reply) { reply.complete(describe(true)); });
    router.add("xgc2.fixture/Sleep", [&](MethodRequest request, MethodReply reply) {
      const auto ms = integer_field(request.body, "ms");
      if (!ms || *ms > udp::max_timeout_ms) {
        reply.error(MethodCode::InvalidArgument, "body must be {\"ms\":N} with N <= 60000");
        return;
      }
      std::lock_guard<std::mutex> lock(sleepers_mutex);
      sleepers.emplace_back([&, ms = *ms, body = std::move(request.body), reply] {
        std::unique_lock<std::mutex> wait(sleepers_mutex);
        const bool shutdown = wake_sleepers.wait_for(wait, std::chrono::milliseconds(ms), [&] { return stopping; });
        wait.unlock();
        if (!shutdown) reply.complete(body);
      });
    });
    router.add("xgc2.fixture/Fail", [](MethodRequest request, MethodReply reply) {
      const auto code = integer_field(request.body, "code");
      if (!code || *code < 1 || *code > 10) {
        reply.error(MethodCode::InvalidArgument, "body must be {\"code\":N} with N in 1..10");
        return;
      }
      reply.error(static_cast<MethodCode>(*code), "requested failure", "{\"requested\":" + std::to_string(*code) + "}");
    });

    udp::ServerOptions udp_options;
    udp_options.bind_address = bind;
    udp_options.port = static_cast<std::uint16_t>(port);
    // One instance for both transports: the udp.v1 reply and the http.v1 fence agree.
    udp_options.instance = udp::instance_from_hex(instance);
    udp::Server udp_server(udp_options, std::move(keys));
    add_methods(udp_server, router);

    HttpIdentity identity;
    identity.instance_id = instance;
    identity.discovery_targets = {"/v1/describe"};
    HttpServer http_server(UnixOptions{socket_path}, [&](HttpRequest request, HttpReply reply) {
      if (handle_method_call(router, request, reply)) return;
      if (request.method == "GET" && request.target == "/v1/describe") {
        HttpResponse response;
        response.headers.emplace_back("Content-Type", "application/json");
        response.body = describe(true);
        reply.complete(std::move(response));
        return;
      }
      reply.complete(http_error(404, "not_found", "no such route"));
    }, HttpLimits{}, identity);

    udp_server.start();
    std::thread http_thread([&] { http_server.run(); });
    std::cout << "READY " << udp_server.port() << ' ' << instance << std::endl;

    int received = 0;
    sigwait(&signals, &received);
    {
      std::lock_guard<std::mutex> lock(sleepers_mutex);
      stopping = true;
    }
    wake_sleepers.notify_all();
    udp_server.shutdown(std::chrono::milliseconds(500));
    http_server.request_stop();
    http_thread.join();
    for (auto &sleeper : sleepers) sleeper.join();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "xgc2-xrpc-method-interop-server: " << error.what() << '\n';
    return 1;
  }
}
