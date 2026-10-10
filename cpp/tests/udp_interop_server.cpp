// xgc2-xrpc-udp-interop-server: a udp.v1 peer for cross-language tests.
//
//   xgc2-xrpc-udp-interop-server --bind <address> --port <port> --key-file <path> --key-id <id>
//
// Serves only the key `--key-id` of the key file (`<key_id> <base64 key>` lines),
// on a numeric IPv4 or IPv6 address; port 0 picks an ephemeral port. Once bound
// it prints `READY <port> <instance-hex>` on stdout and flushes; it runs until
// SIGINT or SIGTERM and then exits 0 after a short drain.
//
// Methods (bodies are JSON; the replies named below are the bodies):
//   test.v1/Echo   returns the request body
//   test.v1/Count  adds one to a counter of this process and returns it as a
//                  decimal number: a retransmitted request id never counts twice
//   test.v1/Sleep  body {"ms":N}: completes the reply from another thread after
//                  N milliseconds (at most 60000) and returns the request body
//   test.v1/Fail   body {"status":N}: replies with status N (1..8 or 10) and an
//                  error body; any other N is invalid_argument
//   test.v1/Big    tries to reply with more than 1200 bytes, which the transport
//                  turns into resource_exhausted
#include "xgc2/xrpc/udp.hpp"
#include <csignal>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
using namespace xgc2::xrpc::udp;

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
  std::cerr << problem << "\nusage: xgc2-xrpc-udp-interop-server --bind <address> --port <port> "
                          "--key-file <path> --key-id <id>\n";
  std::exit(2);
}
} // namespace

int main(int argc, char **argv) {
  std::string bind = "127.0.0.1", key_file;
  long port = 0, key_id = -1;
  for (int i = 1; i < argc; i += 2) {
    const std::string flag = argv[i];
    if (i + 1 >= argc) usage("missing value for " + flag);
    const std::string value = argv[i + 1];
    char *end = nullptr;
    if (flag == "--bind") bind = value;
    else if (flag == "--key-file") key_file = value;
    else if (flag == "--port") port = std::strtol(value.c_str(), &end, 10);
    else if (flag == "--key-id") key_id = std::strtol(value.c_str(), &end, 10);
    else usage("unknown option " + flag);
    if (end && (*end != '\0' || value.empty())) usage("not a number: " + value);
  }
  if (key_file.empty() || key_id < 0 || key_id > 0xffffffffL || port < 0 || port > 65535)
    usage("--key-file, --key-id and a --port of 0..65535 are required");

  // Every thread inherits this mask; the main thread waits for the signals.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  try {
    // Serve exactly the requested key, so other keys of the file stay unknown.
    const auto file = KeyRing::load_file(key_file);
    const auto *key = file.find(static_cast<std::uint32_t>(key_id));
    if (!key) usage("the key file has no key " + std::to_string(key_id));
    KeyRing keys;
    keys.add(static_cast<std::uint32_t>(key_id), std::string(key->begin(), key->end()));

    ServerOptions options;
    options.bind_address = bind;
    options.port = static_cast<std::uint16_t>(port);
    Server server(options, std::move(keys));

    std::atomic<std::uint64_t> counter{0};
    std::mutex sleepers_mutex;
    std::vector<std::thread> sleepers;
    server.add_method("test.v1/Echo", [](Request request, Reply reply) {
      reply.complete(Status::Ok, request.body);
    });
    server.add_method("test.v1/Count", [&counter](Request, Reply reply) {
      reply.complete(Status::Ok, std::to_string(++counter));
    });
    server.add_method("test.v1/Sleep", [&](Request request, Reply reply) {
      const auto ms = integer_field(request.body, "ms");
      if (!ms || *ms > max_timeout_ms) {
        reply.error(Status::InvalidArgument, "body must be {\"ms\":N} with N <= 60000");
        return;
      }
      std::lock_guard<std::mutex> lock(sleepers_mutex);
      sleepers.emplace_back([ms = *ms, body = std::move(request.body), reply = std::move(reply)]() mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        reply.complete(Status::Ok, body);
      });
    });
    server.add_method("test.v1/Fail", [](Request request, Reply reply) {
      const auto status = integer_field(request.body, "status");
      if (!status || *status == 0 || *status == 9 || *status > 10) {
        reply.error(Status::InvalidArgument, "body must be {\"status\":N} with N in 1..8 or 10");
        return;
      }
      reply.error(static_cast<Status>(*status), "requested failure", "{\"requested\":" + std::to_string(*status) + "}");
    });
    server.add_method("test.v1/Big", [](Request, Reply reply) {
      reply.complete(Status::Ok, std::string(2000, 'x'));
    });
    server.start();

    std::cout << "READY " << server.port() << ' ' << server.instance_hex() << std::endl;

    int received = 0;
    sigwait(&signals, &received);
    server.shutdown(std::chrono::milliseconds(500));
    for (auto &sleeper : sleepers) sleeper.join();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "xgc2-xrpc-udp-interop-server: " << error.what() << '\n';
    return 1;
  }
}
