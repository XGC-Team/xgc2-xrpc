// A MethodRouter served on a real udp.v1 server and called by the udp.v1 client
// over loopback.
#include "xgc2/xrpc/method_udp.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
using namespace xgc2::xrpc;
using namespace std::chrono;

namespace {
constexpr std::uint32_t key_id = 7;

udp::KeyRing ring() {
  udp::KeyRing keys;
  keys.add(key_id, std::string(udp::key_bytes, 'k'));
  return keys;
}
udp::Response call(const udp::Server &server, const udp::Client &client, const std::string &method,
                   const std::string &body, milliseconds budget = seconds(2)) {
  return client.call("127.0.0.1:" + std::to_string(server.port()), key_id, method, body, steady_clock::now() + budget);
}
bool is_hex32(const std::string &text) {
  if (text.size() != 32) return false;
  for (const char c : text)
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  return true;
}

// Everything the handlers need that must outlive the server's threads.
struct Fixture {
  std::atomic<int> executed{0};
  std::atomic<int> second_completions{0};
  std::atomic<bool> dropped_without_answer{false};
  std::mutex mutex;
  std::vector<std::thread> workers;
  ~Fixture() {
    for (auto &worker : workers) worker.join();
  }
};

MethodRouter make_router(Fixture &fixture) {
  MethodRouter router;
  router.add("xgc2.test/Echo", [](MethodRequest request, MethodReply reply) { reply.complete(request.body); });
  router.add("xgc2.test/Identity", [](MethodRequest request, MethodReply reply) {
    reply.complete("{\"method\":\"" + request.method + "\",\"request_id\":\"" + request.request_id + "\",\"remaining_ms\":" +
                   std::to_string(duration_cast<milliseconds>(request.deadline - steady_clock::now()).count()) + "}");
  });
  router.add("xgc2.test/Count", [&fixture](MethodRequest, MethodReply reply) { reply.complete(std::to_string(++fixture.executed)); });
  for (int code = 1; code <= 10; ++code)
    router.add("xgc2.test/Fail" + std::to_string(code), [code](MethodRequest, MethodReply reply) {
      reply.error(static_cast<MethodCode>(code), "requested failure", "{\"requested\":" + std::to_string(code) + "}");
    });
  router.add("xgc2.test/Throw", [](MethodRequest, MethodReply) { throw std::runtime_error("handler bug"); });
  router.add("xgc2.test/Silent", [&fixture](MethodRequest, MethodReply reply) {
    (void)reply;
    fixture.dropped_without_answer = true;
  });
  router.add("xgc2.test/Async", [&fixture](MethodRequest request, MethodReply reply) {
    std::lock_guard<std::mutex> lock(fixture.mutex);
    // The reply is completed from another thread, after the handler returned.
    fixture.workers.emplace_back([body = std::move(request.body), reply = std::move(reply)] {
      std::this_thread::sleep_for(milliseconds(60));
      reply.complete(body);
    });
  });
  router.add("xgc2.test/Twice", [&fixture](MethodRequest, MethodReply reply) {
    const MethodReply copy = reply;
    const bool first = reply.complete("{\"first\":true}");
    if (!first || copy.complete("{\"second\":true}") || reply.error(MethodCode::Internal, "late")) ++fixture.second_completions;
  });
  router.add("xgc2.test/Cancelled", [](MethodRequest, MethodReply reply) {
    reply.complete(reply.cancelled() ? "true" : "false"); // a datagram has no connection to lose
  });
  return router;
}

void methods_are_served_under_their_own_names() {
  Fixture fixture;
  udp::ServerOptions options;
  options.bind_address = "127.0.0.1";
  udp::Server server(options, ring());
  {
    // The server copies the handlers: the router may go away.
    MethodRouter router = make_router(fixture);
    add_methods(server, router);
  }
  server.start();
  const udp::Client client(ring());

  auto response = call(server, client, "xgc2.test/Echo", "{\"robot\":\"scout-1\"}");
  assert(response.delivery == udp::Delivery::ResponseReceived && response.status == udp::Status::Ok);
  assert(response.body == "{\"robot\":\"scout-1\"}" && response.instance == server.instance());
  // An empty result is the JSON null.
  response = call(server, client, "xgc2.test/Echo", "");
  assert(response.status == udp::Status::Ok && response.body == "null");

  // The handler sees the name, a 32-digit request id and the server's deadline.
  response = call(server, client, "xgc2.test/Identity", "{}");
  assert(response.status == udp::Status::Ok);
  const auto marker = response.body.find("\"request_id\":\"");
  assert(response.body.find("{\"method\":\"xgc2.test/Identity\"") == 0 && marker != std::string::npos);
  assert(is_hex32(response.body.substr(marker + 14, 32)));
  const auto remaining = std::stoi(response.body.substr(response.body.find("\"remaining_ms\":") + 15));
  assert(remaining > 0 && remaining <= 2000);

  // Every code arrives as its udp.v1 status with the details; "unauthenticated"
  // has no wire form and is sent as permission_denied.
  for (int code = 1; code <= 10; ++code) {
    response = call(server, client, "xgc2.test/Fail" + std::to_string(code), "{}");
    const int expected = code == 9 ? 10 : code;
    assert(response.delivery == udp::Delivery::ResponseReceived && static_cast<int>(response.status) == expected);
    assert(response.body == method_error_body(static_cast<MethodCode>(expected), "requested failure",
                                              "{\"requested\":" + std::to_string(code) + "}"));
  }

  // A handler that completes later from another thread.
  const auto before = steady_clock::now();
  response = call(server, client, "xgc2.test/Async", "{\"later\":1}");
  assert(response.status == udp::Status::Ok && response.body == "{\"later\":1}" && steady_clock::now() - before >= milliseconds(50));

  // Copies of a reply share the call.
  response = call(server, client, "xgc2.test/Twice", "{}");
  assert(response.status == udp::Status::Ok && response.body == "{\"first\":true}");
  assert(fixture.second_completions == 0);
  assert(call(server, client, "xgc2.test/Cancelled", "{}").body == "false");

  // A handler that throws is answered internal and the server keeps serving.
  response = call(server, client, "xgc2.test/Throw", "{}");
  assert(response.delivery == udp::Delivery::ResponseReceived && response.status == udp::Status::Internal);
  assert(response.body == method_error_body(MethodCode::Internal, "handler failed"));

  // A handler that drops its reply sends nothing: an unknown outcome.
  response = call(server, client, "xgc2.test/Silent", "{}", milliseconds(300));
  assert(response.delivery == udp::Delivery::OutcomeUnknown && fixture.dropped_without_answer);

  // The server's own answers are unchanged: an unknown name is not_found.
  response = call(server, client, "xgc2.test/Absent", "{}");
  assert(response.delivery == udp::Delivery::ResponseReceived && response.status == udp::Status::NotFound);
  assert(call(server, client, "xgc2.test/Echo", "{}").status == udp::Status::Ok);
  assert(server.shutdown());
}

void retransmissions_run_a_method_once() {
  // The reply cache of the transport applies to routed methods like any other.
  Fixture fixture;
  udp::ServerOptions options;
  options.bind_address = "127.0.0.1";
  udp::Server server(options, ring());
  MethodRouter router = make_router(fixture);
  add_methods(server, router);
  server.start();
  // A client that retransmits quickly while the handler is slow.
  udp::ClientOptions fast;
  fast.backoff = {milliseconds(5), milliseconds(5), milliseconds(5)};
  fast.steady_interval = milliseconds(5);
  const udp::Client client(ring(), fast);
  const auto response = call(server, client, "xgc2.test/Async", "{\"once\":true}");
  assert(response.status == udp::Status::Ok && response.attempts > 1);
  assert(server.stats().inflight_ignored >= 1);
  assert(call(server, client, "xgc2.test/Count", "{}").body == "1");
  assert(call(server, client, "xgc2.test/Count", "{}").body == "2");
  assert(server.shutdown());
}

void error_bodies_equal_the_transports() {
  // method_error_body is a header-only copy of the library's udp::error_body: the
  // bytes must be identical, escaping and replacement of invalid UTF-8 included.
  const std::string messages[] = {"", "plain", "quote \" backslash \\ tab \t newline \n cr \r bell \x07 del \x7f", "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x99\x82",
                                  "bad \xff\xfe \xc0\xaf \xed\xa0\x80 \xf4\x90\x80\x80 \xe2\x82 end", std::string(300, 'm')};
  const std::string details[] = {"{}", "{\"a\":1}", "  {\"nested\":{\"x\":[1,2]}}\n"};
  for (int code = 1; code <= 10; ++code)
    for (const auto &message : messages)
      for (const auto &detail : details)
        assert(method_error_body(static_cast<MethodCode>(code), message, detail) ==
               udp::error_body(static_cast<udp::Status>(code), message, detail));
}
} // namespace

int main() {
  methods_are_served_under_their_own_names();
  retransmissions_run_a_method_once();
  error_bodies_equal_the_transports();
  std::cout << "PASS routed methods over udp.v1: names, statuses, async completion, shared replies, retransmission and error bodies\n";
}
