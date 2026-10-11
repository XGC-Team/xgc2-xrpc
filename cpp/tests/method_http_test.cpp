// A MethodRouter served on a real http.v1 host over a Unix socket and called by
// the HTTP client: POST /v1/call/<service>/<Method>.
#include "xgc2/xrpc/method_http.hpp"
#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>
using namespace xgc2::xrpc;
using namespace std::chrono;

namespace {
struct Directory {
  std::string path;
  Directory() {
    char pattern[] = "/tmp/xrpc-method-XXXXXX";
    auto created = ::mkdtemp(pattern);
    assert(created);
    path = created;
  }
  ~Directory() { std::filesystem::remove_all(path); }
};

HttpRequest post(const std::string &name, std::string body = {}, const std::string &content_type = "application/json") {
  HttpRequest request;
  request.method = "POST";
  request.target = std::string(method_path_prefix) + name;
  request.body = std::move(body);
  if (!request.body.empty() && !content_type.empty()) request.headers.emplace_back("Content-Type", content_type);
  return request;
}
std::string header(const HttpResponse &response, const std::string &name) {
  for (const auto &[key, value] : response.headers)
    if (key == name) return value;
  return {};
}
std::string envelope(MethodCode code, const std::string &message, const std::string &details = "{}") {
  return "{\"error\":" + method_error_body(code, message, details) + "}";
}

struct Fixture {
  std::atomic<int> handler_runs{0};
  std::atomic<int> second_completions{0};
  std::atomic<int> domain_routes{0};
  std::mutex mutex;
  std::vector<std::thread> workers;
  ~Fixture() {
    for (auto &worker : workers) worker.join();
  }
};

MethodRouter make_router(Fixture &fixture) {
  MethodRouter router;
  router.add("xgc2.test/Echo", [&fixture](MethodRequest request, MethodReply reply) {
    ++fixture.handler_runs;
    reply.complete(request.body);
  });
  router.add("xgc2.test/Identity", [&fixture](MethodRequest request, MethodReply reply) {
    ++fixture.handler_runs;
    reply.complete("{\"method\":\"" + request.method + "\",\"request_id\":\"" + request.request_id + "\",\"remaining_ms\":" +
                   std::to_string(duration_cast<milliseconds>(request.deadline - steady_clock::now()).count()) + "}");
  });
  for (int code = 1; code <= 10; ++code)
    router.add("xgc2.test/Fail" + std::to_string(code), [code](MethodRequest, MethodReply reply) {
      reply.error(static_cast<MethodCode>(code), "requested failure", "{\"requested\":" + std::to_string(code) + "}");
    });
  router.add("xgc2.test/Throw", [](MethodRequest, MethodReply) { throw std::runtime_error("handler bug"); });
  router.add("xgc2.test/Async", [&fixture](MethodRequest request, MethodReply reply) {
    std::lock_guard<std::mutex> lock(fixture.mutex);
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
  return router;
}
} // namespace

int main() {
  Directory directory;
  const auto path = directory.path + "/service.sock";
  const auto instance = new_instance_id();
  Fixture fixture;
  const MethodRouter router = make_router(fixture);
  HttpLimits limits;
  limits.request_bytes = 4096;
  limits.response_bytes = 4096;
  // The domain keeps its own routes beside the method calls.
  std::mutex target_mutex;
  std::string last_target;
  HttpServer host(UnixOptions{path}, [&](HttpRequest request, HttpReply reply) {
    if (handle_method_call(router, request, reply)) return;
    ++fixture.domain_routes;
    {
      std::lock_guard<std::mutex> lock(target_mutex);
      last_target = request.target;
    }
    HttpResponse response;
    response.headers.emplace_back("Content-Type", "application/json");
    response.body = describe_json("method-test", "v1", instance, true);
    reply.complete(std::move(response));
  }, limits, HttpIdentity{instance, {"/v1/describe"}});
  std::thread io([&] { host.run(); });
  HttpClient client(path, limits, instance);
  const auto deadline = [] { return steady_clock::now() + seconds(3); };

  // A call: 200, the JSON the handler produced, as application/json.
  auto response = client.call(post("xgc2.test/Echo", "{\"robot\":\"scout-1\"}"), deadline());
  assert(response.status == 200 && response.body == "{\"robot\":\"scout-1\"}");
  assert(header(response, "Content-Type") == "application/json");
  // No body needs no content type, and an empty result is the JSON null.
  response = client.call(post("xgc2.test/Echo"), deadline());
  assert(response.status == 200 && response.body == "null");
  for (const char *type : {"application/json; charset=utf-8", "Application/JSON", "application/json ;x=y"}) {
    response = client.call(post("xgc2.test/Echo", "[1]", type), deadline());
    assert(response.status == 200 && response.body == "[1]");
  }

  // The handler sees the name, the caller's request id and the host's deadline.
  auto identity = post("xgc2.test/Identity", "{}");
  identity.request_id = "caller:7";
  response = client.call(identity, deadline());
  assert(response.status == 200 && response.body.find("{\"method\":\"xgc2.test/Identity\",\"request_id\":\"caller:7\"") == 0);
  const auto remaining = std::stoi(response.body.substr(response.body.find("\"remaining_ms\":") + 15));
  assert(remaining > 0 && remaining <= 30000);

  // Every code is the status of its HTTP mapping and the XRPC error envelope.
  for (int code = 1; code <= 10; ++code) {
    response = client.call(post("xgc2.test/Fail" + std::to_string(code), "{}"), deadline());
    const auto expected = static_cast<MethodCode>(code);
    assert(response.status == method_code_http_status(expected));
    assert(response.body == envelope(expected, "requested failure", "{\"requested\":" + std::to_string(code) + "}"));
    assert(header(response, "Content-Type") == "application/json");
  }

  // Refusals before any handler runs.
  const int runs = fixture.handler_runs;
  response = client.call(post("xgc2.test/Absent", "{}"), deadline());
  assert(response.status == 404 && response.body == envelope(MethodCode::NotFound, "no such method"));
  for (const char *target : {"/v1/call/", "/v1/call/Echo", "/v1/call/xgc2.test/", "/v1/call/xgc2.test/Echo/more", "/v1/call/xgc2.test/Ec-ho",
                             "/v1/call/xgc2.test/Echo?x=1", "/v1/call/xgc2.test//Echo"}) {
    auto bad = post("xgc2.test/Echo", "{}");
    bad.target = target;
    response = client.call(bad, deadline());
    assert(response.status == 400 && response.body.find("\"code\":\"invalid_argument\"") != std::string::npos);
  }
  auto get = post("xgc2.test/Echo");
  get.method = "GET";
  response = client.call(get, deadline());
  assert(response.status == 405 && header(response, "Allow") == "POST");
  assert(response.body == envelope(MethodCode::InvalidArgument, "method calls are POST"));
  get.target = "/v1/call/xgc2.test/Absent";
  assert(client.call(get, deadline()).status == 404);
  response = client.call(post("xgc2.test/Echo", "{}", "text/plain"), deadline());
  assert(response.status == 415 && response.body == envelope(MethodCode::InvalidArgument, "application/json required"));
  response = client.call(post("xgc2.test/Echo", "{}", ""), deadline());
  assert(response.status == 415);
  assert(fixture.handler_runs == runs);

  // The fence of the host precedes routing: a call that does not name the
  // instance never reaches a handler.
  HttpClient unpinned(path, limits);
  response = unpinned.call(post("xgc2.test/Echo", "{}"), deadline());
  assert(response.status == 409 && fixture.handler_runs == runs);
  // Discovery is chosen by method and path: the first describe of Core carries a
  // wait_ready_ms query and no instance, and the handler sees the whole target.
  HttpRequest describe_wait;
  describe_wait.method = "GET";
  describe_wait.target = "/v1/describe?wait_ready_ms=250";
  response = unpinned.call(describe_wait, deadline());
  assert(response.status == 200 && response.body == describe_json("method-test", "v1", instance, true));
  {
    std::lock_guard<std::mutex> lock(target_mutex);
    assert(last_target == "/v1/describe?wait_ready_ms=250");
  }
  // A query makes neither another route nor a longer path a discovery route.
  for (const char *target : {"/v1/call/xgc2.test/Echo?wait_ready_ms=250", "/v1/describe/more?wait_ready_ms=250", "/v1/other?/v1/describe"}) {
    describe_wait.target = target;
    assert(unpinned.call(describe_wait, deadline()).status == 409);
  }
  assert(fixture.handler_runs == runs);
  unpinned.close();

  // A handler that throws is answered internal.
  response = client.call(post("xgc2.test/Throw", "{}"), deadline());
  assert(response.status == 500 && response.body == envelope(MethodCode::Internal, "handler failed"));
  // A handler that completes later from another thread; the first completion of the shared reply wins.
  const auto before = steady_clock::now();
  response = client.call(post("xgc2.test/Async", "{\"later\":1}"), deadline());
  assert(response.status == 200 && response.body == "{\"later\":1}" && steady_clock::now() - before >= milliseconds(50));
  response = client.call(post("xgc2.test/Twice", "{}"), deadline());
  assert(response.status == 200 && response.body == "{\"first\":true}" && fixture.second_completions == 0);

  // Other targets stay with the domain: handle_method_call returned false for them.
  const int routes = fixture.domain_routes;
  auto describe = post("x");
  describe.method = "GET";
  describe.target = "/v1/describe";
  response = client.call(describe, deadline());
  assert(response.status == 200 && response.body == describe_json("method-test", "v1", instance, true));
  describe.target = "/v1/callx/xgc2.test/Echo";
  assert(client.call(describe, deadline()).status == 200);
  assert(fixture.domain_routes == routes + 2);
  assert(host.stats().malformed_requests == 0);

  client.close();
  host.request_stop();
  io.join();
  std::cout << "PASS routed methods over http.v1: names, statuses, envelopes, refusals, fence, async completion and domain routes\n";
}
