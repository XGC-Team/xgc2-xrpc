#include "xgc2/xrpc/json_http.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
using namespace xgc2::xrpc;
using namespace std::chrono;

void syntax() {
  Json value;
  for (const auto *body : {"{}", "[]", "true", "null", "42", "1.5", "\"text\"", " {\"a\":[1,false]} \n"})
    assert(parse_json(body, value));
  for (const auto *body : {"", "{}garbage", "{} {}", "{}/*x*/", "{/*x*/\"a\":1}",
       "{\"a\":1,\"a\":2}", "{\"a\":{\"x\":1,\"x\":2}}", "{'a':1}", "{\"a\":NaN}",
       "[1,,2]", "{\"a\":1,}", "[1,]", "01", "1e999", "\"\x01\"", "\"\xff\""}) {
    assert(!parse_json(body, value));
    assert(value.is_null());
  }
  assert(parse_json("{\"a\":1,\"b\":{\"a\":2},\"c\":{\"a\":3}}", value));
  assert(parse_json("{}", value, {2, 8}));
  assert(!parse_json("{} ", value, {2, 8}));
  assert(!parse_json("[[[[1]]]]", value, {64, 2}));
  assert(parse_json("{\"n\":18446744073709551615}", value));
  assert(value["n"].is_number_unsigned() && value["n"].get<std::uint64_t>() == UINT64_MAX);
  auto response = json_response(value, 64, 201);
  assert(response.status == 201 && response.body.size() <= 64);
  assert(parse_json(response.body, value));
  value = std::string(10000, 'x');
  for (std::size_t limit : {0, 8, 128}) {
    response = json_response(value, limit);
    assert(response.status == 503 && response.body.size() <= limit);
  }
  value = std::string("\xff");
  assert(json_response(value, 128).status == 500);
}
struct Directory {
  std::string path;
  Directory() {
    char pattern[] = "/tmp/xrpc-json-XXXXXX";
    auto created = ::mkdtemp(pattern);
    assert(created);
    path = created;
  }
  ~Directory() { std::filesystem::remove_all(path); }
};
HttpRequest request_for(const std::string &method, const std::string &target,
                        std::string body = {}) {
  HttpRequest request;
  request.method = method;
  request.target = target;
  request.body = std::move(body);
  if (!request.body.empty())
    request.headers.emplace_back("Content-Type", "application/json");
  return request;
}
Json json_body(const HttpResponse &response) {
  Json value;
  bool content_type = false;
  for (const auto &[name, field] : response.headers)
    content_type = content_type || (name == "Content-Type" && field == "application/json");
  assert(content_type && parse_json(response.body, value));
  return value;
}
void protocol() {
  Directory directory;
  const auto path = directory.path + "/service.sock";
  const auto instance = new_instance_id();
  std::atomic<unsigned> calls{0};
  HttpLimits limits;
  limits.request_bytes = 512;
  limits.response_bytes = 512;
  JsonHttpHandler echo([&](JsonHttpRequest request, JsonHttpReply reply) {
    ++calls;
    assert(!request.request_id.empty() && request.deadline > Clock::now());
    if (request.target == "/large") reply.complete(std::string(10000, 'x'));
    else if (request.target == "/absent") reply.complete(!request.body);
    else if (request.target == "/failure") reply.error(409, "conflict", "domain refusal");
    else reply.complete(*request.body, 201);
  }, {128, 4}, limits.response_bytes);
  // Routing is ordinary domain function composition. No SDK host mode,
  // listener, worker or additional queue is needed by the adapter.
  HttpServer host(UnixOptions{path}, [&](HttpRequest request, HttpReply reply) {
    echo(std::move(request), std::move(reply));
  }, limits, HttpIdentity{instance, {}});
  std::thread io([&] { host.run(); });
  HttpClient client(path, limits, instance);
  const Json sent = Json{{"n", UINT64_MAX}, {"s", "hello"}};
  for (int i = 0; i < 200; ++i) {
    const auto response = client.call(request_for("POST", "/echo", sent.dump()), Clock::now()+seconds(2));
    assert(response.status == 201 && json_body(response) == sent);
  }
  assert(host.stats().accepted_connections == 1);
  assert(json_body(client.call(request_for("POST", "/echo", "null"), Clock::now()+seconds(2))).is_null());
  assert(json_body(client.call(request_for("GET", "/absent"), Clock::now()+seconds(2))) == true);
  assert(client.call(request_for("POST", "/large", "{}"), Clock::now()+seconds(2)).status == 503);
  const auto refusal = client.call(request_for("POST", "/failure", "{}"), Clock::now()+seconds(2));
  assert(refusal.status == 409);
  const auto error = json_body(refusal);
  assert(error["error"]["code"] == "conflict" && error["error"]["message"] == "domain refusal");
  const auto count = calls.load();
  HttpClient raw(path, limits, instance);
  HttpRequest invalid = request_for("POST", "/echo", "{}");
  invalid.headers.clear();
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 415);
  invalid.headers.emplace_back("Content-Type", "application/json");
  invalid.body = "{\"a\":1,\"a\":2}";
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 400);
  invalid.body = "[[[[[1]]]]]";
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 400);
  invalid.body = std::string(129, ' ');
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 413);
  assert(calls.load() == count);
  client.close();
  raw.close();
  host.request_stop();
  io.join();
}
int main() {
  syntax();
  protocol();
  std::cout << "PASS strict bounded JSON, JSON-HTTP handler adapter, connection reuse and protocol rejection\n";
}
