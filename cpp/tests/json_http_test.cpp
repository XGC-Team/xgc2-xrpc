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
    else reply.complete(*request.body, 201);
  }, {128, 4}, limits.response_bytes);
  // Routing is ordinary domain function composition. No SDK host mode,
  // listener, worker or additional queue is needed by the adapter.
  HttpServer host(UnixOptions{path}, [&](HttpRequest request, HttpReply reply) {
    if (request.target == "/invalid-response") {
      HttpResponse response;
      response.body = "not json";
      response.headers.emplace_back("Content-Type", "application/json");
      reply.complete(std::move(response));
    } else echo(std::move(request), std::move(reply));
  }, limits, HttpIdentity{instance, {}});
  std::thread io([&] { host.run(); });
  JsonHttpClient client(path, limits, instance);
  JsonHttpRequest request;
  request.method = "POST";
  request.target = "/echo";
  request.body = Json{{"n", UINT64_MAX}, {"s", "hello"}};
  for (int i = 0; i < 200; ++i) {
    const auto response = client.call(request, Clock::now()+seconds(2));
    assert(response.status == 201 && response.body == request.body);
  }
  assert(host.stats().accepted_connections == 1);
  request.body = nullptr;
  assert(client.call(request, Clock::now()+seconds(2)).body->is_null());
  request.target = "/absent";
  request.body.reset();
  assert(*client.call(request, Clock::now()+seconds(2)).body == true);
  request.target = "/large";
  assert(client.call(request, Clock::now()+seconds(2)).status == 503);
  const auto count = calls.load();
  HttpClient raw(path, limits, instance);
  HttpRequest invalid;
  invalid.method = "POST";
  invalid.target = "/echo";
  invalid.body = "{}";
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 415);
  invalid.headers.emplace_back("Content-Type", "application/json");
  invalid.body = "{\"a\":1,\"a\":2}";
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 400);
  invalid.body = "[[[[[1]]]]]";
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 400);
  invalid.body = std::string(129, ' ');
  assert(raw.call(invalid, Clock::now()+seconds(2)).status == 413);
  assert(calls.load() == count);
  request.body = std::string(1000, 'x');
  try { client.call(request, Clock::now()+seconds(2)); assert(false); }
  catch (const HttpCallError &e) { assert(e.delivery == Delivery::NotSent); }
  request.body.reset();
  request.target = "/invalid-response";
  try { client.call(request, Clock::now()+seconds(2)); assert(false); }
  catch (const HttpCallError &e) {
    assert(e.code == "invalid_response" && e.delivery == Delivery::OutcomeUnknown);
  }
  client.close();
  raw.close();
  host.request_stop();
  io.join();
}
int main() {
  syntax();
  protocol();
  std::cout << "PASS strict bounded JSON, typed HTTP calls, connection reuse, protocol rejection and delivery outcomes\n";
}
