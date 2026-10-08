#include "xgc2/xrpc/http.hpp"
#include <boost/asio.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
namespace net = boost::asio;
namespace http = boost::beast::http;
using Local = net::local::stream_protocol;
using namespace std::chrono;

struct Directory {
  std::string path;
  Directory() {
    char pattern[] = "/tmp/xrpc-wire-XXXXXX";
    const auto value = ::mkdtemp(pattern);
    assert(value);
    path = value;
  }
  ~Directory() { std::filesystem::remove_all(path); }
};

http::response<http::string_body> exchange(
    const std::string &path, const std::string &method, const std::string &target,
    const Headers &headers, const std::string &body = {}) {
  net::io_context io;
  Local::socket socket(io);
  socket.connect(Local::endpoint(path));
  timeval timeout{2, 0};
  assert(::setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO,
                      &timeout, sizeof(timeout)) == 0);
  http::request<http::string_body> request;
  request.version(11);
  request.method_string(method);
  request.target(target);
  request.set(http::field::host, "localhost");
  request.set(http::field::connection, "close");
  for (const auto &[name, value] : headers)
    request.insert(name, value); // preserve duplicate and empty fixture fields
  request.body() = body;
  if (!body.empty())
    request.prepare_payload();
  http::write(socket, request);
  boost::beast::flat_buffer buffer;
  http::response_parser<http::string_body> parser;
  if (method == "HEAD")
    parser.skip(true);
  http::read(socket, buffer, parser);
  return parser.release();
}

int main() {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/wire.sock";
  std::atomic<std::size_t> dispatches{0};
  HttpLimits limits;
  limits.request_timeout = seconds(1);
  limits.request_bytes = 256;
  HttpServer server(options, [&](HttpRequest, HttpReply reply) {
    ++dispatches;
    HttpResponse response;
    response.body = "{}";
    reply.complete(std::move(response));
  }, limits, {"fixture:boot-1", {"/v1/describe"}});
  std::thread owner([&] { server.run(); });
  boost::property_tree::ptree fixture;
  boost::property_tree::read_json(XRPC_WIRE_FIXTURE, fixture);
  std::size_t cases = 0;
  for (const auto &entry : fixture.get_child("cases")) {
    const auto &test = entry.second;
    Headers headers;
    for (const auto &pair : test.get_child("headers")) {
      auto value = pair.second.begin();
      const auto name = value++->second.get_value<std::string>();
      headers.emplace_back(name, value->second.get_value<std::string>());
    }
    const auto before = dispatches.load();
    const auto response = exchange(options.path, test.get<std::string>("method"),
                                  test.get<std::string>("path"), headers);
    const auto expected = test.get<unsigned>("status");
    const auto invoked = test.get<bool>("dispatch") ? 1u : 0u;
    if (response.result_int() != expected || dispatches.load() != before + invoked) {
      std::cerr << test.get<std::string>("name") << ": expected " << expected
                << " observed " << response.result_int() << "\n";
      std::abort();
    }
    if (test.get<std::string>("method") == "HEAD")
      assert(response.body().empty());
    ++cases;
  }
  // Oversized Content-Length is rejected by Beast before metadata dispatch.
  // A normal bound SDK client must receive the correlated 413, not a fence
  // error, and the domain handler must remain untouched.
  const auto before_oversize = dispatches.load();
  HttpClient client(options.path, HttpLimits{}, "fixture:boot-1");
  HttpRequest oversized;
  oversized.method = "POST";
  oversized.target = "/v1/echo";
  oversized.request_id = "oversize-proof";
  oversized.body.assign(257, 'x');
  const auto rejected = client.call(std::move(oversized), Clock::now() + seconds(1));
  assert(rejected.status == 413 && !rejected.keep_alive);
  assert(dispatches.load() == before_oversize);
  assert(std::find(rejected.headers.begin(), rejected.headers.end(),
                   std::pair<std::string, std::string>{"X-Request-ID", "oversize-proof"})
         != rejected.headers.end());
  // Rejecting metadata before the body must close, never reinterpret the
  // rejected body's bytes as another invocation on this connection.
  net::io_context io;
  Local::socket socket(io);
  socket.connect(Local::endpoint(options.path));
  const std::string raw =
      "POST /v1/echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 123\r\n"
      "X-Xrpc-Timeout-Ms: 01\r\nX-Request-ID: bad\r\n"
      "X-Xrpc-Instance-ID: fixture:boot-1\r\n\r\n"
      "GET /v1/echo HTTP/1.1\r\nHost: localhost\r\nX-Xrpc-Timeout-Ms: 1000\r\n"
      "X-Request-ID: hidden\r\nX-Xrpc-Instance-ID: fixture:boot-1\r\n\r\n";
  const auto before = dispatches.load();
  net::write(socket, net::buffer(raw));
  boost::beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(socket, buffer, response);
  assert(response.result_int() == 400 && !response.keep_alive());
  assert(dispatches.load() == before);
  server.request_stop();
  owner.join();
  std::cout << "shared wire cases: " << cases
            << "; rejected-body framing closed without dispatch\n";
}
