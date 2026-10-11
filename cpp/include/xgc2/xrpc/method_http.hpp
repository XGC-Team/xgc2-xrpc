#pragma once
// Serves a MethodRouter on an http.v1 host under POST /v1/call/<service>/<Method>.
// Header only; link XgcXrpc::http. The router comes from method.hpp, which the
// udp component installs.
#include "http.hpp"
#include "method.hpp"
#include <cctype>

namespace xgc2::xrpc {
namespace method_detail {
class HttpSink final : public MethodReply::Sink {
public:
  explicit HttpSink(HttpReply reply) : reply_(std::move(reply)) {}
  bool complete(std::string_view json) override {
    HttpResponse response;
    response.headers.emplace_back("Content-Type", "application/json");
    response.body.assign(json);
    return reply_.complete(std::move(response));
  }
  bool error(MethodCode code, std::string_view message, std::string_view details_json) override {
    return reply_.complete(error_response(code, message, details_json));
  }
  bool cancelled() const noexcept override { return reply_.cancelled(); }
  // The XRPC error envelope: {"error":{"code","message","details"}}.
  static HttpResponse error_response(MethodCode code, std::string_view message, std::string_view details_json = "{}") {
    HttpResponse response;
    response.status = method_code_http_status(code);
    response.headers.emplace_back("Content-Type", "application/json");
    response.body = "{\"error\":" + method_error_body(code, message, details_json) + "}";
    return response;
  }

private:
  HttpReply reply_;
};
inline bool equal_ascii(std::string_view a, std::string_view b) noexcept {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i)
    if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) return false;
  return true;
}
// True for application/json, with or without parameters such as charset.
inline bool json_content_type(const Headers &headers) noexcept {
  for (const auto &header : headers) {
    if (!equal_ascii(header.first, "Content-Type")) continue;
    std::string_view type = header.second;
    type = type.substr(0, type.find(';'));
    while (!type.empty() && (type.back() == ' ' || type.back() == '\t')) type.remove_suffix(1);
    while (!type.empty() && (type.front() == ' ' || type.front() == '\t')) type.remove_prefix(1);
    return equal_ascii(type, "application/json");
  }
  return false;
}
} // namespace method_detail

// Handles a request whose target is under /v1/call/ and returns true; any other
// target is left untouched and gets false, so a domain host keeps its own routes:
//
//   HttpServer host(options, [&](HttpRequest request, HttpReply reply) {
//     if (handle_method_call(router, request, reply)) return;
//     /* the domain's other routes */
//   }, limits, identity);
//
// A call is POST /v1/call/<service>/<Method> with a JSON body (Content-Type
// application/json when there is a body); the reply is 200 with the JSON result,
// or the XRPC error envelope with the status of its code. The route takes no
// query. An invalid name is 400 invalid_argument, an unknown method 404
// not_found, another verb 405 and another content type 415 (both
// invalid_argument), before any handler runs. The handler runs on the host's I/O
// thread and must not block it; it completes the MethodReply from any thread.
// request.body is moved into the MethodRequest when a handler is called. The
// host has checked the instance fence, the request id and the deadline already.
inline bool handle_method_call(const MethodRouter &router, HttpRequest &request, const HttpReply &reply) {
  if (request.target.compare(0, method_path_prefix.size(), method_path_prefix) != 0) return false;
  const auto refuse = [&](MethodCode code, const char *message) {
    reply.complete(method_detail::HttpSink::error_response(code, message));
    return true;
  };
  const std::string_view name = std::string_view(request.target).substr(method_path_prefix.size());
  if (name.find_first_of("?#") != std::string_view::npos) return refuse(MethodCode::InvalidArgument, "method calls take no query");
  if (!split_method(name)) return refuse(MethodCode::InvalidArgument, "the path must be /v1/call/<service>/<Method>");
  const MethodRouter::Handler *handler = router.find(name);
  if (!handler) return refuse(MethodCode::NotFound, "no such method");
  if (request.method != "POST") {
    auto response = method_detail::HttpSink::error_response(MethodCode::InvalidArgument, "method calls are POST");
    response.status = 405;
    response.headers.emplace_back("Allow", "POST");
    reply.complete(std::move(response));
    return true;
  }
  if (!request.body.empty() && !method_detail::json_content_type(request.headers)) {
    auto response = method_detail::HttpSink::error_response(MethodCode::InvalidArgument, "application/json required");
    response.status = 415;
    reply.complete(std::move(response));
    return true;
  }
  MethodRequest call{std::string(name), std::move(request.body), request.request_id, request.deadline};
  MethodReply sink(std::make_shared<method_detail::HttpSink>(reply));
  try {
    (*handler)(std::move(call), sink);
  } catch (...) {
    sink.error(MethodCode::Internal, "handler failed");
  }
  return true;
}
} // namespace xgc2::xrpc
