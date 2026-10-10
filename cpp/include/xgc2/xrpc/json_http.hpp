#pragma once
#include "http.hpp"
#include <nlohmann/json.hpp>
#include <optional>
#include <string_view>

namespace xgc2::xrpc {
using Json = nlohmann::json;
struct JsonLimits {
  std::size_t max_bytes = 65536;
  unsigned max_depth = 32;
};
// Strict JSON syntax, unique object keys, bounded bytes and nesting. On
// rejection value is null. Allocation failures propagate to the host.
bool parse_json(std::string_view body, Json &value, JsonLimits limits = {});
// Streams directly into bounded storage. Overflow returns resource_exhausted;
// invalid output returns invalid_response. Error bodies obey the same bound.
HttpResponse json_response(const Json&, std::size_t max_bytes, int status = 200);

struct JsonHttpRequest {
  std::string method, target, request_id;
  Headers headers;
  std::optional<Json> body; // no body differs from a JSON null body
  Clock::time_point deadline{}; // server-supplied; client call's deadline wins
};
struct JsonHttpResponse {
  int status = 200;
  Headers headers;
  std::optional<Json> body;
};
// Copies retain the underlying admission/endpoint lease until actual work
// ends, including after cancellation. Completion is safe from any thread.
class JsonHttpReply {
public:
  JsonHttpReply() = default;
  bool complete(const Json&, int status = 200) const;
  bool error(int status, const std::string &code, const std::string &message) const;
  bool cancelled() const noexcept { return reply_.cancelled(); }
private:
  friend class JsonHttpHandler;
  JsonHttpReply(HttpReply reply, std::size_t bound)
    : reply_(std::move(reply)), bound_(bound) {}
  HttpReply reply_;
  std::size_t bound_ = 0;
};
// A callable protocol adapter. It owns no endpoint, thread or routing table.
// Supply it to HttpServer, or dispatch to it from the domain's existing host.
// Body parsing occurs once before the domain handler receives the request.
class JsonHttpHandler {
public:
  using Handler = std::function<void(JsonHttpRequest, JsonHttpReply)>;
  JsonHttpHandler(Handler, JsonLimits request_limits = {},
                  std::size_t response_bytes = 65536);
  void operator()(HttpRequest, HttpReply) const;
private:
  Handler handler_;
  JsonLimits request_limits_;
  std::size_t response_bytes_;
};
// Reuses HttpClient's connection, identity fence, deadline and cancellation.
// No retries or replay. Invalid response JSON has an unknown delivery outcome.
class JsonHttpClient {
public:
  explicit JsonHttpClient(std::string socket_path, HttpLimits limits = {},
                          std::string instance_id = {}, unsigned max_depth = 32);
  JsonHttpResponse call(JsonHttpRequest, Clock::time_point deadline,
                         std::stop_token cancellation = {});
  void close() noexcept { client_.close(); }
private:
  HttpLimits limits_;
  unsigned max_depth_;
  HttpClient client_;
};
} // namespace xgc2::xrpc
