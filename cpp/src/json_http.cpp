#include "xgc2/xrpc/json_http.hpp"
#include "xgc2/xrpc/bounded_output.hpp"
#include <set>

namespace xgc2::xrpc {
namespace {
struct RejectedJson {};
HttpResponse bounded_error(int status, const std::string &code,
                            const std::string &message, std::size_t bound) {
  auto response = http_error(status, code, message);
  if (response.body.size() > bound) response.body.clear();
  return response;
}
bool equal_ascii(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    const auto c = static_cast<unsigned char>(a[i]);
    if ((c >= 'A' && c <= 'Z' ? c + ('a'-'A') : c) != b[i]) return false;
  }
  return true;
}
bool json_content_type(const Headers &headers) {
  unsigned count = 0;
  for (const auto &[name, value] : headers) {
    if (!equal_ascii(name, "content-type")) continue;
    ++count;
    // The profile emits this exact media type. Parameters remain valid HTTP.
    auto type = std::string_view(value).substr(0, value.find(';'));
    while (!type.empty() && (type.front() == ' ' || type.front() == '\t')) type.remove_prefix(1);
    while (!type.empty() && (type.back() == ' ' || type.back() == '\t')) type.remove_suffix(1);
    if (!equal_ascii(type, "application/json")) return false;
  }
  return count == 1;
}
}
bool parse_json(std::string_view body, Json &value, JsonLimits limits) {
  value = nullptr;
  if (body.empty() || body.size() > limits.max_bytes || !limits.max_depth) return false;
  // The library performs syntax/UTF-8/number validation in one parse. Its
  // callback adds the profile's depth and duplicate-key rules, and aborts
  // immediately rather than parsing an over-depth subtree and discarding it.
  std::vector<std::set<std::string>> objects;
  const auto callback = [&](int depth, Json::parse_event_t event, Json &parsed) {
    if ((event == Json::parse_event_t::object_start || event == Json::parse_event_t::array_start)
        && static_cast<unsigned>(depth) >= limits.max_depth) throw RejectedJson{};
    if (event == Json::parse_event_t::object_start) objects.emplace_back();
    else if (event == Json::parse_event_t::object_end) objects.pop_back();
    else if (event == Json::parse_event_t::key &&
             !objects.back().insert(parsed.get_ref<const std::string&>()).second)
      throw RejectedJson{};
    return true;
  };
  try {
    auto parsed = Json::parse(body.begin(), body.end(), callback, false);
    if (parsed.is_discarded()) return false;
    value = std::move(parsed);
    return true;
  } catch (const RejectedJson &) { return false; }
}
HttpResponse json_response(const Json &value, std::size_t max_bytes, int status) {
  BoundedOutput output(max_bytes);
  output.stream().exceptions(std::ios::badbit | std::ios::failbit);
  try {
    output.stream() << value;
  } catch (const std::ios_base::failure &) {
    return bounded_error(503, "resource_exhausted", "JSON response limit exceeded", max_bytes);
  } catch (const Json::exception &) {
    return bounded_error(500, "invalid_response", "response cannot be encoded as JSON", max_bytes);
  }
  HttpResponse response;
  response.status = status;
  response.headers.emplace_back("Content-Type", "application/json");
  response.body = std::move(output).take();
  return response;
}
bool JsonHttpReply::complete(const Json &body, int status) const {
  return reply_.complete(json_response(body, bound_, status));
}
bool JsonHttpReply::error(int status, const std::string &code, const std::string &message) const {
  return reply_.complete(bounded_error(status, code, message, bound_));
}
JsonHttpHandler::JsonHttpHandler(Handler handler, JsonLimits limits, std::size_t response_bytes)
  : handler_(std::move(handler)), request_limits_(limits), response_bytes_(response_bytes) {
  if (!handler_ || !limits.max_bytes || !limits.max_depth || !response_bytes)
    throw std::invalid_argument("JSON-HTTP requires handler and finite nonzero limits");
}
void JsonHttpHandler::operator()(HttpRequest request, HttpReply raw_reply) const {
  JsonHttpReply reply(std::move(raw_reply), response_bytes_);
  JsonHttpRequest typed;
  if (request.body.size() > request_limits_.max_bytes) {
    reply.error(413, "resource_exhausted", "JSON request limit exceeded");
    return;
  }
  if (!request.body.empty()) {
    if (!json_content_type(request.headers)) {
      reply.error(415, "invalid_argument", "application/json required");
      return;
    }
    Json parsed;
    if (!parse_json(request.body, parsed, request_limits_)) {
      reply.error(400, "invalid_argument", "invalid JSON request");
      return;
    }
    typed.body = std::move(parsed);
  }
  typed.method = std::move(request.method);
  typed.target = std::move(request.target);
  typed.request_id = std::move(request.request_id);
  typed.headers = std::move(request.headers);
  typed.deadline = request.deadline;
  handler_(std::move(typed), std::move(reply));
}
JsonHttpClient::JsonHttpClient(std::string path, HttpLimits limits, std::string instance, unsigned depth)
  : limits_(limits), max_depth_(depth), client_(std::move(path), limits, std::move(instance)) {
  if (!depth) throw std::invalid_argument("JSON nesting limit must be nonzero");
}
JsonHttpResponse JsonHttpClient::call(JsonHttpRequest request, Clock::time_point deadline,
                                     std::stop_token cancellation) {
  HttpRequest raw;
  if (request.body) {
    auto encoded = json_response(*request.body, limits_.request_bytes);
    if (encoded.status != 200)
      throw HttpCallError("invalid_argument", Delivery::NotSent, "request cannot be encoded within JSON limit");
    raw.body = std::move(encoded.body);
    // Content-Type is a protocol-owned field, never a downstream override.
    for (const auto &[key, ignored] : request.headers) {
      (void)ignored;
      if (equal_ascii(key, "content-type"))
        throw HttpCallError("invalid_argument", Delivery::NotSent, "Content-Type is managed by JSON-HTTP");
    }
    request.headers.emplace_back("Content-Type", "application/json");
  }
  raw.method = std::move(request.method);
  raw.target = std::move(request.target);
  raw.request_id = std::move(request.request_id);
  raw.headers = std::move(request.headers);
  auto response = client_.call(std::move(raw), deadline, cancellation);
  JsonHttpResponse typed;
  typed.status = response.status;
  typed.headers = std::move(response.headers);
  if (!response.body.empty()) {
    Json value;
    if (!json_content_type(typed.headers) ||
        !parse_json(response.body, value, {limits_.response_bytes, max_depth_}))
      throw HttpCallError("invalid_response", Delivery::OutcomeUnknown, "invalid JSON-HTTP response");
    typed.body = std::move(value);
  }
  return typed;
}
} // namespace xgc2::xrpc
