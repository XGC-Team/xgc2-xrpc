#pragma once
// Method addressing across profiles. A callable domain operation is named
// "<service>/<Method>" (for example "xgc2.chassis.hold/Engage") and takes and
// returns JSON text. One MethodRouter holds the handlers; the transports serve
// them under the same name:
//
//   udp.v1   the datagram's method field        method_udp.hpp:  add_methods
//   http.v1  POST /v1/call/<service>/<Method>   method_http.hpp: handle_method_call
//
// This header depends only on the C++17 standard library and links nothing, so
// a udp.v1 host (the Bionic robots) uses it without any HTTP component. Handlers
// get the body as text and answer with a result or an error through a
// MethodReply that completes at most once from any thread, also after the
// handler has returned.
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace xgc2::xrpc {
inline constexpr std::size_t max_method_name_bytes = 128; // the udp.v1 limit
inline constexpr std::string_view method_path_prefix = "/v1/call/";

namespace method_detail {
inline bool identifier(std::string_view text) noexcept {
  if (text.empty()) return false;
  for (std::size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    const bool letter = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    const bool more = i > 0 && ((c >= '0' && c <= '9') || c == '_');
    if (!letter && !more) return false;
  }
  return true;
}
inline bool service_name(std::string_view text) noexcept {
  std::size_t start = 0;
  for (;;) {
    const auto dot = text.find('.', start);
    const auto part = text.substr(start, dot == std::string_view::npos ? dot : dot - start);
    if (!identifier(part)) return false;
    if (dot == std::string_view::npos) return true;
    start = dot + 1;
  }
}
// Length of the well-formed UTF-8 sequence at text[i] (1 for ASCII), or 0.
inline std::size_t utf8_length(std::string_view text, std::size_t i) noexcept {
  const auto c = static_cast<unsigned char>(text[i]);
  if (c < 0x80) return 1;
  const std::size_t length = c >= 0xc2 && c <= 0xdf ? 2 : c >= 0xe0 && c <= 0xef ? 3 : c >= 0xf0 && c <= 0xf4 ? 4 : 0;
  if (length == 0 || i + length > text.size()) return 0;
  std::uint32_t code = length == 2 ? c & 0x1f : length == 3 ? c & 0x0f : c & 0x07;
  for (std::size_t k = 1; k < length; ++k) {
    const auto next = static_cast<unsigned char>(text[i + k]);
    if ((next & 0xc0) != 0x80) return 0;
    code = code << 6 | (next & 0x3f);
  }
  const bool overlong = (length == 3 && code < 0x800) || (length == 4 && code < 0x10000);
  return overlong || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff) ? 0 : length;
}
// A JSON string literal; bytes that are not UTF-8 become U+FFFD.
inline void append_json_string(std::string &out, std::string_view text) {
  static const char hex[] = "0123456789abcdef";
  out.push_back('"');
  for (std::size_t i = 0; i < text.size();) {
    const auto c = static_cast<unsigned char>(text[i]);
    if (c >= 0x80) {
      if (const auto length = utf8_length(text, i)) {
        out.append(text.substr(i, length));
        i += length;
      } else {
        out += "\xef\xbf\xbd";
        ++i;
      }
      continue;
    }
    ++i;
    if (c == '"' || c == '\\') {
      out.push_back('\\');
      out.push_back(static_cast<char>(c));
    } else if (c == '\n') {
      out += "\\n";
    } else if (c == '\r') {
      out += "\\r";
    } else if (c == '\t') {
      out += "\\t";
    } else if (c < 0x20 || c == 0x7f) {
      out += "\\u00";
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 15]);
    } else {
      out.push_back(static_cast<char>(c));
    }
  }
  out.push_back('"');
}
// text without surrounding white space, which must then be a JSON object
// literal ({...}); an empty text is "{}". The content is not parsed.
inline std::string_view object_literal(std::string_view text, const char *what) {
  const auto blank = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; };
  while (!text.empty() && blank(text.front())) text.remove_prefix(1);
  while (!text.empty() && blank(text.back())) text.remove_suffix(1);
  if (text.empty()) return "{}";
  if (text.front() != '{' || text.back() != '}') throw std::invalid_argument(std::string(what) + " must be a JSON object");
  return text;
}
} // namespace method_detail

// Splits "<service>/<Method>". The service is one or more dot-separated
// identifiers, the method one identifier, an identifier a letter followed by
// letters, digits and underscores; the whole name has at most
// max_method_name_bytes. This alphabet is valid in a URL path, in a udp.v1
// datagram and in a gRPC full method alike.
inline bool split_method(std::string_view name, std::string_view *service = nullptr,
                         std::string_view *method = nullptr) noexcept {
  const auto slash = name.find('/');
  if (slash == std::string_view::npos || name.size() > max_method_name_bytes) return false;
  const auto head = name.substr(0, slash), tail = name.substr(slash + 1);
  if (!method_detail::service_name(head) || !method_detail::identifier(tail)) return false;
  if (service) *service = head;
  if (method) *method = tail;
  return true;
}

// The common error vocabulary, numbered as the udp.v1 statuses. Success is not
// an error and has no code.
enum class MethodCode : std::uint32_t {
  InvalidArgument = 1,
  NotFound = 2,
  Conflict = 3,
  ResourceExhausted = 4,
  DeadlineExceeded = 5,
  Cancelled = 6,
  Unavailable = 7,
  Internal = 8,
  Unauthenticated = 9,
  PermissionDenied = 10
};
inline std::string_view method_code_name(MethodCode code) noexcept {
  switch (code) {
  case MethodCode::InvalidArgument: return "invalid_argument";
  case MethodCode::NotFound: return "not_found";
  case MethodCode::Conflict: return "conflict";
  case MethodCode::ResourceExhausted: return "resource_exhausted";
  case MethodCode::DeadlineExceeded: return "deadline_exceeded";
  case MethodCode::Cancelled: return "cancelled";
  case MethodCode::Unavailable: return "unavailable";
  case MethodCode::Internal: return "internal";
  case MethodCode::Unauthenticated: return "unauthenticated";
  case MethodCode::PermissionDenied: return "permission_denied";
  }
  return "internal";
}
// The HTTP status that carries a code on http.v1.
inline int method_code_http_status(MethodCode code) noexcept {
  switch (code) {
  case MethodCode::InvalidArgument: return 400;
  case MethodCode::Unauthenticated: return 401;
  case MethodCode::PermissionDenied: return 403;
  case MethodCode::NotFound: return 404;
  case MethodCode::Conflict: return 409;
  case MethodCode::ResourceExhausted: return 429;
  case MethodCode::DeadlineExceeded: return 504;
  case MethodCode::Cancelled: return 499;
  case MethodCode::Unavailable: return 503;
  case MethodCode::Internal: return 500;
  }
  return 500;
}
// {"code":"<name>","message":"...","details":{...}}, the error body of udp.v1;
// http.v1 wraps it as {"error": ...}. The message is escaped and bytes that are
// not UTF-8 are replaced. details_json must be a JSON object (it is inserted as
// it is, unparsed); std::invalid_argument otherwise.
inline std::string method_error_body(MethodCode code, std::string_view message, std::string_view details_json = "{}") {
  const auto details = method_detail::object_literal(details_json, "error details");
  std::string out = "{\"code\":";
  method_detail::append_json_string(out, method_code_name(code));
  out += ",\"message\":";
  method_detail::append_json_string(out, message);
  out += ",\"details\":";
  out.append(details);
  out.push_back('}');
  return out;
}

// One call as the handler sees it, whatever the transport.
struct MethodRequest {
  std::string method;     // "<service>/<Method>"
  std::string body;       // the JSON text the caller sent; empty if it sent none
  std::string request_id; // X-Request-ID on http.v1, the 32 hex digits of the request id on udp.v1
  // The host's deadline for the call. A reply completed after it is not delivered.
  std::chrono::steady_clock::time_point deadline;
};

// Completes one call, at most once, from any thread, before the deadline. Copies
// share the call: the first to complete it wins and the others get false. A call
// whose last copy is dropped without completing sends nothing, and the caller
// sees an unknown outcome.
class MethodReply {
public:
  // What a transport adapter implements. complete() and error() return true
  // for the one call that completed the request.
  class Sink {
  public:
    virtual ~Sink() = default;
    virtual bool complete(std::string_view json) = 0;
    virtual bool error(MethodCode code, std::string_view message, std::string_view details_json) = 0;
    virtual bool cancelled() const noexcept = 0;
  };
  MethodReply() = default;
  explicit MethodReply(std::shared_ptr<Sink> sink) noexcept : sink_(std::move(sink)) {}
  // Answers with the JSON result (an empty text is the JSON null). False when the
  // call was already completed, its deadline passed or the host is gone.
  bool complete(std::string_view json) const { return sink_ && sink_->complete(json.empty() ? "null" : json); }
  // Answers with an error; details_json must be a JSON object. Unauthenticated
  // has no wire form on udp.v1 (the carrier authenticates) and is sent as
  // permission_denied there.
  bool error(MethodCode code, std::string_view message, std::string_view details_json = "{}") const {
    return sink_ && sink_->error(code, message, details_json);
  }
  // True when the caller has gone away (http.v1: the connection closed). The
  // business action is not stopped by this; cancellation never means rollback.
  bool cancelled() const noexcept { return sink_ && sink_->cancelled(); }
  explicit operator bool() const noexcept { return sink_ != nullptr; }

private:
  std::shared_ptr<Sink> sink_;
};

// The handlers of one service host, by method name. Build it completely before
// serving; it is not synchronized. Serving copies the handlers, so the router may
// go away afterwards.
class MethodRouter {
public:
  using Handler = std::function<void(MethodRequest, MethodReply)>;
  // Throws std::invalid_argument for a name that split_method rejects, an empty
  // handler or a name that is already registered.
  void add(std::string name, Handler handler) {
    if (!split_method(name)) throw std::invalid_argument("invalid method name: " + name);
    if (!handler) throw std::invalid_argument("empty handler for " + name);
    if (!handlers_.emplace(std::move(name), std::move(handler)).second)
      throw std::invalid_argument("method registered twice");
  }
  const Handler *find(std::string_view name) const noexcept {
    const auto found = handlers_.find(name);
    return found == handlers_.end() ? nullptr : &found->second;
  }
  std::vector<std::string> names() const {
    std::vector<std::string> out;
    for (const auto &entry : handlers_) out.push_back(entry.first);
    return out;
  }
  // The distinct <service> parts, sorted: the capabilities this router serves.
  std::vector<std::string> services() const {
    std::vector<std::string> out;
    for (const auto &entry : handlers_) {
      std::string_view service;
      split_method(entry.first, &service);
      if (out.empty() || out.back() != service) out.emplace_back(service);
    }
    return out;
  }
  bool empty() const noexcept { return handlers_.empty(); }

private:
  std::map<std::string, Handler, std::less<>> handlers_;
};

// A service lists the capability `name` (the <service> part of its method names)
// for these entities: a world host serving every chassis of its world lists
// {"xgc2.chassis.hold", {"scout-1", "scout-2"}}, so a caller can resolve
// "entity X, capability C" to a service without knowing the domain.
struct Capability {
  std::string name;
  std::vector<std::string> entities;
};
inline constexpr std::size_t max_entity_bytes = 128;
// [{"name":"<service>","entities":["<id>",...]},...]: the value of the facts key
// "capabilities". std::invalid_argument for a name that is not a service name, an
// entity that is empty, longer than max_entity_bytes, not UTF-8 or holding a
// control character, or a repeated name or entity.
inline std::string capabilities_json(const std::vector<Capability> &capabilities) {
  std::string out = "[";
  std::vector<std::string_view> seen;
  for (const auto &capability : capabilities) {
    if (!method_detail::service_name(capability.name) || capability.name.size() > max_method_name_bytes)
      throw std::invalid_argument("capability name is not a service name: " + capability.name);
    for (const auto previous : seen)
      if (previous == capability.name) throw std::invalid_argument("capability listed twice: " + capability.name);
    seen.push_back(capability.name);
    if (out.size() > 1) out.push_back(',');
    out += "{\"name\":";
    method_detail::append_json_string(out, capability.name);
    out += ",\"entities\":[";
    for (std::size_t i = 0; i < capability.entities.size(); ++i) {
      const auto &entity = capability.entities[i];
      bool valid = !entity.empty() && entity.size() <= max_entity_bytes;
      for (std::size_t k = 0; valid && k < entity.size();) {
        const auto length = method_detail::utf8_length(entity, k);
        const auto c = static_cast<unsigned char>(entity[k]);
        valid = length != 0 && !(length == 1 && (c < 0x20 || c == 0x7f));
        k += length ? length : 1;
      }
      if (!valid) throw std::invalid_argument("invalid entity identity in capability " + capability.name);
      for (std::size_t k = 0; k < i; ++k)
        if (capability.entities[k] == entity) throw std::invalid_argument("entity listed twice in capability " + capability.name);
      if (i) out.push_back(',');
      method_detail::append_json_string(out, entity);
    }
    out += "]}";
  }
  out.push_back(']');
  return out;
}

// The readiness envelope every service answers:
// {"service":...,"api_version":...,"instance_id":...,"ready":...,"facts":{...}}.
// facts_json must be a JSON object (inserted as it is); a host adds
// "capabilities": capabilities_json(...) to it. The meaning of the facts belongs
// to the domain.
inline std::string describe_json(std::string_view service, std::string_view api_version, std::string_view instance_id,
                                 bool ready, std::string_view facts_json = "{}") {
  const auto facts = method_detail::object_literal(facts_json, "facts");
  std::string out = "{\"service\":";
  method_detail::append_json_string(out, service);
  out += ",\"api_version\":";
  method_detail::append_json_string(out, api_version);
  out += ",\"instance_id\":";
  method_detail::append_json_string(out, instance_id);
  out += ready ? ",\"ready\":true,\"facts\":" : ",\"ready\":false,\"facts\":";
  out.append(facts);
  out.push_back('}');
  return out;
}
} // namespace xgc2::xrpc
