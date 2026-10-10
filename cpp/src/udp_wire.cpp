#include "udp_wire.hpp"
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <cstring>
#include <stdexcept>

namespace xgc2::xrpc::udp {
namespace detail {
namespace {
constexpr char magic[4] = {'X', 'R', 'U', '1'};

void put16(std::uint8_t *out, std::uint32_t value) {
  out[0] = static_cast<std::uint8_t>(value >> 8);
  out[1] = static_cast<std::uint8_t>(value);
}
void put32(std::uint8_t *out, std::uint32_t value) {
  put16(out, value >> 16);
  put16(out + 2, value);
}
std::uint32_t get16(const std::uint8_t *in) {
  return static_cast<std::uint32_t>(in[0]) << 8 | in[1];
}
std::uint32_t get32(const std::uint8_t *in) { return get16(in) << 16 | get16(in + 2); }

// Header, method and body followed by the tag; the caller fills nothing else.
std::vector<std::uint8_t> frame(const Key &key, Type type, std::uint16_t flags,
                                std::uint32_t key_id, const RequestId &request_id,
                                const InstanceId &instance, std::uint32_t word,
                                std::string_view method, std::string_view body) {
  // A request names a method of 1..128 bytes, a reply none.
  if (method.size() > max_method_bytes || (type == Type::Request) == method.empty() ||
      body.size() > max_body_bytes(method.size()))
    throw std::invalid_argument("udp.v1 datagram does not fit " +
                                std::to_string(max_datagram_bytes) + " bytes");
  std::vector<std::uint8_t> out(header_bytes + method.size() + body.size() + tag_bytes);
  std::memcpy(out.data(), magic, 4);
  out[4] = protocol_version;
  out[5] = static_cast<std::uint8_t>(type);
  put16(out.data() + 6, flags);
  put32(out.data() + 8, key_id);
  std::memcpy(out.data() + 12, request_id.data(), request_id.size());
  std::memcpy(out.data() + 28, instance.data(), instance.size());
  put32(out.data() + 44, word);
  put16(out.data() + 48, static_cast<std::uint32_t>(method.size()));
  put16(out.data() + 50, static_cast<std::uint32_t>(body.size()));
  std::memcpy(out.data() + header_bytes, method.data(), method.size());
  std::memcpy(out.data() + header_bytes + method.size(), body.data(), body.size());
  const auto signed_size = out.size() - tag_bytes;
  hmac_sha256(key.data(), key.size(), out.data(), signed_size, out.data() + signed_size);
  return out;
}
} // namespace

Parse parse(const std::uint8_t *data, std::size_t size, Datagram &out) {
  if (size < header_bytes + tag_bytes) return Parse::TooShort;
  if (size > max_datagram_bytes) return Parse::TooLong;
  if (std::memcmp(data, magic, 4) != 0) return Parse::BadMagic;
  if (data[4] != protocol_version) return Parse::BadVersion;
  if (data[5] != static_cast<std::uint8_t>(Type::Request) &&
      data[5] != static_cast<std::uint8_t>(Type::Reply))
    return Parse::BadType;
  const auto type = static_cast<Type>(data[5]);
  const std::size_t method_size = get16(data + 48), body_size = get16(data + 50);
  if (type == Type::Request ? (method_size < 1 || method_size > max_method_bytes)
                            : method_size != 0)
    return Parse::BadLength;
  if (header_bytes + method_size + body_size + tag_bytes != size) return Parse::BadLength;
  out.type = type;
  out.flags = static_cast<std::uint16_t>(get16(data + 6));
  out.key_id = get32(data + 8);
  std::memcpy(out.request_id.data(), data + 12, out.request_id.size());
  std::memcpy(out.instance.data(), data + 28, out.instance.size());
  out.word = get32(data + 44);
  out.method = std::string_view(reinterpret_cast<const char *>(data) + header_bytes, method_size);
  out.body = std::string_view(reinterpret_cast<const char *>(data) + header_bytes + method_size, body_size);
  out.signed_bytes = data;
  out.signed_size = size - tag_bytes;
  out.tag = data + out.signed_size;
  return Parse::Ok;
}

bool verify(const Datagram &datagram, const Key &key) {
  std::uint8_t expected[tag_bytes];
  hmac_sha256(key.data(), key.size(), datagram.signed_bytes, datagram.signed_size, expected);
  return CRYPTO_memcmp(expected, datagram.tag, tag_bytes) == 0;
}

std::vector<std::uint8_t> encode_request(const Key &key, std::uint32_t key_id,
                                         const RequestId &request_id,
                                         const std::optional<InstanceId> &expected,
                                         std::uint32_t timeout_ms, std::string_view method,
                                         std::string_view body) {
  return frame(key, Type::Request, expected ? flag_expected_instance : 0, key_id,
               request_id, expected ? *expected : InstanceId{}, timeout_ms, method, body);
}

std::vector<std::uint8_t> encode_reply(const Key &key, std::uint32_t key_id,
                                       const RequestId &request_id, const InstanceId &instance,
                                       std::uint32_t status, std::string_view body) {
  return frame(key, Type::Reply, 0, key_id, request_id, instance, status, {}, body);
}

void hmac_sha256(const std::uint8_t *key, std::size_t key_size, const std::uint8_t *data,
                 std::size_t size, std::uint8_t out[32]) {
  unsigned int length = 0;
  if (!HMAC(EVP_sha256(), key, static_cast<int>(key_size), data, size, out, &length) ||
      length != 32)
    throw std::runtime_error("HMAC-SHA256 failed");
}

namespace {
constexpr char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
int sextet(char c) {
  if (c >= 'A' && c <= 'Z') return c - 'A';
  if (c >= 'a' && c <= 'z') return c - 'a' + 26;
  if (c >= '0' && c <= '9') return c - '0' + 52;
  return c == '+' ? 62 : c == '/' ? 63 : -1;
}
} // namespace

std::string base64_encode(const std::uint8_t *data, std::size_t size) {
  std::string out;
  out.reserve((size + 2) / 3 * 4);
  for (std::size_t i = 0; i < size; i += 3) {
    const std::uint32_t group = static_cast<std::uint32_t>(data[i]) << 16 |
                                (i + 1 < size ? static_cast<std::uint32_t>(data[i + 1]) << 8 : 0) |
                                (i + 2 < size ? data[i + 2] : 0);
    out.push_back(alphabet[group >> 18 & 63]);
    out.push_back(alphabet[group >> 12 & 63]);
    out.push_back(i + 1 < size ? alphabet[group >> 6 & 63] : '=');
    out.push_back(i + 2 < size ? alphabet[group & 63] : '=');
  }
  return out;
}

std::optional<std::vector<std::uint8_t>> base64_decode(std::string_view text) {
  if (text.size() % 4 != 0) return std::nullopt;
  std::vector<std::uint8_t> out;
  out.reserve(text.size() / 4 * 3);
  for (std::size_t i = 0; i < text.size(); i += 4) {
    const bool last = i + 4 == text.size();
    const std::size_t pad = last ? (text[i + 3] == '=') + (text[i + 2] == '=' && text[i + 3] == '=') : 0;
    const int a = sextet(text[i]), b = sextet(text[i + 1]);
    const int c = pad == 2 ? 0 : sextet(text[i + 2]), d = pad >= 1 ? 0 : sextet(text[i + 3]);
    if (a < 0 || b < 0 || c < 0 || d < 0) return std::nullopt;
    // A '=' anywhere but the last one or two positions never reaches here as
    // a sextet, and the unused low bits must be zero for a canonical encoding.
    if ((pad == 2 && (b & 15)) || (pad == 1 && (c & 3))) return std::nullopt;
    const std::uint32_t group = static_cast<std::uint32_t>(a) << 18 | static_cast<std::uint32_t>(b) << 12 |
                                static_cast<std::uint32_t>(c) << 6 | static_cast<std::uint32_t>(d);
    out.push_back(static_cast<std::uint8_t>(group >> 16));
    if (pad < 2) out.push_back(static_cast<std::uint8_t>(group >> 8));
    if (pad < 1) out.push_back(static_cast<std::uint8_t>(group));
  }
  return out;
}

void random_bytes(std::uint8_t *out, std::size_t size) {
  if (RAND_bytes(out, static_cast<int>(size)) != 1) throw std::runtime_error("RAND_bytes failed");
}

void append_json_string(std::string &out, std::string_view text) {
  static const char hex[] = "0123456789abcdef";
  out.push_back('"');
  for (std::size_t i = 0; i < text.size();) {
    const auto c = static_cast<unsigned char>(text[i]);
    if (c >= 0x80) {
      std::size_t length = c >= 0xc2 && c <= 0xdf ? 2 : c >= 0xe0 && c <= 0xef ? 3
                         : c >= 0xf0 && c <= 0xf4 ? 4 : 0;
      std::uint32_t code = length == 2 ? c & 0x1f : length == 3 ? c & 0x0f : c & 0x07;
      bool valid = length != 0 && i + length <= text.size();
      for (std::size_t k = 1; valid && k < length; ++k) {
        const auto next = static_cast<unsigned char>(text[i + k]);
        valid = (next & 0xc0) == 0x80;
        code = code << 6 | (next & 0x3f);
      }
      valid = valid && !(length == 3 && code < 0x800) && !(length == 4 && code < 0x10000) &&
              code <= 0x10ffff && !(code >= 0xd800 && code <= 0xdfff);
      if (valid) {
        out.append(text.substr(i, length));
        i += length;
      } else {
        out += "\xef\xbf\xbd"; // U+FFFD
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
} // namespace detail

std::string to_hex(const InstanceId &id) {
  static const char hex[] = "0123456789abcdef";
  std::string out;
  for (const auto byte : id) {
    out.push_back(hex[byte >> 4]);
    out.push_back(hex[byte & 15]);
  }
  return out;
}

std::optional<InstanceId> instance_from_hex(std::string_view text) {
  if (text.size() != 2 * InstanceId{}.size()) return std::nullopt;
  InstanceId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    int value = 0;
    for (const char digit : text.substr(2 * i, 2)) {
      const int nibble = digit >= '0' && digit <= '9' ? digit - '0'
                         : digit >= 'a' && digit <= 'f' ? digit - 'a' + 10 : -1;
      if (nibble < 0) return std::nullopt;
      value = value * 16 + nibble;
    }
    id[i] = static_cast<std::uint8_t>(value);
  }
  return id;
}

std::string_view status_name(Status status) noexcept {
  switch (status) {
  case Status::Ok: return "ok";
  case Status::InvalidArgument: return "invalid_argument";
  case Status::NotFound: return "not_found";
  case Status::Conflict: return "conflict";
  case Status::ResourceExhausted: return "resource_exhausted";
  case Status::DeadlineExceeded: return "deadline_exceeded";
  case Status::Cancelled: return "cancelled";
  case Status::Unavailable: return "unavailable";
  case Status::Internal: return "internal";
  case Status::Unauthenticated: return "unauthenticated";
  case Status::PermissionDenied: return "permission_denied";
  }
  return "unknown";
}

std::string error_body(Status status, std::string_view message, std::string_view details_json) {
  while (!details_json.empty() && (details_json.front() == ' ' || details_json.front() == '\t' ||
                                   details_json.front() == '\n' || details_json.front() == '\r'))
    details_json.remove_prefix(1);
  while (!details_json.empty() && (details_json.back() == ' ' || details_json.back() == '\t' ||
                                   details_json.back() == '\n' || details_json.back() == '\r'))
    details_json.remove_suffix(1);
  if (details_json.empty()) details_json = "{}";
  if (details_json.front() != '{' || details_json.back() != '}')
    throw std::invalid_argument("error details must be a JSON object");
  std::string out = "{\"code\":";
  detail::append_json_string(out, status_name(status));
  out += ",\"message\":";
  detail::append_json_string(out, message);
  out += ",\"details\":";
  out.append(details_json);
  out.push_back('}');
  return out;
}
} // namespace xgc2::xrpc::udp
