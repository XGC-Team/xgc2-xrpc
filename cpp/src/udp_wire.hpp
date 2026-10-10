#pragma once
// Datagram codec and primitives of udp.v1. Internal to the library; the tests
// include it directly.
#include "xgc2/xrpc/udp.hpp"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace xgc2::xrpc::udp::detail {
inline constexpr std::size_t header_bytes = 52;
inline constexpr std::size_t tag_bytes = 32;
inline constexpr std::uint8_t protocol_version = 1;
inline constexpr std::uint16_t flag_expected_instance = 1;
enum class Type : std::uint8_t { Request = 1, Reply = 2 };

using Key = std::array<std::uint8_t, key_bytes>;

// Largest body that fits a datagram with a method of this length.
constexpr std::size_t max_body_bytes(std::size_t method_bytes) {
  return max_datagram_bytes - header_bytes - tag_bytes - method_bytes;
}

// A structurally valid datagram. The views point into the parsed buffer.
struct Datagram {
  Type type = Type::Request;
  std::uint16_t flags = 0;
  std::uint32_t key_id = 0;
  RequestId request_id{};
  InstanceId instance{};
  std::uint32_t word = 0; // request: timeout_ms; reply: status
  std::string_view method, body;
  const std::uint8_t *signed_bytes = nullptr; // everything before the tag
  std::size_t signed_size = 0;
  const std::uint8_t *tag = nullptr;
};
enum class Parse { Ok, TooShort, TooLong, BadMagic, BadVersion, BadType, BadLength };
// Layout checks only; says nothing about the tag. A request has a method of
// 1..128 bytes, a reply none.
Parse parse(const std::uint8_t *data, std::size_t size, Datagram &out);
bool verify(const Datagram &datagram, const Key &key);

// Throw std::invalid_argument when the arguments do not fit one datagram.
std::vector<std::uint8_t> encode_request(const Key &key, std::uint32_t key_id,
                                         const RequestId &request_id,
                                         const std::optional<InstanceId> &expected,
                                         std::uint32_t timeout_ms,
                                         std::string_view method,
                                         std::string_view body);
std::vector<std::uint8_t> encode_reply(const Key &key, std::uint32_t key_id,
                                       const RequestId &request_id,
                                       const InstanceId &instance,
                                       std::uint32_t status, std::string_view body);

void hmac_sha256(const std::uint8_t *key, std::size_t key_size,
                 const std::uint8_t *data, std::size_t size, std::uint8_t out[32]);
// Standard alphabet with padding, canonical encodings only: no whitespace, no
// URL-safe characters, no non-zero trailing bits.
std::string base64_encode(const std::uint8_t *data, std::size_t size);
std::optional<std::vector<std::uint8_t>> base64_decode(std::string_view text);
// Throws std::runtime_error if the system source fails.
void random_bytes(std::uint8_t *out, std::size_t size);
// JSON string literal of the text, quotes included; invalid UTF-8 becomes U+FFFD.
void append_json_string(std::string &out, std::string_view text);
// A method name: 1..128 bytes of valid UTF-8 without spaces or control characters.
bool valid_method(std::string_view method);
} // namespace xgc2::xrpc::udp::detail
