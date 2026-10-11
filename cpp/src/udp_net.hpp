#pragma once
// Socket address helpers of udp.v1. Internal to the library.
#include <sys/socket.h>
#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace xgc2::xrpc::udp::detail {
struct Address {
  sockaddr_storage storage{};
  socklen_t size = 0;
  const sockaddr *get() const { return reinterpret_cast<const sockaddr *>(&storage); }
  sockaddr *get() { return reinterpret_cast<sockaddr *>(&storage); }
  int family() const { return storage.ss_family; }
};
// Numeric host (IPv4, or IPv6 with optional zone) and port; no name lookups.
std::optional<Address> make_address(const std::string &host, std::uint16_t port);
// "host:port" or "[ipv6]:port" with a port of 1..65535.
std::optional<Address> parse_endpoint(std::string_view endpoint);
// Numeric "host:port" / "[host]:port" of a socket address.
std::string format_address(const sockaddr *address, socklen_t size);
std::uint16_t port_of(const Address &address);
bool is_unspecified_v6(const Address &address);

// Source address without the port, IPv4 mapped into IPv6 form, for rate limits.
struct SourceKey {
  std::array<std::uint8_t, 16> address{};
  std::uint32_t scope = 0;
  bool operator==(const SourceKey &other) const {
    return address == other.address && scope == other.scope;
  }
};
SourceKey source_key(const Address &address);
struct SourceKeyHash {
  std::size_t operator()(const SourceKey &key) const;
};
} // namespace xgc2::xrpc::udp::detail
