#include "udp_net.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace xgc2::xrpc::udp::detail {
std::optional<Address> make_address(const std::string &host, std::uint16_t port) {
  Address address;
  if (host.find(':') == std::string::npos) {
    // inet_pton is strict dotted-quad; getaddrinfo would also take "127.1".
    auto &v4 = *reinterpret_cast<sockaddr_in *>(&address.storage);
    if (::inet_pton(AF_INET, host.c_str(), &v4.sin_addr) != 1) return std::nullopt;
    v4.sin_family = AF_INET;
    v4.sin_port = htons(port);
    address.size = sizeof v4;
    return address;
  }
  // IPv6, possibly with a zone ("fe80::1%eth0"), which only getaddrinfo reads.
  addrinfo hints{};
  hints.ai_family = AF_INET6;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
  addrinfo *found = nullptr;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found) != 0 || !found)
    return std::nullopt;
  std::optional<Address> result;
  if (found->ai_family == AF_INET6 && found->ai_addrlen <= sizeof(sockaddr_storage)) {
    std::memcpy(&address.storage, found->ai_addr, found->ai_addrlen);
    address.size = found->ai_addrlen;
    result = address;
  }
  ::freeaddrinfo(found);
  return result;
}

std::optional<Address> parse_endpoint(std::string_view endpoint) {
  std::string host;
  std::string_view port_text;
  if (!endpoint.empty() && endpoint.front() == '[') {
    const auto close = endpoint.find(']');
    if (close == std::string_view::npos || close + 1 >= endpoint.size() || endpoint[close + 1] != ':')
      return std::nullopt;
    host.assign(endpoint.substr(1, close - 1));
    port_text = endpoint.substr(close + 2);
  } else {
    const auto colon = endpoint.find(':');
    // An unbracketed IPv6 address would be ambiguous: exactly one colon.
    if (colon == std::string_view::npos || endpoint.find(':', colon + 1) != std::string_view::npos)
      return std::nullopt;
    host.assign(endpoint.substr(0, colon));
    port_text = endpoint.substr(colon + 1);
  }
  if (port_text.empty() || port_text.size() > 5) return std::nullopt;
  unsigned port = 0;
  for (const char digit : port_text) {
    if (digit < '0' || digit > '9') return std::nullopt;
    port = port * 10 + static_cast<unsigned>(digit - '0');
  }
  if (port == 0 || port > 65535) return std::nullopt;
  return make_address(host, static_cast<std::uint16_t>(port));
}

std::string format_address(const sockaddr *address, socklen_t size) {
  char host[NI_MAXHOST], service[NI_MAXSERV];
  if (::getnameinfo(address, size, host, sizeof host, service, sizeof service,
                    NI_NUMERICHOST | NI_NUMERICSERV) != 0)
    return "unknown";
  return address->sa_family == AF_INET6 ? std::string("[") + host + "]:" + service
                                        : std::string(host) + ":" + service;
}

std::uint16_t port_of(const Address &address) {
  if (address.family() == AF_INET6)
    return ntohs(reinterpret_cast<const sockaddr_in6 *>(&address.storage)->sin6_port);
  return ntohs(reinterpret_cast<const sockaddr_in *>(&address.storage)->sin_port);
}

bool is_unspecified_v6(const Address &address) {
  return address.family() == AF_INET6 &&
         IN6_IS_ADDR_UNSPECIFIED(&reinterpret_cast<const sockaddr_in6 *>(&address.storage)->sin6_addr);
}

SourceKey source_key(const Address &address) {
  SourceKey key;
  if (address.family() == AF_INET6) {
    const auto &v6 = *reinterpret_cast<const sockaddr_in6 *>(&address.storage);
    std::memcpy(key.address.data(), &v6.sin6_addr, 16);
    key.scope = v6.sin6_scope_id;
  } else {
    const auto &v4 = *reinterpret_cast<const sockaddr_in *>(&address.storage);
    key.address[10] = key.address[11] = 0xff; // ::ffff:a.b.c.d
    std::memcpy(key.address.data() + 12, &v4.sin_addr, 4);
  }
  return key;
}

std::size_t SourceKeyHash::operator()(const SourceKey &key) const {
  return std::hash<std::string_view>()(
             std::string_view(reinterpret_cast<const char *>(key.address.data()), key.address.size())) ^
         (static_cast<std::size_t>(key.scope) * 0x9e3779b97f4a7c15ull);
}
} // namespace xgc2::xrpc::udp::detail
