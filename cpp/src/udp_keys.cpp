#include "udp_wire.hpp"
#include <openssl/crypto.h>
#include <cstring>
#include <fstream>
#include <iterator>
#include <stdexcept>

namespace xgc2::xrpc::udp {
namespace {
constexpr std::size_t max_key_file_bytes = 1 << 20;

[[noreturn]] void reject(std::size_t line, const std::string &reason) {
  // Never quote the line: it holds key material.
  throw std::invalid_argument("udp key file line " + std::to_string(line) + ": " + reason);
}
bool blank(char c) { return c == ' ' || c == '\t'; }
} // namespace

KeyRing::~KeyRing() {
  for (auto &entry : keys_) OPENSSL_cleanse(entry.second.data(), entry.second.size());
}

void KeyRing::add(std::uint32_t key_id, std::string_view key) {
  if (key.size() != key_bytes) throw std::invalid_argument("udp.v1 keys are exactly 32 bytes");
  if (keys_.count(key_id)) throw std::invalid_argument("duplicate udp key_id " + std::to_string(key_id));
  std::array<std::uint8_t, key_bytes> value;
  std::memcpy(value.data(), key.data(), key_bytes);
  keys_.emplace(key_id, value);
  OPENSSL_cleanse(value.data(), value.size());
}

KeyRing KeyRing::parse(std::string_view text) {
  KeyRing ring;
  for (std::size_t number = 1; !text.empty(); ++number) {
    const auto end = text.find('\n');
    std::string_view line = text.substr(0, end);
    text = end == std::string_view::npos ? std::string_view{} : text.substr(end + 1);
    line = line.substr(0, line.find('#'));
    std::string_view tokens[3];
    std::size_t count = 0;
    for (std::size_t i = 0; i < line.size();) {
      if (blank(line[i]) || line[i] == '\r') {
        ++i;
        continue;
      }
      std::size_t j = i;
      while (j < line.size() && !blank(line[j]) && line[j] != '\r') ++j;
      if (count == 3) reject(number, "expected `<key_id> <base64 key>`");
      tokens[count++] = line.substr(i, j - i);
      i = j;
    }
    if (count == 0) continue;
    if (count != 2) reject(number, "expected `<key_id> <base64 key>`");
    std::uint64_t id = 0;
    if (tokens[0].size() > 10) reject(number, "key_id is not a decimal 32-bit number");
    for (const char digit : tokens[0]) {
      if (digit < '0' || digit > '9') reject(number, "key_id is not a decimal 32-bit number");
      id = id * 10 + static_cast<std::uint64_t>(digit - '0');
    }
    if (id > 0xffffffffu) reject(number, "key_id is not a decimal 32-bit number");
    auto key = detail::base64_decode(tokens[1]);
    if (!key) reject(number, "key is not canonical base64");
    if (key->size() != key_bytes) {
      OPENSSL_cleanse(key->data(), key->size());
      reject(number, "key must be exactly 32 bytes");
    }
    try {
      ring.add(static_cast<std::uint32_t>(id), std::string_view(reinterpret_cast<const char *>(key->data()), key->size()));
    } catch (const std::invalid_argument &) {
      OPENSSL_cleanse(key->data(), key->size());
      reject(number, "duplicate key_id");
    }
    OPENSSL_cleanse(key->data(), key->size());
  }
  return ring;
}

KeyRing KeyRing::load_file(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot open udp key file " + path);
  std::string text((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
  if (input.bad()) throw std::runtime_error("cannot read udp key file " + path);
  if (text.size() > max_key_file_bytes) throw std::runtime_error("udp key file is too large: " + path);
  try {
    auto ring = parse(text);
    OPENSSL_cleanse(&text[0], text.size());
    return ring;
  } catch (...) {
    OPENSSL_cleanse(&text[0], text.size());
    throw;
  }
}

const std::array<std::uint8_t, key_bytes> *KeyRing::find(std::uint32_t key_id) const noexcept {
  const auto found = keys_.find(key_id);
  return found == keys_.end() ? nullptr : &found->second;
}
} // namespace xgc2::xrpc::udp
