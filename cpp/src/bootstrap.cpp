#include <xgc2/xrpc/bootstrap.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <map>
#include <mutex>
#include <set>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/x509.h>

namespace xgc2::xrpc {
namespace {
constexpr std::size_t document_limit = 16 * 1024;
[[noreturn]] void fail(BootstrapErrorCode code = BootstrapErrorCode::InvalidInput) {
  throw BootstrapError(code);
}
bool identifier(std::string_view value) {
  if (value.empty() || value.size() > 128) return false;
  for (unsigned char c : value)
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-'))
      return false;
  return true;
}
bool utf8(std::string_view value) {
  for (std::size_t i = 0; i < value.size();) {
    auto c = static_cast<unsigned char>(value[i++]);
    if (c < 0x80) continue;
    unsigned count = 0;
    std::uint32_t n = 0, minimum = 0;
    if (c >= 0xc2 && c <= 0xdf) { count = 1; n = c & 31; minimum = 0x80; }
    else if (c >= 0xe0 && c <= 0xef) { count = 2; n = c & 15; minimum = 0x800; }
    else if (c >= 0xf0 && c <= 0xf4) { count = 3; n = c & 7; minimum = 0x10000; }
    else return false;
    if (value.size() - i < count) return false;
    while (count--) {
      auto next = static_cast<unsigned char>(value[i++]);
      if ((next & 0xc0) != 0x80) return false;
      n = (n << 6) | (next & 63);
    }
    if (n < minimum || n > 0x10ffff || (n >= 0xd800 && n <= 0xdfff)) return false;
  }
  return true;
}
bool whitespace(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }
std::string_view trim(std::string_view value) {
  while (!value.empty() && whitespace(value.front())) value.remove_prefix(1);
  while (!value.empty() && whitespace(value.back())) value.remove_suffix(1);
  return value;
}

// Startup documents are bounded, typed and retain application JSON verbatim.
// Duplicate keys are rejected in every object, including the opaque application.
struct Json {
  enum class Kind { Null, Boolean, Number, String, Object, Array } kind;
  std::string text;
  std::map<std::string, Json> object;
  std::vector<Json> array;
  std::string_view raw;
};
class JsonReader {
public:
  explicit JsonReader(std::string_view input) : input_(input) {}
  Json read() {
    if (input_.empty() || input_.size() > document_limit || !utf8(input_)) fail();
    auto result = value(0);
    skip();
    if (at_ != input_.size()) fail();
    return result;
  }
private:
  std::string_view input_;
  std::size_t at_ = 0;
  void skip() { while (at_ < input_.size() && whitespace(input_[at_])) ++at_; }
  bool take(char c) {
    if (at_ < input_.size() && input_[at_] == c) { ++at_; return true; }
    return false;
  }
  void require(char c) { if (!take(c)) fail(); }
  std::uint32_t hex4() {
    std::uint32_t n = 0;
    for (unsigned i = 0; i < 4; ++i) {
      if (at_ == input_.size()) fail();
      unsigned char c = input_[at_++];
      if (c >= '0' && c <= '9') n = (n << 4) | (c - '0');
      else if (c >= 'a' && c <= 'f') n = (n << 4) | (c - 'a' + 10);
      else if (c >= 'A' && c <= 'F') n = (n << 4) | (c - 'A' + 10);
      else fail();
    }
    return n;
  }
  static void append_utf8(std::string &out, std::uint32_t n) {
    if (n < 0x80) out.push_back(static_cast<char>(n));
    else if (n < 0x800) {
      out.push_back(static_cast<char>(0xc0 | (n >> 6)));
      out.push_back(static_cast<char>(0x80 | (n & 63)));
    } else if (n < 0x10000) {
      out.push_back(static_cast<char>(0xe0 | (n >> 12)));
      out.push_back(static_cast<char>(0x80 | ((n >> 6) & 63)));
      out.push_back(static_cast<char>(0x80 | (n & 63)));
    } else {
      out.push_back(static_cast<char>(0xf0 | (n >> 18)));
      out.push_back(static_cast<char>(0x80 | ((n >> 12) & 63)));
      out.push_back(static_cast<char>(0x80 | ((n >> 6) & 63)));
      out.push_back(static_cast<char>(0x80 | (n & 63)));
    }
  }
  std::string string() {
    require('"');
    std::string out;
    while (at_ < input_.size()) {
      unsigned char c = input_[at_++];
      if (c == '"') return out;
      if (c < 32) fail();
      if (c != '\\') { out.push_back(static_cast<char>(c)); continue; }
      if (at_ == input_.size()) fail();
      switch (input_[at_++]) {
      case '"': out.push_back('"'); break;
      case '\\': out.push_back('\\'); break;
      case '/': out.push_back('/'); break;
      case 'b': out.push_back('\b'); break;
      case 'f': out.push_back('\f'); break;
      case 'n': out.push_back('\n'); break;
      case 'r': out.push_back('\r'); break;
      case 't': out.push_back('\t'); break;
      case 'u': {
        auto n = hex4();
        if (n >= 0xd800 && n <= 0xdbff) {
          require('\\'); require('u');
          auto low = hex4();
          if (low < 0xdc00 || low > 0xdfff) fail();
          n = 0x10000 + ((n - 0xd800) << 10) + low - 0xdc00;
        } else if (n >= 0xdc00 && n <= 0xdfff) fail();
        append_utf8(out, n);
        break;
      }
      default: fail();
      }
    }
    fail();
  }
  Json value(unsigned depth) {
    if (depth > 36) fail();
    skip();
    const auto start = at_;
    if (at_ == input_.size()) fail();
    Json out{};
    if (take('{')) {
      out.kind = Json::Kind::Object;
      skip();
      if (!take('}')) {
        do {
          skip();
          auto key = string();
          skip(); require(':');
          auto child = value(depth + 1);
          if (!out.object.emplace(std::move(key), std::move(child)).second) fail();
          skip();
          if (take('}')) break;
          require(',');
        } while (true);
      }
    } else if (take('[')) {
      out.kind = Json::Kind::Array;
      skip();
      if (!take(']')) {
        do {
          out.array.push_back(value(depth + 1));
          skip();
          if (take(']')) break;
          require(',');
        } while (true);
      }
    } else if (input_[at_] == '"') {
      out.kind = Json::Kind::String;
      out.text = string();
    } else if (input_.substr(at_, 4) == "null") {
      out.kind = Json::Kind::Null; at_ += 4;
    } else if (input_.substr(at_, 4) == "true") {
      out.kind = Json::Kind::Boolean; at_ += 4;
    } else if (input_.substr(at_, 5) == "false") {
      out.kind = Json::Kind::Boolean; at_ += 5;
    } else {
      out.kind = Json::Kind::Number;
      take('-');
      if (!take('0')) {
        if (at_ == input_.size() || input_[at_] < '1' || input_[at_] > '9') fail();
        do { ++at_; } while (at_ < input_.size() && input_[at_] >= '0' && input_[at_] <= '9');
      }
      if (take('.')) {
        auto begin = at_;
        while (at_ < input_.size() && input_[at_] >= '0' && input_[at_] <= '9') ++at_;
        if (at_ == begin) fail();
      }
      if (take('e') || take('E')) {
        if (!take('+')) take('-');
        auto begin = at_;
        while (at_ < input_.size() && input_[at_] >= '0' && input_[at_] <= '9') ++at_;
        if (at_ == begin) fail();
      }
      out.text = std::string(input_.substr(start, at_ - start));
    }
    out.raw = input_.substr(start, at_ - start);
    return out;
  }
};
void fields(const Json &object, std::initializer_list<std::string_view> required,
            std::initializer_list<std::string_view> optional = {}) {
  if (object.kind != Json::Kind::Object) fail();
  for (auto name : required) if (!object.object.count(std::string(name))) fail();
  for (const auto &[key, unused] : object.object) {
    (void)unused;
    if (std::find(required.begin(), required.end(), key) == required.end() &&
        std::find(optional.begin(), optional.end(), key) == optional.end()) fail();
  }
}
const Json &get(const Json &object, const char *key) {
  auto at = object.object.find(key);
  if (at == object.object.end()) fail();
  return at->second;
}
const std::string &text(const Json &value) {
  if (value.kind != Json::Kind::String) fail();
  return value.text;
}
std::string name(const Json &value) {
  const auto &out = text(value);
  if (!identifier(out)) fail();
  return out;
}
void version(const Json &object) {
  const auto &v = get(object, "schema_version");
  if (v.kind != Json::Kind::Number || v.text != "1") fail();
}
void application_depth(const Json &value, unsigned depth = 0) {
  if (value.kind != Json::Kind::Object && value.kind != Json::Kind::Array) return;
  if (depth >= 32) fail();
  for (const auto &[key, child] : value.object) { (void)key; application_depth(child, depth + 1); }
  for (const auto &child : value.array) application_depth(child, depth + 1);
}

bool canonical_path(std::string_view path, std::size_t limit) {
  if (path.size() < 2 || path.size() > limit || path.front() != '/' ||
      path.back() == '/' || path.find('\0') != path.npos || !utf8(path)) return false;
  std::size_t begin = 1;
  while (begin < path.size()) {
    auto end = path.find('/', begin);
    if (end == path.npos) end = path.size();
    auto part = path.substr(begin, end - begin);
    if (part.empty() || part == "." || part == "..") return false;
    begin = end + 1;
  }
  return true;
}
bool host(std::string_view value) {
  if (value.empty() || value.size() > 253) return false;
  std::string input(value);
  std::array<unsigned char, 16> bytes{};
  std::array<char, INET6_ADDRSTRLEN> canonical{};
  for (auto af : {AF_INET, AF_INET6}) {
    if (::inet_pton(af, input.c_str(), bytes.data()) == 1)
      return ::inet_ntop(af, bytes.data(), canonical.data(), canonical.size()) && value == canonical.data();
  }
  if (value.find(':') != value.npos) return false;
  std::size_t begin = 0;
  while (begin < value.size()) {
    auto end = value.find('.', begin);
    if (end == value.npos) end = value.size();
    auto label = value.substr(begin, end - begin);
    if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') return false;
    for (unsigned char c : label)
      if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) return false;
    if (end == value.size()) return true;
    begin = end + 1;
  }
  return false;
}
bool port(std::string_view value) {
  if (value.empty() || value.size() > 5 || value.front() == '0') return false;
  unsigned n = 0;
  for (auto c : value) {
    if (c < '0' || c > '9') return false;
    n = n * 10 + static_cast<unsigned>(c - '0');
  }
  return n >= 1 && n <= 65535;
}
bool authority(std::string_view value, bool required_port) {
  if (value.empty()) return false;
  std::string_view hostname, port_value;
  bool has_port = false;
  if (value.front() == '[') {
    auto end = value.find(']');
    if (end == value.npos) return false;
    hostname = value.substr(1, end - 1);
    if (hostname.find(':') == hostname.npos) return false;
    auto rest = value.substr(end + 1);
    if (!rest.empty()) {
      if (rest.front() != ':') return false;
      port_value = rest.substr(1); has_port = true;
    }
  } else {
    auto separator = value.find(':');
    hostname = value.substr(0, separator);
    if (separator != value.npos) { port_value = value.substr(separator + 1); has_port = true; }
  }
  return host(hostname) && (!required_port || has_port) && (!has_port || port(port_value));
}
void validate_endpoint(const Endpoint &value) {
  if (value.address.empty() || value.address.size() > 2048) fail();
  for (unsigned char c : value.address) if (c <= 32 || c == 127) fail();
  if (value.kind == "unix") {
    if (!canonical_path(value.address, 107)) fail();
  } else if (value.kind == "https") {
    if (!value.address.starts_with("https://") ||
        !authority(std::string_view(value.address).substr(8), false)) fail();
  } else if (value.kind == "tls") {
    if (!authority(value.address, true)) fail();
  } else fail();
}

struct Fd {
  int value = -1;
  explicit Fd(int fd = -1) : value(fd) {}
  ~Fd() { if (value >= 0) ::close(value); }
  Fd(Fd &&other) noexcept : value(std::exchange(other.value, -1)) {}
  Fd &operator=(Fd &&other) noexcept {
    if (this != &other) { if (value >= 0) ::close(value); value = std::exchange(other.value, -1); }
    return *this;
  }
  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;
};
int duplicate(int fd) {
  int result;
  do { result = ::fcntl(fd, F_DUPFD_CLOEXEC, 0); } while (result < 0 && errno == EINTR);
  if (result < 0) fail(BootstrapErrorCode::UnsafeFile);
  return result;
}
Fd open_parent(std::string_view path, std::string &leaf) {
  if (!canonical_path(path, 4096)) fail(BootstrapErrorCode::UnsafeFile);
  Fd dir(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (dir.value < 0) fail(BootstrapErrorCode::UnsafeFile);
  auto final = path.rfind('/');
  std::size_t begin = 1;
  while (begin < final) {
    auto end = path.find('/', begin);
    std::string part(path.substr(begin, end - begin));
    Fd next(::openat(dir.value, part.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (next.value < 0) fail(BootstrapErrorCode::UnsafeFile);
    dir = std::move(next);
    begin = end + 1;
  }
  leaf = std::string(path.substr(final + 1));
  return dir;
}
void private_directory(int fd) {
  struct stat s{};
  if (::fstat(fd, &s) || !S_ISDIR(s.st_mode) || s.st_uid != ::geteuid() ||
      (s.st_mode & 07777) != 0700) fail(BootstrapErrorCode::UnsafeFile);
}
struct Secret {
  std::string value;
  Secret() = default;
  explicit Secret(std::string text) : value(std::move(text)) {}
  ~Secret() { if (!value.empty()) OPENSSL_cleanse(value.data(), value.size()); }
  Secret(Secret &&other) noexcept { value.swap(other.value); }
  Secret &operator=(Secret &&other) noexcept {
    if (this != &other) {
      if (!value.empty()) OPENSSL_cleanse(value.data(), value.size());
      value.clear(); value.swap(other.value);
    }
    return *this;
  }
  Secret(const Secret &) = delete;
  Secret &operator=(const Secret &) = delete;
};
Secret read_private(std::string_view path, std::size_t limit) {
  std::string leaf;
  auto parent = open_parent(path, leaf);
  private_directory(parent.value);
  Fd fd(::openat(parent.value, leaf.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  struct stat s{};
  if (fd.value < 0 || ::fstat(fd.value, &s) || !S_ISREG(s.st_mode) || s.st_uid != ::geteuid() ||
      s.st_nlink != 1 || (s.st_mode & 07777) != 0600 || s.st_size <= 0 ||
      static_cast<std::uintmax_t>(s.st_size) > limit) fail(BootstrapErrorCode::UnsafeFile);
  Secret out;
  // Allocate the complete bounded buffer once so partial secret reads never
  // survive a string reallocation. One extra byte detects concurrent growth.
  out.value.resize(limit + 1);
  std::size_t used = 0;
  while (used < out.value.size()) {
    auto n = ::read(fd.value, out.value.data() + used, out.value.size() - used);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) fail(BootstrapErrorCode::UnsafeFile);
    if (!n) break;
    used += static_cast<std::size_t>(n);
  }
  if (!used || used > limit) fail(BootstrapErrorCode::UnsafeFile);
  out.value.resize(used);
  return out;
}
using X509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using BioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
bool pem_body(std::string_view value) {
  if (value.empty()) return false;
  for (unsigned char c : value)
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=' || whitespace(c))) return false;
  return true;
}
std::vector<X509Ptr> certificates(std::string_view value) {
  std::vector<X509Ptr> result;
  constexpr std::string_view begin = "-----BEGIN CERTIFICATE-----", end = "-----END CERTIFICATE-----";
  value = trim(value);
  while (!value.empty()) {
    if (!value.starts_with(begin)) fail(BootstrapErrorCode::InvalidCredentials);
    auto finish = value.find(end, begin.size());
    if (finish == value.npos || !pem_body(value.substr(begin.size(), finish - begin.size())))
      fail(BootstrapErrorCode::InvalidCredentials);
    auto block = value.substr(0, finish + end.size());
    BioPtr bio(BIO_new_mem_buf(block.data(), static_cast<int>(block.size())), BIO_free);
    X509Ptr cert(bio ? PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr) : nullptr, X509_free);
    if (!cert) { ERR_clear_error(); fail(BootstrapErrorCode::InvalidCredentials); }
    result.push_back(std::move(cert));
    value = trim(value.substr(block.size()));
  }
  if (result.empty()) fail(BootstrapErrorCode::InvalidCredentials);
  return result;
}
int no_password(char *, int, int, void *) { return 0; }
void identity(std::string_view cert_pem, std::string_view key_pem) {
  auto chain = certificates(cert_pem);
  auto key_text = trim(key_pem);
  std::string_view label;
  for (auto candidate : {std::string_view("PRIVATE KEY"), std::string_view("RSA PRIVATE KEY"),
                         std::string_view("EC PRIVATE KEY"), std::string_view("DSA PRIVATE KEY")}) {
    auto prefix = "-----BEGIN " + std::string(candidate) + "-----";
    if (key_text.starts_with(prefix)) { label = candidate; break; }
  }
  if (label.empty()) fail(BootstrapErrorCode::InvalidCredentials);
  auto prefix = "-----BEGIN " + std::string(label) + "-----";
  auto suffix = "-----END " + std::string(label) + "-----";
  auto end = key_text.find(suffix, prefix.size());
  if (end == key_text.npos || end + suffix.size() != key_text.size() ||
      !pem_body(key_text.substr(prefix.size(), end - prefix.size()))) fail(BootstrapErrorCode::InvalidCredentials);
  BioPtr bio(BIO_new_mem_buf(key_text.data(), static_cast<int>(key_text.size())), BIO_free);
  KeyPtr key(bio ? PEM_read_bio_PrivateKey(bio.get(), nullptr, no_password, nullptr) : nullptr, EVP_PKEY_free);
  KeyPtr public_key(X509_get_pubkey(chain.front().get()), EVP_PKEY_free);
  if (!key || !public_key) { ERR_clear_error(); fail(BootstrapErrorCode::InvalidCredentials); }
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  auto equal = EVP_PKEY_eq(key.get(), public_key.get());
#else
  auto equal = EVP_PKEY_cmp(key.get(), public_key.get());
#endif
  if (equal != 1) fail(BootstrapErrorCode::InvalidCredentials);
}
bool bearer(std::string_view token) {
  if (token.empty() || token.size() > 1024) return false;
  std::size_t body = 0;
  bool padding = false;
  for (unsigned char c : token) {
    if (c == '=') { padding = true; continue; }
    if (padding || !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '~' ||
                    c == '+' || c == '/' || c == '-')) return false;
    ++body;
  }
  return body != 0;
}
bool authorization_name(std::string_view name) {
  constexpr std::string_view expected = "authorization";
  if (name.size() != expected.size()) return false;
  for (std::size_t i = 0; i < name.size(); ++i) {
    auto c = name[i];
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + ('a' - 'A'));
    if (c != expected[i]) return false;
  }
  return true;
}
} // namespace

BootstrapError::BootstrapError(BootstrapErrorCode value)
    : std::runtime_error("xrpc: explicit bootstrap input unavailable or invalid"), code(value) {}
bool OpaqueGrantHandle::matches(std::string_view owner_name) const noexcept { return name_ == owner_name; }
namespace detail {
struct BootstrapAccess {
  static OpaqueGrantHandle handle(GrantPurpose purpose, std::string value) {
    OpaqueGrantHandle result; result.purpose_ = purpose; result.name_ = std::move(value); return result;
  }
  static BootstrapBinding binding(const Json &value) {
    fields(value, {"schema_version", "target_id", "service", "api_version", "profile", "endpoint",
                   "runtime_grant", "authentication", "secret_handles", "storage_grants"});
    version(value);
    BootstrapBinding out;
    out.target_id_ = name(get(value, "target_id"));
    out.service_ = name(get(value, "service"));
    out.api_version_ = name(get(value, "api_version"));
    out.profile_ = text(get(value, "profile"));
    if (out.profile_ != "http.v1" && out.profile_ != "grpc.v1") fail();
    const auto &ep = get(value, "endpoint");
    fields(ep, {"kind", "address"});
    out.endpoint_ = {text(get(ep, "kind")), text(get(ep, "address"))};
    validate_endpoint(out.endpoint_);
    if ((out.profile_ == "http.v1" && out.endpoint_.kind == "tls") ||
        (out.profile_ == "grpc.v1" && out.endpoint_.kind == "https")) fail();
    out.runtime_ = handle(GrantPurpose::Runtime, name(get(value, "runtime_grant")));
    out.authentication_ = text(get(value, "authentication"));
    const auto &secret = get(value, "secret_handles");
    fields(secret, {}, {"tls_identity", "tls_trust", "authorization"});
    std::set<std::string> seen;
    auto optional_handle = [&](const char *key, GrantPurpose purpose) -> std::optional<OpaqueGrantHandle> {
      auto at = secret.object.find(key);
      if (at == secret.object.end()) return std::nullopt;
      auto value = name(at->second);
      if (!seen.insert(value).second) fail();
      return handle(purpose, std::move(value));
    };
    out.identity_ = optional_handle("tls_identity", GrantPurpose::TlsIdentity);
    out.trust_ = optional_handle("tls_trust", GrantPurpose::TlsTrust);
    out.authorization_ = optional_handle("authorization", GrantPurpose::Authorization);
    if (out.endpoint_.kind == "unix") {
      if (out.authentication_ != "local_private") fail();
    } else if ((out.authentication_ != "server_tls" && out.authentication_ != "mutual_tls") ||
               !out.identity_ || !out.trust_ || !out.authorization_) fail();
    const auto &storage = get(value, "storage_grants");
    if (storage.kind != Json::Kind::Array || storage.array.size() > 32) fail();
    seen.clear();
    for (const auto &item : storage.array) {
      auto value = name(item);
      if (!seen.insert(value).second) fail();
      out.storage_.push_back(handle(GrantPurpose::Storage, std::move(value)));
    }
    return out;
  }
  static const auto &identity_handle(const BootstrapBinding &b) { return b.identity_; }
  static const auto &trust_handle(const BootstrapBinding &b) { return b.trust_; }
  static const auto &authorization_handle(const BootstrapBinding &b) { return b.authorization_; }
  static const std::string &handle_name(const OpaqueGrantHandle &h) { return h.name_; }
  static int directory_fd(const DirectoryGrant &value);
};
} // namespace detail
BootstrapBinding parseBootstrapBinding(std::string_view json) {
  return detail::BootstrapAccess::binding(JsonReader(json).read());
}
ServiceRef BootstrapBinding::service_ref(std::string_view instance_id) const {
  if (!identifier(instance_id) || !identifier(target_id_) || !identifier(service_) ||
      !identifier(api_version_) || !identifier(detail::BootstrapAccess::handle_name(runtime_)) ||
      (profile_ != "http.v1" && profile_ != "grpc.v1")) fail();
  validate_endpoint(endpoint_);
  return {target_id_, service_, api_version_, std::string(instance_id), profile_, endpoint_};
}
void BootstrapBinding::check_reference(const ServiceRef &reference) const {
  auto expected = service_ref(reference.instance_id);
  if (reference.target_id != expected.target_id || reference.service != expected.service ||
      reference.api_version != expected.api_version || reference.profile != expected.profile ||
      reference.endpoint.kind != expected.endpoint.kind || reference.endpoint.address != expected.endpoint.address)
    fail(BootstrapErrorCode::GrantMismatch);
}
struct DirectoryGrant::Impl {
  Fd fd;
  GrantPurpose purpose;
  Impl(Fd value, GrantPurpose p) : fd(std::move(value)), purpose(p) {}
};
DirectoryGrant DirectoryGrant::from_owned_directory(int borrowed_fd, GrantPurpose purpose) {
  if (purpose != GrantPurpose::Runtime && purpose != GrantPurpose::Storage) fail(BootstrapErrorCode::GrantMismatch);
  Fd fd(duplicate(borrowed_fd));
  struct stat s{};
  if (::fstat(fd.value, &s) || !S_ISDIR(s.st_mode) || s.st_uid != ::geteuid()) fail(BootstrapErrorCode::UnsafeFile);
  if (purpose == GrantPurpose::Runtime) private_directory(fd.value);
  return DirectoryGrant(std::make_shared<Impl>(std::move(fd), purpose));
}
GrantPurpose DirectoryGrant::purpose() const noexcept { return impl_ ? impl_->purpose : GrantPurpose::Invalid; }
int DirectoryGrant::duplicate_fd() const {
  if (!impl_) fail(BootstrapErrorCode::UnresolvedGrant);
  return duplicate(impl_->fd.value);
}
int detail::BootstrapAccess::directory_fd(const DirectoryGrant &value) {
  if (!value.impl_) fail(BootstrapErrorCode::UnresolvedGrant);
  return value.impl_->fd.value;
}

namespace {
struct Credential {
  GrantPurpose purpose;
  Secret cert, key, ca, authorization;
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
  explicit Credential(GrantPurpose p) : purpose(p) {}
  ~Credential() { OPENSSL_cleanse(digest.data(), digest.size()); }
  Credential(Credential &&) noexcept = default;
  Credential &operator=(Credential &&) noexcept = default;
  Credential(const Credential &) = delete;
};
using Credentials = std::map<std::string, Credential>;
Credential credential(const Json &value) {
  if (value.kind != Json::Kind::Object) fail();
  const auto &kind = text(get(value, "kind"));
  if (kind == "tls_identity") {
    fields(value, {"kind", "cert_file", "key_file"});
    Credential out(GrantPurpose::TlsIdentity);
    out.cert = read_private(text(get(value, "cert_file")), 128 * 1024);
    out.key = read_private(text(get(value, "key_file")), 64 * 1024);
    identity(out.cert.value, out.key.value);
    return out;
  }
  if (kind == "tls_trust") {
    fields(value, {"kind", "ca_file"});
    Credential out(GrantPurpose::TlsTrust);
    out.ca = read_private(text(get(value, "ca_file")), 128 * 1024);
    (void)certificates(out.ca.value);
    return out;
  }
  if (kind == "bearer") {
    fields(value, {"kind", "token_file"});
    Credential out(GrantPurpose::Authorization);
    auto token = read_private(text(get(value, "token_file")), 1024);
    if (!bearer(token.value)) fail(BootstrapErrorCode::InvalidCredentials);
    out.authorization.value.reserve(7 + token.value.size());
    out.authorization.value = "Bearer ";
    out.authorization.value += token.value;
    SHA256(reinterpret_cast<const unsigned char *>(out.authorization.value.data()),
           out.authorization.value.size(), out.digest.data());
    return out;
  }
  fail();
}
const Credential *resolve(const Credentials &grants, const std::optional<OpaqueGrantHandle> &handle) {
  if (!handle) return nullptr;
  auto found = grants.find(detail::BootstrapAccess::handle_name(*handle));
  if (found == grants.end()) fail(BootstrapErrorCode::UnresolvedGrant);
  if (found->second.purpose != handle->purpose()) fail(BootstrapErrorCode::GrantMismatch);
  return &found->second;
}
} // namespace
struct BootstrapInput::Impl {
  BootstrapBinding binding;
  BootstrapRole role;
  Credentials credentials;
  std::optional<std::string> application;
  const Credential *identity = nullptr, *trust = nullptr, *authorization = nullptr;
  mutable std::mutex owner_mutex;
  mutable bool runtime_attempted = false, storage_attempted = false;
  mutable std::optional<DirectoryGrant> runtime;
  mutable std::optional<std::vector<DirectoryGrant>> storage;
};
BootstrapInput::BootstrapInput(std::unique_ptr<Impl> value) : impl_(std::move(value)) {}
BootstrapInput::BootstrapInput(BootstrapInput &&) noexcept = default;
BootstrapInput &BootstrapInput::operator=(BootstrapInput &&) noexcept = default;
BootstrapInput::~BootstrapInput() = default;
const BootstrapBinding &BootstrapInput::binding() const noexcept { return impl_->binding; }
std::optional<std::string_view> BootstrapInput::application_json() const noexcept {
  if (!impl_->application) return std::nullopt;
  return *impl_->application;
}
BootstrapInput loadBootstrapInput(std::string_view path, BootstrapRole role) {
  if (role != BootstrapRole::Server && role != BootstrapRole::Client) fail();
  auto file = read_private(path, document_limit);
  auto document = JsonReader(file.value).read();
  fields(document, {"schema_version", "binding", "grants"}, {"application"});
  version(document);
  auto result = std::make_unique<BootstrapInput::Impl>();
  result->binding = detail::BootstrapAccess::binding(get(document, "binding"));
  result->role = role;
  if (auto app = document.object.find("application"); app != document.object.end()) {
    application_depth(app->second);
    result->application = std::string(app->second.raw);
  }
  const auto &grants = get(document, "grants");
  if (grants.kind != Json::Kind::Object || grants.object.size() > 32) fail();
  for (const auto &[key, value] : grants.object) {
    if (!identifier(key)) fail();
    result->credentials.emplace(key, credential(value));
  }
  result->identity = resolve(result->credentials, detail::BootstrapAccess::identity_handle(result->binding));
  result->trust = resolve(result->credentials, detail::BootstrapAccess::trust_handle(result->binding));
  result->authorization = resolve(result->credentials, detail::BootstrapAccess::authorization_handle(result->binding));
  return BootstrapInput(std::move(result));
}
DirectoryGrant BootstrapInput::resolve_runtime(const OwnerGrantResolver &owner) const {
  std::lock_guard lock(impl_->owner_mutex);
  if (impl_->runtime) {
    private_directory(detail::BootstrapAccess::directory_fd(*impl_->runtime));
    return *impl_->runtime;
  }
  if (impl_->runtime_attempted || !owner) fail(BootstrapErrorCode::UnresolvedGrant);
  impl_->runtime_attempted = true;
  try {
    auto grant = owner(impl_->binding.runtime_grant(), impl_->binding);
    if (grant.purpose() != GrantPurpose::Runtime) fail(BootstrapErrorCode::GrantMismatch);
    auto fd = detail::BootstrapAccess::directory_fd(grant);
    private_directory(fd);
    if (impl_->binding.endpoint().kind == "unix") {
      std::string leaf;
      auto parent = open_parent(impl_->binding.endpoint().address, leaf);
      private_directory(parent.value);
      struct stat a{}, b{};
      if (::fstat(fd, &a) || ::fstat(parent.value, &b) || a.st_dev != b.st_dev || a.st_ino != b.st_ino)
        fail(BootstrapErrorCode::GrantMismatch);
    }
    impl_->runtime = grant;
    return grant;
  } catch (const BootstrapError &) { throw; }
    catch (...) { fail(BootstrapErrorCode::UnresolvedGrant); }
}
std::vector<DirectoryGrant> BootstrapInput::resolve_storage(const OwnerGrantResolver &owner) const {
  std::lock_guard lock(impl_->owner_mutex);
  if (impl_->storage) {
    for (const auto &grant : *impl_->storage) {
      struct stat s{};
      if (::fstat(detail::BootstrapAccess::directory_fd(grant), &s) || !S_ISDIR(s.st_mode) || s.st_uid != ::geteuid())
        fail(BootstrapErrorCode::UnsafeFile);
    }
    return *impl_->storage;
  }
  if (impl_->storage_attempted || (!owner && !impl_->binding.storage_grants().empty())) fail(BootstrapErrorCode::UnresolvedGrant);
  impl_->storage_attempted = true;
  try {
    std::vector<DirectoryGrant> grants;
    grants.reserve(impl_->binding.storage_grants().size());
    for (const auto &handle : impl_->binding.storage_grants()) {
      auto grant = owner(handle, impl_->binding);
      if (grant.purpose() != GrantPurpose::Storage) fail(BootstrapErrorCode::GrantMismatch);
      struct stat s{};
      if (::fstat(detail::BootstrapAccess::directory_fd(grant), &s) || !S_ISDIR(s.st_mode) || s.st_uid != ::geteuid())
        fail(BootstrapErrorCode::UnsafeFile);
      grants.push_back(std::move(grant));
    }
    impl_->storage = grants;
    return grants;
  } catch (const BootstrapError &) { throw; }
    catch (...) { fail(BootstrapErrorCode::UnresolvedGrant); }
}
void BootstrapInput::consume_tls(const std::function<void(std::string_view, std::string_view, std::string_view)> &native) const {
  if (!native || !impl_->identity || !impl_->trust) fail(BootstrapErrorCode::UnresolvedGrant);
  native(impl_->identity->cert.value, impl_->identity->key.value, impl_->trust->ca.value);
}
void BootstrapInput::apply_authorization(std::vector<std::pair<std::string, std::string>> &headers) const {
  if (impl_->role != BootstrapRole::Client) fail(BootstrapErrorCode::GrantMismatch);
  if (!impl_->authorization) return;
  for (const auto &[name, value] : headers) {
    (void)value;
    if (authorization_name(name)) fail(BootstrapErrorCode::GrantMismatch);
  }
  headers.emplace_back("Authorization", impl_->authorization->authorization.value);
}
bool BootstrapInput::authorize(std::span<const std::string_view> values,
                              std::chrono::steady_clock::time_point deadline) const noexcept {
  if (!impl_ || impl_->role != BootstrapRole::Server ||
      deadline == std::chrono::steady_clock::time_point::max() ||
      std::chrono::steady_clock::now() >= deadline) return false;
  if (!impl_->authorization) return true;
  if (values.size() != 1 || values.front().size() > 1031) return false;
  std::array<unsigned char, SHA256_DIGEST_LENGTH> actual{};
  SHA256(reinterpret_cast<const unsigned char *>(values.front().data()), values.front().size(), actual.data());
  return CRYPTO_memcmp(actual.data(), impl_->authorization->digest.data(), actual.size()) == 0 &&
         std::chrono::steady_clock::now() < deadline;
}
} // namespace xgc2::xrpc
