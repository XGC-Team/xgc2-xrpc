#include "xgc2/xrpc/bootstrap.hpp"
#include <boost/property_tree/json_parser.hpp>
#include <array>
#include <cassert>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509v3.h>

using namespace xgc2::xrpc;
using namespace std::chrono_literals;

struct Directory {
  std::string path;
  Directory() {
    char name[] = "/tmp/xrpc-bootstrap-XXXXXX";
    const auto result = ::mkdtemp(name);
    assert(result);
    path = result;
  }
  ~Directory() { std::filesystem::remove_all(path); }
  std::string file(const char* name) const { return path + "/" + name; }
};
struct Fd {
  int value;
  explicit Fd(int fd) : value(fd) { assert(fd >= 0); }
  ~Fd() { if (value >= 0) ::close(value); }
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;
  void close() { assert(::close(value) == 0); value = -1; }
};
template<class F> void expect_error(BootstrapErrorCode expected, F&& operation) {
  bool rejected = false;
  try { operation(); }
  catch (const BootstrapError& error) {
    assert(error.code == expected);
    rejected = true;
  }
  assert(rejected);
}
void write_private(const std::string& path, std::string_view bytes) {
  Fd fd(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600));
  assert(::fchmod(fd.value, 0600) == 0);
  while (!bytes.empty()) {
    const auto count = ::write(fd.value, bytes.data(), bytes.size());
    if (count < 0 && errno == EINTR) continue;
    assert(count > 0);
    bytes.remove_prefix(static_cast<std::size_t>(count));
  }
}
std::string read_file(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  assert(input.good());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
struct stat info(const std::string& path) {
  struct stat result{};
  assert(::lstat(path.c_str(), &result) == 0);
  return result;
}
void same_inode(const struct stat& a, const struct stat& b) {
  assert(a.st_dev == b.st_dev && a.st_ino == b.st_ino);
}
std::size_t descriptor_count() {
  std::size_t count = 0;
  for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
    (void)entry;
    ++count;
  }
  return count;
}
std::string quote(std::string_view value) {
  std::string out = "\"";
  for (const unsigned char c : value) {
    assert(c >= 32);
    if (c == '\\' || c == '"') out += '\\';
    out += static_cast<char>(c);
  }
  return out + '"';
}
std::string local_binding(const std::string& parent, bool storage = false) {
  return R"({"schema_version":1,"target_id":"fixture:target","service":"fixture","api_version":"v1","profile":"http.v1","endpoint":{"kind":"unix","address":)" +
      quote(parent + "/rpc.sock") +
      R"(},"runtime_grant":"owner.runtime","authentication":"local_private","secret_handles":{},"storage_grants":)" +
      (storage ? R"(["owner.state"]})" : "[]}");
}
std::string document(std::string_view binding, std::string_view grants = "{}",
                     std::string_view application = {}) {
  std::string out = R"({"schema_version":1,"binding":)" + std::string(binding) +
      R"(,"grants":)" + std::string(grants);
  if (!application.empty()) out += R"(,"application":)" + std::string(application);
  return out + '}';
}
std::string replaced(std::string text, std::string_view before, std::string_view after) {
  const auto at = text.find(before);
  assert(at != text.npos);
  text.replace(at, before.size(), after);
  return text;
}

// Slice only the trusted shared fixture. Property_tree reads case metadata;
// binding bytes never pass through its serializer, which would erase types.
std::size_t balanced_end(std::string_view text, std::size_t start) {
  assert(text[start] == '{' || text[start] == '[');
  unsigned depth = 0;
  bool string = false, escaped = false;
  for (auto at = start; at < text.size(); ++at) {
    const auto c = text[at];
    if (string) {
      if (escaped) escaped = false;
      else if (c == '\\') escaped = true;
      else if (c == '"') string = false;
    } else if (c == '"') string = true;
    else if (c == '{' || c == '[') ++depth;
    else if ((c == '}' || c == ']') && --depth == 0) return at + 1;
  }
  assert(false);
  return text.size();
}
std::size_t skip_space(std::string_view text, std::size_t at) {
  while (at < text.size() && (text[at] == ' ' || text[at] == '\n' ||
      text[at] == '\r' || text[at] == '\t')) ++at;
  return at;
}
void shared_binding_cases() {
  const auto fixture = read_file(XRPC_BOOTSTRAP_FIXTURE);
  const auto marker = fixture.find("\"cases\"");
  assert(marker != fixture.npos);
  auto at = fixture.find('[', marker) + 1;
  unsigned cases = 0;
  for (;;) {
    at = skip_space(fixture, at);
    if (fixture[at] == ']') break;
    if (fixture[at] == ',') { ++at; continue; }
    const auto end = balanced_end(fixture, at);
    const std::string raw_case = fixture.substr(at, end - at);
    boost::property_tree::ptree metadata;
    std::istringstream stream(raw_case);
    boost::property_tree::read_json(stream, metadata);
    const auto name = metadata.get<std::string>("name");
    const bool expected = metadata.get<bool>("valid");
    const auto binding_key = raw_case.find("\"binding\"");
    assert(binding_key != raw_case.npos);
    const auto begin = skip_space(raw_case, raw_case.find(':', binding_key) + 1);
    const auto binding_end = balanced_end(raw_case, begin);
    const auto raw_binding = std::string_view(raw_case).substr(begin, binding_end - begin);
    bool accepted = false;
    try {
      const auto binding = parseBootstrapBinding(raw_binding);
      assert(binding.target_id() == "fixture:target" && binding.service() == "fixture");
      auto reference = binding.service_ref("fixture:live-1");
      binding.check_reference(reference);
      reference.target_id = "other:target";
      expect_error(BootstrapErrorCode::GrantMismatch, [&] { binding.check_reference(reference); });
      accepted = true;
    } catch (const BootstrapError& error) { assert(error.code == BootstrapErrorCode::InvalidInput); }
    if (accepted != expected) std::cerr << "bootstrap fixture mismatch: " << name << '\n';
    assert(accepted == expected);
    ++cases;
    at = end;
  }
  assert(cases == 16);
}

void private_input_and_application() {
  Directory directory;
  const auto path = directory.file("input.json");
  const auto binding = local_binding(directory.path);
  const auto input = document(binding);
  write_private(path, input);
  assert((info(directory.path).st_mode & 07777) == 0700);
  assert((info(path).st_mode & 07777) == 0600 && info(path).st_uid == ::geteuid());
  {
    auto loaded = loadBootstrapInput(path);
    assert(loaded.binding().runtime_grant().purpose() == GrantPurpose::Runtime);
    assert(loaded.binding().runtime_grant().matches("owner.runtime"));
    assert(!loaded.application_json() && loaded.resolve_storage({}).empty());
    assert(loaded.resolve_storage({}).empty());
    assert(loaded.authorize({}, std::chrono::steady_clock::now() + 1s));
    assert(!loaded.authorize({}, std::chrono::steady_clock::time_point::max()));
    assert(!loaded.authorize({}, std::chrono::steady_clock::now() - 1ms));
    expect_error(BootstrapErrorCode::UnresolvedGrant, [&] { loaded.consume_tls({}); });
    auto moved = std::move(loaded);
    assert(!loaded.authorize({}, std::chrono::steady_clock::now() + 1s));
    assert(moved.authorize({}, std::chrono::steady_clock::now() + 1s));
  }
  assert(::chmod(path.c_str(), 0644) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(path); });
  assert(::chmod(path.c_str(), 0600) == 0);
  assert(::chmod(directory.path.c_str(), 0750) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(path); });
  assert(::chmod(directory.path.c_str(), 0700) == 0);
  const auto link = directory.file("symlink");
  assert(::symlink("input.json", link.c_str()) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(link); });
  const auto parent_link = directory.file("parent-link");
  assert(::symlink(directory.path.c_str(), parent_link.c_str()) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(parent_link + "/input.json"); });
  const auto hard = directory.file("hardlink");
  assert(::link(path.c_str(), hard.c_str()) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(hard); });
  assert(::unlink(hard.c_str()) == 0);
  const auto fifo = directory.file("fifo");
  assert(::mkfifo(fifo.c_str(), 0600) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(fifo); });
  const auto non_file = directory.file("directory");
  assert(::mkdir(non_file.c_str(), 0700) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(non_file); });
  write_private(path, std::string(16 * 1024 + 1, ' '));
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(path); });

  const std::string opaque = R"({"text":"机器人 \uD83D\uDE80","number":1.25,"yes":true,"none":null,"items":[false,2,"s"]})";
  write_private(path, document(binding, "{}", opaque));
  assert(loadBootstrapInput(path).application_json() == opaque);
  const auto nested = [](unsigned count) { return std::string(count, '[') + "true" + std::string(count, ']'); };
  write_private(path, document(binding, "{}", nested(32)));
  assert(loadBootstrapInput(path).application_json() == nested(32));
  write_private(path, document(binding, "{}", nested(33)));
  expect_error(BootstrapErrorCode::InvalidInput, [&] { (void)loadBootstrapInput(path); });
  write_private(path, document(binding, "{}", R"({"x":true,"\u0078":false})"));
  expect_error(BootstrapErrorCode::InvalidInput, [&] { (void)loadBootstrapInput(path); });
  std::string invalid_utf8 = "\"";
  invalid_utf8 += static_cast<char>(0xc0); invalid_utf8 += static_cast<char>(0xaf); invalid_utf8 += '"';
  write_private(path, document(binding, "{}", invalid_utf8));
  expect_error(BootstrapErrorCode::InvalidInput, [&] { (void)loadBootstrapInput(path); });
  expect_error(BootstrapErrorCode::InvalidInput, [&] {
    (void)parseBootstrapBinding(replaced(binding, R"("schema_version":1)", R"("schema_version":"1")"));
  });
  expect_error(BootstrapErrorCode::InvalidInput, [&] {
    (void)parseBootstrapBinding(replaced(binding, R"("target_id":"fixture:target")",
        R"("target_id":"fixture:target","target_id":"other")"));
  });
}

void owned_directory_resolution() {
  Directory owner;
  const auto runtime_path = owner.file("runtime"), storage_path = owner.file("storage");
  assert(::mkdir(runtime_path.c_str(), 0700) == 0);
  assert(::mkdir(storage_path.c_str(), 0750) == 0);
  assert(::chmod(storage_path.c_str(), 0750) == 0);
  const auto marker = storage_path + "/persistent";
  write_private(marker, "file-owner data");
  const auto storage_before = info(storage_path), marker_before = info(marker);
  const auto path = owner.file("input.json");
  write_private(path, document(local_binding(runtime_path, true)));
  const auto fds_before = descriptor_count();
  {
    Fd runtime_fd(::open(runtime_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    Fd storage_fd(::open(storage_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    auto runtime = DirectoryGrant::from_owned_directory(runtime_fd.value, GrantPurpose::Runtime);
    auto storage = DirectoryGrant::from_owned_directory(storage_fd.value, GrantPurpose::Storage);
    expect_error(BootstrapErrorCode::UnsafeFile, [&] {
      (void)DirectoryGrant::from_owned_directory(storage_fd.value, GrantPurpose::Runtime);
    });
    runtime_fd.close(); storage_fd.close();
    unsigned runtime_calls = 0, storage_calls = 0;
    auto loaded = loadBootstrapInput(path);
    const OwnerGrantResolver resolver = [&](const OpaqueGrantHandle& handle, const BootstrapBinding& binding) {
      assert(binding.target_id() == "fixture:target");
      if (handle.purpose() == GrantPurpose::Runtime) {
        assert(handle.matches("owner.runtime")); ++runtime_calls; return runtime;
      }
      assert(handle.purpose() == GrantPurpose::Storage && handle.matches("owner.state"));
      ++storage_calls; return storage;
    };
    auto pinned = loaded.resolve_runtime(resolver);
    Fd duplicate(pinned.duplicate_fd());
    assert((::fcntl(duplicate.value, F_GETFD) & FD_CLOEXEC) != 0);
    struct stat actual{}; assert(::fstat(duplicate.value, &actual) == 0);
    same_inode(actual, info(runtime_path));
    (void)loaded.resolve_runtime(resolver);
    auto storage_grants = loaded.resolve_storage(resolver);
    assert(storage_grants.size() == 1 && storage_grants.front().purpose() == GrantPurpose::Storage);
    (void)loaded.resolve_storage(resolver);
    assert(runtime_calls == 1 && storage_calls == 1);
    assert(::chmod(runtime_path.c_str(), 0750) == 0);
    expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loaded.resolve_runtime(resolver); });
    assert(runtime_calls == 1);
    assert(::chmod(runtime_path.c_str(), 0700) == 0);
    auto wrong_purpose = loadBootstrapInput(path);
    unsigned wrong_calls = 0;
    expect_error(BootstrapErrorCode::GrantMismatch, [&] {
      (void)wrong_purpose.resolve_runtime([&](const auto&, const auto&) { ++wrong_calls; return storage; });
    });
    expect_error(BootstrapErrorCode::UnresolvedGrant, [&] {
      (void)wrong_purpose.resolve_runtime([&](const auto&, const auto&) { ++wrong_calls; return runtime; });
    });
    assert(wrong_calls == 1);
    auto wrong_storage = loadBootstrapInput(path);
    unsigned wrong_storage_calls = 0;
    expect_error(BootstrapErrorCode::GrantMismatch, [&] {
      (void)wrong_storage.resolve_storage([&](const auto&, const auto&) { ++wrong_storage_calls; return runtime; });
    });
    expect_error(BootstrapErrorCode::UnresolvedGrant, [&] {
      (void)wrong_storage.resolve_storage([&](const auto&, const auto&) { ++wrong_storage_calls; return storage; });
    });
    assert(wrong_storage_calls == 1);
    Directory different;
    Fd different_fd(::open(different.path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    auto different_grant = DirectoryGrant::from_owned_directory(different_fd.value, GrantPurpose::Runtime);
    auto wrong_inode = loadBootstrapInput(path);
    expect_error(BootstrapErrorCode::GrantMismatch, [&] {
      (void)wrong_inode.resolve_runtime([&](const auto&, const auto&) { return different_grant; });
    });
    auto moved_from = DirectoryGrant::from_owned_directory(different_fd.value, GrantPurpose::Runtime);
    auto live_grant = std::move(moved_from);
    assert(live_grant.purpose() == GrantPurpose::Runtime && moved_from.purpose() == GrantPurpose::Invalid);
    expect_error(BootstrapErrorCode::UnresolvedGrant, [&] { (void)moved_from.duplicate_fd(); });
    auto rejected_moved = loadBootstrapInput(path);
    expect_error(BootstrapErrorCode::GrantMismatch, [&] {
      (void)rejected_moved.resolve_runtime([&](const auto&, const auto&) { return moved_from; });
    });
  }
  assert(descriptor_count() == fds_before);
  same_inode(storage_before, info(storage_path));
  assert((info(storage_path).st_mode & 07777) == 0750);
  same_inode(marker_before, info(marker));
  assert(read_file(marker) == "file-owner data");
  // A caller's duplicate remains its own even after the final grant is gone.
  int caller_owned = -1;
  {
    Fd borrowed(::open(runtime_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    auto grant = DirectoryGrant::from_owned_directory(borrowed.value, GrantPurpose::Runtime);
    caller_owned = grant.duplicate_fd();
  }
  { Fd last(caller_owned); struct stat actual{}; assert(::fstat(last.value, &actual) == 0); }
  assert(descriptor_count() == fds_before);
}

struct Pem { std::string certificate, key; };
Pem ephemeral_pem(bool authority) {
  using Context = std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)>;
  using Key = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
  using Certificate = std::unique_ptr<X509, decltype(&X509_free)>;
  using Bio = std::unique_ptr<BIO, decltype(&BIO_free)>;
  using Extension = std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)>;
  Context context(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
  assert(context && EVP_PKEY_keygen_init(context.get()) == 1);
  assert(EVP_PKEY_CTX_set_rsa_keygen_bits(context.get(), 2048) == 1);
  EVP_PKEY* generated = nullptr;
  assert(EVP_PKEY_keygen(context.get(), &generated) == 1);
  Key key(generated, EVP_PKEY_free);
  Certificate cert(X509_new(), X509_free);
  assert(cert && X509_set_version(cert.get(), 2) == 1);
  assert(ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), authority ? 2 : 1) == 1);
  assert(X509_gmtime_adj(X509_get_notBefore(cert.get()), -60));
  assert(X509_gmtime_adj(X509_get_notAfter(cert.get()), 3600));
  assert(X509_set_pubkey(cert.get(), key.get()) == 1);
  auto* subject = X509_get_subject_name(cert.get());
  const auto* common_name = reinterpret_cast<const unsigned char*>(authority ? "fixture CA" : "fixture identity");
  assert(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, common_name, -1, -1, 0) == 1);
  assert(X509_set_issuer_name(cert.get(), subject) == 1);
  Extension constraints(X509V3_EXT_conf_nid(nullptr, nullptr, NID_basic_constraints,
      const_cast<char*>(authority ? "critical,CA:TRUE" : "CA:FALSE")), X509_EXTENSION_free);
  assert(constraints && X509_add_ext(cert.get(), constraints.get(), -1) == 1);
  assert(X509_sign(cert.get(), key.get(), EVP_sha256()) > 0);
  Bio certificate(BIO_new(BIO_s_mem()), BIO_free), private_key(BIO_new(BIO_s_mem()), BIO_free);
  assert(certificate && private_key);
  assert(PEM_write_bio_X509(certificate.get(), cert.get()) == 1);
  assert(PEM_write_bio_PrivateKey(private_key.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr) == 1);
  const auto bytes = [](BIO* bio) {
    char* data = nullptr;
    const auto size = BIO_get_mem_data(bio, &data);
    assert(size > 0);
    return std::string(data, static_cast<std::size_t>(size));
  };
  return {bytes(certificate.get()), bytes(private_key.get())};
}
void credentials_and_roles() {
  Directory directory;
  const auto identity = ephemeral_pem(false), ca = ephemeral_pem(true);
  const auto cert_path = directory.file("cert.pem"), key_path = directory.file("key.pem");
  const auto ca_path = directory.file("ca.pem"), token_path = directory.file("token");
  write_private(cert_path, identity.certificate); write_private(key_path, identity.key);
  write_private(ca_path, ca.certificate); write_private(token_path, "ephemeral.token==");
  const std::string binding = R"({"schema_version":1,"target_id":"fixture:target","service":"fixture","api_version":"v1","profile":"http.v1","endpoint":{"kind":"https","address":"https://localhost:8443"},"runtime_grant":"owner.runtime","authentication":"mutual_tls","secret_handles":{"tls_identity":"owner.identity","tls_trust":"owner.trust","authorization":"owner.auth"},"storage_grants":[]})";
  const auto grants = R"({"owner.identity":{"kind":"tls_identity","cert_file":)" + quote(cert_path) +
      R"(,"key_file":)" + quote(key_path) + R"(},"owner.trust":{"kind":"tls_trust","ca_file":)" + quote(ca_path) +
      R"(},"owner.auth":{"kind":"bearer","token_file":)" + quote(token_path) + "}}";
  const auto path = directory.file("input.json");
  write_private(path, document(binding, grants));
  auto server = loadBootstrapInput(path, BootstrapRole::Server);
  auto client = loadBootstrapInput(path, BootstrapRole::Client);
  unsigned native_calls = 0;
  server.consume_tls([&](auto cert, auto key, auto trust) {
    ++native_calls;
    assert(cert == identity.certificate && key == identity.key && trust == ca.certificate);
  });
  assert(native_calls == 1);
  const auto live = std::chrono::steady_clock::now() + 1s;
  const std::array<std::string_view, 1> correct{"Bearer ephemeral.token=="}, wrong{"Bearer other"};
  const std::array<std::string_view, 2> duplicate{correct.front(), correct.front()};
  assert(server.authorize(correct, live));
  assert(!server.authorize({}, live) && !server.authorize(wrong, live) && !server.authorize(duplicate, live));
  assert(!server.authorize(correct, std::chrono::steady_clock::time_point::max()));
  assert(!server.authorize(correct, std::chrono::steady_clock::now() - 1ms));
  assert(!client.authorize(correct, live));
  std::vector<std::pair<std::string, std::string>> headers;
  client.apply_authorization(headers);
  assert(headers.size() == 1 && headers.front().first == "Authorization" && headers.front().second == correct.front());
  headers.front().first = "aUtHoRiZaTiOn";
  expect_error(BootstrapErrorCode::GrantMismatch, [&] { client.apply_authorization(headers); });
  expect_error(BootstrapErrorCode::GrantMismatch, [&] { server.apply_authorization(headers); });
  write_private(key_path, ca.key);
  expect_error(BootstrapErrorCode::InvalidCredentials, [&] { (void)loadBootstrapInput(path); });
  write_private(key_path, identity.key);
  write_private(ca_path, "not a PEM certificate");
  expect_error(BootstrapErrorCode::InvalidCredentials, [&] { (void)loadBootstrapInput(path); });
  write_private(ca_path, ca.certificate);
  write_private(token_path, "ephemeral.token==\n");
  expect_error(BootstrapErrorCode::InvalidCredentials, [&] { (void)loadBootstrapInput(path); });
  assert(server.authorize(correct, std::chrono::steady_clock::now() + 1s));
  write_private(token_path, "ephemeral.token==");
  assert(::chmod(cert_path.c_str(), 0644) == 0);
  expect_error(BootstrapErrorCode::UnsafeFile, [&] { (void)loadBootstrapInput(path); });
  assert(::chmod(cert_path.c_str(), 0600) == 0);
  write_private(path, document(binding));
  expect_error(BootstrapErrorCode::UnresolvedGrant, [&] { (void)loadBootstrapInput(path); });
  const auto wrong_kind = replaced(grants, R"({"kind":"bearer","token_file":)" + quote(token_path) + "}",
      R"({"kind":"tls_trust","ca_file":)" + quote(ca_path) + "}");
  write_private(path, document(binding, wrong_kind));
  expect_error(BootstrapErrorCode::GrantMismatch, [&] { (void)loadBootstrapInput(path); });
}

int main() {
  shared_binding_cases();
  private_input_and_application();
  owned_directory_resolution();
  credentials_and_roles();
  std::cout << "bootstrap shared 16 bindings, safe input, opaque application, owned grants and explicit credentials passed\n";
}
