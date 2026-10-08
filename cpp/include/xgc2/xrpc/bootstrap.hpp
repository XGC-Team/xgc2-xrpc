#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace xgc2::xrpc {
enum class BootstrapRole { Server, Client };
enum class GrantPurpose { Runtime, Storage, TlsIdentity, TlsTrust, Authorization, Invalid };
enum class BootstrapErrorCode { InvalidInput, UnsafeFile, InvalidCredentials,
                                UnresolvedGrant, GrantMismatch };
class BootstrapError : public std::runtime_error {
public:
  explicit BootstrapError(BootstrapErrorCode code);
  const BootstrapErrorCode code;
};
namespace detail { struct BootstrapAccess; }

// Handles have a purpose, but no path/material/string/stream conversion.
class OpaqueGrantHandle {
public:
  GrantPurpose purpose() const noexcept { return purpose_; }
  bool matches(std::string_view owner_name) const noexcept;
private:
  GrantPurpose purpose_ = GrantPurpose::Runtime;
  std::string name_;
  friend struct detail::BootstrapAccess;
};
struct Endpoint { std::string kind, address; };
struct ServiceRef {
  std::string target_id, service, api_version, instance_id, profile;
  Endpoint endpoint;
};
class BootstrapBinding {
public:
  std::string_view target_id() const noexcept { return target_id_; }
  std::string_view service() const noexcept { return service_; }
  std::string_view api_version() const noexcept { return api_version_; }
  std::string_view profile() const noexcept { return profile_; }
  std::string_view authentication() const noexcept { return authentication_; }
  const Endpoint &endpoint() const noexcept { return endpoint_; }
  const OpaqueGrantHandle &runtime_grant() const noexcept { return runtime_; }
  std::span<const OpaqueGrantHandle> storage_grants() const noexcept { return storage_; }
  ServiceRef service_ref(std::string_view instance_id) const;
  // Checks binding metadata and instance syntax. The transport owner must also
  // compare the response's instance metadata with the expected ServiceRef;
  // persisted bootstrap metadata cannot determine the currently live instance.
  void check_reference(const ServiceRef &reference) const;
private:
  std::string target_id_, service_, api_version_, profile_, authentication_;
  Endpoint endpoint_;
  OpaqueGrantHandle runtime_;
  std::vector<OpaqueGrantHandle> storage_;
  std::optional<OpaqueGrantHandle> identity_, trust_, authorization_;
  friend struct detail::BootstrapAccess;
};
using Binding = BootstrapBinding;
BootstrapBinding parseBootstrapBinding(std::string_view json);

// Pins an existing process/file-owner allocation. Never creates, chmods or
// removes directories. Runtime requires owned mode0700; storage preserves its
// owner's permissions. Every duplicate_fd result belongs to its caller.
class DirectoryGrant {
public:
  static DirectoryGrant from_owned_directory(int borrowed_fd, GrantPurpose purpose);
  GrantPurpose purpose() const noexcept;
  int duplicate_fd() const;
private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  explicit DirectoryGrant(std::shared_ptr<Impl> value) : impl_(std::move(value)) {}
  friend struct detail::BootstrapAccess;
};
// Startup resolver callbacks must not reenter grant resolution on this input.
using OwnerGrantResolver = std::function<DirectoryGrant(
    const OpaqueGrantHandle &, const BootstrapBinding &)>;

class BootstrapInput {
public:
  // A moved-from input supports destruction or move assignment only;
  // authorize safely returns false for it.
  BootstrapInput(BootstrapInput &&) noexcept;
  BootstrapInput &operator=(BootstrapInput &&) noexcept;
  ~BootstrapInput();
  BootstrapInput(const BootstrapInput &) = delete;
  BootstrapInput &operator=(const BootstrapInput &) = delete;
  const BootstrapBinding &binding() const noexcept;
  std::optional<std::string_view> application_json() const noexcept;
  // Resolve once at startup, before bind/dial. Runtime Unix resolution checks
  // the retained grant against the endpoint's parent inode. Pass duplicate_fd
  // to the transport's retained-parent overload; do not reopen the pathname.
  DirectoryGrant resolve_runtime(const OwnerGrantResolver &owner) const;
  std::vector<DirectoryGrant> resolve_storage(const OwnerGrantResolver &owner) const;
  // Explicit native credential consumption, never an effective-policy/log
  // getter. Callback views last only for this invocation; no implicit TLS dial.
  // The native adapter must enforce TLS >=1.2, trust/hostname verification and
  // peer authentication required by binding().authentication().
  void consume_tls(const std::function<void(std::string_view cert_pem,
                   std::string_view key_pem, std::string_view ca_pem)> &native) const;
  void apply_authorization(std::vector<std::pair<std::string, std::string>> &headers) const;
  bool authorize(std::span<const std::string_view> authorization_values,
                 std::chrono::steady_clock::time_point deadline) const noexcept;
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  explicit BootstrapInput(std::unique_ptr<Impl> value);
  friend BootstrapInput loadBootstrapInput(std::string_view, BootstrapRole);
};
BootstrapInput loadBootstrapInput(std::string_view explicit_path,
                                 BootstrapRole role = BootstrapRole::Server);
} // namespace xgc2::xrpc
