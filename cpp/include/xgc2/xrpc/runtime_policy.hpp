#pragma once
#include "runtime_policy_registry.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace xgc2::xrpc {
namespace detail {
// The generated JSON is the only field/default registry. The owning generator
// emits a name member for each field, so storage follows registry additions.
constexpr std::size_t runtime_policy_field_count() {
  std::size_t count = 0, position = 0;
  while ((position = runtime_policy_registry.find("\"name\"", position)) !=
         std::string_view::npos) {
    position += 6;
    auto next = position;
    while (next < runtime_policy_registry.size() &&
           (runtime_policy_registry[next] == ' ' ||
            runtime_policy_registry[next] == '\t' ||
            runtime_policy_registry[next] == '\r' ||
            runtime_policy_registry[next] == '\n'))
      ++next;
    if (next < runtime_policy_registry.size() &&
        runtime_policy_registry[next] == ':')
      ++count;
  }
  return count;
}
} // namespace detail
inline constexpr std::size_t runtime_policy_capacity =
    detail::runtime_policy_field_count();

class RuntimePolicyError : public std::runtime_error {
public:
  RuntimePolicyError(std::string field, std::string reason);
  const std::string field;
  const std::string reason;
};

// The process composition root supplies one explicit environment snapshot;
// this library never reads or watches the process environment. Defaults and
// ceilings use short registry names, while environment uses complete names.
// Capabilities are the selected owners' enforcement declaration. Unsupported
// explicit fields fail; unselected defaults are absent from the effective view.
struct RuntimePolicyOptions {
  std::vector<std::pair<std::string, std::string>> environment;
  std::map<std::string, std::string, std::less<>> defaults;
  std::string default_source;
  std::map<std::string, std::int64_t, std::less<>> ceilings;
  std::vector<std::string> capabilities{"host", "http", "rpc", "transport"};
};

using RuntimePolicyValue = std::variant<std::int64_t, std::string>;
struct EffectiveRuntimeField {
  std::string_view name;
  RuntimePolicyValue value;
  std::string_view source;
  std::string source_detail;
  bool dynamic = false;
  std::optional<std::int64_t> ceiling;
  // Registry maximum includes the common integer limit. Enum fields have none.
  std::optional<std::int64_t> maximum;
  std::string_view unit, capability;
};
struct RuntimePolicySnapshot {
  std::uint64_t revision = 1;
  std::array<EffectiveRuntimeField, runtime_policy_capacity> fields{};
  std::size_t count = 0;
  std::span<const EffectiveRuntimeField> entries() const noexcept {
    return {fields.data(), count};
  }
};

// Resolved policies contain fixed field records and own their values/source
// details. They retain neither the environment snapshot nor unrelated secrets.
// Share const instances with hosts/clients after resolution at startup.
class RuntimePolicy {
public:
  std::uint64_t revision() const noexcept { return snapshot_.revision; }
  std::span<const EffectiveRuntimeField> fields() const noexcept {
    return snapshot_.entries();
  }
  RuntimePolicySnapshot effective() const { return snapshot_; }
  const EffectiveRuntimeField &field(std::string_view name) const;
  std::int64_t integer(std::string_view name) const;
  std::string_view text(std::string_view name) const;
  bool supports(std::string_view name) const noexcept;

  // A transport must reject explicitly selected fields it cannot enforce.
  // Unrelated SDK defaults without a declared product ceiling do not require
  // that transport to implement them.
  void check_applied(std::span<const std::string_view> names) const;
  void check_applied(std::initializer_list<std::string_view> names) const {
    check_applied(std::span<const std::string_view>{names.begin(), names.size()});
  }
  // A shared process policy may include several owners. Check only this
  // owner's capabilities while retaining all fields/source metadata unchanged.
  void check_applied(std::span<const std::string_view> names,
                     std::span<const std::string_view> owned_capabilities) const;
  void check_applied(
      std::initializer_list<std::string_view> names,
      std::initializer_list<std::string_view> owned_capabilities) const {
    check_applied(
        std::span<const std::string_view>{names.begin(), names.size()},
        std::span<const std::string_view>{owned_capabilities.begin(),
                                          owned_capabilities.size()});
  }

private:
  explicit RuntimePolicy(RuntimePolicySnapshot snapshot)
      : snapshot_(std::move(snapshot)) {}
  RuntimePolicySnapshot snapshot_;
  friend RuntimePolicy resolve_runtime_policy(const RuntimePolicyOptions &);
};

// SDK defaults < deployment defaults < startup environment. Product ceilings
// constrain the final result; exceeding one fails instead of clamping/fallback.
RuntimePolicy resolve_runtime_policy(const RuntimePolicyOptions &options);
} // namespace xgc2::xrpc
