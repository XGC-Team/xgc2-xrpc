#include "xgc2/xrpc/runtime_policy.hpp"
#include <algorithm>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <charconv>
#include <limits>
#include <sstream>

namespace xgc2::xrpc {
namespace {
struct Definition {
  std::string name, type, unit, capability, default_value;
  std::vector<std::string> values;
  bool dynamic = false;
  std::int64_t maximum = 0;
};
struct Registry {
  std::string prefix;
  std::int64_t integer_maximum = 0;
  std::array<Definition, runtime_policy_capacity> fields;
};

[[noreturn]] void invalid(std::string_view name, std::string reason) {
  throw RuntimePolicyError(std::string(name), std::move(reason));
}

RuntimePolicyValue parse(const Definition &field, std::string_view raw) {
  if (field.type == "enum") {
    const auto found = std::find(field.values.begin(), field.values.end(), raw);
    if (found == field.values.end())
      invalid(field.name, "requires an exact declared enum token");
    return std::string(raw);
  }
  if (raw.empty() || raw.size() > 10 || raw.front() < '1' || raw.front() > '9' ||
      !std::all_of(raw.begin() + 1, raw.end(),
                   [](char c) { return c >= '0' && c <= '9'; }))
    invalid(field.name, "requires a canonical positive ASCII decimal integer");
  std::int64_t value = 0;
  const auto result = std::from_chars(raw.data(), raw.data() + raw.size(), value);
  if (result.ec != std::errc{} || result.ptr != raw.data() + raw.size() ||
      value > field.maximum)
    invalid(field.name, "exceeds registry maximum " +
                            std::to_string(field.maximum));
  return value;
}

const Registry &registry() {
  // Cold startup metadata only. No environment access or worker initialization.
  static const Registry value = [] {
    try {
      boost::property_tree::ptree tree;
      std::istringstream input{std::string(runtime_policy_registry)};
      boost::property_tree::read_json(input, tree);
      if (tree.get<unsigned>("schema_version") != 1 ||
          tree.get<std::string>("integer_syntax") != "[1-9][0-9]*")
        throw std::runtime_error("invalid schema");
      Registry result;
      result.prefix = tree.get<std::string>("prefix");
      result.integer_maximum = tree.get<std::int64_t>("integer_max");
      if (result.prefix != "XGC2_XRPC_" || result.integer_maximum <= 0 ||
          result.integer_maximum > std::numeric_limits<std::int32_t>::max())
        throw std::runtime_error("invalid limits");
      const auto &fields = tree.get_child("fields");
      if (fields.size() != result.fields.size() || fields.empty())
        throw std::runtime_error("invalid field count");
      std::size_t i = 0;
      for (const auto &[key, item] : fields) {
        if (!key.empty())
          throw std::runtime_error("invalid field array");
        auto &field = result.fields[i++];
        field.name = item.get<std::string>("name");
        field.type = item.get<std::string>("type");
        field.unit = item.get<std::string>("unit", "");
        field.capability = item.get<std::string>("capability");
        field.default_value = item.get<std::string>("default");
        field.dynamic = item.get<bool>("dynamic");
        field.maximum = std::min(
            result.integer_maximum,
            item.get<std::int64_t>("maximum", result.integer_maximum));
        if (field.name.empty() || field.capability.empty() ||
            (field.type != "integer" && field.type != "enum") ||
            field.maximum <= 0)
          throw std::runtime_error("invalid field");
        if (field.type == "enum") {
          for (const auto &[value_key, enum_value] : item.get_child("values")) {
            if (!value_key.empty() || enum_value.data().empty())
              throw std::runtime_error("invalid enum");
            field.values.push_back(enum_value.get_value<std::string>());
          }
        }
        (void)parse(field, field.default_value);
      }
      for (std::size_t first = 0; first < result.fields.size(); ++first)
        for (std::size_t second = first + 1; second < result.fields.size();
             ++second)
          if (result.fields[first].name == result.fields[second].name)
            throw std::runtime_error("duplicate field");
      return result;
    } catch (const std::exception &) {
      // Generated metadata failures do not expose source documents or values.
      invalid("registry", "invalid generated runtime policy registry");
    }
  }();
  return value;
}

const Definition &definition(const Registry &reg, std::string_view name,
                             std::string_view error_name) {
  const auto found = std::find_if(
      reg.fields.begin(), reg.fields.end(),
      [&](const Definition &field) { return field.name == name; });
  if (found == reg.fields.end())
    invalid(error_name, "unknown runtime setting");
  return *found;
}

bool enabled(const RuntimePolicyOptions &options, std::string_view capability) {
  return std::find(options.capabilities.begin(), options.capabilities.end(),
                   capability) != options.capabilities.end();
}

const Definition &selected(const Registry &reg,
                           const RuntimePolicyOptions &options,
                           std::string_view name, std::string_view error_name) {
  const auto &field = definition(reg, name, error_name);
  if (!enabled(options, field.capability))
    invalid(field.name, "setting is not enforced by selected capabilities");
  return field;
}
} // namespace

RuntimePolicyError::RuntimePolicyError(std::string field, std::string reason)
    : std::runtime_error(field + ": " + reason), field(std::move(field)),
      reason(std::move(reason)) {}

RuntimePolicy resolve_runtime_policy(const RuntimePolicyOptions &options) {
  const auto &reg = registry();
  for (const auto &capability : options.capabilities)
    if (std::none_of(reg.fields.begin(), reg.fields.end(),
                     [&](const Definition &field) {
                       return field.capability == capability;
                     }))
      invalid(capability, "unknown runtime policy capability");

  // Keep only reserved names; unrelated environment entries never enter the
  // policy. Pairs preserve duplicates so a malformed snapshot cannot hide one.
  std::map<std::string_view, RuntimePolicyValue, std::less<>> environment;
  for (const auto &[name, raw] : options.environment) {
    if (!std::string_view(name).starts_with(reg.prefix))
      continue;
    const auto short_name = std::string_view(name).substr(reg.prefix.size());
    const auto &field = selected(reg, options, short_name, name);
    if (environment.contains(short_name))
      invalid(name, "duplicate environment name");
    environment.emplace(short_name, parse(field, raw));
  }

  std::map<std::string_view, RuntimePolicyValue, std::less<>> defaults;
  for (const auto &[name, raw] : options.defaults) {
    const auto &field = selected(reg, options, name, name);
    defaults.emplace(name, parse(field, raw));
  }
  for (const auto &[name, ceiling] : options.ceilings) {
    const auto &field = selected(reg, options, name, name);
    if (field.type != "integer" || ceiling <= 0 ||
        ceiling > reg.integer_maximum)
      invalid(name, "ceiling requires a positive supported integer limit");
  }

  RuntimePolicySnapshot snapshot;
  for (const auto &field : reg.fields) {
    if (!enabled(options, field.capability))
      continue;
    auto &entry = snapshot.fields[snapshot.count++];
    entry.name = field.name;
    entry.value = parse(field, field.default_value);
    entry.source = "sdk_default";
    entry.dynamic = field.dynamic;
    entry.unit = field.unit;
    entry.capability = field.capability;
    if (field.type == "integer")
      entry.maximum = field.maximum;
    if (const auto value = defaults.find(field.name); value != defaults.end()) {
      entry.value = value->second;
      entry.source = "deployment";
      entry.source_detail = options.default_source;
    }
    if (const auto value = environment.find(field.name);
        value != environment.end()) {
      entry.value = value->second;
      entry.source = "environment";
      entry.source_detail.clear();
    }
    if (const auto ceiling = options.ceilings.find(field.name);
        ceiling != options.ceilings.end()) {
      entry.ceiling = ceiling->second;
      if (std::get<std::int64_t>(entry.value) > ceiling->second)
        invalid(field.name, "resolved value exceeds declared ceiling " +
                                std::to_string(ceiling->second));
    }
  }
  return RuntimePolicy(std::move(snapshot));
}

bool RuntimePolicy::supports(std::string_view name) const noexcept {
  return std::any_of(fields().begin(), fields().end(),
                     [&](const EffectiveRuntimeField &field) {
                       return field.name == name;
                     });
}

const EffectiveRuntimeField &RuntimePolicy::field(std::string_view name) const {
  const auto found = std::find_if(
      fields().begin(), fields().end(),
      [&](const EffectiveRuntimeField &field) { return field.name == name; });
  if (found == fields().end())
    invalid(name, "runtime policy field is not selected");
  return *found;
}

std::int64_t RuntimePolicy::integer(std::string_view name) const {
  if (const auto value = std::get_if<std::int64_t>(&field(name).value))
    return *value;
  invalid(name, "runtime policy field is not an integer");
}

std::string_view RuntimePolicy::text(std::string_view name) const {
  if (const auto value = std::get_if<std::string>(&field(name).value))
    return *value;
  invalid(name, "runtime policy field is not an enum");
}

void RuntimePolicy::check_applied(std::span<const std::string_view> names) const {
  for (const auto &entry : fields()) {
    if (entry.source == "sdk_default" && !entry.ceiling)
      continue;
    if (std::find(names.begin(), names.end(), entry.name) == names.end())
      invalid(entry.name, "selected transport cannot enforce runtime setting");
  }
}

void RuntimePolicy::check_applied(
    std::span<const std::string_view> names,
    std::span<const std::string_view> owned_capabilities) const {
  for (const auto &entry : fields()) {
    if (std::find(owned_capabilities.begin(), owned_capabilities.end(),
                  entry.capability) == owned_capabilities.end() ||
        (entry.source == "sdk_default" && !entry.ceiling))
      continue;
    if (std::find(names.begin(), names.end(), entry.name) == names.end())
      invalid(entry.name, "selected transport cannot enforce runtime setting");
  }
}
} // namespace xgc2::xrpc
