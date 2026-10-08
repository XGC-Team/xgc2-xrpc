#include "xgc2/xrpc/runtime_policy.hpp"
#include <algorithm>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <thread>

using namespace xgc2::xrpc;
namespace {
using Tree = boost::property_tree::ptree;

RuntimePolicyOptions all_registry_capabilities() {
  // Validate the shared parsing corpus independently of any transport's
  // capability declaration. Production composition selects actual owners.
  Tree registry;
  std::istringstream input{std::string(runtime_policy_registry)};
  boost::property_tree::read_json(input, registry);
  RuntimePolicyOptions options;
  options.capabilities.clear();
  for (const auto &[key, field] : registry.get_child("fields")) {
    (void)key;
    const auto capability = field.get<std::string>("capability");
    if (std::find(options.capabilities.begin(), options.capabilities.end(),
                  capability) == options.capabilities.end())
      options.capabilities.push_back(capability);
  }
  return options;
}

template <typename Action>
void rejects(std::string_view field, Action action) {
  try {
    action();
    std::cerr << "expected rejection for " << field << '\n';
    assert(false);
  } catch (const RuntimePolicyError &error) {
    assert(error.field == field);
    assert(!error.reason.empty());
    // Errors must identify the field and constraint, never dump raw inputs.
    assert(std::string_view(error.what()).find("not-returned") ==
           std::string_view::npos);
  }
}

std::size_t common_environment_cases(const std::filesystem::path &path) {
  Tree fixture;
  boost::property_tree::read_json(path.string(), fixture);
  assert(fixture.get<unsigned>("schema_version") == 1);
  std::size_t tested = 0;
  for (const auto &[key, item] : fixture.get_child("cases")) {
    (void)key;
    auto options = all_registry_capabilities();
    if (const auto environment = item.get_child_optional("environment"))
      for (const auto &[name, value] : *environment)
        options.environment.emplace_back(name, value.get_value<std::string>());
    if (const auto defaults = item.get_child_optional("defaults"))
      for (const auto &[name, value] : *defaults)
        options.defaults.emplace(name, value.get_value<std::string>());
    if (const auto ceilings = item.get_child_optional("ceilings"))
      for (const auto &[name, value] : *ceilings)
        options.ceilings.emplace(name, value.get_value<std::int64_t>());
    if (const auto capabilities = item.get_child_optional("capabilities")) {
      options.capabilities.clear();
      for (const auto &[name, value] : *capabilities) {
        (void)name;
        options.capabilities.push_back(value.get_value<std::string>());
      }
    }
    const auto error = item.get_optional<std::string>("error_field");
    if (error) {
      rejects(*error, [&] { (void)resolve_runtime_policy(options); });
    } else {
      const auto policy = resolve_runtime_policy(options);
      assert(policy.revision() == 1);
      if (const auto values = item.get_child_optional("values")) {
        for (const auto &[name, expected] : *values) {
          const auto &actual = policy.field(name);
          if (std::holds_alternative<std::string>(actual.value))
            assert(policy.text(name) == expected.get_value<std::string>());
          else
            assert(policy.integer(name) == expected.get_value<std::int64_t>());
        }
      }
      if (const auto sources = item.get_child_optional("sources"))
        for (const auto &[name, source] : *sources)
          assert(policy.field(name).source == source.get_value<std::string>());
      for (const auto &entry : policy.fields()) {
        assert(entry.name != "OTHER_SECRET");
        assert(entry.source_detail.find("not-returned") == std::string::npos);
        if (const auto value = std::get_if<std::string>(&entry.value))
          assert(value->find("not-returned") == std::string::npos);
      }
    }
    ++tested;
  }
  assert(tested != 0);
  return tested;
}

void effective_snapshot_and_enforcement() {
  RuntimePolicyOptions options;
  options.defaults["HOST_MAX_CONNECTIONS"] = "7";
  options.default_source = "deployment-role";
  options.ceilings["HOST_MAX_CONNECTIONS"] = 9;
  auto deployment = resolve_runtime_policy(options);
  assert(deployment.integer("HOST_MAX_CONNECTIONS") == 7);
  const auto &entry = deployment.field("HOST_MAX_CONNECTIONS");
  assert(entry.source == "deployment");
  assert(entry.source_detail == "deployment-role");
  assert(entry.ceiling == 9 && entry.maximum.has_value());
  assert(entry.maximum.value() >= entry.ceiling.value());
  assert(!entry.dynamic && entry.unit == "connections");
  assert(entry.capability == "host");
  assert(!deployment.supports("LOG_LEVEL"));
  assert(!deployment.supports("CLIENT_MAX_CONNECTIONS"));
  assert(!deployment.supports("GRPC_MAX_STREAMS_PER_CONNECTION"));
  rejects("LOG_LEVEL", [&] { (void)deployment.field("LOG_LEVEL"); });
  rejects("HOST_MAX_CONNECTIONS",
          [&] { (void)deployment.text("HOST_MAX_CONNECTIONS"); });
  deployment.check_applied({"HOST_MAX_CONNECTIONS"});
  rejects("HOST_MAX_CONNECTIONS", [&] { deployment.check_applied({}); });

  options.environment.emplace_back("XGC2_XRPC_HOST_MAX_CONNECTIONS", "9");
  const auto environment = resolve_runtime_policy(options);
  assert(environment.integer("HOST_MAX_CONNECTIONS") == 9);
  assert(environment.field("HOST_MAX_CONNECTIONS").source == "environment");
  assert(environment.field("HOST_MAX_CONNECTIONS").source_detail.empty());
  options.environment[0].second = "8";
  options.defaults["HOST_MAX_CONNECTIONS"] = "6";
  options.default_source = "changed-owner";
  assert(environment.integer("HOST_MAX_CONNECTIONS") == 9);
  assert(deployment.integer("HOST_MAX_CONNECTIONS") == 7);
  assert(deployment.field("HOST_MAX_CONNECTIONS").source_detail ==
         "deployment-role");
  auto copy = environment.effective();
  assert(copy.count == environment.fields().size());
  for (auto &value : copy.fields)
    value.value = std::int64_t{1};
  assert(environment.integer("HOST_MAX_CONNECTIONS") == 9);

  const auto defaults = resolve_runtime_policy(RuntimePolicyOptions{});
  defaults.check_applied({});
  RuntimePolicyOptions constrained_defaults;
  constrained_defaults.ceilings["HOST_MAX_CONNECTIONS"] =
      defaults.integer("HOST_MAX_CONNECTIONS");
  const auto constrained = resolve_runtime_policy(constrained_defaults);
  assert(constrained.field("HOST_MAX_CONNECTIONS").source == "sdk_default");
  rejects("HOST_MAX_CONNECTIONS", [&] { constrained.check_applied({}); });
  constrained.check_applied({"HOST_MAX_CONNECTIONS"});
  std::thread first([&] {
    for (unsigned i = 0; i < 1000; ++i)
      assert(environment.integer("HOST_MAX_CONNECTIONS") == 9);
  });
  std::thread second([&] {
    for (unsigned i = 0; i < 1000; ++i)
      assert(deployment.field("HOST_MAX_CONNECTIONS").source_detail ==
             "deployment-role");
  });
  first.join();
  second.join();
}

void additional_negative_inputs() {
  for (const std::string value : {"999999999999999999999999999999999", "1\n",
                                 "1\t", "1 0", "00", "0x10"}) {
    RuntimePolicyOptions options;
    options.environment.emplace_back("XGC2_XRPC_HOST_MAX_CONNECTIONS", value);
    rejects("HOST_MAX_CONNECTIONS",
            [&] { (void)resolve_runtime_policy(options); });
  }
  RuntimePolicyOptions duplicate;
  duplicate.environment = {{"XGC2_XRPC_HOST_MAX_CONNECTIONS", "1"},
                           {"XGC2_XRPC_HOST_MAX_CONNECTIONS", "2"}};
  rejects("XGC2_XRPC_HOST_MAX_CONNECTIONS",
          [&] { (void)resolve_runtime_policy(duplicate); });
  duplicate.environment = {{"OTHER_SECRET", "not-returned"},
                           {"OTHER_SECRET", "also-not-returned"}};
  (void)resolve_runtime_policy(duplicate);

  RuntimePolicyOptions unsupported;
  unsupported.defaults["GRPC_MAX_STREAMS_PER_CONNECTION"] = "4";
  rejects("GRPC_MAX_STREAMS_PER_CONNECTION",
          [&] { (void)resolve_runtime_policy(unsupported); });
  unsupported.defaults.clear();
  unsupported.ceilings["GRPC_MAX_STREAMS_PER_CONNECTION"] = 4;
  rejects("GRPC_MAX_STREAMS_PER_CONNECTION",
          [&] { (void)resolve_runtime_policy(unsupported); });
  unsupported.ceilings.clear();
  unsupported.capabilities.push_back("unimplemented-capability");
  rejects("unimplemented-capability",
          [&] { (void)resolve_runtime_policy(unsupported); });

  RuntimePolicyOptions invalid_default;
  invalid_default.defaults["HOST_MAX_CONNECTIONS"] = "0";
  invalid_default.environment.emplace_back("XGC2_XRPC_HOST_MAX_CONNECTIONS",
                                           "8");
  rejects("HOST_MAX_CONNECTIONS",
          [&] { (void)resolve_runtime_policy(invalid_default); });
  invalid_default.defaults.clear();
  invalid_default.defaults["UNDECLARED"] = "1";
  rejects("UNDECLARED",
          [&] { (void)resolve_runtime_policy(invalid_default); });

  for (std::int64_t ceiling : {std::int64_t{0}, std::int64_t{-1},
                              std::int64_t{2147483648}}) {
    RuntimePolicyOptions options;
    options.ceilings["HOST_MAX_CONNECTIONS"] = ceiling;
    rejects("HOST_MAX_CONNECTIONS",
            [&] { (void)resolve_runtime_policy(options); });
  }
  auto enum_ceiling = all_registry_capabilities();
  enum_ceiling.ceilings["LOG_LEVEL"] = 1;
  rejects("LOG_LEVEL",
          [&] { (void)resolve_runtime_policy(enum_ceiling); });

  RuntimePolicyOptions maximum;
  maximum.environment.emplace_back("XGC2_XRPC_HOST_MAX_CONNECTIONS",
                                   "2147483647");
  assert(resolve_runtime_policy(maximum).integer("HOST_MAX_CONNECTIONS") ==
         2147483647);
}

void shared_policy_owner_scopes() {
  RuntimePolicyOptions options;
  options.capabilities.push_back("diagnostics");
  options.capabilities.push_back("grpc");
  options.environment = {{"XGC2_XRPC_LOG_LEVEL", "debug"},
                         {"XGC2_XRPC_HOST_MAX_CONNECTIONS", "9"},
                         {"XGC2_XRPC_GRPC_MAX_STREAMS_PER_CONNECTION", "4"}};
  const auto policy = resolve_runtime_policy(options);
  policy.check_applied({"HOST_MAX_CONNECTIONS"},
                       {"host", "http", "rpc", "transport"});
  policy.check_applied({"LOG_LEVEL", "LOG_FORMAT"}, {"diagnostics"});
  policy.check_applied({"HOST_MAX_CONNECTIONS", "GRPC_MAX_STREAMS_PER_CONNECTION"},
                       {"host", "rpc", "transport", "grpc"});
  rejects("LOG_LEVEL", [&] { policy.check_applied({"HOST_MAX_CONNECTIONS"}); });
  rejects("HOST_MAX_CONNECTIONS", [&] {
    policy.check_applied({"GRPC_MAX_STREAMS_PER_CONNECTION"}, {"host", "grpc"});
  });
  rejects("LOG_LEVEL", [&] {
    policy.check_applied({"LOG_FORMAT"}, {"diagnostics"});
  });
  policy.check_applied({}, {});
  for (const auto name : {"LOG_LEVEL", "HOST_MAX_CONNECTIONS",
                          "GRPC_MAX_STREAMS_PER_CONNECTION"})
    assert(policy.field(name).source == "environment");
  assert(policy.text("LOG_LEVEL") == "debug");
  assert(policy.integer("HOST_MAX_CONNECTIONS") == 9);
  assert(policy.integer("GRPC_MAX_STREAMS_PER_CONNECTION") == 4);
}
} // namespace

int main(int argc, char **argv) {
  const auto fixture =
      argc == 2 ? std::filesystem::path(argv[1])
                : std::filesystem::path(__FILE__).parent_path().parent_path()
                          .parent_path() /
                      "contracts/fixtures/environment.json";
  const auto count = common_environment_cases(fixture);
  effective_snapshot_and_enforcement();
  additional_negative_inputs();
  shared_policy_owner_scopes();
  std::cout << "runtime environment: " << count
            << " shared cases, provenance, ceilings, duplicate/unsupported "
               "inputs and immutable snapshots passed\n";
}
