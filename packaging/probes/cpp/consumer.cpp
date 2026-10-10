#include <xgc2/xrpc/json_http.hpp>
#include <xgc2/xrpc/bootstrap.hpp>
#include <xgc2/xrpc/runtime_policy.hpp>
#include <xgc2/xrpc/diagnostics.hpp>
#include <condition_variable>
#include <mutex>
#include <span>
#include <stop_token>
#ifdef XRPC_CHECK_GRPC
#include <xgc2/xrpc/grpc.hpp>
#endif
int main() {
  xgc2::xrpc::Json value;
  if (!xgc2::xrpc::parse_json("{\"n\":18446744073709551615}", value) ||
      value["n"].get<std::uint64_t>() != UINT64_MAX) return 9;
  if (xgc2::xrpc::json_response(value, 128).status != 200) return 10;
  try {
    (void)xgc2::xrpc::parseBootstrapBinding("{}");
    return 7;
  } catch (const xgc2::xrpc::BootstrapError &error) {
    if (error.code != xgc2::xrpc::BootstrapErrorCode::InvalidInput) return 8;
  }
  xgc2::xrpc::RuntimePolicyOptions options;
#ifdef XRPC_CHECK_GRPC
  options.capabilities.push_back("grpc");
#endif
  const auto policy = xgc2::xrpc::resolve_runtime_policy(options);
  const auto limits = xgc2::xrpc::http_limits(policy);
  const auto instance = xgc2::xrpc::new_instance_id();
  xgc2::xrpc::HttpClient client("/unused-installed-sdk-probe.sock", limits, instance);
  client.close();
  if (xgc2::xrpc::diagnostic_code_name(xgc2::xrpc::DiagnosticCode::CallCompleted).empty()) return 6;
  std::stop_source stop;
  stop.request_stop();
  std::mutex mutex;
  std::unique_lock lock(mutex);
  std::condition_variable_any condition;
  if (condition.wait_until(lock, stop.get_token(), std::chrono::steady_clock::now(), [] { return false; })) return 2;
  const int values[] = {1, 2};
  if (std::span(values).size() != 2 || limits.connections == 0) return 3;
#ifdef XRPC_CHECK_GRPC
  if (xgc2::xrpc::grpc_limits(policy).inflight == 0) return 4;
  xgc2::xrpc::GrpcAdmission admission(instance, xgc2::xrpc::grpc_limits(policy));
  admission.request_stop();
  if (!admission.wait_until(xgc2::xrpc::GrpcClock::now())) return 5;
#endif
  return 0;
}
