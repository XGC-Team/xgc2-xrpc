#include <xgc2/xrpc/json_http.hpp>
#include <xgc2/xrpc/bootstrap.hpp>
#include <xgc2/xrpc/diagnostics.hpp>
#include <condition_variable>
#include <mutex>
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
  const xgc2::xrpc::HttpLimits limits;
  const auto instance = xgc2::xrpc::new_instance_id();
  xgc2::xrpc::HttpClient client("/unused-installed-sdk-probe.sock", limits, instance);
  client.close();
  if (xgc2::xrpc::diagnostic_code_name(xgc2::xrpc::DiagnosticCode::CallCompleted).empty()) return 6;
  xgc2::xrpc::StopSource stop;
  stop.request_stop();
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  std::condition_variable condition;
  if (xgc2::xrpc::wait_until(condition, lock, stop.get_token(),
                             std::chrono::steady_clock::now() + std::chrono::seconds(10),
                             [] { return false; })) return 2;
  if (!stop.get_token().stop_requested() || limits.connections == 0) return 3;
#ifdef XRPC_CHECK_GRPC
  const xgc2::xrpc::GrpcLimits grpc_limits;
  if (grpc_limits.inflight == 0) return 4;
  xgc2::xrpc::GrpcAdmission admission(instance, grpc_limits);
  admission.request_stop();
  if (!admission.wait_until(xgc2::xrpc::GrpcClock::now())) return 5;
#endif
  return 0;
}
