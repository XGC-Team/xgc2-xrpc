#pragma once
#include "stop.hpp"
#include "unix.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>

namespace xgc2::xrpc {
class Diagnostics;
using GrpcClock = std::chrono::steady_clock;
// Hard bounds for a gRPC host and its channels. The defaults suit a small
// private control service; the owner sets other bounds explicitly. The library
// reads no environment variables and keeps no hidden policy.
struct GrpcLimits {
  std::size_t connections = 32;             // accepted connections at one time
  std::size_t inflight = 32;                // admitted calls without a result
  std::size_t streams_per_connection = 32;  // native concurrent streams
  std::size_t request_bytes = 1048576;      // largest request message
  std::size_t response_bytes = 1048576;     // largest response message
  std::size_t header_bytes = 16384;         // largest metadata block
  std::chrono::milliseconds call_timeout{30000};    // host cap on one call
  std::chrono::milliseconds idle_timeout{30000};    // idle connection eviction
  std::chrono::milliseconds shutdown_timeout{5000}; // default shutdown budget
  // MAX_POLLERS alone does not bound handlers. ResourceQuota caps the native
  // synchronous pool; one additional SDK thread accepts all connections.
  int native_threads = 8;
  std::size_t native_memory_bytes = 16 * 1048576;
};
// Leave room for native grpc-timeout rounding while preserving the caller's
// finite budget. Throws if the host budget cannot accommodate that margin.
GrpcClock::time_point grpc_stream_deadline(const GrpcLimits&,
                                          GrpcClock::time_point caller_deadline);
struct GrpcStats {
  std::size_t inflight_calls = 0, active_connections = 0;
  std::uint64_t admitted_calls = 0, rejected_calls = 0,
                accepted_connections = 0, rejected_connections = 0;
  bool stopping = false;
};
namespace detail { struct GrpcAdmissionState; }

// A single bounded work handoff. Moving this token to an owned worker keeps
// admission and the endpoint lease until actual work exits, even after the
// native method returns. Cancellation is advisory; it does not imply rollback.
class GrpcWorkPermit {
public:
  GrpcWorkPermit() = default;
  ~GrpcWorkPermit();
  GrpcWorkPermit(GrpcWorkPermit&&) noexcept;
  GrpcWorkPermit& operator=(GrpcWorkPermit&&) noexcept;
  GrpcWorkPermit(const GrpcWorkPermit&) = delete;
  GrpcWorkPermit& operator=(const GrpcWorkPermit&) = delete;
  bool cancelled() const noexcept;
private:
  friend class GrpcCallScope;
  GrpcWorkPermit(std::shared_ptr<detail::GrpcAdmissionState>, std::size_t);
  std::shared_ptr<detail::GrpcAdmissionState> state_;
  std::size_t slot_ = 0;
};

class GrpcCallScope {
public:
  GrpcCallScope() = default;
  ~GrpcCallScope();
  GrpcCallScope(GrpcCallScope&&) noexcept;
  GrpcCallScope& operator=(GrpcCallScope&&) noexcept;
  GrpcCallScope(const GrpcCallScope&) = delete;
  GrpcCallScope& operator=(const GrpcCallScope&) = delete;
  const grpc::Status& status() const noexcept { return status_; }
  explicit operator bool() const noexcept { return status_.ok(); }
  const std::string& request_id() const noexcept { return request_id_; }
  GrpcClock::time_point deadline() const noexcept { return deadline_; }
  bool cancelled() const noexcept;
  // Exactly one handoff per admission; throws on a rejected call or a second
  // handoff. The domain must also bound its own queue before accepting work.
  GrpcWorkPermit retain_work();
private:
  friend class GrpcAdmission;
  std::shared_ptr<detail::GrpcAdmissionState> state_;
  std::size_t slot_ = 0;
  grpc::Status status_{grpc::StatusCode::INTERNAL, "no call admitted"};
  std::string request_id_;
  GrpcClock::time_point deadline_{};
};

// Call begin as the first statement of every synchronous generated method.
// Scope must be destroyed before its ServerContext, including exception paths.
// Async CQ APIs require their own completion-tag lifetimes and are not covered.
class GrpcAdmission {
public:
  explicit GrpcAdmission(std::string instance_id, GrpcLimits limits = {});
  // Owner configures before serving; sink must outlive host and work permits.
  void set_diagnostics(Diagnostics*, std::string service = "grpc");
  // Only an explicitly declared unary discovery method may omit the instance
  // key. Supplied empty/duplicate/mismatched keys still fail; streams cannot
  // use discovery. Request identity, budget and admission always apply.
  GrpcCallScope begin(grpc::ServerContext&, bool streaming = false,
                      bool discovery = false);
  template<class NativeServerStream>
  GrpcCallScope begin_stream(grpc::ServerContext& context, NativeServerStream& stream) {
    auto call = begin(context, true);
    // Publish fence before a server blocks reading or a client sends payload.
    if (call) stream.SendInitialMetadata();
    return call;
  }
  void request_stop() noexcept;
  bool wait_until(GrpcClock::time_point deadline) const;
  GrpcStats stats() const noexcept;
  const GrpcLimits& limits() const noexcept;
  const std::string& instance_id() const noexcept;
private:
  friend class GrpcUnixServer;
  std::shared_ptr<detail::GrpcAdmissionState> state_;
};

// Local private Unix host: the lease binds and cleans the pathname, while gRPC
// owns accepted descriptors only. gRPC's AddListeningPort(unix:...) unlinks
// sockets without inode checking and is deliberately not used here.
// Services/admission must outlive this host. Services remain domain-owned.
class GrpcUnixServer {
public:
  GrpcUnixServer(UnixOptions, GrpcAdmission&, std::vector<grpc::Service*>);
  ~GrpcUnixServer();
  GrpcUnixServer(const GrpcUnixServer&) = delete;
  GrpcUnixServer& operator=(const GrpcUnixServer&) = delete;
  void request_stop() noexcept;
  // Graceful owner-thread stop: stop admission/acceptance, preserve admitted
  // native calls until deadline, then cancel the remainder. False retains lease.
  bool drain_until(GrpcClock::time_point deadline);
  // False retains server, admission and lease. Retry after work quiesces.
  // Owner-thread operation; must not race destruction or another shutdown.
  bool shutdown_until(GrpcClock::time_point deadline);
  bool shutdown();
  GrpcStats stats() const noexcept;
  const std::string& socket_path() const noexcept;
private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// Reuse one channel per ServiceRef. This local helper never dials a remote
// Unix pathname. Remote credentials/routes are injected by their owner. Only
// the message and metadata bounds of the limits apply to a channel; idle
// eviction and stream counts are server-side and the channel's owner decides
// its lifetime.
std::shared_ptr<grpc::Channel> make_grpc_unix_channel(
    const std::string& path, const GrpcLimits& = {});

// Fresh, caller-owned native context. The scope must outlive native RPC/stream
// completion, then be destroyed before ClientContext. No automatic replay.
enum class GrpcDelivery { NotSent, OutcomeUnknown };
class GrpcClientCall {
public:
  // Explicit unary discovery alone permits an empty instance_id, meaning the
  // metadata key is omitted. A supplied nonempty instance still binds exactly.
  // The caller must use a fresh context without manually adding XRPC keys.
  GrpcClientCall(grpc::ClientContext&, std::string instance_id,
                 GrpcClock::time_point deadline,
                 StopToken cancellation = {}, std::string request_id = {},
                 bool discovery = false);
  ~GrpcClientCall();
  GrpcClientCall(const GrpcClientCall&) = delete;
  GrpcClientCall& operator=(const GrpcClientCall&) = delete;
  const std::string& request_id() const noexcept;
  // Empty until successful checked response metadata. Explicit unary
  // discovery may omit instance_id; its returned ServiceRef must match this.
  const std::string& response_instance_id() const noexcept;
  // Must precede invoking a native stub/opening a native stream. Native gRPC
  // cannot report exact bytes dispatched, so all subsequent failures have
  // conservatively unknown outcome. Validation/pre-cancel remain NotSent.
  grpc::Status mark_dispatched();
  GrpcDelivery delivery() const noexcept;
  template<class NativeUnary> grpc::Status invoke(NativeUnary&& operation) {
    const auto ready = mark_dispatched();
    if (!ready.ok()) return ready;
    return verify(operation());
  }
  // After opening a stream, before the first Read/Write. Server integration
  // uses begin_stream to publish metadata before blocking on a native Read.
  template<class NativeClientStream>
  grpc::Status receive_initial_metadata(NativeClientStream& stream) {
    const auto mode = check_stream_mode();
    if (!mode.ok()) return mode;
    stream.WaitForInitialMetadata();
    return verify_initial_metadata();
  }
  grpc::Status verify_initial_metadata() const;
  // Call after unary return or stream Finish. Native failures without response
  // metadata keep their native status; successful/malformed replies are fenced.
  grpc::Status verify(grpc::Status native_status) const;
private:
  grpc::Status check_stream_mode() const;
  class Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace xgc2::xrpc
