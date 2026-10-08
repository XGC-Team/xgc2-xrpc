#include "xgc2/xrpc/grpc.hpp"
#include "xgc2/xrpc/diagnostics.hpp"
#include <grpc/impl/codegen/grpc_types.h>
#include <grpcpp/server_posix.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <fcntl.h>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

#ifndef GPR_SUPPORT_CHANNELS_FROM_FD
#error "XgcXrpc gRPC Unix host requires the native POSIX accepted-fd API"
#endif
namespace xgc2::xrpc {
namespace {
bool valid_id(const std::string& id) {
  if (id.empty() || id.size() > 128) return false;
  return std::all_of(id.begin(), id.end(), [](unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-';
  });
}
void validate(const GrpcLimits& l) {
  constexpr auto int_max = static_cast<std::size_t>(2147483647);
  if (!l.connections || !l.inflight || !l.streams_per_connection ||
      l.connections > 65536 || l.inflight > 65536 ||
      l.streams_per_connection > int_max || !l.request_bytes ||
      l.request_bytes > int_max || !l.response_bytes ||
      l.response_bytes > int_max || !l.header_bytes || l.header_bytes > int_max ||
      l.native_threads < 2 ||
      !l.native_memory_bytes || l.call_timeout.count() < 1 ||
      l.call_timeout > std::chrono::hours(24) || l.idle_timeout.count() < 1 ||
      l.idle_timeout.count() >= 2147483647 || l.shutdown_timeout.count() < 1 ||
      l.shutdown_timeout.count() > 2147483647)
    throw std::invalid_argument("invalid gRPC limits");
}
template<class Map>
bool one_metadata(const Map& metadata, const char* name, std::string& out) {
  auto range = metadata.equal_range(grpc::string_ref(name));
  if (range.first == range.second) return false;
  auto first = range.first++;
  if (range.first != range.second || first->second.size() > 128) return false;
  out.assign(first->second.data(), first->second.size());
  return valid_id(out);
}
grpc::Status error(grpc::StatusCode code, const char* reason) {
  return {code, reason};
}
void close_fd(int& fd) noexcept { if (fd >= 0) { ::close(fd); fd = -1; } }
void apply_limits(GrpcLimits& limits, const RuntimePolicy& policy) {
  limits.connections = policy.integer("HOST_MAX_CONNECTIONS");
  limits.inflight = policy.integer("HOST_MAX_IN_FLIGHT");
  limits.streams_per_connection = policy.integer("GRPC_MAX_STREAMS_PER_CONNECTION");
  limits.request_bytes = policy.integer("MAX_REQUEST_BYTES");
  limits.response_bytes = policy.integer("MAX_RESPONSE_BYTES");
  if (policy.supports("MAX_HEADER_BYTES")) limits.header_bytes = policy.integer("MAX_HEADER_BYTES");
  limits.call_timeout = std::chrono::milliseconds(policy.integer("CALL_TIMEOUT_MS"));
  limits.idle_timeout = std::chrono::milliseconds(policy.integer("IDLE_TIMEOUT_MS"));
  limits.shutdown_timeout = std::chrono::milliseconds(policy.integer("SHUTDOWN_TIMEOUT_MS"));
}
}
GrpcLimits::GrpcLimits() {
  static const auto defaults = [] {
    RuntimePolicyOptions options;
    options.capabilities = {"host", "http", "rpc", "transport", "grpc"};
    return resolve_runtime_policy(options);
  }();
  apply_limits(*this, defaults);
}
GrpcLimits grpc_limits(const RuntimePolicy& policy) {
  policy.check_applied({"HOST_MAX_CONNECTIONS", "HOST_MAX_IN_FLIGHT",
      "GRPC_MAX_STREAMS_PER_CONNECTION", "MAX_HEADER_BYTES", "MAX_REQUEST_BYTES",
      "MAX_RESPONSE_BYTES", "CALL_TIMEOUT_MS", "IDLE_TIMEOUT_MS", "SHUTDOWN_TIMEOUT_MS"},
      {"host", "rpc", "transport", "grpc"});
  GrpcLimits limits;
  apply_limits(limits, policy);
  if (limits.idle_timeout.count() >= 2147483647)
    throw RuntimePolicyError("IDLE_TIMEOUT_MS", "native INT_MAX idle value means unlimited");
  validate(limits);
  return limits;
}
GrpcLimits grpc_client_limits(const RuntimePolicy& policy,
                             std::span<const std::string_view> owner_applied) {
  std::vector<std::string_view> applied{"MAX_REQUEST_BYTES", "MAX_RESPONSE_BYTES",
      "CALL_TIMEOUT_MS", "IDLE_TIMEOUT_MS", "MAX_HEADER_BYTES"};
  for (const auto name : owner_applied) {
    if (name != "GRPC_MAX_STREAMS_PER_CONNECTION")
      throw RuntimePolicyError(std::string(name), "unsupported client owner field");
    applied.push_back(name);
  }
  constexpr std::array<std::string_view, 3> capabilities{"rpc", "transport", "grpc"};
  policy.check_applied(applied, capabilities);
  GrpcLimits limits;
  limits.request_bytes = policy.integer("MAX_REQUEST_BYTES");
  limits.response_bytes = policy.integer("MAX_RESPONSE_BYTES");
  limits.call_timeout = std::chrono::milliseconds(policy.integer("CALL_TIMEOUT_MS"));
  limits.idle_timeout = std::chrono::milliseconds(policy.integer("IDLE_TIMEOUT_MS"));
  if (policy.supports("MAX_HEADER_BYTES"))
    limits.header_bytes = policy.integer("MAX_HEADER_BYTES");
  if (policy.supports("GRPC_MAX_STREAMS_PER_CONNECTION"))
    limits.streams_per_connection = policy.integer("GRPC_MAX_STREAMS_PER_CONNECTION");
  // Stay within the documented native client idle range (minimum one second).
  if (limits.idle_timeout < std::chrono::seconds(1))
    throw RuntimePolicyError("IDLE_TIMEOUT_MS", "native client idle minimum is 1000 ms");
  if (limits.idle_timeout.count() >= 2147483647)
    throw RuntimePolicyError("IDLE_TIMEOUT_MS", "native INT_MAX idle value means unlimited");
  validate(limits);
  return limits;
}
GrpcClock::time_point grpc_stream_deadline(const GrpcLimits& limits,
                                          GrpcClock::time_point caller_deadline) {
  validate(limits);
  const auto now = GrpcClock::now();
  if (caller_deadline == GrpcClock::time_point::max() || caller_deadline <= now ||
      caller_deadline - now > std::chrono::hours(24))
    throw std::invalid_argument("finite stream caller deadline within 24 hours required");
  // gRPC 1.51 rounds native deadlines to milliseconds and timeout encoding
  // upward by at most one percent. Do not relax the host's actual-native check.
  const auto margin = std::chrono::milliseconds((limits.call_timeout.count() + 99) / 100 + 3);
  if (limits.call_timeout <= margin)
    throw std::invalid_argument("stream host budget is too small for native timeout rounding");
  return std::min(caller_deadline, now + limits.call_timeout - margin);
}
void configure_grpc_server(grpc::ServerBuilder& builder, const GrpcLimits& limits) {
  validate(limits);
  builder.SetMaxReceiveMessageSize(static_cast<int>(limits.request_bytes));
  builder.SetMaxSendMessageSize(static_cast<int>(limits.response_bytes));
  builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, 1);
  builder.SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, 1);
  builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, 1);
  grpc::ResourceQuota quota;
  quota.Resize(limits.native_memory_bytes).SetMaxThreads(limits.native_threads);
  builder.SetResourceQuota(quota);
  builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS,
                             static_cast<int>(limits.streams_per_connection));
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_IDLE_MS,
                             static_cast<int>(limits.idle_timeout.count()));
  builder.AddChannelArgument(GRPC_ARG_SERVER_HANDSHAKE_TIMEOUT_MS,
                             static_cast<int>(limits.idle_timeout.count()));
  builder.AddChannelArgument(GRPC_ARG_MAX_METADATA_SIZE, static_cast<int>(limits.header_bytes));
}
namespace detail {
struct GrpcAdmissionState {
  struct Slot {
    grpc::ServerContext* context = nullptr;
    GrpcClock::time_point deadline{};
    unsigned refs = 0;
    bool retained = false, cancelled = false;
    std::array<char, 128> request{};
    std::size_t request_size = 0;
    GrpcClock::time_point started{};
  };
  GrpcAdmissionState(std::string identity, GrpcLimits limits)
      : instance(std::move(identity)), limits(limits), slots(limits.inflight) {}
  std::string instance;
  GrpcLimits limits;
  mutable std::mutex mutex;
  mutable std::condition_variable cv;
  std::vector<Slot> slots;
  std::size_t active = 0;
  std::uint64_t admitted = 0, rejected = 0;
  bool stopping = false;
  Diagnostics* diagnostics = nullptr;
  std::string service;
  void emit(DiagnosticCode code, LogSeverity severity, const Slot* slot = nullptr) noexcept {
    if (!diagnostics) return;
    diagnostics->try_emit(code, severity,
        {service, instance, slot ? std::string_view(slot->request.data(), slot->request_size) : std::string_view{}},
        slot ? std::chrono::duration_cast<std::chrono::nanoseconds>(GrpcClock::now() - slot->started)
             : std::chrono::nanoseconds{});
  }
  void release(std::size_t index, bool context_owner) noexcept {
    std::lock_guard lock(mutex);
    auto& slot = slots[index];
    if (context_owner) {
      if (!slot.cancelled && ((slot.context && slot.context->IsCancelled()) ||
                             GrpcClock::now() >= slot.deadline)) {
        slot.cancelled = true; emit(DiagnosticCode::CallCancelled, LogSeverity::Info, &slot);
      }
      slot.context = nullptr;
    }
    if (--slot.refs == 0) {
      emit(DiagnosticCode::CallCompleted, LogSeverity::Info, &slot);
      --active; cv.notify_all();
    }
  }
  bool cancelled(std::size_t index) noexcept {
    std::lock_guard lock(mutex);
    auto& slot = slots[index];
    if (!slot.cancelled && slot.context && slot.context->IsCancelled()) {
      slot.cancelled = true; emit(DiagnosticCode::CallCancelled, LogSeverity::Info, &slot);
    }
    return slot.cancelled || GrpcClock::now() >= slot.deadline;
  }
  void stop(bool cancel = true) noexcept {
    std::lock_guard lock(mutex);
    if (!stopping) emit(DiagnosticCode::DrainStarted, LogSeverity::Info);
    stopping = true;
    for (auto& slot : slots) if (cancel && slot.refs) {
      if (!slot.cancelled) {
        slot.cancelled = true; emit(DiagnosticCode::CallCancelled, LogSeverity::Info, &slot);
      }
      // The mutex also guards context destruction by scope.release().
      if (slot.context) slot.context->TryCancel();
    }
    cv.notify_all();
  }
  bool wait(GrpcClock::time_point deadline) const {
    std::unique_lock lock(mutex);
    return cv.wait_until(lock, deadline, [&] { return active == 0; });
  }
};
}
GrpcWorkPermit::GrpcWorkPermit(std::shared_ptr<detail::GrpcAdmissionState> s,
                               std::size_t slot) : state_(std::move(s)), slot_(slot) {}
GrpcWorkPermit::~GrpcWorkPermit() { if (state_) state_->release(slot_, false); }
GrpcWorkPermit::GrpcWorkPermit(GrpcWorkPermit&& other) noexcept
    : state_(std::move(other.state_)), slot_(other.slot_) {}
GrpcWorkPermit& GrpcWorkPermit::operator=(GrpcWorkPermit&& other) noexcept {
  if (this != &other) {
    if (state_) state_->release(slot_, false);
    state_ = std::move(other.state_); slot_ = other.slot_;
  }
  return *this;
}
bool GrpcWorkPermit::cancelled() const noexcept {
  return !state_ || state_->cancelled(slot_);
}
GrpcCallScope::~GrpcCallScope() { if (state_) state_->release(slot_, true); }
GrpcCallScope::GrpcCallScope(GrpcCallScope&& other) noexcept
    : state_(std::move(other.state_)), slot_(other.slot_),
      status_(std::move(other.status_)), request_id_(std::move(other.request_id_)),
      deadline_(other.deadline_) {}
GrpcCallScope& GrpcCallScope::operator=(GrpcCallScope&& other) noexcept {
  if (this != &other) {
    if (state_) state_->release(slot_, true);
    state_ = std::move(other.state_); slot_ = other.slot_;
    status_ = std::move(other.status_); request_id_ = std::move(other.request_id_);
    deadline_ = other.deadline_;
  }
  return *this;
}
bool GrpcCallScope::cancelled() const noexcept {
  return !state_ || state_->cancelled(slot_);
}
GrpcWorkPermit GrpcCallScope::retain_work() {
  if (!state_) throw std::logic_error("cannot retain rejected gRPC work");
  std::lock_guard lock(state_->mutex);
  auto& slot = state_->slots[slot_];
  if (slot.retained) throw std::logic_error("gRPC work already retained");
  slot.retained = true; ++slot.refs;
  return GrpcWorkPermit(state_, slot_);
}
GrpcAdmission::GrpcAdmission(std::string instance, GrpcLimits limits) {
  validate(limits);
  if (!valid_id(instance)) throw std::invalid_argument("invalid gRPC instance ID");
  state_ = std::make_shared<detail::GrpcAdmissionState>(std::move(instance), limits);
}
void GrpcAdmission::set_diagnostics(Diagnostics* diagnostics, std::string service) {
  if (!valid_id(service)) throw std::invalid_argument("invalid gRPC diagnostic service identity");
  std::lock_guard lock(state_->mutex);
  if (state_->admitted || state_->active || state_->stopping)
    throw std::logic_error("gRPC diagnostics must be configured before serving");
  state_->service = std::move(service); state_->diagnostics = diagnostics;
}
GrpcCallScope GrpcAdmission::begin(grpc::ServerContext& context, bool streaming,
                                  bool discovery) {
  GrpcCallScope call;
  // Native metadata is already parsed by gRPC. Message/metadata transport caps
  // constrain allocation before this application admission stage.
  context.AddInitialMetadata("x-xrpc-instance-id", state_->instance);
  std::string instance;
  auto instance_range = context.client_metadata().equal_range(grpc::string_ref("x-xrpc-instance-id"));
  const bool instance_absent = instance_range.first == instance_range.second;
  const bool instance_duplicate = !instance_absent && ++instance_range.first != instance_range.second;
  if (streaming && discovery) {
    call.status_ = error(grpc::StatusCode::INVALID_ARGUMENT, "discovery requires a unary method");
  } else if (instance_duplicate) {
    call.status_ = error(grpc::StatusCode::INVALID_ARGUMENT, "duplicate instance metadata");
  } else if (!(discovery && instance_absent) &&
      (!one_metadata(context.client_metadata(), "x-xrpc-instance-id", instance) ||
       instance != state_->instance)) {
    call.status_ = error(grpc::StatusCode::FAILED_PRECONDITION, "instance fence mismatch");
  } else if (!one_metadata(context.client_metadata(), "x-request-id", call.request_id_)) {
    call.status_ = error(grpc::StatusCode::INVALID_ARGUMENT, "invalid request ID metadata");
  } else {
    context.AddInitialMetadata("x-request-id", call.request_id_);
    const auto now = std::chrono::system_clock::now();
    const auto native_deadline = context.deadline();
    const auto remaining = native_deadline - now;
    if (native_deadline == std::chrono::system_clock::time_point::max()) {
      call.status_ = error(grpc::StatusCode::INVALID_ARGUMENT, "finite caller deadline required");
    } else if (remaining <= std::chrono::system_clock::duration::zero()) {
      call.status_ = error(grpc::StatusCode::DEADLINE_EXCEEDED, "caller deadline expired");
    } else if (streaming && remaining > state_->limits.call_timeout) {
      call.status_ = error(grpc::StatusCode::INVALID_ARGUMENT, "stream deadline exceeds host budget");
    } else {
      call.deadline_ = GrpcClock::now() + std::min(
          std::chrono::duration_cast<GrpcClock::duration>(remaining),
          std::chrono::duration_cast<GrpcClock::duration>(state_->limits.call_timeout));
      std::lock_guard lock(state_->mutex);
      if (state_->stopping) {
        call.status_ = error(grpc::StatusCode::UNAVAILABLE, "host stopping");
      } else if (state_->active == state_->slots.size()) {
        call.status_ = error(grpc::StatusCode::RESOURCE_EXHAUSTED, "call admission exhausted");
      } else {
        auto it = std::find_if(state_->slots.begin(), state_->slots.end(),
                               [](const auto& slot) { return slot.refs == 0; });
        call.slot_ = static_cast<std::size_t>(it - state_->slots.begin());
        *it = {&context, call.deadline_, 1, false, false};
        std::copy(call.request_id_.begin(), call.request_id_.end(), it->request.begin());
        it->request_size = call.request_id_.size(); it->started = GrpcClock::now();
        call.state_ = state_; call.status_ = grpc::Status::OK;
        ++state_->active; ++state_->admitted;
        state_->emit(DiagnosticCode::CallStarted, LogSeverity::Info, &*it);
        return call;
      }
      ++state_->rejected;
      state_->emit(DiagnosticCode::AdmissionRejected, LogSeverity::Warn);
      return call;
    }
  }
  std::lock_guard lock(state_->mutex); ++state_->rejected;
  state_->emit(DiagnosticCode::AdmissionRejected, LogSeverity::Warn);
  return call;
}
void GrpcAdmission::request_stop() noexcept { state_->stop(); }
bool GrpcAdmission::wait_until(GrpcClock::time_point deadline) const { return state_->wait(deadline); }
GrpcStats GrpcAdmission::stats() const noexcept {
  std::lock_guard lock(state_->mutex);
  GrpcStats result;
  result.inflight_calls = state_->active; result.admitted_calls = state_->admitted;
  result.rejected_calls = state_->rejected; result.stopping = state_->stopping;
  return result;
}
const GrpcLimits& GrpcAdmission::limits() const noexcept { return state_->limits; }
const std::string& GrpcAdmission::instance_id() const noexcept { return state_->instance; }

class GrpcUnixServer::Impl {
public:
  std::string path;
  std::unique_ptr<UnixPathLease> lease;
  GrpcAdmission& admission;
  std::unique_ptr<grpc::Server> server;
  int listener = -1, wake = -1;
  std::vector<pollfd> polls;
  std::thread worker;
  std::atomic<bool> stopping{false};
  std::chrono::system_clock::time_point native_shutdown_deadline{};
  std::atomic<std::size_t> connections{0};
  std::atomic<std::uint64_t> accepted{0}, rejected{0};
  std::mutex mutex;
  std::mutex wake_mutex;
  std::condition_variable cv;
  bool done = false, cleaned = false;
  Impl(UnixOptions options, GrpcAdmission& a, const std::vector<grpc::Service*>& services)
      : path(options.path), lease(std::make_unique<UnixPathLease>(std::move(options))),
        admission(a), polls(a.limits().connections + 2) {
    if (services.empty() || std::any_of(services.begin(), services.end(),
                                     [](auto* service) { return !service; }))
      throw std::invalid_argument("gRPC host requires services");
    const auto& limits = admission.limits();
    grpc::ServerBuilder builder;
    for (auto* service : services) builder.RegisterService(service);
    configure_grpc_server(builder, limits);
    try {
      // No native pathname listener: gRPC must never unlink the leased path.
      server = builder.BuildAndStart();
      if (!server) throw std::runtime_error("native gRPC server failed to start");
      listener = lease->bind_stream(static_cast<int>(limits.connections));
      wake = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
      if (wake < 0) throw std::runtime_error("gRPC wake descriptor creation failed");
      polls[0] = {wake, POLLIN, 0}; polls[1] = {listener, POLLIN, 0};
      for (std::size_t i = 2; i < polls.size(); ++i) polls[i] = {-1, POLLRDHUP, 0};
      worker = std::thread([this] { run(); });
    } catch (...) {
      close_fd(listener); close_fd(wake);
      if (server) { server->Shutdown(std::chrono::system_clock::now()); server->Wait(); }
      throw;
    }
  }
  void stop() noexcept {
    admission.request_stop();
    begin_close(std::chrono::system_clock::now());
  }
  void begin_close(std::chrono::system_clock::time_point deadline) noexcept {
    // Serialize publication+write against owner closure: the worker may see
    // stopping on an unrelated poll event before this caller writes its wake.
    std::lock_guard lock(wake_mutex);
    if (!stopping.exchange(true)) {
      native_shutdown_deadline = deadline;
      const std::uint64_t one = 1;
      const auto ignored = ::write(wake, &one, sizeof(one)); (void)ignored;
    }
  }
  void close_wake() noexcept {
    std::lock_guard lock(wake_mutex);
    close_fd(wake);
  }
  void drop(std::size_t i) noexcept {
    ::shutdown(polls[i].fd, SHUT_RDWR); close_fd(polls[i].fd); --connections;
  }
  void run() noexcept {
    while (!stopping.load()) {
      const int ready = ::poll(polls.data(), polls.size(), -1);
      if (ready < 0) { if (errno == EINTR) continue; stop(); break; }
      if (stopping.load()) break;
      for (std::size_t i = 2; i < polls.size(); ++i)
        if (polls[i].fd >= 0 && (polls[i].revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL))) drop(i);
      if (polls[1].revents & (POLLERR | POLLHUP | POLLNVAL)) { stop(); break; }
      if (!(polls[1].revents & POLLIN)) continue;
      // A finite batch also permits shutdown under a connection flood.
      for (unsigned batch = 0; batch < 32 && !stopping.load(); ++batch) {
        int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
          if (errno == EINTR) continue;
          if (errno != EAGAIN && errno != EWOULDBLOCK) { stop(); }
          break;
        }
        auto slot = std::find_if(polls.begin() + 2, polls.end(), [](const auto& p) { return p.fd < 0; });
        if (slot == polls.end()) { ++rejected; ::close(fd); continue; }
        const int monitor = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (monitor < 0) { ++rejected; ::close(fd); continue; }
        *slot = {monitor, POLLRDHUP, 0}; ++connections; ++accepted;
        grpc::AddInsecureChannelFromFd(server.get(), fd); // Native runtime owns fd.
      }
    }
    close_fd(listener); polls[1].fd = -1;
    std::chrono::system_clock::time_point deadline;
    { std::lock_guard lock(wake_mutex); deadline = native_shutdown_deadline; }
    // Let admitted native calls send results during graceful shutdown. Native
    // Shutdown cancels remaining calls at its deadline but may wait for their
    // handlers beyond it; the outer owner wait remains bounded.
    server->Shutdown(deadline);
    server->Wait(); // May wait beyond deadline for noncooperative native handlers.
    for (std::size_t i = 2; i < polls.size(); ++i) if (polls[i].fd >= 0) drop(i);
    { std::lock_guard lock(mutex); done = true; }
    cv.notify_all();
  }
  bool shutdown(GrpcClock::time_point deadline, bool graceful = false) {
    if (graceful) {
      admission.state_->stop(false);
      const auto remaining = std::max(deadline - GrpcClock::now(), GrpcClock::duration::zero());
      begin_close(std::chrono::system_clock::now() + remaining);
    } else stop();
    { std::unique_lock lock(mutex);
      if (!cv.wait_until(lock, deadline, [&] { return done; })) {
        lock.unlock(); stop(); return false;
      } }
    if (!admission.wait_until(deadline)) { stop(); return false; }
    if (!cleaned) {
      worker.join(); server.reset(); lease.reset(); close_wake(); cleaned = true;
      std::lock_guard lock(admission.state_->mutex);
      admission.state_->emit(DiagnosticCode::DrainCompleted, LogSeverity::Info);
    }
    return true;
  }
  ~Impl() {
    stop();
    if (worker.joinable()) worker.join();
    admission.wait_until(GrpcClock::time_point::max());
    server.reset(); lease.reset(); close_wake(); close_fd(listener);
  }
};
GrpcUnixServer::GrpcUnixServer(UnixOptions options, GrpcAdmission& admission,
                              std::vector<grpc::Service*> services)
    : impl_(std::make_unique<Impl>(std::move(options), admission, services)) {}
GrpcUnixServer::~GrpcUnixServer() = default;
void GrpcUnixServer::request_stop() noexcept { impl_->stop(); }
bool GrpcUnixServer::drain_until(GrpcClock::time_point deadline) { return impl_->shutdown(deadline, true); }
bool GrpcUnixServer::shutdown_until(GrpcClock::time_point deadline) { return impl_->shutdown(deadline); }
bool GrpcUnixServer::shutdown() {
  return shutdown_until(GrpcClock::now() + impl_->admission.limits().shutdown_timeout);
}
GrpcStats GrpcUnixServer::stats() const noexcept {
  auto stats = impl_->admission.stats();
  stats.active_connections = impl_->connections.load();
  stats.accepted_connections = impl_->accepted.load();
  stats.rejected_connections = impl_->rejected.load();
  return stats;
}
const std::string& GrpcUnixServer::socket_path() const noexcept { return impl_->path; }
grpc::ChannelArguments grpc_channel_arguments(const GrpcLimits& limits) {
  validate(limits);
  if (limits.idle_timeout < std::chrono::seconds(1))
    throw std::invalid_argument("native client idle minimum is 1000 ms");
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(static_cast<int>(limits.response_bytes));
  args.SetMaxSendMessageSize(static_cast<int>(limits.request_bytes));
  args.SetInt(GRPC_ARG_MAX_METADATA_SIZE, static_cast<int>(limits.header_bytes));
  args.SetInt(GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS, static_cast<int>(limits.idle_timeout.count()));
  args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
  grpc::ResourceQuota quota;
  quota.Resize(limits.native_memory_bytes).SetMaxThreads(limits.native_threads);
  args.SetResourceQuota(quota);
  return args;
}
std::shared_ptr<grpc::Channel> make_grpc_unix_channel(const std::string& path,
                                                    const GrpcLimits& limits) {
  if (path.empty() || path[0] != '/' || path.find('\0') != std::string::npos)
    throw std::invalid_argument("local absolute Unix path required");
  return grpc::CreateCustomChannel("unix:" + path, grpc::InsecureChannelCredentials(),
                                   grpc_channel_arguments(limits));
}
class GrpcClientCall::Impl {
public:
  struct Cancel { grpc::ClientContext* context; void operator()() const noexcept { context->TryCancel(); } };
  grpc::ClientContext& context;
  std::string instance, request, response_instance;
  GrpcClock::time_point deadline;
  std::stop_token cancellation;
  bool dispatched = false, discovery = false, stream_mode_rejected = false;
  // Destruction unregisters/synchronizes callback before context may die.
  std::unique_ptr<std::stop_callback<Cancel>> callback;
  Impl(grpc::ClientContext& c, std::string i, GrpcClock::time_point deadline,
       std::stop_token cancellation, std::string id, bool discover)
      : context(c), instance(std::move(i)), request(std::move(id)),
        deadline(deadline), cancellation(cancellation), discovery(discover) {
    if (request.empty()) request = new_instance_id();
    if ((!discovery || !instance.empty()) && !valid_id(instance))
      throw std::invalid_argument("invalid gRPC instance identity");
    if (!valid_id(request)) throw std::invalid_argument("invalid gRPC request identity");
    const auto now = GrpcClock::now();
    if (deadline == GrpcClock::time_point::max() || deadline <= now ||
        deadline - now > std::chrono::hours(24))
      throw std::invalid_argument("finite gRPC deadline within 24 hours required");
    context.set_deadline(std::chrono::system_clock::now() + (deadline - now));
    if (!instance.empty()) context.AddMetadata("x-xrpc-instance-id", instance);
    context.AddMetadata("x-request-id", request);
    callback = std::make_unique<std::stop_callback<Cancel>>(cancellation, Cancel{&context});
  }
};
GrpcClientCall::GrpcClientCall(grpc::ClientContext& context, std::string instance,
    GrpcClock::time_point deadline, std::stop_token cancellation, std::string request,
    bool discovery)
    : impl_(std::make_unique<Impl>(context, std::move(instance), deadline, cancellation,
                                 std::move(request), discovery)) {}
GrpcClientCall::~GrpcClientCall() = default;
const std::string& GrpcClientCall::request_id() const noexcept { return impl_->request; }
const std::string& GrpcClientCall::response_instance_id() const noexcept { return impl_->response_instance; }
grpc::Status GrpcClientCall::mark_dispatched() {
  if (impl_->stream_mode_rejected)
    return error(grpc::StatusCode::INVALID_ARGUMENT, "discovery requires a unary method");
  if (impl_->dispatched) throw std::logic_error("native gRPC call already dispatched");
  if (impl_->cancellation.stop_requested())
    return error(grpc::StatusCode::CANCELLED, "call cancelled before dispatch");
  if (GrpcClock::now() >= impl_->deadline)
    return error(grpc::StatusCode::DEADLINE_EXCEEDED, "deadline expired before dispatch");
  impl_->dispatched = true;
  return grpc::Status::OK;
}
GrpcDelivery GrpcClientCall::delivery() const noexcept {
  return impl_->dispatched ? GrpcDelivery::OutcomeUnknown : GrpcDelivery::NotSent;
}
grpc::Status GrpcClientCall::check_stream_mode() const {
  if (!impl_->discovery) return grpc::Status::OK;
  impl_->stream_mode_rejected = true;
  impl_->response_instance.clear();
  impl_->context.TryCancel();
  return error(grpc::StatusCode::INVALID_ARGUMENT, "discovery requires a unary method");
}
grpc::Status GrpcClientCall::verify_initial_metadata() const {
  const auto mode = check_stream_mode();
  if (!mode.ok()) return mode;
  impl_->response_instance.clear();
  const auto& metadata = impl_->context.GetServerInitialMetadata();
  std::string instance, request;
  if (!one_metadata(metadata, "x-xrpc-instance-id", instance) || instance != impl_->instance ||
      !one_metadata(metadata, "x-request-id", request) || request != impl_->request) {
    impl_->context.TryCancel();
    return error(grpc::StatusCode::FAILED_PRECONDITION, "stream response identity mismatch");
  }
  impl_->response_instance = std::move(instance);
  return grpc::Status::OK;
}
grpc::Status GrpcClientCall::verify(grpc::Status status) const {
  impl_->response_instance.clear();
  if (impl_->stream_mode_rejected)
    return error(grpc::StatusCode::INVALID_ARGUMENT, "discovery requires a unary method");
  const auto& metadata = impl_->context.GetServerInitialMetadata();
  if (metadata.empty() && !status.ok()) return status;
  std::string instance, request;
  if (!one_metadata(metadata, "x-xrpc-instance-id", instance) ||
      ((!impl_->discovery || !impl_->instance.empty()) && instance != impl_->instance))
    return error(grpc::StatusCode::FAILED_PRECONDITION, "response instance fence mismatch");
  // A rejected call may lack valid request metadata. Preserve its native error.
  if (!one_metadata(metadata, "x-request-id", request) || request != impl_->request)
    return status.ok() ? error(grpc::StatusCode::FAILED_PRECONDITION, "response request ID mismatch") : status;
  if (status.ok()) impl_->response_instance = std::move(instance);
  return status;
}
} // namespace xgc2::xrpc
