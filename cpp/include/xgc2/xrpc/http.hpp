#pragma once
#include "delivery.hpp"
#include "stop.hpp"
#include "unix.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace xgc2 {
namespace xrpc {
class Diagnostics;
using Clock = std::chrono::steady_clock;
using Headers = std::vector<std::pair<std::string, std::string>>;
// Hard bounds for a host or a client. The defaults suit a small private
// control service; an owner that needs other bounds sets them explicitly.
// The library reads no environment variables and keeps no hidden policy.
struct HttpLimits {
  std::size_t connections = 32;          // accepted connections at one time
  std::size_t header_bytes = 16384;      // header section of a request/response
  std::size_t request_bytes = 1048576;   // request body
  std::size_t response_bytes = 1048576;  // response body
  std::size_t inflight = 32;             // admitted calls without a reply yet
  std::chrono::milliseconds request_timeout{30000}; // server cap on one call
  std::chrono::milliseconds idle_timeout{30000};    // wait for the next request
  std::chrono::milliseconds header_timeout{5000};   // receive a full header
  std::chrono::milliseconds shutdown_timeout{5000}; // default drain() budget
};
struct HttpIdentity {
  std::string instance_id;
  std::vector<std::string> discovery_targets;
};
struct HttpRequest {
  std::string method, target, body, request_id;
  Headers headers;
  Clock::time_point deadline;
};
struct HttpResponse {
  int status = 200;
  Headers headers;
  std::string body;
  bool keep_alive = true;
};
HttpResponse http_error(int status, const std::string &code,
                        const std::string &message);
class HttpReply {
public:
  HttpReply() = default;
  // At most one caller wins. Safe from any thread; completion is dispatched
  // back to the host's IO loop. False means already completed or cancelled.
  bool complete(HttpResponse response) const;
  bool cancelled() const noexcept;
  // All copies retain host admission and the endpoint lease, even after
  // cancellation. Release a completed/cancelled reply when its business work
  // has actually ended; cancellation alone does not prove quiescence.

private:
  struct State;
  explicit HttpReply(std::shared_ptr<State> state);
  std::shared_ptr<State> state_;
  friend class HttpServer;
};
struct HttpStats {
  std::uint64_t accepted_connections = 0, rejected_connections = 0,
                completed_calls = 0;
  std::size_t active_connections = 0, inflight_calls = 0;
  std::uint64_t admitted_calls = 0, rejected_calls = 0,
                deadline_exceeded = 0, idle_timeouts = 0,
                cancelled_calls = 0, malformed_requests = 0, peer_errors = 0;
  Clock::time_point source_time{};
  bool stopping = false;
};
class HttpServer {
public:
  using Handler = std::function<void(HttpRequest, HttpReply)>;
  HttpServer(UnixOptions options, Handler handler, HttpLimits limits = {},
             HttpIdentity identity = {});
  // Borrow a resolved runtime-directory grant. The host duplicates and pins
  // the descriptor, verifies it owns the endpoint's parent, and retains it
  // through actual admitted work. The caller still owns its descriptor.
  HttpServer(UnixOptions options, Handler handler, HttpLimits limits,
             HttpIdentity identity, int retained_parent_fd);
  ~HttpServer();
  HttpServer(const HttpServer &) = delete;
  HttpServer &operator=(const HttpServer &) = delete;
  // run/poll and destruction belong to one owner. They must not race each
  // other. Only request_stop, wake, reply completion and stats are
  // cross-thread.
  void run();
  void poll(std::chrono::milliseconds maximum_wait = std::chrono::milliseconds{
                0});
  // Configure once on the owner before run. wake() coalesces notifications,
  // performs no allocation or waiting, and is safe from a realtime producer.
  // Producers must stop before destruction. The callback runs on the owner.
  void set_wakeup_handler(std::function<void()> handler);
  // Configure on the owner before serving. The shared diagnostics owner must
  // outlive this host; producers only enqueue fixed redacted records.
  void set_diagnostics(Diagnostics *diagnostics, std::string service = {});
  void wake() noexcept;
  // Immediate cross-thread cancellation; does not wait for business work.
  void request_stop() noexcept;
  // Owner-thread graceful shutdown: stop admission, close idle peers, let
  // already admitted replies finish until deadline, then cancel the remainder.
  // Handler/wakeup callbacks must hand off bounded work and never block IO.
  bool drain_until(Clock::time_point deadline);
  bool drain();
  HttpStats stats() const noexcept;
  const std::string &socket_path() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
// A call that produced no usable HTTP response. `delivery` is NotSent when no
// request bytes were written, OutcomeUnknown when they may have reached the
// server (timeout, reset, a response that is not from the pinned instance or
// not for this request), and ResponseReceived when a response arrived but the
// client cannot hand it over (its body exceeds the client's response limit).
// A response with any HTTP status, including 4xx and 5xx, is not an error: call
// returns it, and such a result is a received response by definition.
class HttpCallError : public std::runtime_error {
public:
  HttpCallError(std::string code, Delivery delivery, const std::string &detail);
  const std::string code;
  const Delivery delivery;
};
// Calls serialize on one reusable HTTP connection. Failed calls are never
// replayed, including failures on a stale keep-alive connection.
class HttpClient {
public:
  explicit HttpClient(std::string socket_path, HttpLimits limits = {},
                      std::string instance_id = {});
  ~HttpClient();
  HttpClient(const HttpClient &) = delete;
  HttpClient &operator=(const HttpClient &) = delete;
  HttpResponse call(HttpRequest request, Clock::time_point deadline,
                    StopToken cancellation = {});
  // Terminal: cancel the active call and reject queued/future calls. Join all
  // calling threads before destruction; close itself does not wait for them.
  void close() noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace xrpc
} // namespace xgc2
