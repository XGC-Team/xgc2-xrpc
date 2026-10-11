#pragma once
// Serves a MethodRouter on a udp.v1 server. Header only; link XgcXrpc::udp.
#include "method.hpp"
#include "udp.hpp"
#include <mutex>

namespace xgc2::xrpc {
namespace method_detail {
class UdpSink final : public MethodReply::Sink {
public:
  explicit UdpSink(udp::Reply reply) : reply_(std::move(reply)) {}
  bool complete(std::string_view json) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return reply_.complete(udp::Status::Ok, json);
  }
  bool error(MethodCode code, std::string_view message, std::string_view details_json) override {
    // A domain that says "unauthenticated" has no wire form here: the carrier
    // authenticates, and a caller that got this far was authenticated.
    if (code == MethodCode::Unauthenticated) code = MethodCode::PermissionDenied;
    std::lock_guard<std::mutex> lock(mutex_);
    return reply_.error(static_cast<udp::Status>(code), message, details_json);
  }
  // A datagram has no connection to lose.
  bool cancelled() const noexcept override { return false; }

private:
  std::mutex mutex_; // udp::Reply is one object; copies of the MethodReply share it
  udp::Reply reply_;
};
} // namespace method_detail

// Registers every method of the router with the server under its own name.
// Call it before Server::start(). The handlers are copied. They run on the
// server's I/O thread and must not block it: hand slow work to another thread
// and complete the MethodReply from there before MethodRequest::deadline. A
// handler that throws before answering is answered internal.
inline void add_methods(udp::Server &server, const MethodRouter &router) {
  for (const auto &name : router.names()) {
    server.add_method(name, [handler = *router.find(name)](udp::Request request, udp::Reply reply) {
      MethodRequest call{std::move(request.method), std::move(request.body), udp::to_hex(request.request_id), request.deadline};
      MethodReply sink(std::make_shared<method_detail::UdpSink>(std::move(reply)));
      try {
        handler(std::move(call), sink);
      } catch (...) {
        sink.error(MethodCode::Internal, "handler failed");
      }
    });
  }
}
} // namespace xgc2::xrpc
