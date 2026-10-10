#include "udp_net.hpp"
#include "udp_wire.hpp"
#include <poll.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace xgc2::xrpc::udp {
namespace {
using SteadyClock = std::chrono::steady_clock;

struct Socket {
  int fd;
  explicit Socket(int value) : fd(value) {}
  Socket(const Socket &) = delete;
  Socket &operator=(const Socket &) = delete;
  ~Socket() {
    if (fd >= 0) ::close(fd);
  }
};

Response not_sent(Status status, std::string message) {
  Response response;
  response.delivery = Delivery::NotSent;
  response.status = status;
  response.message = std::move(message);
  return response;
}

// Milliseconds to wait until `target`, rounded up so the loop never spins.
int wait_ms(SteadyClock::time_point now, SteadyClock::time_point target) {
  const auto wait = std::chrono::ceil<std::chrono::milliseconds>(target - now);
  return static_cast<int>(std::max<std::int64_t>(1, wait.count()));
}
} // namespace

Client::Client(KeyRing keys, ClientOptions options)
    : keys_(std::move(keys)), options_(std::move(options)) {
  const auto positive = [](std::chrono::milliseconds value) { return value.count() > 0; };
  if (!positive(options_.steady_interval) ||
      !std::all_of(options_.backoff.begin(), options_.backoff.end(), positive))
    throw std::invalid_argument("retransmission intervals must be positive");
}

Response Client::call(std::string_view endpoint, std::uint32_t key_id, std::string_view method,
                      std::string_view body, SteadyClock::time_point deadline,
                      const std::optional<InstanceId> &expected_instance) const {
  const detail::Key *key = keys_.find(key_id);
  if (!key) return not_sent(Status::InvalidArgument, "no key for this key_id");
  const auto target = detail::parse_endpoint(endpoint);
  if (!target) return not_sent(Status::InvalidArgument, "endpoint must be a numeric host:port");
  if (method.empty() || method.size() > max_method_bytes)
    return not_sent(Status::InvalidArgument, "method must be 1..128 bytes");
  if (body.size() > detail::max_body_bytes(method.size()))
    return not_sent(Status::ResourceExhausted, "request does not fit one 1200-byte datagram");
  if (deadline == SteadyClock::time_point::max())
    return not_sent(Status::InvalidArgument, "a finite deadline is required");
  auto now = SteadyClock::now();
  if (deadline <= now) return not_sent(Status::DeadlineExceeded, "deadline already passed");
  deadline = std::min<SteadyClock::time_point>(
      deadline, now + std::chrono::milliseconds(max_timeout_ms));
  const auto timeout_ms = static_cast<std::uint32_t>(std::clamp<std::int64_t>(
      std::chrono::ceil<std::chrono::milliseconds>(deadline - now).count(), 1, max_timeout_ms));

  RequestId request_id;
  detail::random_bytes(request_id.data(), request_id.size());
  const auto datagram = detail::encode_request(*key, key_id, request_id, expected_instance,
                                               timeout_ms, method, body);
  Socket socket(::socket(target->family(), SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (socket.fd < 0)
    return not_sent(Status::Unavailable, std::string("socket: ") + std::strerror(errno));

  Response response;
  std::string last_error;
  std::array<std::uint8_t, 2048> buffer;
  std::size_t step = 0;
  auto next_send = now;
  while ((now = SteadyClock::now()) < deadline) {
    if (now >= next_send) {
      const auto n = ::sendto(socket.fd, datagram.data(), datagram.size(), MSG_NOSIGNAL,
                              target->get(), target->size);
      if (n == static_cast<ssize_t>(datagram.size())) ++response.attempts;
      else last_error = std::strerror(errno);
      next_send = now + (step < options_.backoff.size() ? options_.backoff[step]
                                                        : options_.steady_interval);
      ++step;
    }
    pollfd ready{socket.fd, POLLIN, 0};
    const int events = ::poll(&ready, 1, wait_ms(now, std::min(next_send, deadline)));
    if (events <= 0) continue;
    for (;;) {
      const auto n = ::recvfrom(socket.fd, buffer.data(), buffer.size(), 0, nullptr, nullptr);
      if (n < 0) {
        if (errno == EINTR) continue;
        break; // EAGAIN, or an ICMP error that says nothing about a reply
      }
      detail::Datagram reply;
      // Anything that is not an authentic answer to this very request is
      // ignored, whoever sent it: the tag, not the source address, decides.
      if (detail::parse(buffer.data(), static_cast<std::size_t>(n), reply) != detail::Parse::Ok ||
          reply.type != detail::Type::Reply || reply.key_id != key_id ||
          reply.request_id != request_id || reply.flags != 0 || reply.word > 10 ||
          !detail::verify(reply, *key))
        continue;
      const auto status = static_cast<Status>(reply.word);
      // A pinned client that reaches another instance gets conflict from it.
      if (expected_instance && reply.instance != *expected_instance && status != Status::Conflict)
        continue;
      response.delivery = Delivery::ResponseReceived;
      response.status = status;
      response.body.assign(reply.body);
      response.instance = reply.instance;
      return response;
    }
  }
  if (response.attempts == 0) {
    response.delivery = Delivery::NotSent;
    response.status = Status::Unavailable;
    response.message = "no datagram could be sent: " + (last_error.empty() ? "deadline too short" : last_error);
  } else {
    response.delivery = Delivery::OutcomeUnknown;
    response.status = Status::DeadlineExceeded;
    response.message = "no valid reply before the deadline";
  }
  return response;
}
} // namespace xgc2::xrpc::udp
