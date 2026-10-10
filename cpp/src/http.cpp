#include "xgc2/xrpc/http.hpp"
#include "xgc2/xrpc/diagnostics.hpp"
#include <algorithm>
#include <boost/asio.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <sys/eventfd.h>
#include <unistd.h>

namespace xgc2 {
namespace xrpc {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using Local = net::local::stream_protocol;
using Error = boost::system::error_code;
namespace {
void validate(const HttpLimits &l) {
  if (!l.connections || !l.inflight || l.header_bytes < 256 ||
      l.header_bytes > 1048576 || !l.request_bytes || l.response_bytes < 256 ||
      l.request_timeout.count() <= 0 || l.idle_timeout.count() <= 0 ||
      l.header_timeout.count() <= 0 || l.shutdown_timeout.count() <= 0)
    throw std::invalid_argument("invalid HTTP host limits");
}
std::string escaped(const std::string &input) {
  std::string out;
  for (const unsigned char c : input) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += static_cast<char>(c);
    } else if (c < 32)
      out += ' ';
    else
      out += static_cast<char>(c);
  }
  return out;
}
bool valid_id(const std::string &value) {
  return !value.empty() && value.size() <= 128 &&
         std::all_of(value.begin(), value.end(),
                     [](unsigned char c) {
                       return (c >= 'a' && c <= 'z') ||
                              (c >= 'A' && c <= 'Z') ||
                              (c >= '0' && c <= '9') || c == '.' ||
                              c == '_' || c == ':' || c == '-';
                     });
}
std::chrono::milliseconds parse_timeout(beast::string_view value) {
  if (value.empty() || value.size() > 8 || value.front() == '0')
    throw std::invalid_argument("invalid X-Xrpc-Timeout-Ms");
  std::uint64_t n = 0;
  for (const char c : value) {
    if (c < '0' || c > '9')
      throw std::invalid_argument("invalid X-Xrpc-Timeout-Ms");
    n = n * 10 + static_cast<unsigned>(c - '0');
  }
  if (!n || n > 86400000)
    throw std::invalid_argument("invalid X-Xrpc-Timeout-Ms");
  return std::chrono::milliseconds(n);
}
bool valid_target(const std::string &target) {
  return !target.empty() && target.front() == '/' &&
         std::all_of(target.begin(), target.end(), [](unsigned char c) {
           return c > 32 && c < 127 && c != '#';
         });
}
void validate_header(const std::string &name, const std::string &value) {
  const auto token = [](unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') ||
           (c != 0 && std::strchr("!#$%&'*+-.^_`|~", c) != nullptr);
  };
  if (name.empty() || !std::all_of(name.begin(), name.end(), token) ||
      !std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return (c >= 32 && c != 127) || c == '\t';
      }))
    throw std::invalid_argument("invalid HTTP header name or value");
}
void signal_fd(int fd) noexcept {
  const std::uint64_t one = 1;
  ssize_t count;
  do {
    count = ::write(fd, &one, sizeof(one));
  } while (count < 0 && errno == EINTR);
  // EAGAIN is safe: an existing nonzero counter will wake the owner.
}
bool reserved_header(const std::string &name) {
  return beast::iequals(name, "Content-Length") ||
         beast::iequals(name, "Transfer-Encoding") ||
         beast::iequals(name, "Connection") ||
         beast::iequals(name, "X-Xrpc-Instance-ID") ||
         beast::iequals(name, "X-Request-ID") ||
         beast::iequals(name, "X-Xrpc-Timeout-Ms");
}
} // namespace
HttpResponse http_error(int status, const std::string &code,
                        const std::string &message) {
  HttpResponse r;
  r.status = status;
  r.headers.emplace_back("Content-Type", "application/json");
  r.body = "{\"error\":{\"code\":\"" + escaped(code) + "\",\"message\":\"" +
           escaped(message) + "\"}}";
  return r;
}
// Survives the IO owner while admitted business work retains a reply. The
// endpoint cannot be replaced by a new owner until that work releases it.
struct HttpWorkLifetime {
  explicit HttpWorkLifetime(UnixOptions options)
      : lease(std::move(options)) {}
  HttpWorkLifetime(UnixOptions options, int retained_parent_fd)
      : lease(std::move(options), retained_parent_fd) {}
  void release() noexcept {
    std::lock_guard<std::mutex> guard(mutex);
    --inflight;
    if (stopped && inflight.load() == 0)
      lease.cleanup();
  }
  void stop() noexcept {
    std::lock_guard<std::mutex> guard(mutex);
    stopped = true;
    if (inflight.load() == 0)
      lease.cleanup();
  }
  UnixPathLease lease;
  std::atomic<std::size_t> inflight{0};
  std::mutex mutex;
  bool stopped = false;
};
struct HttpReply::State {
  ~State() {
    if (work) {
      work->release();
      if (notify_release)
        notify_release();
    }
  }
  std::atomic<bool> finished{false}, cancelled{false};
  std::function<bool(HttpResponse)> dispatch;
  std::shared_ptr<HttpWorkLifetime> work;
  std::function<void()> notify_release;
};
HttpReply::HttpReply(std::shared_ptr<State> state) : state_(std::move(state)) {}
bool HttpReply::complete(HttpResponse response) const {
  if (!state_ || state_->cancelled.load())
    return false;
  bool expected = false;
  if (!state_->finished.compare_exchange_strong(expected, true))
    return false;
  return state_->dispatch(std::move(response));
}
bool HttpReply::cancelled() const noexcept {
  return !state_ || state_->cancelled.load();
}

class HttpServer::Impl {
  struct Dispatch {
    std::mutex mutex;
    std::function<void(std::function<void()>)> post;
    std::function<void()> wake;
  };
  class Session : public std::enable_shared_from_this<Session> {
  public:
    Session(Impl &o, Local::socket s, std::uint64_t key)
        : owner(o), socket(std::move(s)), timer(o.io),
          buffer(o.limits.header_bytes + 8192), id(key) {}
    ~Session() {
      if (reply_state)
        reply_state->cancelled.store(true);
    }
    void arm(Clock::time_point when, bool idle = false) {
      timer.expires_at(when);
      const auto self = shared_from_this();
      timer.async_wait([self, idle](Error error) {
        if (!error) {
          if (idle)
            ++self->owner.idle_expired;
          else
            ++self->owner.expired;
          if (!idle)
            self->owner.emit(DiagnosticCode::DeadlineExceeded, LogSeverity::Warn,
                             self->request_id, self->started);
          self->close();
        }
      });
    }
    void read() {
      if (closed || owner.stopping.load()) {
        close();
        return;
      }
      parser.reset(new http::request_parser<http::string_body>);
      parser->header_limit(
          static_cast<std::uint32_t>(owner.limits.header_bytes));
      parser->body_limit(owner.limits.request_bytes);
      request_id.clear();
      deadline = Clock::time_point{};
      if (buffer.size()) {
        read_headers();
        return;
      }
      arm(Clock::now() + owner.limits.idle_timeout, true);
      const auto self = shared_from_this();
      socket.async_wait(Local::socket::wait_read, [self](Error error) {
        if (error || self->closed) {
          self->close();
          return;
        }
        self->read_headers();
      });
    }
    void read_headers() {
      if (closed || owner.stopping.load()) {
        close();
        return;
      }
      started = Clock::now();
      deadline = started + owner.limits.header_timeout;
      arm(deadline);
      const auto self = shared_from_this();
      http::async_read_header(socket, buffer, *parser,
                              [self](Error error, std::size_t) {
                                if (error) {
                                  self->read_error(error);
                                  return;
                                }
                                self->headers();
                              });
    }
    void read_error(Error error) {
      if (closed)
        return;
      if (error == http::error::end_of_stream ||
          error == net::error::operation_aborted || error == net::error::eof) {
        close();
        return;
      }
      // Beast can reject Content-Length before headers() runs. Correlate a
      // single valid field it already parsed without dispatching the request.
      const auto &request = parser->get();
      if (request.count("X-Request-ID") == 1 &&
          valid_id(std::string(request["X-Request-ID"])))
        request_id = std::string(request["X-Request-ID"]);
      int status = 400;
      if (error == http::error::body_limit)
        status = 413;
      else if (error == http::error::header_limit ||
               error == http::error::buffer_overflow)
        status = 431;
      auto response = http_error(
          status, status == 400 ? "invalid_argument" : "resource_exhausted",
          error.message());
      ++owner.malformed;
      response.keep_alive = false;
      write(std::move(response));
    }
    void headers() {
      if (closed)
        return;
      const auto &request = parser->get();
      try {
        if (request.version() != 11 ||
            !valid_target(std::string(request.target())))
          throw std::invalid_argument("HTTP/1.1 origin-form target required");
        const auto &identity = owner.identity;
        // Correlate infrastructure rejections as well as admitted calls.
        if (request.count("X-Request-ID") == 1 &&
            valid_id(std::string(request["X-Request-ID"])))
          request_id = std::string(request["X-Request-ID"]);
        const bool discovery = request.method() == http::verb::get &&
                               std::find(identity.discovery_targets.begin(),
                                         identity.discovery_targets.end(),
                                         std::string(request.target())) !=
                                   identity.discovery_targets.end();
        const auto instance_count = request.count("X-Xrpc-Instance-ID");
        if (instance_count > 1)
          throw std::invalid_argument("one instance ID is permitted");
        if (!identity.instance_id.empty() &&
            ((!discovery && instance_count != 1) ||
             (instance_count != 0 &&
              (instance_count != 1 ||
               request["X-Xrpc-Instance-ID"] != identity.instance_id)))) {
          auto response =
              http_error(409, "conflict", "service instance identity mismatch");
          response.keep_alive = false;
          write(std::move(response));
          return;
        }
        if (request.count("X-Xrpc-Timeout-Ms") != 1 ||
            request.count("X-Request-ID") != 1)
          throw std::invalid_argument(
              "one request ID and finite timeout are required");
        deadline = started + std::min(
            owner.limits.request_timeout,
            parse_timeout(request["X-Xrpc-Timeout-Ms"]));
        request_id = std::string(request["X-Request-ID"]);
        if (!valid_id(request_id))
          throw std::invalid_argument("invalid X-Request-ID");
        if (request.count(http::field::expect))
          throw std::invalid_argument("Expect is not supported");
        if (Clock::now() >= deadline) {
          write(
              http_error(504, "deadline_exceeded", "request deadline elapsed"));
          return;
        }
        arm(deadline);
        const auto self = shared_from_this();
        if (parser->is_done())
          dispatch();
        else
          http::async_read(socket, buffer, *parser,
                           [self](Error error, std::size_t) {
                             if (error)
                               self->read_error(error);
                             else
                               self->dispatch();
                           });
      } catch (const std::exception &e) {
        ++owner.malformed;
        auto response = http_error(400, "invalid_argument", e.what());
        response.keep_alive = false;
        write(std::move(response));
      }
    }
    void dispatch() {
      if (closed)
        return;
      if (Clock::now() >= deadline) {
        close();
        return;
      }
      if (owner.work->inflight.load() >= owner.limits.inflight) {
        ++owner.rejected_calls;
        if (!owner.admission_overloaded) {
          owner.admission_overloaded = true;
          owner.emit(DiagnosticCode::AdmissionRejected, LogSeverity::Warn,
                     request_id, started);
        }
        write(http_error(503, "resource_exhausted",
                         "host in-flight limit reached"));
        return;
      }
      admitted = true;
      HttpRequest request;
      auto &source = parser->get();
      request.method = std::string(source.method_string());
      request.target = std::string(source.target());
      request.body = std::move(source.body());
      request.request_id = request_id;
      request.deadline = deadline;
      for (const auto &h : source.base())
        request.headers.emplace_back(std::string(h.name_string()),
                                     std::string(h.value()));
      reply_state = std::make_shared<HttpReply::State>();
      const std::weak_ptr<Session> weak = shared_from_this();
      const auto gate = owner.dispatcher;
      reply_state->notify_release = [gate] {
        std::lock_guard<std::mutex> guard(gate->mutex);
        if (gate->wake)
          gate->wake();
      };
      ++owner.work->inflight;
      ++owner.admitted_calls;
      owner.emit(DiagnosticCode::CallStarted, LogSeverity::Debug, request_id, started);
      reply_state->work = owner.work;
      reply_state->dispatch = [weak, gate](HttpResponse response) {
        std::lock_guard<std::mutex> guard(gate->mutex);
        const auto session = weak.lock();
        if (!gate->post || !session || session->closed.load())
          return false;
        gate->post([session, response = std::move(response)]() mutable {
          if (!session->closed)
            session->write(std::move(response));
        });
        return true;
      };
      // A held reply still observes caller disconnect; the deadline remains
      // the upper bound when pipelined bytes are waiting in the receive queue.
      const auto self = shared_from_this();
      socket.async_wait(Local::socket::wait_read, [self](Error error) {
        if (error || self->closed || self->writing)
          return;
        char byte;
        const auto count = ::recv(self->socket.native_handle(), &byte, 1,
                                  MSG_PEEK | MSG_DONTWAIT);
        if (count == 0 || (count < 0 && errno != EAGAIN &&
                           errno != EWOULDBLOCK && errno != EINTR))
          self->close();
      });
      try {
        owner.handler(std::move(request), HttpReply(reply_state));
      } catch (const std::exception &) {
        HttpReply(reply_state)
            .complete(http_error(500, "internal", "handler failed"));
      } catch (...) {
        HttpReply(reply_state)
            .complete(http_error(500, "internal", "handler failed"));
      }
    }
    void write(HttpResponse response) {
      if (closed || writing)
        return;
      writing = true;
      Error ignored;
      socket.cancel(ignored);
      if (response.body.size() > owner.limits.response_bytes)
        response = http_error(500, "resource_exhausted",
                              "response body exceeds host limit");
      if (response.status < 200 || response.status > 599)
        response = http_error(500, "internal", "invalid response status");
      message.reset(new http::response<http::string_body>(
          static_cast<http::status>(response.status), 11));
      std::size_t header_bytes = 0;
      try {
        for (const auto &h : response.headers) {
          header_bytes += h.first.size() + h.second.size() + 4;
          if (header_bytes > owner.limits.header_bytes)
            throw std::length_error("response headers exceed host limit");
          validate_header(h.first, h.second);
          if (!reserved_header(h.first))
            message->set(h.first, h.second);
        }
      } catch (...) {
        response = http_error(500, "internal", "invalid response headers");
        message.reset(new http::response<http::string_body>(
            http::status::internal_server_error, 11));
        message->set(http::field::content_type, "application/json");
      }
      if (!owner.identity.instance_id.empty())
        message->set("X-Xrpc-Instance-ID", owner.identity.instance_id);
      message->set("X-Request-ID",
                   valid_id(request_id) ? request_id : "xrpc-error");
      const bool keep = response.keep_alive && parser && parser->is_done() &&
                        parser->get().keep_alive() && !owner.stopping.load();
      message->keep_alive(keep);
      const bool head = parser && parser->get().method() == http::verb::head;
      const bool carries_body =
          response.status != 204 && response.status != 304;
      if (!head && carries_body)
        message->body() = std::move(response.body);
      message->prepare_payload();
      if (head && carries_body)
        message->content_length(response.body.size());
      // Errors discovered before a request budget is parsed also get finite
      // I/O.
      if (deadline == Clock::time_point{})
        deadline = Clock::now() + owner.limits.request_timeout;
      arm(deadline);
      const auto self = shared_from_this();
      http::async_write(socket, *message,
                        [self, keep](Error error, std::size_t) {
                          if (self->closed)
                            return;
                          if (error && error != net::error::operation_aborted)
                            ++self->owner.peer_errors;
                          if (self->admitted) {
                            self->admitted = false;
                            self->owner.emit(error ? DiagnosticCode::PeerError : DiagnosticCode::CallCompleted,
                                             error ? LogSeverity::Warn : LogSeverity::Info,
                                             self->request_id, self->started);
                          }
                          if (self->reply_state)
                            self->reply_state->cancelled.store(true);
                          self->reply_state.reset();
                          ++self->owner.completed;
                          self->writing = false;
                          self->message.reset();
                          if (error || !keep || self->owner.stopping.load())
                            self->close();
                          else
                            self->read();
                        });
    }
    bool is_admitted() const noexcept { return admitted; }
    void close() {
      if (closed.exchange(true))
        return;
      if (reply_state)
        reply_state->cancelled.store(true);
      if (reply_state && !reply_state->finished.load())
        ++owner.cancelled_calls;
      if (reply_state && !reply_state->finished.load())
        owner.emit(DiagnosticCode::CallCancelled, LogSeverity::Info, request_id, started);
      reply_state.reset();
      if (admitted) {
        admitted = false;
      }
      Error ignored;
      timer.cancel(ignored);
      socket.cancel(ignored);
      socket.close(ignored);
      owner.sessions.erase(id);
      --owner.active;
      if (owner.stopping.load() && owner.sessions.empty())
        owner.finish_stop();
    }
    Impl &owner;
    Local::socket socket;
    net::steady_timer timer;
    beast::flat_buffer buffer;
    std::unique_ptr<http::request_parser<http::string_body>> parser;
    std::unique_ptr<http::response<http::string_body>> message;
    std::shared_ptr<HttpReply::State> reply_state;
    std::uint64_t id;
    std::string request_id;
    Clock::time_point started{}, deadline{};
    std::atomic<bool> closed{false};
    bool admitted = false, writing = false;
  };

public:
  void emit(DiagnosticCode code, LogSeverity severity,
            std::string_view request = {}, Clock::time_point began = {}) noexcept {
    if (diagnostics)
      diagnostics->try_emit(code, severity,
          {diagnostic_service, identity.instance_id, request},
          began == Clock::time_point{} ? std::chrono::nanoseconds{}
                                      : std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - began));
  }
  Impl(UnixOptions options, Handler h, HttpLimits l,
       HttpIdentity identity_value, int retained_parent_fd = -1)
      : identity(std::move(identity_value)), limits(l), handler(std::move(h)),
        work(retained_parent_fd < 0
                 ? std::make_shared<HttpWorkLifetime>(std::move(options))
                 : std::make_shared<HttpWorkLifetime>(std::move(options),
                                                      retained_parent_fd)),
        acceptor(io), notification(io),
        dispatcher(std::make_shared<Dispatch>()) {
    validate(limits);
    if (!identity.instance_id.empty() && !valid_id(identity.instance_id))
      throw std::invalid_argument("invalid instance identity");
    if (!handler)
      throw std::invalid_argument("HTTP handler is required");
    const int fd = work->lease.bind_stream(
        static_cast<int>(std::min<std::size_t>(limits.connections, 4096)));
    Error error;
    acceptor.assign(Local(), fd, error);
    if (error) {
      ::close(fd);
      throw boost::system::system_error(error);
    }
    const int notification_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notification_fd < 0)
      throw std::system_error(errno, std::generic_category(), "eventfd");
    notification.assign(notification_fd, error);
    if (error) {
      ::close(notification_fd);
      throw boost::system::system_error(error);
    }
    listen_notification();
    dispatcher->post = [this](std::function<void()> task) {
      net::post(io, std::move(task));
    };
    dispatcher->wake = [this] { signal_fd(notification.native_handle()); };
    accept();
  }
  ~Impl() {
    {
      std::lock_guard<std::mutex> guard(dispatcher->mutex);
      dispatcher->post = {};
      dispatcher->wake = {};
    }
    stopping.store(true);
    stop();
    io.restart();
    io.poll();
  }
  void listen_notification() {
    net::async_read(
        notification,
        net::buffer(&notification_count, sizeof(notification_count)),
        [this](Error error, std::size_t) {
          if (error)
            return;
          if (stop_requested.load()) {
            stop();
            return;
          }
          if (wake_requested.exchange(false) && wakeup_handler)
            wakeup_handler();
          if (admission_overloaded && work->inflight.load() <= limits.inflight / 2) {
            admission_overloaded = false;
            emit(DiagnosticCode::ResourceRecovered, LogSeverity::Info);
          }
          if (stopping.load() && sessions.empty() && work->inflight.load() == 0)
            finish_stop();
          else
            listen_notification();
        });
  }
  void accept() {
    if (stopping.load())
      return;
    auto socket = std::make_shared<Local::socket>(io);
    acceptor.async_accept(*socket, [this, socket](Error error) {
      if (!error && !stopping.load()) {
        if (active.load() >= limits.connections) {
          ++rejected;
          emit(DiagnosticCode::ConnectionRejected, LogSeverity::Warn);
          Error ignored;
          socket->close(ignored);
        } else {
          const auto id = ++accepted;
          auto session =
              std::make_shared<Session>(*this, std::move(*socket), id);
          sessions.emplace(id, session);
          ++active;
          emit(DiagnosticCode::ConnectionAccepted, LogSeverity::Debug);
          session->read();
        }
      }
      if (!stopping.load() && error != net::error::operation_aborted)
        accept();
    });
  }
  void finish_stop() {
    if (work->inflight.load() != 0)
      return;
    Error ignored;
    notification.cancel(ignored);
    work->stop();
    if (!drain_reported) {
      drain_reported = true;
      emit(DiagnosticCode::DrainCompleted, LogSeverity::Info);
    }
  }
  void stop() {
    stopping.store(true);
    Error ignored;
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    while (!sessions.empty()) {
      const auto session = sessions.begin()->second;
      session->close();
    }
    notification.cancel(ignored);
    work->stop();
    if (work->inflight.load() == 0)
      finish_stop();
  }
  void begin_drain() {
    emit(DiagnosticCode::DrainStarted, LogSeverity::Info);
    stopping.store(true);
    Error ignored;
    acceptor.cancel(ignored);
    acceptor.close(ignored);
    for (auto i = sessions.begin(); i != sessions.end();) {
      const auto session = i++->second;
      if (!session->is_admitted())
        session->close();
    }
    if (sessions.empty())
      finish_stop();
  }
  HttpIdentity identity;
  HttpLimits limits;
  Handler handler;
  Diagnostics *diagnostics = nullptr;
  std::string diagnostic_service;
  bool admission_overloaded = false, drain_reported = false;
  std::shared_ptr<HttpWorkLifetime> work;
  net::io_context io;
  Local::acceptor acceptor;
  net::posix::stream_descriptor notification;
  std::uint64_t notification_count = 0;
  std::function<void()> wakeup_handler;
  std::shared_ptr<Dispatch> dispatcher;
  std::map<std::uint64_t, std::shared_ptr<Session>> sessions;
  std::atomic<bool> stopping{false}, stop_requested{false}, wake_requested{false};
  std::atomic<std::uint64_t> accepted{0}, rejected{0}, completed{0};
  std::atomic<std::uint64_t> admitted_calls{0}, rejected_calls{0}, expired{0},
      idle_expired{0}, cancelled_calls{0}, malformed{0}, peer_errors{0};
  std::atomic<std::size_t> active{0};
};
HttpServer::HttpServer(UnixOptions options, Handler handler, HttpLimits limits,
                       HttpIdentity identity)
    : impl_(new Impl(std::move(options), std::move(handler), limits,
                     std::move(identity))) {}
HttpServer::HttpServer(UnixOptions options, Handler handler, HttpLimits limits,
                       HttpIdentity identity, int retained_parent_fd) {
  if (retained_parent_fd < 0)
    throw std::invalid_argument("runtime directory descriptor is required");
  impl_.reset(new Impl(std::move(options), std::move(handler), limits,
                       std::move(identity), retained_parent_fd));
}
HttpServer::~HttpServer() = default;
void HttpServer::run() {
  impl_->io.restart();
  impl_->io.run();
}
void HttpServer::poll(std::chrono::milliseconds wait) {
  impl_->io.restart();
  if (wait.count() > 0)
    impl_->io.run_for(wait);
  else
    for (unsigned i = 0; i < 64 && impl_->io.poll_one(); ++i) {
    }
}
void HttpServer::set_wakeup_handler(std::function<void()> handler) {
  impl_->wakeup_handler = std::move(handler);
}
void HttpServer::set_diagnostics(Diagnostics *diagnostics, std::string service) {
  if (!service.empty() && !valid_id(service))
    throw std::invalid_argument("invalid diagnostic service identity");
  impl_->diagnostics = diagnostics;
  impl_->diagnostic_service = std::move(service);
}
void HttpServer::wake() noexcept {
  impl_->wake_requested.store(true);
  signal_fd(impl_->notification.native_handle());
}
void HttpServer::request_stop() noexcept {
  impl_->stop_requested.store(true);
  signal_fd(impl_->notification.native_handle());
}
bool HttpServer::drain_until(Clock::time_point deadline) {
  impl_->begin_drain();
  while ((impl_->active || impl_->work->inflight) && Clock::now() < deadline &&
         !impl_->stop_requested.load()) {
    impl_->io.restart();
    impl_->io.run_one_until(deadline);
  }
  const bool drained = impl_->active.load() == 0 &&
                       impl_->work->inflight.load() == 0;
  impl_->stop();
  impl_->io.restart();
  impl_->io.poll();
  return drained;
}
bool HttpServer::drain() {
  return drain_until(Clock::now() + impl_->limits.shutdown_timeout);
}
HttpStats HttpServer::stats() const noexcept {
  HttpStats s;
  s.accepted_connections = impl_->accepted;
  s.rejected_connections = impl_->rejected;
  s.completed_calls = impl_->completed;
  s.active_connections = impl_->active;
  s.inflight_calls = impl_->work->inflight;
  s.admitted_calls = impl_->admitted_calls;
  s.rejected_calls = impl_->rejected_calls;
  s.deadline_exceeded = impl_->expired;
  s.idle_timeouts = impl_->idle_expired;
  s.cancelled_calls = impl_->cancelled_calls;
  s.malformed_requests = impl_->malformed;
  s.peer_errors = impl_->peer_errors;
  s.source_time = Clock::now();
  s.stopping = impl_->stopping;
  return s;
}
const std::string &HttpServer::socket_path() const noexcept {
  return impl_->work->lease.path();
}

HttpCallError::HttpCallError(std::string value, Delivery phase,
                             const std::string &detail)
    : std::runtime_error(detail), code(std::move(value)), delivery(phase) {}
class HttpClient::Impl {
public:
  Impl(std::string p, HttpLimits l, std::string identity)
      : path(std::move(p)), instance_id(std::move(identity)), limits(l),
        socket(io), timer(io), cancellation_io(io),
        buffer(l.header_bytes + 8192) {
    validate(limits);
    if (path.empty() || path.front() != '/' ||
        path.size() >= sizeof(sockaddr_un::sun_path) ||
        path.find('\0') != std::string::npos)
      throw std::invalid_argument(
          "absolute Unix socket path exceeds platform limit");
    if (!instance_id.empty() && !valid_id(instance_id))
      throw std::invalid_argument("invalid service instance ID");
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0)
      throw std::system_error(errno, std::generic_category(),
                              "client cancellation eventfd");
    Error error;
    cancellation_io.assign(fd, error);
    if (error) {
      ::close(fd);
      throw boost::system::system_error(error);
    }
  }
  void close_socket() noexcept {
    Error ignored;
    socket.cancel(ignored);
    socket.close(ignored);
    buffer.consume(buffer.size());
  }
  void close() noexcept {
    {
      std::lock_guard<std::mutex> guard(admission);
      closed = true;
      if (busy)
        active_stop.request_stop();
      else
        close_socket();
    }
    available.notify_all();
  }
  HttpResponse call(HttpRequest request, Clock::time_point deadline,
                    std::stop_token cancellation) {
    std::stop_source stop;
    {
      std::unique_lock<std::mutex> guard(admission);
      if (deadline == Clock::time_point::max())
        throw HttpCallError("invalid_argument", Delivery::NotSent,
                            "finite deadline required");
      if (!available.wait_until(guard, cancellation, deadline,
                                [this] { return closed || !busy; }) ||
          Clock::now() >= deadline || cancellation.stop_requested())
        throw HttpCallError(cancellation.stop_requested() ? "cancelled"
                                                          : "deadline_exceeded",
                            Delivery::NotSent, "call ended during admission");
      if (closed)
        throw HttpCallError("unavailable", Delivery::NotSent,
                            "client owner has closed");
      active_stop = stop;
      busy = true;
    }
    struct Admission {
      Impl &owner;
      ~Admission() {
        {
          std::lock_guard<std::mutex> guard(owner.admission);
          owner.busy = false;
        }
        owner.available.notify_all();
      }
    } release{*this};
    // std::stop_callback deregistration joins an executing callback before the
    // next call may use this descriptor. No per-call polling or worker thread.
    std::stop_callback external_stop(cancellation,
                                     [stop]() mutable { stop.request_stop(); });
    std::stop_callback interrupted(stop.get_token(), [this] {
      signal_fd(cancellation_io.native_handle());
    });
    if (request.body.size() > limits.request_bytes ||
        !valid_target(request.target))
      throw HttpCallError("invalid_argument", Delivery::NotSent,
                          "invalid request target or body limit");
    const auto verb = http::string_to_verb(request.method);
    if (verb == http::verb::unknown || verb == http::verb::connect)
      throw HttpCallError("invalid_argument", Delivery::NotSent,
                          "unsupported method");
    if (request.request_id.empty())
      request.request_id = new_instance_id();
    if (!valid_id(request.request_id))
      throw HttpCallError("invalid_argument", Delivery::NotSent,
                          "invalid request ID");
    http::request<http::string_body> message(verb, request.target, 11);
    message.set(http::field::host, "localhost");
    std::size_t headers = 128 + request.target.size() +
                          request.request_id.size() + instance_id.size();
    for (const auto &h : request.headers) {
      headers += h.first.size() + h.second.size() + 4;
      if (headers > limits.header_bytes)
        throw HttpCallError("resource_exhausted", Delivery::NotSent,
                            "request headers exceed limit");
      try {
        validate_header(h.first, h.second);
        if (!reserved_header(h.first))
          message.set(h.first, h.second);
      } catch (const std::exception &) {
        throw HttpCallError("invalid_argument", Delivery::NotSent,
                            "invalid request header");
      }
    }
    if (headers > limits.header_bytes)
      throw HttpCallError("resource_exhausted", Delivery::NotSent,
                          "request headers exceed limit");
    if (!instance_id.empty())
      message.set("X-Xrpc-Instance-ID", instance_id);
    message.set("X-Request-ID", request.request_id);
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                              Clock::now());
    if (remaining.count() <= 0 || stop.stop_requested())
      throw HttpCallError(stop.stop_requested() ? "cancelled"
                                                : "deadline_exceeded",
                          Delivery::NotSent, "call ended before send");
    message.set("X-Xrpc-Timeout-Ms", std::to_string(std::min<std::int64_t>(
                                         86400000, remaining.count())));
    message.keep_alive(true);
    message.body() = std::move(request.body);
    message.prepare_payload();
    http::response_parser<http::string_body> parser;
    parser.header_limit(static_cast<std::uint32_t>(limits.header_bytes));
    parser.body_limit(limits.response_bytes);
    if (verb == http::verb::head)
      parser.skip(true);
    bool done = false, sent = false;
    std::string code, detail;
    auto finish = [&](const std::string &c, const std::string &d) {
      if (done)
        return;
      done = true;
      code = c;
      detail = d;
      Error ignored;
      timer.cancel(ignored);
      cancellation_io.cancel(ignored);
      if (!c.empty())
        close_socket();
    };
    auto write = [&] {
      if (done)
        return;
      if (stop.stop_requested()) {
        finish("cancelled", "call cancelled before send");
        return;
      }
      if (Clock::now() >= deadline) {
        finish("deadline_exceeded", "call deadline exceeded before send");
        return;
      }
      sent = true;
      http::async_write(socket, message, [&](Error e, std::size_t bytes) {
        if (done)
          return;
        if (e) {
          sent = bytes != 0;
          finish("unavailable", e.message());
          return;
        }
        http::async_read(socket, buffer, parser,
                         [&](Error read_error, std::size_t) {
                           if (done)
                             return;
                           if (read_error)
                             finish("unavailable", read_error.message());
                           else
                             finish({}, {});
                         });
      });
    };
    if (socket.is_open() && Clock::now() - last_used >= limits.idle_timeout)
      close_socket();
    std::uint64_t cancellation_count = 0;
    // Clear any cancelled previous call's count before registering this read.
    while (::read(cancellation_io.native_handle(), &cancellation_count,
                  sizeof(cancellation_count)) > 0) {
    }
    io.restart();
    try {
      timer.expires_at(deadline);
      timer.async_wait([&](Error error) {
        if (!error)
          finish("deadline_exceeded", "call deadline exceeded");
      });
      net::async_read(
          cancellation_io,
          net::buffer(&cancellation_count, sizeof(cancellation_count)),
          [&](Error error, std::size_t) {
            if (!error)
              finish("cancelled", "call cancelled");
          });
      if (stop.stop_requested())
        finish("cancelled", "call cancelled before send");
      else if (socket.is_open())
        write();
      else
        socket.async_connect(Local::endpoint(path), [&](Error error) {
          if (done)
            return;
          if (error)
            finish("unavailable", error.message());
          else
            write();
        });
      io.run();
    } catch (...) {
      // Even allocation failure must cancel/drain before captured stack
      // objects disappear. Do not allocate an error string on this path.
      done = true;
      Error ignored;
      timer.cancel(ignored);
      cancellation_io.cancel(ignored);
      close_socket();
      io.restart();
      io.run();
      throw;
    }
    if (!code.empty())
      throw HttpCallError(
          code, sent ? Delivery::OutcomeUnknown : Delivery::NotSent, detail);
    auto &received = parser.get();
    if (!instance_id.empty() &&
        (received.count("X-Xrpc-Instance-ID") != 1 ||
         received["X-Xrpc-Instance-ID"] != instance_id)) {
      close_socket();
      throw HttpCallError("conflict", Delivery::OutcomeUnknown,
                          "response instance identity mismatch");
    }
    if (received.count("X-Request-ID") != 1 ||
        received["X-Request-ID"] != request.request_id) {
      close_socket();
      throw HttpCallError("conflict", Delivery::OutcomeUnknown,
                          "response request identity mismatch");
    }
    HttpResponse result;
    result.status = received.result_int();
    result.body = std::move(received.body());
    result.keep_alive = received.keep_alive();
    for (const auto &h : received.base())
      result.headers.emplace_back(std::string(h.name_string()),
                                  std::string(h.value()));
    if (!received.keep_alive())
      close_socket();
    last_used = Clock::now();
    return result;
  }
  std::string path, instance_id;
  HttpLimits limits;
  net::io_context io;
  Local::socket socket;
  net::steady_timer timer;
  net::posix::stream_descriptor cancellation_io;
  beast::flat_buffer buffer;
  std::mutex admission;
  std::condition_variable_any available;
  bool busy = false, closed = false;
  std::stop_source active_stop;
  Clock::time_point last_used{};
};
HttpClient::HttpClient(std::string path, HttpLimits limits,
                       std::string instance_id)
    : impl_(new Impl(std::move(path), limits, std::move(instance_id))) {}
HttpClient::~HttpClient() = default;
HttpResponse HttpClient::call(HttpRequest request, Clock::time_point deadline,
                              std::stop_token cancellation) {
  return impl_->call(std::move(request), deadline, cancellation);
}
void HttpClient::close() noexcept { impl_->close(); }
} // namespace xrpc
} // namespace xgc2
