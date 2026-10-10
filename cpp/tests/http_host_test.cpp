#include "xgc2/xrpc/http.hpp"
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <dirent.h>
#include <iostream>
#include <mutex>
#include <new>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono;
namespace {
std::atomic<bool> reject_allocations{false};
struct Directory {
  std::string path;
  Directory() {
    char pattern[] = "/tmp/xrpc-http-host-XXXXXX";
    const auto value = ::mkdtemp(pattern);
    assert(value);
    path = value;
  }
  ~Directory() {
    auto *dir = ::opendir(path.c_str());
    if (dir) {
      while (auto *entry = ::readdir(dir)) {
        const std::string name = entry->d_name;
        if (name != "." && name != "..")
          ::unlink((path + "/" + name).c_str());
      }
      ::closedir(dir);
    }
    ::rmdir(path.c_str());
  }
};
void await(const std::function<bool()> &condition) {
  const auto deadline = Clock::now() + seconds(2);
  while (!condition() && Clock::now() < deadline)
    std::this_thread::sleep_for(milliseconds(1));
  assert(condition());
}
HttpRequest request(const std::string &path) {
  HttpRequest r;
  r.method = "GET";
  r.target = path;
  return r;
}
void request_validation_and_deadlines() {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/http.sock";
  HttpLimits limits;
  limits.request_timeout = seconds(2);
  limits.idle_timeout = seconds(2);
  std::atomic<int> held{0}, calls{0};
  HttpServer server(
      options,
      [&](HttpRequest req, HttpReply reply) {
        ++calls;
        if (req.target == "/hold") {
          ++held;
          return;
        }
        HttpResponse response;
        response.body = req.target == "/id" ? req.request_id : "ok";
        if (req.target == "/bad-header")
          response.headers.emplace_back("X-Test", "safe\r\nInjected: yes");
        reply.complete(std::move(response));
      },
      limits);
  std::thread owner([&] { server.run(); });
  HttpClient client(options.path, limits);
  std::thread first([&] {
    try {
      client.call(request("/hold"), Clock::now() + milliseconds(300));
      assert(false);
    } catch (const HttpCallError &) {
    }
  });
  await([&] { return held.load() == 1; });
  const auto began = Clock::now();
  try {
    client.call(request("/hold"), began + milliseconds(20));
    assert(false);
  } catch (const HttpCallError &error) {
    assert(error.code == "deadline_exceeded" &&
           error.delivery == Delivery::NotSent);
  }
  assert(Clock::now() - began < milliseconds(150));
  first.join();
  assert(client.call(request("/ok"), Clock::now() + seconds(1)).status == 200);
  std::this_thread::sleep_for(milliseconds(150));
  assert(client.call(request("/ok"), Clock::now() + milliseconds(100)).status ==
         200);
  const auto response =
      client.call(request("/bad-header"), Clock::now() + seconds(1));
  assert(response.status == 500);
  for (const auto &field : response.headers)
    assert(field.first != "Injected");
  const auto before = calls.load();
  for (const auto &field : Headers{{"X-Test", "safe\r\nInjected: yes"},
                                   {"Bad:Name", "value"},
                                   {std::string("X\0Y", 3), "value"}}) {
    auto req = request("/ok");
    req.headers.push_back(field);
    try {
      client.call(req, Clock::now() + seconds(1));
      assert(false);
    } catch (const HttpCallError &error) {
      assert(error.code == "invalid_argument" &&
             error.delivery == Delivery::NotSent);
    }
  }
  try {
    client.call(request("/bad\r\nInjected: yes"), Clock::now() + seconds(1));
    assert(false);
  } catch (const HttpCallError &error) {
    assert(error.code == "invalid_argument" &&
           error.delivery == Delivery::NotSent);
  }
  assert(calls.load() == before);
  auto head = request("/ok");
  head.method = "HEAD";
  const auto head_result = client.call(head, Clock::now() + milliseconds(100));
  assert(head_result.status == 200 && head_result.body.empty());
  assert(client.call(request("/ok"), Clock::now() + seconds(1)).body == "ok");
  HttpClient second(options.path, limits);
  assert(client.call(request("/id"), Clock::now() + seconds(1)).body !=
         second.call(request("/id"), Clock::now() + seconds(1)).body);
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  assert(fd >= 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::strcpy(address.sun_path, options.path.c_str());
  assert(::connect(fd, reinterpret_cast<sockaddr *>(&address),
                   sizeof(address)) == 0);
  const char raw[] =
      "GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
  assert(::send(fd, raw, sizeof(raw) - 1, MSG_NOSIGNAL) == sizeof(raw) - 1);
  char result[1024];
  const auto size = ::recv(fd, result, sizeof(result), 0);
  assert(size > 0 &&
         std::string(result, size).find("400") != std::string::npos);
  ::close(fd);
  std::atomic<bool> cancelled{false};
  std::thread pending([&] {
    try {
      client.call(request("/hold"), Clock::now() + seconds(2));
      assert(false);
    } catch (const HttpCallError &error) {
      cancelled = error.code == "cancelled";
    }
  });
  await([&] { return held.load() == 2; });
  std::atomic<bool> queued_started{false}, queued_rejected{false};
  std::thread queued([&] {
    queued_started = true;
    try {
      client.call(request("/ok"), Clock::now() + seconds(2));
      assert(false);
    } catch (const HttpCallError &error) {
      queued_rejected =
          error.code == "unavailable" && error.delivery == Delivery::NotSent;
    }
  });
  await([&] { return queued_started.load(); });
  const auto calls_before_close = calls.load();
  const auto close_start = Clock::now();
  client.close();
  assert(Clock::now() - close_start < milliseconds(100));
  pending.join();
  queued.join();
  assert(cancelled && queued_rejected);
  try {
    client.call(request("/ok"), Clock::now() + seconds(1));
    assert(false);
  } catch (const HttpCallError &error) {
    assert(error.code == "unavailable" && error.delivery == Delivery::NotSent);
  }
  assert(calls.load() == calls_before_close);
  server.request_stop();
  owner.join();
}
void graceful_shutdown(bool finish) {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/http.sock";
  std::mutex mutex;
  HttpReply held;
  std::atomic<bool> draining{false}, drained{false}, caller_succeeded{false};
  HttpServer server(options, [&](HttpRequest, HttpReply reply) {
    std::lock_guard<std::mutex> guard(mutex);
    held = reply;
  });
  std::thread owner([&] {
    while (!draining.load())
      server.poll(milliseconds(2));
    drained = server.drain_until(
        Clock::now() + (finish ? milliseconds(300) : milliseconds(25)));
  });
  std::thread caller([&] {
    HttpClient client(options.path);
    try {
      caller_succeeded =
          client.call(request("/hold"), Clock::now() + seconds(1)).body ==
          "complete";
    } catch (const HttpCallError &) {
    }
  });
  await([&] { return server.stats().inflight_calls == 1; });
  draining = true;
  if (finish) {
    std::this_thread::sleep_for(milliseconds(50));
    std::lock_guard<std::mutex> guard(mutex);
    HttpResponse response;
    response.body = "complete";
    assert(held.complete(std::move(response)));
    held = {};
  }
  owner.join();
  caller.join();
  assert(drained.load() == finish && caller_succeeded.load() == finish);
  assert(server.stats().active_connections == 0);
  if (!finish) {
    assert(held.cancelled() && server.stats().inflight_calls == 1);
    assert(::access(options.path.c_str(), F_OK) == 0);
    bool refused = false;
    try {
      UnixPathLease competing(options);
    } catch (const std::exception &) {
      refused = true;
    }
    assert(refused);
    held = {};
  }
  assert(server.stats().inflight_calls == 0 &&
         ::access(options.path.c_str(), F_OK) != 0);
}
void allocation_free_stop() {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/http.sock";
  HttpServer server(options, [](HttpRequest, HttpReply reply) {
    reply.complete(HttpResponse{});
  });
  reject_allocations = true;
  server.request_stop();
  reject_allocations = false;
  server.run();
  assert(::access(options.path.c_str(), F_OK) != 0);
}
void response_correlation() {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/wrong-reply.sock";
  UnixPathLease lease(options);
  const int listener = lease.bind_stream();
  std::thread peer([&] {
    pollfd ready{listener, POLLIN, 0};
    assert(::poll(&ready, 1, 2000) == 1);
    const int fd = ::accept(listener, nullptr, nullptr);
    assert(fd >= 0);
    timeval timeout{2, 0};
    assert(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    std::string request_bytes;
    while (request_bytes.find("\r\n\r\n") == std::string::npos) {
      char incoming[2048];
      const auto count = ::recv(fd, incoming, sizeof(incoming), 0);
      assert(count > 0);
      request_bytes.append(incoming, count);
      assert(request_bytes.size() <= 4096);
    }
    assert(request_bytes.find("caller-stable-id") != std::string::npos);
    const std::string response =
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n"
        "X-Xrpc-Instance-ID: instance-one\r\nX-Request-ID: foreign-id\r\n\r\nok";
    assert(::send(fd, response.data(), response.size(), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(response.size()));
    ::close(fd);
  });
  HttpClient client(options.path, {}, "instance-one");
  auto req = request("/read");
  req.request_id = "caller-stable-id";
  try {
    client.call(req, Clock::now() + seconds(1));
    assert(false);
  } catch (const HttpCallError &error) {
    assert(error.code == "conflict" && error.delivery == Delivery::OutcomeUnknown);
  }
  peer.join();
  ::close(listener);
}
void reply_outlives_host() {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/retained.sock";
  HttpReply held;
  std::mutex mutex;
  auto server = std::make_unique<HttpServer>(options, [&](HttpRequest, HttpReply reply) {
    std::lock_guard<std::mutex> guard(mutex);
    held = reply;
  });
  std::thread owner([&] { server->run(); });
  std::thread caller([&] {
    HttpClient client(options.path);
    try {
      client.call(request("/wait"), Clock::now() + seconds(1));
      assert(false);
    } catch (const HttpCallError &) {
    }
  });
  await([&] { return server->stats().inflight_calls == 1; });
  server->request_stop();
  owner.join();
  caller.join();
  server.reset();
  assert(::access(options.path.c_str(), F_OK) == 0);
  bool refused = false;
  try { UnixPathLease competing(options); } catch (const std::exception &) { refused = true; }
  assert(refused);
  assert(held.cancelled() && !held.complete(HttpResponse{}));
  held = {}; // final business handle releases the surviving endpoint lease
  assert(::access(options.path.c_str(), F_OK) != 0);
  UnixPathLease successor(options);
  const auto fd = successor.bind_stream();
  ::close(fd);
}
void noncooperative_worker_outlives_host(bool expired) {
  Directory dir;
  UnixOptions options;
  options.path = dir.path + "/worker.sock";
  HttpLimits limits;
  limits.request_timeout = seconds(2);
  std::mutex mutex;
  std::condition_variable gate;
  bool worker_started = false, release_worker = false;
  std::atomic<int> effects{0};
  std::atomic<bool> reply_accepted{true}, caller_failed{false};
  HttpReply retained;
  std::thread worker;
  auto server = std::make_unique<HttpServer>(
      options,
      [&](HttpRequest, HttpReply reply) {
        retained = reply;
        worker = std::thread([&, reply] {
          std::unique_lock<std::mutex> lock(mutex);
          worker_started = true;
          gate.notify_all();
          // Deliberately ignore cancellation until the owner releases this
          // gate: a cancelled socket does not prove business work has ended.
          gate.wait(lock, [&] { return release_worker; });
          lock.unlock();
          ++effects;
          HttpResponse response;
          response.body = "late";
          reply_accepted = reply.complete(std::move(response));
        });
      },
      limits);
  std::thread caller([&] {
    HttpClient client(options.path, limits);
    try {
      client.call(request("/effect"),
                  Clock::now() + (expired ? milliseconds(25) : seconds(2)));
      assert(false);
    } catch (const HttpCallError &error) {
      // The host's deadline close can reach the caller before its local timer.
      // Either transport result leaves the admitted effect outcome unknown.
      caller_failed = error.delivery == Delivery::OutcomeUnknown &&
          (error.code == "deadline_exceeded" || error.code == "unavailable");
    }
  });
  const auto poll_until = [&](const auto &condition) {
    const auto deadline = Clock::now() + seconds(2);
    while (!condition() && Clock::now() < deadline)
      server->poll(milliseconds(2));
    assert(condition());
  };
  poll_until([&] { return worker.joinable(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    assert(gate.wait_for(lock, seconds(2), [&] { return worker_started; }));
  }
  assert(server->stats().inflight_calls == 1 && effects.load() == 0);
  if (expired) {
    poll_until([&] { return server->stats().active_connections == 0; });
    assert(retained.cancelled());
  } else {
    assert(server->stats().active_connections == 1 && !retained.cancelled());
  }
  assert(!server->drain_until(Clock::now() + milliseconds(25)));
  assert(server->stats().active_connections == 0 &&
         server->stats().inflight_calls == 1 && retained.cancelled());
  assert(::access(options.path.c_str(), F_OK) == 0 && effects.load() == 0);
  server.reset();
  bool refused = false;
  try {
    UnixPathLease competing(options);
  } catch (const std::exception &) {
    refused = true;
  }
  assert(refused && effects.load() == 0 &&
         ::access(options.path.c_str(), F_OK) == 0);
  caller.join();
  assert(caller_failed.load());
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_worker = true;
  }
  gate.notify_all();
  worker.join();
  assert(effects.load() == 1 && !reply_accepted.load());
  assert(::access(options.path.c_str(), F_OK) == 0);
  retained = {}; // the final reply, rather than socket closure, releases work
  assert(::access(options.path.c_str(), F_OK) != 0);
  UnixPathLease successor(options);
  const auto fd = successor.bind_stream();
  assert(fd >= 0);
  ::close(fd);
}
} // namespace
void *operator new(std::size_t size) {
  if (reject_allocations.load())
    throw std::bad_alloc();
  if (auto *pointer = std::malloc(size))
    return pointer;
  throw std::bad_alloc();
}
void operator delete(void *pointer) noexcept { std::free(pointer); }
void operator delete(void *pointer, std::size_t) noexcept {
  std::free(pointer);
}
int main() {
  request_validation_and_deadlines();
  graceful_shutdown(true);
  graceful_shutdown(false);
  allocation_free_stop();
  response_correlation();
  reply_outlives_host();
  noncooperative_worker_outlives_host(false);
  noncooperative_worker_outlives_host(true);
  std::cout
      << "HTTP host regressions: admission/idle budgets, framing input, HEAD, "
         "metadata, IDs, cancellation and graceful shutdown passed\n";
}
