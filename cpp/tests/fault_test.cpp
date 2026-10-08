#include "xgc2/xrpc/http.hpp"
#include <cassert>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono;

int main() {
  char pattern[] = "/tmp/xrpc-fault-XXXXXX";
  const auto value = ::mkdtemp(pattern);
  assert(value);
  const std::string dir = value;
  UnixOptions options;
  options.path = dir + "/fault.sock";
  int signals[2];
  assert(::pipe(signals) == 0);
  const auto child = ::fork();
  assert(child >= 0);
  if (child == 0) {
    ::close(signals[0]);
    HttpReply held;
    HttpServer server(options, [&](HttpRequest, HttpReply reply) {
      held = reply;
      const char accepted = 'A';
      assert(::write(signals[1], &accepted, 1) == 1);
    }, {}, {"first-boot", {}});
    const char ready = 'R';
    assert(::write(signals[1], &ready, 1) == 1);
    server.run();
    ::_exit(0);
  }
  ::close(signals[1]);
  char signal = 0;
  assert(::read(signals[0], &signal, 1) == 1 && signal == 'R');
  HttpClient stale(options.path, {}, "first-boot");
  std::atomic<bool> unknown{false};
  const auto started = Clock::now();
  std::thread caller([&] {
    HttpRequest request;
    request.method = "POST";
    request.target = "/mutate";
    request.request_id = "caller-stable-operation";
    try {
      stale.call(request, started + milliseconds(250));
      assert(false);
    } catch (const HttpCallError &error) {
      unknown = error.delivery == Delivery::OutcomeUnknown &&
                error.code == "deadline_exceeded";
    }
  });
  // The child explicitly signals domain admission; pause it after the write
  // effect and before its response, rather than guessing with a sleep.
  assert(::read(signals[0], &signal, 1) == 1 && signal == 'A');
  assert(::kill(child, SIGSTOP) == 0);
  caller.join();
  assert(unknown && Clock::now() - started < seconds(1));
  assert(::kill(child, SIGKILL) == 0);
  int status = 0;
  assert(::waitpid(child, &status, 0) == child && WIFSIGNALED(status));
  ::close(signals[0]);

  bool refused = false;
  try {
    UnixPathLease leftover(options);
  } catch (const std::exception &) {
    refused = true;
  }
  assert(refused); // stale endpoints require an explicit reclaim decision
  options.existing = ExistingPath::ReclaimUnreachable;
  std::atomic<unsigned> mutations{0};
  HttpServer replacement(options, [&](HttpRequest request, HttpReply reply) {
    if (request.method == "POST")
      ++mutations;
    HttpResponse response;
    response.body = "new-instance";
    reply.complete(std::move(response));
  }, {}, {"second-boot", {}});
  std::thread owner([&] { replacement.run(); });
  HttpRequest request;
  request.method = "GET";
  request.target = "/read";
  try {
    stale.call(request, Clock::now() + seconds(1));
    assert(false);
  } catch (const HttpCallError &error) {
    assert(error.code == "conflict");
  }
  assert(mutations == 0 && replacement.stats().admitted_calls == 0);
  HttpClient current(options.path, {}, "second-boot");
  assert(current.call(request, Clock::now() + seconds(1)).body == "new-instance");
  assert(mutations == 0); // ambiguous mutation was never automatically replayed
  replacement.request_stop();
  owner.join();
  assert(replacement.stats().stopping && replacement.stats().inflight_calls == 0);
  UnixPathLease next_owner(options); // quiescent stop also releases flock
  const int fd = next_owner.bind_stream();
  ::close(fd);
  next_owner.cleanup();
  std::filesystem::remove_all(dir);
  std::cout << "paused provider deadline, crash/reclaim, incarnation fence and no replay passed\n";
}
