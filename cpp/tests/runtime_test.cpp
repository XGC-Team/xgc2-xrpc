#include "xgc2/xrpc/bounded_output.hpp"
#include "xgc2/xrpc/http.hpp"
#include <cassert>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <iterator>
#include <mutex>
#include <set>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono;
struct Directory {
  std::string path;
  Directory() {
    char value[] = "/tmp/xrpc-cpp-XXXXXX";
    const auto p = ::mkdtemp(value);
    assert(p);
    path = p;
  }
  ~Directory() {
    DIR *dir = ::opendir(path.c_str());
    if (dir) {
      while (auto item = ::readdir(dir)) {
        const std::string n = item->d_name;
        if (n != "." && n != "..")
          ::unlink((path + "/" + n).c_str());
      }
      ::closedir(dir);
    }
    ::rmdir(path.c_str());
  }
  std::string socket() const { return path + "/service.sock"; }
};
template <class F> void throws(F f) {
  bool caught = false;
  try {
    f();
  } catch (const std::exception &) {
    caught = true;
  }
  assert(caught);
}
int raw_connect(const std::string &path) {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  assert(fd >= 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::strcpy(address.sun_path, path.c_str());
  assert(::connect(fd, reinterpret_cast<sockaddr *>(&address),
                   sizeof(address)) == 0);
  timeval timeout{2, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  return fd;
}
struct stat node_info(const std::string &path) {
  struct stat info {};
  assert(::lstat(path.c_str(), &info) == 0);
  return info;
}
void assert_same_node(const std::string &path, const struct stat &before) {
  const auto after = node_info(path);
  assert(after.st_dev == before.st_dev && after.st_ino == before.st_ino);
  assert(after.st_mode == before.st_mode && after.st_uid == before.st_uid);
}
std::string file_contents(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  assert(input.good());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
std::set<std::string> lease_directory_contents(const Directory &directory) {
  std::set<std::string> names;
  for (const auto &entry : std::filesystem::directory_iterator(directory.path)) {
    const auto name = entry.path().filename().string();
    // Ownership locks intentionally survive every lease lifetime.
    if (name != "service.sock.xrpc.lock") names.insert(name);
  }
  return names;
}
void lease_bind_foreign_nodes() {
  {
    Directory directory;
    const auto path = directory.socket();
    const std::string contents = "foreign regular file must survive";
    struct stat before {};
    {
      UnixPathLease lease(UnixOptions{path});
      // The same uid creates the public node after reservation, before bind.
      std::ofstream output(path); output << contents; output.close();
      assert(::chmod(path.c_str(), 0640) == 0);
      before = node_info(path);
      assert(S_ISREG(before.st_mode) && before.st_uid == ::geteuid());
      throws([&] { (void)lease.bind_stream(); });
      assert_same_node(path, before);
      assert(file_contents(path) == contents);
      assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
      lease.cleanup();
      assert_same_node(path, before);
      assert(file_contents(path) == contents);
      assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
    }
    assert_same_node(path, before);
    assert(file_contents(path) == contents);
    assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
  }
  {
    Directory directory;
    const auto path = directory.socket();
    const auto target = directory.path + "/foreign-target";
    const std::string contents = "symlink target must survive unchanged";
    struct stat before {}, target_before {};
    {
      UnixPathLease lease(UnixOptions{path});
      std::ofstream output(target); output << contents; output.close();
      assert(::chmod(target.c_str(), 0640) == 0);
      assert(::symlink("foreign-target", path.c_str()) == 0);
      before = node_info(path); target_before = node_info(target);
      assert(S_ISLNK(before.st_mode) && before.st_uid == ::geteuid());
      const std::set<std::string> expected{"service.sock", "foreign-target"};
      throws([&] { (void)lease.bind_stream(); });
      assert_same_node(path, before); assert_same_node(target, target_before);
      assert(std::filesystem::read_symlink(path) == "foreign-target");
      assert(file_contents(target) == contents);
      assert(lease_directory_contents(directory) == expected);
      lease.cleanup();
      assert_same_node(path, before); assert_same_node(target, target_before);
      assert(std::filesystem::read_symlink(path) == "foreign-target");
      assert(file_contents(target) == contents);
      assert(lease_directory_contents(directory) == expected);
    }
    assert_same_node(path, before); assert_same_node(target, target_before);
    assert(std::filesystem::read_symlink(path) == "foreign-target");
    assert(file_contents(target) == contents);
    assert((lease_directory_contents(directory) == std::set<std::string>{"service.sock", "foreign-target"}));
  }
  {
    Directory directory;
    const auto path = directory.socket();
    const int listener = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    assert(listener >= 0);
    struct stat before {};
    {
      UnixPathLease lease(UnixOptions{path});
      sockaddr_un address{}; address.sun_family = AF_UNIX;
      std::strcpy(address.sun_path, path.c_str());
      assert(::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0);
      assert(::chmod(path.c_str(), 0640) == 0);
      assert(::listen(listener, 4) == 0);
      before = node_info(path);
      assert(S_ISSOCK(before.st_mode) && before.st_uid == ::geteuid());
      throws([&] { (void)lease.bind_stream(); });
      assert_same_node(path, before);
      assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
      lease.cleanup();
      assert_same_node(path, before);
      assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
    }
    assert_same_node(path, before);
    assert(lease_directory_contents(directory) == std::set<std::string>{"service.sock"});
    const int client = raw_connect(path);
    const int accepted = ::accept(listener, nullptr, nullptr);
    assert(accepted >= 0);
    const std::string contents = "foreign socket remains live";
    assert(::send(accepted, contents.data(), contents.size(), MSG_NOSIGNAL) ==
           static_cast<ssize_t>(contents.size()));
    char received[64]{};
    const auto count = ::recv(client, received, contents.size(), MSG_WAITALL);
    assert(count == static_cast<ssize_t>(contents.size()) && std::string(received, count) == contents);
    assert_same_node(path, before);
    ::close(accepted); ::close(client); ::close(listener);
  }
}
void lease_tests() {
  Directory d;
  UnixOptions o;
  o.path = d.socket();
  {
    UnixPathLease owner(o);
    int fd = owner.bind_stream();
    struct stat info {};
    assert(!::lstat(o.path.c_str(), &info));
    assert((info.st_mode & 0777) == 0600);
    const int client = raw_connect(o.path);
    const int accepted = ::accept(fd, nullptr, nullptr);
    assert(accepted >= 0);
    ::close(accepted); ::close(client);
    throws([&] { UnixPathLease second(o); });
    ::unlink(o.path.c_str());
    std::ofstream replacement(o.path);
    replacement << "foreign";
    replacement.close();
    owner.cleanup();
    assert(!::lstat(o.path.c_str(), &info) && S_ISREG(info.st_mode));
    ::close(fd);
  }
  throws([&] { UnixPathLease second(o); });
  ::unlink(o.path.c_str());
  assert(!::symlink("missing", o.path.c_str()));
  throws([&] { UnixPathLease second(o); });
  ::unlink(o.path.c_str());
  int stale = ::socket(AF_UNIX, SOCK_STREAM, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::strcpy(address.sun_path, o.path.c_str());
  assert(
      !::bind(stale, reinterpret_cast<sockaddr *>(&address), sizeof(address)));
  ::close(stale);
  throws([&] { UnixPathLease second(o); });
  o.existing = ExistingPath::ReclaimUnreachable;
  {
    UnixPathLease owner(o);
    const int fd = owner.bind_stream();
    ::close(fd);
  }
  assert(::access(o.path.c_str(), F_OK) != 0);
  UnixOptions bad;
  bad.path = "/tmp/xrpc-invalid-private-parent.sock";
  throws([&] { UnixPathLease owner(bad); });
  BoundedOutput out(4);
  out.stream() << "12345";
  assert(!out.good() && out.value() == "1234");
  // A framework binds through the retained parent, even if the public parent
  // has been replaced after reservation. Cleanup must not touch its contents.
  const auto moved = d.path + "-moved";
  {
    Directory different;
    const int wrong_parent = ::open(different.path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    assert(wrong_parent >= 0);
    throws([&] { UnixPathLease mismatched(o, wrong_parent); });
    ::close(wrong_parent);
  }
  const int retained_parent = ::open(d.path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  assert(retained_parent >= 0);
  {
    UnixPathLease anchored(o, retained_parent);
    // The lease owns its duplicate; the grant owner's FD may end here.
    ::close(retained_parent);
    const auto external = anchored.external_bind_path();
    assert(::rename(d.path.c_str(), moved.c_str()) == 0);
    assert(::mkdir(d.path.c_str(), 0700) == 0);
    std::ofstream foreign(o.path);
    foreign << "replacement directory";
    foreign.close();
    const int listener = anchored.bind_stream();
    assert(::access((moved + "/service.sock").c_str(), F_OK) == 0);
    assert(::access(external.c_str(), F_OK) == 0);
    const int client = raw_connect(moved + "/service.sock");
    const int accepted = ::accept(listener, nullptr, nullptr);
    assert(accepted >= 0);
    ::close(accepted); ::close(client);
    anchored.cleanup();
    assert(::access((moved + "/service.sock").c_str(), F_OK) != 0);
    assert(::access(o.path.c_str(), F_OK) == 0);
    throws([&] { anchored.bind_stream(); });
    throws([&] { anchored.external_bind_path(); });
    ::close(listener);
  }
  std::filesystem::remove_all(moved);
}
void http_tests() {
  Directory d;
  UnixOptions o;
  o.path = d.socket();
  HttpLimits limits;
  limits.request_timeout = milliseconds(500);
  limits.header_timeout = milliseconds(500);
  limits.idle_timeout = milliseconds(500);
  limits.request_bytes = 64;
  limits.connections = 4;
  std::mutex mutex;
  HttpReply held;
  std::atomic<int> invocations{0};
  HttpServer server(
      o,
      [&](HttpRequest request, HttpReply reply) {
        ++invocations;
        if (request.target == "/wait") {
          std::lock_guard<std::mutex> lock(mutex);
          held = reply;
          return;
        }
        HttpResponse response;
        response.body = request.body.empty() ? request.target : request.body;
        assert(reply.complete(std::move(response)));
        assert(!reply.complete(HttpResponse{}));
      },
      limits);
  std::thread io([&] { server.run(); });
  HttpClient client(o.path, limits);
  HttpRequest request;
  request.method = "POST";
  request.target = "/echo";
  request.body = "hello";
  for (int i = 0; i < 3; ++i) {
    auto result = client.call(request, Clock::now() + seconds(1));
    assert(result.body == "hello" && result.status == 200);
  }
  assert(server.stats().accepted_connections == 1); // real keep-alive reuse
  request.target = "/wait";
  request.body.clear();
  std::thread completion([&] {
    for (;;) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (!held.cancelled()) {
          HttpResponse r;
          r.body = "async";
          assert(held.complete(std::move(r)));
          return;
        }
      }
      std::this_thread::sleep_for(milliseconds(1));
    }
  });
  assert(client.call(request, Clock::now() + seconds(1)).body == "async");
  completion.join();
  try {
    client.call(request, Clock::now() + milliseconds(35));
    assert(false);
  } catch (const HttpCallError &e) {
    assert(e.delivery == Delivery::OutcomeUnknown);
    assert(e.code == "deadline_exceeded" || e.code == "unavailable");
  }
  std::this_thread::sleep_for(milliseconds(15));
  {
    std::lock_guard<std::mutex> lock(mutex);
    assert(held.cancelled());
    assert(!held.complete(HttpResponse{}));
    held = {}; // business work has quiesced after observing cancellation
  }
  // Oversized body is rejected before a business handler sees it.
  const int before = invocations.load();
  int raw = raw_connect(o.path);
  const std::string oversized =
      "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65\r\n\r\n" +
      std::string(65, 'a');
  assert(::send(raw, oversized.data(), oversized.size(), MSG_NOSIGNAL) > 0);
  char response[2048]{};
  const auto n = ::recv(raw, response, sizeof(response), 0);
  assert(n > 0 && std::string(response, n).find("413") != std::string::npos);
  ::close(raw);
  assert(invocations.load() == before);
  // Trickle input cannot keep resetting the absolute request deadline.
  raw = raw_connect(o.path);
  const std::string partial = "GET / HTTP/1.1\r\nHost: localhost\r\nX:";
  assert(::send(raw, partial.data(), partial.size(), MSG_NOSIGNAL) > 0);
  for (int i = 0; i < 6; ++i) {
    std::this_thread::sleep_for(milliseconds(100));
    ::send(raw, "a", 1, MSG_NOSIGNAL);
  }
  assert(::recv(raw, response, sizeof(response), 0) <= 0);
  ::close(raw);
  // Stop closes a partial request promptly; no worker waits indefinitely.
  raw = raw_connect(o.path);
  ::send(raw, "GET ", 4, MSG_NOSIGNAL);
  server.request_stop();
  io.join();
  assert(server.stats().active_connections == 0);
  ::close(raw);
  assert(::access(o.path.c_str(), F_OK) != 0);
  std::stop_source cancellation;
  cancellation.request_stop();
  try {
    client.call(request, Clock::now() + seconds(1), cancellation.get_token());
    assert(false);
  } catch (const HttpCallError &e) {
    assert(e.code == "cancelled" && e.delivery == Delivery::NotSent);
  }
}

void admission_and_identity_tests() {
  Directory d;
  UnixOptions o;
  o.path = d.socket();
  HttpLimits limits;
  limits.connections = 2;
  limits.inflight = 1;
  std::atomic<int> handled{0}, notifications{0};
  HttpReply held;
  std::mutex mutex;
  HttpIdentity identity;
  identity.instance_id = "world-unique";
  identity.discovery_targets = {"/describe"};
  HttpServer server(
      o,
      [&](HttpRequest request, HttpReply reply) {
        ++handled;
        if (request.target == "/wait") {
          std::lock_guard<std::mutex> lock(mutex);
          held = reply;
        } else {
          HttpResponse r;
          r.body = "ok";
          reply.complete(std::move(r));
        }
      },
      limits, identity);
  server.set_wakeup_handler([&] { ++notifications; });
  std::thread io([&] { server.run(); });
  HttpRequest request;
  request.method = "GET";
  request.target = "/describe";
  {
    HttpClient discovery(o.path, limits);
    assert(discovery.call(request, Clock::now() + seconds(1)).status == 200);
  }
  {
    HttpClient missing(o.path, limits);
    request.target = "/protected";
    assert(missing.call(request, Clock::now() + seconds(1)).status == 409);
  }
  assert(handled == 1);
  HttpClient first(o.path, limits, identity.instance_id),
      second(o.path, limits, identity.instance_id);
  std::stop_source cancellation;
  std::atomic<bool> cancelled{false};
  request.target = "/wait";
  std::thread caller([&] {
    try {
      first.call(request, Clock::now() + seconds(3), cancellation.get_token());
      assert(false);
    } catch (const HttpCallError &e) {
      assert(e.code == "cancelled" && e.delivery == Delivery::OutcomeUnknown);
      cancelled = true;
    }
  });
  for (int n = 0; n < 1000 && server.stats().inflight_calls != 1; ++n)
    std::this_thread::sleep_for(milliseconds(1));
  assert(server.stats().inflight_calls == 1);
  request.target = "/protected";
  assert(second.call(request, Clock::now() + seconds(1)).status == 503);
  const int raw = raw_connect(o.path);
  for (int n = 0; n < 1000 && !server.stats().rejected_connections; ++n)
    std::this_thread::sleep_for(milliseconds(1));
  assert(server.stats().active_connections <= 2 &&
         server.stats().rejected_connections >= 1);
  ::close(raw);
  cancellation.request_stop();
  caller.join();
  assert(cancelled);
  for (int n = 0; n < 1000; ++n) {
    std::lock_guard<std::mutex> lock(mutex);
    if (held.cancelled())
      break;
    std::this_thread::sleep_for(milliseconds(1));
  }
  assert(server.stats().inflight_calls == 1);
  assert(second.call(request, Clock::now() + seconds(1)).status == 503);
  {
    std::lock_guard<std::mutex> lock(mutex);
    assert(held.cancelled() && !held.complete(HttpResponse{}));
    held = {}; // only now has the caller's owned business work ended
  }
  for (int n = 0; n < 1000 && server.stats().inflight_calls; ++n)
    std::this_thread::sleep_for(milliseconds(1));
  assert(server.stats().inflight_calls == 0);
  std::thread realtime_producer([&] {
    for (int n = 0; n < 100000; ++n)
      server.wake();
  });
  realtime_producer.join();
  for (int n = 0; n < 1000 && !notifications; ++n)
    std::this_thread::sleep_for(milliseconds(1));
  assert(notifications > 0 && notifications <= 100000);
  // Reusing connection/session and bounded reply states under repeated calls.
  for (int n = 0; n < 1000; ++n)
    assert(second.call(request, Clock::now() + seconds(1)).body == "ok");
  assert(server.stats().active_connections <= 2);
  server.request_stop();
  io.join();
  {
    std::lock_guard<std::mutex> lock(mutex);
    assert(held.cancelled());
    assert(!held.complete(HttpResponse{}));
  }
  throws([&] { HttpClient invalid(std::string(300, 'x')); });
}
int main() {
  lease_bind_foreign_nodes();
  lease_tests();
  http_tests();
  admission_and_identity_tests();
  std::cout << "xrpc C++ ownership, limits, async replies, identity and "
               "keep-alive passed\n";
}
