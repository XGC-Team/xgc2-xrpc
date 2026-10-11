// Drives the xgc2-xrpc-method-interop-server binary (the peer of the
// cross-language method tests) as a separate process through the C++ clients:
// its command line, its READY line, the same methods over http.v1 and udp.v1,
// the shared instance and its shutdown on SIGTERM.
//
// Usage: method_interop_test <path of xgc2-xrpc-method-interop-server>
#include "xgc2/xrpc/http.hpp"
#include "xgc2/xrpc/method.hpp"
#include "xgc2/xrpc/udp.hpp"
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
using namespace xgc2::xrpc;
using namespace std::chrono;

namespace {
// The base64 of the bytes 1..32.
const char *key_base64 = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
std::string key_bytes() {
  std::string key;
  for (int i = 1; i <= 32; ++i) key.push_back(static_cast<char>(i));
  return key;
}

class Child {
public:
  Child(const std::string &program, const std::vector<std::string> &arguments) {
    int pipes[2];
    assert(::pipe(pipes) == 0);
    pid_ = ::fork();
    assert(pid_ >= 0);
    if (pid_ == 0) {
      ::dup2(pipes[1], STDOUT_FILENO);
      ::close(pipes[0]);
      ::close(pipes[1]);
      std::vector<char *> argv{const_cast<char *>(program.c_str())};
      for (const auto &argument : arguments) argv.push_back(const_cast<char *>(argument.c_str()));
      argv.push_back(nullptr);
      ::execv(program.c_str(), argv.data());
      ::_exit(127);
    }
    ::close(pipes[1]);
    out_ = pipes[0];
  }
  ~Child() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
    }
    ::close(out_);
  }
  std::string line(milliseconds timeout) {
    std::string text;
    const auto deadline = steady_clock::now() + timeout;
    while (steady_clock::now() < deadline) {
      pollfd ready{out_, POLLIN, 0};
      if (::poll(&ready, 1, 50) <= 0) continue;
      char c;
      if (::read(out_, &c, 1) != 1) return text;
      if (c == '\n') return text;
      text.push_back(c);
    }
    return text;
  }
  int wait(milliseconds timeout) {
    const auto deadline = steady_clock::now() + timeout;
    while (steady_clock::now() < deadline) {
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
      }
      std::this_thread::sleep_for(milliseconds(10));
    }
    return -1;
  }
  int terminate(int signal_number, milliseconds timeout) {
    ::kill(pid_, signal_number);
    return wait(timeout);
  }

private:
  pid_t pid_ = -1;
  int out_ = -1;
};

HttpRequest post(const std::string &name, std::string body) {
  HttpRequest request;
  request.method = "POST";
  request.target = std::string(method_path_prefix) + name;
  request.body = std::move(body);
  if (!request.body.empty()) request.headers.emplace_back("Content-Type", "application/json");
  return request;
}
} // namespace

int main(int argc, char **argv) {
  assert(argc == 2);
  const std::string program = argv[1];
  std::filesystem::path directory = std::filesystem::temp_directory_path() / ("xrpc-method-interop-" + std::to_string(::getpid()));
  std::filesystem::create_directories(directory);
  ::chmod(directory.c_str(), 0700);
  const std::string socket = (directory / "service.sock").string(), key_file = (directory / "keys").string();
  std::ofstream(key_file) << "# interop key\n11 " << key_base64 << "\n";

  // Bad command lines fail fast with status 2.
  for (const std::vector<std::string> &bad : std::vector<std::vector<std::string>>{
           {}, {"--unix", socket}, {"--unix", socket, "--key-file", key_file},
           {"--unix", socket, "--key-file", key_file, "--key-id", "11", "--port", "70000"},
           {"--unix", socket, "--key-file", key_file, "--key-id", "11", "--unknown", "1"},
           {"--unix", socket, "--key-file", key_file, "--key-id", "99", "--port", "0"}}) {
    Child child(program, bad);
    assert(child.wait(seconds(5)) == 2);
  }

  Child server(program, {"--unix", socket, "--bind", "127.0.0.1", "--port", "0", "--key-file", key_file, "--key-id", "11"});
  std::istringstream ready(server.line(seconds(10)));
  std::string word, instance;
  unsigned port = 0;
  assert((ready >> word >> port >> instance) && word == "READY" && port > 0 && port < 65536 && instance.size() == 32);

  udp::KeyRing keys;
  keys.add(11, key_bytes());
  const udp::Client udp_client(keys);
  const std::string endpoint = "127.0.0.1:" + std::to_string(port);
  const auto deadline = [] { return steady_clock::now() + seconds(3); };
  const auto call_udp = [&](const std::string &name, const std::string &body) {
    return udp_client.call(endpoint, 11, name, body, deadline(), udp::instance_from_hex(instance));
  };
  HttpClient http_client(socket, HttpLimits{}, instance);

  // The same names and bodies give the same results on both transports.
  const std::string echoed = "{\"robot\":\"fixture-1\",\"text\":\"caf\xc3\xa9\"}";
  const auto echo_udp = call_udp("xgc2.fixture/Echo", echoed);
  const auto echo_http = http_client.call(post("xgc2.fixture/Echo", echoed), deadline());
  assert(echo_udp.status == udp::Status::Ok && echo_udp.body == echoed && echo_udp.instance == *udp::instance_from_hex(instance));
  assert(echo_http.status == 200 && echo_http.body == echoed);
  // Count: one counter for both transports.
  assert(call_udp("xgc2.fixture/Count", "").body == "1");
  assert(http_client.call(post("xgc2.fixture/Count", ""), deadline()).body == "2");

  // Every failure code, with its details, as its udp.v1 status and its HTTP status.
  for (int code = 1; code <= 10; ++code) {
    const std::string body = "{\"code\":" + std::to_string(code) + "}";
    const auto over_http = http_client.call(post("xgc2.fixture/Fail", body), deadline());
    const auto expected = static_cast<MethodCode>(code);
    assert(over_http.status == method_code_http_status(expected));
    assert(over_http.body == "{\"error\":" + method_error_body(expected, "requested failure", "{\"requested\":" + std::to_string(code) + "}") + "}");
    const auto over_udp = call_udp("xgc2.fixture/Fail", body);
    const auto wire = static_cast<udp::Status>(code == 9 ? 10 : code); // unauthenticated has no udp.v1 form
    assert(over_udp.delivery == udp::Delivery::ResponseReceived && over_udp.status == wire);
    assert(over_udp.body == method_error_body(static_cast<MethodCode>(code == 9 ? 10 : code), "requested failure", "{\"requested\":" + std::to_string(code) + "}"));
  }
  assert(http_client.call(post("xgc2.fixture/Fail", "{\"code\":11}"), deadline()).status == 400);
  assert(call_udp("xgc2.fixture/Fail", "{\"code\":0}").status == udp::Status::InvalidArgument);

  // A reply completed from another thread.
  assert(call_udp("xgc2.fixture/Sleep", "{\"ms\":40}").body == "{\"ms\":40}");
  assert(http_client.call(post("xgc2.fixture/Sleep", "{\"ms\":40}"), deadline()).body == "{\"ms\":40}");

  // The readiness envelope lists the capability per entity, on both transports.
  const std::string describe = "{\"service\":\"xgc2-fixture-host\",\"api_version\":\"v1\",\"instance_id\":\"" + instance +
                               "\",\"ready\":true,\"facts\":{\"capabilities\":[{\"name\":\"xgc2.fixture\",\"entities\":[\"fixture-1\",\"fixture-2\"]}]}}";
  assert(call_udp("xgc2.fixture/Describe", "").body == describe);
  HttpRequest get;
  get.method = "GET";
  get.target = "/v1/describe";
  assert(http_client.call(get, deadline()).body == describe);

  // Unknown methods: not_found on both.
  assert(call_udp("xgc2.fixture/Absent", "{}").status == udp::Status::NotFound);
  assert(http_client.call(post("xgc2.fixture/Absent", "{}"), deadline()).status == 404);

  http_client.close();
  assert(server.terminate(SIGTERM, seconds(10)) == 0);
  std::filesystem::remove_all(directory);
  std::cout << "PASS the method interop server: command line, shared instance, the same methods on http.v1 and udp.v1, describe and shutdown\n";
}
