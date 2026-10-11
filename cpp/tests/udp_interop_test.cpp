// Drives the xgc2-xrpc-udp-interop-server binary (the peer of the cross-language
// tests) through the C++ client, as a separate process: its command line, its
// READY line, each test method and its shutdown on SIGTERM.
//
// Usage: udp_interop_test <path of xgc2-xrpc-udp-interop-server>
#include "../src/udp_net.hpp"
#include "../src/udp_wire.hpp"
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
using namespace xgc2::xrpc::udp;
using namespace std::chrono;

namespace {
const detail::Key key_a = [] {
  detail::Key key;
  for (std::size_t i = 0; i < key.size(); ++i) key[i] = static_cast<std::uint8_t>(i + 1);
  return key;
}();
const detail::Key key_b = [] {
  detail::Key key;
  key.fill(0x5a);
  return key;
}();

// A child process with its stdout on a pipe.
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
  // One line of stdout, or "" on timeout or end of file.
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
  // Exit status once the process has ended, or -1 if it has not within the time.
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

std::string temp_path() {
  char pattern[] = "/tmp/xrpc-udp-interop-XXXXXX";
  const int fd = ::mkstemp(pattern);
  assert(fd >= 0);
  ::close(fd);
  return pattern;
}
std::string key_line(std::uint32_t id, const detail::Key &key) {
  return std::to_string(id) + " " + detail::base64_encode(key.data(), key.size()) + "\n";
}
KeyRing client_keys() {
  KeyRing keys;
  keys.add(11, std::string(key_a.begin(), key_a.end()));
  keys.add(12, std::string(key_b.begin(), key_b.end()));
  return keys;
}
bool contains(const std::string &text, const std::string &part) { return text.find(part) != std::string::npos; }
} // namespace

int main(int argc, char **argv) {
  assert(argc == 2);
  const std::string program = argv[1];
  const std::string key_file = temp_path();
  std::ofstream(key_file) << "# interop keys\n" << key_line(11, key_a) << key_line(12, key_b);

  // Bad command lines fail fast with status 2 and say why.
  for (const std::vector<std::string> &bad : std::vector<std::vector<std::string>>{
           {}, {"--key-file", key_file}, {"--key-file", key_file, "--key-id", "11", "--port", "x"},
           {"--key-file", key_file, "--key-id", "11", "--port", "70000"},
           {"--key-file", key_file, "--key-id", "11", "--unknown", "1"},
           {"--key-file", key_file, "--key-id", "11", "--port"}}) {
    Child child(program, bad);
    assert(child.wait(seconds(5)) == 2);
  }
  {
    Child child(program, {"--key-file", key_file, "--key-id", "99", "--port", "0"});
    assert(child.wait(seconds(5)) == 2); // no such key in the file
  }
  {
    Child child(program, {"--bind", "not-an-address", "--key-file", key_file, "--key-id", "11", "--port", "0"});
    assert(child.wait(seconds(5)) == 1);
  }

  Child server(program, {"--bind", "127.0.0.1", "--port", "0", "--key-file", key_file, "--key-id", "11"});
  std::istringstream ready(server.line(seconds(10)));
  std::string word, instance_hex;
  unsigned port = 0;
  ready >> word >> port >> instance_hex;
  assert(word == "READY" && port > 0 && port < 65536 && instance_from_hex(instance_hex));
  const auto instance = *instance_from_hex(instance_hex);
  const std::string endpoint = "127.0.0.1:" + std::to_string(port);
  Client client(client_keys());
  const auto call = [&](const std::string &method, const std::string &body, milliseconds timeout = seconds(3),
                        std::uint32_t key_id = 11, std::optional<InstanceId> pin = std::nullopt) {
    return client.call(endpoint, key_id, method, body, steady_clock::now() + timeout, pin);
  };

  const auto echo = call("test.v1/Echo", "{\"x\":[1,2,3]}");
  assert(echo.delivery == Delivery::ResponseReceived && echo.status == Status::Ok);
  assert(echo.body == "{\"x\":[1,2,3]}" && echo.instance == instance);
  // The pin from the READY line holds; another one is fenced.
  assert(call("test.v1/Echo", "{}", seconds(3), 11, instance).status == Status::Ok);
  auto stale = instance;
  stale[3] ^= 0x40;
  assert(call("test.v1/Echo", "{}", seconds(3), 11, stale).status == Status::Conflict);

  // Count proves at-most-once: a client that retransmits, and raw duplicates,
  // advance the counter once per request id.
  assert(call("test.v1/Count", "").body == "1");
  assert(call("test.v1/Count", "").body == "2");
  {
    const auto id = RequestId{{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}};
    const auto datagram = detail::encode_request(key_a, 11, id, std::nullopt, 2000, "test.v1/Count", "");
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    assert(fd >= 0);
    const auto target = *detail::make_address("127.0.0.1", static_cast<std::uint16_t>(port));
    for (int i = 0; i < 4; ++i)
      assert(::sendto(fd, datagram.data(), datagram.size(), 0, target.get(), target.size) == static_cast<ssize_t>(datagram.size()));
    std::this_thread::sleep_for(milliseconds(200));
    ::close(fd);
    assert(call("test.v1/Count", "").body == "4"); // 3 was the raw request, once
  }

  // Sleep replies from another thread; a deadline shorter than the sleep gives no reply.
  const auto start = steady_clock::now();
  const auto slept = call("test.v1/Sleep", "{\"ms\":120}");
  assert(slept.status == Status::Ok && slept.body == "{\"ms\":120}");
  assert(steady_clock::now() - start >= milliseconds(115));
  const auto missed = call("test.v1/Sleep", "{ \"ms\" : 600 }", milliseconds(150));
  assert(missed.delivery == Delivery::OutcomeUnknown && missed.status == Status::DeadlineExceeded);
  for (const char *bad : {"", "{}", "{\"ms\":-1}", "{\"ms\":1.5}", "{\"ms\":60001}", "{\"ms\":1,\"x\":2}", "[1]"}) {
    const auto refused = call("test.v1/Sleep", bad);
    assert(refused.status == Status::InvalidArgument && contains(refused.body, "\"code\":\"invalid_argument\""));
  }

  // Fail replies with the requested status and an error body; 0 and 9 cannot be sent.
  for (const std::uint32_t status : {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u, 10u}) {
    const auto failed = call("test.v1/Fail", "{\"status\":" + std::to_string(status) + "}");
    assert(failed.delivery == Delivery::ResponseReceived && static_cast<std::uint32_t>(failed.status) == status);
    assert(contains(failed.body, "\"code\":\"" + std::string(status_name(failed.status)) + "\""));
    assert(contains(failed.body, "\"message\":\"requested failure\"") && contains(failed.body, "\"details\":{\"requested\":"));
  }
  for (const char *unsendable : {"{\"status\":0}", "{\"status\":9}", "{\"status\":11}", "{}", "nonsense"})
    assert(call("test.v1/Fail", unsendable).status == Status::InvalidArgument);

  // Big cannot be answered within one datagram.
  const auto big = call("test.v1/Big", "{}");
  assert(big.delivery == Delivery::ResponseReceived && big.status == Status::ResourceExhausted);
  assert(contains(big.body, "\"code\":\"resource_exhausted\"") && big.body.size() < 200);
  assert(call("test.v1/Missing", "{}").status == Status::NotFound);

  // Only the key named on the command line is known.
  const auto other_key = call("test.v1/Echo", "{}", milliseconds(200), 12);
  assert(other_key.delivery == Delivery::OutcomeUnknown);

  assert(server.terminate(SIGTERM, seconds(5)) == 0);
  ::unlink(key_file.c_str());
  std::cout << "xgc2-xrpc-udp-interop-server: command line, READY line, Echo, Count, Sleep, Fail, Big, "
               "key selection and SIGTERM passed\n";
}
