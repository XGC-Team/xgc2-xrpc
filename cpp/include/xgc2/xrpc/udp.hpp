#pragma once
// udp.v1: one authenticated JSON request and one reply per datagram, for
// low-rate control calls on a LAN. This header depends only on the C++17
// standard library; the implementation adds POSIX sockets and OpenSSL libcrypto.
//
// Wire format (network byte order), at most 1200 bytes per datagram:
//   off len  field
//     0   4  magic "XRU1"
//     4   1  version = 1
//     5   1  type: 1 request, 2 reply
//     6   2  flags: bit 0 (requests) says expected_instance is set; others 0
//     8   4  key_id, which selects the HMAC key
//    12  16  request_id, chosen by the client, constant across retransmissions
//    28  16  instance: request: the expected server instance or zero; reply: the server's
//    44   4  request: timeout_ms (1..60000); reply: status
//    48   2  method length (request: 1..128; reply: 0)
//    50   2  body length
//    52  ..  method, then body (UTF-8, JSON), then
//    ..  32  tag = HMAC-SHA256(key[key_id], all preceding bytes)
// Datagrams with an unknown key, a bad tag or a malformed layout are dropped
// without a reply. Execution is at most once per (key_id, request_id) inside
// the server's reply-cache window (see ServerOptions); beyond it the transport
// cannot tell a replay from a new call, so non-idempotent methods must fence
// themselves with the server instance and a domain revision.
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace xgc2::xrpc::udp {
inline constexpr std::size_t max_datagram_bytes = 1200;
inline constexpr std::size_t key_bytes = 32;
inline constexpr std::size_t max_method_bytes = 128;
inline constexpr std::uint32_t max_timeout_ms = 60000;

using RequestId = std::array<std::uint8_t, 16>;
using InstanceId = std::array<std::uint8_t, 16>;
// 32 lowercase hexadecimal digits, and the strict inverse.
std::string to_hex(const InstanceId &id);
std::optional<InstanceId> instance_from_hex(std::string_view hex);

// What a client can know about the effect of a finished call; the same three
// values as xgc2::xrpc::Delivery of the other components, kept here so that
// this header stands alone.
enum class Delivery {
  NotSent,         // no datagram left this host; retrying is safe
  OutcomeUnknown,  // a request was sent and no valid reply arrived in time
  ResponseReceived // an authenticated reply from the addressed instance arrived
};
constexpr const char *delivery_name(Delivery delivery) noexcept {
  return delivery == Delivery::NotSent          ? "not_sent"
         : delivery == Delivery::OutcomeUnknown ? "outcome_unknown"
                                                : "response_received";
}

// Reply status. Anything but Ok carries an error body (see error_body).
// Unauthenticated is never sent: datagrams that fail authentication get no reply.
enum class Status : std::uint32_t {
  Ok = 0,
  InvalidArgument = 1,
  NotFound = 2,
  Conflict = 3,
  ResourceExhausted = 4,
  DeadlineExceeded = 5,
  Cancelled = 6,
  Unavailable = 7,
  Internal = 8,
  Unauthenticated = 9,
  PermissionDenied = 10
};
std::string_view status_name(Status status) noexcept; // "ok", "invalid_argument", ...

// {"code":"<name>","message":"...","details":{...}}. The message is escaped
// and invalid UTF-8 is replaced; details_json must be a JSON object and is
// inserted as it is. Throws std::invalid_argument if it does not look like one.
std::string error_body(Status status, std::string_view message,
                       std::string_view details_json = "{}");

// HMAC keys by public key_id. Keys are exactly 32 bytes.
class KeyRing {
public:
  KeyRing() = default;
  KeyRing(const KeyRing &) = default;
  KeyRing(KeyRing &&) noexcept = default;
  KeyRing &operator=(const KeyRing &) = default;
  KeyRing &operator=(KeyRing &&) noexcept = default;
  ~KeyRing(); // wipes the keys
  // Throws std::invalid_argument for a key that is not 32 bytes or an id that
  // is already present.
  void add(std::uint32_t key_id, std::string_view key);
  // Text of `<key_id> <base64 key>` lines. key_id is decimal 0..4294967295; the
  // key is canonical standard base64 of 32 bytes. Blank lines and `#` comments
  // (whole line or after the key) are ignored. Any other line, a duplicate id
  // or a key of another length throws std::invalid_argument naming the line.
  static KeyRing parse(std::string_view text);
  // parse() of a file's contents. The file should be readable only by the
  // service's user. Throws std::runtime_error if it cannot be read.
  static KeyRing load_file(const std::string &path);
  const std::array<std::uint8_t, key_bytes> *find(std::uint32_t key_id) const noexcept;
  std::size_t size() const noexcept { return keys_.size(); }
  bool empty() const noexcept { return keys_.empty(); }

private:
  std::map<std::uint32_t, std::array<std::uint8_t, key_bytes>> keys_;
};

// ---- server -------------------------------------------------------------

struct Request {
  std::string method;
  std::string body;
  std::uint32_t key_id = 0;
  RequestId request_id{};
  std::string peer; // numeric "address:port" of the sender, for diagnostics
  // Server deadline: receipt time plus the caller's timeout_ms, capped by the
  // server's call budget. A reply completed after it is not sent.
  std::chrono::steady_clock::time_point deadline;
};

namespace detail {
class ServerCore;
}

// Completes one request, at most once, from any thread before its deadline, so
// a handler can wait for a device tick without blocking the I/O thread. A Reply
// destroyed without completing sends nothing: the client sees an unknown outcome.
class Reply {
public:
  Reply() noexcept;
  Reply(Reply &&) noexcept;
  Reply &operator=(Reply &&) noexcept;
  Reply(const Reply &) = delete;
  Reply &operator=(const Reply &) = delete;
  ~Reply();
  // Sends the reply and returns true if this call completed the request. False
  // when it was already completed, its deadline passed, or the server shut
  // down: nothing is sent then. A body that does not fit the 1200-byte
  // datagram is replaced by a resource_exhausted error. The server never sends
  // Unauthenticated; passing it (or a value outside the enum) throws
  // std::invalid_argument.
  bool complete(Status status, std::string_view body);
  // complete(status, error_body(status, message, details_json)).
  bool error(Status status, std::string_view message,
             std::string_view details_json = "{}");

private:
  friend class detail::ServerCore;
  struct State;
  explicit Reply(std::unique_ptr<State> state) noexcept;
  std::unique_ptr<State> state_;
};

struct ServerOptions {
  // Numeric IPv4 or IPv6 address. "::" also accepts IPv4 senders.
  std::string bind_address = "0.0.0.0";
  std::uint16_t port = 0; // 0 selects an ephemeral port (see Server::port)
  // The 128-bit instance every reply carries. Unset: a random one per server,
  // which is what an independent service wants. A host whose domain already has
  // an instance identity sets it here, so that the transport fence and the
  // domain agree. Must not be all zero (a zero pin means "any" nowhere).
  std::optional<InstanceId> instance;
  // Upper bound for any call, whatever timeout_ms the client asks for.
  std::chrono::milliseconds call_budget{2000};
  // Requests whose handler has not completed yet; further ones get
  // resource_exhausted. Must be below reply_cache_capacity.
  std::size_t max_inflight = 64;
  // Replies kept so a retransmitted request id is answered from the cache and
  // never run again. ttl must exceed call_budget.
  std::size_t reply_cache_capacity = 1024;
  std::chrono::seconds reply_cache_ttl{120};
  // Authenticated requests accepted per source address: a token bucket
  // refilled at requests_per_second up to burst; excess datagrams are dropped.
  std::uint32_t requests_per_second = 50;
  std::uint32_t burst = 100;
  std::size_t max_rate_sources = 4096; // tracked source addresses
};

struct ServerStats {
  std::uint64_t received = 0;          // every datagram read from the socket
  std::uint64_t dropped_malformed = 0; // bad layout, never answered
  std::uint64_t dropped_auth = 0;      // unknown key or bad tag, never answered
  std::uint64_t dropped_rate = 0;      // over the per-source budget
  std::uint64_t cache_hits = 0;        // answered again from the reply cache
  std::uint64_t inflight_ignored = 0;  // retransmission of a running or expired call
  std::uint64_t unanswered = 0;        // admitted calls that ended without a reply
  std::uint64_t replies_sent = 0;      // reply datagrams written to the socket
  std::size_t inflight = 0;            // handlers not yet completed
};

// One bound UDP socket and one I/O thread. Handlers run on that thread and
// must hand slow work to others, completing through their Reply. A Reply may
// outlive the Server; completing it afterwards just returns false.
class Server {
public:
  using Handler = std::function<void(Request, Reply)>;
  // Binds immediately. Throws std::invalid_argument for unusable options or an
  // empty key ring and std::system_error if the socket cannot be bound.
  Server(ServerOptions options, KeyRing keys);
  ~Server(); // shutdown(0): stops at once, completions then return false
  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;
  // Registers a method before start(); throws std::logic_error afterwards or
  // for a duplicate or invalid name (1..128 bytes).
  void add_method(std::string name, Handler handler);
  void start();
  // Stops admitting new requests (they get unavailable), waits up to
  // drain_budget for running handlers to complete and send their replies, then
  // stops the I/O thread and closes the socket. Returns true if nothing was
  // still running. Idempotent.
  bool shutdown(std::chrono::milliseconds drain_budget = std::chrono::milliseconds{1000});
  std::uint16_t port() const noexcept;
  // ServerOptions::instance, else random per server; every reply carries it.
  const InstanceId &instance() const noexcept;
  std::string instance_hex() const { return to_hex(instance()); }
  ServerStats stats() const noexcept;

private:
  std::shared_ptr<detail::ServerCore> core_;
};

// ---- client -------------------------------------------------------------

struct ClientOptions {
  // Waits before each retransmission of the same datagram after the first
  // send, then steady_interval until a valid reply arrives or the deadline.
  std::vector<std::chrono::milliseconds> backoff{
      std::chrono::milliseconds{30}, std::chrono::milliseconds{60},
      std::chrono::milliseconds{120}, std::chrono::milliseconds{240}};
  std::chrono::milliseconds steady_interval{250};
};

struct Response {
  Delivery delivery = Delivery::NotSent;
  // ResponseReceived: the server's status. Otherwise the local failure class:
  // InvalidArgument (unusable arguments), ResourceExhausted (does not fit one
  // datagram), DeadlineExceeded (deadline passed first), Unavailable (no
  // datagram could be sent), or Conflict with OutcomeUnknown (see Client::call).
  Status status = Status::Unavailable;
  std::string body;    // the server's reply body whenever a reply was accepted
  std::string message; // local failure detail, empty when ResponseReceived
  InstanceId instance{}; // the replying server's instance; zero if none replied
  unsigned attempts = 0; // datagrams sent
};

// Blocking calls with retransmission. A call keeps no state, so one Client may
// serve any number of threads.
class Client {
public:
  explicit Client(KeyRing keys, ClientOptions options = {});
  // endpoint is a numeric "host:port" ("[::1]:19520" for IPv6); names are not
  // resolved because the resolver cannot honor the deadline. The datagram is
  // sent now and again on the backoff schedule with the same request id (the
  // server runs it once), until a valid reply arrives or the deadline passes.
  // A deadline further than max_timeout_ms away is shortened to it. A reply is
  // valid only if its tag verifies with key_id's key and it is a reply to this
  // request id (from the pinned instance, if one is set). With expected_instance
  // set, a server of another instance answers conflict without running the
  // request; that reply is returned as status Conflict with OutcomeUnknown,
  // because the pinned instance may have run the request before it went away,
  // and with the instance that answered so the caller can look at it again.
  // Other replies of a foreign instance are ignored.
  Response call(std::string_view endpoint, std::uint32_t key_id,
                std::string_view method, std::string_view body,
                std::chrono::steady_clock::time_point deadline,
                const std::optional<InstanceId> &expected_instance = std::nullopt) const;

private:
  KeyRing keys_;
  ClientOptions options_;
};
} // namespace xgc2::xrpc::udp
