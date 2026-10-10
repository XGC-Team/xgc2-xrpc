// Codec, HMAC, base64, key ring and error body of udp.v1. No sockets involved.
#include "../src/udp_wire.hpp"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
using namespace xgc2::xrpc::udp;
using namespace xgc2::xrpc::udp::detail;

namespace {
std::string from_hex(const std::string &hex) {
  std::string bytes;
  for (std::size_t i = 0; i + 1 < hex.size(); i += 2)
    bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  return bytes;
}
std::string to_hex_string(const std::uint8_t *data, std::size_t size) {
  static const char digits[] = "0123456789abcdef";
  std::string out;
  for (std::size_t i = 0; i < size; ++i) {
    out.push_back(digits[data[i] >> 4]);
    out.push_back(digits[data[i] & 15]);
  }
  return out;
}
std::string hex_of(const std::vector<std::uint8_t> &bytes) {
  return to_hex_string(bytes.data(), bytes.size());
}
template <class Exception, class F> void throws(F &&f) {
  bool caught = false;
  try {
    f();
  } catch (const Exception &) {
    caught = true;
  }
  assert(caught);
}
Key sequential_key() {
  Key key;
  for (std::size_t i = 0; i < key.size(); ++i) key[i] = static_cast<std::uint8_t>(i);
  return key;
}
RequestId sequential_id(std::uint8_t first) {
  RequestId id;
  for (std::size_t i = 0; i < id.size(); ++i) id[i] = static_cast<std::uint8_t>(first + i);
  return id;
}

void hmac_vectors() {
  // RFC 4231 test cases 1, 2, 3 and 6 (a key longer than the block size).
  const auto check = [](const std::string &key, const std::string &data, const char *expected) {
    std::uint8_t tag[32];
    hmac_sha256(reinterpret_cast<const std::uint8_t *>(key.data()), key.size(),
                reinterpret_cast<const std::uint8_t *>(data.data()), data.size(), tag);
    assert(to_hex_string(tag, 32) == expected);
  };
  check(std::string(20, '\x0b'), "Hi There", "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
  check("Jefe", "what do ya want for nothing?", "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
  check(std::string(20, '\xaa'), std::string(50, '\xdd'), "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe");
  check(std::string(131, '\xaa'), "Test Using Larger Than Block-Size Key - Hash Key First",
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

void base64_codec() {
  const std::pair<const char *, const char *> vectors[] = {
      {"", ""}, {"f", "Zg=="}, {"fo", "Zm8="}, {"foo", "Zm9v"}, {"foob", "Zm9vYg=="},
      {"fooba", "Zm9vYmE="}, {"foobar", "Zm9vYmFy"}};
  for (const auto &[plain, encoded] : vectors) {
    const std::string text = plain;
    assert(base64_encode(reinterpret_cast<const std::uint8_t *>(text.data()), text.size()) == encoded);
    const auto decoded = base64_decode(encoded);
    assert(decoded && std::string(decoded->begin(), decoded->end()) == text);
  }
  // Every length round-trips, including all byte values.
  for (std::size_t size = 0; size < 70; ++size) {
    std::vector<std::uint8_t> bytes(size);
    for (std::size_t i = 0; i < size; ++i) bytes[i] = static_cast<std::uint8_t>(i * 37 + size);
    const auto text = base64_encode(bytes.data(), bytes.size());
    assert(text.size() % 4 == 0 && base64_decode(text) == bytes);
  }
  // Only the canonical encoding is accepted.
  for (const char *bad : {"Zg", "Zg=", "Zg===", "Z", "====", "Zm9", "Zm9v=", "=Zm9", "Zg=A", "Zh==",
                          "Zm9=", "Zm 9v", "Zm9v\n", " Zm9v", "Zm9v ", "Zm-v", "Zm_v", "Zm9v!", "Zg==Zg==",
                          "Zm8=Zm9v", "Z\xff==", "Zm9\x01"})
    assert(!base64_decode(bad));
  assert(base64_encode(nullptr, 0).empty());
}

void error_bodies_and_names() {
  assert(error_body(Status::InvalidArgument, "bad \"x\"\n\\ \x01") ==
         "{\"code\":\"invalid_argument\",\"message\":\"bad \\\"x\\\"\\n\\\\ \\u0001\",\"details\":{}}");
  assert(error_body(Status::Conflict, "m", " {\"revision\":4} ") ==
         "{\"code\":\"conflict\",\"message\":\"m\",\"details\":{\"revision\":4}}");
  assert(error_body(Status::Internal, "m", "") == "{\"code\":\"internal\",\"message\":\"m\",\"details\":{}}");
  // UTF-8 passes through; invalid sequences become U+FFFD, never invalid JSON.
  assert(error_body(Status::Internal, "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80").find("caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80") != std::string::npos);
  for (const char *invalid : {"\xff", "\xc0\xaf", "\xe0\x80\x80", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xc3", "a\xe2\x82"}) {
    const auto body = error_body(Status::Internal, invalid);
    assert(body.find("\xef\xbf\xbd") != std::string::npos);
    for (const unsigned char c : body) assert(c >= 0x20);
  }
  throws<std::invalid_argument>([] { error_body(Status::Internal, "m", "[1]"); });
  throws<std::invalid_argument>([] { error_body(Status::Internal, "m", "{"); });
  const char *names[] = {"ok", "invalid_argument", "not_found", "conflict", "resource_exhausted",
                         "deadline_exceeded", "cancelled", "unavailable", "internal",
                         "unauthenticated", "permission_denied"};
  for (std::uint32_t code = 0; code <= 10; ++code) assert(status_name(static_cast<Status>(code)) == names[code]);
  assert(status_name(static_cast<Status>(11)) == "unknown");
  assert(std::string(delivery_name(Delivery::NotSent)) == "not_sent");
  assert(std::string(delivery_name(Delivery::OutcomeUnknown)) == "outcome_unknown");
  assert(std::string(delivery_name(Delivery::ResponseReceived)) == "response_received");
}

void hex_ids() {
  const auto id = sequential_id(0x10);
  assert(to_hex(id) == "101112131415161718191a1b1c1d1e1f");
  assert(instance_from_hex("101112131415161718191a1b1c1d1e1f") == id);
  for (const char *bad : {"", "10", "101112131415161718191a1b1c1d1e1", "101112131415161718191a1b1c1d1e1f0",
                          "101112131415161718191A1B1C1D1E1F", "g01112131415161718191a1b1c1d1e1f"})
    assert(!instance_from_hex(bad));
}

// Datagrams computed with an independent implementation (Python hmac/struct)
// from the layout in xrpc.md section 5.1. Key bytes 0..31, key_id 7.
const char *golden_request_pinned =
    "585255310101000100000007101112131415161718191a1b1c1d1e1fa0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "000005dc000c0007746573742e76312f4563686f7b2261223a317dba10e3f9fa8c727b73a5e3111201352827"
    "609fd6255b7668121ae5bd6d121ef7";
const char *golden_request_minimal =
    "585255310101000000000007101112131415161718191a1b1c1d1e1f000000000000000000000000000000000000"
    "ea60000100006da82ec99a9e5ccbbfa4e4e9dcc5815db8add4fb019abe171f834545f2d2a564ea";
const char *golden_reply_ok =
    "585255310102000000000007101112131415161718191a1b1c1d1e1fb0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
    "000000000000000b7b226f6b223a747275657dcce15a9ffd7d5c358574c80be080299b1542881fa9ca6af36b05"
    "b67c127236cb";
const char *golden_reply_conflict =
    "585255310102000000000007101112131415161718191a1b1c1d1e1fb0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
    "00000003000000467b22636f6465223a22636f6e666c696374222c226d657373616765223a22736572766963"
    "6520696e7374616e6365206d69736d61746368222c2264657461696c73223a7b7d7d7ed55df0ae4d3c55a87b"
    "ba1dec4bba79c564e8a918006dbb9bdedaed41e7a52a";

std::vector<std::uint8_t> bytes_of(const std::string &hex) {
  const auto raw = from_hex(hex);
  return {raw.begin(), raw.end()};
}

void golden_datagrams() {
  const auto key = sequential_key();
  const auto id = sequential_id(0x10), pin = sequential_id(0xa0), server = sequential_id(0xb0);
  assert(hex_of(encode_request(key, 7, id, pin, 1500, "test.v1/Echo", "{\"a\":1}")) == golden_request_pinned);
  assert(hex_of(encode_request(key, 7, id, std::nullopt, 60000, "m", "")) == golden_request_minimal);
  assert(hex_of(encode_reply(key, 7, id, server, 0, "{\"ok\":true}")) == golden_reply_ok);
  const std::string conflict = error_body(Status::Conflict, "service instance mismatch");
  assert(conflict == "{\"code\":\"conflict\",\"message\":\"service instance mismatch\",\"details\":{}}");
  assert(hex_of(encode_reply(key, 7, id, server, 3, conflict)) == golden_reply_conflict);

  Datagram d;
  auto bytes = bytes_of(golden_request_pinned);
  assert(parse(bytes.data(), bytes.size(), d) == Parse::Ok && verify(d, key));
  assert(d.type == Type::Request && d.flags == flag_expected_instance && d.key_id == 7);
  assert(d.request_id == id && d.instance == pin && d.word == 1500);
  assert(d.method == "test.v1/Echo" && d.body == "{\"a\":1}");
  bytes = bytes_of(golden_reply_conflict);
  assert(parse(bytes.data(), bytes.size(), d) == Parse::Ok && verify(d, key));
  assert(d.type == Type::Reply && d.flags == 0 && d.instance == server && d.word == 3 && d.method.empty());
  assert(d.body == conflict);
}

// The vectors of the Go library's own test (go/udpx/wire_test.go), built by a
// different implementation from the same layout: both must encode and verify the
// same bytes. Same key and key_id 7, request id 0..15, instance 16..31.
const char *go_golden_request =
    "585255310101000100000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    "000005dc000c0007746573742e76312f4563686f7b2278223a317d09c76d589a3784cfc703d60ccc7e9ef946"
    "f765b0534b882e984df6eb47e68627";
const char *go_golden_reply =
    "585255310102000000000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    "00000004000000387b22636f6465223a227265736f757263655f657868617573746564222c226d657373616765"
    "223a226d222c2264657461696c73223a7b7d7dad367ae0cb0e986951e2e062f13293da29d6c591219f93a52d62"
    "2ded4080c7e8";

void go_golden_datagrams() {
  const auto key = sequential_key();
  const auto id = sequential_id(0), instance = sequential_id(0x10);
  assert(hex_of(encode_request(key, 7, id, instance, 1500, "test.v1/Echo", "{\"x\":1}")) == go_golden_request);
  const auto body = error_body(Status::ResourceExhausted, "m");
  assert(hex_of(encode_reply(key, 7, id, instance, 4, body)) == go_golden_reply);
  Datagram d;
  for (const char *vector : {go_golden_request, go_golden_reply}) {
    const auto bytes = bytes_of(vector);
    assert(parse(bytes.data(), bytes.size(), d) == Parse::Ok && verify(d, key) && d.request_id == id);
  }
}

void round_trips() {
  const auto key = sequential_key();
  const auto id = sequential_id(1), pin = sequential_id(100);
  Datagram d;
  for (const std::uint32_t key_id : {0u, 1u, 0x01020304u, 0xffffffffu})
    for (const auto &expected : {std::optional<InstanceId>{}, std::optional<InstanceId>{pin}})
      for (const std::uint32_t timeout : {1u, 2u, 60000u}) {
        const auto bytes = encode_request(key, key_id, id, expected, timeout, "xgc2.chassis.hold.v2/Engage", "{}");
        assert(parse(bytes.data(), bytes.size(), d) == Parse::Ok && verify(d, key));
        assert(d.key_id == key_id && d.word == timeout && d.request_id == id);
        assert((d.flags & flag_expected_instance) == (expected ? 1u : 0u));
        assert(d.instance == (expected ? *expected : InstanceId{}));
        assert(d.method == "xgc2.chassis.hold.v2/Engage" && d.body == "{}");
      }
  // The largest datagram: a 128-byte method and a body that fills 1200 bytes.
  const std::string method(max_method_bytes, 'm'), body(max_body_bytes(method.size()), 'b');
  const auto largest = encode_request(key, 9, id, std::nullopt, 5, method, body);
  assert(largest.size() == max_datagram_bytes);
  assert(parse(largest.data(), largest.size(), d) == Parse::Ok && verify(d, key));
  assert(d.method == method && d.body == body);
  throws<std::invalid_argument>([&] { encode_request(key, 9, id, std::nullopt, 5, method, body + "x"); });
  throws<std::invalid_argument>([&] { encode_request(key, 9, id, std::nullopt, 5, method + "m", ""); });
  throws<std::invalid_argument>([&] { encode_request(key, 9, id, std::nullopt, 5, "", "{}"); });
  const std::string reply_body(max_body_bytes(0), 'r');
  const auto big_reply = encode_reply(key, 9, id, pin, 0, reply_body);
  assert(big_reply.size() == max_datagram_bytes);
  assert(parse(big_reply.data(), big_reply.size(), d) == Parse::Ok && d.body == reply_body);
  throws<std::invalid_argument>([&] { encode_reply(key, 9, id, pin, 0, reply_body + "x"); });
  for (const std::uint32_t status : {0u, 4u, 10u}) {
    const auto reply = encode_reply(key, 3, id, pin, status, "{}");
    assert(parse(reply.data(), reply.size(), d) == Parse::Ok && d.word == status && d.type == Type::Reply);
  }
}

void malformed_layouts() {
  const auto key = sequential_key();
  const auto id = sequential_id(1);
  const auto good = encode_request(key, 7, id, std::nullopt, 100, "a/b", "body");
  Datagram d;
  assert(parse(good.data(), good.size(), d) == Parse::Ok);
  // Size limits.
  for (std::size_t size : {std::size_t{0}, std::size_t{1}, header_bytes + tag_bytes - 1})
    assert(parse(good.data(), size, d) == Parse::TooShort);
  std::vector<std::uint8_t> huge(max_datagram_bytes + 1, 0);
  assert(parse(huge.data(), huge.size(), d) == Parse::TooLong);
  // Each header byte that must hold a fixed value.
  const auto corrupted = [&](std::size_t offset, std::uint8_t value) {
    auto copy = good;
    copy[offset] = value;
    return parse(copy.data(), copy.size(), d);
  };
  for (std::size_t offset = 0; offset < 4; ++offset) assert(corrupted(offset, 0) == Parse::BadMagic);
  assert(corrupted(4, 0) == Parse::BadVersion && corrupted(4, 2) == Parse::BadVersion);
  assert(corrupted(5, 0) == Parse::BadType && corrupted(5, 3) == Parse::BadType);
  // Lengths: a request needs a method of 1..128 bytes; both must add up.
  const auto with_lengths = [&](std::uint16_t method, std::uint16_t body, std::size_t total) {
    auto copy = good;
    copy.resize(total, 0);
    copy[48] = static_cast<std::uint8_t>(method >> 8); copy[49] = static_cast<std::uint8_t>(method);
    copy[50] = static_cast<std::uint8_t>(body >> 8); copy[51] = static_cast<std::uint8_t>(body);
    return parse(copy.data(), copy.size(), d);
  };
  assert(with_lengths(3, 4, good.size()) == Parse::Ok);
  assert(with_lengths(0, 7, good.size()) == Parse::BadLength);
  assert(with_lengths(129, 0, header_bytes + 129 + tag_bytes) == Parse::BadLength);
  assert(with_lengths(128, 0, header_bytes + 128 + tag_bytes) == Parse::Ok);
  assert(with_lengths(3, 5, good.size()) == Parse::BadLength);
  assert(with_lengths(3, 3, good.size()) == Parse::BadLength);
  assert(with_lengths(65535, 65535, good.size()) == Parse::BadLength);
  assert(with_lengths(3, 4, good.size() + 1) == Parse::BadLength);
  assert(with_lengths(3, 4, good.size() - 1) == Parse::BadLength);
  // A reply must not carry a method.
  const auto reply = encode_reply(key, 7, id, id, 0, "{}");
  assert(parse(reply.data(), reply.size(), d) == Parse::Ok);
  auto with_method = reply;
  with_method[49] = 1;
  with_method.insert(with_method.begin() + header_bytes, 'x');
  assert(parse(with_method.data(), with_method.size(), d) == Parse::BadLength);
}

void tags_bind_every_byte() {
  const auto key = sequential_key();
  const auto id = sequential_id(1);
  const auto request = encode_request(key, 7, id, std::nullopt, 100, "a/b", "body");
  Datagram d;
  for (std::size_t i = 0; i < request.size(); ++i)
    for (const std::uint8_t flip : {0x01, 0x80}) {
      auto copy = request;
      copy[i] ^= flip;
      if (parse(copy.data(), copy.size(), d) == Parse::Ok) assert(!verify(d, key));
    }
  assert(parse(request.data(), request.size(), d) == Parse::Ok && verify(d, key));
  auto other = key;
  other[31] ^= 1;
  assert(!verify(d, other));
  // The tag is the HMAC of every preceding byte and nothing else.
  std::uint8_t expected[32];
  hmac_sha256(key.data(), key.size(), request.data(), request.size() - tag_bytes, expected);
  assert(std::memcmp(expected, request.data() + request.size() - tag_bytes, tag_bytes) == 0);
}

struct TempFile {
  std::string path;
  explicit TempFile(const std::string &content) {
    char pattern[] = "/tmp/xrpc-udp-keys-XXXXXX";
    const int fd = ::mkstemp(pattern);
    assert(fd >= 0);
    ::close(fd);
    path = pattern;
    std::ofstream(path, std::ios::binary) << content;
  }
  ~TempFile() { ::unlink(path.c_str()); }
};

void key_rings() {
  const std::string key_a = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="; // bytes 0..31
  const std::string key_b = "//////////////////////////////////////////8=";  // 32 x 0xff
  const auto ring = KeyRing::parse(
      "# udp.v1 keys\n\n"
      "  7 " + key_a + "   # trailing comment\n"
      "\t4294967295\t" + key_b + "\r\n"
      "0 " + key_b + "#no space before the comment\n"
      "   # indented comment\n"
      "0000000012 " + key_a);  // leading zeros, no final newline
  assert(ring.size() == 4 && !ring.empty());
  assert(ring.find(7) && *ring.find(7) == sequential_key());
  assert(ring.find(12) && *ring.find(12) == sequential_key());
  assert(ring.find(4294967295u) && (*ring.find(4294967295u))[0] == 0xff);
  assert(ring.find(0) && !ring.find(1) && !ring.find(8));
  assert(KeyRing::parse("").empty() && KeyRing::parse("\n# nothing\n\n").empty());

  // Every malformed shape is refused, and the message never quotes the key.
  const std::string short_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHg==";   // 31 bytes
  const std::string long_key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gIQ=="; // 34 bytes
  const std::vector<std::string> bad = {
      "7\n", "7 " + key_a + " extra\n", "7 8 " + key_a + "\n", "seven " + key_a + "\n",
      "-1 " + key_a + "\n", "+7 " + key_a + "\n", "4294967296 " + key_a + "\n",
      "99999999999 " + key_a + "\n", "0x7 " + key_a + "\n", "7.0 " + key_a + "\n",
      "7 " + short_key + "\n", "7 " + long_key + "\n", "7 \n", "7 AAAA\n",
      "7 " + key_a.substr(0, 43) + "\n", "7 " + key_a + "=\n",
      "7 " + key_a.substr(0, 42) + "9=\n", /* non-zero trailing bits */
      "7 " + std::string(44, '!') + "\n", "7 " + key_a + "\n7 " + key_b + "\n",
      "7 " + key_a + "\n8 " + key_b + "\nbroken\n"};
  for (const auto &text : bad) {
    try {
      KeyRing::parse(text);
      std::cerr << "accepted: " << text << '\n';
      assert(false);
    } catch (const std::invalid_argument &error) {
      assert(std::string(error.what()).find("udp key file line ") == 0);
      assert(std::string(error.what()).find(key_a) == std::string::npos);
    }
  }
  try {
    KeyRing::parse("7 " + key_a + "\n8 " + key_b + "\nbroken\n");
    assert(false);
  } catch (const std::invalid_argument &error) {
    assert(std::string(error.what()).find("line 3") != std::string::npos);
  }
  throws<std::invalid_argument>([] { KeyRing::parse(std::string("7 ") + std::string(44, '\0') + "\n"); });

  // Programmatic keys: exactly 32 bytes, unique ids.
  KeyRing manual;
  manual.add(1, std::string(32, 'k'));
  throws<std::invalid_argument>([&] { manual.add(1, std::string(32, 'j')); });
  for (const std::size_t size : {std::size_t{0}, std::size_t{16}, std::size_t{31}, std::size_t{33}})
    throws<std::invalid_argument>([&] { manual.add(2, std::string(size, 'k')); });
  assert(manual.size() == 1 && manual.find(1) && !manual.find(2));
  KeyRing copy = manual;
  copy.add(3, std::string(32, 'c'));
  assert(copy.size() == 2 && manual.size() == 1);

  // Files.
  const TempFile file("1 " + key_a + "\n2 " + key_b + "\n");
  const auto loaded = KeyRing::load_file(file.path);
  assert(loaded.size() == 2 && *loaded.find(1) == sequential_key());
  throws<std::runtime_error>([] { KeyRing::load_file("/nonexistent/udp-keys"); });
  const TempFile broken("1 " + key_a + "\n2 nonsense\n");
  throws<std::invalid_argument>([&] { KeyRing::load_file(broken.path); });
  const TempFile large(std::string(2 << 20, '#'));
  throws<std::runtime_error>([&] { KeyRing::load_file(large.path); });
}

void random_ids() {
  RequestId a{}, b{};
  random_bytes(a.data(), a.size());
  random_bytes(b.data(), b.size());
  assert(a != b && a != RequestId{} && b != RequestId{});
}
} // namespace

int main() {
  hmac_vectors();
  base64_codec();
  error_bodies_and_names();
  hex_ids();
  golden_datagrams();
  go_golden_datagrams();
  round_trips();
  malformed_layouts();
  tags_bind_every_byte();
  key_rings();
  random_ids();
  std::cout << "udp.v1 codec: HMAC and base64 vectors, golden datagrams, round trips, malformed "
               "layouts, per-byte tags, key ring parsing and error bodies passed\n";
}
