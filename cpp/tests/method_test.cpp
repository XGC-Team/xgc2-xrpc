// MethodRouter and the helpers of method.hpp, with no transport: names, error
// bodies, the router, reply sharing, capability facts and the describe envelope.
#include "xgc2/xrpc/method.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
using namespace xgc2::xrpc;

namespace {
template <class Exception = std::invalid_argument, class F> void throws(F f) {
  bool caught = false;
  try {
    f();
  } catch (const Exception &) {
    caught = true;
  }
  assert(caught);
}

void names() {
  std::string_view service, method;
  assert(split_method("xgc2.chassis.hold/Engage", &service, &method));
  assert(service == "xgc2.chassis.hold" && method == "Engage");
  assert(split_method("test.v1/Echo") && split_method("a/b") && split_method("Svc_1.Pkg/Do_It2"));
  const std::string longest = std::string(100, 's') + "/" + std::string(27, 'm');
  assert(longest.size() == max_method_name_bytes && split_method(longest));
  assert(!split_method(std::string(100, 's') + "/" + std::string(28, 'm')));
  for (const char *bad : {"", "/", "Engage", "svc/", "/Engage", "svc/Method/extra", "svc//Method", ".svc/Method",
                          "svc./Method", "s..v/Method", "1svc/Method", "svc/1Method", "svc/Meth-od", "svc-x/Method",
                          "sv c/Method", "svc/Method ", "svc/Method\n", "svc/M\xc3\xa9thod", "svc/Method?x=1",
                          "svc/Method#x", "svc/../Method", "svc/_Method"})
    assert(!split_method(bad));
  assert(!split_method(std::string("svc/Method\0x", 12)));
  // The outputs are optional.
  assert(split_method("a/b", nullptr, nullptr));
}

void codes() {
  struct Row {
    MethodCode code;
    const char *name;
    int http;
  };
  const Row rows[] = {{MethodCode::InvalidArgument, "invalid_argument", 400}, {MethodCode::NotFound, "not_found", 404},
                      {MethodCode::Conflict, "conflict", 409}, {MethodCode::ResourceExhausted, "resource_exhausted", 429},
                      {MethodCode::DeadlineExceeded, "deadline_exceeded", 504}, {MethodCode::Cancelled, "cancelled", 499},
                      {MethodCode::Unavailable, "unavailable", 503}, {MethodCode::Internal, "internal", 500},
                      {MethodCode::Unauthenticated, "unauthenticated", 401}, {MethodCode::PermissionDenied, "permission_denied", 403}};
  // The rows are in the order of the udp.v1 statuses 1..10.
  std::uint32_t number = 0;
  for (const auto &row : rows) {
    ++number;
    assert(static_cast<std::uint32_t>(row.code) == number);
    assert(method_code_name(row.code) == row.name && method_code_http_status(row.code) == row.http);
  }
  assert(number == 10);
}

void error_bodies() {
  assert(method_error_body(MethodCode::Conflict, "stale revision") == R"({"code":"conflict","message":"stale revision","details":{}})");
  assert(method_error_body(MethodCode::NotFound, "no such robot", R"({"robot":"scout-1"})") ==
         R"({"code":"not_found","message":"no such robot","details":{"robot":"scout-1"}})");
  // Surrounding white space around the details is dropped, an empty text is {}.
  assert(method_error_body(MethodCode::Internal, "m", " \n{\"a\":1}\t ") == R"({"code":"internal","message":"m","details":{"a":1}})");
  assert(method_error_body(MethodCode::Internal, "m", "") == R"({"code":"internal","message":"m","details":{}})");
  // Escaping: quote, backslash, controls, DEL, and bytes that are not UTF-8.
  assert(method_error_body(MethodCode::Internal, "a\"b\\c\n\r\t\x01\x7f") ==
         "{\"code\":\"internal\",\"message\":\"a\\\"b\\\\c\\n\\r\\t\\u0001\\u007f\",\"details\":{}}");
  assert(method_error_body(MethodCode::Internal, "caf\xc3\xa9 \xe2\x82\xac") ==
         "{\"code\":\"internal\",\"message\":\"caf\xc3\xa9 \xe2\x82\xac\",\"details\":{}}");
  // Every byte that is not part of a well-formed sequence becomes one U+FFFD.
  struct Invalid {
    const char *bytes;
    int replaced;
  };
  for (const Invalid &invalid : {Invalid{"\xff", 1}, Invalid{"\xc0\xaf", 2}, Invalid{"\xed\xa0\x80", 3},
                                 Invalid{"\xf4\x90\x80\x80", 4}, Invalid{"\xe2\x82", 2}, Invalid{"\x80", 1}}) {
    std::string expected = "{\"code\":\"internal\",\"message\":\"x";
    for (int i = 0; i < invalid.replaced; ++i) expected += "\xef\xbf\xbd";
    expected += "y\",\"details\":{}}";
    assert(method_error_body(MethodCode::Internal, std::string("x") + invalid.bytes + "y") == expected);
  }
  throws([] { method_error_body(MethodCode::Internal, "m", "[1]"); });
  throws([] { method_error_body(MethodCode::Internal, "m", "\"x\""); });
  throws([] { method_error_body(MethodCode::Internal, "m", "{"); });
}

void router() {
  MethodRouter router;
  assert(router.empty() && router.names().empty() && router.services().empty());
  int calls = 0;
  const auto handler = [&calls](MethodRequest, MethodReply) { ++calls; };
  router.add("xgc2.chassis.hold/Engage", handler);
  router.add("xgc2.chassis.hold/Release", handler);
  router.add("xgc2.chassis/Describe", handler);
  router.add("xgc2.chassis.hold.v1/State", handler);
  router.add("a.b/Z", handler);
  router.add("a.b/X", handler);
  assert(!router.empty());
  // Sorted by name; services are distinct and sorted, though "a.b" names are not adjacent by service text.
  assert((router.names() == std::vector<std::string>{"a.b/X", "a.b/Z", "xgc2.chassis.hold.v1/State", "xgc2.chassis.hold/Engage",
                                                     "xgc2.chassis.hold/Release", "xgc2.chassis/Describe"}));
  assert((router.services() == std::vector<std::string>{"a.b", "xgc2.chassis.hold.v1", "xgc2.chassis.hold", "xgc2.chassis"}));
  const auto *found = router.find("xgc2.chassis.hold/Engage");
  assert(found);
  (*found)({}, {});
  assert(calls == 1);
  assert(!router.find("xgc2.chassis.hold/Absent") && !router.find("") && !router.find("xgc2.chassis.hold"));
  throws([&] { router.add("xgc2.chassis.hold/Engage", handler); });
  throws([&] { router.add("Engage", handler); });
  throws([&] { router.add("svc/Method", MethodRouter::Handler{}); });
  assert(router.names().size() == 6);
}

struct Recording final : MethodReply::Sink {
  std::vector<std::string> events;
  bool done = false;
  bool cancelled_flag = false;
  bool complete(std::string_view json) override {
    if (done) return false;
    done = true;
    events.push_back("ok " + std::string(json));
    return true;
  }
  bool error(MethodCode code, std::string_view message, std::string_view details) override {
    if (done) return false;
    done = true;
    events.push_back(std::string(method_code_name(code)) + " " + std::string(message) + " " + std::string(details));
    return true;
  }
  bool cancelled() const noexcept override { return cancelled_flag; }
};

void replies() {
  MethodReply unbound;
  assert(!unbound && !unbound.complete("{}") && !unbound.error(MethodCode::Internal, "m") && !unbound.cancelled());
  auto sink = std::make_shared<Recording>();
  MethodReply reply(sink), copy = reply;
  assert(reply && copy && !reply.cancelled());
  sink->cancelled_flag = true;
  assert(copy.cancelled());
  assert(copy.complete(""));        // an empty result is the JSON null
  assert(!reply.complete("{}"));    // the copies share one call
  assert(!reply.error(MethodCode::Conflict, "late"));
  assert(sink->events.size() == 1 && sink->events[0] == "ok null");
  auto second = std::make_shared<Recording>();
  MethodReply failing(second);
  assert(failing.error(MethodCode::Conflict, "stale", R"({"revision":3})"));
  assert(second->events[0] == R"(conflict stale {"revision":3})");
  // Dropping every copy without completing sends nothing.
  auto third = std::make_shared<Recording>();
  { MethodReply dropped(third); }
  assert(third->events.empty() && third.use_count() == 1);
}

void capabilities() {
  assert(capabilities_json({}) == "[]");
  assert(capabilities_json({{"xgc2.chassis.hold", {"scout-1", "scout-2"}}, {"xgc2.world.reset", {}}}) ==
         R"([{"name":"xgc2.chassis.hold","entities":["scout-1","scout-2"]},{"name":"xgc2.world.reset","entities":[]}])");
  assert(capabilities_json({{"a.b", {"caf\xc3\xa9", "q\"uote"}}}) == "[{\"name\":\"a.b\",\"entities\":[\"caf\xc3\xa9\",\"q\\\"uote\"]}]");
  throws([] { capabilities_json({{"bad name", {}}}); });
  throws([] { capabilities_json({{"a/b", {}}}); });
  throws([] { capabilities_json({{"", {}}}); });
  throws([] { capabilities_json({{"a.b", {""}}}); });
  throws([] { capabilities_json({{"a.b", {"x\ny"}}}); });
  throws([] { capabilities_json({{"a.b", {"x\x7f"}}}); });
  throws([] { capabilities_json({{"a.b", {"\xff"}}}); });
  throws([] { capabilities_json({{"a.b", {std::string(max_entity_bytes + 1, 'e')}}}); });
  assert(!capabilities_json({{"a.b", {std::string(max_entity_bytes, 'e')}}}).empty());
  throws([] { capabilities_json({{"a.b", {"x", "x"}}}); });
  throws([] { capabilities_json({{"a.b", {}}, {"a.b", {}}}); });
}

void describe() {
  assert(describe_json("gazebo-world", "v1", "0123456789abcdef0123456789abcdef", true) ==
         R"({"service":"gazebo-world","api_version":"v1","instance_id":"0123456789abcdef0123456789abcdef","ready":true,"facts":{}})");
  const auto facts = std::string("{\"world_generation\":7,\"capabilities\":") +
                     capabilities_json({{"xgc2.chassis.hold", {"scout-1"}}}) + "}";
  assert(describe_json("w", "v1", "i", false, facts) ==
         R"({"service":"w","api_version":"v1","instance_id":"i","ready":false,"facts":{"world_generation":7,"capabilities":[{"name":"xgc2.chassis.hold","entities":["scout-1"]}]}})");
  assert(describe_json("w\"", "v\n", "i", true, "  ") == "{\"service\":\"w\\\"\",\"api_version\":\"v\\n\",\"instance_id\":\"i\",\"ready\":true,\"facts\":{}}");
  throws([] { describe_json("w", "v1", "i", true, "[]"); });
  throws([] { describe_json("w", "v1", "i", true, "null"); });
}
} // namespace

int main() {
  names();
  codes();
  error_bodies();
  router();
  replies();
  capabilities();
  describe();
  std::cout << "PASS method names, error bodies, router, shared replies, capability facts and describe envelope\n";
}
