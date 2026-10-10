#include "xgc2/xrpc/grpc.hpp"
#include "xgc2/xrpc/diagnostics.hpp"
#include "grpc_fixture.grpc.pb.h"
#include <array>
#include <cassert>
#include <condition_variable>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono_literals;
using Packet = xgc2::xrpc::test::Packet;
using Fixture = xgc2::xrpc::test::Fixture;
struct Directory {
  std::string path;
  Directory() { char name[] = "/tmp/xrpc-grpc-XXXXXX"; auto* p = ::mkdtemp(name); assert(p); path = p; }
  ~Directory() {
    if (DIR* dir = ::opendir(path.c_str())) {
      while (auto* item = ::readdir(dir)) {
        std::string name = item->d_name;
        if (name != "." && name != "..") ::unlink((path + "/" + name).c_str());
      }
      ::closedir(dir); ::rmdir(path.c_str());
    }
  }
  std::string socket() const { return path + "/service.sock"; }
};
template<class F> void throws(F&& f) {
  bool caught = false; try { f(); } catch (const std::exception&) { caught = true; } assert(caught);
}
template<class F> void await(F&& predicate) {
  const auto deadline = GrpcClock::now() + 3s;
  while (!predicate()) { assert(GrpcClock::now() < deadline); std::this_thread::sleep_for(1ms); }
}
struct Service : Fixture::Service {
  GrpcAdmission& admission;
  std::mutex mutex;
  std::condition_variable cv;
  bool entered = false, release = false;
  std::optional<GrpcWorkPermit> retained;
  std::atomic<unsigned> stream_entries{0};
  std::atomic<unsigned> discovery_entries{0};
  std::atomic<unsigned> malformed_discovery_replies{0};
  explicit Service(GrpcAdmission& a) : admission(a) {}
  grpc::Status Echo(grpc::ServerContext* context, const Packet* input, Packet* output) override {
    auto call = admission.begin(*context);
    if (!call) return call.status();
    if (input->value() == "hold") {
      std::unique_lock lock(mutex); entered = true; cv.notify_all();
      cv.wait(lock, [&] { return release; });
    } else if (input->value() == "retain") {
      std::lock_guard lock(mutex); retained.emplace(call.retain_work());
      throws([&] { auto extra = call.retain_work(); });
    } else if (input->value() == "bad-fence") {
      context->AddInitialMetadata("x-xrpc-instance-id", "foreign");
    }
    if (call.cancelled()) return {grpc::StatusCode::CANCELLED, "call cancelled"};
    output->set_value(input->value()); return grpc::Status::OK;
  }
  grpc::Status Upload(grpc::ServerContext* context, grpc::ServerReader<Packet>* reader,
                      Packet* output) override {
    auto call = admission.begin_stream(*context, *reader);
    if (!call) return call.status();
    ++stream_entries;
    Packet input; unsigned count = 0;
    while (reader->Read(&input)) ++count;
    if (call.cancelled()) return {grpc::StatusCode::CANCELLED, "stream cancelled"};
    output->set_value(std::to_string(count)); return grpc::Status::OK;
  }
  grpc::Status Watch(grpc::ServerContext* context, const Packet* input,
                     grpc::ServerWriter<Packet>* writer) override {
    // The hostile response fixture deliberately adds duplicate identity first.
    if (input->value() == "bad-fence") context->AddInitialMetadata("x-xrpc-instance-id", "foreign");
    auto call = admission.begin_stream(*context, *writer);
    if (!call) return call.status();
    ++stream_entries;
    for (unsigned i = 0; i < 4; ++i) if (!writer->Write(*input))
      return {grpc::StatusCode::CANCELLED, "write cancelled"};
    return grpc::Status::OK;
  }
  grpc::Status Chat(grpc::ServerContext* context,
                    grpc::ServerReaderWriter<Packet, Packet>* stream) override {
    auto call = admission.begin_stream(*context, *stream);
    if (!call) return call.status();
    ++stream_entries;
    Packet input;
    while (stream->Read(&input)) if (!stream->Write(input)) break;
    return call.cancelled() ? grpc::Status(grpc::StatusCode::CANCELLED, "stream cancelled") : grpc::Status::OK;
  }
  grpc::Status Discover(grpc::ServerContext* context, const Packet* input, Packet* output) override {
    const auto& mode = input->value();
    if (mode == "response-bad-request" || mode == "response-empty-instance" ||
        mode == "response-duplicate-instance") {
      // Test-only faulty native peer: bypass response decoration so an empty
      // singleton identity can be tested independently of a duplicate value.
      const auto request = context->client_metadata().find(grpc::string_ref("x-request-id"));
      assert(request != context->client_metadata().end());
      context->AddInitialMetadata("x-request-id", mode == "response-bad-request" ?
          "different.request" : std::string(request->second.data(), request->second.size()));
      context->AddInitialMetadata("x-xrpc-instance-id", mode == "response-empty-instance" ?
          "" : admission.instance_id());
      if (mode == "response-duplicate-instance")
        context->AddInitialMetadata("x-xrpc-instance-id", admission.instance_id());
      ++malformed_discovery_replies;
      output->set_value(admission.instance_id());
      return grpc::Status::OK;
    }
    auto call = admission.begin(*context, false, true);
    if (!call) return call.status();
    ++discovery_entries;
    output->set_value(admission.instance_id());
    return grpc::Status::OK;
  }
  grpc::Status InvalidStreamingDiscovery(grpc::ServerContext* context,
      grpc::ServerReaderWriter<Packet, Packet>*) override {
    auto call = admission.begin(*context, true, true);
    if (!call) return call.status();
    ++discovery_entries;
    return {grpc::StatusCode::INTERNAL, "streaming discovery was incorrectly admitted"};
  }
  void wait_entered() {
    std::unique_lock lock(mutex); assert(cv.wait_for(lock, 3s, [&] { return entered; }));
  }
  void release_handler() { std::lock_guard lock(mutex); release = true; cv.notify_all(); }
};
template<class GeneratedService>
struct ServerFirstReverse : GeneratedService::Service {
  GrpcAdmission& admission;
  std::string greeting;
  std::atomic<unsigned> domain_entries{0};
  ServerFirstReverse(GrpcAdmission& gate, std::string value)
      : admission(gate), greeting(std::move(value)) {}
  grpc::Status Connect(grpc::ServerContext* context,
                        grpc::ServerReaderWriter<Packet, Packet>* stream) override {
    auto scope = admission.begin_stream(*context, *stream);
    if (!scope) return scope.status();
    ++domain_entries;
    Packet first; first.set_value(greeting);
    if (!stream->Write(first)) return {grpc::StatusCode::CANCELLED, "initial write cancelled"};
    Packet incoming;
    while (stream->Read(&incoming)) {
      if (!stream->Write(incoming)) return {grpc::StatusCode::CANCELLED, "reply write cancelled"};
    }
    return scope.cancelled() ? grpc::Status(grpc::StatusCode::CANCELLED, "reverse stream cancelled")
                             : grpc::Status::OK;
  }
};
struct Host {
  Directory directory;
  GrpcLimits limits;
  GrpcAdmission admission;
  Service service;
  GrpcUnixServer server;
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<Fixture::Stub> stub;
  explicit Host(GrpcLimits l = {}) : limits(l), admission("instance-one", l),
      service(admission), server(UnixOptions{directory.socket()}, admission, {&service}),
      channel(make_grpc_unix_channel(directory.socket(), l)), stub(Fixture::NewStub(channel)) {}
};
grpc::Status echo(Host& host, const std::string& value = "hello", std::string instance = "instance-one") {
  grpc::ClientContext context;
  GrpcClientCall call(context, std::move(instance), GrpcClock::now() + 2s, {}, "caller.request-1");
  Packet request, response; request.set_value(value);
  auto status = call.invoke([&] { return host.stub->Echo(&context, request, &response); });
  assert(call.delivery() == GrpcDelivery::OutcomeUnknown);
  if (status.ok()) assert(response.value() == value);
  return status;
}
void multi_service_server_first_reverse_streams() {
  Directory directory;
  GrpcLimits limits; limits.inflight = 2; limits.call_timeout = 2s;
  GrpcAdmission admission("shared-reverse-instance", limits);
  Service unary(admission);
  ServerFirstReverse<xgc2::xrpc::test::ReverseOne> first_service(admission, "first.ready");
  ServerFirstReverse<xgc2::xrpc::test::ReverseTwo> second_service(admission, "second.ready");
  GrpcUnixServer server(UnixOptions{directory.socket()}, admission,
                        {&unary, &first_service, &second_service});
  auto channel = make_grpc_unix_channel(directory.socket(), limits);
  auto unary_stub = Fixture::NewStub(channel);
  auto first_stub = xgc2::xrpc::test::ReverseOne::NewStub(channel);
  auto second_stub = xgc2::xrpc::test::ReverseTwo::NewStub(channel);
  StopSource cancel_first;
  grpc::ClientContext first_context, second_context;
  GrpcClientCall first_call(first_context, admission.instance_id(),
      grpc_stream_deadline(limits, GrpcClock::now() + 2s), cancel_first.get_token(), "reverse.first");
  GrpcClientCall second_call(second_context, admission.instance_id(),
      grpc_stream_deadline(limits, GrpcClock::now() + 500ms), {}, "reverse.second");
  assert(first_call.mark_dispatched().ok());
  auto first = first_stub->Connect(&first_context);
  assert(first_call.receive_initial_metadata(*first).ok());
  assert(second_call.mark_dispatched().ok());
  auto second = second_stub->Connect(&second_context);
  assert(second_call.receive_initial_metadata(*second).ok());
  // Both peers send first: client accepts no native payload before its fence.
  Packet first_ready, second_ready;
  assert(first->Read(&first_ready) && first_ready.value() == "first.ready");
  assert(second->Read(&second_ready) && second_ready.value() == "second.ready");
  assert(admission.stats().inflight_calls == 2);
  assert(first_service.domain_entries == 1 && second_service.domain_entries == 1);
  {
    grpc::ClientContext third_context;
    GrpcClientCall third_call(third_context, admission.instance_id(), GrpcClock::now() + 1s);
    Packet input, output;
    assert(third_call.invoke([&] { return unary_stub->Echo(&third_context, input, &output); }).error_code()
           == grpc::StatusCode::RESOURCE_EXHAUSTED);
  }
  {
    grpc::ClientContext wrong_context;
    GrpcClientCall wrong_call(wrong_context, "stale-reverse-instance", GrpcClock::now() + 1s);
    assert(wrong_call.mark_dispatched().ok());
    auto wrong = first_stub->Connect(&wrong_context);
    assert(wrong_call.receive_initial_metadata(*wrong).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
    assert(!wrong_call.verify(wrong->Finish()).ok());
    assert(first_service.domain_entries == 1);
  }
  cancel_first.request_stop();
  Packet unused;
  assert(!first->Read(&unused));
  assert(first_call.verify(first->Finish()).error_code() == grpc::StatusCode::CANCELLED);
  await([&] { return admission.stats().inflight_calls == 1; });
  // Second peer remains blocked in native Read until its own finite deadline.
  assert(!second->Read(&unused));
  assert(second_call.verify(second->Finish()).error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
  await([&] { return admission.stats().inflight_calls == 0; });
  assert(server.stats().accepted_connections == 1 && server.stats().active_connections == 1);
  assert(server.shutdown_until(GrpcClock::now() + 2s));
}
void explicit_unary_discovery() {
  Host host;
  for (const auto* mode : {"missing", "matching", "empty", "duplicate", "wrong"}) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    context.AddMetadata("x-request-id", "discover.request");
    const std::string selected = mode;
    if (selected != "missing") context.AddMetadata("x-xrpc-instance-id",
        selected == "empty" ? "" : selected == "wrong" ? "stale-instance" : host.admission.instance_id());
    if (selected == "duplicate") context.AddMetadata("x-xrpc-instance-id", host.admission.instance_id());
    Packet input, output;
    const auto status = host.stub->Discover(&context, input, &output);
    if (selected == "missing" || selected == "matching") {
      assert(status.ok() && output.value() == host.admission.instance_id());
      const auto& metadata = context.GetServerInitialMetadata();
      const auto range = metadata.equal_range(grpc::string_ref("x-xrpc-instance-id"));
      assert(range.first != range.second);
      auto item = range.first;
      assert(++item == range.second);
      assert(std::string(range.first->second.data(), range.first->second.size()) == output.value());
      const auto requests = metadata.equal_range(grpc::string_ref("x-request-id"));
      assert(requests.first != requests.second);
      auto request = requests.first;
      assert(++request == requests.second);
      assert(std::string(requests.first->second.data(), requests.first->second.size()) == "discover.request");
    } else assert(status.error_code() == (selected == "duplicate" ?
        grpc::StatusCode::INVALID_ARGUMENT : grpc::StatusCode::FAILED_PRECONDITION));
  }
  assert(host.service.discovery_entries == 2);
  for (const bool request_id_present : {false, true}) {
    grpc::ClientContext context;
    if (!request_id_present) context.set_deadline(std::chrono::system_clock::now() + 1s);
    else context.AddMetadata("x-request-id", "discover.no-deadline");
    Packet input, output;
    assert(host.stub->Discover(&context, input, &output).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(host.service.discovery_entries == 2);
  }
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    context.AddMetadata("x-request-id", "discover.stream");
    auto stream = host.stub->InvalidStreamingDiscovery(&context);
    Packet output;
    assert(!stream->Read(&output));
    assert(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(host.service.discovery_entries == 2);
  }
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void client_explicit_unary_discovery() {
  Host host;
  for (const bool provide_instance : {false, true}) {
    grpc::ClientContext context;
    GrpcClientCall call(context, provide_instance ? host.admission.instance_id() : "",
        GrpcClock::now() + 1s, {}, "discovery.wrapper", true);
    assert(call.response_instance_id().empty());
    Packet input, output;
    assert(call.invoke([&] { return host.stub->Discover(&context, input, &output); }).ok());
    assert(call.response_instance_id() == host.admission.instance_id());
    assert(output.value() == call.response_instance_id());
    assert(call.delivery() == GrpcDelivery::OutcomeUnknown);
  }
  assert(host.service.discovery_entries == 2);
  for (const auto* mode : {"wrong", "empty", "duplicate"}) {
    const std::string selected = mode;
    grpc::ClientContext context;
    GrpcClientCall call(context, selected == "wrong" ? "stale-instance" :
        selected == "duplicate" ? host.admission.instance_id() : "",
        GrpcClock::now() + 1s, {}, "discovery.rejected", true);
    if (selected == "empty") context.AddMetadata("x-xrpc-instance-id", "");
    if (selected == "duplicate")
      context.AddMetadata("x-xrpc-instance-id", host.admission.instance_id());
    Packet input, output;
    const auto status = call.invoke([&] { return host.stub->Discover(&context, input, &output); });
    assert(status.error_code() == (selected == "duplicate" ?
        grpc::StatusCode::INVALID_ARGUMENT : grpc::StatusCode::FAILED_PRECONDITION));
    assert(call.response_instance_id().empty());
    assert(host.service.discovery_entries == 2);
  }
  {
    grpc::ClientContext context;
    throws([&] { GrpcClientCall call(context, "", GrpcClock::now() + 1s); });
  }
  for (const auto deadline : {GrpcClock::time_point::max(), GrpcClock::now() - 1ms}) {
    grpc::ClientContext context;
    throws([&] { GrpcClientCall call(context, "", deadline, {}, "discovery.deadline", true); });
  }
  {
    grpc::ClientContext context;
    throws([&] { GrpcClientCall call(context, "", GrpcClock::now() + 1s, {}, "bad request", true); });
  }
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "", GrpcClock::now() + 1s, {}, "discovery.no-deadline", true);
    // The native context remains caller owned; discovery does not exempt a
    // missing native deadline if a caller overrides the wrapper's deadline.
    context.set_deadline(std::chrono::system_clock::time_point::max());
    Packet input, output;
    assert(call.invoke([&] { return host.stub->Discover(&context, input, &output); }).error_code()
           == grpc::StatusCode::INVALID_ARGUMENT);
    assert(call.response_instance_id().empty());
    assert(host.service.discovery_entries == 2);
  }
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    context.AddMetadata("x-request-id", "bad request");
    Packet input, output;
    assert(host.stub->Discover(&context, input, &output).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(host.service.discovery_entries == 2);
  }
  for (const auto* mode : {"response-bad-request", "response-empty-instance", "response-duplicate-instance"}) {
    grpc::ClientContext context;
    GrpcClientCall call(context, "", GrpcClock::now() + 1s, {}, "discovery.faulty-peer", true);
    Packet input, output; input.set_value(mode);
    grpc::Status native_status;
    const auto status = call.invoke([&] {
      native_status = host.stub->Discover(&context, input, &output);
      return native_status;
    });
    assert(native_status.ok());
    assert(status.error_code() == grpc::StatusCode::FAILED_PRECONDITION);
    assert(call.response_instance_id().empty());
    assert(call.delivery() == GrpcDelivery::OutcomeUnknown);
  }
  assert(host.service.discovery_entries == 2 && host.service.malformed_discovery_replies == 3);
  {
    struct MetadataWaitProbe {
      unsigned waits = 0;
      void WaitForInitialMetadata() { ++waits; }
    } probe;
    grpc::ClientContext context;
    GrpcClientCall call(context, "", GrpcClock::now() + 1s, {}, "discovery.no-stream-wait", true);
    assert(call.receive_initial_metadata(probe).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(probe.waits == 0 && call.delivery() == GrpcDelivery::NotSent);
    assert(call.response_instance_id().empty());
    assert(call.verify_initial_metadata().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    // No native RPC has run, so sticky rejection must precede native metadata access.
    assert(call.verify(grpc::Status::OK).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(call.response_instance_id().empty());
  }
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, host.admission.instance_id(),
        grpc_stream_deadline(host.limits, GrpcClock::now() + 1s), {}, "discovery.not-a-stream", true);
    assert(call.mark_dispatched().ok());
    auto stream = host.stub->Chat(&context);
    assert(call.receive_initial_metadata(*stream).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    const auto native_status = stream->Finish();
    assert(native_status.error_code() == grpc::StatusCode::CANCELLED);
    assert(call.verify(native_status).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(call.response_instance_id().empty());
    await([&] { return host.admission.stats().inflight_calls == 0; });
  }
  {
    struct MetadataWaitProbe {
      unsigned waits = 0;
      void WaitForInitialMetadata() { ++waits; }
    } probe;
    grpc::ClientContext context;
    GrpcClientCall call(context, "", GrpcClock::now() + 1s, {}, "discovery.late-stream-misuse", true);
    Packet input, output;
    grpc::Status native_status;
    assert(call.invoke([&] {
      native_status = host.stub->Discover(&context, input, &output);
      return native_status;
    }).ok());
    assert(native_status.ok() && call.response_instance_id() == host.admission.instance_id());
    // The RPC already completed successfully: a late TryCancel cannot change
    // its native status, so the wrapper must retain its local mode rejection.
    assert(call.receive_initial_metadata(probe).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(probe.waits == 0 && call.response_instance_id().empty());
    assert(call.verify(native_status).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(call.response_instance_id().empty());
    assert(host.service.discovery_entries == 3);
  }
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void limits_validation() {
  // Defaults are valid and documented in grpc.hpp.
  const GrpcLimits defaults;
  assert(defaults.connections == 32 && defaults.inflight == 32 &&
         defaults.streams_per_connection == 32 && defaults.request_bytes == 1048576 &&
         defaults.response_bytes == 1048576 && defaults.header_bytes == 16384 &&
         defaults.call_timeout == 30000ms && defaults.idle_timeout == 30000ms &&
         defaults.shutdown_timeout == 5000ms && defaults.native_threads == 8);
  Directory directory;
  (void)make_grpc_unix_channel(directory.socket(), defaults);
  auto manual = defaults;
  // The native idle option treats zero and INT_MAX as unlimited or invalid.
  manual.idle_timeout = 0ms;
  throws([&] { (void)make_grpc_unix_channel(directory.socket(), manual); });
  manual.idle_timeout = 2147483647ms;
  throws([&] { (void)make_grpc_unix_channel(directory.socket(), manual); });
  manual = defaults; manual.request_bytes = 0;
  throws([&] { GrpcAdmission invalid("limits-instance", manual); });
  manual = defaults; manual.native_threads = 1;
  throws([&] { GrpcAdmission invalid("limits-instance", manual); });
  throws([&] { (void)make_grpc_unix_channel("relative.sock", defaults); });
}
void unary_metadata_and_limits() {
  GrpcLimits l; l.request_bytes = 256; l.response_bytes = 256;
  Host host(l);
  assert(echo(host).ok());
  assert(echo(host, "hello", "old-instance").error_code() == grpc::StatusCode::FAILED_PRECONDITION);
  assert(echo(host, "bad-fence").error_code() == grpc::StatusCode::FAILED_PRECONDITION);
  for (const auto& mode : {"missing-instance", "empty-instance", "duplicate-instance", "duplicate-request", "bad-request", "no-deadline"}) {
    grpc::ClientContext context;
    if (std::string(mode) != "no-deadline") context.set_deadline(std::chrono::system_clock::now() + 1s);
    if (std::string(mode) != "missing-instance") context.AddMetadata("x-xrpc-instance-id",
        std::string(mode) == "empty-instance" ? "" : "instance-one");
    if (std::string(mode) == "duplicate-instance") context.AddMetadata("x-xrpc-instance-id", "instance-one");
    context.AddMetadata("x-request-id", std::string(mode) == "bad-request" ? "bad request" : "request-1");
    if (std::string(mode) == "duplicate-request") context.AddMetadata("x-request-id", "request-1");
    Packet request, response;
    const auto status = host.stub->Echo(&context, request, &response);
    const auto expected = std::string(mode).find("instance") != std::string::npos &&
        std::string(mode) != "duplicate-instance" ?
        grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::INVALID_ARGUMENT;
    assert(status.error_code() == expected);
  }
  assert(echo(host, std::string(512, 'x')).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
  grpc::ClientContext context;
  throws([&] { GrpcClientCall invalid(context, "instance-one", GrpcClock::time_point::max()); });
  throws([&] { GrpcAdmission invalid("bad identity"); });
  grpc::ClientContext c1, c2;
  GrpcClientCall a(c1, "instance-one", GrpcClock::now() + 1s);
  GrpcClientCall b(c2, "instance-one", GrpcClock::now() + 1s);
  assert(a.request_id() != b.request_id());
  StopSource stop; stop.request_stop();
  grpc::ClientContext cancelled_context;
  GrpcClientCall cancelled(cancelled_context, "instance-one", GrpcClock::now() + 1s, stop.get_token());
  bool invoked = false;
  assert(cancelled.invoke([&] { invoked = true; return grpc::Status::OK; }).error_code() == grpc::StatusCode::CANCELLED);
  assert(!invoked && cancelled.delivery() == GrpcDelivery::NotSent);
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
  assert(::access(host.directory.socket().c_str(), F_OK) != 0);
}
void typed_streams() {
  GrpcLimits l; l.call_timeout = 1s;
  Host host(l);
  Packet input; input.set_value("stream-data");
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 800ms);
    assert(call.mark_dispatched().ok());
    Packet response;
    auto writer = host.stub->Upload(&context, &response);
    assert(call.receive_initial_metadata(*writer).ok());
    for (unsigned i = 0; i < 5; ++i) assert(writer->Write(input));
    assert(writer->WritesDone()); assert(call.verify(writer->Finish()).ok());
    assert(response.value() == "5");
  }
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 800ms);
    assert(call.mark_dispatched().ok());
    auto reader = host.stub->Watch(&context, input);
    assert(call.receive_initial_metadata(*reader).ok());
    Packet response; unsigned count = 0;
    while (reader->Read(&response)) { ++count; assert(response.value() == input.value()); }
    assert(count == 4); assert(call.verify(reader->Finish()).ok());
  }
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 800ms);
    assert(call.mark_dispatched().ok());
    auto stream = host.stub->Chat(&context);
    assert(call.receive_initial_metadata(*stream).ok());
    Packet response;
    assert(stream->Write(input)); assert(stream->Read(&response));
    assert(response.value() == input.value());
    assert(stream->WritesDone()); assert(!stream->Read(&response));
    assert(call.verify(stream->Finish()).ok());
  }
  const auto entries = host.service.stream_entries.load();
  for (bool finite : {false, true}) {
    grpc::ClientContext context;
    context.AddMetadata("x-xrpc-instance-id", "instance-one");
    context.AddMetadata("x-request-id", "request-1");
    if (finite) context.set_deadline(std::chrono::system_clock::now() + 5s);
    auto stream = host.stub->Chat(&context); Packet response;
    assert(!stream->Read(&response));
    assert(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
  }
  assert(entries == host.service.stream_entries.load());
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 800ms);
    context.AddMetadata("x-xrpc-instance-id", "instance-one");
    context.AddMetadata("x-xrpc-instance-id", "instance-one");
    context.AddMetadata("x-request-id", "duplicate.stream-instance");
    auto stream = host.stub->Chat(&context);
    Packet response;
    assert(!stream->Read(&response));
    assert(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
    assert(entries == host.service.stream_entries.load());
  }
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 800ms);
    assert(call.mark_dispatched().ok());
    Packet hostile; hostile.set_value("bad-fence");
    auto reader = host.stub->Watch(&context, hostile);
    assert(call.receive_initial_metadata(*reader).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
    // Do not call native Read after the fence fails; domain sees no payload.
    assert(!call.verify(reader->Finish()).ok());
  }
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void native_stream_deadline_and_cancel() {
  GrpcLimits l; l.call_timeout = 1s; l.inflight = 1;
  Host host(l);
  {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 150ms);
    assert(call.mark_dispatched().ok());
    auto stream = host.stub->Chat(&context);
    assert(call.receive_initial_metadata(*stream).ok());
    Packet input, output; input.set_value("start");
    assert(stream->Write(input)); assert(stream->Read(&output));
    // Server is blocked in native Read; caller deadline must wake it.
    const auto start = GrpcClock::now();
    assert(!stream->Read(&output));
    assert(stream->Finish().error_code() == grpc::StatusCode::DEADLINE_EXCEEDED);
    assert(GrpcClock::now() - start < 1s);
  }
  await([&] { return host.admission.stats().inflight_calls == 0; });
  {
    StopSource stop;
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 800ms, stop.get_token());
    assert(call.mark_dispatched().ok());
    auto stream = host.stub->Chat(&context);
    assert(call.receive_initial_metadata(*stream).ok());
    Packet input, output; input.set_value("cancel");
    assert(stream->Write(input)); assert(stream->Read(&output));
    stop.request_stop();
    assert(!stream->Read(&output));
    assert(stream->Finish().error_code() == grpc::StatusCode::CANCELLED);
  }
  await([&] { return host.admission.stats().inflight_calls == 0; });
  assert(echo(host).ok());
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void admission_and_failed_quiescence() {
  GrpcLimits l; l.inflight = 1; l.native_threads = 4;
  Host host(l);
  StopSource stop;
  grpc::Status held_status;
  std::thread client([&] {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 2s, stop.get_token());
    Packet input, output; input.set_value("hold");
    held_status = call.invoke([&] { return host.stub->Echo(&context, input, &output); });
  });
  host.service.wait_entered(); stop.request_stop(); client.join();
  assert(held_status.error_code() == grpc::StatusCode::CANCELLED);
  assert(host.admission.stats().inflight_calls == 1);
  assert(echo(host).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
  const auto start = GrpcClock::now();
  assert(!host.server.shutdown_until(start + 40ms));
  assert(GrpcClock::now() - start < 500ms);
  throws([&] { UnixPathLease other(UnixOptions{host.directory.socket()}); });
  host.service.release_handler();
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
  UnixPathLease next(UnixOptions{host.directory.socket()});
}
void retained_work_lease() {
  GrpcLimits l; l.inflight = 1;
  Host host(l);
  assert(echo(host, "retain").ok());
  assert(host.admission.stats().inflight_calls == 1);
  assert(echo(host).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
  assert(!host.server.shutdown_until(GrpcClock::now() + 40ms));
  throws([&] { UnixPathLease other(UnixOptions{host.directory.socket()}); });
  { std::lock_guard lock(host.service.mutex);
    assert(host.service.retained->cancelled()); host.service.retained.reset(); }
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void graceful_drain_preserves_admitted_result() {
  Host host;
  grpc::Status status;
  std::thread client([&] {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 2s);
    Packet input, output; input.set_value("hold");
    status = call.invoke([&] { return host.stub->Echo(&context, input, &output); });
    if (status.ok()) assert(output.value() == "hold");
  });
  host.service.wait_entered();
  std::thread release([&] { std::this_thread::sleep_for(40ms); host.service.release_handler(); });
  assert(host.server.drain_until(GrpcClock::now() + 1s));
  client.join(); release.join();
  assert(status.ok());
  assert(host.admission.stats().inflight_calls == 0);
  UnixPathLease next(UnixOptions{host.directory.socket()});
}
void stream_budget_rounding_margin() {
  for (auto budget : {1000ms, 1100ms, 30000ms}) {
    GrpcLimits limits; limits.call_timeout = budget;
    Host host(limits);
    for (unsigned repeat = 0; repeat < 16; ++repeat) {
      grpc::ClientContext context;
      const auto caller = GrpcClock::now() + budget;
      const auto native = grpc_stream_deadline(limits, caller);
      assert(native < caller);
      GrpcClientCall call(context, "instance-one", native);
      assert(call.mark_dispatched().ok());
      auto stream = host.stub->Chat(&context);
      assert(call.receive_initial_metadata(*stream).ok());
      assert(stream->WritesDone()); Packet response;
      assert(!stream->Read(&response)); assert(call.verify(stream->Finish()).ok());
    }
    assert(host.server.shutdown_until(GrpcClock::now() + 2s));
  }
  GrpcLimits tiny; tiny.call_timeout = 1ms;
  throws([&] { (void)grpc_stream_deadline(tiny, GrpcClock::now() + 1s); });
}
int raw_socket(const std::string& path, bool bind) {
  const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0); assert(fd >= 0);
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  assert(path.size() < sizeof(address.sun_path)); std::strcpy(address.sun_path, path.c_str());
  const int result = bind ? ::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) :
                            ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
  assert(result == 0); return fd;
}
void replacement_and_connection_cap() {
  GrpcLimits l; l.connections = 1;
  Host host(l);
  assert(echo(host).ok());
  std::vector<int> excess;
  for (unsigned i = 0; i < 8; ++i) excess.push_back(raw_socket(host.directory.socket(), false));
  await([&] { return host.server.stats().rejected_connections >= 8; });
  assert(host.server.stats().active_connections == 1);
  for (int fd : excess) ::close(fd);
  // Framework shutdown must preserve a replacement socket at the same path.
  assert(::unlink(host.directory.socket().c_str()) == 0);
  int replacement = raw_socket(host.directory.socket(), true);
  struct stat before{}, after{};
  assert(::lstat(host.directory.socket().c_str(), &before) == 0);
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
  assert(::lstat(host.directory.socket().c_str(), &after) == 0);
  assert(before.st_dev == after.st_dev && before.st_ino == after.st_ino);
  ::close(replacement);
}
void native_idle_connection_cleanup() {
  GrpcLimits limits; limits.connections = 1; limits.idle_timeout = 1000ms;
  Host host(limits);
  const int peer = raw_socket(host.directory.socket(), false);
  await([&] { return host.server.stats().active_connections == 1; });
  // A stalled preface must not pin the sole connection forever; native close
  // must also be observed by the duplicate-fd monitor.
  await([&] { return host.server.stats().active_connections == 0; });
  ::close(peer);
  assert(echo(host).ok());
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void renamed_directory_anchor() {
  Host host;
  assert(echo(host).ok());
  const auto old = host.directory.path;
  const auto moved = old + "-moved";
  assert(::rename(old.c_str(), moved.c_str()) == 0);
  assert(::mkdir(old.c_str(), 0700) == 0);
  { std::ofstream sentinel(old + "/service.sock"); sentinel << "foreign"; }
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
  assert(::access((moved + "/service.sock").c_str(), F_OK) != 0);
  std::ifstream sentinel(old + "/service.sock"); std::string content; sentinel >> content;
  assert(content == "foreign");
  ::unlink((old + "/service.sock").c_str()); ::rmdir(old.c_str());
  host.directory.path = moved;
}
std::size_t directory_count(const char* path) {
  auto* dir = ::opendir(path); assert(dir);
  std::size_t count = 0; while (auto* item = ::readdir(dir)) if (item->d_name[0] != '.') ++count;
  ::closedir(dir); return count;
}
void native_thread_quota() {
  GrpcLimits limits; limits.inflight = 16; limits.native_threads = 4;
  Host host(limits);
  for (unsigned i = 0; i < 20; ++i) assert(echo(host).ok());
  const auto baseline = directory_count("/proc/self/task");
  constexpr std::size_t callers = 12;
  std::vector<grpc::Status> statuses(callers);
  std::vector<std::thread> clients;
  for (std::size_t i = 0; i < callers; ++i) clients.emplace_back([&, i] {
    grpc::ClientContext context;
    GrpcClientCall call(context, "instance-one", GrpcClock::now() + 2s);
    Packet input, output; input.set_value("hold");
    statuses[i] = call.invoke([&] { return host.stub->Echo(&context, input, &output); });
  });
  host.service.wait_entered();
  await([&] { return host.admission.stats().inflight_calls >= 3; });
  const auto peak = directory_count("/proc/self/task");
  // Includes caller-owned test threads and gRPC global background threads.
  assert(peak <= baseline + callers + static_cast<std::size_t>(limits.native_threads) + 4);
  assert(host.admission.stats().inflight_calls <= static_cast<std::size_t>(limits.native_threads));
  host.service.release_handler();
  for (auto& client : clients) client.join();
  for (const auto& status : statuses) assert(status.ok() || status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
  std::cout << "gRPC threads warmed=" << baseline << " saturated=" << peak
            << " including " << callers << " caller threads, native cap=" << limits.native_threads << '\n';
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
void concurrent_stop_and_owner_close() {
  for (unsigned round = 0; round < 8; ++round) {
    Host host;
    assert(echo(host).ok());
    std::atomic<bool> start{false};
    std::vector<std::thread> stoppers;
    for (unsigned i = 0; i < 8; ++i) stoppers.emplace_back([&] {
      while (!start.load()) std::this_thread::yield();
      for (unsigned attempt = 0; attempt < 100; ++attempt) host.server.request_stop();
    });
    start = true;
    assert(host.server.shutdown_until(GrpcClock::now() + 2s));
    // request_stop callers remain valid while owner closes its wake FD. A new
    // eventfd must never receive a delayed write targeting a reused old fd.
    int unrelated = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); assert(unrelated >= 0);
    for (auto& stopper : stoppers) stopper.join();
    std::uint64_t value = 0;
    assert(::read(unrelated, &value, sizeof(value)) == -1 && errno == EAGAIN);
    ::close(unrelated);
    assert(host.server.shutdown_until(GrpcClock::now() + 1s));
  }
}
void injected_diagnostics() {
  Directory directory;
  Diagnostics diagnostics;
  GrpcAdmission admission("diagnostic-instance");
  admission.set_diagnostics(&diagnostics, "fixture");
  Service service(admission);
  GrpcUnixServer server(UnixOptions{directory.socket()}, admission, {&service});
  auto channel = make_grpc_unix_channel(directory.socket());
  auto stub = Fixture::NewStub(channel);
  grpc::ClientContext context;
  GrpcClientCall call(context, "diagnostic-instance", GrpcClock::now() + 1s, {}, "diag-req");
  Packet input, output; input.set_value("secret-domain-payload");
  assert(call.invoke([&] { return stub->Echo(&context, input, &output); }).ok());
  assert(server.drain_until(GrpcClock::now() + 1s));
  std::string records;
  diagnostics.drain(64, [](void* storage, std::string_view record) {
    static_cast<std::string*>(storage)->append(record); return true;
  }, &records);
  for (const auto* event : {"call_started", "call_completed", "drain_started", "drain_completed"})
    assert(records.find(event) != std::string::npos);
  assert(records.find("diag-req") != std::string::npos);
  assert(records.find("secret-domain-payload") == std::string::npos);
}
void reusable_resources() {
  Host host;
  for (unsigned i = 0; i < 100; ++i) assert(echo(host).ok());
  const auto before = directory_count("/proc/self/fd");
  for (unsigned i = 0; i < 1000; ++i) assert(echo(host).ok());
  const auto after = directory_count("/proc/self/fd");
  assert(after <= before + 1);
  assert(host.server.stats().accepted_connections == 1);
  assert(host.server.stats().active_connections == 1);
  std::cout << "gRPC reuse FD " << before << " -> " << after << ", one native channel\n";
  assert(host.server.shutdown_until(GrpcClock::now() + 2s));
}
int main() {
  multi_service_server_first_reverse_streams(); explicit_unary_discovery(); client_explicit_unary_discovery();
  limits_validation();
  unary_metadata_and_limits(); typed_streams(); native_stream_deadline_and_cancel();
  admission_and_failed_quiescence(); retained_work_lease();
  graceful_drain_preserves_admitted_result(); stream_budget_rounding_margin();
  replacement_and_connection_cap(); native_idle_connection_cleanup();
  renamed_directory_anchor(); native_thread_quota();
  concurrent_stop_and_owner_close(); injected_diagnostics(); reusable_resources();
  std::cout << "native gRPC unary/streams/fence/cancel/lease passed\n";
}
