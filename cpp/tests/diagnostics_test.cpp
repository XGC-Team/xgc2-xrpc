#include "xgc2/xrpc/diagnostics.hpp"
#include <algorithm>
#include <atomic>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <sstream>
#include <thread>

using namespace xgc2::xrpc;
namespace {
std::atomic<bool> reject_allocations{false};
std::atomic<std::uint64_t> allocations{0};
template <typename T>
concept HasBody = requires(T record) { record.body; };
template <typename T>
concept HasHeaders = requires(T record) { record.headers; };
template <typename T>
concept HasMessage = requires(T record) { record.message; };
template <typename T>
concept HasPayload = requires(T record) { record.payload; };
static_assert(!HasBody<DiagnosticRecord> && !HasHeaders<DiagnosticRecord> &&
              !HasMessage<DiagnosticRecord> && !HasPayload<DiagnosticRecord>);
static_assert(!HasBody<DiagnosticContext> && !HasHeaders<DiagnosticContext> &&
              !HasMessage<DiagnosticContext> && !HasPayload<DiagnosticContext>);

DiagnosticsOptions settings(std::size_t capacity = 256,
                            std::uint32_t per_event_per_second = 64,
                            LogSeverity level = LogSeverity::Info,
                            LogFormat format = LogFormat::Json) {
  DiagnosticsOptions options;
  options.capacity = capacity;
  options.per_event_per_second = per_event_per_second;
  options.level = level;
  options.format = format;
  return options;
}
struct Capture {
  std::array<char, diagnostic_output_capacity> bytes{};
  std::size_t size = 0, calls = 0;
};
bool capture(void *state, std::string_view record) {
  auto &output = *static_cast<Capture *>(state);
  assert(record.size() <= output.bytes.size());
  std::memcpy(output.bytes.data(), record.data(), record.size());
  output.size = record.size();
  ++output.calls;
  return true;
}
bool discard(void *, std::string_view) { return true; }
bool reject_sink(void *, std::string_view) { return false; }
bool throwing_sink(void *, std::string_view) {
  throw std::runtime_error("sink failed");
}

void filtering_saturation_and_schema() {
  Diagnostics diagnostics(settings(2, 1000));
  assert(!diagnostics.try_emit(DiagnosticCode::CallStarted, LogSeverity::Debug));
  assert(diagnostics.stats().filtered == 1);
  const DiagnosticContext context{"xgc2.test", "boot-1", "request-1"};
  assert(diagnostics.try_emit(DiagnosticCode::CallStarted, LogSeverity::Info,
                              context, std::chrono::nanoseconds{31}));
  assert(diagnostics.try_emit(DiagnosticCode::CallCompleted, LogSeverity::Info,
                              context));
  assert(!diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error));
  assert(diagnostics.stats().queued == 2 && diagnostics.stats().dropped_full == 1);
  Capture output;
  assert(diagnostics.drain(1, capture, &output) == 1);
  assert(diagnostics.stats().queued == 1 && output.calls == 1);
  boost::property_tree::ptree json;
  std::istringstream input(std::string(output.bytes.data(), output.size));
  boost::property_tree::read_json(input, json);
  assert(json.size() == 7);
  assert(json.get<std::string>("code") == "call_started");
  assert(json.get<std::string>("severity") == "info");
  assert(json.get<std::string>("service") == "xgc2.test");
  assert(json.get<std::string>("instance") == "boot-1");
  assert(json.get<std::string>("request_id") == "request-1");
  assert(json.get<std::uint64_t>("elapsed_ns") == 31);
  assert(json.get<std::uint64_t>("monotonic_ns") > 0);
  const std::string_view encoded(output.bytes.data(), output.size);
  for (const auto secret_field : {"Authorization", "cookie", "token", "body",
                                   "headers", "payload", "not-returned"})
    assert(encoded.find(secret_field) == std::string_view::npos);
  assert(diagnostics.drain(10, capture, &output) == 1);
  assert(diagnostics.drain(10, capture, &output) == 0);
  assert(diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error));
  assert(diagnostics.drain(1, reject_sink) == 1);
  assert(diagnostics.stats().dropped_sink == 1);
  assert(diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error));
  try {
    diagnostics.drain(1, throwing_sink);
    assert(false);
  } catch (const std::runtime_error &) {
  }
  assert(diagnostics.stats().dropped_sink == 2);
  assert(diagnostics.stats().queued == 0);
}

void bounded_encoding_and_escaping() {
  DiagnosticRecord record;
  record.code = DiagnosticCode::PeerError;
  record.severity = LogSeverity::Error;
  record.monotonic_ns = std::numeric_limits<std::uint64_t>::max();
  record.elapsed_ns = std::numeric_limits<std::uint64_t>::max();
  // Worst-case ASCII encoding: every byte expands into six JSON characters.
  record.service.bytes.fill('\0');
  record.instance.bytes.fill('\1');
  record.request_id.bytes.fill('\n');
  record.service.size = record.instance.size = record.request_id.size =
      diagnostic_identity_capacity;
  std::array<char, diagnostic_output_capacity + 2> output;
  output.fill('!');
  for (const auto format : {LogFormat::Json, LogFormat::Text}) {
    const auto length = format_diagnostic(
        record, format, output.data() + 1, diagnostic_output_capacity);
    assert(length && length <= diagnostic_output_capacity);
    assert(output.front() == '!' && output.back() == '!');
    assert(output[length] == '\n');
    assert(std::count(output.begin() + 1, output.begin() + 1 + length, '\n') == 1);
    if (format == LogFormat::Json) {
      boost::property_tree::ptree parsed;
      std::istringstream input(std::string(output.data() + 1, length));
      boost::property_tree::read_json(input, parsed);
      assert(parsed.get<std::string>("service") == std::string(128, '\0'));
      assert(parsed.get<std::string>("instance") == std::string(128, '\1'));
      assert(parsed.get<std::string>("request_id") == std::string(128, '\n'));
      assert(parsed.get<std::uint64_t>("elapsed_ns") == record.elapsed_ns);
    }
  }
  const std::string escaped = "quote\"slash\\line\n\x7f";
  record.service = {};
  std::memcpy(record.service.bytes.data(), escaped.data(), escaped.size());
  record.service.size = escaped.size();
  const auto length = format_diagnostic(record, LogFormat::Json, output.data(), output.size());
  const std::string_view encoded(output.data(), length);
  assert(encoded.find("quote\\\"slash\\\\line\\u000a\\u007f") !=
         std::string_view::npos);
  std::array<char, 8> too_small;
  too_small.fill('x');
  assert(!format_diagnostic(record, LogFormat::Json, too_small.data(), too_small.size()));
  assert(std::all_of(too_small.begin(), too_small.end(),
                     [](char value) { return value == 'x'; }));
  record.service.size = diagnostic_identity_capacity + 1;
  assert(!format_diagnostic(record, LogFormat::Json, output.data(), output.size()));
  record.service.size = 0;
  record.code = DiagnosticCode::Count;
  assert(!format_diagnostic(record, LogFormat::Json, output.data(), output.size()));
}

void bounded_allocation_and_inputs() {
  Diagnostics diagnostics(settings(4, 100000));
  const DiagnosticContext context{"xgc2.test", "boot-1", "request-1"};
  const auto before = allocations.load();
  reject_allocations = true;
  for (unsigned i = 0; i < 10000; ++i) {
    assert(diagnostics.try_emit(DiagnosticCode::CallCompleted, LogSeverity::Info,
                                context));
    assert(diagnostics.drain(1, discard) == 1);
  }
  reject_allocations = false;
  assert(allocations.load() == before);
  assert(diagnostics.stats().emitted == 10000 && diagnostics.stats().queued == 0);
  assert(diagnostics.stats().capacity == 4);
  const std::string too_long(diagnostic_identity_capacity + 1, 'a');
  assert(!diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error,
                               {too_long, {}, {}}));
  assert(!diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error,
                               {"\xc3\xa9", {}, {}}));
  assert(!diagnostics.try_emit(DiagnosticCode::Count, LogSeverity::Error));
  assert(!diagnostics.try_emit(DiagnosticCode::PeerError,
                               static_cast<LogSeverity>(255)));
  assert(!diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error, {},
                               std::chrono::nanoseconds{-1}));
  assert(diagnostics.stats().dropped_invalid == 5);
  for (const auto options : {settings(0, 1), settings(65537, 1), settings(1, 0),
                             settings(1, 1, static_cast<LogSeverity>(255)),
                             settings(1, 1, LogSeverity::Info,
                                      static_cast<LogFormat>(255))}) {
    try {
      Diagnostics invalid(options);
      assert(false);
    } catch (const std::invalid_argument &) {
    }
  }
}

void rate_and_bounded_drain() {
  Diagnostics limited(settings(16, 2));
  for (unsigned i = 0; i < 100; ++i) {
    (void)limited.try_emit(DiagnosticCode::PeerError, LogSeverity::Error);
    limited.drain(1, discard);
  }
  assert(limited.stats().dropped_rate_limit > 0);
  // A distinct fixed event code has its own budget, independent of request ID.
  assert(limited.try_emit(DiagnosticCode::ResourceRecovered, LogSeverity::Info));

  Diagnostics bounded(settings(2, 1000));
  assert(bounded.try_emit(DiagnosticCode::CallCompleted, LogSeverity::Info));
  const auto replenish = [](void *state, std::string_view) {
    auto &owner = *static_cast<Diagnostics *>(state);
    assert(owner.try_emit(DiagnosticCode::CallCompleted, LogSeverity::Info));
    return true;
  };
  assert(bounded.drain(std::numeric_limits<std::size_t>::max(), replenish,
                       &bounded) == 2);
  assert(bounded.stats().queued == 1);
}

void revision_and_format_settings() {
  Diagnostics diagnostics(settings(256, 64, LogSeverity::Debug, LogFormat::Text));
  auto initial = diagnostics.settings();
  assert(initial.revision == 1 && initial.level == LogSeverity::Debug &&
         initial.format == LogFormat::Text);
  assert(diagnostics.update(0, {{LogSeverity::Error}, {}}) ==
         DiagnosticUpdateResult::RevisionConflict);
  assert(diagnostics.update(1, {{}, {LogFormat::Json}}) ==
         DiagnosticUpdateResult::RestartRequired);
  assert(diagnostics.update(1, {{static_cast<LogSeverity>(255)}, {}}) ==
         DiagnosticUpdateResult::InvalidArgument);
  assert(diagnostics.settings().revision == 1);
  const auto before = allocations.load();
  reject_allocations = true;
  assert(diagnostics.update(1, {{LogSeverity::Warn}, {}}) ==
         DiagnosticUpdateResult::Applied);
  reject_allocations = false;
  assert(allocations.load() == before);
  auto changed = diagnostics.settings();
  assert(changed.revision == 2 && changed.level == LogSeverity::Warn);
  assert(changed.format == LogFormat::Text);
  assert(diagnostics.update(1, {{LogSeverity::Trace}, {}}) ==
         DiagnosticUpdateResult::RevisionConflict);
  assert(!diagnostics.try_emit(DiagnosticCode::CallStarted, LogSeverity::Info));
  assert(diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error));
  Capture output;
  diagnostics.drain(1, capture, &output);
  assert(std::string_view(output.bytes.data(), output.size).starts_with(
      "monotonic_ns="));

  std::atomic<unsigned> winners{0}, conflicts{0};
  std::array<std::thread, 4> updaters;
  for (auto &thread : updaters)
    thread = std::thread([&] {
      const auto result = diagnostics.update(2, {{LogSeverity::Debug}, {}});
      if (result == DiagnosticUpdateResult::Applied)
        ++winners;
      else if (result == DiagnosticUpdateResult::RevisionConflict)
        ++conflicts;
      else
        assert(false);
    });
  for (auto &thread : updaters)
    thread.join();
  assert(winners == 1 && conflicts == 3);
  assert(diagnostics.settings().revision == 3);
}

void concurrent_nonblocking_producers() {
  Diagnostics diagnostics(settings(64, 100000));
  std::atomic<unsigned> ready{0}, done{0};
  std::atomic<bool> start{false};
  std::array<std::thread, 4> producers;
  for (auto &thread : producers)
    thread = std::thread([&] {
      ++ready;
      while (!start.load())
        std::this_thread::yield();
      for (unsigned i = 0; i < 10000; ++i)
        (void)diagnostics.try_emit(DiagnosticCode::PeerError, LogSeverity::Error,
                                   {"xgc2.test", "boot-1", "request-1"});
      ++done;
    });
  while (ready != producers.size())
    std::this_thread::yield();
  const auto before = allocations.load();
  reject_allocations = true;
  start = true;
  while (done != producers.size())
    std::this_thread::yield();
  reject_allocations = false;
  for (auto &thread : producers)
    thread.join();
  assert(allocations.load() == before);
  const auto stats = diagnostics.stats();
  assert(stats.queued == 64 && stats.emitted == 64);
  assert(stats.emitted + stats.dropped_full + stats.dropped_contention == 40000);
  assert(diagnostics.drain(64, discard) == 64);

  struct Blocked {
    std::atomic<bool> entered{false}, release{false};
  } blocked;
  const auto sink = [](void *state, std::string_view) {
    auto &block = *static_cast<Blocked *>(state);
    block.entered = true;
    while (!block.release.load())
      std::this_thread::yield();
    return true;
  };
  assert(diagnostics.try_emit(DiagnosticCode::CallStarted, LogSeverity::Info));
  std::thread owner([&] { assert(diagnostics.drain(1, sink, &blocked) == 1); });
  while (!blocked.entered.load())
    std::this_thread::yield();
  // Sink execution holds no producer lock, even while the sink remains blocked.
  assert(diagnostics.try_emit(DiagnosticCode::CallCompleted, LogSeverity::Info));
  blocked.release = true;
  owner.join();
}
} // namespace

void *operator new(std::size_t size) {
  if (reject_allocations.load())
    throw std::bad_alloc();
  allocations.fetch_add(1);
  if (auto *pointer = std::malloc(size ? size : 1))
    return pointer;
  throw std::bad_alloc();
}
void *operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void *pointer) noexcept { std::free(pointer); }
void operator delete(void *pointer, std::size_t) noexcept { std::free(pointer); }
void operator delete[](void *pointer) noexcept { std::free(pointer); }
void operator delete[](void *pointer, std::size_t) noexcept { std::free(pointer); }

int main() {
  filtering_saturation_and_schema();
  bounded_encoding_and_escaping();
  bounded_allocation_and_inputs();
  rate_and_bounded_drain();
  revision_and_format_settings();
  concurrent_nonblocking_producers();
  std::cout << "bounded diagnostics: schema, escaping, saturation/rate drops, "
               "allocation-free producers/drain, revision CAS and owner sinks "
               "passed\n";
}
