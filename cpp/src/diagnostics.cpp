#include "xgc2/xrpc/diagnostics.hpp"
#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace xgc2::xrpc {
namespace {
static_assert(std::atomic<std::uint64_t>::is_always_lock_free &&
              std::atomic<std::size_t>::is_always_lock_free,
              "diagnostics requires lock-free counters on its target");
constexpr std::array<std::string_view, 5> severity_names{
    "error", "warn", "info", "debug", "trace"};
constexpr std::array<std::string_view,
                     static_cast<std::size_t>(DiagnosticCode::Count)>
    code_names{"connection_accepted", "connection_rejected", "call_started",
               "call_completed", "deadline_exceeded", "call_cancelled",
               "peer_error", "admission_rejected", "drain_started",
               "drain_completed", "resource_recovered"};
constexpr unsigned level_bits = 8;
constexpr std::uint64_t level_mask = 255;
constexpr std::uint64_t maximum_revision =
    std::numeric_limits<std::uint64_t>::max() >> level_bits;

LogSeverity parse_level(std::string_view value) {
  const auto found = std::find(severity_names.begin(), severity_names.end(), value);
  if (found == severity_names.end())
    throw RuntimePolicyError("LOG_LEVEL", "diagnostic severity is not supported");
  return static_cast<LogSeverity>(found - severity_names.begin());
}
LogFormat parse_format(std::string_view value) {
  if (value == "json")
    return LogFormat::Json;
  if (value == "text")
    return LogFormat::Text;
  throw RuntimePolicyError("LOG_FORMAT", "diagnostic format is not supported");
}
DiagnosticsOptions validate_options(DiagnosticsOptions options) {
  if (!options.capacity || options.capacity > 65536)
    throw std::invalid_argument("diagnostics capacity requires 1..65536 records");
  if (!options.per_event_per_second)
    throw std::invalid_argument("diagnostics event rate must be positive");
  return options;
}
std::uint64_t pack(std::uint64_t revision, LogSeverity level) {
  if (revision == 0 || revision > maximum_revision)
    throw RuntimePolicyError("revision", "diagnostic policy revision out of range");
  return (revision << level_bits) | static_cast<std::uint64_t>(level);
}
bool valid_identity(std::string_view value) noexcept {
  return value.size() <= diagnostic_identity_capacity &&
         std::all_of(value.begin(), value.end(),
                     [](unsigned char byte) { return byte <= 0x7f; });
}
bool valid_identity(const DiagnosticIdentity &value) noexcept {
  return value.size <= value.bytes.size() && valid_identity(value.view());
}
void copy_identity(DiagnosticIdentity &output, std::string_view input) noexcept {
  if (!input.empty())
    std::memcpy(output.bytes.data(), input.data(), input.size());
  output.size = static_cast<std::uint16_t>(input.size());
}
class TryGuard {
public:
  explicit TryGuard(std::atomic_flag &flag) noexcept
      : flag_(flag), held_(!flag.test_and_set(std::memory_order_acquire)) {}
  ~TryGuard() {
    if (held_)
      flag_.clear(std::memory_order_release);
  }
  explicit operator bool() const noexcept { return held_; }

private:
  std::atomic_flag &flag_;
  bool held_;
};

// Count first, then encode only when a complete output fits. All input bounds
// have been checked; neither pass allocates or invokes a sink.
struct Encoder {
  char *output = nullptr;
  std::size_t size = 0;
  void append(std::string_view text) noexcept {
    if (output && !text.empty())
      std::memcpy(output + size, text.data(), text.size());
    size += text.size();
  }
  void number(std::uint64_t value) noexcept {
    std::array<char, 20> buffer{};
    const auto result = std::to_chars(buffer.data(), buffer.data() + buffer.size(),
                                      value);
    append({buffer.data(), static_cast<std::size_t>(result.ptr - buffer.data())});
  }
  void quoted(std::string_view text) noexcept {
    constexpr char hex[] = "0123456789abcdef";
    append("\"");
    for (unsigned char byte : text) {
      if (byte == '"' || byte == '\\') {
        const char escaped[] = {'\\', static_cast<char>(byte)};
        append({escaped, 2});
      } else if (byte < 0x20 || byte == 0x7f) {
        const char escaped[] = {'\\', 'u', '0', '0', hex[byte >> 4],
                                hex[byte & 15]};
        append({escaped, 6});
      } else {
        const char value = static_cast<char>(byte);
        append({&value, 1});
      }
    }
    append("\"");
  }
};
void encode(Encoder &output, const DiagnosticRecord &record,
            LogFormat format) noexcept {
  const bool json = format == LogFormat::Json;
  output.append(json ? "{\"monotonic_ns\":" : "monotonic_ns=");
  output.number(record.monotonic_ns);
  output.append(json ? ",\"severity\":" : " severity=");
  output.quoted(log_severity_name(record.severity));
  output.append(json ? ",\"code\":" : " code=");
  output.quoted(diagnostic_code_name(record.code));
  output.append(json ? ",\"service\":" : " service=");
  output.quoted(record.service.view());
  output.append(json ? ",\"instance\":" : " instance=");
  output.quoted(record.instance.view());
  output.append(json ? ",\"request_id\":" : " request_id=");
  output.quoted(record.request_id.view());
  output.append(json ? ",\"elapsed_ns\":" : " elapsed_ns=");
  output.number(record.elapsed_ns);
  output.append(json ? "}\n" : "\n");
}
} // namespace

std::string_view log_severity_name(LogSeverity severity) noexcept {
  const auto index = static_cast<std::size_t>(severity);
  return index < severity_names.size() ? severity_names[index] : std::string_view{};
}
std::string_view diagnostic_code_name(DiagnosticCode code) noexcept {
  const auto index = static_cast<std::size_t>(code);
  return index < code_names.size() ? code_names[index] : std::string_view{};
}

std::size_t format_diagnostic(const DiagnosticRecord &record, LogFormat format,
                              std::span<char> output) noexcept {
  if (log_severity_name(record.severity).empty() ||
      diagnostic_code_name(record.code).empty() ||
      (format != LogFormat::Json && format != LogFormat::Text) ||
      !valid_identity(record.service) || !valid_identity(record.instance) ||
      !valid_identity(record.request_id))
    return 0;
  Encoder counted;
  encode(counted, record, format);
  if (counted.size > output.size())
    return 0;
  Encoder encoded{output.data()};
  encode(encoded, record, format);
  return encoded.size;
}

Diagnostics::Diagnostics(const RuntimePolicy &policy, DiagnosticsOptions options)
    : options_(validate_options(options)),
      format_(parse_format(policy.text("LOG_FORMAT"))),
      initial_revision_(policy.revision()), level_origin_(policy.field("LOG_LEVEL")),
      format_origin_(policy.field("LOG_FORMAT")),
      revision_level_(pack(initial_revision_, parse_level(policy.text("LOG_LEVEL")))),
      ring_(std::make_unique<DiagnosticRecord[]>(options_.capacity)) {
  policy.check_applied({"LOG_LEVEL", "LOG_FORMAT"}, {"diagnostics"});
  if (!level_origin_.dynamic || format_origin_.dynamic)
    throw RuntimePolicyError("diagnostics", "unsupported registry mutability");
}

bool Diagnostics::try_emit(DiagnosticCode code, LogSeverity severity,
                           DiagnosticContext context,
                           std::chrono::nanoseconds elapsed) noexcept {
  if (diagnostic_code_name(code).empty() || log_severity_name(severity).empty() ||
      elapsed.count() < 0 || !valid_identity(context.service) ||
      !valid_identity(context.instance) || !valid_identity(context.request_id)) {
    invalid_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const auto level = revision_level_.load(std::memory_order_acquire) & level_mask;
  if (static_cast<std::uint64_t>(severity) > level) {
    filtered_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const auto time = std::chrono::steady_clock::now().time_since_epoch();
  const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(time);
  DiagnosticRecord record;
  record.monotonic_ns = static_cast<std::uint64_t>(nanos.count());
  record.elapsed_ns = static_cast<std::uint64_t>(elapsed.count());
  record.code = code;
  record.severity = severity;
  copy_identity(record.service, context.service);
  copy_identity(record.instance, context.instance);
  copy_identity(record.request_id, context.request_id);

  TryGuard guard(queue_lock_);
  if (!guard) {
    contention_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  if (count_ == options_.capacity) {
    full_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  auto &bucket = rates_[static_cast<std::size_t>(code)];
  const auto second = record.monotonic_ns / 1000000000;
  if (bucket.second != second) {
    bucket.second = second;
    bucket.count = 0;
  }
  if (bucket.count >= options_.per_event_per_second) {
    rate_limited_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  ++bucket.count;
  ring_[write_] = record;
  write_ = write_ + 1 == options_.capacity ? 0 : write_ + 1;
  ++count_;
  queued_.store(count_, std::memory_order_relaxed);
  emitted_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

std::size_t Diagnostics::drain(std::size_t maximum_records, Sink sink,
                               void *state) {
  if (!sink)
    throw std::invalid_argument("diagnostic sink is required");
  const auto budget = std::min(maximum_records, options_.capacity);
  std::size_t drained = 0;
  for (; drained < budget; ++drained) {
    DiagnosticRecord record;
    {
      TryGuard guard(queue_lock_);
      if (!guard || !count_)
        break;
      record = ring_[read_];
      read_ = read_ + 1 == options_.capacity ? 0 : read_ + 1;
      --count_;
      queued_.store(count_, std::memory_order_relaxed);
    }
    std::array<char, diagnostic_output_capacity> buffer{};
    const auto size = format_diagnostic(record, format_, buffer);
    try {
      if (!size || !sink(state, {buffer.data(), size}))
        sink_dropped_.fetch_add(1, std::memory_order_relaxed);
    } catch (...) {
      sink_dropped_.fetch_add(1, std::memory_order_relaxed);
      throw;
    }
  }
  return drained;
}

DiagnosticsStats Diagnostics::stats() const noexcept {
  return {emitted_.load(std::memory_order_relaxed),
          filtered_.load(std::memory_order_relaxed),
          full_.load(std::memory_order_relaxed),
          contention_.load(std::memory_order_relaxed),
          invalid_.load(std::memory_order_relaxed),
          rate_limited_.load(std::memory_order_relaxed),
          sink_dropped_.load(std::memory_order_relaxed),
          queued_.load(std::memory_order_relaxed), options_.capacity};
}

DiagnosticUpdateResult
Diagnostics::update(std::uint64_t expected_revision,
                     DiagnosticPolicyUpdate updates) noexcept {
  auto prior = revision_level_.load(std::memory_order_acquire);
  if ((prior >> level_bits) != expected_revision)
    return DiagnosticUpdateResult::RevisionConflict;
  if ((updates.level && log_severity_name(*updates.level).empty()) ||
      (updates.format && *updates.format != LogFormat::Json &&
       *updates.format != LogFormat::Text))
    return DiagnosticUpdateResult::InvalidArgument;
  if (updates.format)
    return DiagnosticUpdateResult::RestartRequired;
  if (!updates.level)
    return DiagnosticUpdateResult::Applied;
  if (expected_revision == maximum_revision)
    return DiagnosticUpdateResult::RevisionExhausted;
  const auto desired = ((expected_revision + 1) << level_bits) |
                       static_cast<std::uint64_t>(*updates.level);
  if (!revision_level_.compare_exchange_strong(prior, desired,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire))
    return DiagnosticUpdateResult::RevisionConflict;
  return DiagnosticUpdateResult::Applied;
}

RuntimePolicySnapshot Diagnostics::effective_policy() const {
  const auto current = revision_level_.load(std::memory_order_acquire);
  RuntimePolicySnapshot snapshot;
  snapshot.revision = current >> level_bits;
  snapshot.count = 2;
  snapshot.fields[0] = level_origin_;
  snapshot.fields[0].value = std::string(log_severity_name(
      static_cast<LogSeverity>(current & level_mask)));
  if (snapshot.revision != initial_revision_) {
    snapshot.fields[0].source = "administrative";
    snapshot.fields[0].source_detail.clear();
  }
  snapshot.fields[1] = format_origin_;
  return snapshot;
}
} // namespace xgc2::xrpc
