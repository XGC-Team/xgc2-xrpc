#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

namespace xgc2::xrpc {
enum class LogSeverity : std::uint8_t { Error, Warn, Info, Debug, Trace };
enum class LogFormat : std::uint8_t { Json, Text };
// Stable transport codes. Request identities are record fields, never labels
// that create new queues, counters or rate-limit buckets.
enum class DiagnosticCode : std::uint8_t {
  ConnectionAccepted,
  ConnectionRejected,
  CallStarted,
  CallCompleted,
  DeadlineExceeded,
  CallCancelled,
  PeerError,
  AdmissionRejected,
  DrainStarted,
  DrainCompleted,
  ResourceRecovered,
  Count
};
std::string_view log_severity_name(LogSeverity severity) noexcept;
std::string_view diagnostic_code_name(DiagnosticCode code) noexcept;

inline constexpr std::size_t diagnostic_identity_capacity = 128;
inline constexpr std::size_t diagnostic_output_capacity = 3072;
struct DiagnosticContext {
  std::string_view service, instance, request_id;
};
struct DiagnosticIdentity {
  std::array<char, diagnostic_identity_capacity> bytes{};
  std::uint16_t size = 0;
  std::string_view view() const noexcept { return {bytes.data(), size}; }
};
// There is deliberately no free-form message, headers, configuration or body.
struct DiagnosticRecord {
  std::uint64_t monotonic_ns = 0, elapsed_ns = 0;
  DiagnosticCode code = DiagnosticCode::ConnectionAccepted;
  LogSeverity severity = LogSeverity::Info;
  DiagnosticIdentity service, instance, request_id;
};
// Zero means invalid record/format or insufficient space. On failure output is
// untouched; success returns a complete newline-terminated record, no NUL.
std::size_t format_diagnostic(const DiagnosticRecord &record, LogFormat format,
                              char *output, std::size_t capacity) noexcept;

// Plain construction settings; the library reads no environment variables.
struct DiagnosticsOptions {
  std::size_t capacity = 256; // Positive, maximum 65536 fixed records.
  std::uint32_t per_event_per_second = 64; // Positive, fixed code buckets.
  LogSeverity level = LogSeverity::Info; // Records above this are filtered.
  LogFormat format = LogFormat::Json; // Fixed for the owner's lifetime.
};
struct DiagnosticsStats {
  std::uint64_t emitted = 0, filtered = 0, dropped_full = 0,
                dropped_contention = 0, dropped_invalid = 0,
                dropped_rate_limit = 0, dropped_sink = 0;
  std::size_t queued = 0, capacity = 0;
  std::uint64_t dropped() const noexcept {
    return dropped_full + dropped_contention + dropped_invalid +
           dropped_rate_limit + dropped_sink;
  }
};
struct DiagnosticPolicyUpdate {
  std::optional<LogSeverity> level;
  std::optional<LogFormat> format;
};
enum class DiagnosticUpdateResult {
  Applied,
  RevisionConflict,
  RestartRequired,
  InvalidArgument,
  RevisionExhausted
};
// Live settings with the revision that guards administrative updates.
struct DiagnosticsSettings {
  std::uint64_t revision = 1;
  LogSeverity level = LogSeverity::Info;
  LogFormat format = LogFormat::Json;
};

class Diagnostics {
public:
  // Callers share this owner. Construction is the only ring allocation; no
  // thread, file, automatic endpoint or environment access is created.
  // Throws std::invalid_argument for an unusable option.
  explicit Diagnostics(DiagnosticsOptions options = {});
  ~Diagnostics() = default;
  Diagnostics(const Diagnostics &) = delete;
  Diagnostics &operator=(const Diagnostics &) = delete;

  // Safe for concurrent producers. At most one queue-lock attempt, bounded
  // copies, no allocation or sink I/O. False includes filtered/dropped records.
  // Identity inputs are ASCII, at most 128 bytes each; never pass payloads or
  // secret material in an identity field. Empty identifies an unavailable ID.
  bool try_emit(DiagnosticCode code, LogSeverity severity,
                DiagnosticContext context = {},
                std::chrono::nanoseconds elapsed = {}) noexcept;

  // Only one owner may drain. The sink runs outside the queue lock and may
  // block ONLY this owner. Its view is valid until the callback returns. False
  // or an exception drops the consumed record without retry; throws propagate.
  // A drain processes at most min(maximum_records, capacity) records.
  using Sink = bool (*)(void *state, std::string_view record);
  std::size_t drain(std::size_t maximum_records, Sink sink,
                    void *state = nullptr);
  DiagnosticsStats stats() const noexcept;

  // The process owner must authorize callers before invoking this API. There
  // is no built-in administrative listener. One successful CAS updates level
  // and its revision together; the format always requires process restart.
  DiagnosticUpdateResult update(std::uint64_t expected_revision,
                                DiagnosticPolicyUpdate updates) noexcept;
  DiagnosticsSettings settings() const noexcept;

private:
  struct RateBucket {
    std::uint64_t second = 0;
    std::uint32_t count = 0;
  };
  const DiagnosticsOptions options_;
  std::atomic<std::uint64_t> revision_level_;
  std::unique_ptr<DiagnosticRecord[]> ring_;
  std::atomic_flag queue_lock_ = ATOMIC_FLAG_INIT;
  std::size_t read_ = 0, write_ = 0, count_ = 0;
  std::array<RateBucket, static_cast<std::size_t>(DiagnosticCode::Count)> rates_{};
  std::atomic<std::size_t> queued_{0};
  std::atomic<std::uint64_t> emitted_{0}, filtered_{0}, full_{0}, contention_{0},
      invalid_{0}, rate_limited_{0}, sink_dropped_{0};
};
} // namespace xgc2::xrpc
