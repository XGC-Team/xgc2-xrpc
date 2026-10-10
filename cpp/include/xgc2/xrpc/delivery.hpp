#pragma once

namespace xgc2::xrpc {
// What a client can know about the effect of a finished call. The same three
// values exist in every language of the SDK.
enum class Delivery {
  // Nothing reached the peer; retrying is safe.
  NotSent,
  // Bytes may have reached the peer and no usable response arrived. The
  // operation may or may not have happened, and the SDK never retries it.
  OutcomeUnknown,
  // A response from the addressed instance arrived. It may still describe a
  // failure; the response says nothing about rollback.
  ResponseReceived
};
constexpr const char *delivery_name(Delivery delivery) noexcept {
  return delivery == Delivery::NotSent          ? "not_sent"
         : delivery == Delivery::OutcomeUnknown ? "outcome_unknown"
                                                : "response_received";
}
} // namespace xgc2::xrpc
