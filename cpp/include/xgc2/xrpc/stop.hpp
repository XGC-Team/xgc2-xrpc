#pragma once
// Cooperative cancellation for C++17: the part of std::stop_source,
// std::stop_token and std::stop_callback that the SDK needs, with the same
// names and semantics, plus a cancellable condition-variable wait.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace xgc2::xrpc {
namespace detail {
struct StopNode {
  StopNode *previous = nullptr, *next = nullptr;
  void (*invoke)(StopNode *) noexcept = nullptr;
  bool *destroyed = nullptr; // valid only while this callback executes
  bool linked = false, running = false, finished = false;
};

class StopState {
public:
  bool stop_requested() const noexcept {
    return requested_.load(std::memory_order_acquire);
  }
  // False when stop was already requested: the caller runs the callback itself.
  bool add(StopNode *node) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (requested_.load(std::memory_order_relaxed))
      return false;
    node->previous = nullptr;
    node->next = head_;
    if (head_)
      head_->previous = node;
    head_ = node;
    node->linked = true;
    return true;
  }
  // Deregistration waits for a callback that another thread is executing, so
  // its captures may be destroyed once this returns. A callback may destroy
  // its own registration: waiting for itself would never return.
  void remove(StopNode *node) noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    if (node->linked) {
      unlink(node);
    } else if (node->running) {
      if (runner_ == std::this_thread::get_id())
        *node->destroyed = true;
      else
        finished_.wait(lock, [node] { return node->finished; });
    }
  }
  // True for the one caller that moved the state to "requested"; callbacks run
  // on that caller's thread, outside the state's lock.
  bool request() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (requested_.load(std::memory_order_relaxed))
      return false;
    requested_.store(true, std::memory_order_release);
    runner_ = std::this_thread::get_id();
    while (head_) {
      StopNode *node = head_;
      unlink(node);
      node->running = true;
      bool destroyed = false;
      node->destroyed = &destroyed;
      lock.unlock();
      node->invoke(node);
      lock.lock();
      if (!destroyed) { // otherwise the node's memory is already gone
        node->running = false;
        node->finished = true;
        finished_.notify_all();
      }
    }
    runner_ = std::thread::id();
    return true;
  }

private:
  void unlink(StopNode *node) noexcept {
    if (node->previous)
      node->previous->next = node->next;
    else
      head_ = node->next;
    if (node->next)
      node->next->previous = node->previous;
    node->previous = node->next = nullptr;
    node->linked = false;
  }
  std::atomic<bool> requested_{false};
  std::mutex mutex_;
  std::condition_variable finished_;
  StopNode *head_ = nullptr;
  std::thread::id runner_;
};
} // namespace detail

template <class Callback> class StopCallback;

// Observes a StopSource. A default-constructed token can never be stopped.
class StopToken {
public:
  StopToken() noexcept = default;
  bool stop_requested() const noexcept { return state_ && state_->stop_requested(); }
  bool stop_possible() const noexcept { return state_ != nullptr; }

private:
  friend class StopSource;
  template <class Callback> friend class StopCallback;
  explicit StopToken(std::shared_ptr<detail::StopState> state) noexcept
      : state_(std::move(state)) {}
  std::shared_ptr<detail::StopState> state_;
};

// Copies share one stop state. request_stop is safe from any thread and runs
// the registered callbacks synchronously, once.
class StopSource {
public:
  StopSource() : state_(std::make_shared<detail::StopState>()) {}
  StopToken get_token() const noexcept { return StopToken(state_); }
  bool stop_requested() const noexcept { return state_->stop_requested(); }
  bool request_stop() { return state_->request(); }

private:
  std::shared_ptr<detail::StopState> state_;
};

// Runs the callback once when the token's source requests stop, or at once on
// the constructing thread if it already did. The destructor deregisters and
// waits for a callback that another thread is still running.
template <class Callback> class StopCallback : private detail::StopNode {
public:
  template <class C>
  StopCallback(const StopToken &token, C &&callback)
      : callback_(std::forward<C>(callback)) {
    invoke = &StopCallback::run;
    if (!token.state_)
      return;
    state_ = token.state_;
    if (!state_->add(this)) {
      state_.reset();
      callback_();
    }
  }
  ~StopCallback() {
    if (state_)
      state_->remove(this);
  }
  StopCallback(const StopCallback &) = delete;
  StopCallback &operator=(const StopCallback &) = delete;

private:
  static void run(detail::StopNode *node) noexcept {
    static_cast<StopCallback *>(node)->callback_();
  }
  Callback callback_;
  std::shared_ptr<detail::StopState> state_;
};
template <class Callback>
StopCallback(const StopToken &, Callback) -> StopCallback<Callback>;

// Like std::condition_variable_any::wait_until with a stop token: returns
// predicate() once it holds, false when the deadline passes or stop is
// requested first. `lock` is held on entry and on return, and predicate()
// is always evaluated under it.
template <class Predicate>
bool wait_until(std::condition_variable &condition,
                std::unique_lock<std::mutex> &lock, const StopToken &stop,
                std::chrono::steady_clock::time_point deadline,
                Predicate predicate) {
  std::mutex &mutex = *lock.mutex();
  while (!predicate()) {
    if (stop.stop_requested() || std::chrono::steady_clock::now() >= deadline)
      return false;
    // The wake callback passes through the waiter's mutex, so a stop
    // requested after the final check below cannot slip in before the sleep
    // begins. For the same reason the mutex must not be held while the
    // callback is registered (it may run inline) or deregistered (that waits
    // for a running callback).
    lock.unlock();
    {
      StopCallback wake(stop, [&condition, &mutex] {
        { std::lock_guard<std::mutex> pass(mutex); }
        condition.notify_all();
      });
      lock.lock();
      if (!predicate() && !stop.stop_requested())
        condition.wait_until(lock, deadline);
      lock.unlock();
    }
    lock.lock();
  }
  return true;
}
} // namespace xgc2::xrpc
