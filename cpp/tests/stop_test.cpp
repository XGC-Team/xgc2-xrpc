#include "xgc2/xrpc/stop.hpp"
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
using namespace xgc2::xrpc;
using namespace std::chrono;

void states_and_sharing() {
  StopToken never;
  assert(!never.stop_possible() && !never.stop_requested());
  StopSource source;
  const auto token = source.get_token();
  StopSource copy = source;
  assert(token.stop_possible() && !token.stop_requested() && !source.stop_requested());
  assert(source.request_stop());
  assert(!source.request_stop() && !copy.request_stop()); // exactly one winner
  assert(token.stop_requested() && copy.stop_requested() && copy.get_token().stop_requested());
  assert(!never.stop_requested());
}

void callbacks_run_once_on_the_requesting_thread() {
  StopSource source;
  std::atomic<int> first{0}, second{0};
  std::thread::id first_thread;
  StopCallback a(source.get_token(), [&] { ++first; first_thread = std::this_thread::get_id(); });
  StopCallback b(source.get_token(), [&] { ++second; });
  assert(first == 0 && second == 0);
  std::thread requester([&] {
    assert(source.request_stop());
    assert(first_thread == std::this_thread::get_id());
  });
  requester.join();
  assert(first == 1 && second == 1);
  source.request_stop();
  assert(first == 1 && second == 1);
  // Registering after the request runs the callback immediately, right here.
  std::thread::id late_thread;
  int late = 0;
  StopCallback c(source.get_token(), [&] { ++late; late_thread = std::this_thread::get_id(); });
  assert(late == 1 && late_thread == std::this_thread::get_id());
  // A token without a source never calls back.
  bool called = false;
  { StopCallback d(StopToken{}, [&] { called = true; }); }
  assert(!called);
}

void deregistered_callbacks_never_run() {
  StopSource source;
  int calls = 0;
  { StopCallback a(source.get_token(), [&] { ++calls; }); }
  auto b = std::make_unique<StopCallback<std::function<void()>>>(
      source.get_token(), std::function<void()>([&] { ++calls; }));
  b.reset();
  source.request_stop();
  assert(calls == 0);
}

void destruction_waits_for_a_running_callback() {
  StopSource source;
  std::mutex mutex;
  std::condition_variable changed;
  bool entered = false, release = false, finished = false;
  auto callback = std::make_unique<StopCallback<std::function<void()>>>(
      source.get_token(), std::function<void()>([&] {
        std::unique_lock<std::mutex> lock(mutex);
        entered = true;
        changed.notify_all();
        changed.wait(lock, [&] { return release; });
        finished = true;
      }));
  std::thread requester([&] { source.request_stop(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    assert(changed.wait_for(lock, seconds(5), [&] { return entered; }));
  }
  std::atomic<bool> destroyed{false};
  std::thread destroyer([&] { callback.reset(); destroyed = true; });
  std::this_thread::sleep_for(milliseconds(100));
  assert(!destroyed); // the callback is still running; its captures are alive
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  changed.notify_all();
  destroyer.join();
  requester.join();
  assert(destroyed && finished);
}

void a_callback_may_destroy_its_own_registration() {
  StopSource source;
  std::unique_ptr<StopCallback<std::function<void()>>> self;
  int calls = 0;
  self = std::make_unique<StopCallback<std::function<void()>>>(
      source.get_token(), std::function<void()>([&] {
        ++calls;
        self.reset(); // would deadlock if it waited for its own completion
      }));
  std::thread requester([&] { source.request_stop(); });
  requester.join();
  assert(calls == 1 && !self);
}

void a_callback_may_destroy_a_pending_sibling() {
  StopSource source;
  int sibling_calls = 0;
  std::unique_ptr<StopCallback<std::function<void()>>> sibling;
  // The newest registration runs first, so the sibling is still pending.
  sibling = std::make_unique<StopCallback<std::function<void()>>>(
      source.get_token(), std::function<void()>([&] { ++sibling_calls; }));
  StopCallback first(source.get_token(), [&] { sibling.reset(); });
  source.request_stop();
  assert(sibling_calls == 0 && !sibling);
}

void registration_races_with_stop() {
  for (int round = 0; round < 200; ++round) {
    StopSource source;
    std::atomic<int> started{0}, ran{0}, registered{0};
    std::atomic<bool> go{false};
    constexpr int workers = 6;
    std::vector<std::thread> threads;
    for (int i = 0; i < workers; ++i)
      threads.emplace_back([&] {
        ++started;
        while (!go.load()) std::this_thread::yield();
        for (int n = 0; n < 50; ++n) {
          ++registered;
          int local = 0;
          {
            StopCallback callback(source.get_token(), [&] { ++ran; ++local; });
          }
          // Every registration either ran before it was destroyed or never ran.
          assert(local <= 1);
        }
      });
    while (started != workers) std::this_thread::yield();
    go = true;
    std::this_thread::yield();
    source.request_stop();
    for (auto &thread : threads) thread.join();
    assert(registered == workers * 50 && ran <= registered);
    // Anything registered after stop ran inline exactly once.
    int after = 0;
    StopCallback tail(source.get_token(), [&] { ++after; });
    assert(after == 1);
  }
}

void cancellable_wait() {
  std::mutex mutex;
  std::condition_variable condition;
  bool ready = false;
  StopSource source;
  // Predicate already true.
  {
    std::unique_lock<std::mutex> lock(mutex);
    assert(wait_until(condition, lock, source.get_token(), steady_clock::now(), [] { return true; }));
  }
  // Deadline passes with nothing happening.
  {
    std::unique_lock<std::mutex> lock(mutex);
    const auto start = steady_clock::now();
    assert(!wait_until(condition, lock, source.get_token(), start + milliseconds(40), [&] { return ready; }));
    assert(steady_clock::now() - start >= milliseconds(35) && lock.owns_lock());
    // No token at all: the same deadline behavior.
    assert(!wait_until(condition, lock, StopToken{}, steady_clock::now() + milliseconds(10), [&] { return ready; }));
  }
  // The predicate becomes true from another thread.
  {
    std::thread setter([&] {
      std::this_thread::sleep_for(milliseconds(20));
      { std::lock_guard<std::mutex> lock(mutex); ready = true; }
      condition.notify_all();
    });
    std::unique_lock<std::mutex> lock(mutex);
    assert(wait_until(condition, lock, source.get_token(), steady_clock::now() + seconds(5), [&] { return ready; }));
    setter.join();
  }
  // Stop interrupts a long wait promptly.
  {
    ready = false;
    std::thread stopper([&] {
      std::this_thread::sleep_for(milliseconds(30));
      source.request_stop();
    });
    std::unique_lock<std::mutex> lock(mutex);
    const auto start = steady_clock::now();
    assert(!wait_until(condition, lock, source.get_token(), start + seconds(30), [&] { return ready; }));
    assert(steady_clock::now() - start < seconds(5));
    stopper.join();
    // An already stopped token returns at once, with the predicate false.
    assert(!wait_until(condition, lock, source.get_token(), steady_clock::now() + seconds(30), [&] { return ready; }));
  }
}

void stop_never_slips_past_a_waiter() {
  // The request lands anywhere between the predicate check and the sleep.
  for (int round = 0; round < 500; ++round) {
    std::mutex mutex;
    std::condition_variable condition;
    StopSource source;
    std::atomic<bool> waiting{false};
    std::atomic<bool> woke{false};
    std::thread waiter([&] {
      std::unique_lock<std::mutex> lock(mutex);
      waiting = true;
      (void)wait_until(condition, lock, source.get_token(), steady_clock::now() + seconds(20), [] { return false; });
      woke = true;
    });
    while (!waiting.load()) std::this_thread::yield();
    if (round % 3 == 1) std::this_thread::yield();
    if (round % 3 == 2) std::this_thread::sleep_for(microseconds(50));
    source.request_stop();
    const auto limit = steady_clock::now() + seconds(5);
    while (!woke.load() && steady_clock::now() < limit) std::this_thread::sleep_for(milliseconds(1));
    assert(woke.load());
    waiter.join();
  }
}

int main() {
  states_and_sharing();
  callbacks_run_once_on_the_requesting_thread();
  deregistered_callbacks_never_run();
  destruction_waits_for_a_running_callback();
  a_callback_may_destroy_its_own_registration();
  a_callback_may_destroy_a_pending_sibling();
  registration_races_with_stop();
  cancellable_wait();
  stop_never_slips_past_a_waiter();
  std::cout << "stop source: single winner, inline late registration, synchronized deregistration, "
               "self-destruction, registration races and cancellable waits passed\n";
}
