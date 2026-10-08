#include "xgc2/xrpc/http.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <new>
#include <thread>
#include <unistd.h>
using namespace xgc2::xrpc;
using namespace std::chrono;
namespace {
std::atomic<std::uint64_t> allocation_requests{0}, allocation_bytes{0};
std::size_t resident_bytes() {
  std::ifstream input("/proc/self/statm");
  std::size_t virtual_pages = 0, resident_pages = 0;
  input >> virtual_pages >> resident_pages;
  assert(input && resident_pages);
  return resident_pages * static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
}
std::size_t descriptors() {
  DIR *directory = ::opendir("/proc/self/fd");
  assert(directory);
  std::size_t count = 0;
  while (const auto *entry = ::readdir(directory))
    if (entry->d_name[0] != '.')
      ++count;
  ::closedir(directory);
  return count;
}
} // namespace
// Count ordinary C++ new requests (including SDK shared libraries), not native
// malloc/aligned allocators. This is a workload observation, not a hard bound.
void *operator new(std::size_t size) {
  allocation_requests.fetch_add(1, std::memory_order_relaxed);
  allocation_bytes.fetch_add(size, std::memory_order_relaxed);
  if (void *value = std::malloc(size ? size : 1))
    return value;
  throw std::bad_alloc();
}
void operator delete(void *value) noexcept { std::free(value); }
void operator delete(void *value, std::size_t) noexcept { std::free(value); }
int main() {
  char directory[] = "/tmp/xrpc-resources-XXXXXX";
  assert(::mkdtemp(directory));
  UnixOptions options;
  options.path = std::string(directory) + "/http.sock";
  {
    HttpServer server(options, [](HttpRequest request, HttpReply reply) {
      HttpResponse response;
      response.body = std::move(request.body);
      reply.complete(std::move(response));
    });
    std::thread owner([&] { server.run(); });
    HttpClient client(options.path);
    HttpRequest request;
    request.method = "POST";
    request.target = "/echo";
    request.body.assign(65536, 'x');
    auto call = [&] {
      assert(client.call(request, Clock::now() + seconds(2)).body ==
             request.body);
    };
    for (int n = 0; n < 250; ++n)
      call();
    const auto initial_fds = descriptors(), baseline = resident_bytes();
    auto highest = baseline;
    std::array<std::uint64_t, 2000> latency_ns{};
    std::size_t sample = 0;
    const auto first_allocations = allocation_requests.load();
    const auto first_bytes = allocation_bytes.load();
    const auto began = Clock::now();
    for (int batch = 0; batch < 4; ++batch) {
      for (int n = 0; n < 500; ++n) {
        const auto started = Clock::now();
        call();
        latency_ns[sample++] = duration_cast<nanoseconds>(Clock::now() - started).count();
      }
      highest = std::max(highest, resident_bytes());
      assert(descriptors() == initial_fds);
      assert(server.stats().accepted_connections == 1);
    }
    const auto allocated = allocation_requests.load() - first_allocations;
    const auto requested_bytes = allocation_bytes.load() - first_bytes;
    const auto elapsed = Clock::now() - began;
    std::sort(latency_ns.begin(), latency_ns.end());
    // This workload bound detects retained per-call state; it is not a global
    // allocator budget or a worst-case domain-memory proof.
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define XRPC_RESOURCE_ASAN 1
#endif
#endif
#if !defined(__SANITIZE_ADDRESS__) && !defined(XRPC_RESOURCE_ASAN)
    assert(highest - baseline < 8 * 1024 * 1024);
#endif
    // ASan intentionally retains freed allocations in its quarantine; RSS
    // stabilization is measured in the normal allocator build above.
    std::cout << "warm_rss_bytes=" << baseline << " max_rss_bytes=" << highest
              << " fd_count=" << initial_fds
              << " persistent_calls=2000 payload_bytes=65536 elapsed_ms="
              << duration_cast<milliseconds>(elapsed).count()
              << " p50_us=" << latency_ns[999] / 1000.0
              << " p95_us=" << latency_ns[1899] / 1000.0
              << " p99_us=" << latency_ns[1979] / 1000.0
              << " max_us=" << latency_ns.back() / 1000.0
              << " ordinary_new_requests=" << allocated
              << " ordinary_new_requested_bytes=" << requested_bytes
              << " payload_mib_per_second=" << 125.0 / duration<double>(elapsed).count()
              << '\n';
    server.request_stop();
    owner.join();
  }
  ::unlink((options.path + ".xrpc.lock").c_str());
  ::rmdir(directory);
}
