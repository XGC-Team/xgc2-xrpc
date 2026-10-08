#pragma once
#include <chrono>
#include <memory>
#include <string>
#include <sys/types.h>

namespace xgc2 {
namespace xrpc {
// Fresh process/service incarnation, independent of configured IDs and PIDs.
std::string new_instance_id();
enum class ExistingPath { Fail, ReclaimUnreachable };
struct UnixOptions {
  std::string path;
  mode_t mode = 0600;
  ExistingPath existing = ExistingPath::Fail;
  std::chrono::milliseconds probe_timeout{200};
};

// One exclusive pathname lifetime. The parent must already exist, belong to
// this uid, and exclude access by other users. Lock files intentionally
// persist: unlinking a lock permits two concurrent owners to lock different
// inodes.
class UnixPathLease {
public:
  explicit UnixPathLease(UnixOptions options);
  // Borrows only during construction; internally duplicates with CLOEXEC and
  // verifies directory ownership/privacy and the endpoint parent's identity.
  // The caller retains ownership and may close its descriptor after return.
  UnixPathLease(UnixOptions options, int retained_parent_fd);
  ~UnixPathLease();
  UnixPathLease(UnixPathLease &&) noexcept;
  UnixPathLease &operator=(UnixPathLease &&) noexcept;
  UnixPathLease(const UnixPathLease &) = delete;
  UnixPathLease &operator=(const UnixPathLease &) = delete;
  // Transfers a nonblocking, CLOEXEC listening descriptor to the caller.
  // Bind/protect/listen occur in a retained private child; atomic linkat
  // publication refuses any existing public node without adopting its inode.
  int bind_stream(int backlog = 32);
  // External frameworks must bind this retained-directory address, rather
  // than resolve the public pathname again. Framework unlink behavior must
  // separately satisfy inode ownership (gRPC uses accepted FDs instead).
  std::string external_bind_path() const;
  // Legacy external-owner integration: the framework must establish its own
  // descriptor/path identity, serialize pathname changes, and set options.mode
  // before recording. This API only inspects the pathname, cannot prove which
  // descriptor created it, and never changes public permissions. Framework
  // cleanup must honor inode ownership. Native gRPC uses bind_stream plus
  // accepted descriptors instead of a pathname listener.
  void record_external_bind();
  // Terminal release: removes only the recorded socket inode and releases
  // flock. This lease cannot bind again after cleanup.
  void cleanup() noexcept;
  const std::string &path() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace xrpc
} // namespace xgc2
