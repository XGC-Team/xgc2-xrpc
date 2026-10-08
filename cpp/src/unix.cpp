#include "xgc2/xrpc/unix.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <stdexcept>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace xgc2 {
namespace xrpc {
std::string new_instance_id() {
  unsigned char bytes[16];
  std::size_t used = 0;
  while (used < sizeof(bytes)) {
    const auto count = ::getrandom(bytes + used, sizeof(bytes) - used, 0);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0)
      throw std::runtime_error("cannot generate service incarnation");
    used += static_cast<std::size_t>(count);
  }
  static const char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(32);
  for (const auto byte : bytes) {
    result += hex[byte >> 4];
    result += hex[byte & 15];
  }
  return result;
}

namespace {
void fail(const std::string &what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}
class Fd {
public:
  explicit Fd(int value = -1) : value(value) {}
  ~Fd() {
    if (value >= 0)
      ::close(value);
  }
  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;
  int release() {
    const int result = value;
    value = -1;
    return result;
  }
  int value;
};
int open_parent(const std::string &parent) {
  Fd directory(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (directory.value < 0)
    fail("open filesystem root");
  std::size_t start = 1;
  while (start < parent.size()) {
    const auto end = parent.find('/', start);
    const auto component =
        parent.substr(start, end == std::string::npos ? end : end - start);
    if (component.empty() || component == "." || component == "..")
      throw std::invalid_argument("invalid Unix socket directory component");
    const int next = ::openat(directory.value, component.c_str(),
                              O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (next < 0)
      fail("open Unix socket parent");
    ::close(directory.value);
    directory.value = next;
    if (end == std::string::npos)
      break;
    start = end + 1;
  }
  struct stat info {};
  if (::fstat(directory.value, &info))
    fail("inspect Unix socket parent");
  if (info.st_uid != ::geteuid() || (info.st_mode & 0077))
    throw std::runtime_error(
        "Unix socket parent must be owned by current uid and private (0700)");
  return directory.release();
}
int duplicate_parent(const std::string &parent, int retained_parent_fd) {
  Fd retained(::fcntl(retained_parent_fd, F_DUPFD_CLOEXEC, 0));
  if (retained.value < 0)
    fail("duplicate retained Unix socket parent");
  struct stat retained_info {};
  if (::fstat(retained.value, &retained_info))
    fail("inspect retained Unix socket parent");
  if (!S_ISDIR(retained_info.st_mode) || retained_info.st_uid != ::geteuid() ||
      (retained_info.st_mode & 0077))
    throw std::runtime_error(
        "retained Unix socket parent must be a private directory owned by current uid");
  // Resolve the declared endpoint once for validation only. All subsequent
  // lease operations use the duplicated grant descriptor, never this walk.
  Fd declared(open_parent(parent));
  struct stat declared_info {};
  if (::fstat(declared.value, &declared_info))
    fail("inspect declared Unix socket parent");
  if (retained_info.st_dev != declared_info.st_dev ||
      retained_info.st_ino != declared_info.st_ino)
    throw std::runtime_error("retained Unix socket parent does not match endpoint path");
  return retained.release();
}
} // namespace
class UnixPathLease::Impl {
public:
  explicit Impl(UnixOptions value, int retained_parent_fd = -1) : options(std::move(value)) {
    const auto &path = options.path;
    if (path.empty() || path.front() != '/' || path.back() == '/' ||
        path.size() >= sizeof(sockaddr_un::sun_path) ||
        path.find('\0') != std::string::npos)
      throw std::invalid_argument(
          "Unix socket path must be a bounded absolute path");
    if ((options.mode & ~0777) || (options.mode & 0007) ||
        options.probe_timeout.count() <= 0 ||
        options.probe_timeout.count() > 5000)
      throw std::invalid_argument("invalid Unix socket mode or probe deadline");
    const auto slash = path.rfind('/');
    name = path.substr(slash + 1);
    if (name == "." || name == "..")
      throw std::invalid_argument("invalid Unix socket filename");
    dir.value = retained_parent_fd < 0 ? open_parent(path.substr(0, slash)) :
        duplicate_parent(path.substr(0, slash), retained_parent_fd);
    const auto lock_name = name + ".xrpc.lock";
    lock.value = ::openat(dir.value, lock_name.c_str(),
                          O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (lock.value < 0)
      fail("open Unix socket ownership lock");
    struct stat info {};
    if (::fstat(lock.value, &info))
      fail("inspect Unix socket ownership lock");
    if (!S_ISREG(info.st_mode) || info.st_uid != ::geteuid() ||
        (info.st_mode & 0077) || info.st_nlink != 1)
      throw std::runtime_error(
          "Unix socket ownership lock is not a private owned file");
    if (::flock(lock.value, LOCK_EX | LOCK_NB))
      fail("lock Unix socket ownership");
    if (::fstatat(dir.value, name.c_str(), &info, AT_SYMLINK_NOFOLLOW)) {
      if (errno != ENOENT)
        fail("inspect Unix socket path");
      return;
    }
    if (!S_ISSOCK(info.st_mode) || info.st_uid != ::geteuid())
      throw std::runtime_error("Unix socket path is not an owned socket");
    if (options.existing == ExistingPath::Fail)
      throw std::runtime_error("Unix socket path already exists");
    Fd probe(::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (probe.value < 0)
      fail("create Unix socket probe");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const auto anchored = bind_path();
    std::memcpy(address.sun_path, anchored.c_str(), anchored.size() + 1);
    int result = ::connect(probe.value, reinterpret_cast<sockaddr *>(&address),
                           sizeof(address));
    int error = result == 0 ? 0 : errno;
    if (error == EINPROGRESS || error == EAGAIN) {
      pollfd p{probe.value, POLLOUT, 0};
      result = ::poll(&p, 1, static_cast<int>(options.probe_timeout.count()));
      if (result <= 0)
        throw std::runtime_error(
            "Unix socket probe did not prove unreachability");
      socklen_t length = sizeof(error);
      if (::getsockopt(probe.value, SOL_SOCKET, SO_ERROR, &error, &length))
        fail("read Unix socket probe result");
    }
    if (error != ECONNREFUSED)
      throw std::runtime_error(
          "Unix socket is live or unreachability is unproven");
    struct stat current {};
    if (::fstatat(dir.value, name.c_str(), &current, AT_SYMLINK_NOFOLLOW) ||
        current.st_dev != info.st_dev || current.st_ino != info.st_ino ||
        !S_ISSOCK(current.st_mode))
      throw std::runtime_error("Unix socket changed during probe");
    if (::unlinkat(dir.value, name.c_str(), 0))
      fail("remove unreachable Unix socket");
  }
  ~Impl() { cleanup(); }
  std::string bind_path() const {
    const auto address = "/proc/self/fd/" + std::to_string(dir.value) + "/" + name;
    if (address.size() >= sizeof(sockaddr_un::sun_path))
      throw std::invalid_argument("Unix filename too long");
    return address;
  }
  void record() {
    struct stat info {};
    if (::fstatat(dir.value, name.c_str(), &info, AT_SYMLINK_NOFOLLOW))
      fail("inspect bound Unix socket");
    if (!S_ISSOCK(info.st_mode) || info.st_uid != ::geteuid())
      throw std::runtime_error("bound Unix socket ownership mismatch");
    if ((info.st_mode & 0777) != options.mode)
      throw std::runtime_error("external Unix socket mode was not protected by its owner");
    device = info.st_dev;
    inode = info.st_ino;
    owned = true;
  }
  void cleanup() noexcept {
    if (!owned)
      return;
    struct stat info {};
    if (::fstatat(dir.value, name.c_str(), &info, AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISSOCK(info.st_mode) && info.st_dev == device && info.st_ino == inode)
      ::unlinkat(dir.value, name.c_str(), 0);
    owned = false;
  }
  UnixOptions options;
  Fd dir, lock;
  std::string name;
  dev_t device{};
  ino_t inode{};
  bool owned = false;
  bool closed = false;
};
UnixPathLease::UnixPathLease(UnixOptions options)
    : impl_(new Impl(std::move(options))) {}
UnixPathLease::UnixPathLease(UnixOptions options, int retained_parent_fd) {
  if (retained_parent_fd < 0)
    throw std::invalid_argument("retained Unix socket parent descriptor required");
  impl_.reset(new Impl(std::move(options), retained_parent_fd));
}
UnixPathLease::~UnixPathLease() = default;
UnixPathLease::UnixPathLease(UnixPathLease &&) noexcept = default;
UnixPathLease &UnixPathLease::operator=(UnixPathLease &&) noexcept = default;
int UnixPathLease::bind_stream(int backlog) {
  if (!impl_ || impl_->closed || backlog < 1 || impl_->owned)
    throw std::invalid_argument("invalid or repeated Unix bind");
  Fd socket(::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (socket.value < 0)
    fail("create Unix listener");
  // Keep bind/record/chmod off the public name: another same-uid actor may
  // install a foreign node there after reservation. Publish only our recorded
  // private inode, atomically refusing any existing destination.
  Fd private_dir;
  std::string private_name;
  constexpr auto socket_name = "socket";
  struct stat directory_info {}, socket_info {};
  bool directory_created = false, directory_recorded = false;
  bool socket_recorded = false;
  const auto cleanup_private = [&]() noexcept {
    struct stat current {};
    if (private_dir.value >= 0 && socket_recorded &&
        ::fstatat(private_dir.value, socket_name, &current,
                  AT_SYMLINK_NOFOLLOW) == 0 && S_ISSOCK(current.st_mode) &&
        current.st_dev == socket_info.st_dev &&
        current.st_ino == socket_info.st_ino)
      ::unlinkat(private_dir.value, socket_name, 0);
    if (directory_created && directory_recorded &&
        ::fstatat(impl_->dir.value, private_name.c_str(), &current,
                  AT_SYMLINK_NOFOLLOW) == 0 && S_ISDIR(current.st_mode) &&
        current.st_dev == directory_info.st_dev &&
        current.st_ino == directory_info.st_ino)
      ::unlinkat(impl_->dir.value, private_name.c_str(), AT_REMOVEDIR);
  };
  try {
    for (unsigned attempt = 0; attempt < 16; ++attempt) {
      private_name = ".xrpc-bind-" + new_instance_id();
      if (::mkdirat(impl_->dir.value, private_name.c_str(), 0700) == 0) {
        directory_created = true;
        break;
      }
      if (errno != EEXIST)
        fail("create private Unix bind directory");
    }
    if (!directory_created)
      throw std::runtime_error("private Unix bind directory collision limit");
    if (::fstatat(impl_->dir.value, private_name.c_str(), &directory_info,
                  AT_SYMLINK_NOFOLLOW))
      fail("inspect private Unix bind directory");
    if (!S_ISDIR(directory_info.st_mode) ||
        directory_info.st_uid != ::geteuid())
      throw std::runtime_error("private Unix bind directory ownership mismatch");
    directory_recorded = true;
    private_dir.value = ::openat(impl_->dir.value, private_name.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (private_dir.value < 0)
      fail("open private Unix bind directory");
    struct stat opened {};
    if (::fstat(private_dir.value, &opened))
      fail("inspect retained private Unix bind directory");
    if (opened.st_dev != directory_info.st_dev ||
        opened.st_ino != directory_info.st_ino)
      throw std::runtime_error("private Unix bind directory changed");
    if (::fchmod(private_dir.value, 0700))
      fail("protect private Unix bind directory");
    const auto path = "/proc/self/fd/" + std::to_string(private_dir.value) +
                      "/" + socket_name;
    if (path.size() >= sizeof(sockaddr_un::sun_path))
      throw std::invalid_argument("private Unix bind address too long");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
    if (::bind(socket.value, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)))
      fail("bind private Unix listener");
    if (::fstatat(private_dir.value, socket_name, &socket_info,
                  AT_SYMLINK_NOFOLLOW))
      fail("inspect private Unix socket");
    if (!S_ISSOCK(socket_info.st_mode) || socket_info.st_uid != ::geteuid())
      throw std::runtime_error("private Unix socket ownership mismatch");
    socket_recorded = true;
    if (::fchmodat(private_dir.value, socket_name, impl_->options.mode, 0))
      fail("protect private Unix socket");
    if (::listen(socket.value, backlog))
      fail("listen Unix socket");
    if (::linkat(private_dir.value, socket_name, impl_->dir.value,
                 impl_->name.c_str(), 0))
      fail("publish Unix socket");
    impl_->device = socket_info.st_dev;
    impl_->inode = socket_info.st_ino;
    impl_->owned = true;
    if (::unlinkat(private_dir.value, socket_name, 0))
      fail("remove private Unix socket link");
    socket_recorded = false;
    struct stat current {};
    if (::fstatat(impl_->dir.value, private_name.c_str(), &current,
                  AT_SYMLINK_NOFOLLOW) || !S_ISDIR(current.st_mode) ||
        current.st_dev != directory_info.st_dev ||
        current.st_ino != directory_info.st_ino)
      throw std::runtime_error("private Unix bind directory changed during publication");
    if (::unlinkat(impl_->dir.value, private_name.c_str(), AT_REMOVEDIR))
      fail("remove private Unix bind directory");
    directory_created = false;
  } catch (...) {
    cleanup_private();
    impl_->cleanup();
    throw;
  }
  return socket.release();
}
std::string UnixPathLease::external_bind_path() const {
  if (!impl_ || impl_->closed)
    throw std::logic_error("Unix lease is closed");
  return impl_->bind_path();
}
void UnixPathLease::record_external_bind() {
  if (!impl_ || impl_->closed || impl_->owned)
    throw std::logic_error("socket already recorded");
  impl_->record();
}
void UnixPathLease::cleanup() noexcept {
  if (impl_ && !impl_->closed) {
    impl_->cleanup();
    impl_->closed = true;
    if (impl_->lock.value >= 0) {
      ::close(impl_->lock.value);
      impl_->lock.value = -1;
    }
  }
}
const std::string &UnixPathLease::path() const noexcept {
  return impl_->options.path;
}
} // namespace xrpc
} // namespace xgc2
