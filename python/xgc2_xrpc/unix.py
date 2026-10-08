"""Exclusive Unix endpoint lifetime, shared by HTTP and external gRPC binders."""

import errno
import fcntl
import math
import os
import socket
import stat


class UnixLease:
    def __init__(self, path, *, mode=0o600, reclaim_unreachable=False, probe_timeout=0.2):
        if isinstance(probe_timeout,bool) or not isinstance(probe_timeout,(int,float)) or not math.isfinite(probe_timeout) or probe_timeout<=0:
            raise ValueError("finite positive endpoint probe timeout required")
        if type(mode) is not int or mode<0 or mode>0o777:
            raise ValueError("socket mode must contain permission bits only")
        if not os.path.isabs(path) or os.path.normpath(path) != path or len(os.fsencode(path)) > 107:
            raise ValueError("Unix endpoint must be an absolute path within 107 bytes")
        parent, self.name = os.path.split(path)
        if not self.name or self.name in (".", ".."):
            raise ValueError("Unix endpoint must name a socket")
        self.path, self.mode = path, mode
        self.parent = self.lock = None
        self.identity = None
        self.socket = None
        try:
            self.parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            for component in parent.split("/"):
                if not component:
                    continue
                next_parent = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.parent)
                os.close(self.parent)
                self.parent = next_parent
            directory = os.fstat(self.parent)
            if directory.st_uid != os.geteuid() or stat.S_IMODE(directory.st_mode) != 0o700:
                raise PermissionError("runtime directory must be owned by effective user and mode 0700")
            self.lock = os.open(self.name + ".xrpc.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
                                0o600, dir_fd=self.parent)
            lock = os.fstat(self.lock)
            if not stat.S_ISREG(lock.st_mode) or lock.st_uid != os.geteuid() or lock.st_nlink != 1 or stat.S_IMODE(lock.st_mode) != 0o600:
                raise FileExistsError("endpoint lock must be owned regular 0600 file with one link")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise FileExistsError("Unix endpoint already has an owner") from error
            try:
                prior = os.stat(self.name, dir_fd=self.parent, follow_symlinks=False)
            except FileNotFoundError:
                prior = None
            if prior is not None:
                if not reclaim_unreachable or not stat.S_ISSOCK(prior.st_mode):
                    raise FileExistsError("Unix endpoint path exists")
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                    probe.settimeout(probe_timeout)
                    try:
                        probe.connect(self.bind_address)
                    except OSError as error:
                        if error.errno != errno.ECONNREFUSED:
                            raise FileExistsError("existing endpoint is not provably unreachable") from error
                    else:
                        raise FileExistsError("existing endpoint is listening")
                current = os.stat(self.name, dir_fd=self.parent, follow_symlinks=False)
                if (prior.st_dev, prior.st_ino) != (current.st_dev, current.st_ino):
                    raise FileExistsError("Unix endpoint changed during probe")
                os.unlink(self.name, dir_fd=self.parent)
        except BaseException:
            self.close()
            raise

    @property
    def bind_address(self):
        return "/proc/self/fd/%d/%s" % (self.parent, self.name)

    def record_bound(self):
        current = os.stat(self.name, dir_fd=self.parent, follow_symlinks=False)
        if not stat.S_ISSOCK(current.st_mode):
            raise RuntimeError("bound endpoint is not a socket")
        self.identity = (current.st_dev, current.st_ino)
        os.chmod(self.name, self.mode, dir_fd=self.parent, follow_symlinks=False)

    def bind(self, backlog=16):
        try:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.bind(self.bind_address)
            self.record_bound()
            # Permission is established before accepting any connections.
            self.socket.listen(backlog)
            return self.socket
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None
        if self.parent is not None and self.identity is not None:
            try:
                current = os.stat(self.name, dir_fd=self.parent, follow_symlinks=False)
                if stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == self.identity:
                    os.unlink(self.name, dir_fd=self.parent)
            except FileNotFoundError:
                pass
            self.identity = None
        if self.lock is not None:
            os.close(self.lock)
            self.lock = None
        if self.parent is not None:
            os.close(self.parent)
            self.parent = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
