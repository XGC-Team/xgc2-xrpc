//! Linux dirfd-anchored lifetime ownership, shared by HTTP and native gRPC binders.
use socket2::{Domain, SockAddr, Socket, Type};
use std::{
    ffi::{CString, OsString},
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::MetadataExt,
            net::UnixListener,
        },
    },
    path::{Path, PathBuf},
};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn open_at(parent: i32, name: &CString, flags: i32, mode: u32) -> io::Result<File> {
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}
fn stat_at(parent: &File, name: &CString) -> io::Result<libc::stat> {
    let mut value = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            value.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { value.assume_init() })
    }
}
fn unlink_at(parent: &File, name: &CString) -> io::Result<()> {
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn socket_identity(stat: &libc::stat) -> Option<(u64, u64)> {
    (stat.st_mode & libc::S_IFMT == libc::S_IFSOCK)
        .then_some((stat.st_dev as u64, stat.st_ino as u64))
}

pub struct UnixLease {
    parent: File,
    name: CString,
    _lock: File,
    identity: Option<(u64, u64)>,
}
impl UnixLease {
    pub fn reserve(path: &Path, reclaim_unreachable: bool) -> io::Result<Self> {
        let bytes = path.as_os_str().as_bytes();
        if !path.is_absolute()
            || bytes.len() > 107
            || bytes[1..]
                .split(|b| *b == b'/')
                .any(|p| p.is_empty() || p == b"." || p == b"..")
        {
            return Err(invalid(
                "canonical absolute Unix path within 107 bytes required",
            ));
        }
        let components: Vec<_> = bytes[1..].split(|b| *b == b'/').collect();
        let mut parent = open_at(
            libc::AT_FDCWD,
            &CString::new("/").unwrap(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        for component in &components[..components.len() - 1] {
            parent = open_at(
                parent.as_raw_fd(),
                &CString::new(*component).map_err(|_| invalid("NUL in endpoint"))?,
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
        }
        let metadata = parent.metadata()?;
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o7777 != 0o700 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "runtime directory must be owned by effective user and mode 0700",
            ));
        }
        let name =
            CString::new(*components.last().unwrap()).map_err(|_| invalid("NUL in endpoint"))?;
        let mut lock_name = name.as_bytes().to_vec();
        lock_name.extend_from_slice(b".xrpc.lock");
        let lock = open_at(
            parent.as_raw_fd(),
            &CString::new(lock_name).unwrap(),
            libc::O_CREAT | libc::O_RDWR,
            0o600,
        )?;
        let metadata = lock.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o7777 != 0o600
            || metadata.nlink() != 1
        {
            return Err(invalid(
                "lease lock must be owned regular 0600 file with one link",
            ));
        }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let lease = Self {
            parent,
            name,
            _lock: lock,
            identity: None,
        };
        match stat_at(&lease.parent, &lease.name) {
            Ok(prior) => {
                if !reclaim_unreachable || socket_identity(&prior).is_none() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "endpoint exists",
                    ));
                }
                let probe = Socket::new(Domain::UNIX, Type::STREAM, None)?;
                probe.set_nonblocking(true)?;
                match probe.connect(&SockAddr::unix(lease.bind_address())?) {
                    Err(error) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {}
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            "endpoint reachable or busy",
                        ))
                    }
                }
                let current = stat_at(&lease.parent, &lease.name)?;
                if socket_identity(&current) != socket_identity(&prior) {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "endpoint changed",
                    ));
                }
                unlink_at(&lease.parent, &lease.name)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(lease)
    }
    /// External native binders use this path while retaining the lease.
    pub fn bind_address(&self) -> PathBuf {
        let mut path = PathBuf::from(format!("/proc/self/fd/{}", self.parent.as_raw_fd()));
        path.push(OsString::from_vec(self.name.as_bytes().to_vec()));
        path
    }
    pub fn record_bound(&mut self, mode: u32) -> io::Result<()> {
        self.identity = Some(
            socket_identity(&stat_at(&self.parent, &self.name)?)
                .ok_or_else(|| invalid("bound endpoint is not socket"))?,
        );
        if unsafe { libc::fchmodat(self.parent.as_raw_fd(), self.name.as_ptr(), mode, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub fn bind(
        path: &Path,
        reclaim_unreachable: bool,
        mode: u32,
    ) -> io::Result<(Self, UnixListener)> {
        let mut lease = Self::reserve(path, reclaim_unreachable)?;
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.bind(&SockAddr::unix(lease.bind_address())?)?;
        lease.record_bound(mode)?;
        socket.listen(128)?;
        socket.set_nonblocking(true)?;
        Ok((lease, socket.into()))
    }
}
impl Drop for UnixLease {
    fn drop(&mut self) {
        if let (Some(owned), Ok(current)) = (self.identity, stat_at(&self.parent, &self.name)) {
            if socket_identity(&current) == Some(owned) {
                let _ = unlink_at(&self.parent, &self.name);
            }
        }
        // The permanent lock inode prevents competing generations from locking
        // different files. File closes release flock only after owned cleanup.
    }
}
