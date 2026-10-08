// Package unix owns one private Unix socket lifetime without CGO.
package unix

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	sys "golang.org/x/sys/unix"
)

type ExistingPathPolicy uint8

const (
	FailIfExists ExistingPathPolicy = iota
	ReclaimUnreachable
)

type Options struct {
	Mode         os.FileMode
	ExistingPath ExistingPathPolicy
	ProbeTimeout time.Duration
}

// Lease pins a private parent directory and reserves one lifetime lock before
// bind. External binders must use BindPath, then RecordBound. Path is the public
// endpoint. Close unlinks only the socket inode recorded by this lease.
type Lease struct {
	mu           sync.Mutex
	path, name   string
	parent, lock int
	bound        *sys.Stat_t
	listener     net.Listener
	closed       bool
	mode         os.FileMode
}

// openParent walks from / without following symlinks. Only the final runtime
// directory must be owned by this uid with mode0700; ancestors can be shared.
func openParent(path string) (int, error) {
	fd, err := sys.Open("/", sys.O_RDONLY|sys.O_DIRECTORY|sys.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(path), "/"), "/") {
		if part == "" {
			continue
		}
		next, openErr := sys.Openat(fd, part, sys.O_RDONLY|sys.O_DIRECTORY|sys.O_CLOEXEC|sys.O_NOFOLLOW, 0)
		if errors.Is(openErr, sys.ENOENT) {
			if mkdirErr := sys.Mkdirat(fd, part, 0700); mkdirErr != nil && !errors.Is(mkdirErr, sys.EEXIST) {
				sys.Close(fd)
				return -1, mkdirErr
			}
			next, openErr = sys.Openat(fd, part, sys.O_RDONLY|sys.O_DIRECTORY|sys.O_CLOEXEC|sys.O_NOFOLLOW, 0)
		}
		sys.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	var st sys.Stat_t
	if err = sys.Fstat(fd, &st); err != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0700 {
		sys.Close(fd)
		return -1, errors.New("xrpc: runtime directory must be owned mode0700 without symlink ancestors")
	}
	return fd, nil
}
func (l *Lease) stat() (*sys.Stat_t, error) {
	var st sys.Stat_t
	err := sys.Fstatat(l.parent, l.name, &st, sys.AT_SYMLINK_NOFOLLOW)
	return &st, err
}
func same(a, b *sys.Stat_t) bool { return a != nil && b != nil && a.Dev == b.Dev && a.Ino == b.Ino }
func Reserve(ctx context.Context, path string, options Options) (*Lease, error) {
	if ctx == nil {
		return nil, errors.New("xrpc: context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := (xrpc.Endpoint{Kind: "unix", Address: path}).Validate(); err != nil {
		return nil, err
	}
	if options.ExistingPath > ReclaimUnreachable {
		return nil, errors.New("xrpc: invalid existing path policy")
	}
	if options.Mode == 0 {
		options.Mode = 0600
	}
	if options.Mode != 0600 && options.Mode != 0660 {
		return nil, errors.New("xrpc: socket mode must be0600 or0660")
	}
	if options.ProbeTimeout <= 0 {
		options.ProbeTimeout = 250 * time.Millisecond
	}
	parent, err := openParent(path)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	lock, err := sys.Openat(parent, name+".xrpc.lock", sys.O_CREAT|sys.O_RDWR|sys.O_CLOEXEC|sys.O_NOFOLLOW, 0600)
	if err != nil {
		sys.Close(parent)
		return nil, fmt.Errorf("xrpc: open owner lock: %w", err)
	}
	cleanup := func() { sys.Close(lock); sys.Close(parent) }
	var st sys.Stat_t
	if err = sys.Fstat(lock, &st); err != nil || st.Mode&sys.S_IFMT != sys.S_IFREG || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0600 || st.Nlink != 1 {
		cleanup()
		return nil, errors.New("xrpc: owner lock must be private, owned, single-link regular file")
	}
	if err = sys.Flock(lock, sys.LOCK_EX|sys.LOCK_NB); err != nil {
		cleanup()
		return nil, fmt.Errorf("xrpc: endpoint already owned: %w", err)
	}
	l := &Lease{path: path, name: name, parent: parent, lock: lock, mode: options.Mode}
	previous, err := l.stat()
	if errors.Is(err, sys.ENOENT) {
		return l, nil
	}
	if err != nil {
		l.Close()
		return nil, err
	}
	if previous.Mode&sys.S_IFMT != sys.S_IFSOCK || options.ExistingPath == FailIfExists {
		l.Close()
		return nil, errors.New("xrpc: existing endpoint path rejected")
	}
	probe, cancel := context.WithTimeout(ctx, options.ProbeTimeout)
	connection, dialErr := (&net.Dialer{}).DialContext(probe, "unix", l.BindPath())
	cancel()
	if dialErr == nil {
		connection.Close()
		l.Close()
		return nil, errors.New("xrpc: existing endpoint is reachable")
	}
	if ctx.Err() != nil || !errors.Is(dialErr, sys.ECONNREFUSED) {
		l.Close()
		return nil, fmt.Errorf("xrpc: cannot prove stale endpoint: %w", dialErr)
	}
	current, err := l.stat()
	if err != nil || !same(previous, current) {
		l.Close()
		return nil, errors.New("xrpc: endpoint changed during reclaim")
	}
	if err = sys.Unlinkat(l.parent, l.name, 0); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}
func (l *Lease) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// BindPath stays in the pinned namespace even if an ancestor is renamed.
// The returned path is only valid while the lease is alive.
func (l *Lease) BindPath() string { return fmt.Sprintf("/proc/self/fd/%d/%s", l.parent, l.name) }
func (l *Lease) Listen() (net.Listener, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.listener != nil || l.bound != nil {
		return nil, errors.New("xrpc: lease unavailable for bind")
	}
	listener, err := net.Listen("unix", l.BindPath())
	if err != nil {
		return nil, err
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	l.listener = listener
	if err = l.recordBound(); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// ValidateListener checks the native socket's bound address and retained
// pathname inode against this still-live lease. It accepts Lease.Listen and
// native external binders that used BindPath followed by RecordBound.
func (l *Lease) ValidateListener(listener net.Listener) error {
	if l == nil || listener == nil {
		return errors.New("xrpc: matching endpoint lease required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.bound == nil {
		return errors.New("xrpc: bound endpoint lease required")
	}
	current, err := l.stat()
	if err != nil || !same(current, l.bound) {
		return errors.New("xrpc: leased endpoint was replaced")
	}
	connection, ok := listener.(syscall.Conn)
	if !ok {
		return errors.New("xrpc: native leased Unix listener required")
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	matched := false
	var socketError error
	err = raw.Control(func(fd uintptr) {
		address, lookupErr := sys.Getsockname(int(fd))
		socketError = lookupErr
		if address, ok := address.(*sys.SockaddrUnix); ok {
			matched = address.Name == l.BindPath()
		}
	})
	if err != nil || socketError != nil || !matched {
		return errors.New("xrpc: listener does not belong to endpoint lease")
	}
	return nil
}

func (l *Lease) recordBound() error {
	st, err := l.stat()
	if err != nil {
		return err
	}
	if st.Mode&sys.S_IFMT != sys.S_IFSOCK || st.Uid != uint32(os.Geteuid()) {
		return errors.New("xrpc: binder did not create an owned socket")
	}
	l.bound = st
	return sys.Fchmodat(l.parent, l.name, uint32(l.mode), sys.AT_SYMLINK_NOFOLLOW)
}
func (l *Lease) RecordBound() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.bound != nil {
		return errors.New("xrpc: lease unavailable")
	}
	return l.recordBound()
}
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var result error
	if l.listener != nil {
		err := l.listener.Close()
		if !errors.Is(err, net.ErrClosed) {
			result = errors.Join(result, err)
		}
	}
	if current, err := l.stat(); err == nil && same(current, l.bound) {
		result = errors.Join(result, sys.Unlinkat(l.parent, l.name, 0))
	}
	result = errors.Join(result, sys.Flock(l.lock, sys.LOCK_UN), sys.Close(l.lock), sys.Close(l.parent))
	return result
}
