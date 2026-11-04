package unix

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"unsafe"

	sys "golang.org/x/sys/unix"
)

func TestLeaseSocketModes(t *testing.T) {
	testLeaseSocketModes(t)
}

func testLeaseSocketModes(t *testing.T) {
	t.Helper()
	for _, external := range []bool{false, true} {
		for _, mode := range []os.FileMode{0600, 0660} {
			t.Run(fmt.Sprintf("external=%t/mode=%o", external, mode), func(t *testing.T) {
				path := filepath.Join(privateTempDir(t), "rpc.sock")
				lease, err := Reserve(context.Background(), path, Options{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Close()
				var listener net.Listener
				if external {
					listener, err = net.Listen("unix", lease.BindPath())
					if err != nil {
						t.Fatal(err)
					}
					listener.(*net.UnixListener).SetUnlinkOnClose(false)
					defer listener.Close()
					if err = os.Chmod(path, 0777); err != nil {
						t.Fatal(err)
					}
					err = lease.RecordBound()
				} else {
					listener, err = lease.Listen()
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = lease.ValidateListener(listener); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(path)
				if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != mode {
					t.Fatalf("socket permissions: info=%v err=%v, want %o", info, err, mode)
				}
				if err = lease.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err = os.Lstat(path); !os.IsNotExist(err) {
					t.Fatalf("owned socket not removed: %v", err)
				}
				lock, err := os.Stat(path + ".xrpc.lock")
				if err != nil || lock.Mode().Perm() != 0600 {
					t.Fatalf("owner lock not preserved: info=%v err=%v", lock, err)
				}
			})
		}
	}
}

// Run with fchmodat2 returning ENOSYS, as on supported older robot kernels.
// Restrict only a subprocess; TSYNC includes Go's syscall worker threads.
func TestLeaseWithoutFchmodat2(t *testing.T) {
	if os.Getenv("XRPC_TEST_NO_FCHMODAT2") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLeaseWithoutFchmodat2$", "-test.v")
		cmd.Env = append(os.Environ(), "XRPC_TEST_NO_FCHMODAT2=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("old-kernel subprocess: %v\n%s", err, out)
		}
		return
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	filter := []sys.SockFilter{
		{Code: sys.BPF_LD | sys.BPF_W | sys.BPF_ABS, K: 0}, // seccomp_data.nr
		{Code: sys.BPF_JMP | sys.BPF_JEQ | sys.BPF_K, Jf: 1, K: sys.SYS_FCHMODAT2},
		{Code: sys.BPF_RET | sys.BPF_K, K: sys.SECCOMP_RET_ERRNO | uint32(sys.ENOSYS)},
		{Code: sys.BPF_RET | sys.BPF_K, K: sys.SECCOMP_RET_ALLOW},
	}
	program := sys.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := sys.Prctl(sys.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	result, _, errno := sys.Syscall(sys.SYS_SECCOMP, sys.SECCOMP_SET_MODE_FILTER, sys.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&program)))
	runtime.KeepAlive(filter)
	if errno != 0 || result != 0 {
		t.Fatalf("install old-kernel syscall filter: result=%d errno=%v", result, errno)
	}
	if err := sys.Fchmodat(sys.AT_FDCWD, filepath.Join(privateTempDir(t), "missing"), 0600, sys.AT_SYMLINK_NOFOLLOW); !errors.Is(err, sys.EOPNOTSUPP) {
		t.Fatalf("fchmodat2 unavailability not reproduced: %v", err)
	}
	testLeaseSocketModes(t)
}

func TestRecordBoundRejectsSymlink(t *testing.T) {
	parent := privateTempDir(t)
	path := filepath.Join(parent, "rpc.sock")
	lease, err := Reserve(context.Background(), path, Options{Mode: 0660})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	target := filepath.Join(parent, "target.sock")
	listener, err := net.Listen("unix", target)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = os.Chmod(target, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if err = lease.RecordBound(); err == nil {
		t.Fatal("symlink accepted as bound socket")
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(target); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("symlink target permissions changed: info=%v err=%v", info, err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("unowned symlink removed: info=%v err=%v", info, err)
	}
}
