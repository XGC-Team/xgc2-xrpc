package unix

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestOwnershipAndReplacement(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "rpc.sock")
	lease, err := Reserve(context.Background(), path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Reserve(context.Background(), path, Options{ExistingPath: ReclaimUnreachable}); err == nil {
		t.Fatal("second owner accepted")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("lease deleted replacement", err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestExplicitStaleReclaimAndNonSocket(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "rpc.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = listener.Close()
	if _, err = Reserve(context.Background(), path, Options{}); err == nil {
		t.Fatal("default reclaimed")
	}
	lease, err := Reserve(context.Background(), path, Options{ExistingPath: ReclaimUnreachable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lease.Listen(); err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	if err = os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Reserve(context.Background(), path, Options{ExistingPath: ReclaimUnreachable}); err == nil {
		t.Fatal("regular file accepted")
	}
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestPinnedParentSurvivesNamespaceReplacement(t *testing.T) {
	root := privateTempDir(t)
	parent := filepath.Join(root, "owner")
	path := filepath.Join(parent, "rpc.sock")
	lease, err := Reserve(context.Background(), path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	moved := filepath.Join(root, "moved")
	if err = os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(parent, "rpc.sock")
	if err = os.WriteFile(foreign, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = lease.Listen(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(foreign); err != nil || string(data) != "foreign" {
		t.Fatalf("bind escaped pinned namespace: %s %v", data, err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(filepath.Join(moved, "rpc.sock")); !os.IsNotExist(err) {
		t.Fatalf("owned socket not removed: %v", err)
	}
	if _, err = os.Stat(foreign); err != nil {
		t.Fatal("cleanup touched foreign namespace", err)
	}
}
func TestRejectSharedRuntimeAndLinkedLock(t *testing.T) {
	root := privateTempDir(t)
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "rpc.sock")
	if l, e := Reserve(context.Background(), path, Options{}); e == nil {
		l.Close()
		t.Fatal("shared directory accepted")
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	lock := path + ".xrpc.lock"
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(lock, filepath.Join(root, "other")); err != nil {
		t.Fatal(err)
	}
	if l, e := Reserve(context.Background(), path, Options{}); e == nil {
		l.Close()
		t.Fatal("hardlinked lock accepted")
	}
}
