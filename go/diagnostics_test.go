package xrpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"golang.org/x/sys/unix"
)

type blockingSink struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	written bytes.Buffer
}

func TestFileDiagnosticRejectsFIFOAndSmallerArchiveGrant(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(directory, "fifo.log"), 0600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if sink, err := xrpc.NewRotatingFileSink(xrpc.FileSinkOptions{Directory: directory, Name: "fifo.log", MaxBytes: 256, Files: 1}); err == nil {
		sink.Close()
		t.Fatal("FIFO log accepted")
	}
	if time.Since(start) > time.Second {
		t.Fatal("FIFO constructor blocked")
	}
	if err := os.WriteFile(filepath.Join(directory, "small.log.1"), []byte("previous archive"), 0600); err != nil {
		t.Fatal(err)
	}
	if sink, err := xrpc.NewRotatingFileSink(xrpc.FileSinkOptions{Directory: directory, Name: "small.log", MaxBytes: 256, Files: 1}); err == nil {
		sink.Close()
		t.Fatal("old archive escaped smaller grant")
	}
}

func (w *blockingSink) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.written.Write(body)
}

func TestDiagnosticsSaturationAndRealWriterDrain(t *testing.T) {
	sink := &blockingSink{started: make(chan struct{}), release: make(chan struct{})}
	diagnostics, err := xrpc.NewDiagnostics(xrpc.DiagnosticOptions{Sink: sink, Level: "debug", MaxQueuedRecords: 2, MaxRecordBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", RequestID: "first"})
	<-sink.started
	start := time.Now()
	for i := 0; i < 1000; i++ {
		diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", RequestID: "saturation"})
	}
	if time.Since(start) > time.Second {
		t.Fatal("request path blocked on diagnostic sink")
	}
	status := diagnostics.Status()
	if status.QueuedRecords > 2 || status.Dropped < 998 || status.MaxRecordBytes != 512 {
		t.Fatalf("unbounded diagnostic queue %+v", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = diagnostics.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-diagnostics.Done():
		t.Fatal("reported real writer drain while blocked")
	default:
	}
	close(sink.release)
	finish, cancelFinish := context.WithTimeout(context.Background(), time.Second)
	defer cancelFinish()
	if err = diagnostics.Close(finish); err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(sink.written.Bytes()), []byte{'\n'}) {
		if len(line) > 512 || !json.Valid(line) {
			t.Fatalf("invalid/unbounded record %d", len(line))
		}
	}
}

func TestDiagnosticsLiveLevelAndFormat(t *testing.T) {
	var sink bytes.Buffer
	diagnostics, err := xrpc.NewDiagnostics(xrpc.DiagnosticOptions{Sink: &sink, Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "suppressed_debug"})
	if err = diagnostics.SetLevel("debug"); err != nil {
		t.Fatal(err)
	}
	diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "visible_debug"})
	if err = diagnostics.SetLevel("verbose"); err == nil {
		t.Fatal("unknown level accepted")
	}
	for _, bad := range []xrpc.DiagnosticOptions{{Sink: &sink, Level: "loud"}, {Sink: &sink, Format: "xml"}, {}} {
		if _, err = xrpc.NewDiagnostics(bad); err == nil {
			t.Fatalf("invalid options accepted: %+v", bad)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = diagnostics.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = diagnostics.SetLevel("trace"); err == nil {
		t.Fatal("closed owner accepted a level change")
	}
	if output := sink.String(); strings.Contains(output, "suppressed_debug") || !strings.Contains(output, "visible_debug") || json.Valid([]byte(output)) {
		t.Fatalf("verbosity/format not applied %q", output)
	}
	if status := diagnostics.Status(); status.Level != "debug" || status.Format != "text" {
		t.Fatalf("diagnostic settings lost %+v", status)
	}
}

type failingSink struct{}

func (failingSink) Write([]byte) (int, error) { return 0, syscall.ENOSPC }
func TestDiagnosticSinkFailureAndRepeatedEventRateLimit(t *testing.T) {
	diagnostics, err := xrpc.NewDiagnostics(xrpc.DiagnosticOptions{Sink: failingSink{}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		diagnostics.Emit(xrpc.Diagnostic{Level: "warn", Event: "admission_rejected", Category: "resource_exhausted"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = diagnostics.Close(ctx); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	status := diagnostics.Status()
	if status.Written != 0 || status.SinkErrors == 0 || status.SinkErrors > 8 || status.Dropped != 100 {
		t.Fatalf("sink/rate counts %+v", status)
	}
}

func TestFileDiagnosticGrantRotationAndNamespaceReplacement(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	sink, err := xrpc.NewRotatingFileSink(xrpc.FileSinkOptions{Directory: directory, Name: "xrpc.log", MaxBytes: 256, Files: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err = sink.Write([]byte(strings.Repeat("x", 200) + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = sink.Write([]byte(strings.Repeat("y", 257))); err == nil {
		t.Fatal("unbounded record accepted")
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatalf("rotation files=%d", len(files))
	}
	for _, file := range files {
		info, err := file.Info()
		if err != nil || info.Size() > 256 || info.Mode().Perm() != 0600 {
			t.Fatalf("unsafe archive %+v %v", info, err)
		}
	}
	if other, err := xrpc.NewRotatingFileSink(xrpc.FileSinkOptions{Directory: directory, Name: "xrpc.log", MaxBytes: 256, Files: 3}); err == nil {
		other.Close()
		t.Fatal("second independent writer accepted")
	}
	active := filepath.Join(directory, "xrpc.log")
	if err = os.Rename(active, filepath.Join(directory, "old-owner")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(active, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = sink.Write([]byte("late-owner")); err == nil {
		t.Fatal("writer followed replacement inode")
	}
	sink.Close()
	if body, err := os.ReadFile(active); err != nil || string(body) != "replacement" {
		t.Fatal("replacement changed", err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err = os.Symlink(directory, alias); err != nil {
		t.Fatal(err)
	}
	if rejected, err := xrpc.NewRotatingFileSink(xrpc.FileSinkOptions{Directory: alias, Name: "log", MaxBytes: 256, Files: 1}); err == nil {
		rejected.Close()
		t.Fatal("symlink grant accepted")
	}
}
