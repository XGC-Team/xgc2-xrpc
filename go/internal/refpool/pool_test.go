package refpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestActiveCapacityAndIdleRetirement(t *testing.T) {
	var closed atomic.Int32
	p := New[string, int](2, 20*time.Millisecond, func(int) { closed.Add(1) })
	defer p.Close()
	_, one, e := p.Acquire("one", func() (int, error) { return 1, nil })
	if e != nil {
		t.Fatal(e)
	}
	_, two, e := p.Acquire("two", func() (int, error) { return 2, nil })
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = p.Acquire("three", func() (int, error) { return 3, nil }); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	one()
	two()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for closed.Load() != 2 {
		select {
		case <-deadline.C:
			t.Fatal("idle values retained without subsequent calls")
		case <-ticker.C:
		}
	}
	p.Close()
	if _, _, e = p.Acquire("four", func() (int, error) { return 4, nil }); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}

func TestNoncooperativeFactoryKeepsBoundedOwnershipAndCallerDeadline(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	p := New[string, int](2, time.Hour, func(int) { closed.Add(1) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := p.AcquireContext(ctx, "slow", func() (int, error) { close(started); <-release; return 1, nil })
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("factory blocked caller beyond deadline")
	}
	if refs, _, capacity := p.Snapshot(); refs != 1 || capacity != 2 {
		t.Fatalf("factory ownership=%d capacity=%d", refs, capacity)
	}
	second, cancelSecond := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSecond()
	if _, _, err := p.AcquireContext(second, "queued", func() (int, error) { t.Error("cancelled queued factory ran"); return 2, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	p.Close()
	select {
	case <-p.Drained():
		t.Fatal("uncooperative factory falsely drained")
	default:
	}
	close(release)
	select {
	case <-p.Drained():
	case <-time.After(time.Second):
		t.Fatal("factory did not really drain")
	}
	if closed.Load() != 1 {
		t.Fatalf("late-created resource not closed exactly once: %d", closed.Load())
	}
}
func TestChurnKeepsLiveResourcesBounded(t *testing.T) {
	var live, peak atomic.Int32
	p := New[int, int](4, time.Hour, func(int) { live.Add(-1) })
	for i := 0; i < 10000; i++ {
		_, release, err := p.Acquire(i, func() (int, error) {
			n := live.Add(1)
			for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
			}
			return i, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if peak.Load() > 4 {
		t.Fatalf("resource cap violated during replacement: %d", peak.Load())
	}
	p.Close()
	<-p.Drained()
	if live.Load() != 0 {
		t.Fatal("close leaked owned resources")
	}
}
