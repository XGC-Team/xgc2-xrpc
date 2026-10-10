package udpx

import (
	"net/netip"
	"testing"
	"time"
)

func TestLimiterBurstAndRefill(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(50, 100)
	source := netip.MustParseAddr("192.168.1.20")
	granted := 0
	for i := 0; i < 150; i++ {
		if l.allow(source, now) {
			granted++
		}
	}
	if granted != 100 {
		t.Fatalf("burst granted %d, want 100", granted)
	}
	// 50 tokens per second: 100 ms refills 5.
	granted = 0
	for i := 0; i < 20; i++ {
		if l.allow(source, now.Add(100*time.Millisecond)) {
			granted++
		}
	}
	if granted != 5 {
		t.Fatalf("refill granted %d, want 5", granted)
	}
	// A long pause refills the bucket to the burst, never beyond it.
	granted = 0
	for i := 0; i < 150; i++ {
		if l.allow(source, now.Add(time.Hour)) {
			granted++
		}
	}
	if granted != 100 {
		t.Fatalf("capped refill granted %d, want 100", granted)
	}
}

func TestLimiterSourcesAreIndependent(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(1, 2)
	a, b := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2")
	if !l.allow(a, now) || !l.allow(a, now) || l.allow(a, now) {
		t.Fatal("source a should get exactly its burst of 2")
	}
	if !l.allow(b, now) {
		t.Fatal("source b was charged for source a")
	}
}

func TestLimiterTableIsBounded(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(1, 2)
	for i := 0; i < maxSources; i++ {
		var raw [4]byte
		raw[0], raw[1], raw[2], raw[3] = 10, byte(i>>16), byte(i>>8), byte(i)
		if !l.allow(netip.AddrFrom4(raw), now) {
			t.Fatalf("source %d refused before the table was full", i)
		}
	}
	extra := netip.MustParseAddr("172.16.0.1")
	if l.allow(extra, now) {
		t.Fatal("a source beyond the table bound was admitted while every bucket was in use")
	}
	// Once the old buckets have refilled they are forgotten and the newcomer fits.
	if !l.allow(extra, now.Add(10*time.Second)) {
		t.Fatal("full buckets were not swept")
	}
	if len(l.sources) >= maxSources {
		t.Fatalf("table not swept: %d", len(l.sources))
	}
}
