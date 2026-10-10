package udpx

import (
	"testing"
	"time"
)

func key(n byte) cacheKey { return cacheKey{keyID: 1, id: RequestID{n}} }

func TestCacheClassifiesRetransmissions(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newReplyCache(120*time.Second, 8, 8)
	outcome, entry, _ := c.admit(key(1), now)
	if outcome != admitNew {
		t.Fatalf("first arrival: %v", outcome)
	}
	if outcome, _, _ = c.admit(key(1), now); outcome != admitPending {
		t.Fatalf("retransmission while running: %v", outcome)
	}
	c.complete(entry, []byte("reply"), now)
	if outcome, _, reply := c.admit(key(1), now); outcome != admitCached || string(reply) != "reply" {
		t.Fatalf("retransmission after reply: %v %q", outcome, reply)
	}
	other, entry2, _ := c.admit(key(2), now)
	if other != admitNew {
		t.Fatal("a different id must be new")
	}
	c.complete(entry2, nil, now)
	if outcome, _, reply := c.admit(key(2), now); outcome != admitAbandoned || reply != nil {
		t.Fatalf("retransmission after the deadline: %v", outcome)
	}
	if outcome, _, _ := c.admit(cacheKey{keyID: 2, id: RequestID{1}}, now); outcome != admitNew {
		t.Fatal("the same id under another key is another request")
	}
}

func TestCacheExpiresAfterTTL(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newReplyCache(120*time.Second, 8, 8)
	_, entry, _ := c.admit(key(1), now)
	c.complete(entry, []byte("reply"), now)
	if outcome, _, _ := c.admit(key(1), now.Add(119*time.Second)); outcome != admitCached {
		t.Fatalf("inside the ttl: %v", outcome)
	}
	// The ttl runs from completion, not from the last retransmission.
	if outcome, _, _ := c.admit(key(1), now.Add(120*time.Second)); outcome != admitNew {
		t.Fatalf("after the ttl the request is new again: %v", outcome)
	}
	if c.len() != 1 {
		t.Fatalf("expired entry kept: %d", c.len())
	}
}

func TestCachePendingEntriesAreNeverEvicted(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newReplyCache(time.Hour, 3, 3)
	var entries []*cacheEntry
	for i := byte(1); i <= 3; i++ {
		outcome, entry, _ := c.admit(key(i), now)
		if outcome != admitNew {
			t.Fatal(outcome)
		}
		entries = append(entries, entry)
	}
	if outcome, _, _ := c.admit(key(4), now); outcome != admitCacheFull {
		t.Fatalf("all pending and full: %v", outcome)
	}
	// Completing entries in order 2, 1 makes 2 the oldest evictable one.
	c.complete(entries[1], []byte("two"), now)
	c.complete(entries[0], []byte("one"), now.Add(time.Second))
	if outcome, _, _ := c.admit(key(4), now.Add(2*time.Second)); outcome != admitNew {
		t.Fatalf("room after completion: %v", outcome)
	}
	if outcome, _, _ := c.admit(key(2), now.Add(2*time.Second)); outcome != admitNew {
		t.Fatalf("oldest completed entry should have been evicted: %v", outcome)
	}
	if c.len() > 3 {
		t.Fatalf("capacity exceeded: %d", c.len())
	}
	if outcome, _, _ := c.admit(key(3), now.Add(2*time.Second)); outcome != admitPending {
		t.Fatalf("pending entry was evicted: %v", outcome)
	}
}

func TestCacheLimitsRunningRequestsSeparately(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newReplyCache(time.Hour, 10, 2)
	_, first, _ := c.admit(key(1), now)
	if outcome, _, _ := c.admit(key(2), now); outcome != admitNew {
		t.Fatal(outcome)
	}
	if outcome, _, _ := c.admit(key(3), now); outcome != admitCacheFull {
		t.Fatalf("a third running request: %v", outcome)
	}
	// Retransmissions of the running ones are still classified, not refused.
	if outcome, _, _ := c.admit(key(1), now); outcome != admitPending {
		t.Fatalf("retransmission at the limit: %v", outcome)
	}
	c.complete(first, []byte("done"), now)
	if outcome, _, _ := c.admit(key(3), now); outcome != admitNew {
		t.Fatalf("a slot was freed: %v", outcome)
	}
	if outcome, reply := c.peek(key(1), now); outcome != admitCached || string(reply) != "done" {
		t.Fatalf("peek: %v %q", outcome, reply)
	}
	if outcome, _ := c.peek(key(9), now); outcome != admitNew || c.len() != 3 {
		t.Fatal("peek must not record anything")
	}
}
