package udpx

import (
	"container/list"
	"sync"
	"time"
)

// cacheKey identifies a request: the same client key and the same 128-bit id.
type cacheKey struct {
	keyID uint32
	id    RequestID
}

type entryState uint8

const (
	entryPending   entryState = iota // the handler has not answered yet
	entryReplied                     // the reply datagram is cached
	entryAbandoned                   // the deadline passed; no reply exists
)

type cacheEntry struct {
	key     cacheKey
	state   entryState
	reply   []byte
	expires time.Time
}

type admission uint8

const (
	admitNew       admission = iota // not seen: the caller must run the request
	admitCached                     // seen and answered: resend the cached reply
	admitPending                    // seen and still running: ignore
	admitAbandoned                  // seen and unanswered: stay silent
	admitCacheFull                  // not seen, but no entry can be evicted
)

// replyCache remembers every executed request so that a retransmission is
// answered from the cache instead of running the handler again. Entries are
// kept for ttl after completion; pending entries are never evicted, so the
// handler count of one request id stays at most one.
type replyCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	entries  map[cacheKey]*cacheEntry
	done     *list.List // completed entries, oldest completion first
}

func newReplyCache(ttl time.Duration, capacity int) *replyCache {
	return &replyCache{ttl: ttl, capacity: capacity, entries: make(map[cacheKey]*cacheEntry), done: list.New()}
}

func (c *replyCache) purge(now time.Time) {
	for front := c.done.Front(); front != nil; front = c.done.Front() {
		entry := front.Value.(*cacheEntry)
		if entry.expires.After(now) {
			return
		}
		c.done.Remove(front)
		delete(c.entries, entry.key)
	}
}

// admit classifies a request. For admitNew it registers a pending entry and
// returns it; for admitCached it returns the cached datagram.
func (c *replyCache) admit(key cacheKey, now time.Time) (admission, *cacheEntry, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purge(now)
	if entry, ok := c.entries[key]; ok {
		switch entry.state {
		case entryReplied:
			return admitCached, entry, entry.reply
		case entryAbandoned:
			return admitAbandoned, entry, nil
		default:
			return admitPending, entry, nil
		}
	}
	if len(c.entries) >= c.capacity {
		oldest := c.done.Front()
		if oldest == nil {
			return admitCacheFull, nil, nil
		}
		evicted := c.done.Remove(oldest).(*cacheEntry)
		delete(c.entries, evicted.key)
	}
	entry := &cacheEntry{key: key}
	c.entries[key] = entry
	return admitNew, entry, nil
}

// complete records the outcome of a pending entry: the reply datagram, or nil
// when the deadline passed without one.
func (c *replyCache) complete(entry *cacheEntry, reply []byte, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reply == nil {
		entry.state = entryAbandoned
	} else {
		entry.state, entry.reply = entryReplied, reply
	}
	entry.expires = now.Add(c.ttl)
	c.done.PushBack(entry)
}

func (c *replyCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
