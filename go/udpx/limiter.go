package udpx

import (
	"net/netip"
	"sync"
	"time"
)

// maxSources bounds the token-bucket table. Only authenticated datagrams
// create entries, so the table grows with key holders, not with attackers.
const maxSources = 4096

// limiter is a token bucket per source address: rate tokens per second up to
// burst. A datagram that finds the bucket empty is dropped.
type limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	sources map[netip.Addr]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(rate float64, burst int) *limiter {
	return &limiter{rate: rate, burst: float64(burst), sources: make(map[netip.Addr]*bucket)}
}

// allow takes one token from source's bucket.
func (l *limiter) allow(source netip.Addr, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.sources[source]
	if !ok {
		if len(l.sources) >= maxSources {
			l.sweep(now)
			if len(l.sources) >= maxSources {
				return false
			}
		}
		b = &bucket{tokens: l.burst, last: now}
		l.sources[source] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweep forgets sources whose bucket has refilled: they are indistinguishable
// from sources never seen.
func (l *limiter) sweep(now time.Time) {
	for source, b := range l.sources {
		if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
			delete(l.sources, source)
		}
	}
}
