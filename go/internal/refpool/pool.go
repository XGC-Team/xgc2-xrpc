// Package refpool owns a finite reference table, one factory/close worker and
// one idle-timer owner. Foreign callbacks never run while holding the table
// mutex. Cancelled callers stop waiting; noncooperative factories retain their
// reserved slot and real ownership until they actually return.
package refpool

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrFull = errors.New("xrpc: all reference pool slots are active")
var ErrClosed = errors.New("xrpc: reference pool closed")

type entry[K comparable, V any] struct {
	key                                     K
	value                                   V
	active                                  int
	idle                                    time.Time
	creating, working, closing, valueClosed bool
	ready                                   chan struct{}
	err                                     error
	create                                  func() (V, error)
	previous                                V
	hasPrevious                             bool
}
type Pool[K comparable, V any] struct {
	mu                       sync.Mutex
	entries                  []*entry[K, V]
	ttl                      time.Duration
	closeValue               func(V)
	wake, timerWake          chan struct{}
	stop, drained, timerDone chan struct{}
	closed                   bool
}

func New[K comparable, V any](capacity int, idle time.Duration, closeValue func(V)) *Pool[K, V] {
	if capacity <= 0 {
		capacity = 64
	}
	if idle <= 0 {
		idle = 30 * time.Second
	}
	p := &Pool[K, V]{entries: make([]*entry[K, V], capacity), ttl: idle, closeValue: closeValue, wake: make(chan struct{}, 1), timerWake: make(chan struct{}, 1), stop: make(chan struct{}), drained: make(chan struct{}), timerDone: make(chan struct{})}
	go p.work()
	go p.expire()
	return p
}
func (p *Pool[K, V]) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
	select {
	case p.timerWake <- struct{}{}:
	default:
	}
}

func (p *Pool[K, V]) Acquire(key K, create func() (V, error)) (V, func(), error) {
	return p.AcquireContext(context.Background(), key, create)
}
func (p *Pool[K, V]) AcquireContext(ctx context.Context, key K, create func() (V, error)) (V, func(), error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return zero, nil, ErrClosed
	}
	slot := -1
	var chosen *entry[K, V]
	for i, e := range p.entries {
		if e != nil && e.key == key && !e.closing {
			chosen = e
			break
		}
		if e == nil {
			slot = i
			continue
		}
		if e.active == 0 && !e.creating && !e.working && !e.closing && (slot < 0 || (p.entries[slot] != nil && e.idle.Before(p.entries[slot].idle))) {
			slot = i
		}
	}
	if chosen == nil {
		if slot < 0 {
			p.mu.Unlock()
			return zero, nil, ErrFull
		}
		chosen = &entry[K, V]{key: key, creating: true, ready: make(chan struct{}), create: create}
		if old := p.entries[slot]; old != nil {
			chosen.previous = old.value
			chosen.hasPrevious = true
		}
		p.entries[slot] = chosen
	}
	chosen.active++
	release := p.release(chosen)
	p.signal()
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		release()
		return zero, nil, ctx.Err()
	case <-p.stop:
		release()
		return zero, nil, ErrClosed
	case <-chosen.ready:
	}
	p.mu.Lock()
	value, err := chosen.value, chosen.err
	closed := p.closed
	p.mu.Unlock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && closed {
		err = ErrClosed
	}
	if err != nil {
		release()
		return zero, nil, err
	}
	return value, release, nil
}
func (p *Pool[K, V]) release(e *entry[K, V]) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			e.active--
			if e.active == 0 {
				e.idle = time.Now()
				if e.err != nil && !e.creating || e.valueClosed {
					p.remove(e)
				}
			}
			p.signal()
			p.mu.Unlock()
		})
	}
}
func (p *Pool[K, V]) remove(e *entry[K, V]) {
	for i, current := range p.entries {
		if current == e {
			p.entries[i] = nil
			return
		}
	}
}

func (p *Pool[K, V]) work() {
	defer close(p.drained)
	for {
		p.mu.Lock()
		var selected *entry[K, V]
		any := false
		for _, e := range p.entries {
			if e == nil {
				continue
			}
			any = true
			if e.working {
				continue
			}
			if e.creating || e.closing && !e.valueClosed {
				selected = e
				e.working = true
				break
			}
		}
		if p.closed && !any {
			p.mu.Unlock()
			<-p.timerDone
			return
		}
		if selected == nil {
			p.mu.Unlock()
			<-p.wake
			continue
		}
		e := selected
		creating := e.creating
		if creating {
			previous, hasPrevious := e.previous, e.hasPrevious
			e.hasPrevious = false
			create := e.create
			shouldCreate := !p.closed && e.active > 0
			p.mu.Unlock()
			if hasPrevious {
				p.closeValue(previous)
			}
			var value V
			var err error
			if shouldCreate {
				value, err = create()
			} else {
				err = context.Canceled
			}
			p.mu.Lock()
			e.value = value
			e.err = err
			e.create = nil
			e.creating = false
			e.working = false
			e.idle = time.Now()
			if err != nil {
				e.valueClosed = true
			} else if p.closed {
				e.closing = true
			}
			close(e.ready)
			if e.active == 0 && e.valueClosed {
				p.remove(e)
			}
			p.signal()
			p.mu.Unlock()
			continue
		}
		value := e.value
		p.mu.Unlock()
		p.closeValue(value)
		p.mu.Lock()
		e.valueClosed = true
		e.working = false
		if e.active == 0 {
			p.remove(e)
		}
		p.signal()
		p.mu.Unlock()
	}
}

func (p *Pool[K, V]) expire() {
	defer close(p.timerDone)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		now := time.Now()
		var due time.Time
		for _, e := range p.entries {
			if e == nil || e.active != 0 || e.creating || e.closing {
				continue
			}
			at := e.idle.Add(p.ttl)
			if !now.Before(at) {
				e.closing = true
				select {
				case p.wake <- struct{}{}:
				default:
				}
				continue
			}
			if due.IsZero() || at.Before(due) {
				due = at
			}
		}
		p.mu.Unlock()
		var tick <-chan time.Time
		if !due.IsZero() {
			timer.Reset(time.Until(due))
			tick = timer.C
		}
		select {
		case <-p.stop:
			return
		case <-p.timerWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-tick:
		}
	}
}
func (p *Pool[K, V]) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.stop)
		for _, e := range p.entries {
			if e != nil && !e.creating {
				e.closing = true
			}
		}
		p.signal()
	}
	p.mu.Unlock()
}
func (p *Pool[K, V]) Drained() <-chan struct{} { return p.drained }
func (p *Pool[K, V]) Snapshot() (references, active, capacity int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.entries {
		if e != nil {
			references++
			if e.active > 0 {
				active++
			}
		}
	}
	return references, active, len(p.entries)
}
