// Package netlimit combines the maintained netutil listener admission primitive
// with ownership tracking. Wrap it below TLS so native protocol engines retain
// their transport type and authenticated connection state.
package netlimit

import (
	"golang.org/x/net/netutil"
	"net"
	"sync"
)

type Listener struct {
	net.Listener
	mu          sync.Mutex
	connections map[*connection]struct{}
	closed      bool
}

func New(listener net.Listener, maximum int) *Listener {
	if maximum <= 0 {
		maximum = 128
	}
	return &Listener{Listener: netutil.LimitListener(listener, maximum), connections: make(map[*connection]struct{}, maximum)}
}
func (l *Listener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c := &connection{Conn: raw, owner: l}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		raw.Close()
		return nil, net.ErrClosed
	}
	l.connections[c] = struct{}{}
	return c, nil
}
func (l *Listener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return l.Listener.Close()
}
func (l *Listener) CloseConnections() {
	l.mu.Lock()
	values := make([]*connection, 0, len(l.connections))
	for c := range l.connections {
		values = append(values, c)
	}
	l.mu.Unlock()
	for _, c := range values {
		c.Close()
	}
}
func (l *Listener) Count() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.connections) }

type connection struct {
	net.Conn
	owner *Listener
	once  sync.Once
}

func (c *connection) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.owner.mu.Lock(); delete(c.owner.connections, c); c.owner.mu.Unlock() })
	return err
}
