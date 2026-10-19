package netlimit

import (
	"errors"
	"net"
	"sync"
	"testing"
)

// Closing the inner limitListenerConn releases its admission permit. At that
// point a replacement connection may already be accepted and counted.
func TestConnectionUntrackedBeforeAdmissionRelease(t *testing.T) {
	owner := &Listener{connections: make(map[*connection]struct{})}
	closeErr := errors.New("closed")
	raw := &closeProbe{close: func() error {
		if count := owner.Count(); count != 0 {
			t.Errorf("connection still counted when admission is released: %d", count)
		}
		return closeErr
	}}
	conn := &connection{Conn: raw, owner: owner}
	owner.connections[conn] = struct{}{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := conn.Close(); !errors.Is(err, closeErr) {
				t.Errorf("Close error: %v", err)
			}
		}()
	}
	wg.Wait()
}

type closeProbe struct {
	net.Conn
	close func() error
}

func (conn *closeProbe) Close() error { return conn.close() }
