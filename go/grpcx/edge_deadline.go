package grpcx

import (
	"net"
	"time"
)

// Native gRPC's graceful connection aging has an additional close delay. The
// immutable accepted-socket deadline supplies the actual owner IO boundary;
// native handshake/deadline resets cannot extend it. Go's maintained net poller
// owns deadline scheduling, with no SDK per-stream timer or polling worker.
type edgeDeadlineListener struct {
	net.Listener
	lifetime time.Duration
}

func (l *edgeDeadlineListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	owned := &edgeDeadlineConn{Conn: connection, deadline: time.Now().Add(l.lifetime)}
	if err = owned.SetDeadline(time.Time{}); err != nil {
		connection.Close()
		return nil, err
	}
	return owned, nil
}

type edgeDeadlineConn struct {
	net.Conn
	deadline time.Time
}

func (c *edgeDeadlineConn) bound(deadline time.Time) time.Time {
	if deadline.IsZero() || deadline.After(c.deadline) {
		return c.deadline
	}
	return deadline
}
func (c *edgeDeadlineConn) SetDeadline(deadline time.Time) error {
	return c.Conn.SetDeadline(c.bound(deadline))
}
func (c *edgeDeadlineConn) SetReadDeadline(deadline time.Time) error {
	return c.Conn.SetReadDeadline(c.bound(deadline))
}
func (c *edgeDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	return c.Conn.SetWriteDeadline(c.bound(deadline))
}
