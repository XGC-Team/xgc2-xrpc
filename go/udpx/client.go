package udpx

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

// DefaultRetransmitInterval is the steady retransmission period once the
// default backoff is exhausted.
const DefaultRetransmitInterval = 250 * time.Millisecond

var defaultBackoff = []time.Duration{30 * time.Millisecond, 60 * time.Millisecond, 120 * time.Millisecond, 240 * time.Millisecond}

// ClientConfig holds the plain limits of a Client.
type ClientConfig struct {
	// Keys signs requests and verifies replies; required.
	Keys *KeyRing
	// Backoff lists the waits before the first retransmissions (default 30, 60,
	// 120 and 240 ms). Afterwards the request is retransmitted every Interval
	// (default DefaultRetransmitInterval) until the caller's deadline.
	Backoff  []time.Duration
	Interval time.Duration
}

// Client makes udp.v1 calls. It holds no sockets: every call uses its own, so a
// Client is safe for concurrent use.
type Client struct {
	keys     *KeyRing
	backoff  []time.Duration
	interval time.Duration
}

// NewClient validates config.
func NewClient(config ClientConfig) (*Client, error) {
	if config.Keys == nil {
		return nil, errors.New("udpx: key ring required")
	}
	if config.Backoff == nil {
		config.Backoff = defaultBackoff
	}
	if config.Interval == 0 {
		config.Interval = DefaultRetransmitInterval
	}
	for _, wait := range append([]time.Duration{config.Interval}, config.Backoff...) {
		if wait <= 0 {
			return nil, errors.New("udpx: retransmission waits must be positive")
		}
	}
	return &Client{keys: config.Keys, backoff: append([]time.Duration(nil), config.Backoff...), interval: config.Interval}, nil
}

// CallOption adjusts one call.
type CallOption func(*callOptions)

type callOptions struct {
	id    RequestID
	hasID bool
}

// WithRequestID sends the request under id instead of a fresh random one. A
// repeated id within the server's cache window returns the cached reply
// without executing the request again; use it only to retry a call the domain
// knows is safe to repeat.
func WithRequestID(id RequestID) CallOption {
	return func(o *callOptions) { o.id, o.hasID = id, true }
}

// Reply is an authenticated reply.
type Reply struct {
	Status Status
	// Body is the result for StatusOK and the {"code","message","details"} error
	// body otherwise.
	Body []byte
	// Instance is the server instance that answered.
	Instance [16]byte
	ID       RequestID
	// Sent counts the datagrams transmitted for the call, retransmissions
	// included.
	Sent int
}

// InstanceID is Instance in the lowercase hex form of ServiceRef.InstanceID.
func (r Reply) InstanceID() string { return hex.EncodeToString(r.Instance[:]) }

// Call sends one request and waits for its reply. ctx must carry a deadline: the
// request is retransmitted, always as the same datagram, until a reply arrives
// or the deadline passes, and the server runs it at most once.
//
// Errors are *xrpc.CallError. Disposition not_sent means no datagram left this
// host; response_received means a valid reply arrived, in which case Call
// returns it together with an error for any non-zero status; outcome_unknown
// means at least one datagram was sent and no valid reply arrived. Datagrams
// that fail authentication, are not replies to this request or carry another
// key are ignored: a wrong key shows up as outcome_unknown, never as an
// authentication error. If ref pins an instance, a reply from another instance
// fails with conflict and outcome_unknown.
func (c *Client) Call(ctx context.Context, ref xrpc.ServiceRef, method string, body []byte, options ...CallOption) (Reply, error) {
	var opts callOptions
	for _, option := range options {
		option(&opts)
	}
	notSent := func(code string, err error) (Reply, error) { return Reply{}, xrpc.Failure(code, xrpc.NotSent, err) }
	if err := ref.Validate(); err != nil {
		return notSent("invalid_argument", err)
	}
	if ref.Profile != xrpc.UDP {
		return notSent("invalid_argument", errors.New("udpx: udp.v1 service reference required"))
	}
	if !validMethod(method) {
		return notSent("invalid_argument", errors.New("udpx: method must be 1..128 printable UTF-8 bytes without spaces"))
	}
	if len(method)+len(body) > MaxPayload {
		return notSent("resource_exhausted", fmt.Errorf("udpx: method and body exceed %d bytes", MaxPayload))
	}
	deadline, finite := ctx.Deadline()
	if !finite {
		return notSent("invalid_argument", errors.New("udpx: finite caller deadline is required"))
	}
	if err := ctx.Err(); err != nil {
		return notSent(xrpc.Code(err), err)
	}
	keyID, key, err := c.selectKey(ref)
	if err != nil {
		return notSent("invalid_argument", err)
	}
	timeout := time.Until(deadline).Milliseconds()
	if timeout < 1 {
		return notSent("deadline_exceeded", context.DeadlineExceeded)
	}
	request := message{typ: typeRequest, keyID: keyID, word: uint32(min(timeout, MaxTimeoutMS)), method: []byte(method), body: body}
	if request.id = opts.id; !opts.hasID {
		if request.id, err = NewRequestID(); err != nil {
			return notSent("internal", err)
		}
	}
	if ref.InstanceID != "" {
		request.flags = flagExpectedInstance
		if _, err = hex.Decode(request.instance[:], []byte(ref.InstanceID)); err != nil {
			return notSent("invalid_argument", err)
		}
	}
	datagram := appendDatagram(nil, &request, key)

	var dialer net.Dialer
	connection, err := dialer.DialContext(ctx, "udp", ref.Endpoint.Address)
	if err != nil {
		return notSent(xrpc.Code(err), err)
	}
	defer connection.Close()
	conn := connection.(*net.UDPConn)
	// A canceled context must interrupt the blocking read.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Unix(1, 0)) })
	defer stop()

	sent, retransmits := 0, 0
	nextSend := time.Now()
	buffer := make([]byte, 2*MaxDatagram)
	for {
		now := time.Now()
		if !now.Before(deadline) || ctx.Err() != nil {
			break
		}
		if !now.Before(nextSend) {
			if _, err := conn.Write(datagram); err == nil {
				sent++
			} else if !errors.Is(err, syscall.ECONNREFUSED) && sent == 0 {
				return notSent("unavailable", err)
			}
			nextSend = now.Add(c.wait(retransmits))
			retransmits++
		}
		_ = conn.SetReadDeadline(minTime(nextSend, deadline))
		if ctx.Err() != nil {
			break
		}
		n, err := conn.Read(buffer)
		if err != nil {
			// A timeout is the retransmission timer or the deadline; a refusal
			// is the ICMP echo of an earlier datagram while the server restarts.
			if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) {
				continue
			}
			return Reply{}, xrpc.Failure("unavailable", dispositionAfter(sent), err)
		}
		reply, valid := parseReply(buffer[:n], keyID, key, request.id)
		if !valid {
			continue
		}
		if request.expectedInstance() && reply.instance != request.instance {
			return Reply{}, xrpc.Failure("conflict", xrpc.OutcomeUnknown, errors.New("udpx: server instance changed or does not match the pinned instance"))
		}
		result := Reply{Status: Status(reply.word), Body: bytes.Clone(reply.body), Instance: reply.instance, ID: request.id, Sent: sent}
		if result.Status == StatusOK {
			return result, nil
		}
		return result, xrpc.Failure(result.Status.Code(), xrpc.ResponseReceived, fmt.Errorf("udp.v1 %s: %s", result.Status.Code(), errorMessage(result.Body)))
	}
	cause := ctx.Err()
	if cause == nil {
		cause = context.DeadlineExceeded
	}
	return Reply{}, xrpc.Failure(xrpc.Code(cause), dispositionAfter(sent), cause)
}

// wait is the delay before retransmission number n (0 is the first).
func (c *Client) wait(n int) time.Duration {
	if n < len(c.backoff) {
		return c.backoff[n]
	}
	return c.interval
}

func (c *Client) selectKey(ref xrpc.ServiceRef) (uint32, []byte, error) {
	if ref.KeyID != 0 {
		key, ok := c.keys.lookup(ref.KeyID)
		if !ok {
			return 0, nil, fmt.Errorf("udpx: key_id %d is not in the key ring", ref.KeyID)
		}
		return ref.KeyID, key, nil
	}
	id, key, ok := c.keys.sole()
	if !ok {
		return 0, nil, fmt.Errorf("udpx: service reference names no key_id and the key ring holds %d keys", len(c.keys.keys))
	}
	return id, key, nil
}

// parseReply accepts only a well-formed reply to request id under keyID whose
// tag verifies with key.
func parseReply(datagram []byte, keyID uint32, key []byte, id RequestID) (message, bool) {
	m, err := parse(datagram)
	if err != nil || m.typ != typeReply || m.keyID != keyID || m.id != id || !authentic(datagram, key) {
		return message{}, false
	}
	return m, true
}

func dispositionAfter(sent int) xrpc.Disposition {
	if sent > 0 {
		return xrpc.OutcomeUnknown
	}
	return xrpc.NotSent
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// errorMessage extracts the message of an error body, or a bounded excerpt.
func errorMessage(body []byte) string {
	var decoded struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &decoded) == nil && decoded.Message != "" {
		return decoded.Message
	}
	if len(body) > 128 {
		body = body[:128]
	}
	return string(body)
}
