// Package udpx implements the udp.v1 profile: low-frequency control requests to
// robots on a weak LAN, one HMAC-authenticated datagram per request and per
// reply, with server-side deduplication so that client retransmission never
// executes a request twice.
//
// The datagram layout, status codes and semantics are the udp.v1 contract of
// the XRPC design record; the C++ library implements the same wire format.
package udpx

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"unicode/utf8"
)

const (
	// MaxDatagram is the largest datagram either side may send: the IPv6 minimum
	// MTU, so no datagram is ever fragmented.
	MaxDatagram = 1200
	// MaxMethodLen bounds the method name of a request.
	MaxMethodLen = 128
	// MaxTimeoutMS bounds the budget a request may carry.
	MaxTimeoutMS = 60000
	// KeyLen is the length of an HMAC key.
	KeyLen = 32
	// MaxPayload is the room left for method plus body in one datagram.
	MaxPayload = MaxDatagram - headerLen - tagLen

	headerLen = 52
	tagLen    = sha256.Size
	version   = 1

	typeRequest = 1
	typeReply   = 2

	flagExpectedInstance = 1 << 0
)

var magic = [4]byte{'X', 'R', 'U', '1'}

// Status is the status field of a reply.
type Status uint32

const (
	StatusOK                Status = 0
	StatusInvalidArgument   Status = 1
	StatusNotFound          Status = 2
	StatusConflict          Status = 3
	StatusResourceExhausted Status = 4
	StatusDeadlineExceeded  Status = 5
	StatusCancelled         Status = 6
	StatusUnavailable       Status = 7
	StatusInternal          Status = 8
	// StatusUnauthenticated is never sent: unauthenticated datagrams are
	// dropped without a reply. It exists so every status has a name.
	StatusUnauthenticated  Status = 9
	StatusPermissionDenied Status = 10
)

var statusNames = [...]string{"ok", "invalid_argument", "not_found", "conflict", "resource_exhausted", "deadline_exceeded", "cancelled", "unavailable", "internal", "unauthenticated", "permission_denied"}

// Code returns the common XRPC error code of s, or "ok" for StatusOK. Values
// outside the contract are reported as "internal".
func (s Status) Code() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return "internal"
}

func (s Status) String() string { return s.Code() }

// RequestID is the 128-bit identity of a request. The client chooses it and
// keeps it constant across retransmissions; the server deduplicates on it.
type RequestID [16]byte

// NewRequestID returns a fresh random request identity.
func NewRequestID() (RequestID, error) {
	var id RequestID
	if _, err := rand.Read(id[:]); err != nil {
		return id, errors.New("udpx: request identity unavailable")
	}
	return id, nil
}

func (id RequestID) String() string { return hex.EncodeToString(id[:]) }

// message is the decoded form of one datagram. Method and body alias the
// buffer that was parsed.
type message struct {
	typ      byte
	flags    uint16
	keyID    uint32
	id       RequestID
	instance [16]byte
	word     uint32 // request: timeout_ms; reply: status
	method   []byte
	body     []byte
}

func (m *message) expectedInstance() bool { return m.flags&flagExpectedInstance != 0 }

// appendDatagram encodes m and appends the HMAC tag computed with key.
func appendDatagram(dst []byte, m *message, key []byte) []byte {
	start := len(dst)
	dst = append(dst, magic[:]...)
	dst = append(dst, version, m.typ)
	dst = binary.BigEndian.AppendUint16(dst, m.flags)
	dst = binary.BigEndian.AppendUint32(dst, m.keyID)
	dst = append(dst, m.id[:]...)
	dst = append(dst, m.instance[:]...)
	dst = binary.BigEndian.AppendUint32(dst, m.word)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(m.method)))
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(m.body)))
	dst = append(dst, m.method...)
	dst = append(dst, m.body...)
	mac := hmac.New(sha256.New, key)
	mac.Write(dst[start:])
	return mac.Sum(dst)
}

var errMalformed = errors.New("udpx: not a udp.v1 datagram")

// parse decodes the structure of a datagram without checking its tag: magic,
// version, type, flags and the exact length. Replies carry no method.
func parse(datagram []byte) (message, error) {
	var m message
	n := len(datagram)
	if n < headerLen+tagLen || n > MaxDatagram {
		return m, errMalformed
	}
	if !bytes.Equal(datagram[:4], magic[:]) || datagram[4] != version {
		return m, errMalformed
	}
	m.typ = datagram[5]
	m.flags = binary.BigEndian.Uint16(datagram[6:])
	// Only bit 0 is defined. A reply is authenticated and has no use for the
	// bit, so a peer that echoes it is tolerated.
	if m.typ != typeRequest && m.typ != typeReply || m.flags&^flagExpectedInstance != 0 {
		return m, errMalformed
	}
	m.keyID = binary.BigEndian.Uint32(datagram[8:])
	copy(m.id[:], datagram[12:28])
	copy(m.instance[:], datagram[28:44])
	m.word = binary.BigEndian.Uint32(datagram[44:])
	methodLen := int(binary.BigEndian.Uint16(datagram[48:]))
	bodyLen := int(binary.BigEndian.Uint16(datagram[50:]))
	if headerLen+methodLen+bodyLen+tagLen != n {
		return m, errMalformed
	}
	if m.typ == typeRequest && (methodLen < 1 || methodLen > MaxMethodLen) || m.typ == typeReply && methodLen != 0 {
		return m, errMalformed
	}
	m.method = datagram[headerLen : headerLen+methodLen]
	m.body = datagram[headerLen+methodLen : headerLen+methodLen+bodyLen]
	return m, nil
}

// authentic reports whether the final 32 bytes of datagram are the HMAC-SHA256
// of everything before them under key.
func authentic(datagram, key []byte) bool {
	if len(datagram) < tagLen {
		return false
	}
	body := len(datagram) - tagLen
	mac := hmac.New(sha256.New, key)
	mac.Write(datagram[:body])
	return hmac.Equal(mac.Sum(nil), datagram[body:])
}

// validMethod accepts a UTF-8 name without control characters or spaces.
func validMethod(method string) bool {
	if len(method) < 1 || len(method) > MaxMethodLen || !utf8.ValidString(method) {
		return false
	}
	for _, r := range method {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}
