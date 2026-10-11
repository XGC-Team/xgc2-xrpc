package xrpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Method addressing. A callable domain operation is named "<service>/<Method>"
// and takes and returns JSON. Every profile carries the same name, so a caller
// can invoke a method without knowing which domain implements it:
//
//	udp.v1   the datagram's method field, verbatim
//	http.v1  POST /v1/call/<service>/<Method> with the JSON body; the answer is
//	         the JSON result or the XRPC error envelope
//	grpc.v1  the native full method /<service>/<Method>; the composed gRPC
//	         Caller converts JSON and protobuf (grpcx.Profile does it with the
//	         descriptors the caller links)
//
// ServiceRef.Service names the process or host that serves the call, not the
// capability: a world host serves xgc2.chassis.hold for all its chassis. Only
// grpc.v1 requires ServiceRef.Service to be the protobuf service name.

// MethodPathPrefix starts the http.v1 route of every method call.
const MethodPathPrefix = "/v1/call/"

// MaxMethodNameBytes bounds a method name. It is the udp.v1 limit, the
// smallest of the three profiles.
const MaxMethodNameBytes = 128

// ParseMethod validates a method name "<service>/<Method>" and returns its
// parts. The service is one or more dot-separated identifiers
// ("xgc2.chassis.hold"), the method one identifier ("Engage"); an identifier
// is a letter followed by letters, digits and underscores. This alphabet is
// valid in a URL path, a udp.v1 datagram and a gRPC full method alike.
func ParseMethod(name string) (service, method string, err error) {
	slash := strings.IndexByte(name, '/')
	if slash < 0 || len(name) > MaxMethodNameBytes {
		return "", "", fmt.Errorf("xrpc: method name must be <service>/<Method> of at most %d bytes", MaxMethodNameBytes)
	}
	service, method = name[:slash], name[slash+1:]
	if !validServiceName(service) || !validIdentifier(method) {
		return "", "", errors.New("xrpc: method name must be <service>/<Method> with dotted identifiers for the service and one identifier for the method")
	}
	return service, method, nil
}

func validServiceName(name string) bool {
	if name == "" {
		return false
	}
	for _, part := range strings.Split(name, ".") {
		if !validIdentifier(part) {
			return false
		}
	}
	return true
}

func validIdentifier(part string) bool {
	if part == "" {
		return false
	}
	for i := 0; i < len(part); i++ {
		c := part[i]
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		if !(letter || i > 0 && (c >= '0' && c <= '9' || c == '_')) {
			return false
		}
	}
	return true
}

// MethodPath returns the http.v1 route of a method name.
func MethodPath(name string) (string, error) {
	if _, _, err := ParseMethod(name); err != nil {
		return "", err
	}
	return MethodPathPrefix + name, nil
}

// NewRequestID returns a fresh request identity: 128 random bits in lowercase
// hexadecimal, valid in every profile (udp.v1 uses the bits themselves).
func NewRequestID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", errors.New("xrpc: request identity unavailable")
	}
	return hex.EncodeToString(id[:]), nil
}

// MethodCall maps one method call to the Call its profile understands. The
// body is JSON or empty. The result is for a Caller of ref's profile (or a
// Dispatcher); CallMethod is the usual entry.
func MethodCall(ref ServiceRef, name, requestID string, body json.RawMessage) (Call, error) {
	if err := ref.Validate(); err != nil {
		return Call{}, err
	}
	if _, _, err := ParseMethod(name); err != nil {
		return Call{}, err
	}
	if !ValidID(requestID) {
		return Call{}, errors.New("xrpc: canonical request identity required")
	}
	if len(body) > 0 && !json.Valid(body) {
		return Call{}, errors.New("xrpc: method body must be JSON")
	}
	call := Call{Service: ref, RequestID: requestID, Payload: body}
	switch ref.Profile {
	case HTTP:
		call.Method, call.Path = "POST", MethodPathPrefix+name
	case GRPC:
		call.Method = "/" + name
	default:
		call.Method = name
	}
	return call, nil
}

// CallMethod invokes the method "<service>/<Method>" on ref over ref's profile
// with a fresh request identity and returns the JSON result. Every caller
// obeys the common mechanics: a finite deadline in ctx, one of three
// dispositions in the error, no replay. http.v1 and grpc.v1 references must
// pin an instance; a udp.v1 reference may leave it empty (the first answer
// reveals the instance in Result.InstanceID).
//
// When the server answered with an error the error is a *CallError with
// disposition response_received, and Result carries the answer's body in the
// error envelope {"error":{"code","message","details"?}} (http.v1 and udp.v1)
// and the answering instance. Transport failures return an empty Result.
//
// This is a method of the Dispatcher, not a package function, because the
// transports need their own configuration (local target, TLS, key ring,
// descriptors); the Dispatcher is where the caller composes them.
func (d *Dispatcher) CallMethod(ctx context.Context, ref ServiceRef, name string, body json.RawMessage) (Result, error) {
	if ctx == nil {
		return Result{}, Failure("invalid_argument", NotSent, errors.New("xrpc: context is required"))
	}
	if _, ok := ctx.Deadline(); !ok {
		return Result{}, Failure("invalid_argument", NotSent, errors.New("xrpc: finite caller deadline is required"))
	}
	id, err := NewRequestID()
	if err != nil {
		return Result{}, Failure("internal", NotSent, err)
	}
	call, err := MethodCall(ref, name, id, body)
	if err != nil {
		return Result{}, Failure("invalid_argument", NotSent, err)
	}
	return d.Call(ctx, call)
}

// StatusForCode returns the HTTP status that carries an XRPC error code ("ok"
// is 200), the inverse of how clients read statuses without an envelope.
// Unknown (domain) codes are internal errors.
func StatusForCode(code string) int {
	switch code {
	case "ok":
		return 200
	case "invalid_argument":
		return 400
	case "unauthenticated":
		return 401
	case "permission_denied":
		return 403
	case "not_found":
		return 404
	case "conflict":
		return 409
	case "resource_exhausted":
		return 429
	case "deadline_exceeded":
		return 504
	case "cancelled":
		return 499
	case "unavailable":
		return 503
	default:
		return 500
	}
}
