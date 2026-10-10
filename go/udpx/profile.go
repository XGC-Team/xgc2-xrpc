package udpx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

// Profile adapts a Client to xrpc.Caller, so that an xrpc.Dispatcher composes
// udp.v1 beside http.v1 and grpc.v1 and Dispatcher.CallMethod reaches all three
// with one method name. Call.Method is the datagram's method field and
// Call.Payload its JSON body.
type Profile struct{ client *Client }

// NewProfile wraps client.
func NewProfile(client *Client) (*Profile, error) {
	if client == nil {
		return nil, errors.New("udpx: client required")
	}
	return &Profile{client: client}, nil
}

// Call sends the request and returns the reply body as the Result payload.
// Request identity: a call.RequestID that is 32 lowercase hexadecimal digits
// is the 128-bit request id itself; any other identity maps to the first 128
// bits of its SHA-256, so the same identity is the same request to the
// server's reply cache, and a new identity never collides with it by accident.
// An empty reply body is the JSON null.
//
// A reply with a non-zero status is both a Result and a *xrpc.CallError with
// disposition response_received. The Result carries the error in the same
// envelope http.v1 uses, {"error":{"code","message","details"?}}, and
// the answering instance.
func (p *Profile) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	if len(call.Payload) > 0 && !json.Valid(call.Payload) {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("udpx: JSON payload required"))
	}
	reply, err := p.client.Call(ctx, call.Service, call.Method, call.Payload, WithRequestID(requestIDOf(call.RequestID)))
	if err != nil {
		var failure *xrpc.CallError
		if !errors.As(err, &failure) || failure.Disposition != xrpc.ResponseReceived {
			return xrpc.Result{}, err
		}
		return xrpc.Result{Status: xrpc.StatusForCode(reply.Status.Code()), Payload: envelope(reply), InstanceID: reply.InstanceID()}, err
	}
	body := reply.Body
	if len(body) == 0 {
		body = []byte("null")
	}
	if !json.Valid(body) {
		return xrpc.Result{}, xrpc.Failure("internal", xrpc.ResponseReceived, errors.New("udpx: reply is not JSON"))
	}
	return xrpc.Result{Status: 200, Payload: body, InstanceID: reply.InstanceID()}, nil
}

func requestIDOf(identity string) RequestID {
	var id RequestID
	if len(identity) == 2*len(id) {
		if _, err := hex.Decode(id[:], []byte(identity)); err == nil && hex.EncodeToString(id[:]) == identity {
			return id
		}
	}
	sum := sha256.Sum256([]byte(identity))
	copy(id[:], sum[:])
	return id
}

// envelope wraps an error reply's body as the http.v1 error envelope. A body
// that is not a JSON object is replaced by one that names the status.
func envelope(reply Reply) json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(reply.Body, &object) == nil && object != nil {
		return json.RawMessage(`{"error":` + string(reply.Body) + `}`)
	}
	synthetic, _ := json.Marshal(map[string]any{"error": map[string]string{"code": reply.Status.Code(), "message": errorMessage(reply.Body)}})
	return synthetic
}
