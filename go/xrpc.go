// Package xrpc defines transport-neutral references and finite calls. It has no
// service discovery, provider activation, workflow or persistence dependencies.
package xrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const HTTP = "http.v1"
const GRPC = "grpc.v1"
const UDP = "udp.v1"

type Endpoint struct {
	Kind    string `json:"kind"`
	Address string `json:"address"`
}

func (e Endpoint) Validate() error {
	if e.Address == "" || strings.ContainsAny(e.Address, "\x00\r\n") {
		return errors.New("xrpc: endpoint address is required")
	}
	switch e.Kind {
	case "unix":
		if !filepath.IsAbs(e.Address) || filepath.Clean(e.Address) != e.Address || len(e.Address) >= 108 {
			return errors.New("xrpc: Unix endpoint must be a canonical absolute path shorter than 108 bytes")
		}
	case "https":
		u, err := url.Parse(e.Address)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return errors.New("xrpc: HTTPS endpoint must be an authenticated origin")
		}
	case "tls":
		if _, _, err := net.SplitHostPort(e.Address); err != nil {
			return fmt.Errorf("xrpc: TLS endpoint: %w", err)
		}
	case "udp":
		host, port, err := net.SplitHostPort(e.Address)
		if err != nil || host == "" {
			return errors.New("xrpc: UDP endpoint must be host:port (IPv6 literals in brackets)")
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return errors.New("xrpc: UDP endpoint port must be canonical 1..65535")
		}
	default:
		return fmt.Errorf("xrpc: unsupported endpoint kind %q", e.Kind)
	}
	return nil
}

type ServiceRef struct {
	TargetID   string   `json:"target_id"`
	Service    string   `json:"service"`
	APIVersion string   `json:"api_version"`
	InstanceID string   `json:"instance_id"`
	Profile    string   `json:"profile"`
	Endpoint   Endpoint `json:"endpoint"`
	// KeyID names the udp.v1 HMAC key that authenticates calls to this service.
	// Zero means unspecified: the caller's key ring must then hold exactly one
	// key. It must be zero for other profiles.
	KeyID uint32 `json:"key_id,omitempty"`
}

func (r ServiceRef) Validate() error {
	for _, value := range []string{r.TargetID, r.Service, r.APIVersion} {
		if value == "" || value != strings.TrimSpace(value) || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("xrpc: complete canonical service reference required")
		}
	}
	if r.InstanceID != "" && !ValidID(r.InstanceID) {
		return errors.New("xrpc: instance identity must be canonical")
	}
	if r.Profile != HTTP && r.Profile != GRPC && r.Profile != UDP {
		return errors.New("xrpc: unsupported profile")
	}
	if err := r.Endpoint.Validate(); err != nil {
		return err
	}
	if r.Profile == HTTP && r.Endpoint.Kind == "tls" {
		return errors.New("xrpc: HTTP TLS endpoint must use an HTTPS origin")
	}
	if r.Profile == GRPC && r.Endpoint.Kind == "https" {
		return errors.New("xrpc: gRPC TLS endpoint must use host:port")
	}
	if (r.Profile == UDP) != (r.Endpoint.Kind == "udp") {
		return errors.New("xrpc: udp.v1 requires a udp endpoint and no other profile accepts one")
	}
	if r.Profile == UDP && r.InstanceID != "" && !validUDPInstance(r.InstanceID) {
		return errors.New("xrpc: udp.v1 instance identity must be 32 lowercase hexadecimal digits")
	}
	if r.Profile != UDP && r.KeyID != 0 {
		return errors.New("xrpc: key identity applies to udp.v1 only")
	}
	return nil
}

// validUDPInstance reports whether id is the lowercase hexadecimal form of a
// 128-bit udp.v1 instance identity, the form NewInstanceID produces.
func validUDPInstance(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := range id {
		if c := id[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ValidateInternal rejects unbound discovery references for internal calls.
func (r ServiceRef) ValidateInternal() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.InstanceID == "" {
		return errors.New("xrpc: internal service reference requires instance identity")
	}
	return nil
}

// DialContext is supplied by the transport owner for remote services. An SDK
// must not interpret a different target's Unix pathname as a local endpoint.
type DialContext func(context.Context, ServiceRef) (net.Conn, error)

type Disposition string

const (
	NotSent          Disposition = "not_sent"
	OutcomeUnknown   Disposition = "outcome_unknown"
	ResponseReceived Disposition = "response_received"
)

type CallError struct {
	Code        string      `json:"code"`
	Message     string      `json:"message"`
	Disposition Disposition `json:"disposition"`
	Cause       error       `json:"-"`
}

func (e *CallError) Error() string { return e.Code + ": " + e.Message }
func (e *CallError) Unwrap() error { return e.Cause }
func Failure(code string, disposition Disposition, err error) *CallError {
	return &CallError{Code: code, Message: err.Error(), Disposition: disposition, Cause: err}
}
func Code(err error) string {
	var failure *CallError
	if errors.As(err, &failure) {
		return failure.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "unavailable"
}

func Remaining(ctx context.Context) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("xrpc: context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, errors.New("xrpc: finite caller deadline is required")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	return remaining, nil
}

// Call names a domain-owned route or typed method. Payload is only a client
// representation; the profile encodes it using the domain's actual contract.
type Call struct {
	Service   ServiceRef      `json:"service"`
	Method    string          `json:"method"`
	Path      string          `json:"path,omitempty"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}
type Result struct {
	Status  int             `json:"status,omitempty"`
	Payload json.RawMessage `json:"payload"`
}
type Caller interface {
	Call(context.Context, Call) (Result, error)
}
type Observer interface {
	Observe(context.Context, Call, func(Result) error) error
}

// Dispatcher is immutable local composition, not a registry or discovery API.
// It composes the http.v1 and grpc.v1 profiles; udp.v1 callers use udpx.Client.
type Dispatcher struct{ profiles map[string]Caller }

func NewDispatcher(profiles map[string]Caller) (*Dispatcher, error) {
	copy := make(map[string]Caller, len(profiles))
	for profile, caller := range profiles {
		if (profile != HTTP && profile != GRPC) || caller == nil {
			return nil, errors.New("xrpc: invalid profile caller")
		}
		copy[profile] = caller
	}
	return &Dispatcher{profiles: copy}, nil
}
func (d *Dispatcher) caller(ctx context.Context, call Call) (Caller, error) {
	if _, err := Remaining(ctx); err != nil {
		return nil, Failure(Code(err), NotSent, err)
	}
	if err := call.Service.ValidateInternal(); err != nil {
		return nil, Failure("invalid_argument", NotSent, err)
	}
	if call.Method == "" || !ValidID(call.RequestID) {
		return nil, Failure("invalid_argument", NotSent, errors.New("xrpc: method and request ID required"))
	}
	if d == nil || d.profiles[call.Service.Profile] == nil {
		return nil, Failure("unavailable", NotSent, errors.New("xrpc: profile is not composed"))
	}
	return d.profiles[call.Service.Profile], nil
}
func (d *Dispatcher) Call(ctx context.Context, call Call) (Result, error) {
	caller, err := d.caller(ctx, call)
	if err != nil {
		return Result{}, err
	}
	return caller.Call(ctx, call)
}
func (d *Dispatcher) Observe(ctx context.Context, call Call, emit func(Result) error) error {
	caller, err := d.caller(ctx, call)
	if err != nil {
		return err
	}
	observer, ok := caller.(Observer)
	if !ok || emit == nil {
		return Failure("invalid_argument", NotSent, errors.New("xrpc: profile has no observation implementation"))
	}
	return observer.Observe(ctx, call, emit)
}
