package grpcx

import (
	"fmt"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func policyValue(p *xrpc.Policy, name string, current int64) (int64, error) {
	value, err := p.Integer(name)
	if err != nil {
		return 0, err
	}
	if current != 0 && current != value {
		return 0, fmt.Errorf("xrpc: option conflicts with resolved runtime policy %s", name)
	}
	return value, nil
}

func (o HostOptions) WithPolicy(p *xrpc.Policy) (HostOptions, error) {
	if err := xrpc.CheckDiagnosticPolicy(p, o.Diagnostics); err != nil {
		return o, err
	}
	if o.MaxMessageBytes != 0 {
		return o, fmt.Errorf("xrpc: use distinct request/response policy limits")
	}
	for _, entry := range []struct {
		name string
		dst  *int
	}{
		{"HOST_MAX_CONNECTIONS", &o.MaxConnections}, {"HOST_MAX_IN_FLIGHT", &o.MaxInFlight}, {"MAX_REQUEST_BYTES", &o.MaxRequestBytes}, {"MAX_RESPONSE_BYTES", &o.MaxResponseBytes},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst))
		if err != nil {
			return o, err
		}
		*entry.dst = int(v)
	}
	v, err := policyValue(p, "GRPC_MAX_STREAMS_PER_CONNECTION", int64(o.MaxConcurrentStreams))
	if err != nil {
		return o, err
	}
	o.MaxConcurrentStreams = uint32(v)
	for _, entry := range []struct {
		name string
		dst  *time.Duration
	}{
		{"CALL_TIMEOUT_MS", &o.MaxCallTime}, {"IDLE_TIMEOUT_MS", &o.IdleTimeout}, {"SHUTDOWN_TIMEOUT_MS", &o.ShutdownTimeout},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst/time.Millisecond))
		if err != nil {
			return o, err
		}
		*entry.dst = time.Duration(v) * time.Millisecond
	}
	// HTTP-capability values are also useful to a composition hosting both
	// profiles. Otherwise native gRPC uses its finite SDK header/handshake caps.
	if v, err := p.Integer("MAX_HEADER_BYTES"); err == nil {
		v, err = policyValue(p, "MAX_HEADER_BYTES", int64(o.MaxHeaderBytes))
		if err != nil {
			return o, err
		}
		o.MaxHeaderBytes = uint32(v)
	}
	if v, err := p.Integer("HEADER_TIMEOUT_MS"); err == nil {
		v, err = policyValue(p, "HEADER_TIMEOUT_MS", int64(o.HandshakeTimeout/time.Millisecond))
		if err != nil {
			return o, err
		}
		o.HandshakeTimeout = time.Duration(v) * time.Millisecond
	}
	return o, nil
}

func (o DialOptions) WithPolicy(p *xrpc.Policy) (DialOptions, error) {
	if err := xrpc.CheckDiagnosticPolicy(p, o.Diagnostics); err != nil {
		return o, err
	}
	if o.MaxMessageBytes != 0 {
		return o, fmt.Errorf("xrpc: use distinct request/response policy limits")
	}
	if _, err := p.Integer("MAX_HEADER_BYTES"); err == nil {
		v, err := policyValue(p, "MAX_HEADER_BYTES", int64(o.MaxHeaderBytes))
		if err != nil {
			return o, err
		}
		o.MaxHeaderBytes = uint32(v)
	}
	for _, entry := range []struct {
		name string
		dst  *int
	}{
		{"MAX_REQUEST_BYTES", &o.MaxRequestBytes}, {"MAX_RESPONSE_BYTES", &o.MaxResponseBytes}, {"CLIENT_MAX_REFERENCES", &o.MaxReferences},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst))
		if err != nil {
			return o, err
		}
		*entry.dst = int(v)
	}
	for _, entry := range []struct {
		name string
		dst  *time.Duration
	}{
		{"CALL_TIMEOUT_MS", &o.MaxCallTime}, {"CLIENT_REFERENCE_IDLE_TIMEOUT_MS", &o.ReferenceIdleTimeout}, {"IDLE_TIMEOUT_MS", &o.IdleTimeout},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst/time.Millisecond))
		if err != nil {
			return o, err
		}
		*entry.dst = time.Duration(v) * time.Millisecond
	}
	// grpc-go owns one HTTP/2 connection per immutable reference. That fixed
	// bound satisfies every positive CLIENT_MAX_CONNECTIONS policy value.
	return o, nil
}
