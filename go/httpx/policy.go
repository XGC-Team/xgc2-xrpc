package httpx

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

// WithPolicy applies a shared startup snapshot. Resolve product option choices
// as deployment defaults/ceilings first; a conflicting option fails explicitly.
func (o HostOptions) WithPolicy(p *xrpc.Policy) (HostOptions, error) {
	if err := xrpc.CheckDiagnosticPolicy(p, o.Diagnostics); err != nil {
		return o, err
	}
	for _, entry := range []struct {
		name string
		dst  *int
	}{
		{"HOST_MAX_CONNECTIONS", &o.MaxConnections}, {"HOST_MAX_IN_FLIGHT", &o.MaxInFlight}, {"MAX_HEADER_BYTES", &o.MaxHeaderBytes},
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
		{"HEADER_TIMEOUT_MS", &o.HeaderTimeout}, {"IDLE_TIMEOUT_MS", &o.IdleTimeout}, {"SHUTDOWN_TIMEOUT_MS", &o.ShutdownTimeout},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst/time.Millisecond))
		if err != nil {
			return o, err
		}
		*entry.dst = time.Duration(v) * time.Millisecond
	}
	// Public gateways can select host/http/transport without imposing the finite
	// internal-RPC response budget on their SSE or WebSocket domain contract.
	if _, err := p.Integer("CALL_TIMEOUT_MS"); err == nil {
		v, err := policyValue(p, "CALL_TIMEOUT_MS", int64(o.MaxCallTime/time.Millisecond))
		if err != nil {
			return o, err
		}
		o.MaxCallTime = time.Duration(v) * time.Millisecond
		v, err = policyValue(p, "MAX_REQUEST_BYTES", o.MaxBodyBytes)
		if err != nil {
			return o, err
		}
		o.MaxBodyBytes = v
		v, err = policyValue(p, "MAX_RESPONSE_BYTES", o.MaxResponseBytes)
		if err != nil {
			return o, err
		}
		o.MaxResponseBytes = v
	}
	return o, nil
}

func (c Config) WithPolicy(p *xrpc.Policy) (Config, error) {
	if err := xrpc.CheckDiagnosticPolicy(p, c.Diagnostics); err != nil {
		return c, err
	}
	for _, entry := range []struct {
		name string
		dst  *int64
	}{
		{"MAX_REQUEST_BYTES", &c.MaxRequestBytes}, {"MAX_RESPONSE_BYTES", &c.MaxResponseBytes}, {"MAX_HEADER_BYTES", &c.MaxHeaderBytes},
	} {
		v, err := policyValue(p, entry.name, *entry.dst)
		if err != nil {
			return c, err
		}
		*entry.dst = v
	}
	v, err := policyValue(p, "CLIENT_MAX_CONNECTIONS", int64(c.MaxConnections))
	if err != nil {
		return c, err
	}
	c.MaxConnections = int(v)
	for _, entry := range []struct {
		name string
		dst  *time.Duration
	}{
		{"CALL_TIMEOUT_MS", &c.MaxCallTime}, {"HEADER_TIMEOUT_MS", &c.HeaderTimeout}, {"IDLE_TIMEOUT_MS", &c.IdleTimeout},
	} {
		v, err := policyValue(p, entry.name, int64(*entry.dst/time.Millisecond))
		if err != nil {
			return c, err
		}
		*entry.dst = time.Duration(v) * time.Millisecond
	}
	if _, err := p.Integer("CLIENT_MAX_REFERENCES"); err == nil {
		v, err := policyValue(p, "CLIENT_MAX_REFERENCES", int64(c.MaxReferences))
		if err != nil {
			return c, err
		}
		c.MaxReferences = int(v)
		v, err = policyValue(p, "CLIENT_REFERENCE_IDLE_TIMEOUT_MS", int64(c.ReferenceIdleTimeout/time.Millisecond))
		if err != nil {
			return c, err
		}
		c.ReferenceIdleTimeout = time.Duration(v) * time.Millisecond
	}
	return c, nil
}
