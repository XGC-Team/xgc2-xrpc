// Package httpx implements the http.v1 profile using net/http, not a private parser.
package httpx

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/internal/refpool"
)

const TimeoutHeader = "X-Xrpc-Timeout-Ms"
const RequestIDHeader = "X-Request-ID"
const InstanceIDHeader = "X-Xrpc-Instance-ID"

var ErrResponseTooLarge = errors.New("xrpc: HTTP response exceeds byte limit")

type Config struct {
	LocalTargetID string
	Service       xrpc.ServiceRef
	DialContext   xrpc.DialContext
	TLSConfig     *tls.Config
	TLSForService func(xrpc.ServiceRef) (*tls.Config, error)
	// Headers are copied at construction. Wire-owned identity/budget headers
	// are rejected; authentication remains an explicitly supplied capability.
	Headers              map[string]string
	MaxResponseBytes     int64
	MaxRequestBytes      int64
	MaxHeaderBytes       int64
	MaxCallTime          time.Duration
	HeaderTimeout        time.Duration
	MaxConnections       int
	MaxInFlight          int
	IdleTimeout          time.Duration
	MaxReferences        int
	ReferenceIdleTimeout time.Duration
	Diagnostics          *xrpc.Diagnostics
	Metrics              *xrpc.Metrics
}
type callDeadlineKey struct{}

type Client struct {
	config    Config
	client    *http.Client
	transport *http.Transport
	base      string
	owner     context.Context
	close     context.CancelFunc
	slots     chan struct{}
}

func New(config Config) (*Client, error) {
	if config.Metrics == nil {
		config.Metrics = &xrpc.Metrics{}
	}
	if err := validateHeaders(config.Headers); err != nil {
		return nil, err
	}
	copyHeaders := make(map[string]string, len(config.Headers))
	for key, value := range config.Headers {
		copyHeaders[key] = value
	}
	config.Headers = copyHeaders
	if err := config.Service.Validate(); err != nil {
		return nil, err
	}
	if config.Service.Profile != xrpc.HTTP {
		return nil, errors.New("xrpc: HTTP profile required")
	}
	if config.LocalTargetID == "" {
		return nil, errors.New("xrpc: local target identity required")
	}
	if config.MaxResponseBytes == math.MaxInt64 {
		return nil, errors.New("xrpc: response byte limit is too large")
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = xrpc.DefaultPolicyInteger("MAX_RESPONSE_BYTES")
	}
	if config.MaxRequestBytes <= 0 {
		config.MaxRequestBytes = xrpc.DefaultPolicyInteger("MAX_REQUEST_BYTES")
	}
	if config.MaxHeaderBytes <= 0 {
		config.MaxHeaderBytes = xrpc.DefaultPolicyInteger("MAX_HEADER_BYTES")
	}
	if config.MaxCallTime <= 0 {
		config.MaxCallTime = time.Duration(xrpc.DefaultPolicyInteger("CALL_TIMEOUT_MS")) * time.Millisecond
	}
	if config.HeaderTimeout <= 0 {
		config.HeaderTimeout = time.Duration(xrpc.DefaultPolicyInteger("HEADER_TIMEOUT_MS")) * time.Millisecond
	}
	if config.MaxConnections <= 0 {
		config.MaxConnections = int(xrpc.DefaultPolicyInteger("CLIENT_MAX_CONNECTIONS"))
	}
	if config.MaxInFlight <= 0 {
		config.MaxInFlight = int(xrpc.DefaultPolicyInteger("HOST_MAX_IN_FLIGHT"))
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = time.Duration(xrpc.DefaultPolicyInteger("IDLE_TIMEOUT_MS")) * time.Millisecond
	}
	transport := &http.Transport{Proxy: nil, DisableCompression: true, MaxConnsPerHost: config.MaxConnections, MaxIdleConnsPerHost: config.MaxConnections, MaxIdleConns: config.MaxConnections, IdleConnTimeout: config.IdleTimeout, ResponseHeaderTimeout: config.HeaderTimeout, MaxResponseHeaderBytes: config.MaxHeaderBytes, ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	owner, closeOwner := context.WithCancel(context.Background())
	success := false
	defer func() {
		if !success {
			closeOwner()
		}
	}()
	base := ""
	switch config.Service.Endpoint.Kind {
	case "unix":
		base = "http://unix"
		if config.Service.TargetID != config.LocalTargetID && config.DialContext == nil {
			return nil, errors.New("xrpc: remote Unix service requires injected routing")
		}
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			deadline, _ := ctx.Value(callDeadlineKey{}).(time.Time)
			if deadline.IsZero() {
				return nil, errors.New("xrpc: dial lost caller budget")
			}
			ctx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			if config.DialContext != nil {
				return config.DialContext(ctx, config.Service)
			}
			return (&net.Dialer{}).DialContext(ctx, "unix", config.Service.Endpoint.Address)
		}
	case "https":
		base = strings.TrimSuffix(config.Service.Endpoint.Address, "/")
		if config.TLSForService != nil {
			if config.TLSConfig != nil {
				return nil, errors.New("xrpc: choose TLSConfig or TLSForService")
			}
			resolved, err := config.TLSForService(config.Service)
			if err != nil {
				return nil, err
			}
			if resolved == nil {
				return nil, errors.New("xrpc: TLSForService returned no TLS policy")
			}
			config.TLSConfig = resolved
		}
		if config.TLSConfig != nil {
			if config.TLSConfig.InsecureSkipVerify {
				return nil, errors.New("xrpc: authenticated HTTPS cannot skip certificate verification")
			}
			transport.TLSClientConfig = config.TLSConfig.Clone()
		}
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.MinVersion = max(transport.TLSClientConfig.MinVersion, tls.VersionTLS12)
		transport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			deadline, _ := ctx.Value(callDeadlineKey{}).(time.Time)
			if deadline.IsZero() {
				return nil, errors.New("xrpc: dial lost caller budget")
			}
			ctx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			stop := context.AfterFunc(owner, cancel)
			defer stop()
			var raw net.Conn
			var err error
			if config.DialContext != nil {
				raw, err = config.DialContext(ctx, config.Service)
			} else {
				raw, err = (&net.Dialer{}).DialContext(ctx, network, address)
			}
			if err != nil {
				return nil, err
			}
			policy := transport.TLSClientConfig.Clone()
			if policy.ServerName == "" {
				origin, _ := url.Parse(config.Service.Endpoint.Address)
				policy.ServerName = origin.Hostname()
			}
			secured := tls.Client(raw, policy)
			if err := secured.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			return secured, nil
		}
	default:
		return nil, errors.New("xrpc: unsupported HTTP endpoint")
	}
	success = true
	return &Client{config: config, transport: transport, base: base, owner: owner, close: closeOwner, slots: make(chan struct{}, config.MaxInFlight), client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func validID(value string) bool { return xrpc.ValidID(value) }

func requestPath(path string) error {
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.ContainsAny(path, "#\\\x00\r\n ") {
		return errors.New("xrpc: absolute local HTTP request URI required")
	}
	parsed, err := url.ParseRequestURI(path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" {
		return errors.New("xrpc: invalid HTTP request URI")
	}
	return nil
}

// DoStream returns a size-bounded response body; the caller must close it.
// The caller deadline remains active through the last body read. Failures do not imply
// remote rejection. Mutation requests have no automatic replay path.
func (c *Client) DoStream(ctx context.Context, method, path, requestID, contentType string, body []byte) (*http.Response, error) {
	return c.doStream(ctx, method, path, requestID, contentType, body, nil)
}

func (c *Client) doStream(ctx context.Context, method, path, requestID, contentType string, body []byte, headers map[string]string) (*http.Response, error) {
	if err := validateHeaders(headers); err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	remaining, err := xrpc.Remaining(ctx)
	if err != nil {
		return nil, xrpc.Failure(xrpc.Code(err), xrpc.NotSent, err)
	}
	if c == nil || c.client == nil || !validID(requestID) {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: HTTP client and request identity required"))
	}
	if c.owner.Err() != nil {
		return nil, xrpc.Failure("unavailable", xrpc.NotSent, errors.New("xrpc: HTTP client closed"))
	}
	if int64(len(body)) > c.config.MaxRequestBytes {
		return nil, xrpc.Failure("resource_exhausted", xrpc.NotSent, errors.New("xrpc: HTTP request exceeds byte limit"))
	}
	ctx, budgetCancel := context.WithTimeout(ctx, min(c.config.MaxCallTime, 24*time.Hour))
	remaining, err = xrpc.Remaining(ctx)
	if err != nil {
		budgetCancel()
		return nil, xrpc.Failure(xrpc.Code(err), xrpc.NotSent, err)
	}
	select {
	case c.slots <- struct{}{}:
	default:
		budgetCancel()
		c.config.Metrics.Reject()
		c.config.Diagnostics.Emit(xrpc.Diagnostic{Level: "warn", Event: "admission_rejected", Service: c.config.Service.Service, InstanceID: c.config.Service.InstanceID, RequestID: requestID, Category: "resource_exhausted"})
		return nil, xrpc.Failure("resource_exhausted", xrpc.NotSent, errors.New("xrpc: HTTP client concurrency exhausted"))
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.owner, cancel)
	finishMetric := c.config.Metrics.Admit()
	started := time.Now()
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			c.config.Metrics.Outcome(ctx.Err())
			c.config.Diagnostics.Emit(xrpc.Diagnostic{Level: "debug", Event: "call_finished", Service: c.config.Service.Service, InstanceID: c.config.Service.InstanceID, RequestID: requestID, Operation: method, ElapsedMS: time.Since(started).Milliseconds()})
			finishMetric()
			stop()
			cancel()
			budgetCancel()
			<-c.slots
		})
	}
	transferred := false
	defer func() {
		if !transferred {
			cleanup()
		}
	}()
	if err = requestPath(path); err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	// Supplying an opaque Reader prevents net/http from installing GetBody and
	// automatically replaying a non-empty mutation on a reused connection.
	reader := struct{ io.Reader }{bytes.NewReader(body)}
	deadline, _ := ctx.Deadline()
	ctx = context.WithValue(ctx, callDeadlineKey{}, deadline)
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, xrpc.Failure("invalid_argument", xrpc.NotSent, err)
	}
	req.ContentLength = int64(len(body))
	for key, value := range c.config.Headers {
		req.Header.Set(key, value)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Header.Set(TimeoutHeader, strconv.FormatInt(max(1, remaining.Milliseconds()), 10))
	req.Header.Set(RequestIDHeader, requestID)
	if c.config.Service.InstanceID != "" {
		req.Header.Set(InstanceIDHeader, c.config.Service.InstanceID)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	var connected atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) {
		connected.Store(true)
		// GotConn precedes net/http's request writer, after pool wait and TLS.
		req.Header.Set(TimeoutHeader, strconv.FormatInt(max(1, time.Until(deadline).Milliseconds()), 10))
	}}))
	response, err := c.client.Do(req)
	disposition := xrpc.NotSent
	if connected.Load() {
		disposition = xrpc.OutcomeUnknown
	}
	if err != nil {
		return nil, xrpc.Failure(xrpc.Code(err), disposition, err)
	}
	if c.config.Service.InstanceID != "" && (len(response.Header.Values(InstanceIDHeader)) != 1 || response.Header.Get(InstanceIDHeader) != c.config.Service.InstanceID) {
		_ = response.Body.Close()
		return nil, xrpc.Failure("conflict", xrpc.OutcomeUnknown, errors.New("xrpc: server instance changed or identity missing"))
	}
	if method != http.MethodHead && response.ContentLength > c.config.MaxResponseBytes {
		_ = response.Body.Close()
		return nil, xrpc.Failure("resource_exhausted", xrpc.ResponseReceived, ErrResponseTooLarge)
	}
	response.Body = &boundedBody{ReadCloser: response.Body, remaining: c.config.MaxResponseBytes, cleanup: cleanup}
	transferred = true
	return response, nil
}

// Do buffers a bounded response. DoStream avoids that copy for typed resources.
func (c *Client) Do(ctx context.Context, method, path, requestID, contentType string, body []byte) ([]byte, int, http.Header, error) {
	return c.DoWithHeaders(ctx, method, path, requestID, contentType, body, nil)
}

// DoWithHeaders uses the same finite, no-replay call path as Do. Authentication
// headers are caller-owned; transport identity and framing cannot be overridden.
func (c *Client) DoWithHeaders(ctx context.Context, method, path, requestID, contentType string, body []byte, headers map[string]string) ([]byte, int, http.Header, error) {
	response, err := c.doStream(ctx, method, path, requestID, contentType, body, headers)
	if err != nil {
		return nil, 0, nil, err
	}
	defer response.Body.Close()
	output, err := io.ReadAll(response.Body)
	return output, response.StatusCode, response.Header, err
}

func validateHeaders(headers map[string]string) error {
	seen := make(map[string]bool, len(headers))
	for name, value := range headers {
		lower := strings.ToLower(name)
		if seen[lower] {
			return errors.New("xrpc: duplicate caller header name")
		}
		seen[lower] = true
		switch strings.ToLower(name) {
		case strings.ToLower(TimeoutHeader), strings.ToLower(RequestIDHeader), strings.ToLower(InstanceIDHeader), "host", "connection", "content-length", "transfer-encoding", "trailer", "upgrade":
			return errors.New("xrpc: caller header is owned by transport: " + name)
		}
		if name == "" || strings.ContainsAny(name, " \t\r\n:\x00") || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("xrpc: invalid caller header name/value")
		}
	}
	return nil
}

type boundedBody struct {
	io.ReadCloser
	remaining int64
	cleanup   func()
}

func (b *boundedBody) Close() error { err := b.ReadCloser.Close(); b.cleanup(); return err }

func (b *boundedBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var extra [1]byte
		n, err := b.ReadCloser.Read(extra[:])
		if n > 0 {
			return 0, xrpc.Failure("resource_exhausted", xrpc.ResponseReceived, ErrResponseTooLarge)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, xrpc.Failure(xrpc.Code(err), xrpc.OutcomeUnknown, err)
		}
		return 0, err
	}
	if int64(len(buffer)) > b.remaining {
		buffer = buffer[:b.remaining]
	}
	n, err := b.ReadCloser.Read(buffer)
	b.remaining -= int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		err = xrpc.Failure(xrpc.Code(err), xrpc.OutcomeUnknown, err)
	}
	return n, err
}

func (c *Client) Close() {
	if c != nil && c.transport != nil {
		c.close()
		c.transport.CloseIdleConnections()
	}
}
func (c *Client) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	return c.CallWithHeaders(ctx, call, nil)
}

// Reference returns the immutable binding used by this transport. Complete
// call binding is checked by CallWithHeaders before any network admission.
func (c *Client) Reference() xrpc.ServiceRef {
	if c == nil {
		return xrpc.ServiceRef{}
	}
	return c.config.Service
}

func (c *Client) CallWithHeaders(ctx context.Context, call xrpc.Call, headers map[string]string) (xrpc.Result, error) {
	if c == nil {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: HTTP client required"))
	}
	if call.Service != c.config.Service {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: client service reference mismatch"))
	}
	if len(call.Payload) > 0 && !json.Valid(call.Payload) {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("xrpc: JSON payload required"))
	}
	output, status, _, err := c.DoWithHeaders(ctx, call.Method, call.Path, call.RequestID, "application/json", call.Payload, headers)
	if err != nil {
		return xrpc.Result{}, err
	}
	if status < 200 || status >= 300 {
		return xrpc.Result{}, xrpc.Failure(statusCode(status), xrpc.ResponseReceived, fmt.Errorf("HTTP status %d: %s", status, strings.TrimSpace(string(output))))
	}
	if len(output) == 0 {
		output = []byte("null")
	}
	if !json.Valid(output) {
		return xrpc.Result{}, xrpc.Failure("internal", xrpc.ResponseReceived, errors.New("xrpc: response is not JSON"))
	}
	return xrpc.Result{Status: status, Payload: output}, nil
}
func statusCode(status int) string {
	switch status {
	case 400:
		return "invalid_argument"
	case 404:
		return "not_found"
	case 409:
		return "conflict"
	case 413, 429, 431:
		return "resource_exhausted"
	case 408, 504:
		return "deadline_exceeded"
	case 499:
		return "cancelled"
	case 502, 503:
		return "unavailable"
	default:
		return "internal"
	}
}

// Profile caches clients by complete immutable reference. It does not discover
// endpoints or activate processes. Close releases all pooled connections.
type Profile struct {
	config  Config
	clients *refpool.Pool[xrpc.ServiceRef, *Client]
}

func NewProfile(config Config) *Profile {
	if config.Metrics == nil {
		config.Metrics = &xrpc.Metrics{}
	}
	return &Profile{config: config, clients: refpool.New[xrpc.ServiceRef, *Client](config.MaxReferences, config.ReferenceIdleTimeout, func(c *Client) { c.Close() })}
}
func (p *Profile) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	return p.CallWithHeaders(ctx, call, nil)
}
func (p *Profile) CallWithHeaders(ctx context.Context, call xrpc.Call, headers map[string]string) (xrpc.Result, error) {
	client, release, err := p.clients.AcquireContext(ctx, call.Service, func() (*Client, error) {
		config := p.config
		config.Service = call.Service
		return New(config)
	})
	if err != nil {
		code := "invalid_argument"
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			code = xrpc.Code(err)
		}
		if errors.Is(err, refpool.ErrFull) {
			code = "resource_exhausted"
		}
		if errors.Is(err, refpool.ErrClosed) {
			code = "unavailable"
		}
		return xrpc.Result{}, xrpc.Failure(code, xrpc.NotSent, err)
	}
	defer release()
	return client.CallWithHeaders(ctx, call, headers)
}
func (p *Profile) Close() {
	p.clients.Close()
}

func (p *Profile) Drained() <-chan struct{} { return p.clients.Drained() }
func (p *Profile) Shutdown(ctx context.Context) error {
	if _, err := xrpc.Remaining(ctx); err != nil {
		return err
	}
	p.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.Drained():
		return nil
	}
}

func (c *Client) Status() xrpc.TransportStatus {
	status := xrpc.TransportStatus{Time: time.Now().UTC(), Metrics: c.config.Metrics.Snapshot(), ConnectionLimit: c.config.MaxConnections, InFlightLimit: c.config.MaxInFlight, Stopping: c.owner.Err() != nil}
	if c.config.Diagnostics != nil {
		d := c.config.Diagnostics.Status()
		status.Diagnostics = &d
	}
	return status
}
func (p *Profile) Status() xrpc.TransportStatus {
	refs, active, capacity := p.clients.Snapshot()
	return xrpc.TransportStatus{Time: time.Now().UTC(), References: refs, ActiveReferences: active, ReferenceCapacity: capacity}
}
