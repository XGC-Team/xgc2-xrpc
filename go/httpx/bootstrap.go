package httpx

import (
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/XGC-Team/xgc2-xrpc/go"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
)

// ServeBound consumes the owner's existing listener/lease and resolved startup
// credentials. It neither binds nor starts a provider. Domain method/scope
// authorization remains in handler; this gate verifies the declared credential.
func ServeBound(listener net.Listener, lease *unixlease.Lease, handler http.Handler, credentials *xrpc.BootstrapCredentials, instanceID string, options HostOptions) (*Host, error) {
	if credentials == nil || credentials.Role() != xrpc.BootstrapServer || listener == nil {
		return nil, errors.New("xrpc: resolved server bootstrap required")
	}
	binding := credentials.Binding()
	if binding.Profile != xrpc.HTTP {
		return nil, errors.New("xrpc: HTTP bootstrap profile required")
	}
	if _, err := binding.ServiceRef(instanceID); err != nil {
		return nil, err
	}
	if options.InstanceID != "" && options.InstanceID != instanceID || options.Service != "" && options.Service != binding.Service || options.Authorize != nil {
		return nil, errors.New("xrpc: host options conflict with bootstrap owner")
	}
	options.InstanceID = instanceID
	options.Service = binding.Service
	options.Authorize = func(r *http.Request) bool {
		return credentials.Authorize(r.Context(), r.Header.Values("Authorization"))
	}
	if binding.Endpoint.Kind == "unix" {
		if lease == nil || lease.Path() != binding.Endpoint.Address || listener.Addr().Network() != "unix" {
			return nil, errors.New("xrpc: local bootstrap requires matching private endpoint lease")
		}
		if err := lease.ValidateListener(listener); err != nil {
			return nil, err
		}
		return Serve(listener, lease, handler, options)
	}
	if lease != nil || listener.Addr().Network() == "unix" {
		return nil, errors.New("xrpc: remote bootstrap requires native remote listener")
	}
	return ServeTLS(listener, handler, credentials.TLSConfig(), options)
}

// NewBound validates the complete actual ServiceRef against one startup grant
// snapshot. Config retains the transport owner's injected remote dialer/limits.
func NewBound(credentials *xrpc.BootstrapCredentials, ref xrpc.ServiceRef, config Config) (*Client, error) {
	if credentials == nil || credentials.Role() != xrpc.BootstrapClient || ref.Profile != xrpc.HTTP {
		return nil, errors.New("xrpc: resolved HTTP client bootstrap required")
	}
	if err := credentials.CheckReference(ref); err != nil {
		return nil, err
	}
	if config.Service != (xrpc.ServiceRef{}) && config.Service != ref || config.TLSConfig != nil || config.TLSForService != nil {
		return nil, errors.New("xrpc: client options conflict with bootstrap owner")
	}
	headers := make(map[string]string, len(config.Headers)+1)
	for key, value := range config.Headers {
		if strings.EqualFold(key, "Authorization") {
			return nil, errors.New("xrpc: authorization belongs to bootstrap owner")
		}
		headers[key] = value
	}
	for key, value := range credentials.Headers() {
		headers[key] = value
	}
	config.boundAuthorization = len(credentials.Headers()) != 0
	config.Headers = headers
	config.Service = ref
	config.TLSConfig = credentials.TLSConfig()
	return New(config)
}
