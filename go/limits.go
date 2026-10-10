package xrpc

import "time"

// Default transport limits. Host and client option structs apply them when the
// matching field is zero; nothing here is read from the process environment.
// Limits bound SDK/transport-owned state, not allocations inside a domain
// handler.
const (
	// DefaultMaxConnections bounds accepted connections per host.
	DefaultMaxConnections = 32
	// DefaultMaxInFlight bounds admitted calls per host or client.
	DefaultMaxInFlight = 32
	// DefaultMaxHeaderBytes bounds decoded HTTP header fields and gRPC header lists.
	DefaultMaxHeaderBytes = 16 << 10
	// DefaultMaxMessageBytes bounds one request or response body/message.
	DefaultMaxMessageBytes = 1 << 20
	// DefaultCallTimeout is the longest call budget a host or client honors.
	DefaultCallTimeout = 30 * time.Second
	// DefaultHeaderTimeout bounds reading request headers and connection setup.
	DefaultHeaderTimeout = 5 * time.Second
	// DefaultIdleTimeout closes idle connections.
	DefaultIdleTimeout = 30 * time.Second
	// DefaultShutdownTimeout is the drain budget of RunEdge and the conformance fixture.
	DefaultShutdownTimeout = 5 * time.Second
	// DefaultClientConnections bounds HTTP connections per service reference.
	DefaultClientConnections = 16
	// DefaultMaxReferences bounds cached service references per profile client.
	DefaultMaxReferences = 64
	// DefaultReferenceIdleTimeout retires idle cached references.
	DefaultReferenceIdleTimeout = 30 * time.Second
	// DefaultStreamsPerConnection bounds concurrent native gRPC streams per connection.
	DefaultStreamsPerConnection = 32
)
