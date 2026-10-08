package xrpc

import "time"

// TransportStatus is a maintained snapshot, never a health probe or domain
// completion claim. Unavailable observations are explicitly marked.
type TransportStatus struct {
	Time                 time.Time         `json:"time"`
	Metrics              MetricsSnapshot   `json:"metrics"`
	Connections          int               `json:"connections"`
	ConnectionsAvailable bool              `json:"connections_available"`
	ConnectionLimit      int               `json:"connection_limit"`
	InFlightLimit        int               `json:"in_flight_limit"`
	Stopping             bool              `json:"stopping"`
	Drained              bool              `json:"drained"`
	References           int               `json:"references"`
	ActiveReferences     int               `json:"active_references"`
	ReferenceCapacity    int               `json:"reference_capacity"`
	Diagnostics          *DiagnosticStatus `json:"diagnostics,omitempty"`
}
