package xrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Describe is the readiness envelope every service answers: identity, API
// version, the instance that is running, whether it is ready and the facts that
// make it valid. XRPC defines the envelope only; the meaning of the facts
// (a bound ROS master and its run id, a world generation, ...) belongs to the
// domain. http.v1 serves it at GET /v1/describe, grpc.v1 as the unary method
// Describe and udp.v1 as the method "<service>/Describe".
type Describe struct {
	Service    string                     `json:"service"`
	APIVersion string                     `json:"api_version"`
	InstanceID string                     `json:"instance_id"`
	Ready      bool                       `json:"ready"`
	Facts      map[string]json.RawMessage `json:"facts"`
}

// ParseDescribe decodes an envelope strictly: exactly the five fields, a
// nonempty identity, a boolean ready and a facts object.
func ParseDescribe(data []byte) (Describe, error) {
	var d Describe
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&d); err != nil {
		return Describe{}, fmt.Errorf("xrpc: describe: %w", err)
	}
	if decoder.More() {
		return Describe{}, errors.New("xrpc: describe: trailing data")
	}
	if d.Service == "" || d.APIVersion == "" || !ValidID(d.InstanceID) || d.Facts == nil {
		return Describe{}, errors.New("xrpc: describe: service, api_version, instance_id and facts are required")
	}
	return d, nil
}

// CapabilitiesFact is the facts key that lists capabilities.
const CapabilitiesFact = "capabilities"

// Capability says that a service serves the capability Name (the <service> part
// of its method names, for example xgc2.chassis.hold) for these entities.
// A service that serves one capability for several entities, such as a world
// host serving every chassis of its world, lists them here, so a caller can
// resolve "entity X, capability C" to a ServiceRef without knowing the domain.
type Capability struct {
	Name     string   `json:"name"`
	Entities []string `json:"entities"`
}

// MaxEntityBytes bounds one entity identity.
const MaxEntityBytes = 128

func (c Capability) validate() error {
	if !validServiceName(c.Name) || len(c.Name) > MaxMethodNameBytes {
		return fmt.Errorf("xrpc: capability name %q is not a service name", c.Name)
	}
	seen := make(map[string]bool, len(c.Entities))
	for _, entity := range c.Entities {
		if !validEntity(entity) {
			return fmt.Errorf("xrpc: capability %s: entity %q is not a valid identity", c.Name, entity)
		}
		if seen[entity] {
			return fmt.Errorf("xrpc: capability %s: duplicate entity %q", c.Name, entity)
		}
		seen[entity] = true
	}
	return nil
}

func validEntity(entity string) bool {
	if entity == "" || len(entity) > MaxEntityBytes || !utf8.ValidString(entity) {
		return false
	}
	for _, r := range entity {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// CapabilitiesJSON returns the value of the facts key CapabilitiesFact:
// [{"name":"<service>","entities":["<id>",...]},...]. A host adds it to its
// facts next to the domain's own.
func CapabilitiesJSON(capabilities []Capability) (json.RawMessage, error) {
	seen := make(map[string]bool, len(capabilities))
	out := make([]Capability, len(capabilities))
	for i, c := range capabilities {
		if err := c.validate(); err != nil {
			return nil, err
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("xrpc: duplicate capability %s", c.Name)
		}
		seen[c.Name] = true
		out[i] = Capability{Name: c.Name, Entities: append([]string{}, c.Entities...)}
	}
	return json.Marshal(out)
}

// Capabilities reads the capabilities the service lists in its facts. A
// service without the key serves none.
func (d Describe) Capabilities() ([]Capability, error) {
	raw, ok := d.Facts[CapabilitiesFact]
	if !ok {
		return nil, nil
	}
	var list []Capability
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&list); err != nil {
		return nil, fmt.Errorf("xrpc: describe facts %q: %w", CapabilitiesFact, err)
	}
	seen := make(map[string]bool, len(list))
	for i := range list {
		if err := list[i].validate(); err != nil {
			return nil, err
		}
		if seen[list[i].Name] {
			return nil, fmt.Errorf("xrpc: duplicate capability %s", list[i].Name)
		}
		seen[list[i].Name] = true
		if list[i].Entities == nil {
			list[i].Entities = []string{}
		}
	}
	return list, nil
}

// Serves reports whether the service lists the capability for the entity.
func (d Describe) Serves(capability, entity string) (bool, error) {
	list, err := d.Capabilities()
	if err != nil {
		return false, err
	}
	for _, c := range list {
		if c.Name != capability {
			continue
		}
		for _, e := range c.Entities {
			if e == entity {
				return true, nil
			}
		}
	}
	return false, nil
}
