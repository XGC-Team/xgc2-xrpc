// Package policy resolves the generated common registry against an explicit
// startup snapshot. It does not read the process environment or start workers.
package policy

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

//go:embed registry.json
var registryJSON []byte

type Field struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Values     []string        `json:"values"`
	Default    json.RawMessage `json:"default"`
	Maximum    int64           `json:"maximum"`
	Dynamic    bool            `json:"dynamic"`
	Unit       string          `json:"unit"`
	Capability string          `json:"capability"`
}

var registry = func() struct {
	SchemaVersion int     `json:"schema_version"`
	Prefix        string  `json:"prefix"`
	IntegerMax    int64   `json:"integer_max"`
	Fields        []Field `json:"fields"`
} {
	var result struct {
		SchemaVersion int     `json:"schema_version"`
		Prefix        string  `json:"prefix"`
		IntegerMax    int64   `json:"integer_max"`
		Fields        []Field `json:"fields"`
	}
	if err := json.Unmarshal(registryJSON, &result); err != nil || result.SchemaVersion != 1 || result.Prefix != "XGC2_XRPC_" || result.IntegerMax != 2147483647 {
		panic("xrpc: invalid generated runtime policy registry")
	}
	return result
}()

// Options contains startup inputs. Environment must be explicitly snapshotted
// by the composition root (for example os.Environ()), never by this library.
type Options struct {
	Environment   []string
	Defaults      map[string]string
	DefaultSource string
	Ceilings      map[string]int64
	Capabilities  []string
}

type EffectiveField struct {
	Value      any    `json:"value"`
	Source     string `json:"source"`
	Detail     string `json:"source_detail,omitempty"`
	Dynamic    bool   `json:"dynamic"`
	Ceiling    int64  `json:"ceiling,omitempty"`
	Unit       string `json:"unit,omitempty"`
	Capability string `json:"capability"`
}

type Snapshot struct {
	Revision uint64                    `json:"revision"`
	Fields   map[string]EffectiveField `json:"fields"`
}

// Policy is immutable. Snapshot returns a copy, safe to expose through an
// owner's existing authenticated administrative interface.
type Policy struct {
	revision uint64
	fields   map[string]EffectiveField
}

var supported = map[string]bool{
	"host": true, "http": true, "rpc": true, "transport": true,
	"client_pool": true, "client_registry": true, "grpc": true,
	"diagnostics": true,
}

func parse(field Field, raw string) (any, error) {
	if field.Type == "enum" {
		for _, value := range field.Values {
			if value == raw {
				return raw, nil
			}
		}
		return nil, fmt.Errorf("xrpc: %s requires an exact declared enum token", field.Name)
	}
	if len(raw) == 0 || len(raw) > 10 || raw[0] < '1' || raw[0] > '9' {
		return nil, fmt.Errorf("xrpc: %s requires a canonical positive decimal integer", field.Name)
	}
	for i := 1; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return nil, fmt.Errorf("xrpc: %s requires a canonical positive decimal integer", field.Name)
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	maximum := registry.IntegerMax
	if field.Maximum > 0 {
		maximum = min(maximum, field.Maximum)
	}
	if err != nil || value > maximum {
		return nil, fmt.Errorf("xrpc: %s exceeds maximum %d", field.Name, maximum)
	}
	return value, nil
}

func Resolve(options Options) (*Policy, error) {
	caps := make(map[string]bool)
	if options.Capabilities == nil {
		for capability := range supported {
			caps[capability] = true
		}
	} else {
		for _, capability := range options.Capabilities {
			if !supported[capability] {
				return nil, fmt.Errorf("xrpc: unsupported runtime policy capability %s", capability)
			}
			caps[capability] = true
		}
	}
	known := make(map[string]Field, len(registry.Fields))
	for _, field := range registry.Fields {
		known[field.Name] = field
	}
	check := func(name string) (Field, error) {
		field, ok := known[name]
		if !ok {
			return field, fmt.Errorf("xrpc: unknown runtime policy field %s", name)
		}
		if !caps[field.Capability] {
			return field, fmt.Errorf("xrpc: unsupported runtime policy field %s", name)
		}
		return field, nil
	}
	environment := make(map[string]string)
	for _, item := range options.Environment {
		name, value, hasValue := strings.Cut(item, "=")
		if !strings.HasPrefix(name, registry.Prefix) {
			continue
		}
		if _, duplicate := environment[name]; duplicate {
			return nil, fmt.Errorf("xrpc: duplicate environment name %s", name)
		}
		if !hasValue {
			return nil, fmt.Errorf("xrpc: missing value for %s", name)
		}
		short := strings.TrimPrefix(name, registry.Prefix)
		if _, err := check(short); err != nil {
			return nil, fmt.Errorf("%w (%s)", err, name)
		}
		environment[name] = value
	}
	for name := range options.Defaults {
		if _, err := check(name); err != nil {
			return nil, err
		}
	}
	for name, ceiling := range options.Ceilings {
		field, err := check(name)
		if err != nil {
			return nil, err
		}
		if field.Type != "integer" || ceiling <= 0 || ceiling > registry.IntegerMax {
			return nil, fmt.Errorf("xrpc: invalid ceiling for %s", name)
		}
	}
	policy := &Policy{revision: 1, fields: make(map[string]EffectiveField)}
	for _, field := range registry.Fields {
		if !caps[field.Capability] {
			continue
		}
		raw := string(field.Default)
		if field.Type == "enum" {
			if err := json.Unmarshal(field.Default, &raw); err != nil {
				return nil, fmt.Errorf("xrpc: invalid registry default for %s", field.Name)
			}
		}
		source, detail := "sdk_default", ""
		if value, ok := options.Defaults[field.Name]; ok {
			raw, source, detail = value, "deployment", options.DefaultSource
		}
		if value, ok := environment[registry.Prefix+field.Name]; ok {
			raw, source, detail = value, "environment", ""
		}
		value, err := parse(field, raw)
		if err != nil {
			return nil, err
		}
		ceiling := options.Ceilings[field.Name]
		if n, ok := value.(int64); ok && ceiling > 0 && n > ceiling {
			return nil, fmt.Errorf("xrpc: %s exceeds declared ceiling %d", field.Name, ceiling)
		}
		policy.fields[field.Name] = EffectiveField{Value: value, Source: source, Detail: detail, Dynamic: field.Dynamic, Ceiling: ceiling, Unit: field.Unit, Capability: field.Capability}
	}
	return policy, nil
}

func (p *Policy) Effective() Snapshot {
	if p == nil {
		return Snapshot{}
	}
	result := Snapshot{Revision: p.revision, Fields: make(map[string]EffectiveField, len(p.fields))}
	for name, field := range p.fields {
		result.Fields[name] = field
	}
	return result
}

func (p *Policy) Integer(name string) (int64, error) {
	if p == nil {
		return 0, fmt.Errorf("xrpc: resolved runtime policy is required")
	}
	field, ok := p.fields[name]
	value, integer := field.Value.(int64)
	if !ok || !integer {
		return 0, fmt.Errorf("xrpc: runtime policy field %s is not supported", name)
	}
	return value, nil
}

// Update returns a new snapshot only for declared live fields, after revision
// validation. Logging verbosity can change live; transport fields require restart.
func (p *Policy) Update(expected uint64, updates map[string]string) (*Policy, error) {
	if p == nil || expected != p.revision {
		return nil, fmt.Errorf("xrpc: runtime policy revision conflict")
	}
	if len(updates) == 0 {
		return p, nil
	}
	if p.revision == ^uint64(0) {
		return nil, fmt.Errorf("xrpc: runtime policy revision exhausted")
	}
	for name := range updates {
		field, ok := p.fields[name]
		if !ok {
			return nil, fmt.Errorf("xrpc: unsupported runtime policy field %s", name)
		}
		if !field.Dynamic {
			return nil, fmt.Errorf("xrpc: runtime policy field %s requires restart", name)
		}
	}
	result := &Policy{revision: p.revision + 1, fields: make(map[string]EffectiveField, len(p.fields))}
	for name, field := range p.fields {
		result.fields[name] = field
	}
	for name, raw := range updates {
		var declaration Field
		for _, field := range registry.Fields {
			if field.Name == name {
				declaration = field
				break
			}
		}
		value, err := parse(declaration, raw)
		if err != nil {
			return nil, err
		}
		field := result.fields[name]
		field.Value = value
		field.Source = "administrative"
		field.Detail = ""
		result.fields[name] = field
	}
	return result, nil
}
