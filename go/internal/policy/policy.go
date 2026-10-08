// Package policy resolves the generated common registry against an explicit
// startup snapshot. It does not read the process environment or start workers.
package policy

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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
	Value         any    `json:"value"`
	Source        string `json:"source"`
	Detail        string `json:"source_detail,omitempty"`
	Dynamic       bool   `json:"dynamic"`
	Ceiling       int64  `json:"ceiling,omitempty"`
	Unit          string `json:"unit,omitempty"`
	Capability    string `json:"capability"`
	ParentValue   any    `json:"parent_value,omitempty"`
	ParentSource  string `json:"parent_source,omitempty"`
	ParentCeiling int64  `json:"parent_ceiling,omitempty"`
	RoleCap       int64  `json:"role_cap,omitempty"`
}

type Snapshot struct {
	Revision uint64                    `json:"revision"`
	Fields   map[string]EffectiveField `json:"fields"`
	Role     string                    `json:"role,omitempty"`
	RoleCaps map[string]int64          `json:"role_caps,omitempty"`
	Parent   *Snapshot                 `json:"parent,omitempty"`
}

// Policy holds an immutable startup declaration and a shared pointer to the
// parent's actually applied diagnostic revision. Role views have no updater or
// independent owner. Effective returns copies for an administrative interface.
type Policy struct {
	revision        uint64
	fields          map[string]EffectiveField
	current         *atomic.Pointer[Policy]
	role            string
	roleCaps        map[string]int64
	appliedFlag     atomic.Bool
	diagnosticClaim *atomic.Bool
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
	policy.current = &atomic.Pointer[Policy]{}
	policy.current.Store(policy)
	policy.appliedFlag.Store(true)
	policy.diagnosticClaim = &atomic.Bool{}
	return policy, nil
}

// applied reads one immutable revision; no owner locks or environment reads.
func (p *Policy) applied() *Policy {
	if p == nil || p.current == nil {
		return nil
	}
	current := p.current.Load()
	// Update returns a validated proposal before a Diagnostics owner applies
	// it. Its own snapshot remains readable; actual parent/view queries still
	// follow the published revision, and unpublished proposals cannot derive.
	if p.role == "" && p.revision > current.revision {
		return p
	}
	return current
}

func snapshot(p *Policy) Snapshot {
	result := Snapshot{Revision: p.revision, Fields: make(map[string]EffectiveField, len(p.fields))}
	for name, field := range p.fields {
		result.Fields[name] = field
	}
	return result
}

func (p *Policy) Effective() Snapshot {
	base := p.applied()
	if base == nil {
		return Snapshot{}
	}
	result := snapshot(base)
	if p.role == "" {
		return result
	}
	parent := snapshot(base)
	result.Parent = &parent
	result.Role = p.role
	result.RoleCaps = make(map[string]int64, len(p.roleCaps))
	for name, cap := range p.roleCaps {
		result.RoleCaps[name] = cap
	}
	for name, field := range result.Fields {
		field.ParentValue = field.Value
		field.ParentSource = field.Source
		field.ParentCeiling = field.Ceiling
		if cap, ok := p.roleCaps[name]; ok {
			field.RoleCap = cap
			if value := field.Value.(int64); cap < value {
				field.Value = cap
				field.Source = "role_cap"
			}
			if field.Ceiling == 0 {
				field.Ceiling = cap
			} else {
				field.Ceiling = min(field.Ceiling, cap)
			}
		}
		result.Fields[name] = field
	}
	return result
}

func (p *Policy) Integer(name string) (int64, error) {
	if p == nil {
		return 0, fmt.Errorf("xrpc: resolved runtime policy is required")
	}
	base := p.applied()
	if base == nil {
		return 0, fmt.Errorf("xrpc: resolved runtime policy is required")
	}
	field, ok := base.fields[name]
	value, integer := field.Value.(int64)
	if !ok || !integer {
		return 0, fmt.Errorf("xrpc: runtime policy field %s is not supported", name)
	}
	if cap, ok := p.roleCaps[name]; ok {
		value = min(value, cap)
	}
	return value, nil
}

// Update returns a new snapshot only for declared live fields, after revision
// validation. Logging verbosity can change live; transport fields require restart.
func (p *Policy) Update(expected uint64, updates map[string]string) (*Policy, error) {
	if p != nil && p.role != "" {
		return nil, fmt.Errorf("xrpc: role policy is read-only; update the parent diagnostics owner")
	}
	p = p.applied()
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
	result := &Policy{revision: p.revision + 1, fields: make(map[string]EffectiveField, len(p.fields)), current: p.current, diagnosticClaim: p.diagnosticClaim}
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

// Derive intersects supported numeric budgets after strict parent resolution.
// It never resolves another environment, changes enums or creates an owner.
func Derive(parent *Policy, role string, ceilings map[string]int64) (*Policy, error) {
	if parent == nil || parent.current == nil || parent.role != "" || !parent.appliedFlag.Load() {
		return nil, fmt.Errorf("xrpc: applied resolved parent policy required")
	}
	if len(role) == 0 || len(role) > 128 {
		return nil, fmt.Errorf("xrpc: canonical role identity required")
	}
	for _, c := range role {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:-", c)) {
			return nil, fmt.Errorf("xrpc: canonical role identity required")
		}
	}
	base := parent.current.Load()
	caps := make(map[string]int64, len(ceilings))
	for name, cap := range ceilings {
		var field Field
		found := false
		for _, known := range registry.Fields {
			if known.Name == name {
				field = known
				found = true
				break
			}
		}
		if !found || field.Type != "integer" {
			return nil, fmt.Errorf("xrpc: role ceiling requires a supported integer budget %s", name)
		}
		if _, ok := base.fields[name]; !ok {
			return nil, fmt.Errorf("xrpc: role ceiling requires a supported integer budget %s", name)
		}
		maximum := registry.IntegerMax
		if field.Maximum > 0 {
			maximum = min(maximum, field.Maximum)
		}
		if cap < 1 || cap > maximum {
			return nil, fmt.Errorf("xrpc: role ceiling exceeds numeric range %s", name)
		}
		caps[name] = cap
	}
	return &Policy{current: parent.current, role: role, roleCaps: caps, diagnosticClaim: parent.diagnosticClaim}, nil
}
func IsRole(p *Policy) bool { return p != nil && p.role != "" }

// PublishApplied is called only after the existing diagnostic owner applies
// verbosity. Staged Policy.Update proposals do not claim actual activation.
func PublishApplied(next *Policy, expected uint64) error {
	if next == nil || next.current == nil || next.role != "" {
		return fmt.Errorf("xrpc: applied parent policy required")
	}
	current := next.current.Load()
	if current.revision != expected {
		return fmt.Errorf("xrpc: runtime policy revision conflict")
	}
	if next.revision == expected {
		return nil
	}
	if next.revision != expected+1 || !next.current.CompareAndSwap(current, next) {
		return fmt.Errorf("xrpc: runtime policy revision conflict")
	}
	next.appliedFlag.Store(true)
	return nil
}

func ClaimDiagnostic(p *Policy) error {
	if p == nil || p.current == nil || p.role != "" || !p.appliedFlag.Load() || p.diagnosticClaim == nil {
		return fmt.Errorf("xrpc: applied parent diagnostic policy required")
	}
	if !p.diagnosticClaim.CompareAndSwap(false, true) {
		return fmt.Errorf("xrpc: parent diagnostics owner already exists")
	}
	return nil
}
func ReleaseDiagnostic(p *Policy) {
	if p != nil && p.diagnosticClaim != nil {
		p.diagnosticClaim.Store(false)
	}
}
func SameOwner(a, b *Policy) bool {
	return a != nil && b != nil && a.current != nil && a.current == b.current
}
