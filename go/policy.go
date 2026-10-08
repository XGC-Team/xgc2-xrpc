package xrpc

import "github.com/XGC-Team/xgc2-xrpc/go/internal/policy"

type Policy = policy.Policy
type PolicyOptions = policy.Options
type EffectivePolicy = policy.Snapshot
type EffectivePolicyField = policy.EffectiveField

// ResolvePolicy consumes exactly the caller-provided startup snapshot.
func ResolvePolicy(options PolicyOptions) (*Policy, error) { return policy.Resolve(options) }

// DerivePolicy creates a read-only numeric intersection of one applied parent.
// It shares the parent's actual revision and diagnostics owner.
func DerivePolicy(parent *Policy, role string, ceilings map[string]int64) (*Policy, error) {
	return policy.Derive(parent, role, ceilings)
}

var sdkDefaults = func() *Policy {
	p, err := policy.Resolve(policy.Options{})
	if err != nil {
		panic(err)
	}
	return p
}()

// DefaultPolicyInteger reads a generated SDK default without reading getenv.
func DefaultPolicyInteger(name string) int64 {
	value, err := sdkDefaults.Integer(name)
	if err != nil {
		panic(err)
	}
	return value
}
