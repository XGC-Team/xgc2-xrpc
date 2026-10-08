package xrpc_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func TestCommonEnvironmentCorpus(t *testing.T) {
	raw, err := os.ReadFile("../contracts/fixtures/environment.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		SchemaVersion int `json:"schema_version"`
		Cases         []struct {
			Name         string                     `json:"name"`
			Environment  map[string]string          `json:"environment"`
			Defaults     map[string]string          `json:"defaults"`
			Ceilings     map[string]int64           `json:"ceilings"`
			Capabilities []string                   `json:"capabilities"`
			Values       map[string]json.RawMessage `json:"values"`
			Sources      map[string]string          `json:"sources"`
			ErrorField   string                     `json:"error_field"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.SchemaVersion != 1 || len(corpus.Cases) == 0 {
		t.Fatal("unsupported/empty environment corpus")
	}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			environment := make([]string, 0, len(test.Environment))
			for name, value := range test.Environment {
				environment = append(environment, name+"="+value)
			}
			policy, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: environment, Defaults: test.Defaults, Ceilings: test.Ceilings, Capabilities: test.Capabilities})
			if test.ErrorField != "" {
				if err == nil || !strings.Contains(err.Error(), test.ErrorField) {
					t.Fatalf("expected %s failure, got %v", test.ErrorField, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			effective := policy.Effective()
			if effective.Revision != 1 {
				t.Fatalf("revision=%d", effective.Revision)
			}
			for name, expected := range test.Values {
				actual, err := json.Marshal(effective.Fields[name].Value)
				if err != nil || string(actual) != string(expected) {
					t.Fatalf("%s=%s expected %s", name, actual, expected)
				}
				if source, ok := test.Sources[name]; ok && effective.Fields[name].Source != source {
					t.Fatalf("%s source=%s expected %s", name, effective.Fields[name].Source, source)
				}
			}
		})
	}
}

func TestPolicySnapshotAndRestartRevision(t *testing.T) {
	defaults := map[string]string{"HOST_MAX_CONNECTIONS": "7"}
	policy, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Defaults: defaults, DefaultSource: "product-safety", Environment: []string{"OTHER_SECRET=do-not-expose"}, Ceilings: map[string]int64{"HOST_MAX_CONNECTIONS": 8}})
	if err != nil {
		t.Fatal(err)
	}
	defaults["HOST_MAX_CONNECTIONS"] = "99"
	first := policy.Effective()
	field := first.Fields["HOST_MAX_CONNECTIONS"]
	if field.Value != int64(7) || field.Ceiling != 8 || field.Detail != "product-safety" {
		t.Fatalf("bad provenance: %+v", field)
	}
	first.Fields["HOST_MAX_CONNECTIONS"] = xrpc.EffectivePolicyField{Value: int64(99)}
	if second := policy.Effective(); second.Fields["HOST_MAX_CONNECTIONS"].Value != int64(7) {
		t.Fatal("snapshot mutation changed resolved policy")
	}
	if _, err := policy.Update(0, map[string]string{"HOST_MAX_CONNECTIONS": "8"}); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatal(err)
	}
	if _, err := policy.Update(1, map[string]string{"HOST_MAX_CONNECTIONS": "8"}); err == nil || !strings.Contains(err.Error(), "requires restart") {
		t.Fatal(err)
	}
	if _, err := xrpc.ResolvePolicy(xrpc.PolicyOptions{Environment: []string{"XGC2_XRPC_HOST_MAX_CONNECTIONS=7", "XGC2_XRPC_HOST_MAX_CONNECTIONS=8"}}); err == nil {
		t.Fatal("duplicate environment snapshot accepted")
	}
}
