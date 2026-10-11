package xrpc_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

const worldDescribe = `{"service":"gazebo-world","api_version":"v1","instance_id":"0123456789abcdef0123456789abcdef","ready":true,
"facts":{"world_generation":7,"capabilities":[{"name":"xgc2.chassis.hold","entities":["scout-1","scout-2"]},{"name":"xgc2.world.reset","entities":[]}]}}`

func TestDescribeListsCapabilitiesPerEntity(t *testing.T) {
	describe, err := xrpc.ParseDescribe([]byte(worldDescribe))
	if err != nil {
		t.Fatal(err)
	}
	if describe.Service != "gazebo-world" || describe.APIVersion != "v1" || !describe.Ready || string(describe.Facts["world_generation"]) != "7" {
		t.Fatalf("%+v", describe)
	}
	list, err := describe.Capabilities()
	if err != nil || len(list) != 2 || list[0].Name != "xgc2.chassis.hold" || len(list[0].Entities) != 2 || list[1].Entities == nil || len(list[1].Entities) != 0 {
		t.Fatalf("%+v %v", list, err)
	}
	for _, test := range []struct {
		capability, entity string
		want               bool
	}{
		{"xgc2.chassis.hold", "scout-2", true},
		{"xgc2.chassis.hold", "scout-3", false},
		{"xgc2.world.reset", "scout-1", false},
		{"xgc2.camera", "scout-1", false},
	} {
		if got, err := describe.Serves(test.capability, test.entity); err != nil || got != test.want {
			t.Errorf("%s/%s: %v %v", test.capability, test.entity, got, err)
		}
	}
	// A service without the key serves nothing.
	describe.Facts = map[string]json.RawMessage{"world_generation": json.RawMessage("7")}
	if list, err := describe.Capabilities(); err != nil || len(list) != 0 {
		t.Errorf("%+v %v", list, err)
	}
}

func TestParseDescribeIsStrict(t *testing.T) {
	for name, document := range map[string]string{
		"missing facts":    `{"service":"s","api_version":"v1","instance_id":"i","ready":true}`,
		"missing instance": `{"service":"s","api_version":"v1","instance_id":"","ready":true,"facts":{}}`,
		"bad instance":     `{"service":"s","api_version":"v1","instance_id":"a b","ready":true,"facts":{}}`,
		"missing service":  `{"service":"","api_version":"v1","instance_id":"i","ready":true,"facts":{}}`,
		"extra field":      `{"service":"s","api_version":"v1","instance_id":"i","ready":true,"facts":{},"extra":1}`,
		"ready not bool":   `{"service":"s","api_version":"v1","instance_id":"i","ready":"yes","facts":{}}`,
		"facts not object": `{"service":"s","api_version":"v1","instance_id":"i","ready":true,"facts":[]}`,
		"trailing data":    `{"service":"s","api_version":"v1","instance_id":"i","ready":true,"facts":{}} {}`,
		"not json":         `service`,
	} {
		if _, err := xrpc.ParseDescribe([]byte(document)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := xrpc.ParseDescribe([]byte(`{"service":"s","api_version":"v1","instance_id":"i","ready":false,"facts":{}}`)); err != nil {
		t.Error(err)
	}
}

func TestMalformedCapabilityFactsAreRejected(t *testing.T) {
	for name, facts := range map[string]string{
		"not a list":        `{"capabilities":{"name":"a.b"}}`,
		"bad name":          `{"capabilities":[{"name":"a/b","entities":[]}]}`,
		"empty name":        `{"capabilities":[{"name":"","entities":[]}]}`,
		"unknown field":     `{"capabilities":[{"name":"a.b","entities":[],"extra":1}]}`,
		"empty entity":      `{"capabilities":[{"name":"a.b","entities":[""]}]}`,
		"control character": `{"capabilities":[{"name":"a.b","entities":["x\ny"]}]}`,
		"entity too long":   `{"capabilities":[{"name":"a.b","entities":["` + strings.Repeat("e", 129) + `"]}]}`,
		"duplicate entity":  `{"capabilities":[{"name":"a.b","entities":["x","x"]}]}`,
		"duplicate name":    `{"capabilities":[{"name":"a.b","entities":[]},{"name":"a.b","entities":[]}]}`,
		"entities a string": `{"capabilities":[{"name":"a.b","entities":"x"}]}`,
	} {
		document := `{"service":"s","api_version":"v1","instance_id":"i","ready":true,"facts":` + facts + `}`
		describe, err := xrpc.ParseDescribe([]byte(document))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := describe.Capabilities(); err == nil {
			t.Errorf("%s accepted", name)
		}
		if _, err := describe.Serves("a.b", "x"); err == nil {
			t.Errorf("%s: Serves accepted", name)
		}
	}
}

func TestCapabilitiesJSON(t *testing.T) {
	value, err := xrpc.CapabilitiesJSON([]xrpc.Capability{{Name: "xgc2.chassis.hold", Entities: []string{"a", "b"}}, {Name: "xgc2.world.reset"}})
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != `[{"name":"xgc2.chassis.hold","entities":["a","b"]},{"name":"xgc2.world.reset","entities":[]}]` {
		t.Fatalf("%s", value)
	}
	// What a host builds, a caller reads back.
	document, _ := json.Marshal(map[string]any{"service": "s", "api_version": "v1", "instance_id": "i", "ready": true, "facts": map[string]json.RawMessage{xrpc.CapabilitiesFact: value}})
	describe, err := xrpc.ParseDescribe(document)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := describe.Serves("xgc2.chassis.hold", "b"); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	for name, bad := range map[string][]xrpc.Capability{
		"name":      {{Name: "bad name"}},
		"entity":    {{Name: "a.b", Entities: []string{""}}},
		"duplicate": {{Name: "a.b"}, {Name: "a.b"}},
		"repeat":    {{Name: "a.b", Entities: []string{"x", "x"}}},
	} {
		if _, err := xrpc.CapabilitiesJSON(bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if value, err := xrpc.CapabilitiesJSON(nil); err != nil || string(value) != "[]" {
		t.Errorf("no capabilities: %s %v", value, err)
	}
}
