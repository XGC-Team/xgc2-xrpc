package udpx

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func b64(fill byte, n int) string {
	key := make([]byte, n)
	for i := range key {
		key[i] = fill + byte(i)
	}
	return base64.StdEncoding.EncodeToString(key)
}

func TestParseKeyRing(t *testing.T) {
	text := "# fleet keys\n\n7 " + b64(1, 32) + "\n  42\t" + b64(2, 32) + " # rotated in october\r\n   # indented comment\n4294967295 " + strings.TrimRight(b64(3, 32), "=") + "\n"
	ring, err := ParseKeyRing([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if ids := ring.IDs(); len(ids) != 3 || ids[0] != 7 || ids[1] != 42 || ids[2] != 4294967295 {
		t.Fatalf("ids=%v", ids)
	}
	key, ok := ring.lookup(7)
	want, _ := base64.StdEncoding.DecodeString(b64(1, 32))
	if !ok || string(key) != string(want) {
		t.Fatal("key 7 differs")
	}
	if _, ok := ring.lookup(8); ok {
		t.Fatal("unknown key found")
	}
	if _, _, ok := ring.sole(); ok {
		t.Fatal("three-key ring has a sole key")
	}
	single, err := ParseKeyRing([]byte("0 " + b64(9, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if id, _, ok := single.sole(); !ok || id != 0 {
		t.Fatal("sole key of a one-key ring")
	}
}

func TestParseKeyRingRejects(t *testing.T) {
	good := b64(1, 32)
	cases := map[string]string{
		"empty":            "",
		"only comments":    "# nothing\n\n",
		"missing key":      "7\n",
		"extra field":      "7 " + good + " extra\n",
		"negative id":      "-1 " + good + "\n",
		"id above u32":     "4294967296 " + good + "\n",
		"hex id":           "0x7 " + good + "\n",
		"empty id":         "x " + good + "\n",
		"short key":        "7 " + b64(1, 31) + "\n",
		"long key":         "7 " + b64(1, 33) + "\n",
		"not base64":       "7 !!!!\n",
		"repeated id":      "7 " + good + "\n7 " + b64(2, 32) + "\n",
		"key on next line": "7\n" + good + "\n",
		"oversized":        strings.Repeat("# padding padding padding padding\n", 3000),
	}
	for name, text := range cases {
		if _, err := ParseKeyRing([]byte(text)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	_, err := ParseKeyRing([]byte("7 " + b64(1, 31)))
	if err == nil || strings.Contains(err.Error(), b64(1, 31)) {
		t.Fatalf("error must name the line and not echo key material: %v", err)
	}
}

func TestLoadKeyRing(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "hold.keys")
	if err := os.WriteFile(path, []byte("# comment\n1 "+b64(5, 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ring, err := LoadKeyRing(path)
	if err != nil || len(ring.IDs()) != 1 {
		t.Fatalf("ring=%v err=%v", ring, err)
	}
	if _, err := LoadKeyRing(filepath.Join(directory, "missing")); err == nil {
		t.Fatal("missing file accepted")
	}
	if _, err := LoadKeyRing(directory); err == nil {
		t.Fatal("directory accepted")
	}
}

func TestNewKeyRingCopiesAndValidates(t *testing.T) {
	key := make([]byte, KeyLen)
	ring, err := NewKeyRing(map[uint32][]byte{1: key})
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 0xff
	if stored, _ := ring.lookup(1); stored[0] != 0 {
		t.Fatal("ring aliases the caller's key")
	}
	if _, err := NewKeyRing(nil); err == nil {
		t.Fatal("empty ring accepted")
	}
	if _, err := NewKeyRing(map[uint32][]byte{1: make([]byte, 16)}); err == nil {
		t.Fatal("short key accepted")
	}
}
