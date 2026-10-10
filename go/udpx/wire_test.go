package udpx

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

func testKey() []byte {
	key := make([]byte, KeyLen)
	for i := range key {
		key[i] = byte(i)
	}
	return key
}

func sequence(first byte) (out [16]byte) {
	for i := range out {
		out[i] = first + byte(i)
	}
	return out
}

// The vectors were produced by an independent implementation (Python hmac and
// struct) from the layout in the udp.v1 contract. Any other implementation of
// the profile must encode and verify the same bytes.
const (
	goldenRequest = "585255310101000100000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000005dc000c0007746573742e76312f4563686f7b2278223a317d09c76d589a3784cfc703d60ccc7e9ef946f765b0534b882e984df6eb47e68627"
	goldenReply   = "585255310102000000000007000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f00000004000000387b22636f6465223a227265736f757263655f657868617573746564222c226d657373616765223a226d222c2264657461696c73223a7b7d7dad367ae0cb0e986951e2e062f13293da29d6c591219f93a52d622ded4080c7e8"
)

func TestGoldenVectors(t *testing.T) {
	request := message{typ: typeRequest, flags: flagExpectedInstance, keyID: 7, id: sequence(0), instance: sequence(0x10), word: 1500, method: []byte("test.v1/Echo"), body: []byte(`{"x":1}`)}
	if got := hex.EncodeToString(appendDatagram(nil, &request, testKey())); got != goldenRequest {
		t.Fatalf("request datagram\n got %s\nwant %s", got, goldenRequest)
	}
	reply := message{typ: typeReply, keyID: 7, id: sequence(0), instance: sequence(0x10), word: uint32(StatusResourceExhausted), body: []byte(`{"code":"resource_exhausted","message":"m","details":{}}`)}
	if got := hex.EncodeToString(appendDatagram(nil, &reply, testKey())); got != goldenReply {
		t.Fatalf("reply datagram\n got %s\nwant %s", got, goldenReply)
	}
	for _, vector := range []string{goldenRequest, goldenReply} {
		raw, _ := hex.DecodeString(vector)
		if _, err := parse(raw); err != nil || !authentic(raw, testKey()) {
			t.Fatalf("golden vector rejected: %v", err)
		}
	}
}

func TestRoundTripAndTamper(t *testing.T) {
	request := message{typ: typeRequest, keyID: 0xdeadbeef, id: sequence(1), word: MaxTimeoutMS, method: []byte("a/B"), body: bytes.Repeat([]byte("x"), 300)}
	datagram := appendDatagram(nil, &request, testKey())
	parsed, err := parse(datagram)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.typ != typeRequest || parsed.keyID != 0xdeadbeef || parsed.id != request.id || parsed.word != MaxTimeoutMS || string(parsed.method) != "a/B" || !bytes.Equal(parsed.body, request.body) || parsed.expectedInstance() {
		t.Fatalf("round trip %+v", parsed)
	}
	if !authentic(datagram, testKey()) {
		t.Fatal("tag rejected")
	}
	other := testKey()
	other[0] ^= 1
	if authentic(datagram, other) {
		t.Fatal("tag verified under another key")
	}
	// Every single-bit change anywhere in the datagram must break verification
	// or parsing; the tag covers all preceding bytes, itself included.
	for i := range datagram {
		for bit := 0; bit < 8; bit++ {
			mutated := bytes.Clone(datagram)
			mutated[i] ^= 1 << bit
			if _, err := parse(mutated); err == nil && authentic(mutated, testKey()) {
				t.Fatalf("flipping bit %d of byte %d went unnoticed", bit, i)
			}
		}
	}
}

func TestParseRejectsMalformedDatagrams(t *testing.T) {
	valid := func() []byte {
		m := message{typ: typeRequest, keyID: 1, word: 10, method: []byte("m/x"), body: []byte("{}")}
		return appendDatagram(nil, &m, testKey())
	}
	reply := func() []byte {
		m := message{typ: typeReply, keyID: 1, body: []byte("{}")}
		return appendDatagram(nil, &m, testKey())
	}
	mutate := func(base func() []byte, edit func([]byte) []byte) []byte { return edit(base()) }
	cases := map[string][]byte{
		"empty":             nil,
		"shorter than min":  make([]byte, headerLen+tagLen-1),
		"larger than max":   append(valid(), make([]byte, MaxDatagram)...),
		"bad magic":         mutate(valid, func(d []byte) []byte { d[0] = 'Y'; return d }),
		"bad version":       mutate(valid, func(d []byte) []byte { d[4] = 2; return d }),
		"type zero":         mutate(valid, func(d []byte) []byte { d[5] = 0; return d }),
		"type three":        mutate(valid, func(d []byte) []byte { d[5] = 3; return d }),
		"unknown flag":      mutate(valid, func(d []byte) []byte { d[7] = 2; return d }),
		"flag on reply":     mutate(reply, func(d []byte) []byte { d[7] = 1; return d }),
		"zero method":       mutate(valid, func(d []byte) []byte { binary.BigEndian.PutUint16(d[48:], 0); return d }),
		"method too long":   mutate(valid, func(d []byte) []byte { binary.BigEndian.PutUint16(d[48:], MaxMethodLen+1); return d }),
		"method on reply":   mutate(reply, func(d []byte) []byte { binary.BigEndian.PutUint16(d[48:], 1); return d }),
		"body length long":  mutate(valid, func(d []byte) []byte { binary.BigEndian.PutUint16(d[50:], 3); return d }),
		"body length short": mutate(valid, func(d []byte) []byte { binary.BigEndian.PutUint16(d[50:], 1); return d }),
		"trailing byte":     append(valid(), 0),
		"missing byte":      valid()[:len(valid())-1],
	}
	for name, datagram := range cases {
		if _, err := parse(datagram); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := parse(valid()); err != nil {
		t.Fatal(err)
	}
	if _, err := parse(reply()); err != nil {
		t.Fatal(err)
	}
}

func TestLargestDatagramFits(t *testing.T) {
	method := strings.Repeat("m", MaxMethodLen)
	body := bytes.Repeat([]byte("b"), MaxPayload-len(method))
	m := message{typ: typeRequest, keyID: 1, word: 1, method: []byte(method), body: body}
	datagram := appendDatagram(nil, &m, testKey())
	if len(datagram) != MaxDatagram {
		t.Fatalf("len=%d", len(datagram))
	}
	if _, err := parse(datagram); err != nil {
		t.Fatal(err)
	}
	m.body = append(m.body, 'b')
	if _, err := parse(appendDatagram(nil, &m, testKey())); err == nil {
		t.Fatal("1201-byte datagram accepted")
	}
}

func TestStatusNames(t *testing.T) {
	want := []string{"ok", "invalid_argument", "not_found", "conflict", "resource_exhausted", "deadline_exceeded", "cancelled", "unavailable", "internal", "unauthenticated", "permission_denied"}
	for status, name := range want {
		if got := Status(status).Code(); got != name {
			t.Errorf("status %d is %q, want %q", status, got, name)
		}
	}
	if Status(11).Code() != "internal" || Status(1<<31).Code() != "internal" {
		t.Fatal("statuses outside the contract must read as internal")
	}
}

func TestMethodNames(t *testing.T) {
	for _, ok := range []string{"a", "xgc2.chassis.hold.v2/Engage", "test.v1/Echo", "é/ü", strings.Repeat("m", MaxMethodLen)} {
		if !validMethod(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "has space", "tab\t", "nul\x00", "del\x7f", "line\n", string([]byte{0xff, 0xfe}), strings.Repeat("m", MaxMethodLen+1)} {
		if validMethod(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
