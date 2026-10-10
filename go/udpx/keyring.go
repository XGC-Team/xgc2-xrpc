package udpx

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// maxKeyRingBytes bounds a key ring file; a ring holds a handful of keys.
const maxKeyRingBytes = 64 << 10

// KeyRing maps key identities to 32-byte HMAC keys. A key identity is a public
// label that travels in every datagram; the key itself never does. A KeyRing is
// immutable after construction and safe for concurrent use.
type KeyRing struct {
	keys map[uint32][]byte
}

// NewKeyRing copies keys into a ring. Every key must be KeyLen bytes.
func NewKeyRing(keys map[uint32][]byte) (*KeyRing, error) {
	if len(keys) == 0 {
		return nil, errors.New("udpx: key ring needs at least one key")
	}
	ring := &KeyRing{keys: make(map[uint32][]byte, len(keys))}
	for id, key := range keys {
		if len(key) != KeyLen {
			return nil, fmt.Errorf("udpx: key %d must be %d bytes", id, KeyLen)
		}
		ring.keys[id] = bytes.Clone(key)
	}
	return ring, nil
}

// ParseKeyRing reads the key ring text format shared with the C++ library: one
// "<key_id> <base64 key>" pair per line, where key_id is a decimal u32 and the
// key decodes to 32 bytes. Blank lines and "#" comments (whole-line or trailing)
// are ignored; a repeated key_id is an error.
func ParseKeyRing(data []byte) (*KeyRing, error) {
	if len(data) > maxKeyRingBytes {
		return nil, errors.New("udpx: key ring is too large")
	}
	keys := make(map[uint32][]byte)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 4096), maxKeyRingBytes)
	for line := 1; scanner.Scan(); line++ {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if len(fields) < 2 || len(fields) > 2 && !strings.HasPrefix(fields[2], "#") {
			return nil, fmt.Errorf("udpx: key ring line %d: expected \"<key_id> <base64 key>\"", line)
		}
		id, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("udpx: key ring line %d: key_id must be a decimal u32", line)
		}
		key, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			key, err = base64.RawStdEncoding.DecodeString(fields[1])
		}
		if err != nil || len(key) != KeyLen {
			return nil, fmt.Errorf("udpx: key ring line %d: key must be base64 of %d bytes", line, KeyLen)
		}
		if _, duplicate := keys[uint32(id)]; duplicate {
			return nil, fmt.Errorf("udpx: key ring line %d: key_id %d repeated", line, id)
		}
		keys[uint32(id)] = key
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("udpx: key ring: %w", err)
	}
	return NewKeyRing(keys)
}

// LoadKeyRing reads and parses a key ring file. The file is secret material:
// keep it readable by the service account only.
func LoadKeyRing(path string) (*KeyRing, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("udpx: key ring must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxKeyRingBytes+1))
	if err != nil {
		return nil, err
	}
	return ParseKeyRing(data)
}

// IDs returns the key identities in ascending order.
func (r *KeyRing) IDs() []uint32 {
	ids := make([]uint32, 0, len(r.keys))
	for id := range r.keys {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (r *KeyRing) lookup(id uint32) ([]byte, bool) {
	key, ok := r.keys[id]
	return key, ok
}

// sole returns the only key of a one-key ring.
func (r *KeyRing) sole() (uint32, []byte, bool) {
	if len(r.keys) != 1 {
		return 0, nil, false
	}
	for id, key := range r.keys {
		return id, key, true
	}
	return 0, nil, false
}
