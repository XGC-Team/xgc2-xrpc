package xrpc

import (
	"errors"
	"strconv"
)

// ValidID implements the common request/instance identity alphabet. It is
// intentionally narrower than an HTTP header value, including in gRPC metadata.
func ValidID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := range value {
		c := value[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

// ParseTimeoutMS accepts only the shared canonical finite wire budget.
func ParseTimeoutMS(value string) (int64, error) {
	if len(value) == 0 || len(value) > 8 || value[0] < '1' || value[0] > '9' {
		return 0, errors.New("xrpc: timeout must be canonical decimal 1..86400000")
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, errors.New("xrpc: timeout must be canonical decimal 1..86400000")
		}
	}
	valueMS, err := strconv.ParseInt(value, 10, 64)
	if err != nil || valueMS > 86400000 {
		return 0, errors.New("xrpc: timeout must be canonical decimal 1..86400000")
	}
	return valueMS, nil
}
