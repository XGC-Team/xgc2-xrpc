package httpx

import "net/http"

// HeaderBytes bounds decoded field storage. Native net/http owns wire parsing
// (including its documented 4096-byte request parser allowance); it must not be
// replaced with a second parser just to count raw optional whitespace.
func headerBytes(header http.Header) int64 {
	var size int64
	for name, values := range header {
		for _, value := range values {
			size += int64(len(name) + len(value) + 4)
		}
	}
	return size
}
