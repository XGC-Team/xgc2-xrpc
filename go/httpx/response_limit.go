package httpx

import (
	"bufio"
	"net"
	"net/http"
)

// No response buffer is introduced: native writes remain streaming, and the
// count applies to actual body bytes (HEAD Content-Length is not a body).
type responseLimitWriter struct {
	http.ResponseWriter
	remaining      int64
	exceeded       bool
	committed      bool
	abort          bool
	head           bool
	maxHeaderBytes int64
	unlimitedBody  bool
	headerExceeded bool
}

func (w *responseLimitWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseLimitWriter) WriteHeader(code int) {
	if !w.checkHeaders() {
		return
	}
	if code >= 200 {
		w.committed = true
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *responseLimitWriter) Write(body []byte) (int, error) {
	if !w.checkHeaders() {
		return 0, ErrResponseTooLarge
	}
	if w.head {
		return w.ResponseWriter.Write(body)
	}
	if w.exceeded {
		return 0, ErrResponseTooLarge
	}
	if !w.unlimitedBody && int64(len(body)) > w.remaining {
		w.exceeded = true
		w.abort = w.committed
		if !w.committed {
			// The full body is not accepted, so the domain gets an explicit write
			// error even when the peer can receive the bounded transport error.
			if w.remaining >= 256 {
				writeError(w.ResponseWriter, 429, "resource_exhausted", "response exceeds host limit")
			} else {
				w.ResponseWriter.WriteHeader(429)
			}
		}
		return 0, ErrResponseTooLarge
	}
	w.committed = true
	n, err := w.ResponseWriter.Write(body)
	if !w.unlimitedBody {
		w.remaining -= int64(n)
	}
	return n, err
}
func (w *responseLimitWriter) Flush() {
	if !w.checkHeaders() {
		return
	}
	w.committed = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *responseLimitWriter) checkHeaders() bool {
	if w.headerExceeded {
		return false
	}
	if w.maxHeaderBytes <= 0 || headerBytes(w.Header()) <= w.maxHeaderBytes {
		return true
	}
	w.headerExceeded = true
	w.abort = w.committed
	if !w.committed {
		requestID, instanceID := w.Header().Get(RequestIDHeader), w.Header().Get(InstanceIDHeader)
		for key := range w.Header() {
			w.Header().Del(key)
		}
		if requestID != "" {
			w.Header().Set(RequestIDHeader, requestID)
		}
		if instanceID != "" {
			w.Header().Set(InstanceIDHeader, instanceID)
		}
		if headerBytes(w.Header()) > w.maxHeaderBytes {
			for key := range w.Header() {
				w.Header().Del(key)
			}
		}
		w.ResponseWriter.WriteHeader(429)
		w.committed = true
	}
	return false
}
func (w *responseLimitWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}
