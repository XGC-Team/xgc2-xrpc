package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-xrpc/go"
)

func TestSubscribeHTTPEventsUsesProductLoopbackOriginAndSharedResume(t *testing.T) {
	journal := newJournal()
	journal.append("run", `{"id":"earlier"}`)
	journal.append("run", `{"id":"current"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/events" || r.Header.Get("Last-Event-ID") != "1" {
			http.Error(w, "wrong endpoint or cursor", http.StatusBadRequest)
			return
		}
		_ = ServeEvents(w, r, journal, EventsOptions{})
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := SubscribeHTTPEvents(ctx, nil, server.URL, "/api/events", "1")
	if err != nil {
		t.Fatal(err)
	}
	event := next(t, events)
	if event.ID != "2" || event.Type != "run" || string(event.Data) != `{"id":"current"}` || event.Err != nil {
		t.Fatalf("event = %+v", event)
	}
	cancel()
	closed(t, events)
	// Ordinary HTTP ingress is not a new XRPC service-reference transport.
	if err := httpRef("http", server.URL).Validate(); err == nil {
		t.Fatal("loopback event ingress weakened ServiceRef validation")
	}
}

func TestSubscribeHTTPEventsRejectsUntrustedOriginsAndSharedInvalidArguments(t *testing.T) {
	for _, origin := range []string{
		"http://192.168.1.2:8788", "http://localhost:8788", "http://example.com",
		"https://user:secret@example.com", "https://example.com/path", "https://example.com?token=secret",
		"https://example.com#fragment", "https://example.com?", "https://example.com#", "file:///tmp/events", "https:///missing-host",
	} {
		t.Run(origin, func(t *testing.T) {
			if _, err := SubscribeHTTPEvents(t.Context(), nil, origin, "/events", ""); xrpc.Code(err) != "invalid_argument" {
				t.Fatalf("origin accepted: %v", err)
			}
		})
	}
	for _, test := range []struct {
		path, cursor string
		client       *http.Client
	}{
		{"https://other.example/events", "", nil},
		{"/events", "cursor\nheader", nil},
		{"/events", "", &http.Client{Timeout: time.Second}},
	} {
		if _, err := SubscribeHTTPEvents(t.Context(), test.client, "http://127.0.0.1:1", test.path, test.cursor); xrpc.Code(err) != "invalid_argument" {
			t.Fatalf("invalid event arguments accepted: %v", err)
		}
	}
}
