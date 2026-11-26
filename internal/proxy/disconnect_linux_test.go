package proxy_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/disconnect"
	"github.com/dkoshenkov/chaos-go/internal/observability"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

func TestHTTP1DisconnectWithUnreadBodyReleasesFaultSlot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		delayHeader   string
		contentLength int
		expectHeader  string
		body          string
		upstreamCalls int32
	}{
		{"request delay with complete upload", "X-Chaos-Request-Delay-Ms", 3, "", "abc", 0},
		{"response delay while waiting for 100 Continue", "X-Chaos-Response-Delay-Ms", 1024, "Expect: 100-continue\r\n", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/cancel" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				upstreamCalls.Add(1)
				// Reject the upload without making the transport send its body.
				w.Header().Set("Connection", "close")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
			}))
			t.Cleanup(upstream.Close)
			observer := observability.New(nil)
			settings := chaos.Settings{
				Enabled: true, RequestEnabled: true, ResponseEnabled: true,
				MaxRequestDelay: 5 * time.Second, MaxResponseDelay: 5 * time.Second, MaxActiveFaults: 1,
			}
			handler, err := proxy.NewHandler(upstream.URL, settings, nil, observer)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan struct{})
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cancel" {
					defer close(finished)
				}
				handler.ServeHTTP(w, r)
			}))
			server.Config.ConnContext = disconnect.ConnContext
			server.Start()
			t.Cleanup(server.Close)

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, "tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if _, err := fmt.Fprintf(conn, "POST /cancel HTTP/1.1\r\nHost: proxy\r\nContent-Length: %d\r\n%s: 5000\r\n%s\r\n%s", tc.contentLength, tc.delayHeader, tc.expectHeader, tc.body); err != nil {
				t.Fatal(err)
			}

			metrics := observer.MetricsHandler()
			probe := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				record := httptest.NewRecorder()
				metrics.ServeHTTP(record, probe)
				body := record.Body.String()
				if strings.Contains(body, "\nchaos_proxy_active_faults 1\n") && strings.Contains(body, fmt.Sprintf("\nchaos_proxy_upstream_duration_seconds_count %d\n", tc.upstreamCalls)) {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("proxy did not enter the injected delay")
				case <-ticker.C:
				}
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("disconnect did not interrupt the injected delay")
			}
			if got := upstreamCalls.Load(); got != tc.upstreamCalls {
				t.Fatalf("got %d upstream calls before cancellation, want %d", got, tc.upstreamCalls)
			}
			record := httptest.NewRecorder()
			metrics.ServeHTTP(record, probe)
			if body := record.Body.String(); !strings.Contains(body, "\nchaos_proxy_canceled_requests_total 1\n") || !strings.Contains(body, "\nchaos_proxy_active_faults 0\n") {
				t.Fatalf("metrics did not record the cancellation and release the slot:\n%s", body)
			}
		})
	}
}
