package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/config"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

func TestAdminHandler(t *testing.T) {
	var ready atomic.Bool
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "test_metrics 1")
	})
	handler := adminHandler(&ready, metrics, nil)
	for _, tt := range []struct {
		path   string
		status int
		body   string
	}{
		{"/healthz", http.StatusOK, "ok\n"},
		{"/readyz", http.StatusServiceUnavailable, "not ready\n"},
		{"/metrics", http.StatusOK, "test_metrics 1\n"},
		{"/other", http.StatusNotFound, "404 page not found\n"},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tt.path, nil))
		if response.Code != tt.status || response.Body.String() != tt.body {
			t.Errorf("%s = %d %q, want %d %q", tt.path, response.Code, response.Body.String(), tt.status, tt.body)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/version", nil))
	var info versionInfo
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil || response.Code != http.StatusOK || info.Version == "" {
		t.Fatalf("/version returned status %d, metadata %+v, decode error %v", response.Code, info, err)
	}
	response = httptest.NewRecorder()
	adminHandler(&ready, nil, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusNotFound {
		t.Errorf("disabled /metrics status = %d, want 404", response.Code)
	}
}

func TestUpstreamTransportPreservesCompression(t *testing.T) {
	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	if _, err := gzipWriter.Write([]byte("upstream response")); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Accept-Encoding", r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer upstream.Close()
	transport := upstreamTransport(config.Upstream{
		DialTimeout:           time.Second,
		ResponseHeaderTimeout: time.Second,
	})
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if encoding := response.Header.Get("X-Upstream-Accept-Encoding"); encoding != "" {
		t.Errorf("unexpected upstream Accept-Encoding %q", encoding)
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", encoding)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(body, compressed.Bytes()) {
		t.Fatalf("unexpected compressed response, read error %v", err)
	}
}

func TestProxyProtocolsAndGracefulShutdown(t *testing.T) {
	proxyListener := loopbackListener(t)
	adminListener := loopbackListener(t)
	proxyURL := "http://" + proxyListener.Addr().String()
	adminURL := "http://" + adminListener.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	releaseRequest := sync.OnceFunc(func() { close(release) })
	defer releaseRequest()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(started)
			<-release
		}
		_, _ = fmt.Fprintln(w, r.Proto)
	})
	done := make(chan error, 1)
	go func() {
		cfg := config.Server{ReadHeaderTimeout: time.Second, IdleTimeout: time.Second, ShutdownTimeout: 3 * time.Second}
		done <- serve(ctx, cfg, proxyListener, adminListener, httpDataServer(cfg, handler), nil)
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	if status, _, err := get(t.Context(), client, adminURL+"/readyz"); err != nil || status != http.StatusOK {
		t.Fatalf("startup /readyz status = %d, error = %v", status, err)
	}
	if status, body, err := get(t.Context(), client, proxyURL); err != nil || status != http.StatusOK || body != "HTTP/1.1\n" {
		t.Fatalf("HTTP/1 response = %d %q, error = %v", status, body, err)
	}
	protocols := &http.Protocols{}
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	defer transport.CloseIdleConnections()
	h2Client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	requestDone := make(chan error, 1)
	go func() {
		status, body, err := get(t.Context(), h2Client, proxyURL+"/slow")
		if err == nil && (status != http.StatusOK || body != "HTTP/2.0\n") {
			err = fmt.Errorf("HTTP/2 response = %d %q", status, body)
		}
		requestDone <- err
	}()
	select {
	case <-started:
	case err := <-requestDone:
		t.Fatalf("slow request ended before shutdown: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the slow request")
	}
	cancel()
	waitForUnready(t, client, adminURL)
	releaseRequest()
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTCPGracefulShutdown(t *testing.T) {
	upstreamListener := loopbackListener(t).(*net.TCPListener)
	proxyListener := loopbackListener(t)
	adminListener := loopbackListener(t)
	adminURL := "http://" + adminListener.Addr().String()
	cfg := testTCPConfig(proxyListener.Addr().String(), adminListener.Addr().String(), upstreamListener.Addr().String())
	observer := observability.New(nil)
	data, err := tcpDataServer(cfg, faultSettings(cfg.Chaos), observer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg.Server, proxyListener, adminListener, data, observer.MetricsHandler()) }()

	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	for _, path := range []string{"/readyz", "/healthz", "/version", "/metrics"} {
		if status, _, err := get(t.Context(), client, adminURL+path); err != nil || status != http.StatusOK {
			t.Fatalf("startup %s status = %d, error = %v", path, status, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	dialer := net.Dialer{Deadline: deadline}
	downstream, err := dialer.DialContext(t.Context(), "tcp", proxyListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = downstream.Close() }()
	if err := upstreamListener.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	upstream, err := upstreamListener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()
	for _, conn := range []net.Conn{downstream, upstream} {
		if err := conn.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
	}
	assertRelay(t, downstream, upstream, "\x00from client\xff")
	cancel()
	waitForUnready(t, client, adminURL)
	if status, _, err := get(t.Context(), client, adminURL+"/healthz"); err != nil || status != http.StatusOK {
		t.Fatalf("draining /healthz status = %d, error = %v", status, err)
	}
	assertRelay(t, upstream, downstream, "\xfffrom upstream\x00")
	_ = downstream.Close()
	_ = upstream.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for both listeners to stop")
	}
}

func assertRelay(t *testing.T, sender, receiver net.Conn, want string) {
	t.Helper()
	if _, err := io.WriteString(sender, want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(receiver, got); err != nil || string(got) != want {
		t.Fatalf("relayed bytes = %q, want %q, error = %v", got, want, err)
	}
}

func TestRunClosesProxyListenerOnAdminBindFailure(t *testing.T) {
	adminListener := loopbackListener(t)
	proxyListener := loopbackListener(t)
	proxyAddress := proxyListener.Addr().String()
	if err := proxyListener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := testTCPConfig(proxyAddress, adminListener.Addr().String(), "127.0.0.1:1")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	if err := Run(t.Context(), cfg, logger); err == nil || !strings.Contains(err.Error(), "listen for administrative requests") {
		t.Fatalf("Run returned %v, want admin bind error", err)
	}
	if logs.Len() != 0 {
		t.Errorf("startup logged before both listeners were bound: %s", &logs)
	}
	var listenConfig net.ListenConfig
	rebound, err := listenConfig.Listen(t.Context(), "tcp", proxyAddress)
	if err != nil {
		t.Fatalf("proxy address still in use after admin bind failure: %v", err)
	}
	_ = rebound.Close()
}

func TestServeStopsOtherListenerOnFailure(t *testing.T) {
	for _, name := range []string{"proxy", "admin"} {
		t.Run(name, func(t *testing.T) {
			proxyListener := loopbackListener(t)
			adminListener := loopbackListener(t)
			failed, other := proxyListener, adminListener
			if name == "admin" {
				failed, other = adminListener, proxyListener
			}
			if err := failed.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := testTCPConfig(proxyListener.Addr().String(), adminListener.Addr().String(), "127.0.0.1:1")
			data, err := tcpDataServer(cfg, faultSettings(cfg.Chaos), nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := serve(ctx, cfg.Server, proxyListener, adminListener, data, nil); err == nil {
				t.Fatalf("serve succeeded with a closed %s listener", name)
			}
			if ctx.Err() != nil {
				t.Fatal("serve waited for context timeout")
			}
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", other.Addr().String())
			if err == nil {
				_ = conn.Close()
				t.Fatal("the other listener still accepts connections")
			}
		})
	}
}

func testTCPConfig(proxyAddress, adminAddress, upstreamAddress string) config.Config {
	return config.Config{
		Server: config.Server{
			Mode:              config.ModeTCP,
			Listen:            proxyAddress,
			ReadHeaderTimeout: time.Second,
			IdleTimeout:       time.Second,
			ShutdownTimeout:   3 * time.Second,
		},
		Upstream: config.Upstream{
			URL:                   upstreamAddress,
			DialTimeout:           time.Second,
			ResponseHeaderTimeout: time.Second,
		},
		Chaos: config.Chaos{
			Limits:              config.Limits{MaxActiveFaults: 1},
			StripControlHeaders: true,
		},
		Observability: config.Observability{
			Listen:  adminAddress,
			Logging: config.Logging{Format: "text"},
		},
	}
}

func waitForUnready(t *testing.T, client *http.Client, adminURL string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, _, err := get(t.Context(), client, adminURL+"/readyz")
		if err != nil {
			t.Fatal(err)
		}
		if status == http.StatusServiceUnavailable {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shutdown /readyz status = %d, want 503", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func loopbackListener(t *testing.T) net.Listener {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func get(ctx context.Context, client *http.Client, url string) (int, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, string(body), err
}
