package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/config"
)

func TestAdminHandler(t *testing.T) {
	var ready atomic.Bool
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "test_metrics 1")
	})
	handler := adminHandler(&ready, metrics)
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
	adminHandler(&ready, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
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
		done <- serve(ctx, cfg, proxyListener, adminListener, handler, nil)
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
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, _, err := get(t.Context(), client, adminURL+"/readyz")
		if err != nil {
			t.Fatal(err)
		}
		if status == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shutdown /readyz status = %d, want 503", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseRequest()
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
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
