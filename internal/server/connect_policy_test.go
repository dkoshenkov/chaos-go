package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/config"
)

func TestConnectPolicyChangesAnOpenTunnel(t *testing.T) {
	broker := loopbackListener(t).(*net.TCPListener)
	proxyListener := loopbackListener(t)
	adminListener := loopbackListener(t)
	target := broker.Addr().String()
	proxyAddress := proxyListener.Addr().String()
	adminURL := "http://" + adminListener.Addr().String() + "/connect/policy"
	cfg := testTCPConfig(proxyAddress, adminListener.Addr().String(), target)
	cfg.Server.Mode = config.ModeConnect
	cfg.Chaos.Enabled = true
	cfg.Chaos.Token = "test-token"
	cfg.Chaos.RequestDelay = config.Delay{Enabled: true, Max: 10 * time.Second}
	cfg.Chaos.ResponseDelay = cfg.Chaos.RequestDelay
	data, err := tcpDataServer(cfg, faultSettings(cfg.Chaos), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg.Server, proxyListener, adminListener, data, nil) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	client, err := net.DialTimeout("tcp", proxyAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(client, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT response = %v, error = %v", response, err)
	}
	if err := broker.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	upstream, err := broker.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()
	if err := upstream.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	assertRelay(t, client, upstream, "before")

	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	update := func(method, token, delay string, want int) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, adminURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Chaos-Token", token)
		if delay != "" {
			request.Header.Set("X-Chaos-Request-Delay-Ms", delay)
			request.Header.Set("X-Chaos-Response-Delay-Ms", delay)
		}
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != want {
			t.Fatalf("%s policy returned %d, want %d", method, response.StatusCode, want)
		}
	}
	update(http.MethodPut, "", "5000", http.StatusForbidden)
	update(http.MethodPut, cfg.Chaos.Token, "5000", http.StatusNoContent)
	update(http.MethodPut, cfg.Chaos.Token, "invalid", http.StatusBadRequest)
	update(http.MethodDelete, "wrong", "", http.StatusForbidden)

	if _, err := io.WriteString(client, "request"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(upstream, "response"); err != nil {
		t.Fatal(err)
	}
	assertConnectReadBlocked(t, upstream, upstream)
	assertConnectReadBlocked(t, client, reader)

	update(http.MethodDelete, cfg.Chaos.Token, "", http.StatusNoContent)
	for _, stream := range []struct {
		reader io.Reader
		want   string
	}{{upstream, "request"}, {reader, "response"}} {
		got := make([]byte, len(stream.want))
		if _, err := io.ReadFull(stream.reader, got); err != nil || string(got) != stream.want {
			t.Fatalf("after DELETE got %q, error %v, want %q", got, err, stream.want)
		}
	}
	assertRelay(t, client, upstream, "after")
}

func assertConnectReadBlocked(t *testing.T, conn net.Conn, reader io.Reader) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if n, err := reader.Read(b[:]); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("live policy should delay traffic, read %d bytes with error %v", n, err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
