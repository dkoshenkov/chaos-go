package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestParseOptions(t *testing.T) {
	args := []string{"-proxy", "proxy:18080", "-target", "[::1]:9092"}
	cfg, err := parseOptions(append(args, "-header", "x-chaos-request-delay-ms: 10", "-header", "X-Chaos-Probability: 1"), "token-value", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:19092" || cfg.handshakeTimeout != 5*time.Minute || cfg.target != "[::1]:9092" {
		t.Fatalf("unexpected listen address, handshake timeout, or target")
	}
	if cfg.headers.Get("X-Chaos-Request-Delay-Ms") != "10" || cfg.headers.Get("X-Chaos-Probability") != "1" || cfg.headers.Get(tokenHeader) != "token-value" {
		t.Fatal("expected both control headers and the environment token")
	}
	for _, test := range []struct {
		name  string
		flags []string
		token string
	}{
		{"header injection", []string{"-header", "X-Chaos-Test: test-secret\r\nContent-Length: 1"}, ""},
		{"invalid field name", []string{"-header", "X-Chaos-Bad Name: test-secret"}, ""},
		{"arbitrary header", []string{"-header", "Host: test-secret"}, ""},
		{"token argument", []string{"-header", "x-chaos-token: test-secret"}, ""},
		{"token injection", nil, "test-secret\r\nContent-Length: 1"},
		{"URL target", []string{"-target", "tcp://test-secret:9092"}, ""},
		{"invalid timeout", []string{"-handshake-timeout", "0s"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseOptions(append(args, test.flags...), test.token, io.Discard)
			if err == nil || strings.Contains(err.Error(), "test-secret") {
				t.Fatal("expected a sanitized validation error")
			}
		})
	}
}

func TestAdapterConnectAndHalfClose(t *testing.T) {
	proxyListener := loopbackListener(t)
	adapterListener := loopbackListener(t)
	cfg, err := parseOptions([]string{
		"-proxy", proxyListener.Addr().String(),
		"-target", "broker:9092",
		"-header", "X-Chaos-Request-Delay-Ms: 10",
	}, "token-value", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	clientBytes := []byte{0, 1, 2, 3, 0xff, 0x80, '\r', '\n'}
	firstBytes := []byte{0, 'T', 'C', 'P', 0xff}
	finalBytes := []byte{0x88, '\r', '\n', 0}
	proxyDone := mockProxy(t, proxyListener, func(conn *net.TCPConn) error {
		reader := bufio.NewReader(conn)
		request, err := http.ReadRequest(reader)
		if err != nil {
			return err
		}
		if request.Method != http.MethodConnect || request.RequestURI != "broker:9092" || request.Host != "broker:9092" {
			return fmt.Errorf("unexpected CONNECT request: %s %s, Host %s", request.Method, request.RequestURI, request.Host)
		}
		if request.Header.Get("X-Chaos-Request-Delay-Ms") != "10" || request.Header.Get(tokenHeader) != "token-value" {
			return errors.New("CONNECT handshake missed a control header")
		}
		if _, err := conn.Write(append([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"), firstBytes...)); err != nil {
			return err
		}
		got, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, clientBytes) {
			return fmt.Errorf("forwarded bytes = %x, want %x", got, clientBytes)
		}
		if _, err := conn.Write(finalBytes); err != nil {
			return err
		}
		return conn.CloseWrite()
	})
	serveAdapter(t, cfg, adapterListener, io.Discard)
	client := dialAdapter(t, adapterListener)
	if _, err := client.Write(clientBytes); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(firstBytes, finalBytes...); !bytes.Equal(got, want) {
		t.Fatalf("received %x, want %x", got, want)
	}
	if err := <-proxyDone; err != nil {
		t.Fatal(err)
	}
}

func TestAdapterRejectsUnsuccessfulHandshake(t *testing.T) {
	for _, test := range []struct {
		name     string
		response string
	}{
		{"rejected", "HTTP/1.1 403 proxy-secret\r\nContent-Length: 11\r\n\r\nbody-secret"},
		{"invalid", "proxy-secret\r\n\r\n"},
		{"oversized", "HTTP/1.1 200 Connection Established\r\nX-Big: " + strings.Repeat("x", maxConnectResponseHeader) + "\r\n\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxyListener := loopbackListener(t)
			adapterListener := loopbackListener(t)
			cfg, err := parseOptions([]string{"-proxy", proxyListener.Addr().String(), "-target", "broker:9092"}, "env-secret", io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			proxyDone := mockProxy(t, proxyListener, func(conn *net.TCPConn) error {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return err
				}
				_, err := io.WriteString(conn, test.response)
				return err
			})
			var logs bytes.Buffer
			stop := serveAdapter(t, cfg, adapterListener, &logs)
			client := dialAdapter(t, adapterListener)
			got, err := io.ReadAll(client)
			if len(got) != 0 || !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("client received %x and error %v, want no bytes and connection reset", got, err)
			}
			stop()
			if strings.Contains(logs.String(), "secret") {
				t.Fatal("adapter logged CONNECT response details or token")
			}
			if err := <-proxyDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAdapterCancelsPendingHandshake(t *testing.T) {
	for _, reason := range []string{"adapter shutdown", "client disconnect", "handshake timeout"} {
		t.Run(reason, func(t *testing.T) {
			testCanceledHandshake(t, reason)
		})
	}
}

func testCanceledHandshake(t *testing.T, reason string) {
	t.Helper()
	proxyListener := loopbackListener(t)
	adapterListener := loopbackListener(t)
	cfg, err := parseOptions([]string{"-proxy", proxyListener.Addr().String(), "-target", "broker:9092"}, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if reason == "handshake timeout" {
		cfg.handshakeTimeout = 250 * time.Millisecond
	}
	requestRead := make(chan struct{})
	proxyDone := mockProxy(t, proxyListener, func(conn *net.TCPConn) error {
		reader := bufio.NewReader(conn)
		if _, err := http.ReadRequest(reader); err != nil {
			return err
		}
		close(requestRead)
		if _, err := reader.ReadByte(); err != io.EOF {
			return fmt.Errorf("proxy read after shutdown = %v, want EOF", err)
		}
		return nil
	})
	stop := serveAdapter(t, cfg, adapterListener, io.Discard)
	client := dialAdapter(t, adapterListener)
	select {
	case <-requestRead:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not read CONNECT request")
	}
	switch reason {
	case "client disconnect":
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	case "adapter shutdown":
		stop()
		if _, err := client.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("client read after shutdown = %v, want EOF", err)
		}
	case "handshake timeout":
		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("client read after handshake timeout = %v, want connection reset", err)
		}
	}
	if err := <-proxyDone; err != nil {
		t.Fatal(err)
	}
}

func TestAdapterRelaysUpstreamReset(t *testing.T) {
	proxyListener := loopbackListener(t)
	adapterListener := loopbackListener(t)
	cfg, err := parseOptions([]string{"-proxy", proxyListener.Addr().String(), "-target", "broker:9092"}, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	first := []byte{0x00, 0xff, 0x01}
	proxyDone := mockProxy(t, proxyListener, func(conn *net.TCPConn) error {
		reader := bufio.NewReader(conn)
		if _, err := http.ReadRequest(reader); err != nil {
			return err
		}
		if _, err := conn.Write(append([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"), first...)); err != nil {
			return err
		}
		if _, err := reader.ReadByte(); err != nil {
			return err
		}
		return conn.SetLinger(0)
	})
	serveAdapter(t, cfg, adapterListener, io.Discard)
	client := dialAdapter(t, adapterListener)
	got := make([]byte, len(first))
	if _, err := io.ReadFull(client, got); err != nil || !bytes.Equal(got, first) {
		t.Fatalf("initial bytes = %x, error = %v", got, err)
	}
	if _, err := client.Write([]byte{0x07}); err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(client)
	if len(got) != 0 || !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("client received %x and error %v after relay reset", got, err)
	}
	if err := <-proxyDone; err != nil {
		t.Fatal(err)
	}
}

func loopbackListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func mockProxy(t *testing.T, listener *net.TCPListener, handle func(*net.TCPConn) error) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			done <- err
			return
		}
		done <- handle(conn)
	}()
	return done
}

func serveAdapter(t *testing.T, cfg options, listener *net.TCPListener, logs io.Writer) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := cfg.serve(ctx, listener, log.New(logs, "", 0)); err != nil {
			t.Error(err)
		}
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("adapter did not shut down")
		}
	}
	t.Cleanup(stop)
	return stop
}

func dialAdapter(t *testing.T, listener *net.TCPListener) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn.(*net.TCPConn)
}
