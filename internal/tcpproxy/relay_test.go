package tcpproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/observability"
)

const relayTestTimeout = 3 * time.Second

func TestRawRelayHalfClosesDuringGracePeriod(t *testing.T) {
	upstreamListener := relayTestListener(t)
	proxy := startRawRelay(t, upstreamListener, Options{}, relayTestTimeout, nil)
	client := relayTestDial(t, proxy.listener)
	upstream := relayTestAccept(t, upstreamListener)

	greeting := []byte{0, 255, 128}
	relayTestWrite(t, upstream, greeting)
	gotGreeting := make([]byte, len(greeting))
	if _, err := io.ReadFull(client, gotGreeting); err != nil || !bytes.Equal(gotGreeting, greeting) {
		t.Fatalf("server-first data = %v, %v", gotGreeting, err)
	}
	proxy.cancel()
	select {
	case <-proxy.listener.closed:
	case <-time.After(relayTestTimeout):
		t.Fatal("proxy kept accepting after cancellation")
	}
	select {
	case <-proxy.done:
		t.Fatal("proxy returned with an active connection")
	default:
	}

	request := []byte{255, 0, 200, 1}
	relayTestWrite(t, client, request)
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	forwarded, err := io.ReadAll(upstream)
	if err != nil || !bytes.Equal(forwarded, request) {
		t.Fatalf("request = %v, %v", forwarded, err)
	}
	response := []byte{0, 255, 2, 3}
	relayTestWrite(t, upstream, response)
	if err := upstream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	forwarded, err = io.ReadAll(client)
	if err != nil || !bytes.Equal(forwarded, response) {
		t.Fatalf("response = %v, %v", forwarded, err)
	}
	proxy.wait(t)
}

func TestRawRelayForcesStalledSessionsClosed(t *testing.T) {
	upstreamListener := relayTestListener(t)
	proxy := startRawRelay(t, upstreamListener, Options{}, 10*time.Millisecond, nil)
	client := relayTestDial(t, proxy.listener)
	upstream := relayTestAccept(t, upstreamListener)
	proxy.cancel()
	proxy.wait(t)
	for _, conn := range []*net.TCPConn{client, upstream} {
		if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Errorf("read after forced shutdown = %v, want EOF", err)
		}
	}
}

func TestRawRelayImmediateResetSkipsUpstream(t *testing.T) {
	upstreamListener := relayTestListener(t)
	opts := Options{ResetEnabled: true, Probability: 1, MaxActiveFaults: 1}
	proxy := startRawRelay(t, upstreamListener, opts, time.Second, nil)
	client, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), relayTestTimeout)
	if err == nil {
		defer func() { _ = client.Close() }()
		if err := client.SetReadDeadline(time.Now().Add(relayTestTimeout)); err != nil {
			t.Fatal(err)
		}
		var n int
		n, err = client.Read(make([]byte, 1))
		if n != 0 {
			t.Fatalf("immediate reset sent %d bytes", n)
		}
	}
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("immediate reset = %v", err)
	}
	assertRelayDidNotDial(t, upstreamListener)
}

func TestRawRelayResetThresholdAndLimits(t *testing.T) {
	upstreamListener := relayTestListener(t)
	observer := observability.New(nil)
	opts := Options{ResetEnabled: true, ResetAfterBytes: 3, Probability: 1, MaxActiveFaults: 1}
	proxy := startRawRelay(t, upstreamListener, opts, time.Second, observer)
	client := relayTestDial(t, proxy.listener)
	upstream := relayTestAccept(t, upstreamListener)
	limited := relayTestDial(t, proxy.listener)
	if n, err := limited.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("limited connection = %d bytes, %v", n, err)
	}
	assertRelayDidNotDial(t, upstreamListener)

	relayTestWrite(t, upstream, []byte("a"))
	var first [1]byte
	if _, err := io.ReadFull(client, first[:]); err != nil || first[0] != 'a' {
		t.Fatalf("data before reset = %q, %v", first, err)
	}
	relayTestWrite(t, upstream, []byte("bcdef"))
	remaining, err := io.ReadAll(client)
	if !errors.Is(err, syscall.ECONNRESET) || !strings.HasPrefix("bc", string(remaining)) {
		t.Fatalf("threshold data = %q, %v", remaining, err)
	}
	proxy.cancel()
	proxy.wait(t)
	metrics := httptest.NewRecorder()
	observer.MetricsHandler().ServeHTTP(metrics, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{
		`chaos_proxy_tcp_connections_total{outcome="limited"} 1`,
		`chaos_proxy_tcp_connections_total{outcome="reset"} 1`,
		`chaos_proxy_tcp_bytes_total{direction="upstream_to_client"} 3`,
	} {
		if !strings.Contains(metrics.Body.String(), want+"\n") {
			t.Errorf("missing metric %s", want)
		}
	}
}

func TestRawRelayPreservesAbruptUpstreamDisconnect(t *testing.T) {
	upstreamListener := relayTestListener(t)
	proxy := startRawRelay(t, upstreamListener, Options{ResetEnabled: true, Probability: 0}, time.Second, nil)
	client := relayTestDial(t, proxy.listener)
	upstream := relayTestAccept(t, upstreamListener)

	// Confirm the relay has completed its upstream dial before sending the reset.
	relayTestWrite(t, upstream, []byte("a"))
	var forwarded [1]byte
	if _, err := io.ReadFull(client, forwarded[:]); err != nil || forwarded[0] != 'a' {
		t.Fatalf("data before upstream reset = %q, %v", forwarded, err)
	}
	if err := resetTCP(upstream); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("upstream reset relayed as %v", err)
	}
}

func TestRelayDropsConnectionAcceptedDuringCancellation(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	if err := client.SetReadDeadline(time.Now().Add(relayTestTimeout)); err != nil {
		t.Fatal(err)
	}
	listener := &relayTestAcceptRace{conn: server, accepting: make(chan struct{}), closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	handled := false
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, listener, time.Second, func(context.Context, net.Conn) { handled = true })
	}()
	select {
	case <-listener.accepting:
	case <-time.After(relayTestTimeout):
		t.Fatal("listener never accepted")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil || handled {
			t.Fatalf("serve returned %v with handled = %t", err, handled)
		}
	case <-time.After(relayTestTimeout):
		t.Fatal("serve remained blocked in accept")
	}
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read from canceled connection = %v, want EOF", err)
	}
}

type relayTestAcceptRace struct {
	net.Listener
	conn      net.Conn
	accepting chan struct{}
	closed    chan struct{}
}

func (l *relayTestAcceptRace) Accept() (net.Conn, error) {
	close(l.accepting)
	<-l.closed
	return l.conn, nil
}

func (l *relayTestAcceptRace) Close() error {
	close(l.closed)
	return nil
}

type relayTestListenerState struct {
	*net.TCPListener
	closed chan struct{}
}

func (l *relayTestListenerState) Close() error {
	err := l.TCPListener.Close()
	close(l.closed)
	return err
}

type rawRelayTestServer struct {
	listener *relayTestListenerState
	cancel   context.CancelFunc
	done     chan struct{}
	err      error
}

func startRawRelay(t *testing.T, upstream *net.TCPListener, opts Options, shutdownTimeout time.Duration, observer *observability.Observer) *rawRelayTestServer {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	proxy := &rawRelayTestServer{
		listener: &relayTestListenerState{TCPListener: relayTestListener(t), closed: make(chan struct{})},
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	opts.Upstream = upstream.Addr().String()
	opts.DialTimeout = time.Second
	go func() {
		proxy.err = Serve(ctx, proxy.listener, opts, shutdownTimeout, observer)
		close(proxy.done)
	}()
	t.Cleanup(func() {
		cancel()
		proxy.wait(t)
	})
	return proxy
}

func (p *rawRelayTestServer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatal(p.err)
		}
	case <-time.After(relayTestTimeout):
		t.Fatal("TCP relay did not stop")
	}
}

func relayTestListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func relayTestDial(t *testing.T, listener net.Listener) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), relayTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(relayTestTimeout)); err != nil {
		t.Fatal(err)
	}
	return conn.(*net.TCPConn)
}

func relayTestAccept(t *testing.T, listener *net.TCPListener) *net.TCPConn {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(relayTestTimeout)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(relayTestTimeout)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func relayTestWrite(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	if n, err := conn.Write(data); err != nil || n != len(data) {
		t.Fatalf("write = %d bytes, %v", n, err)
	}
}

func assertRelayDidNotDial(t *testing.T, listener *net.TCPListener) {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if conn, err := listener.Accept(); err == nil {
		_ = conn.Close()
		t.Fatal("proxy unexpectedly dialed upstream")
	} else if networkError, ok := err.(net.Error); !ok || !networkError.Timeout() {
		t.Fatalf("accept = %v, want listener deadline", err)
	}
}
