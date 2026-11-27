package tcpproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

func TestConnectForwardsPipelinedPayloadAndHalfClose(t *testing.T) {
	broker := connectTestListener(t)
	settings := connectTestSettings()
	proxy, _, _ := startConnectTestProxy(t, broker, settings, time.Second, time.Second)
	target := broker.Addr().String()
	client := connectTestClient(t, proxy)
	payload := "pipelined Kafka request\x00\xff"
	connectTestWrite(t, client, connectTestRequest(target, "X-Chaos-Response-Delay-Ms: 30\r\n")+payload)
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(client)
	expectConnectStatus(t, reader, http.StatusOK)
	upstream := connectTestAccept(t, broker)
	connectTestReadAll(t, upstream, payload, nil)

	reply := "Kafka response\x00\xff"
	connectTestWrite(t, upstream, reply)
	if err := upstream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	connectTestReadAll(t, reader, reply, nil)
}

func TestConnectDelaysFirstBrokerPayloadOnly(t *testing.T) {
	const delay = 300 * time.Millisecond
	broker := connectTestListener(t)
	proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
	client := connectTestClient(t, proxy)
	reader := bufio.NewReader(client)
	if err := client.SetReadDeadline(time.Now().Add(delay / 2)); err != nil {
		t.Fatal(err)
	}
	connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "X-Chaos-Response-Delay-Ms: 300\r\n"))
	expectConnectStatus(t, reader, http.StatusOK)
	upstream := connectTestAccept(t, broker)
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	connectTestWrite(t, client, "request")
	connectTestReadString(t, upstream, "request")

	// The delay is measured from the first broker bytes, even for an idle tunnel.
	time.Sleep(delay)
	start := time.Now()
	connectTestWrite(t, upstream, "first")
	connectTestWrite(t, client, "second request")
	connectTestReadString(t, upstream, "second request")
	connectTestReadString(t, reader, "first")
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("first broker payload took %v, want at least %v", elapsed, delay)
	}
	if err := client.SetReadDeadline(time.Now().Add(delay / 2)); err != nil {
		t.Fatal(err)
	}
	connectTestWrite(t, upstream, "second")
	connectTestReadString(t, reader, "second")
}

func TestConnectResponseDelaySkipsEmptyUpstream(t *testing.T) {
	broker := connectTestListener(t)
	proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
	client := connectTestClient(t, proxy)
	reader := bufio.NewReader(client)
	connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "X-Chaos-Response-Delay-Ms: 3000\r\n"))
	expectConnectStatus(t, reader, http.StatusOK)
	upstream := connectTestAccept(t, broker)
	if err := upstream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	connectTestReadAll(t, reader, "", nil)
}

func TestConnectRejectsInvalidHandshake(t *testing.T) {
	broker := connectTestListener(t)
	settings := connectTestSettings()
	settings.Token = "test-token"
	proxy, _, _ := startConnectTestProxy(t, broker, settings, 250*time.Millisecond, time.Second)
	target := broker.Addr().String()
	token := "x-chaos-token: test-token\r\n"
	tests := []struct {
		name    string
		request string
		status  int
	}{
		{"wrong method", "GET / HTTP/1.1\r\nHost: " + target + "\r\n\r\n", http.StatusMethodNotAllowed},
		{"wrong authority", connectTestRequest("127.0.0.1:1", ""), http.StatusForbidden},
		{"URL target", connectTestRequest("tcp://"+target, ""), http.StatusBadRequest},
		{"wrong host", "CONNECT " + target + " HTTP/1.1\r\nHost: other.test:9092\r\n\r\n", http.StatusBadRequest},
		{"missing host", "CONNECT " + target + " HTTP/1.1\r\n\r\n", http.StatusBadRequest},
		{"duplicate host", connectTestRequest(target, "host: "+target+"\r\n"), http.StatusBadRequest},
		{"wrong version", "CONNECT " + target + " HTTP/1.0\r\nHost: " + target + "\r\n\r\n", http.StatusBadRequest},
		{"body length", connectTestRequest(target, "Content-Length: 1\r\n"), http.StatusBadRequest},
		{"transfer encoding", connectTestRequest(target, "Transfer-Encoding: chunked\r\n"), http.StatusBadRequest},
		{"missing token", connectTestRequest(target, "X-Chaos-Timeout-Ms: 1\r\n"), http.StatusForbidden},
		{"duplicate token", connectTestRequest(target, token+token), http.StatusForbidden},
		{"unknown control", connectTestRequest(target, token+"X-Chaos-Unknown: yes\r\n"), http.StatusBadRequest},
		{"duplicate delay", connectTestRequest(target, token+"X-Chaos-Request-Delay-Ms: 1\r\nx-ChAoS-rEqUeSt-DeLaY-mS: 2\r\n"), http.StatusBadRequest},
		{"folded control", connectTestRequest(target, token+"X-Chaos-Request-Delay-Ms:\r\n 1\r\n"), http.StatusBadRequest},
		{"space in name", connectTestRequest(target, token+"X-Chaos-Request-Delay-Ms : 1\r\n"), http.StatusBadRequest},
		{"unsupported status", connectTestRequest(target, token+"X-Chaos-Status-Code: 502\r\n"), http.StatusBadRequest},
		{"unsupported bandwidth", connectTestRequest(target, token+"X-Chaos-Request-Bandwidth-Kbps: 50\r\n"), http.StatusBadRequest},
		{"duplicate timeout", connectTestRequest(target, token+"X-Chaos-Timeout-Ms: 1\r\nx-chaos-timeout-ms: 1\r\n"), http.StatusBadRequest},
		{"timeout list", connectTestRequest(target, token+"X-Chaos-Timeout-Ms: 1, 2\r\n"), http.StatusBadRequest},
		{"negative timeout", connectTestRequest(target, token+"X-Chaos-Timeout-Ms: -1\r\n"), http.StatusBadRequest},
		{"timeout over configured limit", connectTestRequest(target, token+"X-Chaos-Timeout-Ms: 251\r\n"), http.StatusBadRequest},
		{"oversized headers", connectTestRequest(target, "X-Too-Large: "+strings.Repeat("A", maxConnectHeaderBytes)+"\r\n"), http.StatusRequestHeaderFieldsTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := connectTestClient(t, proxy)
			connectTestWrite(t, client, tt.request)
			expectConnectStatus(t, bufio.NewReader(client), tt.status)
		})
	}
	expectNoConnectDial(t, broker)
}

func TestConnectOrdinaryTrafficIgnoresUnselectedFaults(t *testing.T) {
	tests := []struct {
		name     string
		settings chaos.Settings
		headers  string
	}{
		{
			name:     "chaos off",
			settings: chaos.Settings{Token: "test-token"},
			headers:  "X-Chaos-Timeout-Ms: invalid\r\nx-chaos-timeout-ms: duplicate\r\nX-Chaos-Unknown: invalid\r\n",
		},
		{
			name:     "probability zero",
			settings: connectTestSettings(),
			headers:  "x-chaos-probability: 0\r\nx-chaos-timeout-ms: 1\r\nx-chaos-connection-reset: true\r\n",
		},
		{
			name:     "timeout zero",
			settings: connectTestSettings(),
			headers:  "x-chaos-timeout-ms: 0\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := connectTestListener(t)
			tt.settings.MaxActiveFaults = 0
			proxy, _, _ := startConnectTestProxy(t, broker, tt.settings, time.Second, time.Second)
			client := connectTestClient(t, proxy)
			connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), tt.headers))
			expectConnectStatus(t, bufio.NewReader(client), http.StatusOK)
			upstream := connectTestAccept(t, broker)
			connectTestWrite(t, client, "ping")
			got := make([]byte, 4)
			if _, err := io.ReadFull(upstream, got); err != nil || string(got) != "ping" {
				t.Fatalf("broker read (%q, %v), want (ping, nil)", got, err)
			}
		})
	}
}

func TestConnectAppliesRealReset(t *testing.T) {
	t.Run("before CONNECT response", func(t *testing.T) {
		broker := connectTestListener(t)
		proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "X-Chaos-Connection-Reset: true\r\n"))
		connectTestReadAll(t, client, "", syscall.ECONNRESET)
		expectNoConnectDial(t, broker)
	})

	t.Run("after exact number of broker bytes", func(t *testing.T) {
		broker := connectTestListener(t)
		settings := connectTestSettings()
		settings.ResetAfterBytes = 4
		proxy, _, _ := startConnectTestProxy(t, broker, settings, time.Second, time.Second)
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "x-chaos-connection-reset: TRUE\r\n"))
		reader := bufio.NewReader(client)
		expectConnectStatus(t, reader, http.StatusOK)
		upstream := connectTestAccept(t, broker)
		connectTestWrite(t, upstream, "123456789")
		connectTestReadAll(t, reader, "1234", syscall.ECONNRESET)
	})
}

func TestConnectLifetimeTimeout(t *testing.T) {
	t.Run("covers request delay", func(t *testing.T) {
		broker := connectTestListener(t)
		proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "X-Chaos-Request-Delay-Ms: 500\r\nX-Chaos-Timeout-Ms: 30\r\n"))
		expectConnectStatus(t, bufio.NewReader(client), http.StatusGatewayTimeout)
		expectNoConnectDial(t, broker)
	})

	t.Run("covers first broker payload delay", func(t *testing.T) {
		broker := connectTestListener(t)
		proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), "X-Chaos-Response-Delay-Ms: 900\r\nX-Chaos-Timeout-Ms: 300\r\n"))
		reader := bufio.NewReader(client)
		expectConnectStatus(t, reader, http.StatusOK)
		upstream := connectTestAccept(t, broker)
		connectTestWrite(t, client, "request")
		connectTestReadString(t, upstream, "request")
		connectTestWrite(t, upstream, "delayed response")
		connectTestReadAll(t, reader, "", nil)
		connectTestReadAll(t, upstream, "", nil)
	})

	t.Run("covers stalled tunnel and holds bounded fault slot", func(t *testing.T) {
		broker := connectTestListener(t)
		settings := connectTestSettings()
		settings.MaxActiveFaults = 1
		proxy, _, _ := startConnectTestProxy(t, broker, settings, time.Second, time.Second)
		target := broker.Addr().String()
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(target, "X-Chaos-Timeout-Ms: 700\r\n"))
		reader := bufio.NewReader(client)
		expectConnectStatus(t, reader, http.StatusOK)
		upstream := connectTestAccept(t, broker)

		limited := connectTestClient(t, proxy)
		connectTestWrite(t, limited, connectTestRequest(target, "X-Chaos-Connection-Reset: true\r\n"))
		expectConnectStatus(t, bufio.NewReader(limited), http.StatusTooManyRequests)

		connectTestReadAll(t, reader, "", nil)
		connectTestReadAll(t, upstream, "", nil)
	})
}

func TestConnectHeaderDeadlineAndPeerCancellation(t *testing.T) {
	t.Run("bounds partial headers", func(t *testing.T) {
		broker := connectTestListener(t)
		proxy, _, _ := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, 40*time.Millisecond)
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, "CONNECT "+broker.Addr().String()+" HTTP/1.1\r\n")
		expectConnectStatus(t, bufio.NewReader(client), http.StatusRequestTimeout)
	})

	t.Run("abandons pending request delay on peer close", func(t *testing.T) {
		broker := connectTestListener(t)
		settings := connectTestSettings()
		settings.MaxActiveFaults = 1
		observer := observability.New(nil)
		proxy, _, _ := startConnectTestProxyWithObserver(t, broker, settings, time.Second, time.Second, observer)
		client := connectTestClient(t, proxy)
		target := broker.Addr().String()
		connectTestWrite(t, client, connectTestRequest(target, "X-Chaos-Request-Delay-Ms: 3000\r\n"))
		waitForConnectActiveFaults(t, observer, 1)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		waitForConnectActiveFaults(t, observer, 0)
		expectNoConnectDial(t, broker)
	})

	t.Run("abandons pending response delay on peer reset", func(t *testing.T) {
		broker := connectTestListener(t)
		settings := connectTestSettings()
		settings.MaxActiveFaults = 1
		observer := observability.New(nil)
		proxy, _, _ := startConnectTestProxyWithObserver(t, broker, settings, time.Second, time.Second, observer)
		target := broker.Addr().String()
		client := connectTestClient(t, proxy)
		connectTestWrite(t, client, connectTestRequest(target, "X-Chaos-Response-Delay-Ms: 3000\r\n"))
		expectConnectStatus(t, bufio.NewReader(client), http.StatusOK)
		upstream := connectTestAccept(t, broker)
		connectTestWrite(t, upstream, "delayed response")
		waitForConnectActiveFaults(t, observer, 1)
		if err := client.SetLinger(0); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		waitForConnectActiveFaults(t, observer, 0)
		connectTestReadAll(t, upstream, "", nil)
	})
}

func TestConnectShutdownForceClosesStalledSession(t *testing.T) {
	broker := connectTestListener(t)
	proxy, stop, stopped := startConnectTestProxy(t, broker, connectTestSettings(), time.Second, time.Second)
	client := connectTestClient(t, proxy)
	connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), ""))
	reader := bufio.NewReader(client)
	expectConnectStatus(t, reader, http.StatusOK)
	upstream := connectTestAccept(t, broker)
	stop()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("proxy did not stop within the shutdown bound")
	}
	connectTestReadAll(t, reader, "", nil)
	connectTestReadAll(t, upstream, "", nil)
}

func connectTestSettings() chaos.Settings {
	return chaos.Settings{
		Enabled:                  true,
		RequestEnabled:           true,
		ResponseEnabled:          true,
		AbortEnabled:             true,
		ProbabilityEnabled:       true,
		RequestBandwidthEnabled:  true,
		ResponseBandwidthEnabled: true,
		ConnectionResetEnabled:   true,
		ResetAfterBytes:          0,
		MaxRequestDelay:          4 * time.Second,
		MaxResponseDelay:         4 * time.Second,
		DefaultProbability:       1,
		MaxActiveFaults:          4,
	}
}

func startConnectTestProxy(t *testing.T, broker net.Listener, settings chaos.Settings, maxTimeout, headerTimeout time.Duration) (string, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startConnectTestProxyWithObserver(t, broker, settings, maxTimeout, headerTimeout, nil)
}

func startConnectTestProxyWithObserver(t *testing.T, broker net.Listener, settings chaos.Settings, maxTimeout, headerTimeout time.Duration, observer *observability.Observer) (string, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	opts := Options{Upstream: broker.Addr().String(), DialTimeout: time.Second}
	proxy, err := NewConnectServer(opts, settings, maxTimeout, headerTimeout, observer)
	if err != nil {
		t.Fatal(err)
	}
	return serveConnectTestProxy(t, proxy)
}

func serveConnectTestProxy(t *testing.T, proxy *ConnectServer) (string, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	listener := connectTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := proxy.Serve(ctx, listener, 80*time.Millisecond); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Error("CONNECT proxy did not shut down")
		}
	})
	return listener.Addr().String(), cancel, stopped
}

func connectTestListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func connectTestClient(t *testing.T, address string) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn.(*net.TCPConn)
}

func connectTestAccept(t *testing.T, listener *net.TCPListener) *net.TCPConn {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func expectNoConnectDial(t *testing.T, listener *net.TCPListener) {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptTCP()
	if err == nil {
		_ = conn.Close()
		t.Fatal("unexpected dial to broker")
	}
	if !isNetworkTimeout(err) {
		t.Fatalf("expected timeout accepting connection, got %v", err)
	}
}

func connectTestRequest(target, headers string) string {
	return fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", target, target, headers)
}

func connectTestWrite(t *testing.T, writer io.Writer, payload string) {
	t.Helper()
	if _, err := io.WriteString(writer, payload); err != nil {
		t.Fatal(err)
	}
}

func connectTestReadAll(t *testing.T, reader io.Reader, want string, wantErr error) {
	t.Helper()
	got, err := io.ReadAll(reader)
	if !errors.Is(err, wantErr) || string(got) != want {
		t.Fatalf("read (%q, %v), want (%q, %v)", got, err, want, wantErr)
	}
}

func connectTestReadString(t *testing.T, reader io.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != want {
		t.Fatalf("read (%q, %v), want (%q, nil)", got, err, want)
	}
}

func waitForConnectActiveFaults(t *testing.T, observer *observability.Observer, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	metrics := observer.MetricsHandler()
	sample := "\nchaos_proxy_active_faults " + strconv.Itoa(want) + "\n"
	for time.Now().Before(deadline) {
		response := httptest.NewRecorder()
		metrics.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("metrics status = %d, want 200", response.Code)
		}
		if strings.Contains(response.Body.String(), sample) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active CONNECT faults did not reach %d", want)
}

func expectConnectStatus(t *testing.T, reader *bufio.Reader, status int) {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "HTTP/1.1 "+strconv.Itoa(status)+" ") {
		t.Fatalf("response line (%q, %v), want HTTP %d", line, err, status)
	}
	for line != "\r\n" {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
	}
}
