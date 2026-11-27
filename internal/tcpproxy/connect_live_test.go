package tcpproxy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

func TestLiveConnectPauseBackpressureAndResume(t *testing.T) {
	observer := observability.New(nil)
	broker, address, control := startLiveConnectTestProxy(t, connectTestSettings(), observer)
	client, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
	connectTestWrite(t, client, "before pause")
	connectTestReadString(t, upstream, "before pause")

	setLiveConnectPolicy(t, control, http.MethodPut, connectPauseHeader, "true")
	waitForConnectActiveFaults(t, observer, 1)

	request := writeUntilBackpressure(t, client)
	response := writeUntilBackpressure(t, upstream)
	expectLiveConnectSilence(t, upstream, upstream)
	expectLiveConnectSilence(t, client, reader)

	setLiveConnectPolicy(t, control, http.MethodDelete, "", "")
	waitForConnectActiveFaults(t, observer, 0)
	for _, stream := range []struct {
		reader io.Reader
		want   []byte
	}{{upstream, request}, {reader, response}} {
		got, err := io.ReadAll(stream.reader)
		if err != nil || !bytes.Equal(got, stream.want) {
			t.Fatalf("after resume got %d bytes, want %d with matching contents, error %v", len(got), len(stream.want), err)
		}
	}
}

func TestLiveConnectPauseWithTimeout(t *testing.T) {
	broker, address, control := startLiveConnectTestProxy(t, connectTestSettings(), nil)
	if status := liveConnectPolicyStatus(control, http.MethodPut, http.Header{
		connectPauseHeader:   {"true"},
		connectTimeoutHeader: {"500"},
	}); status != http.StatusNoContent {
		t.Fatalf("pause with timeout returned %d, want 204", status)
	}
	client, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
	connectTestWrite(t, client, "paused request")
	connectTestWrite(t, upstream, "paused response")
	expectLiveConnectSilence(t, upstream, upstream)
	expectLiveConnectSilence(t, client, reader)
	connectTestReadAll(t, upstream, "", nil)
	connectTestReadAll(t, reader, "", nil)
}

func TestLiveConnectPauseValidatesHeaderAndAllowsReplacement(t *testing.T) {
	broker, address, control := startLiveConnectTestProxy(t, connectTestSettings(), nil)
	client, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
	setLiveConnectPolicy(t, control, http.MethodPut, connectPauseHeader, "TRUE")
	for _, values := range [][]string{{""}, {"1"}, {"true,false"}, {"true", "false"}} {
		if status := liveConnectPolicyStatus(control, http.MethodPut, http.Header{connectPauseHeader: values}); status != http.StatusBadRequest {
			t.Errorf("pause %q returned %d, want 400", values, status)
		}
	}
	connectTestWrite(t, upstream, "paused response")
	expectLiveConnectSilence(t, client, reader)
	setLiveConnectPolicy(t, control, http.MethodPut, connectPauseHeader, "false")
	connectTestReadString(t, reader, "paused response")
}

func TestLiveConnectPauseInterruptsAnInProgressWrite(t *testing.T) {
	dst, peer := net.Pipe()
	defer func() { _ = dst.Close() }()
	defer func() { _ = peer.Close() }()
	if err := peer.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	policies := &livePolicies{active: make(chan struct{}, 1), tunnels: make(map[*liveTunnel]struct{})}
	conn := liveConnectPipe{Conn: dst}
	tunnel := policies.attach(t.Context(), conn, conn, nil, false)
	defer policies.detach(tunnel)
	const prefix, suffix = "already written", "still pending"
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(&tunnel.upstream, prefix+suffix)
		done <- err
	}()
	connectTestReadString(t, peer, prefix)

	policies.update(livePolicy{policy: chaos.Policy{Probability: 1}, pause: true})
	expectLiveConnectSilence(t, peer, peer)
	policies.update(livePolicy{})
	connectTestReadString(t, peer, suffix)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type liveConnectPipe struct{ net.Conn }

func (liveConnectPipe) CloseWrite() error   { return nil }
func (liveConnectPipe) SetLinger(int) error { return nil }

func writeUntilBackpressure(t *testing.T, conn *net.TCPConn) []byte {
	t.Helper()
	if err := conn.SetWriteBuffer(8 << 10); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("TCP data\x00\xff"), 1<<18)
	n, err := conn.Write(payload)
	if n == 0 || n == len(payload) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected a partial write with backpressure, wrote %d of %d bytes, error %v", n, len(payload), err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	return payload[:n]
}

func expectLiveConnectSilence(t *testing.T, conn net.Conn, reader io.Reader) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if n, err := reader.Read(b[:]); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected a paused stream, read %d bytes, error %v", n, err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestLiveConnectReset(t *testing.T) {
	t.Run("idle tunnel", func(t *testing.T) {
		broker, address, control := startLiveConnectTestProxy(t, connectTestSettings(), nil)
		_, reader, _ := openLiveConnectTestTunnel(t, broker, address)
		setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Connection-Reset", "true")
		connectTestReadAll(t, reader, "", syscall.ECONNRESET)
	})

	t.Run("counts only bytes forwarded during activation", func(t *testing.T) {
		settings := connectTestSettings()
		settings.ResetAfterBytes = 4
		broker, address, control := startLiveConnectTestProxy(t, settings, nil)
		_, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
		connectTestWrite(t, upstream, "before activation")
		connectTestReadString(t, reader, "before activation")

		setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Connection-Reset", "true")
		connectTestWrite(t, upstream, "12")
		connectTestReadString(t, reader, "12")
		setLiveConnectPolicy(t, control, http.MethodDelete, "", "")
		connectTestWrite(t, upstream, "after deletion")
		connectTestReadString(t, reader, "after deletion")

		setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Connection-Reset", "true")
		connectTestWrite(t, upstream, "123456789")
		connectTestReadAll(t, reader, "1234", syscall.ECONNRESET)
	})
}

func TestLiveConnectReplacementCancelsTimeout(t *testing.T) {
	observer := observability.New(nil)
	broker, address, control := startLiveConnectTestProxy(t, connectTestSettings(), observer)
	_, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
	setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Timeout-Ms", "200")
	setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Response-Delay-Ms", "350")
	connectTestWrite(t, upstream, "response")
	connectTestReadString(t, reader, "response")

	setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Timeout-Ms", "60")
	connectTestReadAll(t, reader, "", nil)
	connectTestReadAll(t, upstream, "", nil)
	waitForConnectActiveFaults(t, observer, 0)
}

func TestLiveConnectFaultSlots(t *testing.T) {
	settings := connectTestSettings()
	settings.MaxActiveFaults = 1
	observer := observability.New(nil)
	broker, address, control := startLiveConnectTestProxy(t, settings, observer)
	_, firstReader, firstUpstream := openLiveConnectTestTunnel(t, broker, address)
	connectTestWrite(t, firstUpstream, "ready")
	connectTestReadString(t, firstReader, "ready")

	setLiveConnectPolicy(t, control, http.MethodPut, "X-Chaos-Response-Delay-Ms", "3000")
	_, reader, upstream := openLiveConnectTestTunnel(t, broker, address)
	waitForConnectActiveFaults(t, observer, 1)
	connectTestWrite(t, upstream, "slot unavailable")
	connectTestReadString(t, reader, "slot unavailable")

	setLiveConnectPolicy(t, control, http.MethodDelete, "", "")
	waitForConnectActiveFaults(t, observer, 0)
}

func startLiveConnectTestProxy(t *testing.T, settings chaos.Settings, observer *observability.Observer) (*net.TCPListener, string, http.Handler) {
	t.Helper()
	broker := connectTestListener(t)
	opts := Options{Upstream: broker.Addr().String(), DialTimeout: time.Second}
	proxy, err := NewConnectServer(opts, settings, time.Second, time.Second, observer)
	if err != nil {
		t.Fatal(err)
	}
	control := proxy.EnableLivePolicy()
	address, _, _ := serveConnectTestProxy(t, proxy)
	return broker, address, control
}

func openLiveConnectTestTunnel(t *testing.T, broker *net.TCPListener, address string) (*net.TCPConn, *bufio.Reader, *net.TCPConn) {
	t.Helper()
	client := connectTestClient(t, address)
	reader := bufio.NewReader(client)
	connectTestWrite(t, client, connectTestRequest(broker.Addr().String(), ""))
	expectConnectStatus(t, reader, http.StatusOK)
	return client, reader, connectTestAccept(t, broker)
}

func setLiveConnectPolicy(t *testing.T, handler http.Handler, method, header, value string) {
	t.Helper()
	headers := make(http.Header)
	if header != "" {
		headers.Set(header, value)
	}
	if status := liveConnectPolicyStatus(handler, method, headers); status != http.StatusNoContent {
		t.Fatalf("%s policy returned %d, want 204", method, status)
	}
}

func liveConnectPolicyStatus(handler http.Handler, method string, headers http.Header) int {
	request := httptest.NewRequest(method, "/connect/policy", nil)
	request.Header = headers
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code
}
