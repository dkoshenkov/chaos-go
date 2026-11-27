package tcpproxy

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

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
	request := httptest.NewRequest(method, "/connect/policy", nil)
	if header != "" {
		request.Header.Set(header, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("%s policy returned %d, want 204", method, response.Code)
	}
}
