package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/disconnect"
	"github.com/dkoshenkov/chaos-go/internal/observability"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

const (
	requestDelay  = "X-Chaos-Request-Delay-Ms"
	responseDelay = "X-Chaos-Response-Delay-Ms"
	tokenHeader   = "X-Chaos-Token"
)

func faultSettings() chaos.Settings {
	return chaos.Settings{
		Enabled:          true,
		RequestEnabled:   true,
		ResponseEnabled:  true,
		MaxRequestDelay:  10 * time.Second,
		MaxResponseDelay: 10 * time.Second,
		MaxActiveFaults:  1,
	}
}

func loopbackProxy(t *testing.T, upstream *httptest.Server, settings chaos.Settings, transport http.RoundTripper) http.Handler {
	t.Helper()
	t.Cleanup(upstream.Close)
	handler, err := proxy.NewHandler(upstream.URL, settings, transport, nil)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	return handler
}

func listener(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnContext = disconnect.ConnContext
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func newRequest(t *testing.T, ctx context.Context, method, url string, body io.Reader, headers http.Header) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range headers {
		request.Header[name] = values
	}
	return request
}

func delays(requestMS, responseMS string) http.Header {
	headers := make(http.Header)
	if requestMS != "" {
		headers.Set(requestDelay, requestMS)
	}
	if responseMS != "" {
		headers.Set(responseDelay, responseMS)
	}
	return headers
}

type reply struct {
	status  int
	headers http.Header
	body    string
	at      time.Time
	err     error
}

func call(client *http.Client, request *http.Request) reply {
	response, err := client.Do(request)
	at := time.Now()
	if err != nil {
		return reply{at: at, err: err}
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, headers: response.Header, body: string(body), at: at, err: err}
}

func async(client *http.Client, request *http.Request) <-chan reply {
	result := make(chan reply, 1)
	go func() { result <- call(client, request) }()
	return result
}

func await[T any](t *testing.T, ch <-chan T, timeout time.Duration, event string) T {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", event)
		var zero T
		return zero
	}
}

func checkStatus(t *testing.T, result reply, want int) {
	t.Helper()
	if result.err != nil || result.status != want {
		t.Fatalf("response status=%d body=%q err=%v, want status %d", result.status, result.body, result.err, want)
	}
}

func TestRequestAndFinalResponseTiming(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		headers      http.Header
		requestWait  time.Duration
		responseWait time.Duration
	}{
		{name: "2s request", headers: delays("2000", ""), requestWait: 2 * time.Second},
		{name: "3s response", headers: delays("", "3000"), responseWait: 3 * time.Second},
		{name: "2s request then 3s response", headers: delays("2000", "3000"), requestWait: 2 * time.Second, responseWait: 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			received := make(chan time.Time, 1)
			finalWritten := make(chan time.Time, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				received <- time.Now()
				w.Header().Set(tokenHeader, "backend-private")
				w.Header().Set("Link", "</asset>; rel=preload")
				w.WriteHeader(http.StatusEarlyHints)
				w.Header().Del("Link")
				w.Header().Set("X-Upstream", "final")
				finalWritten <- time.Now()
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, "upstream body")
			}))
			server := listener(t, loopbackProxy(t, upstream, faultSettings(), nil))
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()

			earlyHints := make(chan time.Time, 1)
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
					if code != http.StatusEarlyHints || header.Get("Link") != "</asset>; rel=preload" || header.Get(tokenHeader) != "" {
						return fmt.Errorf("unexpected informational response: %d %v", code, header)
					}
					select {
					case earlyHints <- time.Now():
						return nil
					default:
						return fmt.Errorf("duplicate informational response")
					}
				},
			})
			request := newRequest(t, ctx, http.MethodGet, server.URL, nil, tc.headers)
			clientSent := time.Now()
			result := async(server.Client(), request)
			backendReceived := await(t, received, 5*time.Second, "upstream receipt")
			backendFinal := await(t, finalWritten, 2*time.Second, "upstream final headers")
			clientEarlyHints := await(t, earlyHints, 2*time.Second, "client informational headers")
			got := await(t, result, 6*time.Second, "client final headers and body")

			checkStatus(t, got, http.StatusAccepted)
			if got.headers.Get("X-Upstream") != "final" || got.body != "upstream body" || chaos.HasControls(got.headers) {
				t.Errorf("unexpected final headers %v or body %q", got.headers, got.body)
			}
			checkTiming(t, backendReceived.Sub(clientSent), tc.requestWait, "client send to upstream receipt")
			checkTiming(t, clientEarlyHints.Sub(backendReceived), 0, "upstream receipt to client informational headers")
			checkTiming(t, got.at.Sub(backendFinal), tc.responseWait, "upstream final headers to client final headers")
		})
	}
}

func checkTiming(t *testing.T, elapsed, configured time.Duration, stage string) {
	t.Helper()
	const jitter = 1500 * time.Millisecond
	if elapsed < configured || elapsed > configured+jitter {
		t.Errorf("%s took %v, want between %v and %v", stage, elapsed, configured, configured+jitter)
	}
}

func TestCapacityAndResponseDelayCancellation(t *testing.T) {
	t.Parallel()

	upstreamCanceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delayed":
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("flush upstream headers: %v", err)
				return
			}
			<-r.Context().Done()
			close(upstreamCanceled)
		default:
			_, _ = io.WriteString(w, r.URL.Path)
		}
	}))
	// Signal when the real upstream transport has received the delayed response.
	upstreamHeaders := make(chan struct{})
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	wrapped := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		reply, err := transport.RoundTrip(r)
		if r.URL.Path == "/delayed" && err == nil {
			close(upstreamHeaders)
		}
		return reply, err
	})
	handler := loopbackProxy(t, upstream, faultSettings(), wrapped)
	delayedDone := make(chan struct{})
	server := listener(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/delayed" {
			defer close(delayedDone)
		}
		handler.ServeHTTP(w, r)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	delayedCtx, cancelDelayed := context.WithCancel(ctx)
	defer cancelDelayed()
	delayed := async(server.Client(), newRequest(t, delayedCtx, http.MethodGet, server.URL+"/delayed", nil, delays("", "10000")))
	await(t, upstreamHeaders, time.Second, "upstream transport's final headers")

	ordinary := call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/ordinary", nil, nil))
	checkStatus(t, ordinary, http.StatusOK)
	if ordinary.body != "/ordinary" {
		t.Errorf("concurrent ordinary response body = %q", ordinary.body)
	}
	limited := call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/limited", nil, delays("1", "")))
	checkStatus(t, limited, http.StatusTooManyRequests)
	select {
	case got := <-delayed:
		t.Fatalf("delayed client received final headers before cancellation: %+v", got)
	default:
	}

	cancelDelayed()
	await(t, delayedDone, time.Second, "proxy handler completion after cancellation")
	await(t, upstreamCanceled, time.Second, "upstream request cancellation")
	if got := await(t, delayed, time.Second, "canceled client"); got.err == nil {
		t.Fatalf("canceled response-delay request completed with status %d", got.status)
	}
	followup := call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/after", nil, delays("1", "1")))
	checkStatus(t, followup, http.StatusOK)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestRequestDelayCancellationReleasesSlot(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/delayed" {
			t.Error("canceled request reached upstream")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	observer := observability.New(nil)
	handler, err := proxy.NewHandler(upstream.URL, faultSettings(), nil, observer)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	delayedDone := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("/metrics", observer.MetricsHandler())
	mux.HandleFunc("/delayed", func(w http.ResponseWriter, r *http.Request) {
		defer close(delayedDone)
		handler.ServeHTTP(w, r)
	})
	mux.Handle("/", handler)
	server := listener(t, mux)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	delayedCtx, cancelDelayed := context.WithCancel(ctx)
	defer cancelDelayed()
	delayed := async(server.Client(), newRequest(t, delayedCtx, http.MethodGet, server.URL+"/delayed", nil, delays("10000", "")))
	// Wait for the metric that confirms the request is holding its fault slot.
	probe := newRequest(t, ctx, http.MethodGet, server.URL+"/metrics", nil, nil)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		got := call(server.Client(), probe.Clone(ctx))
		checkStatus(t, got, http.StatusOK)
		if strings.Contains(got.body, "\nchaos_proxy_active_faults 1\n") {
			break
		}
		select {
		case <-delayedDone:
			t.Fatal("request-delay handler exited before cancellation")
		case <-ctx.Done():
			t.Fatal("request-delay handler did not acquire its fault slot")
		case <-ticker.C:
		}
	}
	ordinary := call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/ordinary", nil, nil))
	checkStatus(t, ordinary, http.StatusNoContent)

	cancelDelayed()
	await(t, delayedDone, time.Second, "proxy handler completion after cancellation")
	if got := await(t, delayed, time.Second, "canceled client"); got.err == nil {
		t.Fatalf("canceled request-delay request completed with status %d", got.status)
	}
	followup := call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/after", nil, delays("1", "")))
	checkStatus(t, followup, http.StatusNoContent)
}

func TestBackendErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		upstream http.HandlerFunc
		want     int
	}{
		{"broken connection", func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }, http.StatusBadGateway},
		{"response header timeout", func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(tc.upstream)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.ResponseHeaderTimeout = 150 * time.Millisecond
			t.Cleanup(transport.CloseIdleConnections)
			server := listener(t, loopbackProxy(t, upstream, faultSettings(), transport))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			request := newRequest(t, ctx, http.MethodGet, server.URL, nil, nil)
			checkStatus(t, call(server.Client(), request), tc.want)
		})
	}
}

func TestAuthControlsAndGlobalOff(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		enabled bool
		headers http.Header
		want    int
	}{
		{"ordinary request without token", true, nil, http.StatusOK},
		{"valid token", true, http.Header{tokenHeader: {"test-token"}, requestDelay: {"1"}, "X-Chaos-Enabled": {"true"}}, http.StatusOK},
		{"missing token", true, delays("1", ""), http.StatusForbidden},
		{"wrong token", true, http.Header{tokenHeader: {"incorrect"}, responseDelay: {"1"}}, http.StatusForbidden},
		{"invalid delay", true, http.Header{tokenHeader: {"test-token"}, requestDelay: {"-1"}}, http.StatusBadRequest},
		{"exceeds delay limit", true, http.Header{tokenHeader: {"test-token"}, responseDelay: {"10001"}}, http.StatusBadRequest},
		{"unsupported advanced control", true, http.Header{tokenHeader: {"test-token"}, "X-Chaos-Response-Delay-Probability": {"0.5"}}, http.StatusBadRequest},
		{"off ignores delays and credentials", false, http.Header{tokenHeader: {"wrong-or-unneeded"}, requestDelay: {"10000"}, responseDelay: {"10000"}}, http.StatusOK},
		{"off ignores malformed controls", false, http.Header{"X-Chaos-Custom": {"garbage"}, requestDelay: {"-1"}}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			arrived := make(chan http.Header, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				arrived <- r.Header.Clone()
				_, _ = io.WriteString(w, "upstream")
			}))
			settings := faultSettings()
			settings.Enabled = tc.enabled
			settings.Token = "test-token"
			server := listener(t, loopbackProxy(t, upstream, settings, nil))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			request := newRequest(t, ctx, http.MethodGet, server.URL, nil, tc.headers)
			request.Header.Set("Authorization", "Bearer application-token")
			checkStatus(t, call(server.Client(), request), tc.want)
			select {
			case headers := <-arrived:
				if tc.want != http.StatusOK {
					t.Fatal("rejected request reached upstream")
				}
				if chaos.HasControls(headers) {
					t.Errorf("upstream received a chaos header")
				}
				if got := headers.Get("Authorization"); got != "Bearer application-token" {
					t.Errorf("upstream authorization = %q, want application credential", got)
				}
			default:
				if tc.want == http.StatusOK {
					t.Fatal("accepted request did not reach upstream")
				}
			}
		})
	}
}

func TestStreamsLargeUploadAndDownloadThroughFixedBackend(t *testing.T) {
	t.Parallel()

	const bodySize = 10 << 20
	const firstChunk = 128 << 10
	uploadStarted := make(chan struct{})
	downloadStarted := make(chan struct{})
	chunk := bytes.Repeat([]byte{'a'}, firstChunk)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.RequestURI != "/files?part=1" || r.Header.Get("X-App") != "kept" {
			t.Errorf("upstream received %s %s with X-App %q", r.Method, r.RequestURI, r.Header.Get("X-App"))
		}
		if r.Host != r.Context().Value(http.LocalAddrContextKey).(fmt.Stringer).String() {
			t.Errorf("upstream Host = %q, want fixed listener address", r.Host)
		}
		prefix := make([]byte, firstChunk)
		if _, err := io.ReadFull(r.Body, prefix); err != nil || !bytes.Equal(prefix, chunk) {
			t.Errorf("first upstream chunk was incomplete or altered: %v", err)
			return
		}
		close(uploadStarted)
		if n, err := io.Copy(io.Discard, r.Body); err != nil || n != bodySize-firstChunk {
			t.Errorf("upstream received %d remaining bytes, err=%v", n, err)
			return
		}
		w.Header().Set("Trailer", "X-End")
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write(chunk); err != nil {
			t.Errorf("write response prefix: %v", err)
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush response prefix: %v", err)
			return
		}
		select {
		case <-downloadStarted:
		case <-r.Context().Done():
			return
		}
		if n, err := io.CopyN(w, repeatedByte('b'), bodySize-firstChunk); err != nil || n != bodySize-firstChunk {
			t.Errorf("upstream sent %d remaining bytes, err=%v", n, err)
			return
		}
		w.Header().Set("X-End", "done")
	}))
	server := listener(t, loopbackProxy(t, upstream, faultSettings(), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	request := newRequest(t, ctx, http.MethodPost, server.URL+"/files?part=1", reader, delays("", "100"))
	request.Header.Set("X-App", "kept")
	request.Host = "caller.example.test"
	request.ContentLength = bodySize

	uploaded := make(chan error, 1)
	go func() {
		defer func() { _ = writer.Close() }()
		if _, err := writer.Write(chunk); err != nil {
			uploaded <- err
			return
		}
		select {
		case <-uploadStarted:
		case <-ctx.Done():
			uploaded <- ctx.Err()
			return
		}
		_, err := io.CopyN(writer, repeatedByte('b'), bodySize-firstChunk)
		uploaded <- err
	}()

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", response.StatusCode)
	}
	prefix := make([]byte, firstChunk)
	if _, err := io.ReadFull(response.Body, prefix); err != nil || !bytes.Equal(prefix, chunk) {
		t.Fatalf("first client chunk was incomplete or altered: %v", err)
	}
	close(downloadStarted)
	if n, err := io.Copy(io.Discard, response.Body); err != nil || n != bodySize-firstChunk {
		t.Errorf("client received %d remaining bytes, err=%v", n, err)
	}
	if got := response.Trailer.Get("X-End"); got != "done" {
		t.Errorf("client trailer X-End = %q, want done", got)
	}
	if err := await(t, uploaded, time.Second, "client upload completion"); err != nil {
		t.Errorf("client upload failed: %v", err)
	}
}

type repeatedByte byte

func (b repeatedByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}
