package proxy_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

func TestProxyForwardsRequestAndResponse(t *testing.T) {
	type forwardedRequest struct {
		method string
		host   string
		body   string
		header http.Header
	}
	forwarded := make(chan forwardedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream request body: %v", err)
		}
		forwarded <- forwardedRequest{
			method: r.Method,
			host:   r.Host,
			body:   string(body),
			header: r.Header.Clone(),
		}

		w.Header().Set("X-Upstream", "preserved")
		w.Header().Set("Connection", "X-Response-Hop")
		w.Header().Set("X-Response-Hop", "removed")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.WriteHeader(http.StatusAccepted)
		if _, err := io.WriteString(w, "upstream response"); err != nil {
			t.Errorf("write upstream response body: %v", err)
		}
	}))
	defer upstream.Close()

	handler, err := proxy.New(upstream.URL)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	request := httptest.NewRequest(http.MethodPatch, "http://public.example.test:8080/items/42", strings.NewReader("request body"))
	request.RemoteAddr = "192.0.2.10:12345"
	request.Header.Set("X-Request", "preserved")
	request.Header.Set("Connection", "keep-alive, X-Request-Hop")
	request.Header.Set("X-Request-Hop", "removed")
	request.Header.Set("Keep-Alive", "timeout=5")
	request.Header.Set("Forwarded", "for=192.0.2.50")
	request.Header.Set("X-Forwarded-For", "192.0.2.50")
	request.Header.Set("X-Forwarded-Host", "forged.example.test")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Forwarded-Port", "4444")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	select {
	case got := <-forwarded:
		if got.method != http.MethodPatch || got.body != "request body" {
			t.Errorf("upstream received method %q and body %q, want PATCH and %q", got.method, got.body, "request body")
		}
		if want := upstream.Listener.Addr().String(); got.host != want {
			t.Errorf("upstream received Host %q, want %q", got.host, want)
		}
		for name, want := range map[string]string{
			"X-Request":         "preserved",
			"X-Forwarded-For":   "192.0.2.10",
			"X-Forwarded-Host":  "public.example.test:8080",
			"X-Forwarded-Proto": "http",
		} {
			if values := got.header.Values(name); len(values) != 1 || values[0] != want {
				t.Errorf("upstream received %s values %q, want only %q", name, values, want)
			}
		}
		for _, name := range []string{"Connection", "Keep-Alive", "X-Request-Hop", "Forwarded", "X-Forwarded-Port"} {
			if values := got.header.Values(name); len(values) != 0 {
				t.Errorf("upstream received %s values %q, want header removed", name, values)
			}
		}
	default:
		t.Fatalf("upstream received no request, proxy returned %d", response.Code)
	}
	if response.Code != http.StatusAccepted || response.Body.String() != "upstream response" {
		t.Errorf("proxy returned status %d and body %q, want 202 and %q", response.Code, response.Body.String(), "upstream response")
	}
	if got := response.Header().Get("X-Upstream"); got != "preserved" {
		t.Errorf("proxy returned X-Upstream %q, want %q", got, "preserved")
	}
	for _, name := range []string{"Connection", "Keep-Alive", "X-Response-Hop"} {
		if values := response.Header().Values(name); len(values) != 0 {
			t.Errorf("proxy returned %s values %q, want header removed", name, values)
		}
	}
}

func TestProxyJoinsUpstreamAndRequestURLs(t *testing.T) {
	for _, tt := range []struct {
		name string
		base string
		path string
		want string
	}{
		{"no base path", "", "/items?sort=name", "/items?sort=name"},
		{"base path", "/api/", "/items/42", "/api/items/42"},
		{"root request", "/api", "/", "/api/"},
		{
			name: "escaped paths and combined queries",
			base: "/api%2Fv1?origin=blue&shared=base",
			path: "/items%2F42/%2e?client=green&shared=incoming",
			want: "/api%2Fv1/items%2F42/%2e?origin=blue&shared=base&client=green&shared=incoming",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.WriteString(w, r.RequestURI); err != nil {
					t.Errorf("write upstream request URI: %v", err)
				}
			}))
			defer upstream.Close()

			handler, err := proxy.New(upstream.URL + tt.base)
			if err != nil {
				t.Fatalf("create proxy: %v", err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if response.Code != http.StatusOK || response.Body.String() != tt.want {
				t.Errorf("proxy returned status %d and body %q, want 200 and %q", response.Code, response.Body.String(), tt.want)
			}
		})
	}
}

func TestProxyReturnsBadGatewayOnUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()

	handler, err := proxy.New(upstream.URL)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/first", nil))
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/second", nil))

	if first.Code != http.StatusBadGateway || second.Code != http.StatusBadGateway {
		t.Fatalf("proxy returned statuses %d and %d, want 502 for both", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("proxy returned different failure bodies %q and %q", first.Body.String(), second.Body.String())
	}
}

func TestNewRejectsInvalidUpstreamURLs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		target string
	}{
		{"empty", ""},
		{"relative", "/api"},
		{"unsupported scheme", "ftp://example.test/api"},
		{"missing host", "http:///api"},
		{"malformed escape", "http://example.test/%ZZ"},
		{"user information", "https://user:password@example.test/api"},
		{"fragment", "https://example.test/api#fragment"},
		{"empty fragment", "https://example.test/api#"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := proxy.New(tt.target)
			if err == nil || handler != nil {
				t.Errorf("New(%q) returned handler %T and error %v, want nil handler and a configuration error", tt.target, handler, err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

var delaySettings = chaos.Settings{
	Enabled:          true,
	RequestEnabled:   true,
	ResponseEnabled:  true,
	MaxRequestDelay:  time.Hour,
	MaxResponseDelay: time.Hour,
	MaxActiveFaults:  1,
	Token:            "proxy-test-token",
}

func newTestProxy(t *testing.T, settings chaos.Settings, transport http.RoundTripper) http.Handler {
	t.Helper()
	handler, err := proxy.NewHandler("http://upstream.example.test", settings, transport, nil)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	return handler
}

func TestProxyAppliesRequestAndResponseDelays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		var upstreamStarted time.Duration
		handler := newTestProxy(t, delaySettings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			upstreamStarted = time.Since(started)
			if chaos.HasControls(r.Header) {
				t.Error("upstream received chaos control headers")
			}
			time.Sleep(20 * time.Millisecond)
			return &http.Response{
				StatusCode:    http.StatusAccepted,
				Header:        http.Header{"X-Chaos-Token": {"upstream-secret"}, "X-Upstream": {"original"}},
				Body:          io.NopCloser(strings.NewReader("response")),
				ContentLength: int64(len("response")),
			}, nil
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Chaos-Token", delaySettings.Token)
		req.Header.Set("X-Chaos-Request-Delay-Ms", "50")
		req.Header.Set("X-Chaos-Response-Delay-Ms", "80")
		req.Header.Set("Connection", "X-Chaos-Token")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if upstreamStarted != 50*time.Millisecond || time.Since(started) != 150*time.Millisecond {
			t.Errorf("upstream started at %v, response ended at %v, want 50ms and 150ms", upstreamStarted, time.Since(started))
		}
		if w.Code != http.StatusAccepted || w.Body.String() != "response" || w.Header().Get("X-Upstream") != "original" {
			t.Errorf("response status, body or upstream header changed: %d, %q, %v", w.Code, w.Body.String(), w.Header())
		}
		if chaos.HasControls(w.Header()) {
			t.Error("downstream received chaos control headers")
		}
	})
}

func TestProxyControlRejectionAndGlobalDisable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(*chaos.Settings, http.Header)
		want    int
		proxied bool
	}{
		{"normal request", func(_ *chaos.Settings, _ http.Header) {}, http.StatusOK, true},
		{"missing token", func(_ *chaos.Settings, h http.Header) {
			h.Set("X-Chaos-Request-Delay-Ms", "1")
		}, http.StatusForbidden, false},
		{"wrong token", func(_ *chaos.Settings, h http.Header) {
			h.Set("X-Chaos-Token", "wrong")
			h.Set("X-Chaos-Request-Delay-Ms", "1")
		}, http.StatusForbidden, false},
		{"invalid delay", func(s *chaos.Settings, h http.Header) {
			h.Set("X-Chaos-Token", s.Token)
			h.Set("X-Chaos-Request-Delay-Ms", "-1")
		}, http.StatusBadRequest, false},
		{"unsupported fault", func(s *chaos.Settings, h http.Header) {
			h.Set("X-Chaos-Token", s.Token)
			h.Set("X-Chaos-Reset", "true")
		}, http.StatusBadRequest, false},
		{"direction disabled", func(s *chaos.Settings, h http.Header) {
			s.ResponseEnabled = false
			h.Set("X-Chaos-Token", s.Token)
			h.Set("X-Chaos-Response-Delay-Ms", "1")
		}, http.StatusBadRequest, false},
		{"global disable", func(s *chaos.Settings, h http.Header) {
			s.Enabled = false
			h["x-chaos-token"] = []string{"wrong"}
			h.Set("X-Chaos-Reset", "true")
			h.Set("X-Chaos-Request-Delay-Ms", "-1")
		}, http.StatusOK, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := delaySettings
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			tc.setup(&settings, req.Header)
			calls := 0
			handler := newTestProxy(t, settings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if chaos.HasControls(r.Header) {
					t.Error("upstream received chaos control headers")
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			}))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.want || (calls != 0) != tc.proxied {
				t.Errorf("got status %d and %d upstream calls, want status %d and proxied=%v", w.Code, calls, tc.want, tc.proxied)
			}
			if strings.Contains(w.Body.String(), settings.Token) {
				t.Error("response body exposed the token")
			}
		})
	}
}

func TestProxyCancellationReleasesFaultSlot(t *testing.T) {
	for _, direction := range []string{"request", "response"} {
		t.Run(direction, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				var body *closeTrackingBody
				handler := newTestProxy(t, delaySettings, roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					body = &closeTrackingBody{Reader: strings.NewReader("body")}
					return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: 4}, nil
				}))
				newRequest := func(ctx context.Context, delay string) *http.Request {
					req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
					req.Header.Set("X-Chaos-Token", delaySettings.Token)
					req.Header.Set("X-Chaos-"+direction+"-Delay-Ms", delay)
					return req
				}

				started := time.Now()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w := &testResponseWriter{header: make(http.Header)}
				go handler.ServeHTTP(w, newRequest(ctx, "30000"))
				synctest.Wait()
				if (calls != 0) != (direction == "response") || len(w.codes) != 0 {
					t.Fatalf("before cancellation got %d upstream calls and response statuses %v", calls, w.codes)
				}

				rejected := httptest.NewRecorder()
				handler.ServeHTTP(rejected, newRequest(t.Context(), "1"))
				if rejected.Code != http.StatusTooManyRequests || time.Since(started) != 0 {
					t.Errorf("saturated proxy returned status %d after %v, want 429 immediately", rejected.Code, time.Since(started))
				}

				cancel()
				synctest.Wait()
				if len(w.codes) != 0 || (body != nil && !body.closed) || time.Since(started) != 0 {
					t.Errorf("cancellation left status %v, body %v, elapsed time %v", w.codes, body, time.Since(started))
				}

				next := httptest.NewRecorder()
				handler.ServeHTTP(next, newRequest(t.Context(), "1"))
				if next.Code != http.StatusOK || time.Since(started) != time.Millisecond {
					t.Errorf("after cancellation returned status %d at %v, want 200 after 1ms", next.Code, time.Since(started))
				}
			})
		})
	}
}

type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

type testResponseWriter struct {
	header  http.Header
	codes   []int
	written []http.Header
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) WriteHeader(code int) {
	w.codes = append(w.codes, code)
	w.written = append(w.written, w.header.Clone())
}

func (w *testResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestProxySanitizesInformationalHeaders(t *testing.T) {
	handler := newTestProxy(t, delaySettings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		err := httptrace.ContextClientTrace(r.Context()).Got1xxResponse(http.StatusEarlyHints, textproto.MIMEHeader{
			"X-Chaos-Token": {"upstream-token"},
			"Trailer":       {"X-Chaos-Token"},
			"Link":          {"</style.css>; rel=preload"},
		})
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusCreated, Body: http.NoBody}, nil
	}))
	w := &testResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if len(w.codes) != 2 || w.codes[0] != http.StatusEarlyHints || w.codes[1] != http.StatusCreated {
		t.Fatalf("wrote status codes %v, want [103 201]", w.codes)
	}
	first := w.written[0]
	if chaos.HasControls(first) || first.Get("Trailer") != "" || first.Get("Link") != "</style.css>; rel=preload" {
		t.Errorf("invalid informational headers: %v", first)
	}
}

func TestProxyDistinguishesTransportErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"upstream timeout", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"ordinary failure", errors.New("dial failed"), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			handler := newTestProxy(t, delaySettings, roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return nil, tc.err
			}))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if w.Code != tc.want || calls != 1 {
				t.Errorf("got status %d after %d attempts, want status %d after one attempt", w.Code, calls, tc.want)
			}
		})
	}
}

func TestProxyStreamsAndStripsTrailers(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "request body" {
			t.Errorf("upstream read (%q, %v), want request body", body, err)
		}
		if chaos.HasControls(r.Trailer) || r.Trailer.Get("X-App-Request") != "kept" {
			t.Errorf("invalid upstream request trailers: %v", r.Trailer)
		}
		w.Header().Set("Trailer", "X-App-Response, X-Chaos-Announced")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "second")
		w.Header().Set("X-App-Response", "kept")
		w.Header().Set("X-Chaos-Announced", "private")
		w.Header().Set(http.TrailerPrefix+"X-Chaos-Late", "private")
	}))
	defer upstream.Close()

	handler, err := proxy.New(upstream.URL)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	req, err := http.NewRequest(http.MethodPost, server.URL, io.NopCloser(strings.NewReader("request body")))
	if err != nil {
		t.Fatal(err)
	}
	req.Trailer = http.Header{"X-App-Request": {"kept"}, "X-Chaos-Token": {"private"}}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	}()
	first := make([]byte, len("first"))
	if _, err := io.ReadFull(res.Body, first); err != nil || string(first) != "first" {
		t.Fatalf("first streamed chunk = (%q, %v), want first", first, err)
	}
	if chaos.HasControls(res.Trailer) {
		t.Errorf("chaos trailer announced to downstream: %v", res.Trailer)
	}
	close(release)
	second, err := io.ReadAll(res.Body)
	if err != nil || string(second) != "second" {
		t.Errorf("second streamed chunk = (%q, %v), want second", second, err)
	}
	if chaos.HasControls(res.Trailer) || res.Trailer.Get("X-App-Response") != "kept" {
		t.Errorf("invalid downstream response trailers: %v", res.Trailer)
	}
}
