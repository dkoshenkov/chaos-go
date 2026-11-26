package proxy_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
