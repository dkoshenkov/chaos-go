package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestResetDownstreamHTTP1(t *testing.T) {
	for _, tc := range []struct {
		name string
		tls  bool
	}{
		{name: "cleartext"},
		{name: "TLS", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resetDownstream(resetWireAdapter{w}, r)
			}))
			if tc.tls {
				server.StartTLS()
			} else {
				server.Start()
			}
			t.Cleanup(server.Close)

			var dialer net.Dialer
			conn, err := dialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if tc.tls {
				config := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
				config.ServerName = server.Listener.Addr().(*net.TCPAddr).IP.String()
				conn = tls.Client(conn, config)
			}
			if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: proxy\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			var b [1]byte
			n, err := conn.Read(b[:])
			if n != 0 || !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("read %d bytes with error %v, want TCP reset before HTTP response", n, err)
			}
		})
	}
}

func TestResetAfterHTTP1(t *testing.T) {
	for _, framing := range []string{"content-length", "chunked"} {
		for _, tc := range []struct {
			name  string
			body  string
			reset bool
		}{
			{name: "empty"},
			{name: "short", body: "ab"},
			{name: "exact", body: "abc"},
			{name: "long", body: "abcdef", reset: true},
		} {
			t.Run(framing+"/"+tc.name, func(t *testing.T) {
				var closed atomic.Bool
				reverse := newResetTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
					if framing == "content-length" {
						w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
					}
					w.WriteHeader(http.StatusAccepted)
					if framing == "chunked" {
						if err := http.NewResponseController(w).Flush(); err != nil {
							t.Error(err)
						}
					}
					_, _ = io.WriteString(w, tc.body)
				})
				reverse.ModifyResponse = func(response *http.Response) error {
					response.Body = &resetTestBody{ReadCloser: response.Body, closed: &closed}
					return nil
				}
				type outcome struct {
					status int
					reset  bool
				}
				finished := make(chan outcome, 1)
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					writer := &responseWriter{ResponseWriter: resetWireAdapter{w}}
					var reset bool
					defer func() { finished <- outcome{status: writer.status, reset: reset} }()
					resetter := resetAfterWriter(writer, r, 3, func() {
						if !closed.Load() {
							t.Error("upstream body remains open at reset")
						}
						reset = true
					})
					defer resetter.abort()
					reverse.ServeHTTP(resetter, r)
				}))
				t.Cleanup(proxy.Close)

				client := proxy.Client()
				client.Timeout = 5 * time.Second
				res, err := client.Get(proxy.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = res.Body.Close() }()
				body, err := io.ReadAll(res.Body)
				wantBody := tc.body[:min(len(tc.body), 3)]
				if res.StatusCode != http.StatusAccepted || string(body) != wantBody || (err != nil) != tc.reset {
					t.Fatalf("response status %d, body %q, error %v, want status 202, body %q and reset %t", res.StatusCode, body, err, wantBody, tc.reset)
				}
				if tc.reset && !errors.Is(err, syscall.ECONNRESET) {
					t.Errorf("read error %v, want TCP reset", err)
				}
				if got := <-finished; got.status != http.StatusAccepted || got.reset != tc.reset {
					t.Errorf("logged status %d and reset %t, want status 202 and reset %t", got.status, got.reset, tc.reset)
				}
			})
		}
	}
}

func TestResetAfterHTTP1WhileUpstreamStalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	canceled := make(chan struct{})
	reverse := newResetTestProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "6")
		if _, err := io.WriteString(w, "abc"); err != nil {
			t.Error(err)
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-ctx.Done():
		}
	})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resetter := resetAfterWriter(w, r, 3, func() {})
		defer resetter.abort()
		reverse.ServeHTTP(resetter, r)
	}))
	t.Cleanup(proxy.Close)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if string(body) != "abc" || !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("read body %q and error %v, want three bytes and TCP reset while upstream stalls", body, err)
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("reset did not cancel the stalled upstream request")
	}
}

func TestResetDownstreamHTTP2KeepsSiblingStream(t *testing.T) {
	reverse := newResetTestProxy(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "6")
		_, _ = io.WriteString(w, "abcdef")
	})
	connections := make(chan string, 4)
	release := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("request used %s, want HTTP/2", r.Proto)
		}
		connections <- r.RemoteAddr
		switch r.URL.Path {
		case "/sibling":
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Error(err)
			}
			select {
			case <-release:
				_, _ = io.WriteString(w, "sibling")
			case <-r.Context().Done():
			}
		case "/immediate":
			resetDownstream(w, r)
		case "/after":
			resetter := resetAfterWriter(w, r, 3, func() {})
			defer resetter.abort()
			reverse.ServeHTTP(resetter, r)
		}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second

	sibling, err := client.Get(server.URL + "/sibling")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sibling.Body.Close() }()
	siblingConn := <-connections
	if res, err := client.Get(server.URL + "/immediate"); err == nil {
		_ = res.Body.Close()
		t.Fatal("immediate reset returned an HTTP response")
	} else if !strings.Contains(err.Error(), "stream error: stream ID") {
		t.Errorf("immediate reset error %v, want HTTP/2 stream error", err)
	}
	if conn := <-connections; conn != siblingConn {
		t.Errorf("immediate reset connection %q, want sibling connection %q", conn, siblingConn)
	}

	res, err := client.Get(server.URL + "/after")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if conn := <-connections; conn != siblingConn {
		t.Errorf("mid-body reset connection %q, want sibling connection %q", conn, siblingConn)
	}
	if body, err := io.ReadAll(res.Body); string(body) != "abc" || err == nil || !strings.Contains(err.Error(), "stream error: stream ID") {
		t.Errorf("body %q, error %v, want three bytes and HTTP/2 stream error", body, err)
	}
	close(release)
	if body, err := io.ReadAll(sibling.Body); string(body) != "sibling" || err != nil {
		t.Errorf("sibling body %q, error %v, want complete response", body, err)
	}
}

type resetWireAdapter struct{ http.ResponseWriter }

func (w resetWireAdapter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type resetTestBody struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (b *resetTestBody) Close() error {
	err := b.ReadCloser.Close()
	b.closed.Store(true)
	return err
}

func newResetTestProxy(t *testing.T, upstream http.HandlerFunc) *httputil.ReverseProxy {
	t.Helper()
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return httputil.NewSingleHostReverseProxy(target)
}
