package proxy_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
)

func TestProxySelectsTerminalPolicyBeforeTakingFaultSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := delaySettings
		settings.AbortEnabled = true
		settings.ProbabilityEnabled = true
		settings.DefaultProbability = 1
		var upstreamCalls atomic.Int32
		handler := newTestProxy(t, settings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			upstreamCalls.Add(1)
			if chaos.HasControls(r.Header) {
				t.Error("upstream received chaos control headers")
			}
			return &http.Response{StatusCode: http.StatusAccepted, Body: http.NoBody}, nil
		}))
		newRequest := func(probability, statusCode string) *http.Request {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("X-Chaos-Token", settings.Token)
			request.Header.Set("X-Chaos-Probability", probability)
			request.Header.Set("X-Chaos-Status-Code", statusCode)
			request.Header.Set("X-Chaos-Request-Delay-Ms", "20")
			request.Header.Set("X-Chaos-Response-Delay-Ms", "30")
			return request
		}

		started := time.Now()
		terminal := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			handler.ServeHTTP(terminal, newRequest("1", "503"))
		}()
		synctest.Wait()
		if upstreamCalls.Load() != 0 {
			t.Fatalf("terminal policy contacted upstream at start")
		}

		for _, tc := range []struct {
			probability, statusCode string
			want                    int
		}{
			{"0", "503", http.StatusAccepted},
			{"0", "600", http.StatusBadRequest},
			{"1", "503", http.StatusTooManyRequests},
		} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, newRequest(tc.probability, tc.statusCode))
			if response.Code != tc.want || time.Since(started) != 0 {
				t.Errorf("probability=%s status=%s returned %d after %v, want %d immediately", tc.probability, tc.statusCode, response.Code, time.Since(started), tc.want)
			}
		}

		time.Sleep(49 * time.Millisecond)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("terminal response finished before both delays elapsed")
		default:
		}
		<-done
		if elapsed, calls := time.Since(started), upstreamCalls.Load(); terminal.Code != http.StatusServiceUnavailable || terminal.Body.Len() != 0 || calls != 1 || elapsed != 50*time.Millisecond {
			t.Fatalf("terminal status %d, body %q, upstream calls %d, elapsed %v", terminal.Code, terminal.Body, calls, elapsed)
		}
	})
}

func TestProxyPacesBothBodiesAndPreservesTrailers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := delaySettings
		settings.RequestBandwidthEnabled = true
		settings.ResponseBandwidthEnabled = true
		started := time.Now()
		const bodyDuration = 80 * time.Millisecond
		body := strings.Repeat("abcdefgh", 10)
		handler := newTestProxy(t, settings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			defer func() { _ = r.Body.Close() }()
			got, err := io.ReadAll(r.Body)
			if err != nil || string(got) != body || time.Since(started) != bodyDuration {
				t.Errorf("upstream read (%q, %v) after %v, want body after %v", got, err, time.Since(started), bodyDuration)
			}
			if chaos.HasControls(r.Header) || chaos.HasControls(r.Trailer) || r.Trailer.Get("X-App-Request") != "kept" {
				t.Errorf("unexpected upstream headers or trailers: %v, %v", r.Header, r.Trailer)
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: -1,
				Trailer:       http.Header{"X-App-Response": {"kept"}, "X-Chaos-Token": {"private"}},
			}, nil
		}))

		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		request.Header.Set("X-Chaos-Token", settings.Token)
		request.Header.Set("X-Chaos-Request-Bandwidth-Kbps", "8")
		request.Header.Set("X-Chaos-Response-Bandwidth-Kbps", "8")
		request.Trailer = http.Header{"X-App-Request": {"kept"}, "X-Chaos-Token": {"private"}}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != body || time.Since(started) != 2*bodyDuration {
			t.Errorf("downstream received status %d and body %q after %v, want 200 and body after %v", response.Code, response.Body, time.Since(started), 2*bodyDuration)
		}
		result := response.Result()
		defer func() { _ = result.Body.Close() }()
		if chaos.HasControls(result.Header) || chaos.HasControls(result.Trailer) || result.Trailer.Get("X-App-Response") != "kept" {
			t.Errorf("unexpected downstream headers or trailers: %v, %v", result.Header, result.Trailer)
		}
	})
}

func TestProxyStreamsRateLimitedKnownLengthResponse(t *testing.T) {
	const first, last = "first", "second"
	const length = len(first) + len(last)
	settings := delaySettings
	settings.ResponseBandwidthEnabled = true
	body, upstream := io.Pipe()
	handler := newTestProxy(t, settings, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Length": {strconv.Itoa(length)}},
			Body:          body,
			ContentLength: int64(length),
		}, nil
	}))
	server := httptest.NewServer(handler)
	defer server.Close()
	defer func() { _ = body.Close() }()
	defer func() { _ = upstream.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = upstream.CloseWithError(ctx.Err()) })
	defer stop()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Chaos-Token", settings.Token)
	request.Header.Set("X-Chaos-Response-Bandwidth-Kbps", "1000")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("receive headers before releasing the upstream body: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.ContentLength != int64(length) {
		t.Errorf("Content-Length = %d, want %d", response.ContentLength, length)
	}
	if _, err := io.WriteString(upstream, first); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, len(first))
	if _, err := io.ReadFull(response.Body, prefix); err != nil || string(prefix) != first {
		t.Fatalf("receive first chunk before releasing the rest: %q, %v", prefix, err)
	}
	if _, err := io.WriteString(upstream, last); err != nil {
		t.Fatal(err)
	}
	if err := upstream.Close(); err != nil {
		t.Fatal(err)
	}
	remainder, err := io.ReadAll(response.Body)
	if err != nil || string(remainder) != last {
		t.Fatalf("read remaining chunk: %q, %v", remainder, err)
	}
}

func TestProxyTerminalFaultDoesNotWaitForHTTP1Upload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		control string
		status  int
	}{
		{"synthetic status", "X-Chaos-Status-Code: 503", http.StatusServiceUnavailable},
		{"immediate reset", "X-Chaos-Connection-Reset: true", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := delaySettings
			settings.AbortEnabled = true
			settings.ConnectionResetEnabled = true
			var upstreamCalls atomic.Int32
			handler := newTestProxy(t, settings, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				upstreamCalls.Add(1)
				_ = r.Body.Close()
				return &http.Response{StatusCode: http.StatusAccepted, Body: http.NoBody}, nil
			}))
			server := httptest.NewServer(handler)
			defer server.Close()
			var dialer net.Dialer
			conn, err := dialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			request := "POST / HTTP/1.1\r\nHost: proxy\r\nContent-Length: 500\r\nX-Chaos-Token: " + settings.Token + "\r\n" + tc.control + "\r\n\r\na"
			if _, err := io.WriteString(conn, request); err != nil {
				t.Fatal(err)
			}

			response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
			if response != nil {
				defer func() { _ = response.Body.Close() }()
			}
			if tc.status == 0 {
				var netErr net.Error
				if err == nil || errors.As(err, &netErr) && netErr.Timeout() {
					t.Fatalf("immediate reset returned response %v and error %v", response, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				if response.StatusCode != tc.status || len(body) != 0 || err != nil {
					t.Fatalf("terminal status %d returned body %q and error %v", response.StatusCode, body, err)
				}
			}
			if calls := upstreamCalls.Load(); calls != 0 {
				t.Errorf("terminal fault contacted upstream %d times", calls)
			}
		})
	}
}
