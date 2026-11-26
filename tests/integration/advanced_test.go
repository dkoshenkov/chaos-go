package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

const (
	statusControl      = "X-Chaos-Status-Code"
	probabilityControl = "X-Chaos-Probability"
	requestJitter      = "X-Chaos-Request-Jitter-Ms"
	responseJitter     = "X-Chaos-Response-Jitter-Ms"
	requestBandwidth   = "X-Chaos-Request-Bandwidth-Kbps"
	responseBandwidth  = "X-Chaos-Response-Bandwidth-Kbps"
	resetControl       = "X-Chaos-Connection-Reset"
)

func advancedFaultSettings() chaos.Settings {
	settings := faultSettings()
	settings.AbortEnabled = true
	settings.ProbabilityEnabled = true
	settings.DefaultProbability = 1
	settings.RequestJitterEnabled = true
	settings.ResponseJitterEnabled = true
	settings.RequestBandwidthEnabled = true
	settings.ResponseBandwidthEnabled = true
	settings.ConnectionResetEnabled = true
	return settings
}

func advancedHandler(t *testing.T, upstream string, settings chaos.Settings, transport http.RoundTripper, observer *observability.Observer) http.Handler {
	t.Helper()
	handler, err := proxy.NewHandler(upstream, settings, transport, observer)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func serveAdvanced(handler http.Handler, headers http.Header) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("upload"))
	if headers != nil {
		request.Header = headers.Clone()
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func observedInjections(t *testing.T, observer *observability.Observer, faultType, direction string, count int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	observer.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	sample := fmt.Sprintf(`chaos_proxy_injections_total{direction=%q,fault_type=%q} %d`, direction, faultType, count)
	if !strings.Contains(recorder.Body.String(), "\n"+sample+"\n") {
		t.Errorf("metrics are missing %q", sample)
	}
}

func TestSyntheticStatusHonorsBothDelaysWithoutUpstream(t *testing.T) {
	t.Parallel()
	observer := observability.New(nil)
	handler := advancedHandler(t, "http://upstream.test", advancedFaultSettings(), roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		t.Error("synthetic response contacted the upstream")
		return nil, fmt.Errorf("unexpected upstream request to %s", r.URL.Path)
	}), observer)

	for _, status := range []string{"503", "204"} {
		started := time.Now()
		got := serveAdvanced(handler, http.Header{
			statusControl: {status}, requestDelay: {"20"}, responseDelay: {"30"},
			requestBandwidth: {"1"}, responseBandwidth: {"1"},
		})
		if fmt.Sprint(got.Code) != status || got.Body.Len() != 0 {
			t.Errorf("synthetic response status=%d body=%q, want status %s with empty body", got.Code, got.Body.String(), status)
		}
		if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
			t.Errorf("synthetic response took %v, want at least 50ms", elapsed)
		}
	}
	observedInjections(t, observer, "abort", "response", 2)
	for _, direction := range []string{"request", "response"} {
		observedInjections(t, observer, "delay", direction, 2)
		observedInjections(t, observer, "bandwidth", direction, 0)
	}
}

type loggedInjection struct {
	FaultType string `json:"fault_type"`
	Direction string `json:"direction"`
}

type loggedRequest struct {
	ChaosApplied  bool              `json:"chaos_applied"`
	AppliedFaults []loggedInjection `json:"applied_faults"`
	RequestDelay  float64           `json:"request_delay_ms"`
	ResponseDelay float64           `json:"response_delay_ms"`
}

func nextRequestLog(t *testing.T, decoder *json.Decoder) loggedRequest {
	t.Helper()
	var entry loggedRequest
	if err := decoder.Decode(&entry); err != nil {
		t.Fatalf("decode completion log: %v", err)
	}
	return entry
}

func echoAdvancedRoundTrip(t *testing.T, r *http.Request) (*http.Response, error) {
	t.Helper()
	if chaos.HasControls(r.Header) {
		t.Error("upstream received chaos controls")
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil || string(payload) != "upload" {
		t.Errorf("upstream read %q, err=%v", payload, err)
	}
	_ = r.Body.Close()
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{requestJitter: {"invalid upstream control"}},
		Body:          io.NopCloser(strings.NewReader("upstream")),
		ContentLength: int64(len("upstream")),
	}, nil
}

func TestProbabilitySelectsTheEntirePolicy(t *testing.T) {
	t.Parallel()
	settings := advancedFaultSettings()
	settings.DefaultProbability = 0
	var logs bytes.Buffer
	observer := observability.New(slog.New(slog.NewJSONHandler(&logs, nil)))
	handler := advancedHandler(t, "http://upstream.test", settings, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return echoAdvancedRoundTrip(t, r)
	}), observer)
	headers := http.Header{
		requestDelay: {"2"}, responseDelay: {"2"}, requestJitter: {"1"}, responseJitter: {"1"},
		requestBandwidth: {"1000000"}, responseBandwidth: {"1000000"},
	}
	decoder := json.NewDecoder(&logs)
	request := func(probability string) bool {
		t.Helper()
		if probability != "" {
			headers.Set(probabilityControl, probability)
		}
		got := serveAdvanced(handler, headers)
		if got.Code != http.StatusOK || got.Body.String() != "upstream" || chaos.HasControls(got.Header()) {
			t.Fatalf("proxied response status=%d body=%q headers=%v", got.Code, got.Body.String(), got.Header())
		}
		entry := nextRequestLog(t, decoder)
		if !entry.ChaosApplied {
			if len(entry.AppliedFaults) != 0 {
				t.Fatalf("skipped request recorded injections: %+v", entry)
			}
			return false
		}
		want := map[loggedInjection]struct{}{
			{"delay", "request"}: {}, {"jitter", "request"}: {}, {"bandwidth", "request"}: {},
			{"delay", "response"}: {}, {"jitter", "response"}: {}, {"bandwidth", "response"}: {},
		}
		for _, fault := range entry.AppliedFaults {
			if _, ok := want[fault]; !ok {
				t.Fatalf("unexpected or duplicate logged fault %+v, applied policy = %+v", fault, entry.AppliedFaults)
			}
			delete(want, fault)
		}
		if len(want) != 0 {
			t.Fatalf("applied policy = %+v, missing faults %+v", entry.AppliedFaults, want)
		}
		return true
	}
	if request("") || request("0") {
		t.Fatal("probability zero selected a policy")
	}
	if !request("1") {
		t.Fatal("probability one skipped a policy")
	}
	const samples = 160
	selected := 0
	for range samples {
		if request("0.5") {
			selected++
		}
	}
	if selected < samples/4 || selected > samples*3/4 {
		t.Errorf("probability 0.5 selected %d of %d policies", selected, samples)
	}
	for _, faultType := range []string{"delay", "jitter", "bandwidth"} {
		for _, direction := range []string{"request", "response"} {
			observedInjections(t, observer, faultType, direction, selected+1)
		}
	}
}

func TestProbabilitySkipBypassesOccupiedFaultSlot(t *testing.T) {
	t.Parallel()
	occupied := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/occupy" {
			close(occupied)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server := listener(t, loopbackProxy(t, upstream, advancedFaultSettings(), nil))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	holder := async(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL+"/occupy", nil, delays("", "1")))
	await(t, occupied, 2*time.Second, "selected request reaching upstream")

	headers := http.Header{statusControl: {"503"}, probabilityControl: {"0"}}
	checkStatus(t, call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL, nil, headers)), http.StatusNoContent)
	headers.Set(probabilityControl, "1")
	checkStatus(t, call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL, nil, headers)), http.StatusTooManyRequests)
	close(release)
	checkStatus(t, await(t, holder, time.Second, "selected request releasing its slot"), http.StatusNoContent)
	checkStatus(t, call(server.Client(), newRequest(t, ctx, http.MethodGet, server.URL, nil, headers)), http.StatusServiceUnavailable)
}

func TestJitterValidationAndDirectionIsolation(t *testing.T) {
	t.Parallel()
	settings := advancedFaultSettings()
	settings.MaxRequestDelay = 10 * time.Millisecond
	settings.MaxResponseDelay = 10 * time.Millisecond
	var logs bytes.Buffer
	observer := observability.New(slog.New(slog.NewJSONHandler(&logs, nil)))
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) { return echoAdvancedRoundTrip(t, r) })
	handler := advancedHandler(t, "http://upstream.test", settings, transport, observer)
	decoder := json.NewDecoder(&logs)
	for _, headers := range []http.Header{
		{requestJitter: {"-1"}},
		{requestJitter: {"1.5"}},
		{responseJitter: {"11"}, probabilityControl: {"0"}},
		{responseJitter: {"1", "2"}},
	} {
		if got := serveAdvanced(handler, headers); got.Code != http.StatusBadRequest {
			t.Errorf("malformed jitter %v returned status %d", headers, got.Code)
		}
		if entry := nextRequestLog(t, decoder); entry.ChaosApplied || len(entry.AppliedFaults) != 0 {
			t.Errorf("invalid jitter produced fault records: %+v", entry)
		}
	}
	for _, tc := range []struct {
		direction string
		headers   http.Header
	}{
		{"request", http.Header{requestJitter: {"5"}, responseDelay: {"3"}}},
		{"response", http.Header{requestDelay: {"3"}, responseJitter: {"5"}}},
	} {
		if got := serveAdvanced(handler, tc.headers); got.Code != http.StatusOK {
			t.Fatalf("valid jitter returned status %d, body %q", got.Code, got.Body.String())
		}
		entry := nextRequestLog(t, decoder)
		jittered, fixed := entry.RequestDelay, entry.ResponseDelay
		if tc.direction == "response" {
			jittered, fixed = entry.ResponseDelay, entry.RequestDelay
		}
		if jittered < 0 || jittered > 5 || fixed != 3 {
			t.Errorf("%s jitter produced wait %vms and opposite wait %vms", tc.direction, jittered, fixed)
		}
		observedInjections(t, observer, "jitter", tc.direction, 1)
	}
	if got := serveAdvanced(handler, nil); got.Code != http.StatusOK {
		t.Fatalf("ordinary request returned status %d", got.Code)
	}
	if entry := nextRequestLog(t, decoder); entry.ChaosApplied || len(entry.AppliedFaults) != 0 || entry.RequestDelay != 0 || entry.ResponseDelay != 0 {
		t.Errorf("jitter carried over to a subsequent request: %+v", entry)
	}
	settings.RequestJitterEnabled = false
	disabled := advancedHandler(t, "http://upstream.test", settings, transport, nil)
	if got := serveAdvanced(disabled, http.Header{requestJitter: {"1"}}); got.Code != http.StatusBadRequest {
		t.Errorf("disabled request jitter returned status %d", got.Code)
	}
}

func TestAdvancedControlsAuthorizationAndGlobalOff(t *testing.T) {
	t.Parallel()
	settings := advancedFaultSettings()
	settings.Token = "configured-token"
	enabled := advancedHandler(t, "http://upstream.test", settings, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		t.Error("unauthorized request reached the upstream")
		return nil, fmt.Errorf("unexpected request to %s", r.URL.Path)
	}), nil)
	settings.Enabled = false
	disabled := advancedHandler(t, "http://upstream.test", settings, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return echoAdvancedRoundTrip(t, r)
	}), nil)

	headers := http.Header{
		tokenHeader: {"incorrect"}, statusControl: {"503"}, probabilityControl: {"0"},
		requestJitter: {"malformed"}, responseBandwidth: {"invalid"}, resetControl: {"true"},
	}
	if got := serveAdvanced(enabled, headers); got.Code != http.StatusForbidden {
		t.Errorf("unauthorized advanced controls returned status %d", got.Code)
	}
	if got := serveAdvanced(disabled, headers); got.Code != http.StatusOK || got.Body.String() != "upstream" || chaos.HasControls(got.Header()) {
		t.Errorf("global off returned status %d, body %q, headers %v", got.Code, got.Body.String(), got.Header())
	}
}

func readRateLimitedBody(body io.Reader, payload string) (time.Duration, error) {
	prefix := make([]byte, 1024)
	if _, err := io.ReadFull(body, prefix); err != nil {
		return 0, err
	}
	started := time.Now()
	rest, err := io.ReadAll(body)
	elapsed := time.Since(started)
	if err != nil {
		return elapsed, err
	}
	if string(prefix)+string(rest) != payload {
		return elapsed, fmt.Errorf("body changed in transit, received %d bytes instead of expected payload of %d bytes", len(prefix)+len(rest), len(payload))
	}
	return elapsed, nil
}

func TestBandwidthPacesUploadAndDownloadSeparately(t *testing.T) {
	t.Parallel()
	const rate = "256"
	// This body takes about 512ms at 256kbps. Allow buffering of the opening
	// chunks, but require steady pacing after the receiver sees the prefix.
	payload := strings.Repeat("proxy upload/download test data\n", 512)
	const minimumPacedTail = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, direction := range []string{"request", "response"} {
		t.Run(direction, func(t *testing.T) {
			var uploadElapsed time.Duration
			uploadDone := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if direction == "request" {
					defer close(uploadDone)
					var err error
					uploadElapsed, err = readRateLimitedBody(r.Body, payload)
					if err != nil {
						t.Errorf("read upstream upload: %v", err)
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
				if _, err := io.WriteString(w, payload); err != nil {
					t.Errorf("write upstream download: %v", err)
				}
			}))
			t.Cleanup(upstream.Close)
			observer := observability.New(nil)
			server := listener(t, advancedHandler(t, upstream.URL, advancedFaultSettings(), nil, observer))
			if direction == "request" {
				headers := http.Header{requestBandwidth: {rate}}
				request := newRequest(t, ctx, http.MethodPost, server.URL, strings.NewReader(payload), headers)
				checkStatus(t, call(server.Client(), request), http.StatusNoContent)
				await(t, uploadDone, time.Second, "upstream receiving the upload")
				if uploadElapsed < minimumPacedTail {
					t.Errorf("upload tail arrived in %v, want at least %v", uploadElapsed, minimumPacedTail)
				}
				observedInjections(t, observer, "bandwidth", "request", 1)
				observedInjections(t, observer, "bandwidth", "response", 0)
				return
			}
			headers := http.Header{responseBandwidth: {rate}}
			response, err := server.Client().Do(newRequest(t, ctx, http.MethodGet, server.URL, nil, headers))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("download status = %d", response.StatusCode)
			}
			elapsed, err := readRateLimitedBody(response.Body, payload)
			if err != nil {
				t.Fatal(err)
			}
			if elapsed < minimumPacedTail {
				t.Errorf("download tail arrived in %v, want at least %v", elapsed, minimumPacedTail)
			}
			observedInjections(t, observer, "bandwidth", "request", 0)
			observedInjections(t, observer, "bandwidth", "response", 1)
		})
	}
}
