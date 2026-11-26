package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	observer := New(nil)
	request := observer.Begin(httptest.NewRequest("CUSTOM", "/client-secret?query=client-secret", nil), "https://service.example")
	request.Fault(true)
	request.Fault(true)
	active := scrapeMetrics(t, observer)
	assertSamples(t, active, "chaos_proxy_active_requests 1", "chaos_proxy_active_faults 1")

	request.Delay("request", 100*time.Millisecond, 20*time.Millisecond)
	request.Delay("response", 200*time.Millisecond, 100*time.Millisecond)
	request.Delay("client-secret", time.Second, time.Second)
	request.Upstream(30 * time.Millisecond)
	request.Finish(0, true)
	request.Finish(0, true)
	request.Fault(false)

	rejected := observer.Begin(httptest.NewRequest(http.MethodGet, "/", nil), "https://service.example")
	rejected.Invalid()
	rejected.Finish(http.StatusBadRequest, false)

	output := scrapeMetrics(t, observer)
	assertSamples(t, output,
		`chaos_proxy_requests_total{method="OTHER",status_code="unknown"} 1`,
		`chaos_proxy_requests_total{method="GET",status_code="400"} 1`,
		`chaos_proxy_injections_total{direction="request",fault_type="delay"} 1`,
		`chaos_proxy_injections_total{direction="response",fault_type="delay"} 1`,
		`chaos_proxy_requested_delay_seconds_sum{direction="request"} 0.1`,
		`chaos_proxy_requested_delay_seconds_sum{direction="response"} 0.2`,
		`chaos_proxy_injected_delay_seconds_sum{direction="request"} 0.02`,
		`chaos_proxy_injected_delay_seconds_sum{direction="response"} 0.1`,
		`chaos_proxy_upstream_duration_seconds_count 1`,
		`chaos_proxy_upstream_duration_seconds_sum 0.03`,
		`chaos_proxy_request_duration_seconds_count 2`,
		`chaos_proxy_canceled_requests_total 1`,
		`chaos_proxy_validation_errors_total{reason="invalid_policy"} 1`,
		`chaos_proxy_active_requests 0`,
		`chaos_proxy_active_faults 0`,
	)
	if strings.Contains(output, "client-secret") || strings.Contains(output, "CUSTOM") {
		t.Fatal("scrape contains unbounded labels")
	}
	if !strings.Contains(output, "# TYPE go_goroutines gauge\n") {
		t.Fatal("scrape is missing Go runtime metrics")
	}
	assertSamples(t, scrapeMetrics(t, New(nil)),
		`chaos_proxy_request_duration_seconds_count 0`,
		`chaos_proxy_canceled_requests_total 0`,
		`chaos_proxy_validation_errors_total{reason="invalid_policy"} 0`,
	)
}

func TestRequestLog(t *testing.T) {
	var logs bytes.Buffer
	observer := New(slog.New(slog.NewJSONHandler(&logs, nil)))
	request := httptest.NewRequest(http.MethodPost, "/some%20path?token=query-secret", strings.NewReader("body-secret"))
	request.Header.Set("Authorization", "Bearer header-secret")
	request.Header.Set("X-Request-ID", "spoofed-id")
	state := observer.Begin(request, "https://user:password@service.example/base?token=upstream-secret#fragment-secret")
	state.Policy(10*time.Millisecond, 25*time.Millisecond)
	state.Delay("request", 10*time.Millisecond, 10*time.Millisecond)
	state.Upstream(2300 * time.Microsecond)
	state.Finish(http.StatusOK, true)
	observer.Begin(request, "service.example:8443/base?token=upstream-secret").Finish(http.StatusOK, false)

	for _, secret := range []string{"query-secret", "body-secret", "header-secret", "spoofed-id", "password", "upstream-secret", "fragment-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("request log contains %q", secret)
		}
	}
	decoder := json.NewDecoder(&logs)
	var first, second map[string]any
	if err := decoder.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&second); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]any{
		"method":               http.MethodPost,
		"path":                 "/some%20path",
		"upstream":             "https://service.example/base",
		"chaos_applied":        true,
		"request_delay_ms":     float64(10),
		"response_delay_ms":    float64(25),
		"upstream_duration_ms": 2.3,
		"status_code":          float64(http.StatusOK),
		"canceled":             true,
	} {
		if got := first[field]; got != want {
			t.Errorf("first log field %q = %v, want %v", field, got, want)
		}
	}
	if total, ok := first["total_duration_ms"].(float64); !ok || total < 0 {
		t.Errorf("first log has invalid total duration %v", first["total_duration_ms"])
	}
	firstID, firstExists := first["request_id"].(string)
	secondID, secondExists := second["request_id"].(string)
	if !firstExists || !secondExists || firstID == "" || secondID == "" || firstID == secondID {
		t.Errorf("request IDs must be distinct and nonempty, got %q and %q", firstID, secondID)
	}
	if got := second["upstream"]; got != "service.example:8443/base" {
		t.Errorf("second log upstream = %v", got)
	}
	if got := second["chaos_applied"]; got != false {
		t.Errorf("second log chaos_applied = %v", got)
	}
}

func TestNilObserver(t *testing.T) {
	var observer *Observer
	request := observer.Begin(nil, "")
	request.Policy(time.Second, time.Second)
	request.Fault(true)
	request.Delay("request", time.Second, time.Second)
	request.Upstream(time.Second)
	request.Invalid()
	request.Finish(http.StatusOK, false)
}

func scrapeMetrics(t *testing.T, observer *Observer) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	observer.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics handler status = %d, want %d", recorder.Code, http.StatusOK)
	}
	return recorder.Body.String()
}

func assertSamples(t *testing.T, output string, samples ...string) {
	t.Helper()
	for _, sample := range samples {
		if !strings.Contains(output, "\n"+sample+"\n") {
			t.Errorf("scrape is missing sample %q", sample)
		}
	}
}
