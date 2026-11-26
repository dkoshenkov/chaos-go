package observability

import (
	"context"
	"crypto/rand"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Observer records proxy metrics and logs for one server.
type Observer struct {
	logger           *slog.Logger
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	requestDuration  prometheus.Histogram
	upstreamDuration prometheus.Histogram
	injections       *prometheus.CounterVec
	injectedDelay    *prometheus.HistogramVec
	requestedDelay   *prometheus.HistogramVec
	activeRequests   prometheus.Gauge
	activeFaults     prometheus.Gauge
	canceledRequests prometheus.Counter
	validationErrors prometheus.Counter
}

// New creates an observer with a private Prometheus registry. A nil logger disables request logging.
func New(logger *slog.Logger) *Observer {
	buckets := []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30, 60, 120, 180, 240, 300,
	}
	observer := &Observer{
		logger:   logger,
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chaos_proxy_requests_total",
			Help: "Total proxy requests completed, including rejected and canceled requests.",
		}, []string{"method", "status_code"}),
		requestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chaos_proxy_request_duration_seconds",
			Help:    "Time spent handling a proxy request, including injected delays and response streaming.",
			Buckets: buckets,
		}),
		upstreamDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chaos_proxy_upstream_duration_seconds",
			Help:    "Time spent on upstream round trips through response headers or a transport error.",
			Buckets: buckets,
		}),
		injections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chaos_proxy_injections_total",
			Help: "Total fault injections applied to proxy requests.",
		}, []string{"fault_type", "direction"}),
		injectedDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chaos_proxy_injected_delay_seconds",
			Help:    "Elapsed time spent in injected delay waits.",
			Buckets: buckets,
		}, []string{"direction"}),
		requestedDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chaos_proxy_requested_delay_seconds",
			Help:    "Requested durations of injected delay waits.",
			Buckets: buckets,
		}, []string{"direction"}),
		activeRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chaos_proxy_active_requests",
			Help: "Proxy requests currently being handled.",
		}),
		activeFaults: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chaos_proxy_active_faults",
			Help: "Requests currently holding a fault slot.",
		}),
		canceledRequests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chaos_proxy_canceled_requests_total",
			Help: "Total proxy requests canceled before completion.",
		}),
		validationErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "chaos_proxy_validation_errors_total",
			Help:        "Total requests rejected for invalid fault policies.",
			ConstLabels: prometheus.Labels{"reason": "invalid_policy"},
		}),
	}
	observer.registry.MustRegister(
		observer.requests,
		observer.requestDuration,
		observer.upstreamDuration,
		observer.injections,
		observer.injectedDelay,
		observer.requestedDelay,
		observer.activeRequests,
		observer.activeFaults,
		observer.canceledRequests,
		observer.validationErrors,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	for _, direction := range []string{"request", "response"} {
		for _, faultType := range []string{"delay", "jitter", "bandwidth"} {
			observer.injections.WithLabelValues(faultType, direction)
		}
		observer.injectedDelay.WithLabelValues(direction)
		observer.requestedDelay.WithLabelValues(direction)
	}
	observer.injections.WithLabelValues("abort", "response")
	observer.injections.WithLabelValues("connection_reset", "response")
	return observer
}

// MetricsHandler serves the server's Prometheus registry.
func (o *Observer) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(o.registry, promhttp.HandlerOpts{})
}

type appliedFault struct {
	Type      string `json:"fault_type"`
	Direction string `json:"direction"`
}

// Request holds observability state for one proxy request.
type Request struct {
	observer         *Observer
	ctx              context.Context
	started          time.Time
	method           string
	path             string
	upstream         string
	requestDelay     time.Duration
	responseDelay    time.Duration
	upstreamDuration time.Duration

	// The upstream transport may read the request and report bandwidth
	// injection concurrently with response handling and completion.
	mu            sync.Mutex
	appliedFaults []appliedFault
	faultActive   bool
	finished      bool
}

// Begin records a request as active and returns its observation state.
func (o *Observer) Begin(r *http.Request, upstream string) *Request {
	if o == nil {
		return nil
	}
	started := time.Now()
	o.activeRequests.Inc()
	state := &Request{observer: o, started: started, method: r.Method}
	if o.logger != nil {
		state.appliedFaults = []appliedFault{}
		state.ctx = r.Context()
		if r.URL != nil {
			state.path = r.URL.EscapedPath()
		}
		state.upstream = sanitizeUpstream(upstream)
	}
	return state
}

// Policy records the effective requested delays for the completion log.
func (r *Request) Policy(requestDelay, responseDelay time.Duration) {
	if r == nil {
		return
	}
	r.requestDelay = requestDelay
	r.responseDelay = responseDelay
}

// Fault tracks whether the request holds a fault slot.
func (r *Request) Fault(active bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || r.faultActive == active {
		return
	}
	r.faultActive = active
	if active {
		r.observer.activeFaults.Inc()
	} else {
		r.observer.activeFaults.Dec()
	}
}

// Delay records an injection and the requested and elapsed durations of its wait.
func (r *Request) Delay(direction string, requested, actual time.Duration) {
	r.Wait(direction, requested, actual, true, false)
}

// Wait records the effective wait and the delay and jitter that produced it.
func (r *Request) Wait(direction string, effective, actual time.Duration, baseDelay, jitter bool) {
	if r == nil || !validDirection(direction) {
		return
	}
	if baseDelay {
		r.Injection("delay", direction)
	}
	if jitter {
		r.Injection("jitter", direction)
	}
	if baseDelay || effective > 0 {
		r.observer.requestedDelay.WithLabelValues(direction).Observe(effective.Seconds())
		r.observer.injectedDelay.WithLabelValues(direction).Observe(actual.Seconds())
	}
}

// Injection records an applied fault. It may be called by a streaming transport.
func (r *Request) Injection(faultType, direction string) {
	if r == nil || !validDirection(direction) {
		return
	}
	switch faultType {
	case "abort", "connection_reset":
		if direction != "response" {
			return
		}
	case "delay", "jitter", "bandwidth":
	default:
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	r.observer.injections.WithLabelValues(faultType, direction).Inc()
	if r.observer.logger != nil {
		fault := appliedFault{Type: faultType, Direction: direction}
		if !slices.Contains(r.appliedFaults, fault) {
			r.appliedFaults = append(r.appliedFaults, fault)
		}
	}
}

func validDirection(direction string) bool {
	return direction == "request" || direction == "response"
}

// Upstream records a completed upstream round trip through headers or a transport error.
func (r *Request) Upstream(duration time.Duration) {
	if r == nil {
		return
	}
	r.upstreamDuration = duration
	r.observer.upstreamDuration.Observe(duration.Seconds())
}

// Invalid records a request rejected for invalid fault policy values.
func (r *Request) Invalid() {
	if r == nil {
		return
	}
	r.observer.validationErrors.Inc()
}

// Finish records the outcome, releases active gauges, and logs the request.
func (r *Request) Finish(status int, canceled bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	r.finished = true
	if r.faultActive {
		r.faultActive = false
		r.observer.activeFaults.Dec()
	}
	r.mu.Unlock()
	duration := time.Since(r.started)
	r.observer.activeRequests.Dec()
	r.observer.requests.WithLabelValues(methodLabel(r.method), statusLabel(status)).Inc()
	r.observer.requestDuration.Observe(duration.Seconds())
	if canceled {
		r.observer.canceledRequests.Inc()
	}
	if r.observer.logger != nil && r.observer.logger.Enabled(r.ctx, slog.LevelInfo) {
		r.observer.logger.LogAttrs(r.ctx, slog.LevelInfo, "proxy request",
			slog.String("request_id", rand.Text()),
			slog.String("method", r.method),
			slog.String("path", r.path),
			slog.String("upstream", r.upstream),
			slog.Bool("chaos_applied", len(r.appliedFaults) != 0),
			slog.Any("applied_faults", r.appliedFaults),
			slog.Float64("request_delay_ms", milliseconds(r.requestDelay)),
			slog.Float64("response_delay_ms", milliseconds(r.responseDelay)),
			slog.Float64("upstream_duration_ms", milliseconds(r.upstreamDuration)),
			slog.Float64("total_duration_ms", milliseconds(duration)),
			slog.Int("status_code", status),
			slog.Bool("canceled", canceled),
		)
	}
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

func statusLabel(status int) string {
	if status < 100 || status > 999 {
		return "unknown"
	}
	return strconv.Itoa(status)
}

func sanitizeUpstream(upstream string) string {
	target, err := url.Parse(upstream)
	if err != nil || target.Host == "" {
		target, err = url.Parse("//" + upstream)
	}
	if err != nil || target.Host == "" {
		return ""
	}
	clean := url.URL{Scheme: target.Scheme, Host: target.Host, Path: target.Path, RawPath: target.RawPath}
	if clean.Scheme == "" {
		return clean.Host + clean.EscapedPath()
	}
	return clean.String()
}
