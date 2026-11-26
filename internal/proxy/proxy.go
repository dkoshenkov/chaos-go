package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/disconnect"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

// New returns a reverse proxy for the fixed upstream URL.
func New(upstream string) (http.Handler, error) {
	return NewHandler(upstream, chaos.Settings{}, nil, nil)
}

// NewHandler returns a proxy with per-request fault controls and shared limits.
func NewHandler(upstream string, settings chaos.Settings, transport http.RoundTripper, observer *observability.Observer) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, errors.New("invalid upstream URL")
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" {
		return nil, errors.New("upstream must be an absolute HTTP or HTTPS URL with a host")
	}
	if target.User != nil || strings.Contains(upstream, "#") {
		return nil, errors.New("upstream URL must not include user information or a fragment")
	}
	if settings.MaxActiveFaults < 0 {
		return nil, errors.New("maximum active faults must be nonnegative")
	}
	if settings.ResetAfterBytes < 0 {
		return nil, errors.New("connection reset threshold must be nonnegative")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}

	reverse := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			stripChaosFields(request.Out.Header)
			stripForwardingFields(request.Out.Header)
			rewriteRequestTrailers(request.Out)
			state := exchangeFrom(request.Out.Context())
			if bandwidth := state.policy.RequestBandwidth; bandwidth > 0 && request.Out.Body != nil && request.In.Body != http.NoBody {
				request.Out.Body = state.limitBody(request.Out.Context(), "request", request.Out.Body, bandwidth)
			}
			request.SetURL(target)
			request.SetXForwarded()
		},
		Transport: timedTransport{transport},
		ModifyResponse: func(response *http.Response) error {
			stripChaosFields(response.Header)
			stripChaosFields(response.Trailer)
			ctx := response.Request.Context()
			state := exchangeFrom(ctx)
			if err := state.wait(ctx, "response"); err != nil {
				return err
			}
			if bandwidth := state.policy.ResponseBandwidth; bandwidth > 0 && responseHasBody(response) {
				response.Body = state.limitBody(ctx, "response", response.Body, bandwidth)
			}
			return nil
		},
		ErrorHandler: proxyError,
		// The observer supplies the request log. Transport errors may contain
		// raw header values, so do not log them through ReverseProxy.
		ErrorLog: log.New(io.Discard, "", 0),
	}

	return &handler{
		reverse:  reverse,
		settings: settings,
		active:   make(chan struct{}, settings.MaxActiveFaults),
		observer: observer,
		upstream: target.Host,
	}, nil
}

type handler struct {
	reverse  *httputil.ReverseProxy
	settings chaos.Settings
	active   chan struct{}
	observer *observability.Observer
	upstream string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var stopBodyWatch func()
	var slotHeld bool
	state := &exchange{
		request:     r,
		observation: h.observer.Begin(r, h.upstream),
	}
	writer := &responseWriter{ResponseWriter: w}
	defer func() {
		if slotHeld {
			defer func() { <-h.active }()
		}
		canceled := !state.reset && (state.delayCanceled || ctx.Err() != nil)
		if stopBodyWatch != nil {
			stopBodyWatch()
		}
		// ReverseProxy copies late trailers after the final body write.
		stripChaosFields(writer.Header())
		state.observation.Finish(writer.status, canceled)
	}()

	selected := false
	if h.settings.Enabled && chaos.HasControls(r.Header) {
		if !chaos.Authorized(r.Header, h.settings.Token) {
			http.Error(writer, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		policy, err := chaos.Parse(r.Header, h.settings)
		if err != nil {
			state.observation.Invalid()
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		state.observation.Policy(policy.RequestDelay, policy.ResponseDelay)
		if policy.HasFault() && policy.Selected() {
			state.policy = policy
			selected = true
		}
	}

	if ctx.Err() != nil {
		return
	}
	if selected {
		select {
		case h.active <- struct{}{}:
			slotHeld = true
			state.observation.Fault(true)
		default:
			http.Error(writer, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		sampled := state.policy.SampleJitter(h.settings)
		state.requestDelay = sampled.RequestDelay
		state.responseDelay = sampled.ResponseDelay
		state.observation.Policy(sampled.RequestDelay, sampled.ResponseDelay)
	}

	policy := state.policy
	if policy.RequestBandwidth > 0 || policy.ResponseBandwidth > 0 {
		ctx, stopBodyWatch = disconnect.WatchBody(ctx, r)
	}
	if err := state.wait(ctx, "request"); err != nil {
		return
	}
	immediateReset := policy.ConnectionReset && h.settings.ResetAfterBytes == 0
	if immediateReset || policy.StatusCode != 0 {
		if err := state.wait(ctx, "response"); err != nil {
			return
		}
		if immediateReset {
			state.recordReset()
			resetDownstream(w, r)
			return
		}
		if r.ProtoMajor == 1 && r.Body != nil && r.Body != http.NoBody {
			// Send the terminal status without waiting for the client to finish uploading.
			writer.Header().Set("Connection", "close")
		}
		state.observation.Injection("abort", "response")
		writer.WriteHeader(policy.StatusCode)
		return
	}

	proxyCtx := context.WithValue(ctx, exchangeKey{}, state)
	if policy.RequestBandwidth > 0 {
		// Stop an outstanding upload wait when upstream responds without reading it.
		var cancel context.CancelFunc
		proxyCtx, cancel = context.WithCancel(proxyCtx)
		defer cancel()
	}
	request := r.WithContext(proxyCtx)
	reverse := h.reverse
	if policy.ResponseBandwidth > 0 {
		// Flush paced chunks even when the upstream supplies a Content-Length.
		streaming := *reverse
		streaming.FlushInterval = -1
		reverse = &streaming
	}
	if policy.ConnectionReset {
		resetter := resetAfterWriter(writer, request, h.settings.ResetAfterBytes, state.recordReset)
		// ReverseProxy must close the upstream body before aborting the client.
		defer resetter.abort()
		reverse.ServeHTTP(resetter, request)
		return
	}
	reverse.ServeHTTP(writer, request)
}

type exchangeKey struct{}

type exchange struct {
	request       *http.Request
	policy        chaos.Policy
	observation   *observability.Request
	requestDelay  time.Duration
	responseDelay time.Duration
	delayCanceled bool
	reset         bool
}

func exchangeFrom(ctx context.Context) *exchange {
	return ctx.Value(exchangeKey{}).(*exchange)
}

func (e *exchange) wait(ctx context.Context, direction string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay, base, jitter := e.requestDelay, e.policy.RequestDelay, e.policy.RequestJitter
	if direction == "response" {
		delay, base, jitter = e.responseDelay, e.policy.ResponseDelay, e.policy.ResponseJitter
	}
	if delay == 0 {
		if jitter > 0 {
			e.observation.Wait(direction, 0, 0, false, true)
		}
		return nil
	}

	start := time.Now()
	err := disconnect.Wait(ctx, e.request, delay)
	e.delayCanceled = err != nil
	e.observation.Wait(direction, delay, time.Since(start), base > 0, jitter > 0)
	return err
}

func (e *exchange) recordReset() {
	e.reset = true
	e.observation.Injection("connection_reset", "response")
}

func (e *exchange) limitBody(ctx context.Context, direction string, body io.ReadCloser, kbps int64) io.ReadCloser {
	return limitBody(ctx, &observedBody{
		ReadCloser: body,
		onRead:     func() { e.observation.Injection("bandwidth", direction) },
	}, kbps)
}

type observedBody struct {
	io.ReadCloser
	onRead func()
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 && b.onRead != nil {
		b.onRead()
		b.onRead = nil
	}
	return n, err
}

func responseHasBody(response *http.Response) bool {
	return response.Body != nil && response.Body != http.NoBody &&
		response.Request.Method != http.MethodHead && response.StatusCode >= 200 &&
		response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotModified
}

type timedTransport struct {
	http.RoundTripper
}

func (t timedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()
	response, err := t.RoundTripper.RoundTrip(r)
	exchangeFrom(r.Context()).observation.Upstream(time.Since(start))
	if err == nil {
		// RoundTripper implementations may omit Response.Request.
		response.Request = r
	}
	return response, err
}

func proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil || exchangeFrom(r.Context()).delayCanceled {
		return
	}

	status := http.StatusBadGateway
	var networkError net.Error
	switch {
	case r.Method == http.MethodConnect:
		status = http.StatusMethodNotAllowed
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &networkError) && networkError.Timeout():
		status = http.StatusGatewayTimeout
	}
	w.WriteHeader(status)
}

func rewriteRequestTrailers(out *http.Request) {
	stripChaosFields(out.Trailer)
	stripForwardingFields(out.Trailer)
	if out.Body != nil && len(out.Trailer) > 0 {
		// ReverseProxy cloned the trailer map before reading the body. Read
		// values for declared trailers from the original request after EOF.
		in := exchangeFrom(out.Context()).request
		out.Body = &requestTrailerBody{ReadCloser: out.Body, in: in, trailers: out.Trailer}
	}
}

type requestTrailerBody struct {
	io.ReadCloser
	in       *http.Request
	trailers http.Header
}

func (b *requestTrailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		for name := range b.trailers {
			b.trailers[name] = b.in.Trailer[name]
		}
	}
	return n, err
}

func hasFoldPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func stripForwardingFields(header http.Header) {
	for name := range header {
		if strings.EqualFold(name, "Forwarded") || hasFoldPrefix(name, "X-Forwarded-") {
			delete(header, name)
		}
	}
}

func stripChaosFields(header http.Header) {
	for name, values := range header {
		switch {
		case hasFoldPrefix(name, "X-Chaos-"),
			hasFoldPrefix(name, http.TrailerPrefix) && hasFoldPrefix(name[len(http.TrailerPrefix):], "X-Chaos-"):
			delete(header, name)
		case strings.EqualFold(name, "Trailer"), strings.EqualFold(name, "Connection"), strings.EqualFold(name, "Proxy-Connection"):
			var kept []string
			for _, value := range values {
				fields := strings.Split(value, ",")
				remaining := fields[:0]
				for _, field := range fields {
					if !hasFoldPrefix(strings.TrimSpace(field), "X-Chaos-") {
						remaining = append(remaining, field)
					}
				}
				if len(remaining) > 0 {
					kept = append(kept, strings.Join(remaining, ","))
				}
			}
			if len(kept) > 0 {
				header[name] = kept
			} else {
				delete(header, name)
			}
		}
	}
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	stripChaosFields(w.Header())
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	stripChaosFields(w.Header())
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}
