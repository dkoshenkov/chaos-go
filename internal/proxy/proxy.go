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

// NewHandler returns a proxy with per-request delay controls and shared limits.
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
	if transport == nil {
		transport = http.DefaultTransport
	}

	reverse := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			stripChaosFields(request.Out.Header)
			stripForwardingFields(request.Out.Header)
			rewriteRequestTrailers(request.Out)
			request.SetURL(target)
			request.SetXForwarded()
		},
		Transport: timedTransport{transport},
		ModifyResponse: func(response *http.Response) error {
			stripChaosFields(response.Header)
			stripChaosFields(response.Trailer)
			state := exchangeFrom(response.Request.Context())
			return state.wait(response.Request.Context(), "response", state.policy.ResponseDelay)
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
	state := &exchange{
		request:     r,
		observation: h.observer.Begin(r, h.upstream),
	}
	writer := &responseWriter{ResponseWriter: w}
	defer func() {
		// ReverseProxy copies late trailers after the final body write.
		stripChaosFields(writer.Header())
		state.observation.Finish(writer.status, state.delayCanceled || r.Context().Err() != nil)
	}()

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
		state.policy = policy
		state.observation.Policy(policy.RequestDelay, policy.ResponseDelay)
	}

	if r.Context().Err() != nil {
		return
	}
	if state.policy.RequestDelay > 0 || state.policy.ResponseDelay > 0 {
		select {
		case h.active <- struct{}{}:
			state.observation.Fault(true)
			defer func() {
				state.observation.Fault(false)
				<-h.active
			}()
		default:
			http.Error(writer, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
	}

	if err := state.wait(r.Context(), "request", state.policy.RequestDelay); err != nil {
		return
	}
	ctx := context.WithValue(r.Context(), exchangeKey{}, state)
	h.reverse.ServeHTTP(writer, r.WithContext(ctx))
}

type exchangeKey struct{}

type exchange struct {
	request       *http.Request
	policy        chaos.Policy
	observation   *observability.Request
	delayCanceled bool
}

func exchangeFrom(ctx context.Context) *exchange {
	return ctx.Value(exchangeKey{}).(*exchange)
}

func (e *exchange) wait(ctx context.Context, direction string, delay time.Duration) error {
	if delay == 0 {
		return ctx.Err()
	}

	start := time.Now()
	err := disconnect.Wait(ctx, e.request, delay)
	e.delayCanceled = err != nil
	e.observation.Delay(direction, delay, time.Since(start))
	return err
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
