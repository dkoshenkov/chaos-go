package tcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/config"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

const (
	connectTimeoutHeader  = "X-Chaos-Timeout-Ms"
	maxConnectHeaderBytes = 32 << 10
)

var errConnectHeaderTooLarge = errors.New("CONNECT headers exceed 32 KiB")

// ServeConnect accepts HTTP/1.1 CONNECT handshakes for one fixed upstream.
func ServeConnect(ctx context.Context, listener net.Listener, opts Options, settings chaos.Settings, maxTimeout, headerTimeout, shutdownTimeout time.Duration, observer *observability.Observer) error {
	target, err := parseConnectAuthority(opts.Upstream)
	if err != nil {
		return fmt.Errorf("invalid CONNECT upstream: %w", err)
	}
	if maxTimeout < 0 || headerTimeout <= 0 || opts.DialTimeout < 0 || settings.MaxActiveFaults < 0 || settings.ResetAfterBytes < 0 {
		return errors.New("invalid CONNECT proxy limits")
	}
	h := connectServer{
		upstream:      opts.Upstream,
		target:        target,
		dialer:        net.Dialer{Timeout: opts.DialTimeout},
		settings:      settings,
		maxTimeout:    maxTimeout,
		headerTimeout: headerTimeout,
		active:        make(chan struct{}, settings.MaxActiveFaults),
		observer:      observer,
	}
	return serve(ctx, listener, shutdownTimeout, h.handle)
}

type connectServer struct {
	upstream      string
	target        connectAuthority
	dialer        net.Dialer
	settings      chaos.Settings
	maxTimeout    time.Duration
	headerTimeout time.Duration
	active        chan struct{}
	observer      *observability.Observer
}

func (h *connectServer) handle(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	stopShutdown := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopShutdown()

	client, ok := conn.(tcpConn)
	if !ok {
		return
	}
	observation := h.observer.Begin((&http.Request{Method: http.MethodConnect}).WithContext(ctx), h.upstream)
	defer observation.Finish(0, true)

	if err := client.SetReadDeadline(time.Now().Add(h.headerTimeout)); err != nil {
		return
	}
	reader := bufio.NewReader(client)
	if _, err := reader.Peek(1); err != nil {
		if isNetworkTimeout(err) {
			h.respond(client, http.StatusRequestTimeout, observation)
		}
		return
	}
	received := time.Now()
	request, err := readConnectRequest(reader)
	if err != nil {
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, errConnectHeaderTooLarge):
			status = http.StatusRequestHeaderFieldsTooLarge
		case isNetworkTimeout(err):
			status = http.StatusRequestTimeout
		}
		h.respond(client, status, observation)
		return
	}
	if status := h.validateRequest(request); status != 0 {
		h.respond(client, status, observation)
		return
	}
	policy, timeout, status := h.parsePolicy(request.header)
	if status != 0 {
		if status == http.StatusBadRequest {
			observation.Invalid()
		}
		h.respond(client, status, observation)
		return
	}

	tcpObservation := h.observer.BeginTCP()
	result := relayResult{outcome: "limited"}
	var slotHeld bool
	defer func() {
		tcpObservation.Finish(result.outcome, result.sentUpstream, result.sentDownstream)
		if slotHeld {
			<-h.active
		}
	}()

	observation.Policy(policy.RequestDelay, policy.ResponseDelay)
	if (policy.HasFault() || timeout > 0) && policy.Selected() {
		select {
		case h.active <- struct{}{}:
			slotHeld = true
			tcpObservation.Fault()
		default:
			h.respond(client, http.StatusTooManyRequests, observation)
			return
		}
	} else {
		policy, timeout = chaos.Policy{}, 0
	}

	if err := client.SetReadDeadline(time.Time{}); err != nil {
		result.outcome = "io_error"
		return
	}
	result = h.tunnel(ctx, client, reader, received, policy, timeout, observation, tcpObservation)
}

type connectRequest struct {
	method   string
	target   string
	protocol string
	header   http.Header
}

func (h *connectServer) validateRequest(request connectRequest) int {
	if request.protocol != "HTTP/1.1" {
		return http.StatusBadRequest
	}
	if request.method != http.MethodConnect {
		if request.method == "" || strings.ContainsFunc(request.method, func(c rune) bool {
			return !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-.^`_|~", c)
		}) {
			return http.StatusBadRequest
		}
		return http.StatusMethodNotAllowed
	}
	authority, err := parseConnectAuthority(request.target)
	if err != nil {
		return http.StatusBadRequest
	}
	if authority != h.target {
		return http.StatusForbidden
	}
	hosts := request.header.Values("Host")
	if len(hosts) != 1 || len(request.header.Values("Transfer-Encoding")) != 0 {
		return http.StatusBadRequest
	}
	hostAuthority, err := parseConnectAuthority(hosts[0])
	if err != nil || authority != hostAuthority {
		return http.StatusBadRequest
	}
	if lengths := request.header.Values("Content-Length"); len(lengths) != 0 {
		if len(lengths) != 1 || lengths[0] == "" || strings.Trim(lengths[0], "0") != "" {
			return http.StatusBadRequest
		}
	}
	return 0
}

type connectAuthority struct {
	host string
	port uint16
}

func parseConnectAuthority(value string) (connectAuthority, error) {
	address, err := config.TCPAddress(value)
	if err != nil || address != value {
		return connectAuthority{}, errors.New("authority must be host:port")
	}
	host, port, _ := net.SplitHostPort(address)
	portNumber, _ := strconv.ParseUint(port, 10, 16)
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.String()
	} else {
		host = strings.ToLower(host)
	}
	return connectAuthority{host: host, port: uint16(portNumber)}, nil
}

func readConnectRequest(reader *bufio.Reader) (connectRequest, error) {
	var request connectRequest
	var head bytes.Buffer
	for {
		start := head.Len()
		if start == maxConnectHeaderBytes {
			return request, errConnectHeaderTooLarge
		}
		for {
			fragment, err := reader.ReadSlice('\n')
			if len(fragment) > maxConnectHeaderBytes-head.Len() {
				return request, errConnectHeaderTooLarge
			}
			head.Write(fragment)
			if errors.Is(err, bufio.ErrBufferFull) && head.Len() < maxConnectHeaderBytes {
				continue
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				return request, errConnectHeaderTooLarge
			}
			if err != nil {
				return request, err
			}
			break
		}
		line, ok := bytes.CutSuffix(head.Bytes()[start:], []byte("\r\n"))
		if !ok {
			return request, errors.New("invalid CONNECT header line ending")
		}
		if start == 0 {
			method, rest, ok := strings.Cut(string(line), " ")
			target, protocol, split := strings.Cut(rest, " ")
			if !ok || !split || target == "" {
				return request, errors.New("invalid CONNECT request line")
			}
			request.method, request.target, request.protocol = method, target, protocol
			continue
		}
		if len(line) == 0 {
			break
		}
		name, _, ok := bytes.Cut(line, []byte(":"))
		if !ok || len(name) == 0 || bytes.ContainsAny(name, " \t") {
			return request, errors.New("invalid CONNECT header name")
		}
	}
	if _, err := head.ReadBytes('\n'); err != nil {
		return request, err
	}
	headers, err := textproto.NewReader(bufio.NewReader(&head)).ReadMIMEHeader()
	request.header = http.Header(headers)
	return request, err
}

func (h *connectServer) parsePolicy(headers http.Header) (chaos.Policy, time.Duration, int) {
	if !h.settings.Enabled || !chaos.HasControls(headers) {
		return chaos.Policy{}, 0, 0
	}
	if !chaos.Authorized(headers, h.settings.Token) {
		return chaos.Policy{}, 0, http.StatusForbidden
	}

	var timeout time.Duration
	if values, ok := headers[connectTimeoutHeader]; ok {
		if len(values) != 1 {
			return chaos.Policy{}, 0, http.StatusBadRequest
		}
		ms, err := strconv.ParseUint(strings.Trim(values[0], " \t"), 10, 64)
		if err != nil || ms > uint64(h.maxTimeout.Milliseconds()) {
			return chaos.Policy{}, 0, http.StatusBadRequest
		}
		timeout = time.Duration(ms) * time.Millisecond
		headers.Del(connectTimeoutHeader)
	}
	policy, err := chaos.Parse(headers, h.settings)
	if err != nil || policy.StatusCode != 0 || policy.RequestBandwidth != 0 || policy.ResponseBandwidth != 0 {
		return chaos.Policy{}, 0, http.StatusBadRequest
	}
	return policy, timeout, 0
}

func (h *connectServer) tunnel(ctx context.Context, client tcpConn, reader *bufio.Reader, received time.Time, policy chaos.Policy, timeout time.Duration, observation *observability.Request, tcpObservation *observability.TCPConnection) relayResult {
	tunnelCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		tunnelCtx, cancel = context.WithDeadline(ctx, received.Add(timeout))
		defer cancel()
	}
	peerCtx, stopWatch := watchConnectPeer(tunnelCtx, client, reader)
	defer func() { _ = stopWatch() }()

	sampled := policy.SampleJitter(h.settings)
	observation.Policy(sampled.RequestDelay, sampled.ResponseDelay)
	if err := waitConnect(peerCtx, observation.Wait, "request", sampled.RequestDelay, policy.RequestDelay, policy.RequestJitter); err != nil {
		return h.canceledHandshake(ctx, tunnelCtx, client, observation)
	}
	if policy.ConnectionReset && h.settings.ResetAfterBytes == 0 {
		err := waitConnect(peerCtx, observation.Wait, "response", sampled.ResponseDelay, policy.ResponseDelay, policy.ResponseJitter)
		if err != nil {
			return h.canceledHandshake(ctx, tunnelCtx, client, observation)
		}
		if err := stopWatch(); err != nil {
			return h.canceledHandshake(ctx, tunnelCtx, client, observation)
		}
		if err := resetTCP(client); err != nil {
			return relayResult{outcome: "io_error"}
		}
		observation.Finish(0, false)
		return relayResult{outcome: "reset"}
	}
	start := time.Now()
	conn, err := h.dialer.DialContext(peerCtx, "tcp", h.upstream)
	observation.Upstream(time.Since(start))
	if err != nil {
		if peerCtx.Err() != nil {
			return h.canceledHandshake(ctx, tunnelCtx, client, observation)
		}
		if isNetworkTimeout(err) {
			h.respond(client, http.StatusGatewayTimeout, observation)
			return relayResult{outcome: "timeout"}
		}
		h.respond(client, http.StatusBadGateway, observation)
		return relayResult{outcome: "dial_error"}
	}
	var upstream tcpConn = conn.(*net.TCPConn)
	defer func() { _ = upstream.Close() }()
	if err := stopWatch(); err != nil {
		return h.canceledHandshake(ctx, tunnelCtx, client, observation)
	}

	deadline := time.Now().Add(h.headerTimeout)
	if end, ok := tunnelCtx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := client.SetWriteDeadline(deadline); err != nil {
		return relayResult{outcome: "io_error"}
	}
	if _, err := io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return relayResult{outcome: connectFailure(ctx, tunnelCtx)}
	}
	observation.Finish(http.StatusOK, false)
	if err := client.SetWriteDeadline(time.Time{}); err != nil {
		return relayResult{outcome: "io_error"}
	}
	var resetAfterBytes int64
	if policy.ConnectionReset {
		resetAfterBytes = h.settings.ResetAfterBytes
	}
	if policy.ResponseDelay > 0 || policy.ResponseJitter > 0 {
		delayCtx, cancel := context.WithCancel(tunnelCtx)
		upstream = &connectDelayedUpstream{
			tcpConn: upstream,
			cancel:  cancel,
			wait: func() error {
				return waitConnect(delayCtx, tcpObservation.Wait, "response", sampled.ResponseDelay, policy.ResponseDelay, policy.ResponseJitter)
			},
		}
	}
	result := bridge(tunnelCtx, client, reader, upstream, nil, resetAfterBytes)
	if result.outcome == "shutdown" && errors.Is(tunnelCtx.Err(), context.DeadlineExceeded) {
		result.outcome = "timeout"
	}
	return result
}

// Apply the response delay once, when the broker has produced its first bytes.
// Closing this connection also ends an in-progress wait before the relay joins it.
type connectDelayedUpstream struct {
	tcpConn
	cancel context.CancelFunc
	wait   func() error
}

func (conn *connectDelayedUpstream) Read(p []byte) (int, error) {
	n, err := conn.tcpConn.Read(p)
	if n > 0 && conn.wait != nil {
		waitErr := conn.wait()
		conn.wait = nil
		if waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}

func (conn *connectDelayedUpstream) Close() error {
	conn.cancel()
	return conn.tcpConn.Close()
}

// Before the handshake, read only what fits in the existing buffer. An early
// EOF with pipelined data may be a half-close; the relay must still send it.
func watchConnectPeer(ctx context.Context, conn net.Conn, reader *bufio.Reader) (context.Context, func() error) {
	peerCtx, cancel := context.WithCancel(ctx)
	stopping := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for reader.Buffered() < reader.Size() {
			if _, err := reader.Peek(reader.Buffered() + 1); err != nil {
				select {
				case <-stopping:
					return
				default:
				}
				if !errors.Is(err, io.EOF) || reader.Buffered() == 0 {
					cancel()
				}
				return
			}
		}
	}()
	return peerCtx, sync.OnceValue(func() error {
		close(stopping)
		_ = conn.SetReadDeadline(time.Now())
		<-done
		_ = conn.SetReadDeadline(time.Time{})
		err := peerCtx.Err()
		cancel()
		return err
	})
}

func waitConnect(ctx context.Context, observe func(string, time.Duration, time.Duration, bool, bool), direction string, effective, base, jitter time.Duration) error {
	if base == 0 && jitter == 0 {
		return ctx.Err()
	}
	start := time.Now()
	err := chaos.Wait(ctx, effective)
	observe(direction, effective, time.Since(start), base > 0, jitter > 0)
	return err
}

func (h *connectServer) canceledHandshake(ctx, tunnelCtx context.Context, client net.Conn, observation *observability.Request) relayResult {
	outcome := connectFailure(ctx, tunnelCtx)
	switch outcome {
	case "timeout":
		h.respond(client, http.StatusGatewayTimeout, observation)
	case "io_error":
		outcome = "closed"
	}
	return relayResult{outcome: outcome}
}

func connectFailure(ctx, tunnelCtx context.Context) string {
	switch tunnelCtx.Err() {
	case context.DeadlineExceeded:
		return "timeout"
	default:
		if ctx.Err() != nil {
			return "shutdown"
		}
		return "io_error"
	}
}

func (h *connectServer) respond(conn net.Conn, status int, observation *observability.Request) {
	err := conn.SetWriteDeadline(time.Now().Add(h.headerTimeout))
	if err == nil {
		allow := ""
		if status == http.StatusMethodNotAllowed {
			allow = "Allow: CONNECT\r\n"
		}
		_, err = fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\n%sConnection: close\r\nContent-Length: 0\r\n\r\n", status, http.StatusText(status), allow)
	}
	observation.Finish(status, err != nil)
}

func isNetworkTimeout(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}
