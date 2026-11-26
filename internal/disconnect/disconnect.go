// Package disconnect supplements HTTP/1 request cancellation during fault delays.
package disconnect

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
)

type connKey struct{}

// ConnContext gives the proxy access to the downstream TCP connection.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	if tcp, ok := conn.(*net.TCPConn); ok {
		return context.WithValue(ctx, connKey{}, tcp)
	}
	return ctx
}

// Wait pauses for a delay and closes the downstream socket if the peer hangs up
// before net/http can detect it. HTTP/2 relies on its per-stream context.
func Wait(ctx context.Context, r *http.Request, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 || r.ProtoMajor != 1 || r.Body == nil || r.Body == http.NoBody {
		return chaos.Wait(ctx, delay)
	}
	tcp, _ := ctx.Value(connKey{}).(*net.TCPConn)
	if tcp == nil {
		return chaos.Wait(ctx, delay)
	}
	peerClosed := tcpCloseProbe(tcp)
	if peerClosed == nil {
		return chaos.Wait(ctx, delay)
	}

	// net/http cannot start its background disconnect read until the body has
	// been consumed. Probe without reading from the socket or parking an OS thread.
	const pollInterval = 50 * time.Millisecond
	deadline := time.Now().Add(delay)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		if peerClosed() {
			// Prevent net/http from writing an implicit success response when the
			// handler returns without writing headers on the canceled request.
			_ = tcp.Close()
			return context.Canceled
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ctx.Err()
		}
		timer.Reset(min(remaining, pollInterval))
	}
}
