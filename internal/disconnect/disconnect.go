// Package disconnect supplements HTTP/1 request cancellation during faults.
package disconnect

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
)

type connKey struct{}

const pollInterval = 50 * time.Millisecond

// ConnContext gives the proxy access to the downstream TCP connection.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	if tcp, ok := conn.(*net.TCPConn); ok {
		return context.WithValue(ctx, connKey{}, tcp)
	}
	return ctx
}

func bodyConn(ctx context.Context, r *http.Request) *net.TCPConn {
	if r.ProtoMajor != 1 || r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	tcp, _ := ctx.Value(connKey{}).(*net.TCPConn)
	return tcp
}

// WatchBody cancels a throttled HTTP/1 exchange if the downstream peer closes
// while net/http is still waiting for the handler to consume the request body.
// The caller must stop the watch when the exchange finishes.
func WatchBody(ctx context.Context, r *http.Request) (context.Context, func()) {
	tcp := bodyConn(ctx, r)
	if tcp == nil {
		return ctx, func() {}
	}
	peerClosed := tcpCloseProbe(tcp)
	if peerClosed == nil {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if peerClosed() {
					_ = tcp.Close()
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() {
		cancel()
		<-done
	}
}

// Wait pauses for a delay and closes the downstream socket if the peer hangs up
// before net/http can detect it. HTTP/2 relies on its per-stream context.
func Wait(ctx context.Context, r *http.Request, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return chaos.Wait(ctx, delay)
	}
	tcp := bodyConn(ctx, r)
	if tcp == nil {
		return chaos.Wait(ctx, delay)
	}
	peerClosed := tcpCloseProbe(tcp)
	if peerClosed == nil {
		return chaos.Wait(ctx, delay)
	}

	// net/http cannot start its background disconnect read until the body has
	// been consumed. Probe without reading from the socket or parking an OS thread.
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
