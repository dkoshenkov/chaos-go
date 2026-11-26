// Package tcpproxy relays TCP connections to a fixed upstream.
package tcpproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

// Options configures the upstream and operator-controlled reset fault.
type Options struct {
	Upstream        string
	DialTimeout     time.Duration
	ResetEnabled    bool
	ResetAfterBytes int64
	Probability     float64
	MaxActiveFaults int
}

// Serve relays connections and drains sessions when ctx is canceled.
func Serve(ctx context.Context, listener net.Listener, opts Options, shutdownTimeout time.Duration, observer *observability.Observer) error {
	dialer := net.Dialer{Timeout: opts.DialTimeout}
	activeFaults := make(chan struct{}, opts.MaxActiveFaults)
	return serve(ctx, listener, shutdownTimeout, func(ctx context.Context, conn net.Conn) {
		observation := observer.BeginTCP()
		result := relayResult{outcome: "io_error"}
		slotHeld := false
		defer func() {
			_ = conn.Close()
			observation.Finish(result.outcome, result.sentUpstream, result.sentDownstream)
			if slotHeld {
				<-activeFaults
			}
		}()
		if ctx.Err() != nil {
			result.outcome = "shutdown"
			return
		}
		downstream, ok := conn.(tcpConn)
		if !ok {
			return
		}
		var threshold int64
		if opts.ResetEnabled && (chaos.Policy{Probability: opts.Probability}).Selected() {
			select {
			case activeFaults <- struct{}{}:
				slotHeld = true
				observation.Fault()
			default:
				result.outcome = "limited"
				return
			}
			threshold = opts.ResetAfterBytes
			if threshold == 0 {
				if err := resetTCP(downstream); err == nil {
					result.outcome = "reset"
				}
				return
			}
		}

		upstream, err := dialer.DialContext(ctx, "tcp", opts.Upstream)
		if err != nil {
			result.outcome = "dial_error"
			var networkError net.Error
			if ctx.Err() != nil {
				result.outcome = "shutdown"
			} else if errors.As(err, &networkError) && networkError.Timeout() {
				result.outcome = "timeout"
			}
			return
		}
		defer func() { _ = upstream.Close() }()
		result = bridge(ctx, downstream, nil, upstream.(*net.TCPConn), nil, threshold)
	})
}

func serve(ctx context.Context, listener net.Listener, shutdownTimeout time.Duration, handle func(context.Context, net.Conn)) error {
	closeListener := sync.OnceFunc(func() { _ = listener.Close() })
	stopAccept := context.AfterFunc(ctx, closeListener)
	defer stopAccept()

	// Existing sessions use a separate context so they can finish during the grace period.
	sessionCtx, force := context.WithCancel(context.WithoutCancel(ctx))
	defer force()
	var sessions sync.WaitGroup
	var acceptErr error
	for ctx.Err() == nil {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				acceptErr = err
			}
			break
		}
		if ctx.Err() != nil {
			_ = conn.Close()
			break
		}
		sessions.Go(func() { handle(sessionCtx, conn) })
	}
	closeListener()
	if acceptErr != nil {
		force()
		sessions.Wait()
		return acceptErr
	}

	drained := make(chan struct{})
	go func() {
		sessions.Wait()
		close(drained)
	}()
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		force()
		<-drained
	}
	return nil
}

type tcpConn interface {
	net.Conn
	CloseWrite() error
	SetLinger(int) error
}

// Relay forwards bytes in both directions, preserving TCP half-closes.
// The optional readers take the place of their respective sockets for reading
// and can replay bytes consumed before the relay started.
// Cancellation or a copy error closes both sockets.
func Relay(ctx context.Context, downstream, upstream *net.TCPConn, downstreamReader, upstreamReader io.Reader) error {
	return bridge(ctx, downstream, downstreamReader, upstream, upstreamReader, 0).err
}

type relayResult struct {
	outcome        string
	sentUpstream   int64
	sentDownstream int64
	err            error
}

func bridge(ctx context.Context, downstream tcpConn, downstreamReader io.Reader, upstream tcpConn, upstreamReader io.Reader, resetAfterBytes int64) relayResult {
	if ctx.Err() != nil {
		_ = downstream.Close()
		_ = upstream.Close()
		return relayResult{outcome: "shutdown", err: ctx.Err()}
	}
	if downstreamReader == nil {
		downstreamReader = downstream
	}
	if upstreamReader == nil {
		upstreamReader = upstream
	}
	toUpstream := copyStream(upstream, downstreamReader, 0)
	toDownstream := copyStream(downstream, upstreamReader, resetAfterBytes)
	result := relayResult{outcome: "closed"}
	for toUpstream != nil || toDownstream != nil {
		var reset bool
		select {
		case copied := <-toUpstream:
			toUpstream = nil
			result.sentUpstream = copied.bytes
			result.err = copied.err
		case copied := <-toDownstream:
			toDownstream = nil
			result.sentDownstream = copied.bytes
			result.err = copied.err
			reset = copied.reset
		case <-ctx.Done():
		}
		switch {
		case ctx.Err() != nil:
			result.outcome = "shutdown"
			result.err = ctx.Err()
			_ = downstream.Close()
		case reset:
			result.outcome = "reset"
			if result.err = resetTCP(downstream); result.err != nil {
				result.outcome = "io_error"
			}
		case result.err != nil:
			result.outcome = "io_error"
			if errors.Is(result.err, syscall.ECONNRESET) || errors.Is(result.err, syscall.EPIPE) {
				// Preserve an abrupt peer disconnect through another relay hop.
				_ = resetTCP(downstream)
			} else {
				_ = downstream.Close()
			}
		default:
			continue
		}
		_ = upstream.Close()
		if toUpstream != nil {
			result.sentUpstream = (<-toUpstream).bytes
		}
		if toDownstream != nil {
			result.sentDownstream = (<-toDownstream).bytes
		}
		break
	}
	return result
}

type copyResult struct {
	bytes int64
	err   error
	reset bool
}

func copyStream(dst tcpConn, src io.Reader, resetAfterBytes int64) <-chan copyResult {
	done := make(chan copyResult, 1)
	go func() {
		if resetAfterBytes > 0 {
			src = io.LimitReader(src, resetAfterBytes)
		}
		n, err := io.Copy(dst, src)
		result := copyResult{bytes: n, err: err}
		if err == nil {
			if resetAfterBytes > 0 && n == resetAfterBytes {
				result.reset = true
			} else {
				result.err = dst.CloseWrite()
			}
		}
		done <- result
	}()
	return done
}

func resetTCP(conn tcpConn) error {
	if err := conn.SetLinger(0); err != nil {
		_ = conn.Close()
		return err
	}
	return conn.Close()
}
