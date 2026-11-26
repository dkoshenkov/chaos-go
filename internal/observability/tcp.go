package observability

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

const (
	clientToUpstream = "client_to_upstream"
	upstreamToClient = "upstream_to_client"
	tcpIOError       = "io_error"
)

var tcpOutcomes = [...]string{"closed", "dial_error", tcpIOError, "reset", "limited", "shutdown", "timeout"}

// TCPConnection holds observability state for one proxy TCP connection.
type TCPConnection struct {
	observer    *Observer
	started     time.Time
	mu          sync.Mutex
	faultActive bool
	finished    bool
}

// BeginTCP records a TCP connection as active and returns its observation state.
func (o *Observer) BeginTCP() *TCPConnection {
	if o == nil {
		return nil
	}
	connection := &TCPConnection{observer: o, started: time.Now()}
	o.activeTCP.Inc()
	return connection
}

// Fault records that the connection holds a fault slot until it finishes.
func (c *TCPConnection) Fault() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished && !c.faultActive {
		c.faultActive = true
		c.observer.activeFaults.Inc()
	}
}

// Wait records the effective wait and the delay and jitter that produced it.
func (c *TCPConnection) Wait(direction string, effective, actual time.Duration, baseDelay, jitter bool) {
	if c == nil || !validDirection(direction) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	observer := c.observer
	if baseDelay {
		observer.injections.WithLabelValues("delay", direction).Inc()
	}
	if jitter {
		observer.injections.WithLabelValues("jitter", direction).Inc()
	}
	observer.observeWait(direction, effective, actual, baseDelay)
}

// Finish records the connection's outcome and number of bytes forwarded in each direction.
func (c *TCPConnection) Finish(outcome string, sentUpstream, sentDownstream int64) {
	if c == nil {
		return
	}
	observer := c.observer
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return
	}
	c.finished = true
	if c.faultActive {
		observer.activeFaults.Dec()
	}
	c.mu.Unlock()

	if !slices.Contains(tcpOutcomes[:], outcome) {
		outcome = tcpIOError
	}
	if outcome == "reset" {
		observer.injections.WithLabelValues("connection_reset", "response").Inc()
	}
	sentUpstream = max(sentUpstream, 0)
	sentDownstream = max(sentDownstream, 0)
	duration := time.Since(c.started)
	observer.activeTCP.Dec()
	observer.tcpConnections.WithLabelValues(outcome).Inc()
	observer.tcpBytes.WithLabelValues(clientToUpstream).Add(float64(sentUpstream))
	observer.tcpBytes.WithLabelValues(upstreamToClient).Add(float64(sentDownstream))
	observer.tcpDuration.Observe(duration.Seconds())
	if observer.logger != nil {
		observer.logger.LogAttrs(context.Background(), slog.LevelInfo, "proxy tcp connection",
			slog.String("outcome", outcome),
			slog.Int64("bytes_client_to_upstream", sentUpstream),
			slog.Int64("bytes_upstream_to_client", sentDownstream),
			slog.Float64("total_duration_ms", milliseconds(duration)),
		)
	}
}
