package tcpproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/observability"
)

var (
	errLiveReset   = errors.New("live CONNECT reset")
	errLiveTimeout = errors.New("live CONNECT timeout")
)

// EnableLivePolicy enables live updates and returns their administrative handler.
// Call it before Serve and register the handler for PUT and DELETE.
func (h *ConnectServer) EnableLivePolicy() http.Handler {
	h.live = &livePolicies{
		settings: h.settings,
		active:   h.active,
		tunnels:  make(map[*liveTunnel]struct{}),
	}
	return http.HandlerFunc(h.updateLivePolicy)
}

func (h *ConnectServer) updateLivePolicy(w http.ResponseWriter, r *http.Request) {
	if !chaos.Authorized(r.Header, h.settings.Token) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	var next livePolicy
	if r.Method == http.MethodPut {
		policy, timeout, status := h.parseControls(r.Header)
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if policy.HasFault() || timeout > 0 {
			next = livePolicy{policy: policy, timeout: timeout}
		}
	}
	h.live.update(next)
	w.WriteHeader(http.StatusNoContent)
}

type livePolicy struct {
	policy  chaos.Policy
	timeout time.Duration
}

type livePolicies struct {
	settings chaos.Settings
	active   chan struct{}
	mu       sync.Mutex
	current  livePolicy
	tunnels  map[*liveTunnel]struct{}
}

type liveTunnel struct {
	owner       *livePolicies
	ctx         context.Context
	cancel      context.CancelCauseFunc
	observation *observability.TCPConnection
	handshake   bool // The CONNECT policy already holds a slot for this tunnel.
	fault       atomic.Pointer[liveFault]
}

type liveFault struct {
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *time.Timer
	policy    chaos.Policy
	sampled   chaos.Policy
	remaining int64 // Accessed only by the response writer.
}

func (p *livePolicies) attach(ctx context.Context, observation *observability.TCPConnection, handshake bool) *liveTunnel {
	ctx, cancel := context.WithCancelCause(ctx)
	t := &liveTunnel{owner: p, ctx: ctx, cancel: cancel, observation: observation, handshake: handshake}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tunnels[t] = struct{}{}
	if ctx.Err() == nil && p.current.policy.Selected() {
		p.apply(t, p.current)
	}
	return t
}

func (p *livePolicies) detach(t *liveTunnel) {
	t.cancel(nil)
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.tunnels, t)
	p.clear(t)
}

func (p *livePolicies) update(next livePolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current == next {
		return
	}
	p.current = next
	selected := make([]*liveTunnel, 0, len(p.tunnels))
	for t := range p.tunnels {
		if t.ctx.Err() == nil && next.policy.Selected() {
			selected = append(selected, t)
		} else {
			p.clear(t)
		}
	}
	for _, t := range selected {
		p.apply(t, next)
	}
}

// apply and clear run with p.mu held. A replacement keeps the current slot.
func (p *livePolicies) apply(t *liveTunnel, next livePolicy) {
	if t.fault.Load() == nil && !t.handshake {
		select {
		case p.active <- struct{}{}:
			t.observation.SetFaultActive(true)
		default:
			return
		}
	}
	ctx, cancel := context.WithCancel(t.ctx)
	f := &liveFault{
		ctx:       ctx,
		cancel:    cancel,
		policy:    next.policy,
		sampled:   next.policy.SampleJitter(p.settings),
		remaining: p.settings.ResetAfterBytes,
	}
	if previous := t.fault.Swap(f); previous != nil {
		previous.stop()
	}
	if f.policy.ConnectionReset && p.settings.ResetAfterBytes == 0 {
		t.cancel(errLiveReset)
	} else if next.timeout > 0 {
		f.timer = time.AfterFunc(next.timeout, func() { t.terminate(f, errLiveTimeout) })
	}
}

func (p *livePolicies) clear(t *liveTunnel) {
	if previous := t.fault.Swap(nil); previous != nil {
		previous.stop()
		if !t.handshake {
			t.observation.SetFaultActive(false)
			<-p.active
		}
	}
}

func (f *liveFault) stop() {
	if f.timer != nil {
		f.timer.Stop()
	}
	f.cancel()
}

// A timer or byte limit from a replaced policy cannot close the tunnel.
func (t *liveTunnel) terminate(f *liveFault, cause error) bool {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if t.fault.Load() != f || t.ctx.Err() != nil {
		return false
	}
	t.cancel(cause)
	return true
}

type liveWriter struct {
	tcpConn
	tunnel    *liveTunnel
	direction string
}

func (w *liveWriter) Close() error {
	w.tunnel.cancel(nil)
	return w.tcpConn.Close()
}

func (w *liveWriter) Write(p []byte) (int, error) {
	t := w.tunnel
	written := 0
	for len(p) > 0 {
		if err := t.ctx.Err(); err != nil {
			return written, err
		}
		f := t.fault.Load()
		if f == nil {
			n, err := w.tcpConn.Write(p)
			return written + n, err
		}

		effective, base, jitter := f.sampled.RequestDelay, f.policy.RequestDelay, f.policy.RequestJitter
		if w.direction == "response" {
			effective, base, jitter = f.sampled.ResponseDelay, f.policy.ResponseDelay, f.policy.ResponseJitter
		}
		if err := waitConnect(f.ctx, t.observation.Wait, w.direction, effective, base, jitter); err != nil || t.fault.Load() != f {
			// Keep the same bytes and recheck the current policy after an update.
			continue
		}

		size := len(p)
		reset := w.direction == "response" && f.policy.ConnectionReset
		if reset {
			size = int(min(int64(size), f.remaining))
		}
		n, err := w.tcpConn.Write(p[:size])
		written += n
		if err != nil {
			return written, err
		}
		if n != size {
			return written, io.ErrShortWrite
		}
		if reset {
			f.remaining -= int64(n)
			if f.remaining == 0 && t.terminate(f, errLiveReset) {
				return written, errLiveReset
			}
		}
		p = p[n:]
	}
	return written, nil
}
