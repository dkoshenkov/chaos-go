package proxy

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	bandwidthQuantum = 10 * time.Millisecond
	bandwidthMaxRead = 32 * 1024
	// One decimal kilobit per second is 125 bytes per second.
	bandwidthByteTime = time.Second / 125
)

func limitBody(ctx context.Context, body io.ReadCloser, kbps int64) io.ReadCloser {
	if kbps <= 0 || body == nil || body == http.NoBody {
		return body
	}
	bytesPerQuantum := min(kbps, bandwidthMaxRead) * int64(bandwidthQuantum) / int64(bandwidthByteTime)
	return &bandwidthBody{
		ctx:     ctx,
		body:    body,
		kbps:    kbps,
		maxRead: min(bandwidthMaxRead, int(bytesPerQuantum)),
		closed:  make(chan struct{}),
	}
}

type bandwidthBody struct {
	ctx     context.Context
	body    io.ReadCloser
	kbps    int64
	maxRead int
	readyAt time.Time

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (b *bandwidthBody) Read(p []byte) (int, error) {
	if err := b.stopError(); err != nil {
		return 0, err
	}
	n, err := b.body.Read(p[:min(len(p), b.maxRead)])
	if n > 0 {
		if waitErr := b.wait(n); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}

func (b *bandwidthBody) wait(n int) error {
	now := time.Now()
	// Carry short scheduling delays forward to avoid drifting below the rate.
	// A pause between reads cannot create more than one quantum of credit.
	if b.readyAt.Before(now.Add(-bandwidthQuantum)) {
		b.readyAt = now
	}
	nanos := (int64(n)*int64(bandwidthByteTime)-1)/b.kbps + 1
	b.readyAt = b.readyAt.Add(time.Duration(nanos))
	if remaining := time.Until(b.readyAt); remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-b.ctx.Done():
		case <-b.closed:
		}
	}
	return b.stopError()
}

func (b *bandwidthBody) stopError() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	select {
	case <-b.closed:
		return io.ErrClosedPipe
	default:
		return nil
	}
}

func (b *bandwidthBody) Close() error {
	b.closeOnce.Do(func() {
		close(b.closed)
		b.closeErr = b.body.Close()
	})
	return b.closeErr
}
