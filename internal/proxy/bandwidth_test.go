package proxy

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"
)

func TestBandwidthBodyPacesPayload(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kbps  int64
		bytes int
		want  time.Duration
	}{
		{"one kilobit per second", 1, 25, 200 * time.Millisecond},
		{"eighty kilobits per second", 80, 2500, 250 * time.Millisecond},
		{"one gigabit per second", 1_000_000, 128 * 1024, 1_048_576 * time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				payload := strings.Repeat("a", tc.bytes)
				body := limitBody(t.Context(), io.NopCloser(strings.NewReader(payload)), tc.kbps)
				buf := make([]byte, len(payload)+1)
				started := time.Now()
				n, err := body.Read(buf)
				if err != nil || n == 0 || n > bandwidthMaxRead || n >= tc.bytes || time.Since(started) > 10*time.Millisecond {
					t.Fatalf("first read = (%d, %v) after %v, want a bounded chunk within 10ms", n, err, time.Since(started))
				}
				m, err := io.ReadFull(body, buf[n:tc.bytes])
				if err != nil || n+m != tc.bytes || string(buf[:tc.bytes]) != payload {
					t.Fatalf("received %d bytes, err = %v, want %d original bytes", n+m, err, tc.bytes)
				}
				if elapsed := time.Since(started); elapsed != tc.want {
					t.Errorf("received payload in %v, want %v", elapsed, tc.want)
				}
				eofStarted := time.Now()
				if n, err := body.Read(buf); n != 0 || err != io.EOF || time.Since(eofStarted) != 0 {
					t.Errorf("read past payload = (%d, %v) after %v, want immediate EOF", n, err, time.Since(eofStarted))
				}
			})
		})
	}
}

func TestBandwidthBodyAfterIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := limitBody(t.Context(), io.NopCloser(strings.NewReader("ab")), 1)
		buf := make([]byte, 2)
		if n, err := body.Read(buf); n != 1 || err != nil {
			t.Fatalf("first read = (%d, %v), want one byte", n, err)
		}
		time.Sleep(time.Second)
		started := time.Now()
		if n, err := body.Read(buf); n != 1 || err != nil || time.Since(started) != 8*time.Millisecond {
			t.Errorf("read after idle = (%d, %v) after %v, want one byte after 8ms", n, err, time.Since(started))
		}
	})
}

func TestBandwidthBodyPreservesReadErrorsWithData(t *testing.T) {
	for _, readErr := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(readErr.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source := io.MultiReader(strings.NewReader("a"), iotest.ErrReader(readErr))
				body := limitBody(t.Context(), io.NopCloser(iotest.DataErrReader(source)), 1)
				started := time.Now()
				buf := make([]byte, 1)
				n, err := body.Read(buf)
				if n != 1 || buf[0] != 'a' || err != readErr || time.Since(started) != 8*time.Millisecond {
					t.Errorf("read = (%d, %v, %v) after %v, want one byte with %v after 8ms", n, buf, err, time.Since(started), readErr)
				}
			})
		})
	}
}

func TestBandwidthBodyWaitStopsOnCancelOrClose(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stop    func(context.CancelFunc, io.ReadCloser) error
		stopErr error
		readErr error
	}{
		{
			name: "cancel",
			stop: func(cancel context.CancelFunc, _ io.ReadCloser) error {
				cancel()
				return nil
			},
			readErr: context.Canceled,
		},
		{
			name:    "close",
			stop:    func(_ context.CancelFunc, body io.ReadCloser) error { return body.Close() },
			stopErr: errBandwidthClose,
			readErr: io.ErrClosedPipe,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				inner := &bandwidthTestBody{Reader: strings.NewReader("abc")}
				body := limitBody(ctx, inner, 1)
				started := time.Now()
				result := make(chan error, 1)
				go func() {
					n, err := body.Read(make([]byte, 8))
					if n != 0 {
						t.Errorf("read returned %d bytes during interrupted wait, want none", n)
					}
					result <- err
				}()
				synctest.Wait()
				if err := tc.stop(cancel, body); err != tc.stopErr {
					t.Errorf("stop returned %v, want %v", err, tc.stopErr)
				}
				if err := <-result; err != tc.readErr || time.Since(started) != 0 {
					t.Errorf("read returned %v after %v, want %v immediately", err, time.Since(started), tc.readErr)
				}
				if err := body.Close(); err != errBandwidthClose || inner.closed != 1 {
					t.Errorf("Close returned %v after %d underlying closes, want %v and one close", err, inner.closed, errBandwidthClose)
				}
			})
		})
	}
}

var errBandwidthClose = errors.New("underlying close error")

type bandwidthTestBody struct {
	io.Reader
	closed int
}

func (b *bandwidthTestBody) Close() error {
	b.closed++
	return errBandwidthClose
}

func TestBandwidthBodiesRunIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		payload := strings.Repeat("a", 250)
		var readers sync.WaitGroup
		for _, kbps := range []int64{1, 2, 2} {
			readers.Go(func() {
				body := limitBody(t.Context(), io.NopCloser(strings.NewReader(payload)), kbps)
				got, err := io.ReadAll(body)
				want := 2 * time.Second / time.Duration(kbps)
				if elapsed := time.Since(started); string(got) != payload || err != nil || elapsed != want {
					t.Errorf("body at %d kbps read %d bytes (%v) in %v, want %d bytes in %v", kbps, len(got), err, elapsed, len(payload), want)
				}
			})
		}
		readers.Wait()
	})
}
