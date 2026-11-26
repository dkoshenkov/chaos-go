package chaos

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestParseDelays(t *testing.T) {
	settings := Settings{
		Enabled: true, RequestEnabled: true, ResponseEnabled: true,
		MaxRequestDelay: 300 * time.Millisecond, MaxResponseDelay: 400 * time.Millisecond,
	}
	headers := http.Header{
		"x-chaos-request-delay-ms": {"300"},
		responseDelayHeader:        {"400"},
		enabledHeader:              {"FALSE"},
		tokenHeader:                {"credential"},
	}

	got, err := Parse(headers, settings)
	want := Policy{RequestDelay: 300 * time.Millisecond, ResponseDelay: 400 * time.Millisecond}
	if err != nil || got != want {
		t.Fatalf("Parse() = (%+v, %v), want (%+v, nil)", got, err, want)
	}
}

func TestParseIgnoresControlsWhenOff(t *testing.T) {
	headers := http.Header{
		requestDelayHeader: {"-4", "8"},
		"X-Chaos-Reset":    {"true"},
	}
	got, err := Parse(headers, Settings{})
	if err != nil || got != (Policy{}) {
		t.Fatalf("Parse() = (%+v, %v), want an empty policy without an error", got, err)
	}
}

func TestParseRejectsInvalidControls(t *testing.T) {
	for _, tc := range []struct {
		name             string
		headers          http.Header
		requestDisabled  bool
		responseDisabled bool
	}{
		{name: "empty", headers: http.Header{requestDelayHeader: {""}}},
		{name: "negative", headers: http.Header{requestDelayHeader: {"-4"}}},
		{name: "fractional", headers: http.Header{responseDelayHeader: {"0.5"}}},
		{name: "units", headers: http.Header{requestDelayHeader: {"4ms"}}},
		{name: "over limit", headers: http.Header{responseDelayHeader: {"21"}}},
		{name: "duration overflow", headers: http.Header{requestDelayHeader: {"9223372036855"}}},
		{name: "integer overflow", headers: http.Header{requestDelayHeader: {"18446744073709551616"}}},
		{name: "repeated", headers: http.Header{requestDelayHeader: {"1", "2"}}},
		{name: "combined", headers: http.Header{requestDelayHeader: {"1, 2"}}},
		{name: "different casing", headers: http.Header{requestDelayHeader: {"1"}, "x-chaos-request-delay-ms": {"2"}}},
		{name: "invalid marker", headers: http.Header{enabledHeader: {"1"}}},
		{name: "duplicate marker", headers: http.Header{enabledHeader: {"true, false"}}},
		{name: "duplicate token", headers: http.Header{tokenHeader: {"a", "b"}}},
		{name: "request delay disabled", headers: http.Header{requestDelayHeader: {"0"}}, requestDisabled: true},
		{name: "response delay disabled", headers: http.Header{responseDelayHeader: {"1"}}, responseDisabled: true},
		{name: "status", headers: http.Header{"X-Chaos-Status-Code": {"503"}}},
		{name: "probability", headers: http.Header{"X-Chaos-Probability": {"0.5"}}},
		{name: "jitter", headers: http.Header{"X-Chaos-Jitter": {"10"}}},
		{name: "bandwidth", headers: http.Header{"X-Chaos-Bandwidth": {"1000"}}},
		{name: "reset", headers: http.Header{"X-Chaos-Reset": {"true"}}},
		{name: "unknown", headers: http.Header{"X-Chaos-Other": {"true"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := Settings{
				Enabled: true, RequestEnabled: !tc.requestDisabled, ResponseEnabled: !tc.responseDisabled,
				MaxRequestDelay: 20 * time.Millisecond, MaxResponseDelay: 20 * time.Millisecond,
			}
			if got, err := Parse(tc.headers, settings); err == nil || got != (Policy{}) {
				t.Fatalf("Parse() = (%+v, %v), want an empty policy and an error", got, err)
			}
		})
	}
}

func TestParseLargestRepresentableDelay(t *testing.T) {
	const ms = math.MaxInt64 / time.Millisecond
	settings := Settings{Enabled: true, RequestEnabled: true, MaxRequestDelay: math.MaxInt64}
	headers := http.Header{requestDelayHeader: {strconv.FormatInt(int64(ms), 10)}}
	if got, err := Parse(headers, settings); err != nil || got.RequestDelay != ms*time.Millisecond {
		t.Fatalf("Parse() = (%+v, %v), want a delay of %d ms", got, err, ms)
	}
}

func TestHasControls(t *testing.T) {
	if HasControls(http.Header{"X-Chaos": {"true"}, "Accept": {"*/*"}}) {
		t.Fatal("HasControls() reports controls for unrelated headers")
	}
	if !HasControls(http.Header{"x-CHAOS-unknown": {"1"}}) {
		t.Fatal("HasControls() misses a mixed-case unknown control")
	}
}

func TestAuthorized(t *testing.T) {
	const token = "testing-token"
	for _, tc := range []struct {
		name    string
		headers http.Header
		token   string
		want    bool
	}{
		{name: "no configured token", want: true},
		{name: "matching", headers: http.Header{"x-chaos-token": {token}}, token: token, want: true},
		{name: "missing", token: token},
		{name: "different", headers: http.Header{tokenHeader: {"different"}}, token: token},
		{name: "surrounding whitespace", headers: http.Header{tokenHeader: {" " + token}}, token: token},
		{name: "repeated", headers: http.Header{tokenHeader: {token, token}}, token: token},
		{name: "different casing", headers: http.Header{tokenHeader: {token}, "x-chaos-token": {token}}, token: token},
		{name: "combined", headers: http.Header{tokenHeader: {token + "," + token}}, token: token},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Authorized(tc.headers, tc.token); got != tc.want {
				t.Fatalf("Authorized() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 50 * time.Millisecond
		start := time.Now()
		if err := Wait(t.Context(), delay); err != nil {
			t.Fatalf("Wait() returned %v", err)
		}
		if got := time.Since(start); got != delay {
			t.Fatalf("Wait() took %v, want %v", got, delay)
		}
	})
}

func TestWaitCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()

		result := make(chan error, 1)
		go func() {
			result <- Wait(ctx, time.Hour)
		}()
		synctest.Wait()
		cancel()

		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() returned %v, want context.Canceled", err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("Wait() took %v after cancellation, want 0", got)
		}
		if err := Wait(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() with zero delay returned %v, want context.Canceled", err)
		}
	})
}
