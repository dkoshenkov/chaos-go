package chaos

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
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
	want := Policy{RequestDelay: 300 * time.Millisecond, ResponseDelay: 400 * time.Millisecond, Probability: 1}
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

func TestParseFaultControls(t *testing.T) {
	settings := Settings{
		Enabled: true, RequestEnabled: true, ResponseEnabled: true,
		AbortEnabled: true, ProbabilityEnabled: true,
		RequestJitterEnabled: true, ResponseJitterEnabled: true,
		RequestBandwidthEnabled: true, ResponseBandwidthEnabled: true,
		ConnectionResetEnabled: true,
		MaxRequestDelay:        20 * time.Millisecond, MaxResponseDelay: 40 * time.Millisecond,
		DefaultProbability: 0.5,
	}

	t.Run("compatible controls", func(t *testing.T) {
		headers := http.Header{
			requestDelayHeader:                {"8"},
			statusCodeHeader:                  {"599"},
			probabilityHeader:                 {"\t0.25 "},
			requestJitterHeader:               {"20"},
			responseJitterHeader:              {"40"},
			requestBandwidthHeader:            {"1"},
			"x-chaos-response-bandwidth-kbps": {"1000000"},
			connectionResetHeader:             {"FALSE"},
		}
		want := Policy{
			RequestDelay: 8 * time.Millisecond, StatusCode: 599, Probability: 0.25,
			RequestJitter: 20 * time.Millisecond, ResponseJitter: 40 * time.Millisecond,
			RequestBandwidth: 1, ResponseBandwidth: 1_000_000,
		}
		if got, err := Parse(headers, settings); err != nil || got != want {
			t.Fatalf("Parse() = (%+v, %v), want (%+v, nil)", got, err, want)
		}
	})

	for _, tc := range []struct {
		name    string
		headers http.Header
		want    Policy
	}{
		{"default probability", nil, Policy{Probability: 0.5}},
		{"probability zero", http.Header{probabilityHeader: {"0"}}, Policy{}},
		{"probability one", http.Header{probabilityHeader: {"1"}}, Policy{Probability: 1}},
		{"minimum status", http.Header{statusCodeHeader: {"200"}}, Policy{StatusCode: 200, Probability: 0.5}},
		{"reset", http.Header{connectionResetHeader: {"TRUE"}}, Policy{ConnectionReset: true, Probability: 0.5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := Parse(tc.headers, settings); err != nil || got != tc.want {
				t.Fatalf("Parse() = (%+v, %v), want (%+v, nil)", got, err, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		header string
		value  string
	}{
		{"informational status", statusCodeHeader, "199"},
		{"status above range", statusCodeHeader, "600"},
		{"fractional status", statusCodeHeader, "200.0"},
		{"probability below range", probabilityHeader, "-0.1"},
		{"probability above range", probabilityHeader, "1.01"},
		{"NaN probability", probabilityHeader, "NaN"},
		{"infinite probability", probabilityHeader, "Inf"},
		{"negative jitter", requestJitterHeader, "-1"},
		{"fractional jitter", responseJitterHeader, "1.25"},
		{"request jitter above limit", requestJitterHeader, "21"},
		{"response jitter above limit", responseJitterHeader, "41"},
		{"jitter duration overflow", responseJitterHeader, "9223372036855"},
		{"zero bandwidth", requestBandwidthHeader, "0"},
		{"negative bandwidth", responseBandwidthHeader, "-1"},
		{"fractional bandwidth", requestBandwidthHeader, "1.5"},
		{"bandwidth above range", responseBandwidthHeader, "1000001"},
		{"invalid reset", connectionResetHeader, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := Parse(http.Header{tc.header: {tc.value}}, settings); err == nil || got != (Policy{}) {
				t.Fatalf("Parse() = (%+v, %v), want an empty policy and an error", got, err)
			}
		})
	}

	t.Run("conflicting faults", func(t *testing.T) {
		headers := http.Header{statusCodeHeader: {"503"}, connectionResetHeader: {"true"}}
		if got, err := Parse(headers, settings); err == nil || got != (Policy{}) {
			t.Fatalf("Parse() = (%+v, %v), want an empty policy and an error", got, err)
		}
	})

	t.Run("duplicate control", func(t *testing.T) {
		headers := http.Header{probabilityHeader: {"0"}, "x-chaos-probability": {"1"}}
		if got, err := Parse(headers, settings); err == nil || got != (Policy{}) {
			t.Fatalf("Parse() = (%+v, %v), want an empty policy and an error", got, err)
		}
	})
}

func TestParseDisabledFaultControls(t *testing.T) {
	for _, tc := range []struct {
		header   string
		value    string
		settings Settings
	}{
		{statusCodeHeader, "503", Settings{}},
		{probabilityHeader, "0", Settings{}},
		{requestJitterHeader, "0", Settings{RequestEnabled: true}},
		{requestJitterHeader, "0", Settings{RequestJitterEnabled: true}},
		{responseJitterHeader, "0", Settings{ResponseEnabled: true}},
		{responseJitterHeader, "0", Settings{ResponseJitterEnabled: true}},
		{requestBandwidthHeader, "1", Settings{ResponseBandwidthEnabled: true}},
		{responseBandwidthHeader, "1", Settings{RequestBandwidthEnabled: true}},
		{connectionResetHeader, "false", Settings{}},
	} {
		t.Run(tc.header, func(t *testing.T) {
			tc.settings.Enabled = true
			got, err := Parse(http.Header{tc.header: {tc.value}}, tc.settings)
			if err == nil || !strings.Contains(err.Error(), "disabled") || got != (Policy{}) {
				t.Fatalf("Parse() = (%+v, %v), want an empty policy and a disabled control error", got, err)
			}
		})
	}
}

func TestPolicySelection(t *testing.T) {
	if (Policy{RequestDelay: time.Second, Probability: 0}).Selected() {
		t.Fatal("Selected() chose a policy with probability 0")
	}
	if !(Policy{RequestDelay: time.Second, Probability: 1}).Selected() {
		t.Fatal("Selected() skipped a policy with probability 1")
	}
}

func TestPolicyHasFault(t *testing.T) {
	if (Policy{Probability: 1}).HasFault() {
		t.Fatal("HasFault() treated probability alone as a fault")
	}
	for _, p := range []Policy{
		{RequestDelay: time.Millisecond},
		{ResponseDelay: time.Millisecond},
		{RequestJitter: time.Millisecond},
		{ResponseJitter: time.Millisecond},
		{RequestBandwidth: 1},
		{ResponseBandwidth: 1},
		{StatusCode: 200},
		{ConnectionReset: true},
	} {
		if !p.HasFault() {
			t.Errorf("%+v.HasFault() = false, want true", p)
		}
	}
}

func TestPolicySampleJitter(t *testing.T) {
	const ms = time.Millisecond
	p := Policy{
		RequestDelay: 10 * ms, RequestJitter: 3 * ms, ResponseJitter: 4 * ms,
		StatusCode: 503, RequestBandwidth: 42, Probability: 0.25,
	}
	s := Settings{MaxRequestDelay: 12 * ms, MaxResponseDelay: 20 * ms}
	for range 16 {
		got := p.SampleJitter(s)
		if got.RequestDelay < 7*ms || got.RequestDelay > 12*ms || got.RequestDelay%ms != 0 {
			t.Errorf("sampled request delay = %v, want whole milliseconds in [7ms, 12ms]", got.RequestDelay)
		}
		if got.ResponseDelay < 0 || got.ResponseDelay > 4*ms || got.ResponseDelay%ms != 0 {
			t.Errorf("sampled response delay = %v, want whole milliseconds in [0ms, 4ms]", got.ResponseDelay)
		}
		got.RequestDelay, got.ResponseDelay = p.RequestDelay, p.ResponseDelay
		if got != p {
			t.Errorf("SampleJitter() changed non-delay policy values from %+v to %+v", p, got)
		}
	}

	p.RequestJitter, p.ResponseJitter = 0, 0
	if got := p.SampleJitter(s); got != p {
		t.Errorf("SampleJitter() = %+v, want unchanged policy %+v", got, p)
	}
}

func TestApplyDelayOffsetClamps(t *testing.T) {
	const ms = time.Millisecond
	for _, tc := range []struct {
		name                       string
		delay, offset, limit, want time.Duration
	}{
		{"under zero", ms, -2 * ms, 10 * ms, 0},
		{"at zero", ms, -ms, 10 * ms, 0},
		{"between bounds", ms, 2 * ms, 10 * ms, 3 * ms},
		{"at limit", ms, 9 * ms, 10 * ms, 10 * ms},
		{"above limit", ms, 10 * ms, 10 * ms, 10 * ms},
		{"addition overflows without cap", math.MaxInt64 - ms, 2 * ms, math.MaxInt64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := applyDelayOffset(tc.delay, tc.offset, tc.limit); got != tc.want {
				t.Fatalf("applyDelayOffset(%v, %v, %v) = %v, want %v", tc.delay, tc.offset, tc.limit, got, tc.want)
			}
		})
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
