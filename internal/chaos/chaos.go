// Package chaos validates and samples per-request proxy faults.
package chaos

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	requestDelayHeader      = "X-Chaos-Request-Delay-Ms"
	responseDelayHeader     = "X-Chaos-Response-Delay-Ms"
	statusCodeHeader        = "X-Chaos-Status-Code"
	probabilityHeader       = "X-Chaos-Probability"
	requestJitterHeader     = "X-Chaos-Request-Jitter-Ms"
	responseJitterHeader    = "X-Chaos-Response-Jitter-Ms"
	requestBandwidthHeader  = "X-Chaos-Request-Bandwidth-Kbps"
	responseBandwidthHeader = "X-Chaos-Response-Bandwidth-Kbps"
	connectionResetHeader   = "X-Chaos-Connection-Reset"
	enabledHeader           = "X-Chaos-Enabled"
	tokenHeader             = "X-Chaos-Token"
	maxBandwidthKbps        = 1_000_000
)

// Settings contains the startup limits and authorization for chaos controls.
type Settings struct {
	Enabled                  bool
	RequestEnabled           bool
	ResponseEnabled          bool
	AbortEnabled             bool
	ProbabilityEnabled       bool
	RequestJitterEnabled     bool
	ResponseJitterEnabled    bool
	RequestBandwidthEnabled  bool
	ResponseBandwidthEnabled bool
	ConnectionResetEnabled   bool
	MaxRequestDelay          time.Duration
	MaxResponseDelay         time.Duration
	DefaultProbability       float64
	ResetAfterBytes          int64
	MaxActiveFaults          int
	Token                    string
}

// Policy is the set of faults requested for one proxied HTTP request.
type Policy struct {
	RequestDelay      time.Duration
	ResponseDelay     time.Duration
	StatusCode        int
	Probability       float64
	RequestJitter     time.Duration
	ResponseJitter    time.Duration
	RequestBandwidth  int64 // Decimal kilobits per second.
	ResponseBandwidth int64 // Decimal kilobits per second.
	ConnectionReset   bool
}

// HasFault reports whether the policy requests any fault.
func (p Policy) HasFault() bool {
	return p.RequestDelay > 0 || p.ResponseDelay > 0 || p.StatusCode != 0 ||
		p.RequestJitter > 0 || p.ResponseJitter > 0 ||
		p.RequestBandwidth > 0 || p.ResponseBandwidth > 0 || p.ConnectionReset
}

// Selected samples the probability of applying the policy as a whole.
func (p Policy) Selected() bool {
	return p.Probability >= 1 || (p.Probability > 0 && rand.Float64() < p.Probability)
}

// SampleJitter returns a copy with independently sampled offsets added to the delays.
func (p Policy) SampleJitter(s Settings) Policy {
	p.RequestDelay = sampleDelay(p.RequestDelay, p.RequestJitter, s.MaxRequestDelay)
	p.ResponseDelay = sampleDelay(p.ResponseDelay, p.ResponseJitter, s.MaxResponseDelay)
	return p
}

func sampleDelay(delay, jitter, limit time.Duration) time.Duration {
	if jitter == 0 {
		return delay
	}
	ms := jitter.Milliseconds()
	delta := time.Duration(rand.Int64N(2*ms+1)-ms) * time.Millisecond
	return applyDelayOffset(delay, delta, limit)
}

func applyDelayOffset(delay, delta, limit time.Duration) time.Duration {
	// A parsed base delay lies in [0, limit]. Cap positive offsets before adding.
	if delta > limit-delay {
		return limit
	}
	return max(0, delay+delta)
}

// HasControls reports whether a request contains any X-Chaos header.
func HasControls(h http.Header) bool {
	for name := range h {
		if strings.HasPrefix(strings.ToLower(name), "x-chaos-") {
			return true
		}
	}
	return false
}

// Authorized checks the configured token. Startup validation determines when
// running without a token is permitted.
func Authorized(h http.Header, token string) bool {
	if token == "" {
		return true
	}

	var supplied string
	found := false
	for name, values := range h {
		if !strings.EqualFold(name, tokenHeader) {
			continue
		}
		if found || len(values) != 1 || strings.Contains(values[0], ",") {
			return false
		}
		supplied = values[0]
		found = true
	}
	if !found {
		return false
	}

	// Comparing fixed-size hashes avoids a separate length check on the token.
	want := sha256.Sum256([]byte(token))
	got := sha256.Sum256([]byte(supplied))
	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

// Parse validates the chaos controls and returns their requested faults.
// The enabled header is a routing marker. Authorization is checked separately.
func Parse(h http.Header, s Settings) (Policy, error) {
	var policy Policy
	if !s.Enabled {
		return policy, nil
	}
	policy.Probability = 1
	if s.ProbabilityEnabled {
		policy.Probability = s.DefaultProbability
	}

	seen := make(map[string]bool)
	for name, values := range h {
		if !strings.HasPrefix(strings.ToLower(name), "x-chaos-") {
			continue
		}
		name = http.CanonicalHeaderKey(name)
		if seen[name] || len(values) != 1 || strings.Contains(values[0], ",") {
			return Policy{}, errors.New("each chaos header must have exactly one value")
		}
		seen[name] = true
		value := strings.Trim(values[0], " \t")

		var err error
		switch name {
		case requestDelayHeader:
			policy.RequestDelay, err = parseDelay(name, value, s.RequestEnabled, s.MaxRequestDelay)
		case responseDelayHeader:
			policy.ResponseDelay, err = parseDelay(name, value, s.ResponseEnabled, s.MaxResponseDelay)
		case statusCodeHeader:
			policy.StatusCode, err = parseStatusCode(value, s.AbortEnabled)
		case probabilityHeader:
			policy.Probability, err = parseProbability(value, s.ProbabilityEnabled)
		case requestJitterHeader:
			policy.RequestJitter, err = parseDelay(name, value, s.RequestEnabled && s.RequestJitterEnabled, s.MaxRequestDelay)
		case responseJitterHeader:
			policy.ResponseJitter, err = parseDelay(name, value, s.ResponseEnabled && s.ResponseJitterEnabled, s.MaxResponseDelay)
		case requestBandwidthHeader:
			policy.RequestBandwidth, err = parseBandwidth(name, value, s.RequestBandwidthEnabled)
		case responseBandwidthHeader:
			policy.ResponseBandwidth, err = parseBandwidth(name, value, s.ResponseBandwidthEnabled)
		case connectionResetHeader:
			if !s.ConnectionResetEnabled {
				err = fmt.Errorf("%s is disabled", name)
			} else {
				policy.ConnectionReset, err = parseBool(name, value)
			}
		case enabledHeader:
			_, err = parseBool(name, value)
		case tokenHeader:
			// The token is only used for authorization.
		default:
			err = errors.New("unsupported chaos control")
		}
		if err != nil {
			return Policy{}, err
		}
	}
	if policy.StatusCode != 0 && policy.ConnectionReset {
		return Policy{}, errors.New("X-Chaos-Status-Code and X-Chaos-Connection-Reset cannot be combined")
	}
	return policy, nil
}

func parseStatusCode(value string, enabled bool) (int, error) {
	if !enabled {
		return 0, fmt.Errorf("%s is disabled", statusCodeHeader)
	}
	code, err := strconv.ParseUint(value, 10, 16)
	if err != nil || code < 200 || code > 599 {
		return 0, fmt.Errorf("%s must be an HTTP status code between 200 and 599", statusCodeHeader)
	}
	return int(code), nil
}

func parseProbability(value string, enabled bool) (float64, error) {
	if !enabled {
		return 0, fmt.Errorf("%s is disabled", probabilityHeader)
	}
	p, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(p) || p < 0 || p > 1 {
		return 0, fmt.Errorf("%s must be a finite number between 0 and 1", probabilityHeader)
	}
	return p, nil
}

func parseBandwidth(name, value string, enabled bool) (int64, error) {
	if !enabled {
		return 0, fmt.Errorf("%s is disabled", name)
	}
	kbps, err := strconv.ParseUint(value, 10, 64)
	if err != nil || kbps == 0 || kbps > maxBandwidthKbps {
		return 0, fmt.Errorf("%s must be a whole number between 1 and %d", name, maxBandwidthKbps)
	}
	return int64(kbps), nil
}

func parseBool(name, value string) (bool, error) {
	if strings.EqualFold(value, "true") {
		return true, nil
	}
	if strings.EqualFold(value, "false") {
		return false, nil
	}
	return false, fmt.Errorf("%s must be true or false", name)
}

func parseDelay(name, value string, enabled bool, max time.Duration) (time.Duration, error) {
	if !enabled {
		return 0, fmt.Errorf("%s is disabled", name)
	}

	ms, err := strconv.ParseUint(value, 10, 64)
	if err != nil || ms > uint64(math.MaxInt64/time.Millisecond) {
		return 0, fmt.Errorf("%s must be a nonnegative whole number of milliseconds", name)
	}
	delay := time.Duration(ms) * time.Millisecond
	if delay > max {
		return 0, fmt.Errorf("%s exceeds the configured maximum", name)
	}
	return delay, nil
}

// Wait pauses for d unless the context has been canceled.
func Wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
