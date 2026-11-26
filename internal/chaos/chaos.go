// Package chaos validates requests for bounded proxy delays.
package chaos

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	requestDelayHeader  = "X-Chaos-Request-Delay-Ms"
	responseDelayHeader = "X-Chaos-Response-Delay-Ms"
	enabledHeader       = "X-Chaos-Enabled"
	tokenHeader         = "X-Chaos-Token"
)

// Settings contains the startup limits and authorization for chaos controls.
type Settings struct {
	Enabled          bool
	RequestEnabled   bool
	ResponseEnabled  bool
	MaxRequestDelay  time.Duration
	MaxResponseDelay time.Duration
	MaxActiveFaults  int
	Token            string
}

// Policy is the set of delays requested for one proxied HTTP request.
type Policy struct {
	RequestDelay  time.Duration
	ResponseDelay time.Duration
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

// Parse validates the chaos controls and returns their requested delays.
// The enabled header is a routing marker. Authorization is checked separately.
func Parse(h http.Header, s Settings) (Policy, error) {
	var policy Policy
	if !s.Enabled {
		return policy, nil
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
		case enabledHeader:
			if !strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
				err = errors.New("X-Chaos-Enabled must be true or false")
			}
		case tokenHeader:
			// The token is only used for authorization.
		default:
			err = errors.New("unsupported chaos control")
		}
		if err != nil {
			return Policy{}, err
		}
	}
	return policy, nil
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
