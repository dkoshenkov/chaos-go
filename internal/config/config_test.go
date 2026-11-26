package config

import (
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExampleConfig(t *testing.T) {
	cfg, err := load("../../configs/config.yaml", Overrides{}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := defaults()
	want.Upstream.URL = "http://127.0.0.1:9000"
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadPrecedence(t *testing.T) {
	path := writeConfig(t, `server:
  listen: "127.0.0.1:8000"
  read_header_timeout: 1s
upstream:
  url: "http://example.invalid/yaml"
chaos:
  response_delay:
    enabled: false
    max: 30s
  limits:
    max_active_faults: 200
observability:
  logging:
    level: warn
    format: text
`)
	listen := "127.0.0.1:8002"
	upstream := "https://example.invalid/flag"
	cfg, err := load(path, Overrides{Listen: &listen, UpstreamURL: &upstream}, env(map[string]string{
		"SERVER_LISTEN":               "127.0.0.1:8001",
		"UPSTREAM_URL":                "http://example.invalid/env",
		"CHAOS_ENABLED":               "false",
		"CHAOS_REQUEST_DELAY_MAX":     "500ms",
		"CHAOS_RESPONSE_DELAY_MAX":    "45s",
		"CHAOS_MAX_ACTIVE_FAULTS":     "300",
		"CHAOS_TOKEN":                 "test-token",
		"OBSERVABILITY_LISTEN":        "127.0.0.1:9001",
		"OBSERVABILITY_LOGGING_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := defaults()
	want.Server.Listen = listen
	want.Server.ReadHeaderTimeout = time.Second
	want.Upstream.URL = upstream
	want.Chaos.Enabled = false
	want.Chaos.RequestDelay.Max = 500 * time.Millisecond
	want.Chaos.ResponseDelay = Delay{Max: 45 * time.Second}
	want.Chaos.Limits.MaxActiveFaults = 300
	want.Chaos.Token = "test-token"
	want.Observability.Listen = "127.0.0.1:9001"
	want.Observability.Logging = Logging{Level: slog.LevelDebug, Format: "text"}
	if cfg != want {
		t.Error("defaults, YAML, environment, or flags did not apply in order")
	}
}

func TestLoadTunnelModes(t *testing.T) {
	path := writeConfig(t, `server:
  mode: connect
upstream:
  url: tcp://backend:5432
chaos:
  tunnel_timeout:
    max: 10s
`)
	cfg, err := load(path, Overrides{}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Mode != ModeConnect || cfg.Chaos.TunnelTimeout.Max != 10*time.Second {
		t.Fatalf("YAML mode = %q, timeout max = %s", cfg.Server.Mode, cfg.Chaos.TunnelTimeout.Max)
	}

	vars := env(map[string]string{
		"SERVER_MODE":              ModeTCP,
		"UPSTREAM_URL":             "backend:6432",
		"CHAOS_TUNNEL_TIMEOUT_MAX": "15s",
	})
	cfg, err = load(path, Overrides{}, vars)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Mode != ModeTCP || cfg.Upstream.URL != "backend:6432" || cfg.Chaos.TunnelTimeout.Max != 15*time.Second {
		t.Fatalf("env mode = %q, upstream = %q, timeout max = %s", cfg.Server.Mode, cfg.Upstream.URL, cfg.Chaos.TunnelTimeout.Max)
	}

	mode := ModeHTTP
	upstream := "http://backend:8080"
	cfg, err = load(path, Overrides{Mode: &mode, UpstreamURL: &upstream}, vars)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Mode != ModeHTTP || cfg.Upstream.URL != upstream {
		t.Fatalf("flag mode = %q, upstream = %q", cfg.Server.Mode, cfg.Upstream.URL)
	}
}

func TestAdvancedChaosConfig(t *testing.T) {
	path := writeConfig(t, `upstream:
  url: http://example.invalid
chaos:
  abort:
    enabled: true
  probability:
    enabled: true
    default: 0.25
  request_jitter:
    enabled: true
  response_jitter:
    enabled: true
  request_bandwidth:
    enabled: true
  response_bandwidth:
    enabled: true
  connection_reset:
    enabled: true
    after_bytes: 9223372036854775806
`)
	cfg, err := load(path, Overrides{}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := defaults().Chaos
	want.Abort.Enabled = true
	want.Probability = Probability{Enabled: true, Default: 0.25}
	want.RequestJitter.Enabled = true
	want.ResponseJitter.Enabled = true
	want.RequestBandwidth.Enabled = true
	want.ResponseBandwidth.Enabled = true
	want.ConnectionReset = ConnectionReset{Enabled: true, AfterBytes: math.MaxInt64 - 1}
	if cfg.Chaos != want {
		t.Errorf("from YAML got %+v, want %+v", cfg.Chaos, want)
	}

	cfg, err = load(path, Overrides{}, env(map[string]string{
		"CHAOS_ABORT_ENABLED":                "false",
		"CHAOS_PROBABILITY_ENABLED":          "false",
		"CHAOS_PROBABILITY_DEFAULT":          "0",
		"CHAOS_REQUEST_JITTER_ENABLED":       "false",
		"CHAOS_RESPONSE_JITTER_ENABLED":      "false",
		"CHAOS_REQUEST_BANDWIDTH_ENABLED":    "false",
		"CHAOS_RESPONSE_BANDWIDTH_ENABLED":   "false",
		"CHAOS_CONNECTION_RESET_ENABLED":     "false",
		"CHAOS_CONNECTION_RESET_AFTER_BYTES": "9223372036854775807",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want = defaults().Chaos
	want.Probability.Default = 0
	want.ConnectionReset.AfterBytes = math.MaxInt64
	if cfg.Chaos != want {
		t.Errorf("with environment overrides got %+v, want %+v", cfg.Chaos, want)
	}
}

func TestInvalidStartupConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		vars map[string]string
		want string
	}{
		{"unknown field", "chaos:\n  request_delay:\n    typo: true\n", nil, "field typo not found"},
		{"trailing document", "{}\n---\n{}", nil, "single YAML document"},
		{"unknown mode", "", map[string]string{"SERVER_MODE": "udp"}, "server.mode"},
		{"empty mode", "", map[string]string{"SERVER_MODE": ""}, "server.mode"},
		{"HTTP upstream in TCP mode", "", map[string]string{"SERVER_MODE": ModeTCP}, "upstream.url"},
		{"HTTP upstream in CONNECT mode", "", map[string]string{"SERVER_MODE": ModeConnect}, "upstream.url"},
		{"bad env duration", "", map[string]string{"CHAOS_REQUEST_DELAY_MAX": "one second"}, "CHAOS_REQUEST_DELAY_MAX"},
		{"negative tunnel timeout max", "", map[string]string{"CHAOS_TUNNEL_TIMEOUT_MAX": "-1s"}, "chaos.tunnel_timeout.max"},
		{"bad max", "", map[string]string{"CHAOS_RESPONSE_DELAY_MAX": "-1s"}, "chaos.response_delay.max"},
		{"bad limit", "", map[string]string{"CHAOS_MAX_ACTIVE_FAULTS": "0"}, "chaos.limits.max_active_faults"},
		{"bad log level", "observability:\n  logging:\n    level: all\n", nil, "slog"},
		{"bad log format", "", map[string]string{"OBSERVABILITY_LOGGING_FORMAT": "csv"}, "observability.logging.format"},
		{"forwarded controls", "", map[string]string{"CHAOS_STRIP_CONTROL_HEADERS": "false"}, "chaos.strip_control_headers"},
		{"bad enable flag", "", map[string]string{"CHAOS_REQUEST_JITTER_ENABLED": "sometimes"}, "CHAOS_REQUEST_JITTER_ENABLED"},
		{"probability above one", "", map[string]string{"CHAOS_PROBABILITY_DEFAULT": "1.1"}, "chaos.probability.default"},
		{"negative probability", "chaos:\n  probability:\n    default: -0.1\n", nil, "chaos.probability.default"},
		{"NaN probability", "", map[string]string{"CHAOS_PROBABILITY_DEFAULT": "NaN"}, "chaos.probability.default"},
		{"infinite probability", "chaos:\n  probability:\n    default: .inf\n", nil, "chaos.probability.default"},
		{"negative reset threshold", "", map[string]string{"CHAOS_CONNECTION_RESET_AFTER_BYTES": "-1"}, "chaos.connection_reset.after_bytes"},
		{"overflowing reset threshold", "", map[string]string{"CHAOS_CONNECTION_RESET_AFTER_BYTES": "9223372036854775808"}, "CHAOS_CONNECTION_RESET_AFTER_BYTES"},
		{"overflowing YAML reset threshold", "chaos:\n  connection_reset:\n    after_bytes: 9223372036854775808\n", nil, "chaos.connection_reset.after_bytes"},
		{"fractional reset threshold", "chaos:\n  connection_reset:\n    after_bytes: 1.5\n", nil, "chaos.connection_reset.after_bytes"},
		{"unknown reset setting", "chaos:\n  connection_reset:\n    typo: true\n", nil, "field typo not found"},
		{"tracing", "", map[string]string{"OBSERVABILITY_TRACING_ENABLED": "true"}, "observability.tracing.enabled"},
		{"unsupported token syntax", "", map[string]string{"CHAOS_TOKEN": "secret,value"}, "CHAOS_TOKEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path string
			if tt.yaml != "" {
				path = writeConfig(t, tt.yaml)
			}
			upstream := "http://example.invalid"
			_, err := load(path, Overrides{UpstreamURL: &upstream}, env(tt.vars))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
			if value := tt.vars["CHAOS_TOKEN"]; value != "" && strings.Contains(err.Error(), value) {
				t.Fatal("error exposes configured credential")
			}
		})
	}
}

func TestTokenRequiredForNonLoopbackProxy(t *testing.T) {
	tests := []struct {
		name    string
		address string
		token   string
		enabled string
		wantErr bool
	}{
		{"IPv4 loopback", "127.0.0.2:8080", "", "true", false},
		{"IPv6 loopback", "[::1]:8080", "", "true", false},
		{"hostname", "localhost:8080", "", "true", true},
		{"wildcard", ":8080", "", "true", true},
		{"IPv4 wildcard with token", "0.0.0.0:8080", "test-token", "true", false},
		{"IPv6 wildcard with chaos disabled", "[::]:8080", "", "false", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load("", Overrides{}, env(map[string]string{
				"SERVER_LISTEN":        tt.address,
				"UPSTREAM_URL":         "http://example.invalid",
				"CHAOS_ENABLED":        tt.enabled,
				"CHAOS_TOKEN":          tt.token,
				"OBSERVABILITY_LISTEN": ":9090",
			}))
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "CHAOS_TOKEN")) {
				t.Fatalf("error = %v, want missing token error", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTunnelTokenValidation(t *testing.T) {
	vars := map[string]string{
		"SERVER_MODE":   ModeTCP,
		"SERVER_LISTEN": ":8080",
		"UPSTREAM_URL":  "backend:5432",
	}
	if _, err := load("", Overrides{}, env(vars)); err != nil {
		t.Fatalf("TCP startup without a token: %v", err)
	}
	vars["CHAOS_TOKEN"] = "invalid,token"
	vars["CHAOS_STRIP_CONTROL_HEADERS"] = "false"
	if _, err := load("", Overrides{}, env(vars)); err != nil {
		t.Fatalf("TCP startup with unused HTTP settings: %v", err)
	}
	vars["SERVER_MODE"] = ModeConnect
	vars["CHAOS_STRIP_CONTROL_HEADERS"] = "true"
	for _, token := range []string{"", "invalid,token"} {
		vars["CHAOS_TOKEN"] = token
		if _, err := load("", Overrides{}, env(vars)); err == nil || !strings.Contains(err.Error(), "CHAOS_TOKEN") {
			t.Fatalf("CONNECT startup with missing or invalid token: %v", err)
		}
	}
	vars["CHAOS_TOKEN"] = "test-token"
	if _, err := load("", Overrides{}, env(vars)); err != nil {
		t.Fatalf("CONNECT startup with a token: %v", err)
	}
}

func TestTCPAddress(t *testing.T) {
	valid := map[string]string{
		"backend:1":                    "backend:1",
		"tcp://backend.example.:65535": "backend.example.:65535",
		"tcp://127.0.0.1:5432":         "127.0.0.1:5432",
		"[::1]:5432":                   "[::1]:5432",
		"tcp://[::1]:5432":             "[::1]:5432",
		"tcp://[fe80::1%25eth0]:5432":  "[fe80::1%eth0]:5432",
	}
	for upstream, want := range valid {
		t.Run(upstream, func(t *testing.T) {
			addr, err := TCPAddress(upstream)
			if err != nil || addr != want {
				t.Fatalf("address = %q, err = %v, want %q", addr, err, want)
			}
		})
	}
	invalid := []string{
		"", "backend", "backend:", ":5432", "tcp://:5432", "*:5432", "0.0.0.0:5432", "[::]:5432",
		"[::ffff:0.0.0.0]:5432", "::1:5432", "[backend]:5432", "[127.0.0.1]:5432",
		"backend:0", "backend:-1", "backend:+1", "backend:65536", "backend:postgres", "back end:5432",
		"tcp://backend:5432/", "backend:5432/path", "tcp://backend:5432?", "backend:5432?debug",
		"tcp://backend:5432#", "backend:5432#debug", "tcp://user:pass@backend:5432", "user@backend:5432",
		"tcp:///backend:5432", "http://backend:5432", "bad..name:5432",
	}
	for _, upstream := range invalid {
		t.Run(upstream, func(t *testing.T) {
			if _, err := TCPAddress(upstream); err == nil {
				t.Fatal("expected invalid upstream error")
			}
		})
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func env(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
