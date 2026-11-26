package config

import (
	"log/slog"
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

func TestInvalidStartupConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		vars map[string]string
		want string
	}{
		{"unknown field", "chaos:\n  request_delay:\n    typo: true\n", nil, "field typo not found"},
		{"trailing document", "{}\n---\n{}", nil, "single YAML document"},
		{"bad env duration", "", map[string]string{"CHAOS_REQUEST_DELAY_MAX": "one second"}, "CHAOS_REQUEST_DELAY_MAX"},
		{"bad max", "", map[string]string{"CHAOS_RESPONSE_DELAY_MAX": "-1s"}, "chaos.response_delay.max"},
		{"bad limit", "", map[string]string{"CHAOS_MAX_ACTIVE_FAULTS": "0"}, "chaos.limits.max_active_faults"},
		{"bad log level", "observability:\n  logging:\n    level: all\n", nil, "slog"},
		{"bad log format", "", map[string]string{"OBSERVABILITY_LOGGING_FORMAT": "csv"}, "observability.logging.format"},
		{"forwarded controls", "", map[string]string{"CHAOS_STRIP_CONTROL_HEADERS": "false"}, "chaos.strip_control_headers"},
		{"abort", "", map[string]string{"CHAOS_ABORT_ENABLED": "true"}, "chaos.abort.enabled"},
		{"probability", "", map[string]string{"CHAOS_PROBABILITY_ENABLED": "true"}, "chaos.probability.enabled"},
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
