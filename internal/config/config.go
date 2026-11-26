// Package config loads and validates the proxy's startup settings.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

const (
	ModeHTTP    = "http"
	ModeTCP     = "tcp"
	ModeConnect = "connect"
)

var errServerMode = errors.New("server.mode must be http, tcp, or connect")

type Config struct {
	Server        Server        `yaml:"server"`
	Upstream      Upstream      `yaml:"upstream"`
	Chaos         Chaos         `yaml:"chaos"`
	Observability Observability `yaml:"observability"`
}

type Server struct {
	Mode              string        `yaml:"mode"`
	Listen            string        `yaml:"listen"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
}

type Upstream struct {
	URL                       string        `yaml:"url"`
	DialTimeout               time.Duration `yaml:"dial_timeout"`
	ResponseHeaderTimeout     time.Duration `yaml:"response_header_timeout"`
	MaxIdleConnections        int           `yaml:"max_idle_connections"`
	MaxIdleConnectionsPerHost int           `yaml:"max_idle_connections_per_host"`
}

type Chaos struct {
	Enabled             bool            `yaml:"enabled"`
	RequestDelay        Delay           `yaml:"request_delay"`
	ResponseDelay       Delay           `yaml:"response_delay"`
	Abort               Feature         `yaml:"abort"`
	Probability         Probability     `yaml:"probability"`
	RequestJitter       Feature         `yaml:"request_jitter"`
	ResponseJitter      Feature         `yaml:"response_jitter"`
	RequestBandwidth    Feature         `yaml:"request_bandwidth"`
	ResponseBandwidth   Feature         `yaml:"response_bandwidth"`
	ConnectionReset     ConnectionReset `yaml:"connection_reset"`
	TunnelTimeout       TunnelTimeout   `yaml:"tunnel_timeout"`
	Limits              Limits          `yaml:"limits"`
	StripControlHeaders bool            `yaml:"strip_control_headers"`
	Token               string          `yaml:"-"`
}

type Delay struct {
	Enabled bool          `yaml:"enabled"`
	Max     time.Duration `yaml:"max"`
}

type Feature struct {
	Enabled bool `yaml:"enabled"`
}

type Probability struct {
	Enabled bool    `yaml:"enabled"`
	Default float64 `yaml:"default"`
}

type ConnectionReset struct {
	Enabled    bool      `yaml:"enabled"`
	AfterBytes byteCount `yaml:"after_bytes"`
}

type TunnelTimeout struct {
	Max time.Duration `yaml:"max"`
}

// byteCount rejects the implicit float-to-integer conversion in yaml.v3.
type byteCount int64

func (count *byteCount) UnmarshalYAML(node *yaml.Node) error {
	var value int64
	if node.ShortTag() != "!!int" || node.Decode(&value) != nil {
		return errors.New("chaos.connection_reset.after_bytes must be a signed 64-bit integer")
	}
	*count = byteCount(value)
	return nil
}

type Limits struct {
	MaxActiveFaults int `yaml:"max_active_faults"`
}

type Observability struct {
	Listen  string  `yaml:"listen"`
	Metrics Feature `yaml:"metrics"`
	Logging Logging `yaml:"logging"`
	Tracing Feature `yaml:"tracing"`
}

type Logging struct {
	Level  slog.Level `yaml:"level"`
	Format string     `yaml:"format"`
}

// Overrides contains explicitly supplied command-line flags.
type Overrides struct {
	Mode        *string
	Listen      *string
	UpstreamURL *string
}

func defaults() Config {
	return Config{
		Server: Server{
			Mode:              ModeHTTP,
			Listen:            "127.0.0.1:8080",
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
			ShutdownTimeout:   30 * time.Second,
		},
		Upstream: Upstream{
			DialTimeout:               5 * time.Second,
			ResponseHeaderTimeout:     30 * time.Second,
			MaxIdleConnections:        100,
			MaxIdleConnectionsPerHost: 100,
		},
		Chaos: Chaos{
			Enabled:             true,
			RequestDelay:        Delay{Enabled: true, Max: 120 * time.Second},
			ResponseDelay:       Delay{Enabled: true, Max: 120 * time.Second},
			Probability:         Probability{Default: 1},
			TunnelTimeout:       TunnelTimeout{Max: 120 * time.Second},
			Limits:              Limits{MaxActiveFaults: 1000},
			StripControlHeaders: true,
		},
		Observability: Observability{
			Listen:  "127.0.0.1:9090",
			Metrics: Feature{Enabled: true},
			Logging: Logging{Level: slog.LevelInfo, Format: "json"},
		},
	}
}

// Load applies defaults, an optional YAML file, environment variables, and
// explicit command-line flags, in that order.
func Load(path string, overrides Overrides) (Config, error) {
	return load(path, overrides, os.LookupEnv)
}

func load(path string, overrides Overrides, lookupEnv func(string) (string, bool)) (Config, error) {
	cfg := defaults()
	if path != "" {
		if err := readYAML(path, &cfg); err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
	}
	if err := cfg.applyEnvironment(lookupEnv); err != nil {
		return Config{}, err
	}
	if overrides.Mode != nil {
		cfg.Server.Mode = *overrides.Mode
	}
	if overrides.Listen != nil {
		cfg.Server.Listen = *overrides.Listen
	}
	if overrides.UpstreamURL != nil {
		cfg.Upstream.URL = *overrides.UpstreamURL
	}
	if cfg.Server.Mode == "" {
		return Config{}, errServerMode
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func readYAML(path string, cfg *Config) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("expected a single YAML document")
	}
	return nil
}

func (cfg *Config) applyEnvironment(lookup func(string) (string, bool)) error {
	bindings := []struct {
		name   string
		target any
	}{
		{"SERVER_MODE", &cfg.Server.Mode},
		{"SERVER_LISTEN", &cfg.Server.Listen},
		{"SERVER_READ_HEADER_TIMEOUT", &cfg.Server.ReadHeaderTimeout},
		{"SERVER_IDLE_TIMEOUT", &cfg.Server.IdleTimeout},
		{"SERVER_SHUTDOWN_TIMEOUT", &cfg.Server.ShutdownTimeout},
		{"UPSTREAM_URL", &cfg.Upstream.URL},
		{"UPSTREAM_DIAL_TIMEOUT", &cfg.Upstream.DialTimeout},
		{"UPSTREAM_RESPONSE_HEADER_TIMEOUT", &cfg.Upstream.ResponseHeaderTimeout},
		{"UPSTREAM_MAX_IDLE_CONNECTIONS", &cfg.Upstream.MaxIdleConnections},
		{"UPSTREAM_MAX_IDLE_CONNECTIONS_PER_HOST", &cfg.Upstream.MaxIdleConnectionsPerHost},
		{"CHAOS_ENABLED", &cfg.Chaos.Enabled},
		{"CHAOS_REQUEST_DELAY_ENABLED", &cfg.Chaos.RequestDelay.Enabled},
		{"CHAOS_REQUEST_DELAY_MAX", &cfg.Chaos.RequestDelay.Max},
		{"CHAOS_RESPONSE_DELAY_ENABLED", &cfg.Chaos.ResponseDelay.Enabled},
		{"CHAOS_RESPONSE_DELAY_MAX", &cfg.Chaos.ResponseDelay.Max},
		{"CHAOS_ABORT_ENABLED", &cfg.Chaos.Abort.Enabled},
		{"CHAOS_PROBABILITY_ENABLED", &cfg.Chaos.Probability.Enabled},
		{"CHAOS_PROBABILITY_DEFAULT", &cfg.Chaos.Probability.Default},
		{"CHAOS_REQUEST_JITTER_ENABLED", &cfg.Chaos.RequestJitter.Enabled},
		{"CHAOS_RESPONSE_JITTER_ENABLED", &cfg.Chaos.ResponseJitter.Enabled},
		{"CHAOS_REQUEST_BANDWIDTH_ENABLED", &cfg.Chaos.RequestBandwidth.Enabled},
		{"CHAOS_RESPONSE_BANDWIDTH_ENABLED", &cfg.Chaos.ResponseBandwidth.Enabled},
		{"CHAOS_CONNECTION_RESET_ENABLED", &cfg.Chaos.ConnectionReset.Enabled},
		{"CHAOS_CONNECTION_RESET_AFTER_BYTES", &cfg.Chaos.ConnectionReset.AfterBytes},
		{"CHAOS_TUNNEL_TIMEOUT_MAX", &cfg.Chaos.TunnelTimeout.Max},
		{"CHAOS_MAX_ACTIVE_FAULTS", &cfg.Chaos.Limits.MaxActiveFaults},
		{"CHAOS_STRIP_CONTROL_HEADERS", &cfg.Chaos.StripControlHeaders},
		{"CHAOS_TOKEN", &cfg.Chaos.Token},
		{"OBSERVABILITY_LISTEN", &cfg.Observability.Listen},
		{"OBSERVABILITY_METRICS_ENABLED", &cfg.Observability.Metrics.Enabled},
		{"OBSERVABILITY_LOGGING_LEVEL", &cfg.Observability.Logging.Level},
		{"OBSERVABILITY_LOGGING_FORMAT", &cfg.Observability.Logging.Format},
		{"OBSERVABILITY_TRACING_ENABLED", &cfg.Observability.Tracing.Enabled},
	}
	for _, binding := range bindings {
		if value, ok := lookup(binding.name); ok {
			if err := applyEnvironmentValue(binding.target, value); err != nil {
				return fmt.Errorf("invalid %s: expected %w", binding.name, err)
			}
		}
	}
	return nil
}

func applyEnvironmentValue(target any, value string) error {
	var err error
	var expected string
	switch p := target.(type) {
	case *string:
		*p = value
	case *bool:
		*p, err = strconv.ParseBool(value)
		expected = "a boolean"
	case *int:
		*p, err = strconv.Atoi(value)
		expected = "an integer"
	case *byteCount:
		var count int64
		count, err = strconv.ParseInt(value, 10, 64)
		*p = byteCount(count)
		expected = "a signed 64-bit integer"
	case *float64:
		*p, err = strconv.ParseFloat(value, 64)
		expected = "a number"
	case *time.Duration:
		*p, err = time.ParseDuration(value)
		expected = "a Go duration such as 1s or 250ms"
	case *slog.Level:
		err = p.UnmarshalText([]byte(value))
		expected = "a logging level such as info"
	}
	if err != nil {
		return errors.New(expected)
	}
	return nil
}

// Validate checks the complete startup configuration.
func (cfg Config) Validate() error {
	switch cfg.Server.Mode {
	case "", ModeHTTP, ModeTCP, ModeConnect:
	default:
		return errServerMode
	}
	proxyHost, _, err := net.SplitHostPort(cfg.Server.Listen)
	if err != nil {
		return errors.New("server.listen must be a host:port address")
	}
	if _, _, err := net.SplitHostPort(cfg.Observability.Listen); err != nil {
		return errors.New("observability.listen must be a host:port address")
	}
	if cfg.Upstream.URL == "" {
		return errors.New("upstream.url is required via -upstream, UPSTREAM_URL, or -config")
	}
	if cfg.Server.Mode == ModeTCP || cfg.Server.Mode == ModeConnect {
		if _, err := TCPAddress(cfg.Upstream.URL); err != nil {
			return err
		}
	}
	for _, bound := range []struct {
		name  string
		value time.Duration
	}{
		{"server.read_header_timeout", cfg.Server.ReadHeaderTimeout},
		{"server.idle_timeout", cfg.Server.IdleTimeout},
		{"server.shutdown_timeout", cfg.Server.ShutdownTimeout},
		{"upstream.dial_timeout", cfg.Upstream.DialTimeout},
		{"upstream.response_header_timeout", cfg.Upstream.ResponseHeaderTimeout},
	} {
		if bound.value <= 0 {
			return fmt.Errorf("%s must be positive", bound.name)
		}
	}
	for _, limit := range []struct {
		name  string
		value int
	}{
		{"upstream.max_idle_connections", cfg.Upstream.MaxIdleConnections},
		{"upstream.max_idle_connections_per_host", cfg.Upstream.MaxIdleConnectionsPerHost},
	} {
		if limit.value < 0 {
			return fmt.Errorf("%s must be nonnegative", limit.name)
		}
	}
	if cfg.Chaos.RequestDelay.Max < 0 {
		return errors.New("chaos.request_delay.max must be nonnegative")
	}
	if cfg.Chaos.ResponseDelay.Max < 0 {
		return errors.New("chaos.response_delay.max must be nonnegative")
	}
	if cfg.Chaos.TunnelTimeout.Max < 0 {
		return errors.New("chaos.tunnel_timeout.max must be nonnegative")
	}
	if cfg.Chaos.Limits.MaxActiveFaults <= 0 {
		return errors.New("chaos.limits.max_active_faults must be positive")
	}
	if p := cfg.Chaos.Probability.Default; math.IsNaN(p) || p < 0 || p > 1 {
		return errors.New("chaos.probability.default must be between 0 and 1")
	}
	if cfg.Chaos.ConnectionReset.AfterBytes < 0 {
		return errors.New("chaos.connection_reset.after_bytes must be nonnegative")
	}
	if cfg.Server.Mode != ModeTCP {
		if !cfg.Chaos.StripControlHeaders {
			return errors.New("chaos.strip_control_headers must be true")
		}
		if cfg.Chaos.Token != "" && !validToken(cfg.Chaos.Token) {
			return errors.New("CHAOS_TOKEN must be a single visible HTTP header value without a comma or leading or trailing spaces")
		}
		if cfg.Chaos.Enabled && cfg.Chaos.Token == "" {
			ip, err := netip.ParseAddr(proxyHost)
			if err != nil || !ip.Unmap().IsLoopback() {
				return errors.New("CHAOS_TOKEN is required when chaos is enabled and server.listen is not a literal loopback address")
			}
		}
	}
	if cfg.Observability.Tracing.Enabled {
		return errors.New("observability.tracing.enabled is unsupported and must be false")
	}
	if format := cfg.Observability.Logging.Format; format != "json" && format != "text" {
		return errors.New("observability.logging.format must be json or text")
	}
	return nil
}

// TCPAddress validates a fixed TCP upstream and returns an address suitable for dialing.
func TCPAddress(raw string) (string, error) {
	invalid := errors.New("upstream.url must be a TCP host:port address or tcp://host:port with a concrete host and numeric port 1-65535")
	address, isURL := strings.CutPrefix(raw, "tcp://")
	if strings.ContainsAny(address, "/?#@") || strings.ContainsFunc(address, func(ch rune) bool {
		return unicode.IsSpace(ch) || unicode.IsControl(ch)
	}) {
		return "", invalid
	}
	if isURL {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", invalid
		}
		address = parsed.Host
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", invalid
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return "", invalid
	}
	bracketed := strings.HasPrefix(address, "[")
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Is6() != bracketed || ip.Unmap().IsUnspecified() {
			return "", invalid
		}
	} else if bracketed || !validHostname(host) {
		return "", invalid
	}
	return address, nil
}

func validHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if len(host) > 253 {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		if strings.ContainsFunc(label, func(ch rune) bool {
			switch {
			case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '-', ch == '_':
				return false
			}
			return true
		}) {
			return false
		}
	}
	return true
}

func validToken(token string) bool {
	if token != strings.Trim(token, " ") {
		return false
	}
	for _, ch := range token {
		if ch < ' ' || ch > '~' || ch == ',' {
			return false
		}
	}
	return true
}
