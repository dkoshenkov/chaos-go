// Package server runs the proxy and its separate administrative HTTP listener.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/config"
	"github.com/dkoshenkov/chaos-go/internal/disconnect"
	"github.com/dkoshenkov/chaos-go/internal/observability"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
	"github.com/dkoshenkov/chaos-go/internal/tcpproxy"
)

// Run starts both listeners and drains them when ctx is canceled.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	observer := observability.New(logger)
	settings := faultSettings(cfg.Chaos)
	var data dataServer
	message := "proxy listening"
	switch cfg.Server.Mode {
	case config.ModeTCP, config.ModeConnect:
		var err error
		data, err = tcpDataServer(cfg, settings, observer)
		if err != nil {
			return fmt.Errorf("configure upstream: %w", err)
		}
		message = cfg.Server.Mode + " proxy listening"
	default:
		transport := upstreamTransport(cfg.Upstream)
		defer transport.CloseIdleConnections()
		proxyHandler, err := proxy.NewHandler(cfg.Upstream.URL, settings, transport, observer)
		if err != nil {
			return fmt.Errorf("configure upstream: %w", err)
		}
		data = httpDataServer(cfg.Server, proxyHandler)
	}
	var listenConfig net.ListenConfig
	proxyListener, err := listenConfig.Listen(ctx, "tcp", cfg.Server.Listen)
	if err != nil {
		return fmt.Errorf("listen for proxy requests: %w", err)
	}
	defer func() { _ = proxyListener.Close() }()
	adminListener, err := listenConfig.Listen(ctx, "tcp", cfg.Observability.Listen)
	if err != nil {
		return fmt.Errorf("listen for administrative requests: %w", err)
	}
	defer func() { _ = adminListener.Close() }()

	var metrics http.Handler
	if cfg.Observability.Metrics.Enabled {
		metrics = observer.MetricsHandler()
	}
	logger.Info(message, "address", proxyListener.Addr().String())
	logger.Info("admin listening", "address", adminListener.Addr().String())
	return serve(ctx, cfg.Server, proxyListener, adminListener, data, metrics)
}

func faultSettings(cfg config.Chaos) chaos.Settings {
	return chaos.Settings{
		Enabled:                  cfg.Enabled,
		RequestEnabled:           cfg.RequestDelay.Enabled,
		ResponseEnabled:          cfg.ResponseDelay.Enabled,
		MaxRequestDelay:          cfg.RequestDelay.Max,
		MaxResponseDelay:         cfg.ResponseDelay.Max,
		AbortEnabled:             cfg.Abort.Enabled,
		ProbabilityEnabled:       cfg.Probability.Enabled,
		DefaultProbability:       cfg.Probability.Default,
		RequestJitterEnabled:     cfg.RequestJitter.Enabled,
		ResponseJitterEnabled:    cfg.ResponseJitter.Enabled,
		RequestBandwidthEnabled:  cfg.RequestBandwidth.Enabled,
		ResponseBandwidthEnabled: cfg.ResponseBandwidth.Enabled,
		ConnectionResetEnabled:   cfg.ConnectionReset.Enabled,
		ResetAfterBytes:          int64(cfg.ConnectionReset.AfterBytes),
		MaxActiveFaults:          cfg.Limits.MaxActiveFaults,
		Token:                    cfg.Token,
	}
}

func tcpDataServer(cfg config.Config, settings chaos.Settings, observer *observability.Observer) (dataServer, error) {
	upstream, err := config.TCPAddress(cfg.Upstream.URL)
	if err != nil {
		return dataServer{}, err
	}
	opts := tcpproxy.Options{
		Upstream:        upstream,
		DialTimeout:     cfg.Upstream.DialTimeout,
		ResetEnabled:    settings.Enabled && settings.ConnectionResetEnabled,
		ResetAfterBytes: settings.ResetAfterBytes,
		Probability:     1,
		MaxActiveFaults: settings.MaxActiveFaults,
	}
	if settings.ProbabilityEnabled {
		opts.Probability = settings.DefaultProbability
	}
	if cfg.Server.Mode == config.ModeConnect {
		return dataServer{serve: func(ctx context.Context, listener net.Listener) error {
			return tcpproxy.ServeConnect(ctx, listener, opts, settings, cfg.Chaos.TunnelTimeout.Max,
				cfg.Server.ReadHeaderTimeout, cfg.Server.ShutdownTimeout, observer)
		}}, nil
	}
	return dataServer{serve: func(ctx context.Context, listener net.Listener) error {
		return tcpproxy.Serve(ctx, listener, opts, cfg.Server.ShutdownTimeout, observer)
	}}, nil
}

func upstreamTransport(cfg config.Upstream) *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           pooledUploadDial(dialer.DialContext),
		Protocols:             protocols,
		DisableCompression:    true,
		MaxIdleConns:          cfg.MaxIdleConnections,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnectionsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
}

type dataServer struct {
	serve    func(context.Context, net.Listener) error
	shutdown func(context.Context) error
}

func httpDataServer(cfg config.Server, handler http.Handler) dataServer {
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ConnContext:       disconnect.ConnContext,
		Protocols:         protocols,
		Handler:           handler,
	}
	return dataServer{
		serve: func(_ context.Context, listener net.Listener) error {
			return serveHTTP(server, listener, "proxy")
		},
		shutdown: func(ctx context.Context) error {
			return shutdownHTTP(ctx, server, "proxy")
		},
	}
}

func serve(ctx context.Context, cfg config.Server, proxyListener, adminListener net.Listener, data dataServer, metrics http.Handler) error {
	var ready atomic.Bool
	adminServer := &http.Server{
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		Handler:           adminHandler(&ready, metrics),
	}
	// Let readiness drop before the data listener starts draining.
	dataCtx, stopData := context.WithCancel(context.WithoutCancel(ctx))
	dataDone := make(chan error, 1)
	adminDone := make(chan error, 1)
	ready.Store(true)
	go func() { dataDone <- data.serve(dataCtx, proxyListener) }()
	go func() { adminDone <- serveHTTP(adminServer, adminListener, "admin") }()

	var dataErr, adminErr error
	var dataStopped, adminStopped bool
	select {
	case <-ctx.Done():
	case dataErr = <-dataDone:
		dataStopped = true
	case adminErr = <-adminDone:
		adminStopped = true
	}
	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	stopData()
	var dataShutdownErr error
	if data.shutdown != nil {
		dataShutdownErr = data.shutdown(shutdownCtx)
	}
	if !dataStopped {
		dataErr = <-dataDone
	}
	// Keep readiness available while in-flight proxy requests drain.
	adminShutdownErr := shutdownHTTP(shutdownCtx, adminServer, "admin")
	if !adminStopped {
		adminErr = <-adminDone
	}
	return errors.Join(dataErr, dataShutdownErr, adminErr, adminShutdownErr)
}

func serveHTTP(server *http.Server, listener net.Listener, name string) error {
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve %s HTTP: %w", name, err)
	}
	return nil
}

func shutdownHTTP(ctx context.Context, server *http.Server, name string) error {
	if err := server.Shutdown(ctx); err != nil {
		return errors.Join(fmt.Errorf("shut down %s HTTP: %w", name, err), server.Close())
	}
	return nil
}

func adminHandler(ready *atomic.Bool, metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}
	info := currentVersion()
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	})
	return mux
}
