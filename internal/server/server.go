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
)

// Run starts both listeners and drains them when ctx is canceled.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	transport := upstreamTransport(cfg.Upstream)
	defer transport.CloseIdleConnections()
	observer := observability.New(logger)
	settings := chaos.Settings{
		Enabled:          cfg.Chaos.Enabled,
		RequestEnabled:   cfg.Chaos.RequestDelay.Enabled,
		ResponseEnabled:  cfg.Chaos.ResponseDelay.Enabled,
		MaxRequestDelay:  cfg.Chaos.RequestDelay.Max,
		MaxResponseDelay: cfg.Chaos.ResponseDelay.Max,
		MaxActiveFaults:  cfg.Chaos.Limits.MaxActiveFaults,
		Token:            cfg.Chaos.Token,
	}
	proxyHandler, err := proxy.NewHandler(cfg.Upstream.URL, settings, transport, observer)
	if err != nil {
		return fmt.Errorf("configure upstream: %w", err)
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
	logger.Info("proxy listening", "address", proxyListener.Addr().String())
	logger.Info("admin listening", "address", adminListener.Addr().String())
	return serve(ctx, cfg.Server, proxyListener, adminListener, proxyHandler, metrics)
}

func upstreamTransport(cfg config.Upstream) *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
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

func serve(ctx context.Context, cfg config.Server, proxyListener, adminListener net.Listener, proxyHandler, metrics http.Handler) error {
	var ready atomic.Bool
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	proxyServer := &http.Server{
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ConnContext:       disconnect.ConnContext,
		Protocols:         protocols,
		Handler:           proxyHandler,
	}
	adminServer := &http.Server{
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		Handler:           adminHandler(&ready, metrics),
	}
	ready.Store(true)
	serveErrors := make(chan error, 2)
	go func() { serveErrors <- serveHTTP(proxyServer, proxyListener, "proxy") }()
	go func() { serveErrors <- serveHTTP(adminServer, adminListener, "admin") }()

	remaining := 2
	var result error
	select {
	case <-ctx.Done():
	case result = <-serveErrors:
		remaining--
	}
	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	// Keep readiness available while in-flight proxy requests drain.
	result = errors.Join(result, shutdownHTTP(shutdownCtx, proxyServer, "proxy"))
	result = errors.Join(result, shutdownHTTP(shutdownCtx, adminServer, "admin"))
	for range remaining {
		result = errors.Join(result, <-serveErrors)
	}
	return result
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
