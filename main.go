package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:8080", "address to listen on")
	upstreamURL := flag.String("upstream", "", "fixed upstream HTTP(S) URL (required)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -upstream URL [-listen addr]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 0 {
		log.Fatalf("unexpected argument %q (see -help)", flag.Arg(0))
	}
	if *upstreamURL == "" {
		log.Fatal("missing required -upstream URL (see -help)")
	}
	if err := serve(*listenAddr, *upstreamURL); err != nil {
		log.Fatal(err)
	}
}

func serve(listenAddr, upstreamURL string) error {
	handler, err := proxy.New(upstreamURL)
	if err != nil {
		return fmt.Errorf("configure upstream: %w", err)
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen for HTTP requests: %w", err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	log.Printf("listening on %s", listener.Addr())

	select {
	case err := <-serveDone:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-signalCtx.Done():
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}
