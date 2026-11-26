package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dkoshenkov/chaos-go/internal/config"
)

// Main runs the CLI and returns its process exit status.
func Main(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("chaos-proxy", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "", "path to a YAML configuration file")
	listen := flags.String("listen", "", "override the proxy listen address (default 127.0.0.1:8080)")
	upstream := flags.String("upstream", "", "override the fixed upstream HTTP(S) URL")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: chaos-proxy [-config file] [-listen addr] [-upstream URL]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); errors.Is(err, flag.ErrHelp) {
		return 0
	} else if err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected argument %q (see -help)\n", flags.Arg(0))
		return 2
	}
	var overrides config.Overrides
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "listen":
			overrides.Listen = listen
		case "upstream":
			overrides.UpstreamURL = upstream
		}
	})
	cfg, err := config.Load(*path, overrides)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	options := &slog.HandlerOptions{Level: cfg.Observability.Logging.Level}
	var handler slog.Handler
	if cfg.Observability.Logging.Format == "text" {
		handler = slog.NewTextHandler(stderr, options)
	} else {
		handler = slog.NewJSONHandler(stderr, options)
	}
	logger := slog.New(handler)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, cfg, logger); err != nil {
		logger.Error("proxy stopped", "error", err)
		return 1
	}
	return 0
}
