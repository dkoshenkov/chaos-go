package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/config"
	"github.com/dkoshenkov/chaos-go/internal/tcpproxy"
)

const (
	tokenHeader              = "X-Chaos-Token"
	maxConnectResponseHeader = 32 << 10
)

type options struct {
	listen           string
	proxy            string
	target           string
	headers          http.Header
	handshakeTimeout time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv("CHAOS_TOKEN"), os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, token string, stderr io.Writer) int {
	cfg, err := parseOptions(args, token, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	logger := log.New(stderr, "chaos-tunnel: ", 0)
	if err != nil {
		logger.Print(err)
		return 2
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.listen)
	if err != nil {
		logger.Printf("open listener: %v", err)
		return 1
	}
	if err := cfg.serve(ctx, listener.(*net.TCPListener), logger); err != nil {
		logger.Printf("accept connection: %v", err)
		return 1
	}
	return 0
}

func parseOptions(args []string, token string, output io.Writer) (options, error) {
	var cfg options
	flags := flag.NewFlagSet("chaos-tunnel", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.listen, "listen", "127.0.0.1:19092", "local TCP listen address")
	flags.StringVar(&cfg.proxy, "proxy", "", "HTTP/1 CONNECT proxy address (host:port)")
	flags.StringVar(&cfg.target, "target", "", "fixed CONNECT target authority (host:port)")
	flags.DurationVar(&cfg.handshakeTimeout, "handshake-timeout", 5*time.Minute, "maximum time to dial the proxy and complete the CONNECT handshake")
	var headers []string
	flags.Func("header", "CONNECT control header, repeatable ('X-Chaos-Name: value')", func(value string) error {
		headers = append(headers, value)
		return nil
	})
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return options{}, err
		}
		return options{}, errors.New("invalid command-line arguments")
	}
	if flags.NArg() != 0 {
		return options{}, errors.New("unexpected positional arguments")
	}
	if _, _, err := net.SplitHostPort(cfg.listen); err != nil {
		return options{}, errors.New("-listen must be a host:port address")
	}
	if addr, err := config.TCPAddress(cfg.proxy); err != nil || addr != cfg.proxy {
		return options{}, errors.New("-proxy must be a host:port address with a concrete host and numeric port 1-65535")
	}
	if addr, err := config.TCPAddress(cfg.target); err != nil || addr != cfg.target || strings.Contains(addr, "%") {
		return options{}, errors.New("-target must be a host:port authority with a concrete host and numeric port 1-65535")
	}
	if cfg.handshakeTimeout <= 0 {
		return options{}, errors.New("-handshake-timeout must be positive")
	}
	cfg.headers = make(http.Header)
	for _, line := range headers {
		name, value, ok := strings.Cut(line, ":")
		if !ok || !validControlName(name) || !validHeaderValue(value) {
			return options{}, errors.New("-header must contain an X-Chaos header name and a single-line value")
		}
		if strings.EqualFold(name, tokenHeader) {
			return options{}, errors.New("set the chaos token through CHAOS_TOKEN")
		}
		cfg.headers.Add(name, strings.Trim(value, " \t"))
	}
	if token != "" {
		if !validHeaderValue(token) || token != strings.Trim(token, " ") || strings.ContainsAny(token, ",\t") {
			return options{}, errors.New("CHAOS_TOKEN must be a single visible HTTP header value without a comma or leading or trailing spaces")
		}
		cfg.headers.Set(tokenHeader, token)
	}
	return cfg, nil
}

func validControlName(name string) bool {
	const prefix = "x-chaos-"
	const fieldNameCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&'*+-.^_`|~"
	if len(name) <= len(prefix) || !strings.EqualFold(name[:len(prefix)], prefix) {
		return false
	}
	return !strings.ContainsFunc(name[len(prefix):], func(ch rune) bool {
		return !strings.ContainsRune(fieldNameCharacters, ch)
	})
}

func validHeaderValue(value string) bool {
	return !strings.ContainsFunc(value, func(ch rune) bool {
		return ch != '\t' && (ch < ' ' || ch > '~')
	})
}

func (cfg options) serve(ctx context.Context, listener *net.TCPListener, logger *log.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	var sessions sync.WaitGroup
	defer func() {
		cancel()
		stopAccept()
		_ = listener.Close()
		sessions.Wait()
	}()
	for ctx.Err() == nil {
		client, err := listener.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		sessions.Go(func() { cfg.handle(ctx, client, logger) })
	}
	return nil
}

func (cfg options) handle(ctx context.Context, client *net.TCPConn, logger *log.Logger) {
	defer func() { _ = client.Close() }()
	handshakeCtx, stopClientWatch := watchClient(ctx, client)
	proxy, reader, err := cfg.connect(handshakeCtx)
	canceled := handshakeCtx.Err() != nil
	clientReader := stopClientWatch()
	if err != nil {
		if !canceled {
			_ = client.SetLinger(0)
			logger.Print(err)
		}
		return
	}
	defer func() { _ = proxy.Close() }()
	if !canceled {
		_ = tcpproxy.Relay(ctx, client, proxy, clientReader, reader)
	}
}

// watchClient detects a disconnect before the CONNECT response when the client
// has sent no data. Reading one byte lets a pipelined request keep its half-close.
func watchClient(ctx context.Context, client *net.TCPConn) (context.Context, func() io.Reader) {
	ctx, cancel := context.WithCancel(ctx)
	var first [1]byte
	read := make(chan int, 1)
	go func() {
		n, err := client.Read(first[:])
		if n == 0 && err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			cancel()
		}
		read <- n
	}()
	return ctx, func() io.Reader {
		_ = client.SetReadDeadline(time.Now())
		n := <-read
		_ = client.SetReadDeadline(time.Time{})
		cancel()
		if n == 0 {
			return nil
		}
		return io.MultiReader(bytes.NewReader(first[:n]), client)
	}
}

func (cfg options) connect(ctx context.Context) (*net.TCPConn, io.Reader, error) {
	handshake, cancel := context.WithTimeout(ctx, cfg.handshakeTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(handshake, "tcp", cfg.proxy)
	if err != nil {
		return nil, nil, errors.New("could not dial the CONNECT proxy")
	}
	proxy := conn.(*net.TCPConn)
	established := false
	defer func() {
		if !established {
			_ = proxy.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = proxy.Close() })
	defer stop()
	deadline, _ := handshake.Deadline()
	if err := proxy.SetDeadline(deadline); err != nil {
		return nil, nil, errors.New("could not set the CONNECT handshake deadline")
	}

	request := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: cfg.target},
		Host:   cfg.target,
		Header: cfg.headers,
	}
	if err := request.Write(proxy); err != nil {
		return nil, nil, errors.New("could not write the CONNECT handshake")
	}
	reader, err := readConnectResponse(proxy, request)
	if err != nil {
		return nil, nil, err
	}
	if err := proxy.SetDeadline(time.Time{}); err != nil {
		return nil, nil, errors.New("could not clear the CONNECT handshake deadline")
	}
	established = true
	return proxy, reader, nil
}

func readConnectResponse(proxy io.Reader, request *http.Request) (io.Reader, error) {
	limited := &io.LimitedReader{R: proxy, N: maxConnectResponseHeader}
	reader := bufio.NewReader(limited)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		if limited.N == 0 {
			return nil, errors.New("CONNECT proxy response headers exceeded 32 KiB")
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return nil, errors.New("CONNECT proxy response timed out")
		}
		return nil, errors.New("could not read a valid CONNECT proxy response")
	}
	if response.ProtoMajor != 1 {
		return nil, errors.New("CONNECT proxy did not respond with HTTP/1")
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CONNECT proxy returned HTTP %d", response.StatusCode)
	}
	// A successful CONNECT switches the stream at the end of the headers.
	// Relay starts with any bytes read ahead, followed by the unbounded socket.
	if reader.Buffered() == 0 {
		return proxy, nil
	}
	buffered := io.LimitReader(reader, int64(reader.Buffered()))
	return io.MultiReader(buffered, proxy), nil
}
