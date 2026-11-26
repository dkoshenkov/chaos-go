// Command load runs an open-loop load test through an isolated loopback proxy.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/chaos"
	"github.com/dkoshenkov/chaos-go/internal/disconnect"
	"github.com/dkoshenkov/chaos-go/internal/observability"
	"github.com/dkoshenkov/chaos-go/internal/proxy"
)

const (
	maxDelay       = 120 * time.Second
	maxConcurrency = 2048
)

var (
	errBulk  = errors.New("bulk cancellation")
	errDrain = errors.New("drain timeout")
)

type options struct {
	rps           int
	duration      time.Duration
	delayPercent  int
	requestDelay  time.Duration
	responseDelay time.Duration
	bodyBytes     int
	upload        bool
	inFlight      int
	faultSlots    int
	cancelAfter   time.Duration
	drainTimeout  time.Duration
}

func readOptions() (options, error) {
	var o options
	flag.IntVar(&o.rps, "rps", 100, "offered requests per second (100, 1000, or 5000)")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "duration of the offered load (1ms to 2m)")
	flag.IntVar(&o.delayPercent, "delay-percent", 0, "percentage of requests carrying delays")
	flag.DurationVar(&o.requestDelay, "request-delay", 2*time.Second, "delay before upstream dispatch (0 to 120s, whole milliseconds)")
	flag.DurationVar(&o.responseDelay, "response-delay", 3*time.Second, "delay before final response headers (0 to 120s, whole milliseconds)")
	size := flag.String("body-size", "1KB", "streamed response size (1KB, 100KB, or 10MB, binary units)")
	flag.BoolVar(&o.upload, "upload", false, "also stream a request body of the selected size")
	flag.IntVar(&o.inFlight, "max-in-flight", 512, "upper bound on concurrent client requests (1 to 2048)")
	flag.IntVar(&o.faultSlots, "fault-slots", 256, "proxy active fault limit (1 to 2048)")
	flag.DurationVar(&o.cancelAfter, "cancel-after", 0, "cancel all outstanding delayed requests launched before this point (0 disables)")
	flag.DurationVar(&o.drainTimeout, "drain-timeout", 2*maxDelay+5*time.Second, "wait after the offering window before canceling remaining requests (up to 5m)")
	flag.Parse()

	switch *size {
	case "1KB":
		o.bodyBytes = 1 << 10
	case "100KB":
		o.bodyBytes = 100 << 10
	case "10MB":
		o.bodyBytes = 10 << 20
	default:
		return o, errors.New("body-size must be 1KB, 100KB, or 10MB")
	}
	if flag.NArg() != 0 {
		return o, errors.New("unexpected positional arguments")
	}
	if o.rps != 100 && o.rps != 1000 && o.rps != 5000 {
		return o, errors.New("rps must be 100, 1000, or 5000")
	}
	if o.duration < time.Millisecond || o.duration > 2*time.Minute || o.offerCount() == 0 {
		return o, errors.New("duration must be between 1ms and 2m and allow at least one offer")
	}
	if o.delayPercent < 0 || o.delayPercent > 100 {
		return o, errors.New("delay-percent must be between 0 and 100")
	}
	for _, delay := range []time.Duration{o.requestDelay, o.responseDelay} {
		if delay < 0 || delay > maxDelay || delay%time.Millisecond != 0 {
			return o, errors.New("delays must be between 0ms and 120s in whole milliseconds")
		}
	}
	if o.inFlight < 1 || o.inFlight > maxConcurrency || o.faultSlots < 1 || o.faultSlots > maxConcurrency {
		return o, errors.New("max-in-flight and fault-slots must be between 1 and 2048")
	}
	if o.cancelAfter < 0 || o.cancelAfter > o.duration || (o.cancelAfter > 0 && o.delayPercent == 0) {
		return o, errors.New("cancel-after must fall within the offering window and requires a nonzero delay-percent")
	}
	if o.drainTimeout <= 0 || o.drainTimeout > 5*time.Minute {
		return o, errors.New("drain-timeout must be positive and no more than 5m")
	}
	return o, nil
}

func (o options) offerCount() int {
	return int(o.duration * time.Duration(o.rps) / time.Second)
}

type connectionGauge struct {
	active atomic.Int64
	peak   atomic.Int64
}

func (g *connectionGauge) observe(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		current := g.active.Add(1)
		for peak := g.peak.Load(); current > peak && !g.peak.CompareAndSwap(peak, current); peak = g.peak.Load() {
		}
	case http.StateClosed, http.StateHijacked:
		g.active.Add(-1)
	}
}

func newLoopbackServer(ctx context.Context, handler http.Handler, connections *connectionGauge) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = connections.observe
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	return server
}

func newTransport(maxConns int) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns = maxConns
	transport.MaxIdleConnsPerHost = maxConns
	transport.MaxConnsPerHost = maxConns
	return transport
}

type outcome struct {
	end     time.Time
	headers time.Duration
	full    time.Duration
	status  int
	err     error
	cause   error
}

type results struct {
	mu           sync.Mutex
	beforeEnd    int
	okBeforeEnd  int
	ok           int
	rejected     int
	statusErrors int
	ioErrors     int
	firstIOError error
	canceled     int
	incomplete   int
	headerTimes  [2][]time.Duration
	fullTimes    [2][]time.Duration
}

func (r *results) record(got outcome, delayed bool, cutoff time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if got.err == nil && !got.end.After(cutoff) {
		r.beforeEnd++
	}
	switch {
	case got.err != nil && errors.Is(got.cause, errDrain):
		r.incomplete++
	case got.err != nil && errors.Is(got.cause, errBulk):
		r.canceled++
	case got.err != nil:
		r.ioErrors++
		if r.firstIOError == nil {
			r.firstIOError = got.err
		}
	case got.status == http.StatusTooManyRequests:
		r.rejected++
	case got.status != http.StatusOK:
		r.statusErrors++
	default:
		r.ok++
		if !got.end.After(cutoff) {
			r.okBeforeEnd++
		}
		class := 0
		if delayed {
			class = 1
		}
		r.headerTimes[class] = append(r.headerTimes[class], got.headers)
		r.fullTimes[class] = append(r.fullTimes[class], got.full)
	}
}

func issue(ctx context.Context, client *http.Client, url string, o options, delayed bool, offeredAt time.Time, payload []byte) outcome {
	method := http.MethodGet
	var body io.Reader
	if o.upload {
		method = http.MethodPost
		body = io.LimitReader(repeatingReader{chunk: payload}, int64(o.bodyBytes))
	}
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return outcome{end: time.Now(), err: err}
	}
	if o.upload {
		request.ContentLength = int64(o.bodyBytes)
	}
	if delayed {
		request.Header.Set("X-Chaos-Request-Delay-Ms", strconv.FormatInt(o.requestDelay.Milliseconds(), 10))
		request.Header.Set("X-Chaos-Response-Delay-Ms", strconv.FormatInt(o.responseDelay.Milliseconds(), 10))
	}
	response, err := client.Do(request)
	if err != nil {
		return outcome{end: time.Now(), err: err, cause: context.Cause(ctx)}
	}
	headers := time.Since(offeredAt)
	n, err := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil && response.StatusCode == http.StatusOK && n != int64(o.bodyBytes) {
		err = fmt.Errorf("read response body: %w, read %d of %d bytes", io.ErrUnexpectedEOF, n, o.bodyBytes)
	}
	end := time.Now()
	return outcome{end: end, headers: headers, full: end.Sub(offeredAt), status: response.StatusCode, err: err, cause: context.Cause(ctx)}
}

// The same bounded chunk serves all requests without allocating a body per request.
type repeatingReader struct {
	chunk []byte
}

func (r repeatingReader) Read(p []byte) (int, error) {
	return copy(p, r.chunk), nil
}

func run(o options) error {
	payload := bytes.Repeat([]byte{'a'}, 32<<10)
	var backendConns, proxyConns connectionGauge
	serverContext, stopServers := context.WithCancel(context.Background())
	backend := newLoopbackServer(serverContext, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			return
		}
		if o.upload && n != int64(o.bodyBytes) {
			http.Error(w, "incomplete upload", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(o.bodyBytes))
		_, _ = io.CopyN(w, repeatingReader{chunk: payload}, int64(o.bodyBytes))
	}), &backendConns)
	backend.Start()
	defer func() {
		stopServers()
		backend.Close()
	}()

	upstream := newTransport(o.inFlight)
	defer upstream.CloseIdleConnections()
	handler, err := proxy.NewHandler(backend.URL, chaos.Settings{
		Enabled:          true,
		RequestEnabled:   true,
		ResponseEnabled:  true,
		MaxRequestDelay:  maxDelay,
		MaxResponseDelay: maxDelay,
		MaxActiveFaults:  o.faultSlots,
	}, upstream, observability.New(nil))
	if err != nil {
		return err
	}
	front := newLoopbackServer(serverContext, handler, &proxyConns)
	front.Config.ConnContext = disconnect.ConnContext
	front.Start()
	defer func() {
		stopServers()
		front.Close()
	}()

	transport := newTransport(o.inFlight)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	runContext, stopRun := context.WithCancelCause(context.Background())
	defer stopRun(nil)
	bulkContext, stopBulk := context.WithCancelCause(runContext)
	defer stopBulk(nil)

	samples := beginResources()
	start := time.Now()
	cutoff := start.Add(o.duration)
	cancelAt := start.Add(o.cancelAfter)
	if o.cancelAfter > 0 {
		timer := time.AfterFunc(o.cancelAfter, func() { stopBulk(errBulk) })
		defer timer.Stop()
	}

	var results results
	var workers sync.WaitGroup
	slots := make(chan struct{}, o.inFlight)
	scheduled, started, capDrops, lateDrops := 0, 0, 0, 0
	total := o.offerCount()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		elapsed := now.Sub(start)
		due := min(total, int(elapsed*time.Duration(o.rps)/time.Second))
		// Skip stale slots if the scheduler falls more than 10ms behind.
		onTime := max(scheduled, due-o.rps/100)
		lateDrops += onTime - scheduled
		scheduled = onTime
		for index := scheduled; index < due; index++ {
			select {
			case slots <- struct{}{}:
				started++
				delayed := (index*o.delayPercent)%100 < o.delayPercent
				ctx := runContext
				if delayed && o.cancelAfter > 0 && now.Before(cancelAt) {
					ctx = bulkContext
				}
				workers.Go(func() {
					defer func() { <-slots }()
					got := issue(ctx, client, front.URL, o, delayed, now, payload)
					results.record(got, delayed, cutoff)
				})
			default:
				capDrops++
			}
		}
		scheduled = due
		if !now.Before(cutoff) {
			break
		}
	}
	ticker.Stop()
	offerResources := readProcess()
	offeredElapsed := time.Since(start)

	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	drain := time.NewTimer(o.drainTimeout)
	defer drain.Stop()
	select {
	case <-done:
	case <-drain.C:
		stopRun(errDrain)
		<-done
	}

	elapsed := time.Since(start)
	resourceSummary := samples.stop()
	connectionsBeforeCleanup := [2]int64{proxyConns.active.Load(), backendConns.active.Load()}
	transport.CloseIdleConnections()
	upstream.CloseIdleConnections()
	settleConnections(&proxyConns, &backendConns)
	postCleanup := snapshotResources()
	fmt.Printf("target_rps=%d duration=%s body_bytes=%d upload=%t delay_percent=%d request_delay=%s response_delay=%s\n",
		o.rps, o.duration, o.bodyBytes, o.upload, o.delayPercent, o.requestDelay, o.responseDelay)
	fmt.Printf("offered=%d started=%d dropped_at_in_flight_cap=%d dropped_late=%d max_in_flight=%d fault_slots=%d\n",
		total, started, capDrops, lateDrops, o.inFlight, o.faultSlots)
	fmt.Printf("completed=%d ok=%d rejected_429=%d other_status=%d io_errors=%d bulk_canceled=%d drain_incomplete=%d elapsed=%s\n",
		results.ok+results.rejected+results.statusErrors, results.ok, results.rejected, results.statusErrors,
		results.ioErrors, results.canceled, results.incomplete, elapsed.Round(time.Millisecond))
	if results.firstIOError != nil {
		fmt.Printf("first_io_error=%q\n", results.firstIOError)
	}
	fmt.Printf("offering_completed_rps=%.1f offering_ok_rps=%.1f overall_ok_rps=%.1f\n",
		float64(results.beforeEnd)/o.duration.Seconds(), float64(results.okBeforeEnd)/o.duration.Seconds(), float64(results.ok)/elapsed.Seconds())
	for index, label := range []string{"ordinary", "delayed"} {
		fmt.Printf("%s ", label)
		reportLatencies("final_headers", results.headerTimes[index])
		reportLatencies("full_response", results.fullTimes[index])
		fmt.Println()
	}
	resourceSummary.print(offerResources, postCleanup, offeredElapsed, elapsed)
	fmt.Printf("connections_proxy_peak=%d connections_proxy_before_cleanup=%d connections_proxy_after_cleanup=%d "+
		"connections_backend_peak=%d connections_backend_before_cleanup=%d connections_backend_after_cleanup=%d\n",
		proxyConns.peak.Load(), connectionsBeforeCleanup[0], proxyConns.active.Load(),
		backendConns.peak.Load(), connectionsBeforeCleanup[1], backendConns.active.Load())
	return nil
}

func settleConnections(proxyConns, backendConns *connectionGauge) {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for proxyConns.active.Load() != 0 || backendConns.active.Load() != 0 {
		select {
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
	// Give the client transport goroutines time to finish closing idle sockets.
	<-ticker.C
}

func reportLatencies(name string, times []time.Duration) {
	if len(times) == 0 {
		fmt.Printf("%s_samples=0 ", name)
		return
	}
	slices.Sort(times)
	percentile := func(p int) time.Duration { return times[(p*len(times)+99)/100-1].Round(time.Microsecond) }
	fmt.Printf("%s_samples=%d %s_p50=%s %s_p95=%s %s_p99=%s ",
		name, len(times), name, percentile(50), name, percentile(95), name, percentile(99))
}

func main() {
	options, err := readOptions()
	if err != nil {
		log.Fatal(err)
	}
	if err := run(options); err != nil {
		log.Fatal(err)
	}
}
