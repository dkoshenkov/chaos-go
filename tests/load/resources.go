package main

import (
	"fmt"
	"runtime"
	"time"
)

type processSample struct {
	cpu    time.Duration
	rss    int64
	fds    int
	hasCPU bool
}

type resourceSnapshot struct {
	processSample
	heap  uint64
	goros int
}

type resources struct {
	first     resourceSnapshot
	last      resourceSnapshot
	heapPeak  uint64
	ramPeak   int64
	gorosPeak int
	fdsPeak   int
}

type resourceSampler struct {
	done   chan struct{}
	result chan resources
}

func beginResources() resourceSampler {
	s := resourceSampler{done: make(chan struct{}), result: make(chan resources, 1)}
	summary := resources{ramPeak: -1, fdsPeak: -1}
	summary.sample()
	summary.first = summary.last
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				summary.sample()
			case <-s.done:
				summary.sample()
				s.result <- summary
				return
			}
		}
	}()
	return s
}

func (s resourceSampler) stop() resources {
	close(s.done)
	return <-s.result
}

func snapshotResources() resourceSnapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return resourceSnapshot{processSample: readProcess(), heap: mem.HeapAlloc, goros: runtime.NumGoroutine()}
}

func (r *resources) sample() {
	r.last = snapshotResources()
	r.heapPeak = max(r.heapPeak, r.last.heap)
	r.ramPeak = max(r.ramPeak, r.last.rss)
	r.gorosPeak = max(r.gorosPeak, r.last.goros)
	r.fdsPeak = max(r.fdsPeak, r.last.fds)
}

func (r resources) print(offer processSample, cleaned resourceSnapshot, offerElapsed, elapsed time.Duration) {
	fmt.Printf("go_heap_baseline_bytes=%d go_heap_peak_bytes=%d go_heap_after_cleanup_bytes=%d "+
		"goroutines_baseline=%d goroutines_peak=%d goroutines_after_cleanup=%d",
		r.first.heap, r.heapPeak, cleaned.heap, r.first.goros, r.gorosPeak, cleaned.goros)
	if r.ramPeak >= 0 {
		fmt.Printf(" rss_baseline_bytes=%d rss_peak_bytes=%d rss_after_cleanup_bytes=%d", r.first.rss, r.ramPeak, cleaned.rss)
	}
	if r.fdsPeak >= 0 {
		fmt.Printf(" open_fds_baseline=%d open_fds_peak=%d open_fds_after_cleanup=%d", r.first.fds, r.fdsPeak, cleaned.fds)
	}
	if r.first.hasCPU && r.last.hasCPU && offer.hasCPU {
		fmt.Printf(" cpu_offering_percent=%.1f cpu_overall_percent=%.1f",
			100*float64(offer.cpu-r.first.cpu)/float64(offerElapsed),
			100*float64(r.last.cpu-r.first.cpu)/float64(elapsed))
	}
	fmt.Println()
}
