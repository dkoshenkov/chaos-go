# Loopback load test

The harness starts its own HTTP backend and Chaos-Go proxy on loopback listeners and stops both at the end of the run. The client always uses those listeners. It runs the proxy with request metrics enabled and request logs disabled.

Run these commands from the repository root with Go 1.25 or later.

```sh
go run ./tests/load -rps 100 -duration 10s -body-size 1KB
go run ./tests/load -rps 1000 -duration 10s -delay-percent 10 -request-delay 2s -response-delay 3s -body-size 100KB
go run ./tests/load -rps 5000 -duration 10s -body-size 10MB -upload
go run ./tests/load -rps 100 -duration 5s -delay-percent 50 -request-delay 60s -response-delay 60s -cancel-after 4s -drain-timeout 130s
```

Select 100, 1,000, or 5,000 requests per second and use `-delay-percent` to apply both configured delays to a selected share of requests. Set either delay to zero to exercise the other direction on its own. Body size selects 1 KiB, 100 KiB, or 10 MiB of streamed response data. With `-upload`, the client streams the same amount of request data. The backend discards the upload while counting bytes and sends the response from a shared 32 KiB buffer.

The client offers requests at the selected rate without waiting for earlier requests to complete. It caps outstanding requests with `-max-in-flight` and skips offers when that cap is full or the scheduler falls behind. `-fault-slots` controls how many delayed requests the proxy admits at once. At `-cancel-after`, the harness cancels the outstanding delayed requests started before that time. Later requests use a fresh context. Use `-drain-timeout` to cap the wait for those requests to finish after the offering window.

The report counts completed HTTP responses, I/O errors, bulk cancellations, and requests interrupted after the drain deadline separately. The offering throughput uses completed responses within the offering window. Overall successful throughput includes the drain interval. Final-header and full-response latencies report exact p50, p95, and p99 of successful requests by class.

Go heap, goroutines, and Linux process RSS and FD peaks use 50 ms samples. TCP connection peaks come from listener connection events. Baseline values are taken after both listeners start. Cleanup values follow closing both transports' idle connections and allowing their goroutines to settle. The final heap sample still includes retained latency data and normal allocator state. Linux process CPU time covers the client, proxy, and backend together, so more than 100% indicates use of more than one core. Compare loopback runs on the same host.

The [HTTP upload buffer study](HTTP_UPLOAD.md) covers 10 MiB requests with 1 KiB responses and measures the proxy in a separate Linux process.
