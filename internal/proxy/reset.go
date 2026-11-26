package proxy

import (
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
)

var errResponseReset = errors.New("response reset threshold reached")

func resetDownstream(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 {
		// The proxy writer records hijacks as upgrades. A reset keeps the
		// status already sent to the client, if any.
		if writer, ok := w.(*responseWriter); ok {
			w = writer.Unwrap()
		}
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			// Close below TLS so it does not send a graceful close alert.
			for {
				wrapped, ok := conn.(interface{ NetConn() net.Conn })
				if !ok {
					break
				}
				conn = wrapped.NetConn()
			}
			if socket, ok := conn.(interface{ SetLinger(int) error }); ok {
				_ = socket.SetLinger(0)
			}
			_ = conn.Close()
		}
	}
	// On HTTP/2, net/http translates this panic to a stream reset.
	panic(http.ErrAbortHandler)
}

type bodyResetWriter struct {
	http.ResponseWriter
	request   *http.Request
	limit     int64
	written   int64
	onReset   func()
	triggered bool
}

func resetAfterWriter(w http.ResponseWriter, r *http.Request, after int64, onReset func()) *bodyResetWriter {
	return &bodyResetWriter{ResponseWriter: w, request: r, limit: after, onReset: onReset}
}

func (w *bodyResetWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *bodyResetWriter) Write(p []byte) (int, error) {
	if len(p) == 0 || w.request.Method == http.MethodHead {
		return w.ResponseWriter.Write(p)
	}

	prefix := int(min(int64(len(p)), w.limit-w.written))
	if prefix > 0 {
		n, err := w.ResponseWriter.Write(p[:prefix])
		w.written += int64(n)
		if err != nil {
			return n, err
		}
		if n != prefix {
			return n, io.ErrShortWrite
		}
	}
	if w.written < w.limit {
		return prefix, nil
	}
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		return prefix, err
	}
	if prefix == len(p) {
		// A longer Content-Length proves the response should continue. For an
		// unknown length, wait for another byte or a clean EOF at the limit.
		length, err := strconv.ParseInt(w.Header().Get("Content-Length"), 10, 64)
		if err != nil || length <= w.limit {
			return prefix, nil
		}
	}

	// Returning a copy error lets ReverseProxy close the upstream body and
	// stop any pending flush before the handler's deferred abort runs.
	w.triggered = true
	return prefix, errResponseReset
}

func (w *bodyResetWriter) abort() {
	if w.triggered {
		w.onReset()
		resetDownstream(w.ResponseWriter, w.request)
	}
}
