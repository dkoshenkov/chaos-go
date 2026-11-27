package server

import (
	"context"
	"io"
	"net"
	"sync"
	"syscall"
)

const (
	uploadBufferSize    = 128 << 10
	uploadPoolThreshold = 32 << 10
)

type uploadConn struct {
	*net.TCPConn
	writer  io.Writer
	buffers *sync.Pool
}

func pooledUploadDial(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	buffers := &sync.Pool{New: func() any { return new([uploadBufferSize]byte) }}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return conn, err
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			return &uploadConn{TCPConn: tcp, writer: struct{ io.Writer }{tcp}, buffers: buffers}, nil
		}
		return conn, nil
	}
}

func (c *uploadConn) ReadFrom(src io.Reader) (int64, error) {
	// Keep the standard TCP path for short transfers and sendfile/splice sources.
	limited, ok := src.(*io.LimitedReader)
	if !ok || limited.N < uploadPoolThreshold {
		return c.TCPConn.ReadFrom(src)
	}
	if _, ok := limited.R.(syscall.Conn); ok {
		return c.TCPConn.ReadFrom(src)
	}

	buffer := c.buffers.Get().(*[uploadBufferSize]byte)
	defer c.buffers.Put(buffer)
	// A write-only destination makes io.CopyBuffer use the supplied buffer.
	n, err := io.CopyBuffer(c.writer, limited, buffer[:])
	if err != nil && err != io.EOF {
		err = &net.OpError{Op: "readfrom", Net: c.LocalAddr().Network(), Source: c.LocalAddr(), Addr: c.RemoteAddr(), Err: err}
	}
	return n, err
}
