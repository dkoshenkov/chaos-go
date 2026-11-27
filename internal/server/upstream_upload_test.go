package server

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/dkoshenkov/chaos-go/internal/config"
)

func TestUpstreamUploadReadFrom(t *testing.T) {
	listener := loopbackListener(t)
	transport := upstreamTransport(config.Upstream{DialTimeout: time.Second})
	client, err := transport.DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close upload client: %v", err)
		}
	}()
	conn, ok := client.(*uploadConn)
	if !ok {
		t.Fatalf("upstream dial returned %T, want *uploadConn", client)
	}
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("close upload server connection: %v", err)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}

	const threshold = 32 << 10
	payload := bytes.Repeat([]byte("abcd"), threshold/4)
	file, err := os.CreateTemp(t.TempDir(), "upload")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close upload file: %v", err)
		}
	}()
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	readFailure := errors.New("upload interrupted")
	for _, test := range []struct {
		name    string
		source  io.Reader
		want    []byte
		pooled  bool
		wantErr error
	}{
		{name: "at threshold", source: io.LimitReader(bytes.NewReader(payload), threshold), want: payload, pooled: true},
		{name: "below threshold", source: io.LimitReader(bytes.NewReader(payload), threshold-1), want: payload[:threshold-1]},
		{name: "file", source: io.LimitReader(file, threshold), want: payload},
		{name: "read error", source: io.LimitReader(iotest.ErrReader(readFailure), threshold), pooled: true, wantErr: readFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			usedPool := false
			conn.buffers = &sync.Pool{New: func() any {
				usedPool = true
				return new([uploadBufferSize]byte)
			}}
			n, err := conn.ReadFrom(test.source)
			if n != int64(len(test.want)) || !errors.Is(err, test.wantErr) || usedPool != test.pooled {
				t.Fatalf("ReadFrom = (%d, %v), pool used = %t; want (%d, %v), pool used = %t",
					n, err, usedPool, len(test.want), test.wantErr, test.pooled)
			}
			if test.wantErr != nil {
				var op *net.OpError
				if !errors.As(err, &op) || op.Op != "readfrom" || op.Source != conn.LocalAddr() || op.Addr != conn.RemoteAddr() {
					t.Fatalf("ReadFrom error = %#v, want readfrom with the socket addresses", err)
				}
			}
			got := make([]byte, len(test.want))
			if _, err := io.ReadFull(server, got); err != nil || !bytes.Equal(got, test.want) {
				t.Fatalf("upstream payload mismatch: %v", err)
			}
		})
	}
}
