//go:build !linux

package disconnect

import "net"

func tcpCloseProbe(*net.TCPConn) func() bool {
	return nil
}
