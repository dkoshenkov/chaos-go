package disconnect

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

func tcpCloseProbe(tcp *net.TCPConn) func() bool {
	socket, err := tcp.SyscallConn()
	if err != nil {
		return nil
	}
	return func() bool {
		var peerClosed bool
		// Control keeps the descriptor valid for the duration of the poll.
		err := socket.Control(func(fd uintptr) {
			// A peer FIN reports POLLRDHUP even with unread bytes. TCP presents
			// both Close and CloseWrite this way, just as a read sees EOF once
			// the pending bytes have been consumed.
			fds := [...]unix.PollFd{{Fd: int32(fd), Events: unix.POLLRDHUP}}
			_, err := unix.Poll(fds[:], 0)
			peerClosed = err == nil && fds[0].Revents&(unix.POLLRDHUP|unix.POLLHUP|unix.POLLERR) != 0
		})
		return peerClosed || errors.Is(err, net.ErrClosed)
	}
}
