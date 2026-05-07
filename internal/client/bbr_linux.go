//go:build linux

package client

import (
	"net"
	"syscall"
)

// applyBBR sets TCP_CONGESTION=bbr on the connection's socket.
// BBR (Bottleneck Bandwidth and RTT) maintains a model of the network path
// and achieves higher throughput and lower latency than CUBIC, especially
// on high-BDP links. Falls back silently if BBR is unavailable.
func applyBBR(conn net.Conn) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(func(fd uintptr) {
		syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP, syscall.TCP_CONGESTION, "bbr")
	})
}
