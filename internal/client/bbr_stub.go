//go:build !linux

package client

import "net"

func applyBBR(conn net.Conn) {}
