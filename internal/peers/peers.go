package peers

import (
	"encoding/binary"
	"fmt"
	"net"
)

// Peer represents a single BitTorrent peer address.
type Peer struct {
	IP   net.IP
	Port uint16
}

// Unmarshal parses a compact peer list (6 bytes per peer: 4 IP + 2 port).
func Unmarshal(buf []byte) ([]Peer, error) {
	if len(buf)%6 != 0 {
		return nil, fmt.Errorf("malformed compact peer list: length %d not divisible by 6", len(buf))
	}
	peers := make([]Peer, len(buf)/6)
	for i := range peers {
		offset := i * 6
		peers[i] = Peer{
			IP:   net.IP(buf[offset : offset+4]),
			Port: binary.BigEndian.Uint16(buf[offset+4 : offset+6]),
		}
	}
	return peers, nil
}

func (p Peer) String() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}
