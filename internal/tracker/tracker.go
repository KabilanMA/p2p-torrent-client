package tracker

import (
	"fmt"
	"strings"

	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
)

// Tracker is the interface implemented by HTTP and UDP trackers.
type Tracker interface {
	GetPeers(infoHash [20]byte, peerID [20]byte, port uint16, left int) ([]peers.Peer, error)
}

// New returns the appropriate Tracker implementation for the given announce URL.
func New(announceURL string) (Tracker, error) {
	switch {
	case strings.HasPrefix(announceURL, "http://") || strings.HasPrefix(announceURL, "https://"):
		return &HTTPTracker{URL: announceURL}, nil
	case strings.HasPrefix(announceURL, "udp://"):
		return &UDPTracker{URL: announceURL}, nil
	default:
		return nil, fmt.Errorf("unsupported tracker scheme: %q", announceURL)
	}
}
