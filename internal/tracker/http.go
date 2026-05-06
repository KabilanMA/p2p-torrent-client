package tracker

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	bencode "github.com/jackpal/bencode-go"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
)

// HTTPTracker announces to an HTTP/HTTPS tracker endpoint.
type HTTPTracker struct {
	URL string
}

type bencodeTrackerResp struct {
	FailureReason string `bencode:"failure reason"`
	Interval      int    `bencode:"interval"`
	Peers         string `bencode:"peers"` // compact format
}

func (t *HTTPTracker) GetPeers(infoHash [20]byte, peerID [20]byte, port uint16, left int) ([]peers.Peer, error) {
	base, err := url.Parse(t.URL)
	if err != nil {
		return nil, fmt.Errorf("parse tracker URL: %w", err)
	}

	params := url.Values{}
	params.Set("info_hash", string(infoHash[:]))
	params.Set("peer_id", string(peerID[:]))
	params.Set("port", strconv.Itoa(int(port)))
	params.Set("uploaded", "0")
	params.Set("downloaded", "0")
	params.Set("compact", "1")
	params.Set("left", strconv.Itoa(left))
	params.Set("event", "started")
	base.RawQuery = params.Encode()

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(base.String())
	if err != nil {
		return nil, fmt.Errorf("HTTP tracker request: %w", err)
	}
	defer resp.Body.Close()

	var tr bencodeTrackerResp
	if err := bencode.Unmarshal(resp.Body, &tr); err != nil {
		return nil, fmt.Errorf("decode tracker response: %w", err)
	}
	if tr.FailureReason != "" {
		return nil, fmt.Errorf("tracker failure: %s", tr.FailureReason)
	}

	return peers.Unmarshal([]byte(tr.Peers))
}
