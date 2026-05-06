package tracker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
)

const (
	udpConnectMagic = int64(0x41727101980)
	actionConnect   = uint32(0)
	actionAnnounce  = uint32(1)
)

// UDPTracker announces via the UDP tracker protocol (BEP 15).
type UDPTracker struct {
	URL string
}

func (t *UDPTracker) GetPeers(infoHash [20]byte, peerID [20]byte, port uint16, left int) ([]peers.Peer, error) {
	u, err := url.Parse(t.URL)
	if err != nil {
		return nil, fmt.Errorf("parse UDP tracker URL: %w", err)
	}

	host := u.Host
	if u.Port() == "" {
		host = host + ":80"
	}

	conn, err := net.DialTimeout("udp", host, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("UDP dial %s: %w", host, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second))

	txID := rand.Uint32()

	// Step 1: connect
	connReq := buildConnectRequest(txID)
	if _, err := conn.Write(connReq); err != nil {
		return nil, fmt.Errorf("UDP connect write: %w", err)
	}

	connResp := make([]byte, 16)
	if _, err := conn.Read(connResp); err != nil {
		return nil, fmt.Errorf("UDP connect read: %w", err)
	}

	connID, err := parseConnectResponse(connResp, txID)
	if err != nil {
		return nil, err
	}

	// Step 2: announce
	txID = rand.Uint32()
	announceReq := buildAnnounceRequest(connID, txID, infoHash, peerID, left, port)
	if _, err := conn.Write(announceReq); err != nil {
		return nil, fmt.Errorf("UDP announce write: %w", err)
	}

	// response header (20 bytes) + up to 200 peers (6 bytes each)
	announceResp := make([]byte, 20+6*200)
	n, err := conn.Read(announceResp)
	if err != nil {
		return nil, fmt.Errorf("UDP announce read: %w", err)
	}

	return parseAnnounceResponse(announceResp[:n], txID)
}

func buildConnectRequest(txID uint32) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, udpConnectMagic)
	binary.Write(buf, binary.BigEndian, actionConnect)
	binary.Write(buf, binary.BigEndian, txID)
	return buf.Bytes()
}

func parseConnectResponse(resp []byte, txID uint32) (uint64, error) {
	if len(resp) < 16 {
		return 0, fmt.Errorf("short connect response: %d bytes", len(resp))
	}
	action := binary.BigEndian.Uint32(resp[0:4])
	if action != actionConnect {
		return 0, fmt.Errorf("unexpected action in connect response: %d", action)
	}
	respTxID := binary.BigEndian.Uint32(resp[4:8])
	if respTxID != txID {
		return 0, fmt.Errorf("transaction ID mismatch in connect response")
	}
	return binary.BigEndian.Uint64(resp[8:16]), nil
}

func buildAnnounceRequest(connID uint64, txID uint32, infoHash, peerID [20]byte, left int, port uint16) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, connID)
	binary.Write(buf, binary.BigEndian, actionAnnounce)
	binary.Write(buf, binary.BigEndian, txID)
	buf.Write(infoHash[:])
	buf.Write(peerID[:])
	binary.Write(buf, binary.BigEndian, int64(0))       // downloaded
	binary.Write(buf, binary.BigEndian, int64(left))    // left
	binary.Write(buf, binary.BigEndian, int64(0))       // uploaded
	binary.Write(buf, binary.BigEndian, uint32(0))      // event: none
	binary.Write(buf, binary.BigEndian, uint32(0))      // IP: default
	binary.Write(buf, binary.BigEndian, rand.Uint32())  // key
	binary.Write(buf, binary.BigEndian, int32(-1))      // num_want: -1 = default
	binary.Write(buf, binary.BigEndian, port)
	return buf.Bytes()
}

func parseAnnounceResponse(resp []byte, txID uint32) ([]peers.Peer, error) {
	if len(resp) < 20 {
		return nil, fmt.Errorf("short announce response: %d bytes", len(resp))
	}
	action := binary.BigEndian.Uint32(resp[0:4])
	if action != actionAnnounce {
		return nil, fmt.Errorf("unexpected action in announce response: %d", action)
	}
	respTxID := binary.BigEndian.Uint32(resp[4:8])
	if respTxID != txID {
		return nil, fmt.Errorf("transaction ID mismatch in announce response")
	}
	// bytes 8-12: interval, 12-16: leechers, 16-20: seeders
	return peers.Unmarshal(resp[20:])
}
