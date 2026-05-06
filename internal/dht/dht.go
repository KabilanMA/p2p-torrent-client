// Package dht implements a minimal DHT bootstrap client (BEP 5).
// It queries well-known bootstrap nodes with get_peers to seed peer discovery
// for magnet links. Full Kademlia routing is out of scope.
package dht

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"time"

	bencode "github.com/jackpal/bencode-go"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
)

var bootstrapNodes = []string{
	"dht.transmissionbt.com:6881",
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.aelitis.com:6881",
}

// krpcMsg is the generic KRPC message envelope used by DHT.
type krpcMsg struct {
	T string                 `bencode:"t"`
	Y string                 `bencode:"y"`
	Q string                 `bencode:"q,omitempty"`
	A map[string]interface{} `bencode:"a,omitempty"`
	R map[string]interface{} `bencode:"r,omitempty"`
}

// GetPeers contacts DHT bootstrap nodes and returns any peer addresses found
// for the given info hash within the timeout window.
func GetPeers(infoHash [20]byte, timeout time.Duration) ([]peers.Peer, error) {
	var nodeID [20]byte
	if _, err := rand.Read(nodeID[:]); err != nil {
		return nil, fmt.Errorf("generate node ID: %w", err)
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		return nil, fmt.Errorf("DHT listen: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	// Fan out get_peers to all bootstrap nodes.
	for _, node := range bootstrapNodes {
		addr, err := net.ResolveUDPAddr("udp", node)
		if err != nil {
			continue
		}
		msg := krpcMsg{
			T: "aa",
			Y: "q",
			Q: "get_peers",
			A: map[string]interface{}{
				"id":        string(nodeID[:]),
				"info_hash": string(infoHash[:]),
			},
		}
		var buf bytes.Buffer
		if err := bencode.Marshal(&buf, msg); err != nil {
			continue
		}
		conn.WriteToUDP(buf.Bytes(), addr)
	}

	found := make(map[string]peers.Peer)
	pktBuf := make([]byte, 4096)

	for {
		n, _, err := conn.ReadFromUDP(pktBuf)
		if err != nil {
			break // deadline expired or closed
		}

		var resp krpcMsg
		if err := bencode.Unmarshal(bytes.NewReader(pktBuf[:n]), &resp); err != nil {
			continue
		}
		if resp.Y != "r" || resp.R == nil {
			continue
		}

		// "values" contains a list of compact peer strings (6 bytes each).
		valuesI, ok := resp.R["values"]
		if !ok {
			continue
		}
		values, ok := valuesI.([]interface{})
		if !ok {
			continue
		}
		for _, v := range values {
			peerStr, ok := v.(string)
			if !ok {
				continue
			}
			pList, err := peers.Unmarshal([]byte(peerStr))
			if err != nil {
				continue
			}
			for _, p := range pList {
				found[p.String()] = p
			}
		}
	}

	result := make([]peers.Peer, 0, len(found))
	for _, p := range found {
		result = append(result, p)
	}
	return result, nil
}
