// Package metadata implements BEP 9 (ut_metadata) over BEP 10 (extension protocol)
// to fetch torrent metadata from peers when only a magnet link is available.
package metadata

import (
	"bytes"
	"fmt"
	"net"
	"time"

	bencode "github.com/jackpal/bencode-go"
	"github.com/KabilanMA/p2p-torrent-client/internal/handshake"
	"github.com/KabilanMA/p2p-torrent-client/internal/message"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

const (
	metadataPieceSize = 16384 // 16 KiB per BEP 9
	localUtMetadataID = byte(1)
)

// extHandshake is the BEP 10 extension handshake payload.
type extHandshake struct {
	M            map[string]int `bencode:"m"`
	MetadataSize int            `bencode:"metadata_size,omitempty"`
	V            string         `bencode:"v,omitempty"`
}

// utMetadataMsg is a BEP 9 ut_metadata message header.
type utMetadataMsg struct {
	MsgType   int `bencode:"msg_type"`
	Piece     int `bencode:"piece"`
	TotalSize int `bencode:"total_size,omitempty"`
}

// FetchFromPeers tries each peer in turn until metadata is retrieved.
// It returns a fully populated TorrentInfo or an error if all peers fail.
func FetchFromPeers(base *torrent.TorrentInfo, peerList []peers.Peer, peerID [20]byte) (*torrent.TorrentInfo, error) {
	for _, peer := range peerList {
		info, err := fetchFrom(base, peer, peerID)
		if err == nil {
			return info, nil
		}
	}
	return nil, fmt.Errorf("could not fetch metadata from any of %d peers", len(peerList))
}

func fetchFrom(base *torrent.TorrentInfo, peer peers.Peer, peerID [20]byte) (*torrent.TorrentInfo, error) {
	conn, err := net.DialTimeout("tcp", peer.String(), 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// BitTorrent handshake
	hs := handshake.New(base.InfoHash, peerID)
	if _, err := conn.Write(hs.Serialize()); err != nil {
		return nil, err
	}
	resp, err := handshake.Read(conn)
	if err != nil {
		return nil, err
	}
	if resp.InfoHash != base.InfoHash {
		return nil, fmt.Errorf("info hash mismatch")
	}
	if !resp.SupportsExt {
		return nil, fmt.Errorf("peer does not support extension protocol")
	}

	// Send extension handshake (ext ID 0).
	ourHandshake := extHandshake{
		M: map[string]int{"ut_metadata": int(localUtMetadataID)},
		V: "hyperfast/1.0",
	}
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, ourHandshake); err != nil {
		return nil, err
	}
	extMsg := message.FormatExtended(0, buf.Bytes())
	if _, err := conn.Write(extMsg.Serialize()); err != nil {
		return nil, err
	}

	// Read messages until we get the peer's extension handshake.
	peerUtMetadataID, metadataSize, err := readExtHandshake(conn)
	if err != nil {
		return nil, err
	}
	if peerUtMetadataID == 0 {
		return nil, fmt.Errorf("peer does not support ut_metadata")
	}

	// Request metadata pieces.
	numPieces := (metadataSize + metadataPieceSize - 1) / metadataPieceSize
	rawMetadata := make([]byte, metadataSize)
	received := make([]bool, numPieces)

	for i := 0; i < numPieces; i++ {
		req := utMetadataMsg{MsgType: 0, Piece: i}
		var rb bytes.Buffer
		bencode.Marshal(&rb, req)
		extMsg := message.FormatExtended(peerUtMetadataID, rb.Bytes())
		if _, err := conn.Write(extMsg.Serialize()); err != nil {
			return nil, err
		}
	}

	// Collect data responses.
	for {
		if allReceived(received) {
			break
		}
		msg, err := message.Read(conn)
		if err != nil {
			return nil, err
		}
		if msg == nil || msg.ID != message.MsgExtended {
			continue
		}
		if len(msg.Payload) < 2 {
			continue
		}
		if msg.Payload[0] != localUtMetadataID {
			continue
		}

		// Find the end of the bencode dict header to locate raw data.
		headerEnd, header, err := parseUtMetadataHeader(msg.Payload[1:])
		if err != nil {
			continue
		}
		if header.MsgType != 1 { // data = 1, reject = 2
			continue
		}

		pieceIdx := header.Piece
		if pieceIdx < 0 || pieceIdx >= numPieces {
			continue
		}

		data := msg.Payload[1+headerEnd:]
		start := pieceIdx * metadataPieceSize
		end := start + len(data)
		if end > metadataSize {
			end = metadataSize
		}
		copy(rawMetadata[start:end], data)
		received[pieceIdx] = true
	}

	return torrent.ParseInfoDict(rawMetadata, base.InfoHash)
}

func readExtHandshake(conn net.Conn) (peerExtID byte, metadataSize int, err error) {
	for {
		msg, err := message.Read(conn)
		if err != nil {
			return 0, 0, err
		}
		if msg == nil {
			continue
		}
		if msg.ID != message.MsgExtended {
			continue
		}
		if len(msg.Payload) < 2 {
			continue
		}
		if msg.Payload[0] != 0 { // ext ID 0 = handshake
			continue
		}

		var eh extHandshake
		if err := bencode.Unmarshal(bytes.NewReader(msg.Payload[1:]), &eh); err != nil {
			return 0, 0, fmt.Errorf("decode ext handshake: %w", err)
		}

		id, ok := eh.M["ut_metadata"]
		if !ok {
			return 0, 0, fmt.Errorf("peer did not advertise ut_metadata")
		}
		return byte(id), eh.MetadataSize, nil
	}
}

func parseUtMetadataHeader(payload []byte) (int, utMetadataMsg, error) {
	// bencode dicts end at 'e'; scan for the closing 'e' by decoding.
	// Use a decoder that tracks position via a wrapper.
	tw := &trackingReader{data: payload}
	var msg utMetadataMsg
	if err := bencode.Unmarshal(tw, &msg); err != nil {
		return 0, utMetadataMsg{}, err
	}
	return tw.pos, msg, nil
}

type trackingReader struct {
	data []byte
	pos  int
}

func (t *trackingReader) Read(p []byte) (int, error) {
	if t.pos >= len(t.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, t.data[t.pos:])
	t.pos += n
	return n, nil
}

func allReceived(received []bool) bool {
	for _, r := range received {
		if !r {
			return false
		}
	}
	return true
}
