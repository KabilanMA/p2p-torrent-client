package handshake

import (
	"fmt"
	"io"
)

const (
	pstr = "BitTorrent protocol"
	// reserved byte 5 (0-indexed), bit 4 set: extension protocol support (BEP 10).
	extReservedByte  = 5
	extReservedBit   = 0x10
)

// Handshake is the first message exchanged between peers.
type Handshake struct {
	Pstr         string
	Reserved     [8]byte
	InfoHash     [20]byte
	PeerID       [20]byte
	SupportsExt  bool // peer advertises BEP 10 extension protocol
}

// New builds a handshake for the given torrent, advertising extension protocol support.
func New(infoHash, peerID [20]byte) *Handshake {
	h := &Handshake{
		Pstr:        pstr,
		InfoHash:    infoHash,
		PeerID:      peerID,
		SupportsExt: true,
	}
	h.Reserved[extReservedByte] |= extReservedBit
	return h
}

// Serialize encodes the handshake into the BitTorrent wire format.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 0, 1+len(h.Pstr)+8+20+20)
	buf = append(buf, byte(len(h.Pstr)))
	buf = append(buf, []byte(h.Pstr)...)
	buf = append(buf, h.Reserved[:]...)
	buf = append(buf, h.InfoHash[:]...)
	buf = append(buf, h.PeerID[:]...)
	return buf
}

// Read parses a handshake from the stream.
func Read(r io.Reader) (*Handshake, error) {
	lengthBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, fmt.Errorf("read pstr length: %w", err)
	}
	pstrLen := int(lengthBuf[0])
	if pstrLen == 0 {
		return nil, fmt.Errorf("pstr length is zero")
	}

	rest := make([]byte, pstrLen+48)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, fmt.Errorf("read handshake body: %w", err)
	}

	var reserved [8]byte
	copy(reserved[:], rest[pstrLen:pstrLen+8])

	var infoHash, peerID [20]byte
	copy(infoHash[:], rest[pstrLen+8:pstrLen+28])
	copy(peerID[:], rest[pstrLen+28:pstrLen+48])

	h := &Handshake{
		Pstr:     string(rest[:pstrLen]),
		Reserved: reserved,
		InfoHash: infoHash,
		PeerID:   peerID,
	}
	h.SupportsExt = (reserved[extReservedByte] & extReservedBit) != 0
	return h, nil
}
