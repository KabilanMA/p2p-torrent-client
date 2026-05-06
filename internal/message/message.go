package message

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MessageID is the single-byte type identifier in the BitTorrent protocol.
type MessageID uint8

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
	MsgExtended      MessageID = 20 // BEP 10 extension protocol
)

// Message is a length-prefixed BitTorrent protocol message.
type Message struct {
	ID      MessageID
	Payload []byte
}

// Serialize encodes the message to the BitTorrent wire format.
// A nil Message serializes as a keep-alive (length 0, no ID or payload).
func (m *Message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4) // keep-alive
	}
	length := uint32(1 + len(m.Payload))
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// Read reads one length-prefixed message from r.
// Returns nil for keep-alive messages.
func Read(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, fmt.Errorf("read message length: %w", err)
	}
	length := binary.BigEndian.Uint32(lengthBuf)
	if length == 0 {
		return nil, nil // keep-alive
	}

	msgBuf := make([]byte, length)
	if _, err := io.ReadFull(r, msgBuf); err != nil {
		return nil, fmt.Errorf("read message body: %w", err)
	}
	return &Message{
		ID:      MessageID(msgBuf[0]),
		Payload: msgBuf[1:],
	}, nil
}

// FormatRequest creates a REQUEST message for a block within a piece.
func FormatRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgRequest, Payload: payload}
}

// FormatHave creates a HAVE message announcing ownership of a piece.
func FormatHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &Message{ID: MsgHave, Payload: payload}
}

// ParsePiece extracts block data from a PIECE message into buf.
func ParsePiece(index int, buf []byte, msg *Message) (int, error) {
	if msg.ID != MsgPiece {
		return 0, fmt.Errorf("expected PIECE, got %d", msg.ID)
	}
	if len(msg.Payload) < 8 {
		return 0, fmt.Errorf("PIECE payload too short: %d bytes", len(msg.Payload))
	}
	parsedIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	if parsedIndex != index {
		return 0, fmt.Errorf("PIECE index mismatch: expected %d, got %d", index, parsedIndex)
	}
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	if begin >= len(buf) {
		return 0, fmt.Errorf("PIECE begin offset %d out of range for buf len %d", begin, len(buf))
	}
	data := msg.Payload[8:]
	if begin+len(data) > len(buf) {
		return 0, fmt.Errorf("PIECE data overflows buf: begin %d + len %d > %d", begin, len(data), len(buf))
	}
	copy(buf[begin:], data)
	return len(data), nil
}

// ParseHave extracts the piece index from a HAVE message.
func ParseHave(msg *Message) (int, error) {
	if msg.ID != MsgHave {
		return 0, fmt.Errorf("expected HAVE, got %d", msg.ID)
	}
	if len(msg.Payload) < 4 {
		return 0, fmt.Errorf("HAVE payload too short")
	}
	return int(binary.BigEndian.Uint32(msg.Payload)), nil
}

// FormatExtended creates a BEP 10 extension protocol message.
// extID 0 = extension handshake; other IDs are assigned during handshake.
func FormatExtended(extID byte, payload []byte) *Message {
	buf := make([]byte, 1+len(payload))
	buf[0] = extID
	copy(buf[1:], payload)
	return &Message{ID: MsgExtended, Payload: buf}
}

func (m *Message) String() string {
	if m == nil {
		return "keep-alive"
	}
	names := map[MessageID]string{
		MsgChoke: "Choke", MsgUnchoke: "Unchoke",
		MsgInterested: "Interested", MsgNotInterested: "NotInterested",
		MsgHave: "Have", MsgBitfield: "Bitfield",
		MsgRequest: "Request", MsgPiece: "Piece",
		MsgCancel: "Cancel", MsgExtended: "Extended",
	}
	name, ok := names[m.ID]
	if !ok {
		name = fmt.Sprintf("Unknown(%d)", m.ID)
	}
	return fmt.Sprintf("%s [%d bytes]", name, len(m.Payload))
}
