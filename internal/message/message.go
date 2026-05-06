package message

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
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

// maxPooledBody is the largest message body recycled by bodyPool.
// Sized for one full block: 1 ID + 8 piece-header + 16384 data = 16393 bytes.
const maxPooledBody = 16393

// bodyPool recycles message body buffers for the common case of block messages.
// Larger messages (e.g. large bitfields) fall through to normal allocation.
var bodyPool = sync.Pool{New: func() any { b := make([]byte, maxPooledBody); return &b }}

// Message is a length-prefixed BitTorrent protocol message.
// When the message was read via Read(), its Payload is backed by a pooled buffer.
// Callers MUST call Release() once the payload has been fully consumed so the
// buffer can be reused. Callers that need to retain the payload past Release()
// must first call CopyPayload().
type Message struct {
	ID      MessageID
	Payload []byte  // slice into raw; valid until Release()
	raw     *[]byte // non-nil when backed by bodyPool
}

// Release returns the backing buffer to the pool.
// Safe to call on a nil *Message. Must be called exactly once per pooled message.
func (m *Message) Release() {
	if m != nil && m.raw != nil {
		bodyPool.Put(m.raw)
		m.raw = nil
	}
}

// CopyPayload returns an independent copy of Payload that outlives Release().
func (m *Message) CopyPayload() []byte {
	if m == nil {
		return nil
	}
	cp := make([]byte, len(m.Payload))
	copy(cp, m.Payload)
	return cp
}

// Serialize encodes the message to the BitTorrent wire format.
// A nil Message serializes as a keep-alive (4-byte zero length).
func (m *Message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4)
	}
	length := uint32(1 + len(m.Payload))
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// Read reads one length-prefixed message from r.
// The returned payload is valid until msg.Release() is called.
// Returns (nil, nil) for keep-alive messages, which require no Release.
func Read(r io.Reader) (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read message length: %w", err)
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return nil, nil // keep-alive
	}

	if int(length) <= maxPooledBody {
		// Fast path: borrow a pooled body buffer.
		rawPtr := bodyPool.Get().(*[]byte)
		raw := (*rawPtr)[:length]
		if _, err := io.ReadFull(r, raw); err != nil {
			bodyPool.Put(rawPtr)
			return nil, fmt.Errorf("read message body: %w", err)
		}
		return &Message{
			ID:      MessageID(raw[0]),
			Payload: raw[1:],
			raw:     rawPtr,
		}, nil
	}

	// Large message (oversized bitfield, etc.) — allocate normally; Release is no-op.
	raw := make([]byte, length)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("read message body: %w", err)
	}
	return &Message{ID: MessageID(raw[0]), Payload: raw[1:]}, nil
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
