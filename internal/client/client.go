package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/bitfield"
	"github.com/KabilanMA/p2p-torrent-client/internal/handshake"
	"github.com/KabilanMA/p2p-torrent-client/internal/message"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
)

// Client is an active TCP connection to a single BitTorrent peer.
type Client struct {
	Conn        net.Conn
	bw          *bufio.Writer // write buffer — flushed explicitly after pipeline fills
	Choked      bool
	Bitfield    bitfield.Bitfield
	Peer        peers.Peer
	InfoHash    [20]byte
	PeerID      [20]byte
	SupportsExt bool // BEP 10 extension protocol
}

// New dials peer, tunes the TCP socket, performs the BitTorrent handshake,
// and reads the initial bitfield.
func New(peer peers.Peer, peerID, infoHash [20]byte) (*Client, error) {
	conn, err := net.DialTimeout("tcp", peer.String(), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", peer, err)
	}

	// Tune the TCP socket before any I/O.
	// SetNoDelay is critical: without it Nagle's algorithm batches 17-byte
	// REQUEST messages for up to 200 ms, completely defeating pipelining.
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetReadBuffer(1 << 20)  // 1 MiB — prevents receiver-side stall on fast peers
		tc.SetWriteBuffer(512<<10) // 512 KiB — generous budget for REQUEST bursts
		tc.SetNoDelay(true)        // disable Nagle
	}
	// Strategy 1 — BBR: switch to BBR congestion control for higher throughput
	// and lower latency vs CUBIC on high-BDP paths (Cardwell et al. SIGCOMM 2016).
	applyBBR(conn)

	hs, err := completeHandshake(conn, infoHash, peerID)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake with %s: %w", peer, err)
	}

	bf, err := recvBitfield(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("recv bitfield from %s: %w", peer, err)
	}

	return &Client{
		Conn:        conn,
		bw:          bufio.NewWriterSize(conn, 32<<10), // 32 KiB write buffer
		Choked:      true,
		Bitfield:    bf,
		Peer:        peer,
		InfoHash:    infoHash,
		PeerID:      peerID,
		SupportsExt: hs.SupportsExt,
	}, nil
}

func completeHandshake(conn net.Conn, infoHash, peerID [20]byte) (*handshake.Handshake, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	req := handshake.New(infoHash, peerID)
	if _, err := conn.Write(req.Serialize()); err != nil {
		return nil, fmt.Errorf("send handshake: %w", err)
	}
	resp, err := handshake.Read(conn)
	if err != nil {
		return nil, fmt.Errorf("read handshake: %w", err)
	}
	if resp.InfoHash != infoHash {
		return nil, fmt.Errorf("info hash mismatch: expected %x, got %x", infoHash, resp.InfoHash)
	}
	return resp, nil
}

func recvBitfield(conn net.Conn) (bitfield.Bitfield, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	msg, err := message.Read(conn)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, fmt.Errorf("expected bitfield, got keep-alive")
	}
	if msg.ID == message.MsgExtended {
		// Swallow extension handshake and try once more.
		msg.Release()
		msg, err = message.Read(conn)
		if err != nil {
			return nil, err
		}
	}
	if msg.ID != message.MsgBitfield {
		defer msg.Release()
		return nil, fmt.Errorf("expected bitfield, got %s", msg)
	}
	// CopyPayload before Release: the Bitfield slice must outlive the pooled buffer.
	bf := bitfield.Bitfield(msg.CopyPayload())
	msg.Release()
	return bf, nil
}

// Read reads the next message with a 30-second deadline.
// Use for one-off reads outside the piece download loop.
func (c *Client) Read() (*message.Message, error) {
	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	return message.Read(c.Conn)
}

// ReadMsg reads the next message WITHOUT resetting the deadline.
// Use inside downloadPiece where a piece-level deadline is already set,
// to avoid a SetDeadline syscall on every 16 KiB block.
func (c *Client) ReadMsg() (*message.Message, error) {
	return message.Read(c.Conn)
}

// send serialises msg, writes it into the write buffer, and flushes immediately.
// Use for low-frequency control messages (Unchoke, Interested, Have, etc.).
func (c *Client) send(msg *message.Message) error {
	c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.bw.Write(msg.Serialize()); err != nil {
		return err
	}
	return c.bw.Flush()
}

// SendRequest writes a zero-allocation REQUEST frame into the write buffer
// WITHOUT flushing. Call FlushRequests() after filling the pipeline so all
// REQUESTs go out as a single syscall.
func (c *Client) SendRequest(index, begin, length int) error {
	c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	// All 17 bytes live on the stack — no heap allocation.
	var frame [17]byte
	binary.BigEndian.PutUint32(frame[0:4], 13) // length = 1 ID + 12 payload
	frame[4] = byte(message.MsgRequest)
	binary.BigEndian.PutUint32(frame[5:9], uint32(index))
	binary.BigEndian.PutUint32(frame[9:13], uint32(begin))
	binary.BigEndian.PutUint32(frame[13:17], uint32(length))
	_, err := c.bw.Write(frame[:])
	return err
}

// FlushRequests flushes all buffered REQUEST frames to the peer in one syscall.
// At pipeline depth 64, this collapses 64 syscalls into 1.
func (c *Client) FlushRequests() error {
	return c.bw.Flush()
}

// SendInterested sends an INTERESTED message.
func (c *Client) SendInterested() error {
	return c.send(&message.Message{ID: message.MsgInterested})
}

// SendNotInterested sends a NOT_INTERESTED message.
func (c *Client) SendNotInterested() error {
	return c.send(&message.Message{ID: message.MsgNotInterested})
}

// SendUnchoke sends an UNCHOKE message.
func (c *Client) SendUnchoke() error {
	return c.send(&message.Message{ID: message.MsgUnchoke})
}

// SendHave announces that we have downloaded a piece.
func (c *Client) SendHave(index int) error {
	return c.send(message.FormatHave(index))
}

// SendExtended sends a BEP 10 extension message.
func (c *Client) SendExtended(extID byte, payload []byte) error {
	return c.send(message.FormatExtended(extID, payload))
}

// Close closes the underlying TCP connection.
func (c *Client) Close() {
	c.Conn.Close()
}
