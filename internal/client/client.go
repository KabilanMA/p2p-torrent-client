package client

import (
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
	Choked      bool
	Bitfield    bitfield.Bitfield
	Peer        peers.Peer
	InfoHash    [20]byte
	PeerID      [20]byte
	SupportsExt bool // BEP 10 extension protocol
}

// New dials peer, performs the BitTorrent handshake, and reads the initial
// bitfield. Returns an error if any step fails.
func New(peer peers.Peer, peerID, infoHash [20]byte) (*Client, error) {
	conn, err := net.DialTimeout("tcp", peer.String(), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", peer, err)
	}

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
	// Some peers send unchoke or extension handshake before bitfield; handle gracefully.
	if msg == nil {
		return nil, fmt.Errorf("expected bitfield, got keep-alive")
	}
	if msg.ID == message.MsgExtended {
		// Swallow extension handshake and try once more.
		msg, err = message.Read(conn)
		if err != nil {
			return nil, err
		}
	}
	if msg.ID != message.MsgBitfield {
		return nil, fmt.Errorf("expected bitfield, got %s", msg)
	}
	return bitfield.Bitfield(msg.Payload), nil
}

// Read reads the next message from the peer with a 30-second deadline.
func (c *Client) Read() (*message.Message, error) {
	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	return message.Read(c.Conn)
}

func (c *Client) send(msg *message.Message) error {
	c.Conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendRequest sends a REQUEST message for a block.
func (c *Client) SendRequest(index, begin, length int) error {
	return c.send(message.FormatRequest(index, begin, length))
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
