package torrent

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"

	bencode "github.com/jackpal/bencode-go"
)

type bencodeFile struct {
	Length int      `bencode:"length"`
	Path   []string `bencode:"path"`
}

type bencodeInfo struct {
	Pieces      string        `bencode:"pieces"`
	PieceLength int           `bencode:"piece length"`
	Length      int           `bencode:"length"`
	Name        string        `bencode:"name"`
	Files       []bencodeFile `bencode:"files"`
}

type bencodeTorrent struct {
	Announce     string       `bencode:"announce"`
	AnnounceList [][]string   `bencode:"announce-list"`
	Info         bencodeInfo  `bencode:"info"`
	Comment      string       `bencode:"comment"`
}

// OpenFile parses a .torrent file and returns a TorrentInfo.
func OpenFile(path string) (*TorrentInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open torrent file: %w", err)
	}
	defer f.Close()

	var bt bencodeTorrent
	if err := bencode.Unmarshal(f, &bt); err != nil {
		return nil, fmt.Errorf("parse torrent file: %w", err)
	}
	return bt.toTorrentInfo()
}

func (bt *bencodeTorrent) toTorrentInfo() (*TorrentInfo, error) {
	infoHash, err := bt.infoHash()
	if err != nil {
		return nil, err
	}
	pieceHashes, err := splitPieceHashes(bt.Info.Pieces)
	if err != nil {
		return nil, err
	}

	info := &TorrentInfo{
		InfoHash:     infoHash,
		Name:         bt.Info.Name,
		PieceLength:  bt.Info.PieceLength,
		PieceHashes:  pieceHashes,
		Announce:     bt.Announce,
		AnnounceList: bt.AnnounceList,
	}

	if len(bt.Info.Files) > 0 {
		for _, f := range bt.Info.Files {
			info.Files = append(info.Files, File{
				Length: f.Length,
				Path:   f.Path,
			})
		}
	} else {
		info.Length = bt.Info.Length
	}

	return info, nil
}

func (bt *bencodeTorrent) infoHash() ([20]byte, error) {
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, bt.Info); err != nil {
		return [20]byte{}, fmt.Errorf("hash info dict: %w", err)
	}
	return sha1.Sum(buf.Bytes()), nil
}

func splitPieceHashes(pieces string) ([][20]byte, error) {
	buf := []byte(pieces)
	if len(buf)%20 != 0 {
		return nil, fmt.Errorf("malformed pieces string: length %d not divisible by 20", len(buf))
	}
	hashes := make([][20]byte, len(buf)/20)
	for i := range hashes {
		copy(hashes[i][:], buf[i*20:(i+1)*20])
	}
	return hashes, nil
}

// ParseInfoDict parses a raw bencoded info dictionary (used after BEP 9 metadata fetch).
func ParseInfoDict(raw []byte, expectedHash [20]byte) (*TorrentInfo, error) {
	if sha1.Sum(raw) != expectedHash {
		return nil, fmt.Errorf("metadata hash mismatch")
	}

	var info bencodeInfo
	if err := bencode.Unmarshal(bytes.NewReader(raw), &info); err != nil {
		return nil, fmt.Errorf("decode info dict: %w", err)
	}

	pieceHashes, err := splitPieceHashes(info.Pieces)
	if err != nil {
		return nil, err
	}

	ti := &TorrentInfo{
		InfoHash:    expectedHash,
		Name:        info.Name,
		PieceLength: info.PieceLength,
		PieceHashes: pieceHashes,
	}

	if len(info.Files) > 0 {
		for _, f := range info.Files {
			ti.Files = append(ti.Files, File{
				Length: f.Length,
				Path:   f.Path,
			})
		}
	} else {
		ti.Length = info.Length
	}

	return ti, nil
}
