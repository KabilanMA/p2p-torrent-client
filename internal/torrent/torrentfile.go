package torrent

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"fmt"
	"io"
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
// Transparently decompresses gzip-wrapped torrent files (magic bytes 0x1f 0x8b).
func OpenFile(path string) (*TorrentInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open torrent file: %w", err)
	}
	defer f.Close()

	// Peek at the first two bytes to detect gzip magic (0x1f 0x8b).
	var magic [2]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return nil, fmt.Errorf("read torrent file: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek torrent file: %w", err)
	}

	var r io.Reader = f
	if magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("decompress torrent file: %w", err)
		}
		defer gz.Close()
		r = gz
	}

	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read torrent data: %w", err)
	}

	var bt bencodeTorrent
	if err := bencode.Unmarshal(bytes.NewReader(raw), &bt); err != nil {
		return nil, fmt.Errorf("parse torrent file: %w", err)
	}

	// Re-encode from map[string]interface{} to preserve all fields in the info dict
	m, err := bencode.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode torrent file: %w", err)
	}
	dict, ok := m.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("torrent is not a dictionary")
	}
	info, ok := dict["info"]
	if !ok {
		return nil, fmt.Errorf("torrent missing info dictionary")
	}
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, info); err != nil {
		return nil, fmt.Errorf("hash info dict: %w", err)
	}
	infoHash := sha1.Sum(buf.Bytes())

	return bt.toTorrentInfo(infoHash)
}

func (bt *bencodeTorrent) toTorrentInfo(infoHash [20]byte) (*TorrentInfo, error) {
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
