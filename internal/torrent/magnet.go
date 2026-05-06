package torrent

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// ParseMagnet parses a magnet URI and returns a TorrentInfo.
// PieceHashes and PieceLength will be zero until metadata is fetched via BEP 9.
func ParseMagnet(uri string) (*TorrentInfo, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet URI: %w", err)
	}
	if u.Scheme != "magnet" {
		return nil, fmt.Errorf("not a magnet URI: scheme %q", u.Scheme)
	}

	params := u.Query()

	var infoHash [20]byte
	found := false
	for _, xt := range params["xt"] {
		if !strings.HasPrefix(xt, "urn:btih:") {
			continue
		}
		hash := strings.TrimPrefix(xt, "urn:btih:")
		switch len(hash) {
		case 40: // hex
			b, err := hex.DecodeString(hash)
			if err != nil {
				return nil, fmt.Errorf("invalid info hash hex: %w", err)
			}
			copy(infoHash[:], b)
		case 32: // base32
			b, err := base32.StdEncoding.DecodeString(strings.ToUpper(hash))
			if err != nil {
				return nil, fmt.Errorf("invalid info hash base32: %w", err)
			}
			copy(infoHash[:], b)
		default:
			return nil, fmt.Errorf("info hash has unexpected length %d", len(hash))
		}
		found = true
		break
	}
	if !found {
		return nil, fmt.Errorf("magnet URI missing xt=urn:btih parameter")
	}

	info := &TorrentInfo{InfoHash: infoHash}

	if dn := params.Get("dn"); dn != "" {
		info.Name, _ = url.QueryUnescape(dn)
	}

	for _, tr := range params["tr"] {
		decoded, err := url.QueryUnescape(tr)
		if err != nil {
			decoded = tr
		}
		if info.Announce == "" {
			info.Announce = decoded
		}
		info.AnnounceList = append(info.AnnounceList, []string{decoded})
	}

	return info, nil
}
