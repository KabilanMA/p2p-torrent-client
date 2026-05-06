package torrent

// TorrentInfo holds all metadata required to download a torrent.
// It is populated from either a .torrent file or a magnet URI + metadata fetch.
type TorrentInfo struct {
	InfoHash     [20]byte
	Name         string
	PieceLength  int
	PieceHashes  [][20]byte
	Files        []File     // populated for multi-file torrents
	Length       int        // populated for single-file torrents
	Announce     string
	AnnounceList [][]string // all tracker tiers
}

// File represents one file within a multi-file torrent.
type File struct {
	Length int
	Path   []string
}

// TotalLength returns the sum of all bytes across all files (or Length for single-file).
func (t *TorrentInfo) TotalLength() int {
	if len(t.Files) > 0 {
		total := 0
		for _, f := range t.Files {
			total += f.Length
		}
		return total
	}
	return t.Length
}

// IsMultiFile reports whether this torrent contains multiple files.
func (t *TorrentInfo) IsMultiFile() bool {
	return len(t.Files) > 0
}

// HasMetadata reports whether piece hashes have been resolved.
// False for freshly parsed magnet links before metadata is fetched.
func (t *TorrentInfo) HasMetadata() bool {
	return len(t.PieceHashes) > 0
}
