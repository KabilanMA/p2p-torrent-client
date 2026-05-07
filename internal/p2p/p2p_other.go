//go:build !linux

package p2p

import "github.com/KabilanMA/p2p-torrent-client/internal/torrent"

func newPieceWriter(info *torrent.TorrentInfo, output string) (pieceWriter, error) {
	return newDiskWriter(info, output)
}
