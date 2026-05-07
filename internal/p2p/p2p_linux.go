//go:build linux

package p2p

import "github.com/KabilanMA/p2p-torrent-client/internal/torrent"

// newPieceWriter returns an io_uring-backed disk writer on kernels that support
// it (≥5.1), falling back to the standard WriteAt implementation otherwise.
// The fallback is transparent: uringDiskWriter.loop() detects ENOSYS and
// switches to loopFallback automatically.
func newPieceWriter(info *torrent.TorrentInfo, output string, writtenCb func(int)) (pieceWriter, error) {
	dw, err := newUringDiskWriter(info, output, writtenCb)
	if err != nil {
		// File-open failure — not an io_uring issue, propagate.
		return nil, err
	}
	return dw, nil
}
