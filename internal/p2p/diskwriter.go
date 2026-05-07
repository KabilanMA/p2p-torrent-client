package p2p

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

// pieceWriter is satisfied by diskWriter (standard) and uringDiskWriter (Linux io_uring).
type pieceWriter interface {
	Submit(index int, buf []byte)
	Close() error
}

// diskWriter pre-allocates output files and accepts verified piece buffers on
// a buffered channel so disk latency never blocks network goroutines.
// The zero-copy design: each buffer is owned by the writer after Submit and
// returned to globalPiecePool once the write completes.
type diskWriter struct {
	info      *torrent.TorrentInfo
	files     []*os.File // one per logical file
	offsets   []int64    // flat byte offset where each file begins
	totalLen  int64
	pieceLen  int64
	writeC    chan writeReq
	doneC     chan error
	wg        sync.WaitGroup
}

type writeReq struct {
	index int
	buf   []byte // owned by this request; returned to pool after write
}

// newDiskWriter opens/creates and pre-allocates all output files, then starts
// the background write goroutine. output follows the Engine.Output convention:
// full file path for single-file torrents, parent directory for multi-file.
func newDiskWriter(info *torrent.TorrentInfo, output string) (*diskWriter, error) {
	dw := &diskWriter{
		info:     info,
		totalLen: int64(info.TotalLength()),
		pieceLen: int64(info.PieceLength),
		writeC:   make(chan writeReq, 256), // deep enough to absorb network bursts
		doneC:    make(chan error, 1),
	}

	if info.IsMultiFile() {
		if err := dw.openMultiFile(info, output); err != nil {
			dw.closeFiles()
			return nil, err
		}
	} else {
		f, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("open output file: %w", err)
		}
		// Pre-allocate (sparse on Linux/macOS) so random WriteAt never hits ENOSPC mid-download.
		if err := f.Truncate(int64(info.TotalLength())); err != nil {
			f.Close()
			return nil, fmt.Errorf("pre-allocate output file: %w", err)
		}
		dw.files = []*os.File{f}
		dw.offsets = []int64{0}
	}

	dw.wg.Add(1)
	go dw.loop()
	return dw, nil
}

func (dw *diskWriter) openMultiFile(info *torrent.TorrentInfo, outDir string) error {
	var offset int64
	for _, file := range info.Files {
		parts := append([]string{outDir, info.Name}, file.Path...)
		path := filepath.Join(parts...)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
		}
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		if err := f.Truncate(int64(file.Length)); err != nil {
			f.Close()
			return fmt.Errorf("pre-allocate %s: %w", path, err)
		}
		dw.files = append(dw.files, f)
		dw.offsets = append(dw.offsets, offset)
		offset += int64(file.Length)
	}
	return nil
}

// Submit enqueues a piece for writing. Ownership of buf transfers to diskWriter;
// the caller must not access buf after this call. buf is returned to
// globalPiecePool after the write completes.
func (dw *diskWriter) Submit(index int, buf []byte) {
	dw.writeC <- writeReq{index: index, buf: buf}
}

// Close signals the write goroutine to drain and stop, then closes all files.
// Returns the first write error encountered, if any.
func (dw *diskWriter) Close() error {
	close(dw.writeC)
	dw.wg.Wait()
	dw.closeFiles()
	return <-dw.doneC
}

func (dw *diskWriter) closeFiles() {
	for _, f := range dw.files {
		f.Close()
	}
}

func (dw *diskWriter) loop() {
	defer dw.wg.Done()
	var writeErr error
	for req := range dw.writeC {
		if writeErr == nil {
			if err := dw.writePiece(req.index, req.buf); err != nil {
				writeErr = err
			}
		}
		// Always return buffer to pool regardless of write errors.
		globalPiecePool.put(req.buf)
	}
	dw.doneC <- writeErr
}

// writePiece scatter-writes a piece across file boundaries.
// pieces[i] maps to flat byte range [i*pieceLen, i*pieceLen+len(data)).
func (dw *diskWriter) writePiece(index int, data []byte) error {
	flatBegin := int64(index) * dw.pieceLen
	flatEnd := flatBegin + int64(len(data))

	for i, f := range dw.files {
		fileStart := dw.offsets[i]
		var fileEnd int64
		if i+1 < len(dw.offsets) {
			fileEnd = dw.offsets[i+1]
		} else {
			fileEnd = dw.totalLen
		}

		// No overlap.
		if flatEnd <= fileStart || flatBegin >= fileEnd {
			continue
		}

		start := max64(flatBegin, fileStart)
		end := min64(flatEnd, fileEnd)
		chunk := data[start-flatBegin : end-flatBegin]
		if _, err := f.WriteAt(chunk, start-fileStart); err != nil {
			return fmt.Errorf("WriteAt %s +%d: %w", f.Name(), start-fileStart, err)
		}
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
