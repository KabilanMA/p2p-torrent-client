package stream

import (
	"errors"
	"io"
	"os"
	"sync"
)

// PieceWaiter tracks which torrent pieces have been written to disk and allows
// readers to block until a required piece is available.
type PieceWaiter struct {
	mu       sync.Mutex
	ready    []bool
	cond     *sync.Cond
	closed   bool
	pieceLen int64
	totalLen int64
}

// NewPieceWaiter creates a waiter for a torrent with numPieces pieces.
func NewPieceWaiter(numPieces int, pieceLen, totalLen int64) *PieceWaiter {
	w := &PieceWaiter{
		ready:    make([]bool, numPieces),
		pieceLen: pieceLen,
		totalLen: totalLen,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// MarkReady marks piece index as available and wakes all blocked readers.
func (w *PieceWaiter) MarkReady(index int) {
	w.mu.Lock()
	if index >= 0 && index < len(w.ready) {
		w.ready[index] = true
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

// WaitRange blocks until all pieces covering the flat byte range
// [offset, offset+length) are available, or until the waiter is closed.
func (w *PieceWaiter) WaitRange(offset, length int64) error {
	if length <= 0 {
		return nil
	}
	startPiece := int(offset / w.pieceLen)
	endPiece := int((offset + length - 1) / w.pieceLen)
	if endPiece >= len(w.ready) {
		endPiece = len(w.ready) - 1
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.closed {
			return errors.New("stream: closed while waiting for pieces")
		}
		allReady := true
		for i := startPiece; i <= endPiece; i++ {
			if !w.ready[i] {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		w.cond.Wait()
	}
}

// WaitAll blocks until every piece is marked ready or the waiter is closed.
func (w *PieceWaiter) WaitAll() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.closed {
			return errors.New("stream: closed")
		}
		allReady := true
		for _, r := range w.ready {
			if !r {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		w.cond.Wait()
	}
}

// Close unblocks all waiting readers.
func (w *PieceWaiter) Close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cond.Broadcast()
}

// Downloaded returns the number of pieces marked ready.
func (w *PieceWaiter) Downloaded() int {
	w.mu.Lock()
	n := 0
	for _, r := range w.ready {
		if r {
			n++
		}
	}
	w.mu.Unlock()
	return n
}

// Total returns the total piece count.
func (w *PieceWaiter) Total() int { return len(w.ready) }

// FileReader wraps an *os.File and satisfies io.ReadSeeker for http.ServeContent.
// Each Read blocks until the torrent pieces covering the requested byte range
// have been verified and written to the underlying file.
//
// fileOffset is the byte offset within the flat torrent space where this file
// begins. For single-file torrents it is 0; for multi-file torrents it is the
// cumulative size of all preceding files.
type FileReader struct {
	f          *os.File
	w          *PieceWaiter
	pos        int64 // current read cursor within the file
	size       int64 // total size of this file
	fileOffset int64 // flat-torrent-byte offset of this file's first byte
}

// NewFileReader creates a FileReader. size and fileOffset must match the
// corresponding PlayableFile's Size and FlatOffset fields.
func NewFileReader(f *os.File, w *PieceWaiter, size, fileOffset int64) *FileReader {
	return &FileReader{f: f, w: w, size: size, fileOffset: fileOffset}
}

func (r *FileReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.size-r.pos {
		n = r.size - r.pos
	}
	// Block until the pieces covering this byte range are on disk.
	flatOff := r.fileOffset + r.pos
	if err := r.w.WaitRange(flatOff, n); err != nil {
		return 0, err
	}
	nn, err := r.f.ReadAt(p[:n], r.pos)
	r.pos += int64(nn)
	return nn, err
}

func (r *FileReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("stream: invalid seek whence")
	}
	if abs < 0 {
		return 0, errors.New("stream: negative seek position")
	}
	r.pos = abs
	return abs, nil
}

// nullWaiter is a PieceWaiter whose WaitRange always returns immediately.
// Used for files that are already fully available (zip-extracted files).
type nullWaiter struct{}

func (n *nullWaiter) WaitRange(_, _ int64) error { return nil }
func (n *nullWaiter) Total() int                 { return 0 }
func (n *nullWaiter) Downloaded() int            { return 0 }

// immediateReader wraps an os.File for already-available files (no waiting).
type immediateReader struct {
	f    *os.File
	pos  int64
	size int64
}

func newImmediateReader(f *os.File, size int64) *immediateReader {
	return &immediateReader{f: f, size: size}
}

func (r *immediateReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.size-r.pos {
		n = r.size - r.pos
	}
	nn, err := r.f.ReadAt(p[:n], r.pos)
	r.pos += int64(nn)
	return nn, err
}

func (r *immediateReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("stream: invalid seek whence")
	}
	if abs < 0 {
		return 0, errors.New("stream: negative seek position")
	}
	r.pos = abs
	return abs, nil
}
