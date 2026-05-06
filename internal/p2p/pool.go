package p2p

import "sync"

// piecePool recycles piece-sized byte slices to eliminate per-piece allocations
// and reduce GC pressure during high-speed downloads.
//
// All pieces in a torrent share the same nominal length (except the last piece
// which is shorter). The pool is keyed to the maximum piece length so every
// borrowed slice has sufficient capacity; callers receive a slice resliced to
// the exact piece length they need.
type piecePool struct {
	p       sync.Pool
	maxSize int
}

func newPiecePool(maxPieceSize int) *piecePool {
	pl := &piecePool{maxSize: maxPieceSize}
	pl.p = sync.Pool{
		New: func() any {
			b := make([]byte, maxPieceSize)
			return &b
		},
	}
	return pl
}

// get borrows a buffer of exactly `length` bytes from the pool.
// The returned slice shares its backing array with the pool; callers must
// not retain it after calling put().
func (pl *piecePool) get(length int) []byte {
	ptr := pl.p.Get().(*[]byte)
	return (*ptr)[:length]
}

// put returns a buffer to the pool. The slice's capacity must equal pl.maxSize
// (i.e. it must have been obtained from get() and not resliced further).
func (pl *piecePool) put(b []byte) {
	b = b[:cap(b)] // restore full capacity before pooling
	pl.p.Put(&b)
}

// globalPiecePool is initialised once at the start of Download() and shared
// across all worker goroutines for the lifetime of that download.
var globalPiecePool *piecePool
