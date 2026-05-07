package p2p

import (
	"container/heap"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/KabilanMA/p2p-torrent-client/internal/bitfield"
)

// ── pieceWork ─────────────────────────────────────────────────────────────────

// pieceWork is one unit of download work: download, verify, and deliver piece[index].
type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

// pieceResult carries a verified, pool-owned buffer back to the assembler.
// Ownership of buf transfers to the assembler on send; it is forwarded to
// diskWriter.Submit() which returns it to globalPiecePool after the write.
type pieceResult struct {
	index int
	buf   []byte
}

// ── min-heap over heapItem ────────────────────────────────────────────────────

type heapItem struct {
	pw    *pieceWork
	avail int32 // snapshot of rarity at last heap rebuild
}

type pieceHeap []*heapItem

func (h pieceHeap) Len() int            { return len(h) }
func (h pieceHeap) Less(i, j int) bool  { return h[i].avail < h[j].avail }
func (h pieceHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *pieceHeap) Push(x any)         { *h = append(*h, x.(*heapItem)) }
func (h *pieceHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// ── workQueue ─────────────────────────────────────────────────────────────────

// workQueue is a thread-safe rarest-first piece work queue backed by a min-heap.
// registerBitfield is O(n) and rebuilds the heap in place.
// next() scans the heap array linearly — O(n) worst-case but cache-friendly,
// and heap order ensures the rarest pieces appear near the start, so the scan
// converges quickly for typical connected peers with most pieces available.
//
// When sequential is true, next() returns the lowest-indexed piece the peer
// has rather than the rarest, priming the beginning of the file first for streaming.
type workQueue struct {
	mu         sync.Mutex
	h          pieceHeap
	byIndex    map[int]*heapItem
	avail      []int32 // peer availability count per piece (shadow of heap keys)
	total      int
	sequential bool // stream mode: prefer low-index pieces over rarest
}

func newWorkQueue(pieces [][20]byte, pieceLen, totalLen int) *workQueue {
	q := &workQueue{
		h:       make(pieceHeap, len(pieces)),
		byIndex: make(map[int]*heapItem, len(pieces)),
		avail:   make([]int32, len(pieces)),
		total:   len(pieces),
	}
	for i, h := range pieces {
		begin, end := pieceRange(i, pieceLen, totalLen)
		pw := &pieceWork{index: i, hash: h, length: end - begin}
		item := &heapItem{pw: pw, avail: 0}
		q.h[i] = item
		q.byIndex[i] = item
	}
	heap.Init(&q.h)
	return q
}

// registerBitfield increments availability for all pieces a peer has, then
// rebuilds the heap so rarest pieces bubble to the top. Called once per new peer.
func (q *workQueue) registerBitfield(bf bitfield.Bitfield) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.avail {
		if bf.HasPiece(i) {
			q.avail[i]++
			if item, ok := q.byIndex[i]; ok {
				item.avail = q.avail[i]
			}
		}
	}
	heap.Init(&q.h) // O(n) rebuild — correct and cheaper than n×Fix()
}

// next returns the next pending piece that peer bf has, or nil if none.
// In rarest-first mode (default) it returns the rarest piece the peer has.
// In sequential mode it returns the lowest-indexed piece the peer has so that
// the beginning of the file is ready for playback as early as possible.
func (q *workQueue) next(bf bitfield.Bitfield) *pieceWork {
	q.mu.Lock()
	defer q.mu.Unlock()

	best := -1

	if q.sequential {
		bestIndex := math.MaxInt32
		for i, item := range q.h {
			if bf.HasPiece(item.pw.index) && item.pw.index < bestIndex {
				bestIndex = item.pw.index
				best = i
			}
		}
	} else {
		var bestAvail int32 = math.MaxInt32
		for i, item := range q.h {
			if bf.HasPiece(item.pw.index) && item.avail < bestAvail {
				bestAvail = item.avail
				best = i
				if bestAvail == 0 {
					break // can't do better than exclusive ownership
				}
			}
		}
	}

	if best == -1 {
		return nil
	}

	pw := q.h[best].pw
	heap.Remove(&q.h, best) // O(log n)
	delete(q.byIndex, pw.index)
	return pw
}

// put returns a piece to the queue (called when a worker disconnects mid-piece).
func (q *workQueue) put(pw *pieceWork) {
	q.mu.Lock()
	item := &heapItem{pw: pw, avail: atomic.LoadInt32(&q.avail[pw.index])}
	heap.Push(&q.h, item)
	q.byIndex[pw.index] = item
	q.mu.Unlock()
}

// drainAll removes and returns every remaining piece (used for endgame).
func (q *workQueue) drainAll() []*pieceWork {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*pieceWork, 0, len(q.h))
	for _, item := range q.h {
		out = append(out, item.pw)
	}
	q.h = q.h[:0]
	q.byIndex = make(map[int]*heapItem)
	return out
}

// size returns the number of pending pieces.
func (q *workQueue) size() int {
	q.mu.Lock()
	n := len(q.h)
	q.mu.Unlock()
	return n
}

// ── connTracker ───────────────────────────────────────────────────────────────

// connTracker keeps a live set of open connections so they can all be
// force-closed when the download context is cancelled. This immediately
// unblocks goroutines blocked in ReadMsg() without waiting for a TCP deadline.
type connTracker struct {
	mu      sync.Mutex
	closers map[io.Closer]struct{}
}

func newConnTracker() *connTracker {
	return &connTracker{closers: make(map[io.Closer]struct{})}
}

func (ct *connTracker) add(c io.Closer) {
	ct.mu.Lock()
	ct.closers[c] = struct{}{}
	ct.mu.Unlock()
}

func (ct *connTracker) remove(c io.Closer) {
	ct.mu.Lock()
	delete(ct.closers, c)
	ct.mu.Unlock()
}

// closeAll force-closes every tracked connection. Safe to call concurrently.
func (ct *connTracker) closeAll() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	for c := range ct.closers {
		c.Close()
	}
}
