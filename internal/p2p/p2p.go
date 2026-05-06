// Package p2p implements the hyperfast multi-threaded BitTorrent download engine.
//
// Speed strategy:
//   - One goroutine per peer (up to MaxPeers concurrent connections).
//   - Adaptive request pipelining: starts at 16, grows to 64 on fast peers.
//   - Rarest-first piece selection: pieces owned by fewer peers are prioritised.
//   - Endgame mode: remaining pieces are broadcast to all idle peers simultaneously.
//   - Multi-tracker support: all announce tiers are contacted in parallel.
//   - DHT fallback: used when tracker tiers return no peers.
package p2p

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/bitfield"
	"github.com/KabilanMA/p2p-torrent-client/internal/client"
	"github.com/KabilanMA/p2p-torrent-client/internal/dht"
	"github.com/KabilanMA/p2p-torrent-client/internal/message"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
	"github.com/KabilanMA/p2p-torrent-client/internal/tracker"
)

const (
	maxBlockSize     = 16384 // 16 KiB per BitTorrent spec
	initBacklog      = 16    // initial pipelined requests per peer
	maxBacklog       = 64    // ceiling for adaptive backlog
	endgameThreshold = 20    // enter endgame when this many pieces remain
	listenPort       = uint16(6881)
)

// Engine orchestrates peer discovery, connection management, and downloading.
type Engine struct {
	Info     *torrent.TorrentInfo
	Output   string // output path: file for single-file, directory for multi-file
	MaxPeers int    // maximum concurrent peer connections (default 50)
}

// pieceWork is a unit of download work passed to workers.
type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

// pieceResult carries a verified downloaded piece back to the assembler.
type pieceResult struct {
	index int
	buf   []byte
}

// workQueue is a thread-safe rarest-first piece work queue.
type workQueue struct {
	mu      sync.Mutex
	pending map[int]*pieceWork // pieces waiting to be claimed
	avail   []int32            // atomic: number of peers that have each piece
	total   int
}

func newWorkQueue(pieces [][20]byte, pieceLen, totalLen int) *workQueue {
	q := &workQueue{
		pending: make(map[int]*pieceWork, len(pieces)),
		avail:   make([]int32, len(pieces)),
		total:   len(pieces),
	}
	for i, h := range pieces {
		begin, end := pieceRange(i, pieceLen, totalLen)
		q.pending[i] = &pieceWork{
			index:  i,
			hash:   h,
			length: end - begin,
		}
	}
	return q
}

// registerBitfield increments availability counters for all pieces a peer has.
func (q *workQueue) registerBitfield(bf bitfield.Bitfield) {
	for i := range q.avail {
		if bf.HasPiece(i) {
			atomic.AddInt32(&q.avail[i], 1)
		}
	}
}

// next returns the rarest pending piece that peer bf has, or nil if none.
func (q *workQueue) next(bf bitfield.Bitfield) *pieceWork {
	q.mu.Lock()
	defer q.mu.Unlock()

	best := -1
	bestCount := int32(math.MaxInt32)

	for idx, pw := range q.pending {
		if !bf.HasPiece(pw.index) {
			continue
		}
		count := atomic.LoadInt32(&q.avail[idx])
		if count < bestCount {
			bestCount = count
			best = idx
		}
	}

	if best == -1 {
		return nil
	}
	pw := q.pending[best]
	delete(q.pending, best)
	return pw
}

// put returns a piece back to the queue (e.g. after worker disconnect).
func (q *workQueue) put(pw *pieceWork) {
	q.mu.Lock()
	q.pending[pw.index] = pw
	q.mu.Unlock()
}

// drainAll removes and returns all pending pieces (used for endgame mode).
func (q *workQueue) drainAll() []*pieceWork {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*pieceWork, 0, len(q.pending))
	for _, pw := range q.pending {
		out = append(out, pw)
	}
	q.pending = make(map[int]*pieceWork)
	return out
}

// size returns the number of pending pieces.
func (q *workQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Download resolves peers, connects to them, and downloads the torrent to disk.
func (e *Engine) Download() error {
	if e.MaxPeers == 0 {
		e.MaxPeers = 50
	}

	peerID, err := newPeerID()
	if err != nil {
		return err
	}

	log.Printf("[engine] torrent: %q  pieces: %d  size: %s",
		e.Info.Name, len(e.Info.PieceHashes), humanBytes(e.Info.TotalLength()))

	peerList, err := e.collectPeers(peerID)
	if err != nil {
		return fmt.Errorf("peer collection: %w", err)
	}
	log.Printf("[engine] found %d peers", len(peerList))

	queue := newWorkQueue(e.Info.PieceHashes, e.Info.PieceLength, e.Info.TotalLength())
	results := make(chan *pieceResult, len(e.Info.PieceHashes))

	sem := make(chan struct{}, e.MaxPeers)
	var wg sync.WaitGroup

	for _, peer := range peerList {
		sem <- struct{}{}
		wg.Add(1)
		go func(p peers.Peer) {
			defer wg.Done()
			defer func() { <-sem }()
			e.runWorker(p, peerID, queue, results)
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Assemble pieces, deduplicating endgame results.
	buf := make([]byte, e.Info.TotalLength())
	assembled := make(map[int]bool, len(e.Info.PieceHashes))
	done := 0
	start := time.Now()
	endgameTriggered := false

	for res := range results {
		if assembled[res.index] {
			continue // endgame duplicate
		}
		assembled[res.index] = true

		begin, _ := pieceRange(res.index, e.Info.PieceLength, e.Info.TotalLength())
		copy(buf[begin:], res.buf)
		done++

		elapsed := time.Since(start).Seconds()
		speed := float64(done*e.Info.PieceLength) / elapsed / 1024 / 1024
		pct := float64(done) / float64(len(e.Info.PieceHashes)) * 100
		log.Printf("[engine] %.1f%%  %d/%d pieces  %.2f MB/s",
			pct, done, len(e.Info.PieceHashes), speed)

		if !endgameTriggered && queue.size() <= endgameThreshold && queue.size() > 0 {
			endgameTriggered = true
			go e.runEndgame(queue, peerList, peerID, results)
		}
	}

	if done < len(e.Info.PieceHashes) {
		return fmt.Errorf("download incomplete: %d/%d pieces", done, len(e.Info.PieceHashes))
	}

	log.Printf("[engine] complete in %s  avg %.2f MB/s",
		time.Since(start).Round(time.Second),
		float64(e.Info.TotalLength())/time.Since(start).Seconds()/1024/1024)

	return e.writeOutput(buf)
}

// collectPeers queries all known trackers (and DHT as fallback) in parallel.
func (e *Engine) collectPeers(peerID [20]byte) ([]peers.Peer, error) {
	seen := make(map[string]bool)
	var mu sync.Mutex
	var all []peers.Peer

	urls := collectTrackerURLs(e.Info)
	var wg sync.WaitGroup

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			t, err := tracker.New(u)
			if err != nil {
				return
			}
			got, err := t.GetPeers(e.Info.InfoHash, peerID, listenPort, e.Info.TotalLength())
			if err != nil {
				return
			}
			mu.Lock()
			for _, p := range got {
				k := p.String()
				if !seen[k] {
					seen[k] = true
					all = append(all, p)
				}
			}
			mu.Unlock()
		}(url)
	}
	wg.Wait()

	if len(all) == 0 {
		log.Println("[engine] no tracker peers, querying DHT...")
		got, _ := dht.GetPeers(e.Info.InfoHash, 10*time.Second)
		for _, p := range got {
			if !seen[p.String()] {
				all = append(all, p)
			}
		}
	}

	if len(all) == 0 {
		return nil, fmt.Errorf("no peers found from trackers or DHT")
	}
	return all, nil
}

// runWorker manages the full download loop for one peer.
func (e *Engine) runWorker(peer peers.Peer, peerID [20]byte, queue *workQueue, results chan<- *pieceResult) {
	c, err := client.New(peer, peerID, e.Info.InfoHash)
	if err != nil {
		return
	}
	defer c.Close()

	queue.registerBitfield(c.Bitfield)

	if err := c.SendUnchoke(); err != nil {
		return
	}
	if err := c.SendInterested(); err != nil {
		return
	}

	for {
		pw := queue.next(c.Bitfield)
		if pw == nil {
			return
		}

		buf, err := e.downloadPiece(c, pw)
		if err != nil {
			queue.put(pw)
			return
		}

		if err := checkIntegrity(pw, buf); err != nil {
			queue.put(pw)
			return
		}

		c.SendHave(pw.index)
		results <- &pieceResult{index: pw.index, buf: buf}
	}
}

// pieceProgress tracks the in-progress state of a single piece download.
type pieceProgress struct {
	buf        []byte
	downloaded int
	requested  int
	backlog    int
	backlogMax int
}

// downloadPiece fetches all blocks of pw from peer c with adaptive pipelining.
func (e *Engine) downloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
	state := &pieceProgress{
		buf:        make([]byte, pw.length),
		backlogMax: initBacklog,
	}

	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.Conn.SetDeadline(time.Time{})

	for state.downloaded < pw.length {
		if !c.Choked {
			for state.backlog < state.backlogMax && state.requested < pw.length {
				blockSize := maxBlockSize
				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}
				if err := c.SendRequest(pw.index, state.requested, blockSize); err != nil {
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
		}

		msg, err := c.Read()
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue
		}

		switch msg.ID {
		case message.MsgChoke:
			c.Choked = true
		case message.MsgUnchoke:
			c.Choked = false
		case message.MsgHave:
			if idx, err := message.ParseHave(msg); err == nil {
				c.Bitfield.SetPiece(idx)
			}
		case message.MsgPiece:
			n, err := message.ParsePiece(pw.index, state.buf, msg)
			if err != nil {
				return nil, err
			}
			state.downloaded += n
			state.backlog--
			// Grow pipeline on each successful block.
			if state.backlogMax < maxBacklog {
				state.backlogMax++
			}
		}
	}
	return state.buf, nil
}

// runEndgame drains remaining pieces from the queue and broadcasts them to all peers.
// Results land in the shared results channel; the assembler deduplicates.
func (e *Engine) runEndgame(queue *workQueue, peerList []peers.Peer, peerID [20]byte, results chan<- *pieceResult) {
	remaining := queue.drainAll()
	if len(remaining) == 0 {
		return
	}
	log.Printf("[engine] endgame: broadcasting %d pieces to %d peers", len(remaining), len(peerList))

	var mu sync.Mutex
	dispatched := make(map[int]bool)

	var wg sync.WaitGroup
	for _, pw := range remaining {
		for _, peer := range peerList {
			wg.Add(1)
			go func(p peers.Peer, work *pieceWork) {
				defer wg.Done()

				mu.Lock()
				if dispatched[work.index] {
					mu.Unlock()
					return
				}
				mu.Unlock()

				c, err := client.New(p, peerID, e.Info.InfoHash)
				if err != nil {
					return
				}
				defer c.Close()

				if !c.Bitfield.HasPiece(work.index) {
					return
				}
				c.SendUnchoke()
				c.SendInterested()

				buf, err := e.downloadPiece(c, work)
				if err != nil {
					return
				}
				if checkIntegrity(work, buf) != nil {
					return
				}

				mu.Lock()
				if !dispatched[work.index] {
					dispatched[work.index] = true
					results <- &pieceResult{index: work.index, buf: buf}
				}
				mu.Unlock()
			}(peer, pw)
		}
	}
	wg.Wait()
}

func checkIntegrity(pw *pieceWork, buf []byte) error {
	if sha1.Sum(buf) != pw.hash {
		return fmt.Errorf("piece %d integrity check failed", pw.index)
	}
	return nil
}

// pieceRange returns the byte [begin, end) within the overall file for piece i.
func pieceRange(index, pieceLen, totalLen int) (int, int) {
	begin := index * pieceLen
	end := begin + pieceLen
	if end > totalLen {
		end = totalLen
	}
	return begin, end
}

// writeOutput writes downloaded bytes to disk, building multi-file layout when needed.
func (e *Engine) writeOutput(buf []byte) error {
	if !e.Info.IsMultiFile() {
		return os.WriteFile(e.Output, buf, 0644)
	}

	outDir := filepath.Join(e.Output, e.Info.Name)
	offset := 0
	for _, file := range e.Info.Files {
		parts := append([]string{outDir}, file.Path...)
		filePath := filepath.Join(parts...)
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return err
		}
		end := offset + file.Length
		if end > len(buf) {
			end = len(buf)
		}
		if err := os.WriteFile(filePath, buf[offset:end], 0644); err != nil {
			return err
		}
		offset += file.Length
	}
	return nil
}

func collectTrackerURLs(info *torrent.TorrentInfo) []string {
	seen := make(map[string]bool)
	var urls []string
	add := func(u string) {
		if u != "" && !seen[u] {
			seen[u] = true
			urls = append(urls, u)
		}
	}
	add(info.Announce)
	for _, tier := range info.AnnounceList {
		for _, u := range tier {
			add(u)
		}
	}
	return urls
}

func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-HF0001-")
	if _, err := rand.Read(id[8:]); err != nil {
		return id, fmt.Errorf("generate peer ID: %w", err)
	}
	return id, nil
}

func humanBytes(b int) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
