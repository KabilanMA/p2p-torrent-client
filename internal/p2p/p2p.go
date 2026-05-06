// Package p2p implements the hyperfast multi-threaded BitTorrent download engine.
//
// Speed strategy:
//   - One goroutine per peer (up to MaxPeers concurrent connections).
//   - Adaptive request pipelining: starts at 16, grows to 64 on fast peers;
//     all REQUESTs in one pipeline fill are sent as a single syscall via bufio.
//   - Rarest-first piece selection via a min-heap work queue.
//   - Direct-to-disk WriteAt via diskWriter: no full-torrent RAM buffer.
//   - Buffer pools for both piece data and wire-protocol messages.
//   - Zero-allocation REQUEST frames (17-byte stack buffer in client).
//   - SetNoDelay + large socket buffers on every TCP connection.
//   - Endgame mode: last ≤20 pieces broadcast to all peers simultaneously.
//   - SHA-1 verification bounded to NumCPU goroutines via semaphore.
//
// Verbosity levels (Engine.Verbose):
//
//	0 — silent: only fatal errors are shown
//	1 — normal: start banner, peer count, periodic progress (every 5%), summary
//	2 — verbose: + per-tracker results, every piece verified, peer connect/disconnect
//	3 — debug: + per-block requests, all peer messages, bitfield registration, DHT detail
package p2p

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // registers pprof handlers on DefaultServeMux
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/client"
	"github.com/KabilanMA/p2p-torrent-client/internal/dht"
	"github.com/KabilanMA/p2p-torrent-client/internal/message"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
	"github.com/KabilanMA/p2p-torrent-client/internal/tracker"
)

const (
	maxBlockSize     = 16384      // 16 KiB per BitTorrent spec
	initBacklog      = 16         // initial pipelined requests per peer
	maxBacklog       = 64         // ceiling for adaptive backlog
	endgameThreshold = 20         // enter endgame when this many pieces remain
	listenPort       = uint16(6881)
)

// Engine orchestrates peer discovery, connection management, and downloading.
type Engine struct {
	Info     *torrent.TorrentInfo
	Output   string // file path (single-file) or parent dir (multi-file)
	MaxPeers int    // maximum concurrent peer connections (default 50)
	Verbose  int    // 0 = silent … 3 = debug

	// Context, when set, allows the caller to cancel an in-progress download.
	Context context.Context

	// LogFunc, when set, is called for every log line in addition to stderr.
	LogFunc func(line string)

	// ProgressFunc, when set, is called after each piece is verified.
	ProgressFunc func(done, total int, speedMBps float64)

	// PProfAddr, when non-empty, starts a pprof HTTP server on that address
	// (e.g. "127.0.0.1:6060") for profiling active downloads.
	PProfAddr string

	lg *log.Logger
}

// callbackWriter tees log writes to the LogFunc callback.
type callbackWriter struct {
	out      io.Writer
	callback func(string)
}

func (w *callbackWriter) Write(p []byte) (int, error) {
	n, err := w.out.Write(p)
	if w.callback != nil {
		line := strings.TrimRight(string(p), "\n")
		if line != "" {
			w.callback(line)
		}
	}
	return n, err
}

func (e *Engine) logf(level int, format string, args ...interface{}) {
	if e.Verbose >= level {
		e.lg.Printf(format, args...)
	}
}

func (e *Engine) initLogger() {
	var flags int
	switch {
	case e.Verbose >= 3:
		flags = log.Ltime | log.Lmicroseconds
	case e.Verbose == 2:
		flags = log.Ltime
	default:
		flags = 0
	}
	var base io.Writer = io.Discard
	if e.Verbose > 0 {
		base = os.Stderr
	}
	var out io.Writer = base
	if e.LogFunc != nil {
		out = &callbackWriter{out: base, callback: e.LogFunc}
	}
	e.lg = log.New(out, "", flags)
}

func (e *Engine) ctx() context.Context {
	if e.Context != nil {
		return e.Context
	}
	return context.Background()
}

// Download resolves peers, connects to them, and downloads the torrent to disk.
func (e *Engine) Download() error {
	e.initLogger()
	if e.MaxPeers == 0 {
		e.MaxPeers = 50
	}

	// Optional pprof server for profiling active downloads.
	if e.PProfAddr != "" {
		go func() {
			e.logf(1, "[pprof] http://%s/debug/pprof/", e.PProfAddr)
			http.ListenAndServe(e.PProfAddr, nil) // uses DefaultServeMux with pprof registered
		}()
	}

	peerID, err := newPeerID()
	if err != nil {
		return err
	}

	e.logf(1, "[engine] torrent: %q  pieces: %d  size: %s",
		e.Info.Name, len(e.Info.PieceHashes), humanBytes(e.Info.TotalLength()))

	peerList, err := e.collectPeers(peerID)
	if err != nil {
		return fmt.Errorf("peer collection: %w", err)
	}
	e.logf(1, "[engine] found %d unique peers across all sources", len(peerList))

	// Initialise the piece buffer pool once for this download.
	globalPiecePool = newPiecePool(e.Info.PieceLength)

	// Open output files and start the async disk-write goroutine.
	// Direct WriteAt per piece — no full-torrent RAM buffer.
	dw, err := newDiskWriter(e.Info, e.Output)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}

	queue := newWorkQueue(e.Info.PieceHashes, e.Info.PieceLength, e.Info.TotalLength())

	// results carries verified, pool-owned piece buffers to the assembler.
	// Channel depth = MaxPeers so no worker ever blocks sending a result.
	results := make(chan *pieceResult, e.MaxPeers*2)

	// written[i] is set to true the first time piece i is delivered.
	// atomic.Bool gives us a lock-free CAS for endgame dedup.
	total := len(e.Info.PieceHashes)
	written := make([]atomic.Bool, total)

	// SHA-1 semaphore: cap concurrent verification to physical CPUs.
	verifySem := make(chan struct{}, runtime.NumCPU())

	ctx := e.ctx()

	// Force-close all active connections the moment ctx is cancelled.
	ct := newConnTracker()
	go func() {
		<-ctx.Done()
		ct.closeAll()
	}()

	sem := make(chan struct{}, e.MaxPeers)
	var workersWg sync.WaitGroup

	for _, peer := range peerList {
		sem <- struct{}{}
		workersWg.Add(1)
		go func(p peers.Peer) {
			defer workersWg.Done()
			defer func() { <-sem }()
			e.runWorker(p, peerID, queue, results, ct, verifySem)
		}(peer)
	}

	// We use a separate WaitGroup for endgame workers.
	var endgameWg sync.WaitGroup

	// Start a goroutine that waits for all workers, then checks if endgame is running.
	// We use an atomic flag to safely coordinate this without races.
	var endgameStarted atomic.Bool
	go func() {
		workersWg.Wait()
		if endgameStarted.Load() {
			endgameWg.Wait()
		}
		close(results)
	}()

	// ── Assembler ──────────────────────────────────────────────────────────
	done := 0
	start := time.Now()
	lastLogPct := -1
	var assembleErr error

assemble:
	for {
		// Priority: always honour cancellation before consuming more results.
		select {
		case <-ctx.Done():
			assembleErr = ctx.Err()
			break assemble
		default:
		}

		select {
		case <-ctx.Done():
			assembleErr = ctx.Err()
			break assemble
		case res, ok := <-results:
			if !ok {
				break assemble
			}

			// Atomic CAS dedup: Swap returns the old value.
			// If it was already true, another goroutine got here first (endgame dup).
			if written[res.index].Swap(true) {
				e.logf(3, "[assemble] piece #%d duplicate (endgame) — skipped", res.index)
				globalPiecePool.put(res.buf) // return orphaned buffer
				continue
			}

			// Hand the buffer to the disk writer — async, never blocks the assembler.
			dw.Submit(res.index, res.buf)
			done++

			elapsed := time.Since(start).Seconds()
			speed := float64(done*e.Info.PieceLength) / elapsed / 1024 / 1024
			pct := int(float64(done) / float64(total) * 100)

			e.logf(2, "[piece] #%04d verified  %3d%%  %d/%d  %.2f MB/s",
				res.index, pct, done, total, speed)

			if e.Verbose == 1 && pct/5 != lastLogPct/5 {
				e.logf(1, "[progress] %3d%%  %d/%d pieces  %.2f MB/s",
					pct, done, total, speed)
				lastLogPct = pct
			}

			if e.ProgressFunc != nil {
				e.ProgressFunc(done, total, speed)
			}

			if done == total {
				break assemble
			}

			if !endgameStarted.Load() && (total-done) <= endgameThreshold && (total-done) > 0 {
				endgameStarted.Store(true)
				e.logf(2, "[engine] entering endgame mode (%d pieces remaining)", total-done)
				endgameWg.Add(1)
				go func() {
					defer endgameWg.Done()
					e.runEndgame(peerList, peerID, results, ct, written, verifySem)
				}()
			}
		}
	}

	// Drain the disk writer and close files before reporting errors.
	if writeErr := dw.Close(); writeErr != nil && assembleErr == nil {
		assembleErr = writeErr
	}

	if assembleErr != nil {
		return assembleErr
	}
	if done < total {
		return fmt.Errorf("download incomplete: %d/%d pieces", done, total)
	}

	elapsed := time.Since(start)
	e.logf(1, "[engine] complete in %s  avg %.2f MB/s",
		elapsed.Round(time.Second),
		float64(e.Info.TotalLength())/elapsed.Seconds()/1024/1024)

	return nil
}

// collectPeers queries all known trackers (and DHT as fallback) in parallel.
func (e *Engine) collectPeers(peerID [20]byte) ([]peers.Peer, error) {
	seen := make(map[string]bool)
	var mu sync.Mutex
	var all []peers.Peer

	urls := collectTrackerURLs(e.Info)
	e.logf(2, "[tracker] querying %d tracker URL(s)", len(urls))

	var wg sync.WaitGroup
	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			t, err := tracker.New(u)
			if err != nil {
				e.logf(2, "[tracker] skip %s: %v", u, err)
				return
			}
			got, err := t.GetPeers(e.Info.InfoHash, peerID, listenPort, e.Info.TotalLength())
			if err != nil {
				e.logf(2, "[tracker] %s error: %v", u, err)
				return
			}
			e.logf(2, "[tracker] %s → %d peer(s)", u, len(got))
			mu.Lock()
			for _, p := range got {
				if k := p.String(); !seen[k] {
					seen[k] = true
					all = append(all, p)
				}
			}
			mu.Unlock()
		}(url)
	}
	wg.Wait()

	// Filter out local IPs to avoid connecting to self
	localIPs := make(map[string]bool)
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok {
				localIPs[ipnet.IP.String()] = true
			}
		}
	}
	filtered := make([]peers.Peer, 0, len(all))
	for _, p := range all {
		if !localIPs[p.IP.String()] {
			filtered = append(filtered, p)
		}
	}
	all = filtered

	if len(all) == 0 {
		e.logf(1, "[engine] no tracker peers found, querying DHT bootstrap nodes...")
		e.logf(3, "[dht] bootstrap nodes: dht.transmissionbt.com:6881, router.bittorrent.com:6881, ...")
		got, _ := dht.GetPeers(e.Info.InfoHash, 10*time.Second)
		e.logf(2, "[dht] found %d peer(s) via DHT", len(got))
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
func (e *Engine) runWorker(
	peer peers.Peer, peerID [20]byte,
	queue *workQueue, results chan<- *pieceResult,
	ct *connTracker, verifySem chan struct{},
) {
	ctx := e.ctx()
	select {
	case <-ctx.Done():
		return
	default:
	}

	c, err := client.New(peer, peerID, e.Info.InfoHash)
	if err != nil {
		e.logf(2, "[peer] connect %s FAILED: %v", peer, err)
		return
	}
	ct.add(c.Conn)
	defer ct.remove(c.Conn)
	defer c.Close()

	e.logf(2, "[peer] connected %s  has %d/%d pieces  ext=%v",
		peer, c.Bitfield.Count(), queue.total, c.SupportsExt)

	queue.registerBitfield(c.Bitfield)

	if err := c.SendUnchoke(); err != nil {
		return
	}
	if err := c.SendInterested(); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pw := queue.next(c.Bitfield)
		if pw == nil {
			e.logf(2, "[peer] %s has no more compatible pieces — exiting", peer)
			return
		}

		e.logf(3, "[worker] %s claiming piece #%d (%s)", peer, pw.index, humanBytes(pw.length))

		buf, err := e.downloadPiece(c, pw)
		if err != nil {
			// buf is nil on error (downloadPiece returns the buffer to pool itself).
			e.logf(2, "[worker] %s piece #%d download error: %v — requeueing", peer, pw.index, err)
			queue.put(pw)
			return
		}

		// Cap concurrent SHA-1 verification to NumCPU to prevent CPU oversubscription.
		verifySem <- struct{}{}
		integrityErr := checkIntegrity(pw, buf)
		<-verifySem

		if integrityErr != nil {
			e.logf(2, "[worker] %s piece #%d integrity FAILED — requeueing", peer, pw.index)
			globalPiecePool.put(buf) // return buffer before exit
			queue.put(pw)
			return
		}

		e.logf(3, "[worker] %s piece #%d integrity OK  hash=%x", peer, pw.index, pw.hash[:6])
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
// On error it returns (nil, err) and has already returned the piece buffer to
// globalPiecePool so the caller never sees a leaked buffer.
func (e *Engine) downloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
	buf := globalPiecePool.get(pw.length)
	state := &pieceProgress{buf: buf, backlogMax: initBacklog}

	// Set one deadline for the entire piece download.
	// ReadMsg() inside the loop does NOT reset it — saves ~64 syscalls per piece.
	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.Conn.SetDeadline(time.Time{})

	for state.downloaded < pw.length {
		if !c.Choked {
			for state.backlog < state.backlogMax && state.requested < pw.length {
				blockSize := maxBlockSize
				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}
				e.logf(3, "[block] REQUEST piece=%d begin=%d size=%d  backlog=%d/%d",
					pw.index, state.requested, blockSize, state.backlog, state.backlogMax)
				if err := c.SendRequest(pw.index, state.requested, blockSize); err != nil {
					globalPiecePool.put(buf)
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
			// Flush all buffered REQUESTs as a single syscall.
			if err := c.FlushRequests(); err != nil {
				globalPiecePool.put(buf)
				return nil, err
			}
		}

		// ReadMsg does not reset the deadline — the piece-level deadline above
		// covers the entire loop. This saves one SetDeadline syscall per block.
		msg, err := c.ReadMsg()
		if err != nil {
			globalPiecePool.put(buf)
			return nil, err
		}
		if msg == nil {
			e.logf(3, "[msg] keep-alive from %s", c.Peer)
			continue
		}

		switch msg.ID {
		case message.MsgChoke:
			e.logf(3, "[msg] CHOKE from %s", c.Peer)
			c.Choked = true
		case message.MsgUnchoke:
			e.logf(3, "[msg] UNCHOKE from %s", c.Peer)
			c.Choked = false
		case message.MsgHave:
			if idx, err := message.ParseHave(msg); err == nil {
				e.logf(3, "[msg] HAVE piece=%d from %s", idx, c.Peer)
				c.Bitfield.SetPiece(idx)
			}
		case message.MsgPiece:
			n, err := message.ParsePiece(pw.index, state.buf, msg)
			if err != nil {
				msg.Release()
				globalPiecePool.put(buf)
				return nil, err
			}
			state.downloaded += n
			state.backlog--
			e.logf(3, "[msg] PIECE piece=%d bytes=%d  progress=%d/%d  backlog=%d  pipeline=%d",
				pw.index, n, state.downloaded, pw.length, state.backlog, state.backlogMax)
			if state.backlogMax < maxBacklog {
				state.backlogMax++
			}
		default:
			e.logf(3, "[msg] %s from %s (ignored)", msg, c.Peer)
		}
		// Return the message's pooled body buffer after every message.
		msg.Release()
	}
	return state.buf, nil
}

// runEndgame drains remaining pieces and broadcasts them to all peers.
// written is checked atomically to avoid redundant writes.
func (e *Engine) runEndgame(
	peerList []peers.Peer, peerID [20]byte,
	results chan<- *pieceResult, ct *connTracker,
	written []atomic.Bool, verifySem chan struct{},
) {
	var remaining []*pieceWork
	totalLen := e.Info.TotalLength()
	for i := 0; i < len(e.Info.PieceHashes); i++ {
		if !written[i].Load() {
			begin, end := pieceRange(i, e.Info.PieceLength, totalLen)
			remaining = append(remaining, &pieceWork{
				index:  i,
				hash:   e.Info.PieceHashes[i],
				length: end - begin,
			})
		}
	}
	if len(remaining) == 0 {
		return
	}
	e.logf(1, "[endgame] broadcasting %d piece(s) to %d peers", len(remaining), len(peerList))

	// Bound endgame concurrency to avoid a connection storm.
	endgameSem := make(chan struct{}, 4*runtime.NumCPU())

	var wg sync.WaitGroup
	for _, pw := range remaining {
		if written[pw.index].Load() {
			continue // already done before endgame started
		}
		for _, peer := range peerList {
			wg.Add(1)
			endgameSem <- struct{}{}
			go func(p peers.Peer, work *pieceWork) {
				defer wg.Done()
				defer func() { <-endgameSem }()

				// Fast-path exit if another goroutine already finished this piece.
				if written[work.index].Load() {
					return
				}

				c, err := client.New(p, peerID, e.Info.InfoHash)
				if err != nil {
					return
				}
				ct.add(c.Conn)
				defer func() { ct.remove(c.Conn); c.Close() }()

				if !c.Bitfield.HasPiece(work.index) {
					return
				}
				c.SendUnchoke()
				c.SendInterested()

				buf, err := e.downloadPiece(c, work)
				if err != nil {
					return // buf already returned to pool by downloadPiece
				}

				verifySem <- struct{}{}
				integrityErr := checkIntegrity(work, buf)
				<-verifySem

				if integrityErr != nil {
					globalPiecePool.put(buf)
					return
				}

				// Atomic CAS: only the first goroutine to finish sends the result.
				if written[work.index].Swap(true) {
					globalPiecePool.put(buf) // we lost the race
					return
				}
				e.logf(2, "[endgame] piece #%d received from %s", work.index, p)
				results <- &pieceResult{index: work.index, buf: buf}
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

// pieceRange returns the byte [begin, end) of piece i within the flat file.
func pieceRange(index, pieceLen, totalLen int) (int, int) {
	begin := index * pieceLen
	end := begin + pieceLen
	if end > totalLen {
		end = totalLen
	}
	return begin, end
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
