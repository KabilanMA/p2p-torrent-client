> **Note:** Parts of this project were vibe coded — built fast, iterated freely, and polished along the way. It works. Read the code with that context in mind.

# P2P Torrent Client

A fast, resource-efficient BitTorrent client written in Go. Ships as two binaries: a minimal CLI and a browser-based GUI. Supports `.torrent` files and magnet links. Handles both single-file and multi-file torrents.

---

## Features

- `.torrent` file and magnet link support
- BEP 9 metadata fetching (magnet links with no tracker peers)
- BEP 10 extension protocol handshake
- HTTP and UDP tracker support
- DHT bootstrap fallback when trackers return no peers
- Rarest-first piece selection via min-heap work queue
- Adaptive request pipelining (16 → 64 requests per peer)
- Async disk writer — no full-torrent RAM buffer; direct `WriteAt` per piece
- `sync.Pool` buffer recycling for pieces and wire-protocol messages
- Zero-allocation REQUEST frames (17-byte stack buffer)
- TCP tuning: `SetNoDelay`, 1 MiB read buffer, 512 KiB write buffer
- Endgame mode: last ≤20 pieces broadcast to all peers simultaneously
- SHA-1 verification bounded to `NumCPU` goroutines
- Context-based cancellation that force-closes TCP connections immediately
- Transparent gzip decompression for torrent files and tracker responses
- Optional pprof HTTP server for live profiling
- Browser GUI with concurrent download queue and real-time SSE progress

---

## Installation

**Requirements:** Go 1.22+

```bash
git clone https://github.com/KabilanMA/p2p-torrent-client.git
cd p2p-torrent-client

# CLI
go build -ldflags="-s -w" -o torrent ./cmd/torrent

# GUI
go build -ldflags="-s -w" -o torrent-gui ./cmd/gui
```

Pre-built binaries for Linux, macOS, and Windows are attached to each [GitHub Release](../../releases) and built automatically by CI on every version tag.

> **Linux GUI note:** The directory picker uses `zenity`. Install it with `sudo apt install zenity` (Debian/Ubuntu) or equivalent.

---

## CLI Usage

```
torrent -o <output> [options] <torrent-file|magnet-link>
```

| Flag | Default | Description |
|---|---|---|
| `-o` | *(required)* | Output file path (single-file torrent) or directory (multi-file torrent) |
| `-peers` | `50` | Maximum concurrent peer connections |
| `-verbose` | `0` | Verbosity level (see below) |

**Verbosity levels:**

| Level | What you see |
|---|---|
| `0` | Silent — fatal errors only |
| `1` | Banner, peer count, progress every 5%, completion summary |
| `2` | Per-tracker results, every verified piece, peer connect/disconnect |
| `3` | Per-block requests, all peer messages, DHT detail |

**Examples:**

```bash
# Download a .torrent file silently
./torrent -o debian.iso debian-12.torrent

# Magnet link with progress output
./torrent -o ~/Downloads -verbose 1 "magnet:?xt=urn:btih:..."

# 100 peers, verbose output
./torrent -o ~/Downloads -peers 100 -verbose 2 ubuntu.torrent

# Full debug output
./torrent -o out.iso -verbose 3 file.torrent
```

---

## GUI Usage

```bash
./torrent-gui
# Opens http://127.0.0.1:<random-port> in your default browser automatically
```

The GUI runs a local HTTP server and serves a single-page app. Features:

- Add downloads from `.torrent` files or magnet links
- Native OS directory picker (`zenity` on Linux, `osascript` on macOS)
- Per-download progress cards showing status, piece count, speed, and percentage
- Cancel button per download
- Real-time updates via Server-Sent Events (SSE)
- Multiple simultaneous downloads
- Reconnect-safe: new browser tabs replay the current queue state on connect
- Global log panel with per-download tagged output

---

## Architecture

```
cmd/
  torrent/        CLI entry point
  gui/            Web GUI entry point (embeds ui/index.html)
internal/
  bitfield/       Bitfield operations (HasPiece, SetPiece, Count)
  client/         TCP peer connection — handshake, buffered sends, ReadMsg
  dht/            DHT bootstrap peer discovery
  handshake/      BitTorrent handshake serialisation
  message/        Wire protocol messages — pooled body buffers, Release/CopyPayload
  metadata/       BEP 9 metadata extension — fetches info dict from peers
  p2p/            Download engine
    p2p.go        Engine, worker goroutines, assembler loop, endgame
    workqueue.go  Rarest-first min-heap queue + connTracker
    diskwriter.go Async disk writer with scatter-write across file boundaries
    pool.go       Piece buffer pool (globalPiecePool)
  peers/          Peer address parsing (compact format)
  torrent/        .torrent file parser, magnet link parser, TorrentInfo type
  tracker/        HTTP and UDP tracker clients
```

### Download pipeline

```
Trackers / DHT
      │ peers
      ▼
  runWorker (×MaxPeers goroutines)
      │  piece buffer (from globalPiecePool)
      │  SHA-1 verify (semaphore: NumCPU)
      │  pieceResult
      ▼
  Assembler goroutine
      │  atomic CAS dedup (endgame safety)
      │  dw.Submit(index, buf)
      ▼
  diskWriter goroutine
      │  f.WriteAt(chunk, offset)   ← scatter across file boundaries
      │  globalPiecePool.put(buf)
      ▼
  Output file(s)
```

### Key design decisions

**No full-torrent RAM buffer.** The original design allocated `make([]byte, totalLength)` in memory. The `diskWriter` replaces this with sparse pre-allocated files and `WriteAt` per piece. A 10 GiB torrent uses ~32 MiB of working memory, not 10 GiB.

**Rarest-first scheduling.** Pieces are scheduled in order of fewest seeders. A min-heap (`container/heap`) is rebuilt in O(n) on each new peer connection (`heap.Init`), and the winner is removed in O(log n) (`heap.Remove`). Linear scan of the heap array is cache-friendly and converges quickly because rarest pieces cluster near index 0.

**Pipelining.** Each worker pre-fills a pipeline of 16–64 outstanding REQUEST messages before reading responses. All REQUESTs in one pipeline fill go out as a single `conn.Write` syscall via `bufio.Writer`. Without `SetNoDelay(true)`, Nagle's algorithm would batch these for up to 200 ms and destroy throughput.

**Endgame mode.** When ≤20 pieces remain, all outstanding pieces are broadcast to every connected peer simultaneously. An `atomic.Bool` per piece prevents duplicate writes — the first goroutine to call `Swap(true)` wins; all others return the buffer to the pool.

**Cancellation.** `context.Context` propagates cancellation into TCP connections via `connTracker.closeAll()`, which force-closes every socket. This unblocks goroutines stuck in `ReadMsg()` immediately without waiting for 30-second TCP deadlines to fire.

---

## Performance Profiling

Set `engine.PProfAddr = "127.0.0.1:6060"` programmatically to start a pprof HTTP server alongside an active download:

```bash
# In a separate terminal while a download is running:
go tool pprof http://localhost:6060/debug/pprof/profile   # CPU
go tool pprof http://localhost:6060/debug/pprof/heap      # memory
```

---

## Dependencies

| Package | Purpose |
|---|---|
| [`github.com/jackpal/bencode-go`](https://github.com/jackpal/bencode-go) | Bencode encoding/decoding for .torrent files and tracker responses |

All other functionality — HTTP/UDP trackers, DHT, SSE, TCP tuning, SHA-1, buffer pools — uses the Go standard library.

---

## CI / Releases

GitHub Actions builds CLI and GUI binaries for Linux (amd64), macOS (arm64), and Windows (amd64) on every `v*` tag push. Artifacts are uploaded to the GitHub Release as `.tar.gz` (Unix) and `.zip` (Windows).

---

## License

MIT — see [LICENSE](LICENSE) for details.
