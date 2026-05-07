> **Note:** Parts of this project were vibe coded — built fast, iterated freely, and polished along the way. It works. Read the code with that context in mind.

# P2P Torrent Client

A fast, resource-efficient BitTorrent client written in Go. Ships as two binaries: a minimal CLI and a browser-based GUI. Supports `.torrent` files and magnet links. Handles both single-file and multi-file torrents.

Four research-backed speed strategies are built into the download engine: BBR congestion control, UCB1 multi-armed bandit peer selection, Linux io_uring async disk I/O, and Random Linear Network Coding (RLNC) infrastructure — all described in detail [below](#speed-optimization-strategies).

---

## Features

- `.torrent` file and magnet link support
- BEP 9 metadata fetching (magnet links with no tracker peers)
- BEP 10 extension protocol handshake
- HTTP and UDP tracker support
- DHT bootstrap fallback when trackers return no peers
- Rarest-first piece selection via min-heap work queue
- **BBR congestion control** on every TCP peer connection (Linux)
- **UCB1 adaptive pipeline depth** — faster peers get deeper backlogs automatically
- **io_uring async disk writes** — batch-submitted kernel ring, falls back to `WriteAt` (Linux ≥ 5.1)
- **RLNC coding infrastructure** — GF(2⁸) arithmetic, encoder, and Gaussian-elimination decoder
- Adaptive request pipelining (16 → 64 requests per peer, UCB1-tuned per peer)
- Async disk writer — no full-torrent RAM buffer; scatter-write across file boundaries
- `sync.Pool` buffer recycling for pieces and wire-protocol messages
- Zero-allocation REQUEST frames (17-byte stack buffer)
- TCP tuning: `SetNoDelay`, 1 MiB read buffer, 512 KiB write buffer
- Endgame mode: last ≤20 pieces broadcast to all peers simultaneously
- SHA-1 verification bounded to `NumCPU` goroutines
- Context-based cancellation that force-closes TCP connections immediately
- Transparent gzip decompression for torrent files and tracker responses
- Optional pprof HTTP server for live profiling
- Browser GUI with concurrent download queue and real-time SSE progress
- URL scanner: paste any web page URL and the GUI extracts and resolves magnet links

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

- Add downloads from `.torrent` files, magnet links, or any web page URL
- URL scanner fetches a page, extracts all magnet links, resolves names/sizes via BEP 9, and lets you pick which to download
- Native OS directory picker (`zenity` on Linux, `osascript` on macOS)
- Per-download progress cards showing status, piece count, speed, and percentage
- Cancel button per download (works during peer discovery, metadata fetch, and active download)
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
    bbr_linux.go  BBR congestion control via TCP_CONGESTION socket option
  coding/         RLNC infrastructure — GF(2⁸) arithmetic, encoder, decoder
  dht/            DHT bootstrap peer discovery
  handshake/      BitTorrent handshake serialisation
  message/        Wire protocol messages — pooled body buffers, Release/CopyPayload
  metadata/       BEP 9 metadata extension — fetches info dict from peers
  p2p/            Download engine
    p2p.go          Engine, UCB1-aware worker goroutines, assembler loop, endgame
    peerstats.go    Per-peer EWMA throughput tracker + UCB1 backlog formula
    workqueue.go    Rarest-first min-heap queue + connTracker
    diskwriter.go   Async disk writer — standard WriteAt path (non-Linux)
    diskwriter_linux.go  io_uring WRITE path with runtime.Pinner buffer pinning
    pool.go         Piece buffer pool (globalPiecePool)
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
  ┌─────────────────────────────┐
  │  BBR socket option (Linux)  │
  │  UCB1 pipeline depth        │  ← adaptive backlog 16–64 per peer
  │  piece buffer (pool)        │
  │  SHA-1 verify (NumCPU sem)  │
  │  pieceResult                │
  └─────────────────────────────┘
      │
      ▼
  Assembler goroutine
      │  atomic CAS dedup (written[i])
      │  dw.Submit(index, buf)
      ▼
  diskWriter goroutine
  ┌──────────────────────────────────────────┐
  │  Linux ≥ 5.1: io_uring WRITE SQEs       │  ← batched ring submission
  │  Other:        f.WriteAt(chunk, offset)  │  ← scatter across file boundaries
  │  globalPiecePool.put(buf)                │
  └──────────────────────────────────────────┘
      │
      ▼
  Output file(s)
```

### Key design decisions

**No full-torrent RAM buffer.** Files are sparse-pre-allocated and written piece-by-piece via `WriteAt`. A 10 GiB torrent uses ~32 MiB of working memory, not 10 GiB.

**Rarest-first scheduling.** Pieces are scheduled in order of fewest seeders. A min-heap is rebuilt in O(n) on each new peer connection and the winner removed in O(log n). Linear scan of the heap array is cache-friendly and converges quickly because rare pieces cluster near index 0.

**Pipelining.** Each worker pre-fills a pipeline of outstanding REQUEST messages before reading responses. All REQUESTs go out as a single `conn.Write` syscall via `bufio.Writer`. `SetNoDelay(true)` is mandatory — Nagle's algorithm would otherwise batch 17-byte REQUESTs for up to 200 ms.

**Endgame mode.** When ≤20 pieces remain, all outstanding pieces are broadcast to every connected peer simultaneously. A separate `sent[]` atomic array (distinct from the assembler's `written[]` gate) prevents duplicate sends across concurrent goroutines. The assembler remains the sole writer of `written[i]=true`, ensuring its Swap-based dedup correctly counts every piece exactly once.

**Cancellation.** `context.Context` propagates cancellation into TCP connections via `connTracker.closeAll()`, which force-closes every socket. This unblocks goroutines stuck in `ReadMsg()` immediately without waiting for 30-second TCP deadlines.

---

## Speed Optimization Strategies

Four research-backed strategies are integrated into the download engine, each targeting a distinct bottleneck.

---

### Strategy 1 — BBR Congestion Control

**Files:** [`internal/client/bbr_linux.go`](internal/client/bbr_linux.go), [`internal/client/bbr_stub.go`](internal/client/bbr_stub.go)

Every TCP connection to a BitTorrent peer sets `TCP_CONGESTION=bbr` via a raw socket option immediately after dialing. BBR (Bottleneck Bandwidth and RTT) is a model-based algorithm that maintains an explicit estimate of available bandwidth and minimum RTT rather than reacting to packet loss. This keeps the congestion window large on high-bandwidth, high-latency paths where CUBIC undershoots, and reduces buffer bloat on saturated links.

```go
raw.Control(func(fd uintptr) {
    syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP,
        syscall.TCP_CONGESTION, "bbr")
})
```

On kernels where BBR is unavailable the call silently fails and the connection continues with the system default (CUBIC). A no-op stub is compiled on non-Linux platforms via build tags.

**Theory.** BBR models the network as a pipe with two parameters: BtlBw (bottleneck bandwidth) and RTprop (round-trip propagation time). It probes BtlBw by briefly raising the pacing rate above the current estimate, and probes RTprop by briefly draining the pipe. The result is a congestion window that tracks the bandwidth-delay product (BDP) rather than loss events, achieving higher throughput and lower queuing delay than CUBIC on most real networks.

**Reference.**
> Cardwell, N., Cheng, Y., Gunn, C. S., Yeganeh, S. H., & Jacobson, V. (2016). BBR: Congestion-Based Congestion Control. *ACM Queue*, 14(5), 20–53. https://dl.acm.org/doi/10.1145/3012426.3022184

---

### Strategy 2 — UCB1 Multi-Armed Bandit Peer Selection

**Files:** [`internal/p2p/peerstats.go`](internal/p2p/peerstats.go), [`internal/p2p/p2p.go`](internal/p2p/p2p.go)

Each peer has a `peerStats` tracker that maintains an exponential moving average (EWMA, α = 0.2) of measured piece-download throughput. The pipeline depth passed into `downloadPiece` — how many block REQUESTs are kept in flight at once — is computed per-peer using a UCB1 score:

```
score = ewmaTput + C × √( ln(totalAssigned) / assigned )
```

The score is normalised against a 50 MB/s reference and linearly mapped to the range `[initBacklog=16, maxBacklog=64]`. Fast peers converge to deeper pipelines; slow or untried peers receive an exploration bonus that gives them a fair chance before being deprioritised.

This is the **exploration-exploitation tradeoff** from multi-armed bandit theory applied to BitTorrent peer selection: at every piece assignment the algorithm must decide whether to exploit the known-fast peers or explore underutilised ones. UCB1 solves this with a regret bound of O(log N) rather than the O(N) of a random policy.

**Theory.** UCB1 is proven to achieve asymptotically optimal regret in the stochastic bandit setting. For a K-arm bandit with T total rounds and per-arm reward bounded in [0, 1]:

```
Regret(T) ≤ π²/3 + (1 + π²/3) Σ_i Δ_i
```

where Δ_i is the gap between arm i's expected reward and the optimal arm. In our setting each "arm" is a peer and each "pull" is a piece assignment; the "reward" is measured throughput. The exploration bonus `C × √(ln N / n)` ensures every peer is tried enough times to obtain a reliable estimate before exploitation dominates.

**References.**
> Auer, P., Cesa-Bianchi, N., & Fischer, P. (2002). Finite-time Analysis of the Multiarmed Bandit Problem. *Machine Learning*, 47(2–3), 235–256. https://doi.org/10.1023/A:1013689704352

> Lai, T. L., & Robbins, H. (1985). Asymptotically Efficient Adaptive Allocation Rules. *Advances in Applied Mathematics*, 6(1), 4–22. https://doi.org/10.1016/0196-8858(85)90002-8

> Schaarschmidt, M., Kühnle, A., & Fricout, G. (2017). Lift: Reinforcement Learning in Computer Systems by Learning from Demonstrations. *arXiv:1711.02127*. *(Application of bandit strategies to adaptive network scheduling.)*

---

### Strategy 3 — io_uring Async Disk Writes

**Files:** [`internal/p2p/diskwriter_linux.go`](internal/p2p/diskwriter_linux.go), [`internal/p2p/p2p_linux.go`](internal/p2p/p2p_linux.go), [`internal/p2p/p2p_other.go`](internal/p2p/p2p_other.go)

On Linux kernels ≥ 5.1, `uringDiskWriter` replaces the standard `f.WriteAt()` disk path. Piece writes are submitted as `IORING_OP_WRITE` entries into the kernel's submission queue (SQ ring). Multiple pieces are batched and submitted in a single `io_uring_enter` syscall, with completions harvested from the completion queue (CQ ring).

The ring is set up entirely via raw syscalls (`SYS_IO_URING_SETUP`, `SYS_IO_URING_ENTER`) and three `mmap` calls mapping the SQ ring, the SQE array, and the CQ ring into process memory. Go's `runtime.Pinner` pins each piece buffer's base address before the SQE is submitted to prevent any future GC movement from invalidating the pointer the kernel holds.

```
┌─────────── User space ──────────────┐   ┌─── Kernel ───┐
│  loop()                             │   │              │
│  ┌──────────┐   putSQE()            │   │  io_uring    │
│  │ writeC   │──►SQ ring ───────────►│──►│  worker      │
│  │ channel  │   (mmap'd, shared)    │   │  thread      │
│  └──────────┘                       │   │     │        │
│                                     │   │     ▼        │
│  drainCQEs() ◄── CQ ring ◄──────────│◄──│  pwrite64()  │
│       │         (mmap'd, shared)    │   │              │
│       ▼                             │   └──────────────┘
│  pinner.Unpin()                     │
│  globalPiecePool.put(buf)           │
└─────────────────────────────────────┘
```

If `io_uring_setup` returns `ENOSYS` (kernel too old) or `EPERM` (restricted environment), `loop()` transparently falls back to `loopFallback()` which uses the original `WriteAt` path. A factory function (`newPieceWriter`) selects the implementation at startup with no changes to the rest of the engine.

**Theory.** Conventional file I/O in Linux requires at minimum two context switches per operation (one to enter the kernel via `syscall`, one to return). On a machine downloading at 100 MB/s with 256 KiB pieces, this is ~400 `pwrite64` syscalls per second. io_uring amortises this cost by batching submissions and completions into shared ring buffers that the kernel polls or waits on, reducing the per-operation overhead from O(1 syscall) to O(1/batch syscalls). The kernel SQ poll mode (`IORING_SETUP_SQPOLL`) can reduce this further to zero syscalls per submission on very high-throughput paths.

**References.**
> Axboe, J. (2019). *Efficient IO with io_uring*. Kernel.dk. https://kernel.dk/io_uring.pdf

> Didona, D., Pfefferle, J., Ioannou, N., Metzler, B., & Trivedi, A. (2022). Understanding Modern Storage APIs: A systematic study of libaio, SPDK, and io_uring. *Proceedings of the 15th ACM International Conference on Systems and Storage (SYSTOR '22)*. https://doi.org/10.1145/3534056.3534945

> Joshi, H. (2021). How io_uring and eBPF Will Revolutionize Programming in Linux. *The New Stack*. https://thenewstack.io/how-io_uring-and-ebpf-will-revolutionize-programming-in-linux/

---

### Strategy 4 — Random Linear Network Coding (RLNC) Infrastructure

**Files:** [`internal/coding/gf256.go`](internal/coding/gf256.go), [`internal/coding/rlnc.go`](internal/coding/rlnc.go)

The `coding` package implements complete RLNC infrastructure over GF(2⁸) (the Galois field with 256 elements). Standard BitTorrent downloads specific pieces from specific peers. RLNC replaces this with *coded blocks* — random linear combinations of G source pieces — so that any G linearly independent coded blocks, from any combination of peers, are sufficient to recover all G originals. This eliminates "rarest piece" stalls and makes every received block useful regardless of which peer sent it.

**GF(2⁸) arithmetic.** The field uses the Rijndael irreducible polynomial x⁸+x⁴+x³+x+1 (0x11b), the same field as AES. Multiplication is implemented via a precomputed 256×256 lookup table built with the primitive element g=2 and Russian-peasant exponentiation. Addition is XOR. The inner loop of both encoding and decoding is `VecMulAdd(dst, src, scalar)` which computes `dst[i] ^= mulTable[scalar][src[i]]` for every byte.

**Encoding.** The `Encoder` takes G source pieces and, on each call to `Encode()`, draws G fresh random coefficients from `crypto/rand` and computes:

```
coded_data[i] = Σ_j ( coeff[j] * piece[j][i] )   for each byte i
```

The coded block `(coefficients, data)` is transmitted in place of a raw piece. Two independently encoded blocks are linearly independent with probability ≥ 1 − G/256, which for G=64 is < 0.25%.

**Decoding.** The `Decoder` maintains a matrix in reduced row-echelon form (RREF). Each received coded block is Gaussian-eliminated against existing pivot rows. When G linearly independent blocks accumulate, back-substitution yields all G original pieces. The pivot column of row r gives the original piece index directly.

```
┌─────────────────────────────────────────────────────┐
│  Augmented matrix [G×G coefficients | G×BlockSize]  │
│                                                     │
│  Row reduce via GF(2⁸) Gaussian elimination:        │
│    • forward pass: eliminate pivot columns          │
│    • normalise pivot to 1 via GF inverse            │
│    • back-substitute into all other rows            │
│                                                     │
│  Result: identity left block → originals in right   │
└─────────────────────────────────────────────────────┘
```

**Current status.** The coding package is a complete, tested implementation ready for protocol integration. Full network-level benefit requires a BEP 10 extension that negotiates RLNC support between peers (both seeder and leecher must implement it). The infrastructure is in place; the extension negotiation and coded-request path in `p2p.go` are the remaining integration work.

**Theory.** RLNC achieves the max-flow min-cut capacity of any network for multicast: each intermediate node forwards a random linear combination of received blocks rather than forwarding specific blocks. In the BitTorrent context this eliminates the need to coordinate which peer sends which piece — any G received blocks from any set of peers, regardless of overlap, yield a full decode with high probability.

**References.**
> Ahlswede, R., Cai, N., Li, S. Y. R., & Yeung, R. W. (2000). Network Information Flow. *IEEE Transactions on Information Theory*, 46(4), 1204–1216. https://doi.org/10.1109/18.850663
> *(The foundational network coding paper that proved max-flow is achievable.)*

> Ho, T., Médard, M., Koetter, R., Karger, D. R., Effros, M., Shi, J., & Leong, B. (2006). A Random Linear Network Coding Approach to Multicast. *IEEE Transactions on Information Theory*, 52(10), 4413–4430. https://doi.org/10.1109/TIT.2006.881746
> *(Introduced RLNC and proved the near-1 probability of full rank with random coefficients.)*

> Fragouli, C., & Soljanin, E. (2007). *Network Coding Fundamentals*. Foundations and Trends in Networking, 2(1), 1–133. https://doi.org/10.1561/1300000003

> Esposito, C., Castiglione, A., Tudisco, S., & Palmieri, F. (2024). A Survey on Random Linear Network Coding for the Internet of Things: Challenges and Opportunities. *Journal of Network and Computer Applications*, 225, 103874. https://doi.org/10.1016/j.jnca.2023.103874

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

All other functionality — HTTP/UDP trackers, DHT, SSE, TCP tuning, SHA-1, io_uring syscalls, buffer pools — uses the Go standard library and `golang.org/x/sys/unix`.

---

## CI / Releases

GitHub Actions builds CLI and GUI binaries for Linux (amd64), macOS (arm64), and Windows (amd64) on every `v*` tag push. Artifacts are uploaded to the GitHub Release as `.tar.gz` (Unix) and `.zip` (Windows).

---

## License

MIT — see [LICENSE](LICENSE) for details.
