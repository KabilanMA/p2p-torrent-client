# 🌊 P2P Torrent Client

A lightning-fast, resource-efficient BitTorrent client written in Go. Engineered for maximum network throughput, low memory footprint, and minimized CPU contention.

---

## 📖 Part 1: User Guide

### 🚀 Features
* **Blazing Fast Downloads:** Utilizes aggressive request pipelining to saturate your network connection.
* **Low Resource Usage:** Consumes minimal RAM and CPU, making it perfect for background execution on older hardware or Raspberry Pis.
* **Smart Disk I/O:** Asynchronous disk writes prevent slow storage media from bottlenecking your download speeds.
* **Simple CLI:** No bloated GUIs. Just point to a `.torrent` file and start downloading.

### 📦 Installation

**Prerequisites:** Ensure you have Go 1.20 or higher installed.

```bash
# Clone the repository
git clone https://github.com/yourusername/p2p-torrent-client.git
cd p2p-torrent-client

# Build the executable
go build -o torrent-client cmd/torrent/main.go
```

### 💻 Usage

Running the client is as simple as providing the path to your `.torrent` file. 

```bash
./torrent-client download ubuntu-22.04-desktop-amd64.iso.torrent -o ~/Downloads/
```

**Available Flags:**
* `-o, --output <path>`: Specify the directory where the downloaded file should be saved (default: current directory).
* `-p, --port <number>`: Specify the port to listen on for incoming peer connections (default: 6881).
* `--max-peers <number>`: Set the maximum number of active peer connections (default: 50).

---

## 🛠️ Part 2: Minute Details & Developer Guide

### 🏗️ Architecture Overview

The project is structured to strictly separate concerns, ensuring that network operations, state management, and disk I/O do not block one another:

* `internal/torrent`: Manages the global state of the torrent, the piece bitfield, and coordinates the endgame mode.
* `internal/p2p`: Handles the BitTorrent wire protocol, parsing incoming messages, and managing peer state (choked/unchoked/interested).
* `internal/tracker`: Communicates with HTTP/UDP trackers to gather peer IP addresses.
* `internal/storage`: Manages asynchronous disk writes, `mmap` allocations, and file chunking.

### ⚡ Deep-Dive Optimizations

This client is aggressively optimized to circumvent common garbage collection (GC) and I/O bottlenecks found in standard Go network applications:

1. **Zero-Copy I/O & `io.Copy`:** Wire protocol messages and block transfers are routed directly using `io.Copy` and heavily optimized socket buffer streams to prevent byte slices from escaping to the heap.
2. **Buffer Pooling (`sync.Pool`):** We reuse 16KB block buffers and full-piece buffers. This almost entirely eliminates heap allocations during high-speed transfers, drastically reducing GC pauses.
3. **Lock-Free State Management:** Global piece tracking relies on fine-grained `sync.RWMutex` locks and atomic operations rather than coarse-grained mutexes, preventing CPU starvation when multiple worker goroutines finish downloading pieces simultaneously.
4. **Asynchronous Writer:** Disk writes are dispatched to a dedicated worker goroutine via buffered channels, decoupling network speeds from disk latency.

### 🤝 Contributing

We welcome contributions from performance enthusiasts! Here is how you can help:

**1. Setting up your environment:**
```bash
make setup
make test
```

**2. Profiling & Benchmarking:**
If you are submitting a PR related to performance optimization, please include standard Go benchmarks and `pprof` graphs. You can run the built-in profiling server by starting the client with the `--pprof` flag:
```bash
./torrent-client download file.torrent --pprof
# In a new terminal tab:
go tool pprof http://localhost:6060/debug/pprof/profile
```

**3. PR Guidelines:**
* Ensure `make lint` passes completely (we use `golangci-lint`).
* Avoid adding new dependencies unless absolutely necessary.
* Write unit tests for any new BitTorrent algorithms (like modifications to Rarest-First piece selection).

### 📝 License
This project is licensed under the MIT License - see the LICENSE file for details.
