package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/metadata"
	"github.com/KabilanMA/p2p-torrent-client/internal/p2p"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
	"github.com/KabilanMA/p2p-torrent-client/internal/tracker"
)

func main() {
	output := flag.String("o", "", "Output file (single-file) or directory (multi-file) — required")
	maxPeers := flag.Int("peers", 50, "Maximum concurrent peer connections")
	verbose := flag.Int("verbose", 0, "Verbosity level:\n"+
		"\t0  silent  — only fatal errors (default)\n"+
		"\t1  normal  — banner, peer count, progress every 5%, summary\n"+
		"\t2  verbose — per-tracker results, every piece, peer connect/disconnect\n"+
		"\t3  debug   — per-block requests, all peer messages, DHT details")
	timeout := flag.Duration("timeout", 10*time.Minute,
		"Maximum time for the entire download (0 = no limit). e.g. 30m, 1h, 2h30m")
	flag.Usage = usage
	flag.Parse()

	// Route log output based on verbosity so the standard logger
	// used for fatal errors still works at all levels.
	log.SetFlags(0)

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: missing <torrent-file|magnet-link> argument")
		usage()
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintln(os.Stderr, "error: -o output path is required")
		usage()
		os.Exit(1)
	}

	input := flag.Arg(0)

	info, err := loadTorrent(input)
	if err != nil {
		log.Fatalf("load torrent: %v", err)
	}

	// Magnet links arrive without piece hashes; fetch metadata from peers first.
	if !info.HasMetadata() {
		logv(*verbose, 1, "fetching metadata from peers (magnet link)...")
		peerID, err := newPeerID()
		if err != nil {
			log.Fatalf("generate peer ID: %v", err)
		}
		peerList, err := gatherPeers(info, peerID, *verbose)
		if err != nil {
			log.Fatalf("gather peers for metadata: %v", err)
		}
		info, err = metadata.FetchFromPeers(info, peerList, peerID)
		if err != nil {
			log.Fatalf("fetch metadata: %v", err)
		}
		logv(*verbose, 1, "metadata ok: %q  pieces: %d", info.Name, len(info.PieceHashes))
	}

	engine := &p2p.Engine{
		Info:     info,
		Output:   *output,
		MaxPeers: *maxPeers,
		Verbose:  *verbose,
		Timeout:  *timeout,
	}

	if err := engine.Download(); err != nil {
		log.Fatalf("download failed: %v", err)
	}

	fmt.Printf("Downloaded %q → %s\n", info.Name, *output)
}

// logv emits a formatted message only when the current verbosity >= required level.
func logv(current, required int, format string, args ...interface{}) {
	if current >= required {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func loadTorrent(input string) (*torrent.TorrentInfo, error) {
	if strings.HasPrefix(input, "magnet:") {
		return torrent.ParseMagnet(input)
	}
	return torrent.OpenFile(input)
}

// gatherPeers contacts all trackers in the torrent concurrently.
func gatherPeers(info *torrent.TorrentInfo, peerID [20]byte, verbose int) ([]peers.Peer, error) {
	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
		all  []peers.Peer
		wg   sync.WaitGroup
	)

	urls := trackerURLs(info)
	for _, u := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			t, err := tracker.New(url)
			if err != nil {
				return
			}
			got, err := t.GetPeers(info.InfoHash, peerID, 6881, info.TotalLength())
			if err != nil {
				logv(verbose, 2, "[tracker] %s error: %v", url, err)
				return
			}
			logv(verbose, 2, "[tracker] %s → %d peer(s)", url, len(got))
			mu.Lock()
			for _, p := range got {
				if k := p.String(); !seen[k] {
					seen[k] = true
					all = append(all, p)
				}
			}
			mu.Unlock()
		}(u)
	}
	wg.Wait()

	if len(all) == 0 {
		return nil, fmt.Errorf("no peers returned by any tracker")
	}
	return all, nil
}

func trackerURLs(info *torrent.TorrentInfo) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(u string) {
		if u != "" && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	add(info.Announce)
	for _, tier := range info.AnnounceList {
		for _, u := range tier {
			add(u)
		}
	}
	return out
}

func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-HF0001-")
	if _, err := rand.Read(id[8:]); err != nil {
		return id, fmt.Errorf("generate peer ID: %w", err)
	}
	return id, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: torrent -o <output> [options] <torrent-file|magnet-link>

Options:
`)
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `
Examples:
  torrent -o debian.iso debian-12.torrent
  torrent -o debian.iso -verbose 1 debian-12.torrent
  torrent -o ~/Downloads -peers 100 -verbose 2 "magnet:?xt=urn:btih:..."
  torrent -o out.iso -verbose 3 ubuntu.torrent           # full debug output
  torrent -o ~/Downloads -timeout 1h "magnet:?xt=urn:btih:..."  # 1-hour cap
  torrent -o ~/Downloads -timeout 0 ubuntu.torrent       # no timeout
`)
}
