// Package main is the web-based GUI for the torrent client.
// It starts a local HTTP server and opens the default browser automatically.
// No CGO, no system GUI libraries required.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/KabilanMA/p2p-torrent-client/internal/metadata"
	"github.com/KabilanMA/p2p-torrent-client/internal/p2p"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
	"github.com/KabilanMA/p2p-torrent-client/internal/tracker"
)

//go:embed ui/index.html
var uiFS embed.FS

// sseClient is a channel through which a single SSE connection receives messages.
type sseClient chan string

// hub fans out SSE messages to all connected browser tabs.
type hub struct {
	mu      sync.Mutex
	clients map[sseClient]struct{}
}

func newHub() *hub { return &hub{clients: make(map[sseClient]struct{})} }

func (h *hub) add(c sseClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(c sseClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *hub) send(event string, payload any) {
	data, _ := json.Marshal(payload)
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c <- msg:
		default:
		}
	}
}

// dlEntry holds live state for one enqueued download.
type dlEntry struct {
	mu         sync.Mutex
	id         string
	name       string
	sourceType string
	outputDir  string
	status     string // "running"|"done"|"error"|"cancelled"
	done       int
	total      int
	speed      float64
	errMsg     string
	cancel     context.CancelFunc
}

// dlSnapshot is the JSON-serialisable view of a dlEntry.
type dlSnapshot struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	SourceType string  `json:"source_type"`
	OutputDir  string  `json:"output_dir"`
	Status     string  `json:"status"`
	Done       int     `json:"done"`
	Total      int     `json:"total"`
	Pct        float64 `json:"pct"`
	Speed      float64 `json:"speed"`
	ErrMsg     string  `json:"error,omitempty"`
}

func (d *dlEntry) snapshot() dlSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	pct := 0.0
	if d.total > 0 {
		pct = float64(d.done) / float64(d.total) * 100
	}
	return dlSnapshot{
		ID:         d.id,
		Name:       d.name,
		SourceType: d.sourceType,
		OutputDir:  d.outputDir,
		Status:     d.status,
		Done:       d.done,
		Total:      d.total,
		Pct:        pct,
		Speed:      d.speed,
		ErrMsg:     d.errMsg,
	}
}

// server holds all state shared across HTTP handlers.
type server struct {
	hub       *hub
	mu        sync.Mutex
	downloads map[string]*dlEntry
}

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := &server{
		hub:       newHub(),
		downloads: make(map[string]*dlEntry),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/",                 srv.serveIndex)
	mux.HandleFunc("/api/browse-dir",   srv.handleBrowseDir)
	mux.HandleFunc("/api/clean-magnet", srv.handleCleanMagnet)
	mux.HandleFunc("/api/download",     srv.handleDownload)
	mux.HandleFunc("/api/downloads",    srv.handleDownloads)
	mux.HandleFunc("/api/cancel",       srv.handleCancel)
	mux.HandleFunc("/api/events",       srv.handleEvents)

	addr := fmt.Sprintf("http://%s", ln.Addr())
	fmt.Printf("Torrent GUI → %s\n", addr)
	openBrowser(addr)

	log.Fatal(http.Serve(ln, mux))
}

func (s *server) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, _ := uiFS.ReadFile("ui/index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

// handleBrowseDir opens the OS native folder picker and returns the chosen path.
// Returns 204 if the user cancels; 501 if the picker is unavailable.
func (s *server) handleBrowseDir(w http.ResponseWriter, r *http.Request) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("zenity", "--file-selection", "--directory",
			"--title=Select Output Directory")
	case "darwin":
		cmd = exec.Command("osascript", "-e",
			"set f to choose folder with prompt \"Select Output Directory\"\nreturn POSIX path of f")
	default:
		http.Error(w, "directory picker not supported on this platform", http.StatusNotImplemented)
		return
	}
	out, err := cmd.Output()
	if err != nil {
		// User cancelled or tool not available — return 204 so the browser does nothing.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	path := filepath.Clean(strings.TrimSpace(string(out)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"path": path})
}

func (s *server) handleCleanMagnet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw := strings.TrimSpace(string(body))
	cleaned, err := torrent.ExtractMagnet(raw)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"ok":      true,
		"magnet":  cleaned,
		"changed": cleaned != raw,
	})
}

// handleDownloads returns the current snapshot of every tracked download.
func (s *server) handleDownloads(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	snaps := make([]dlSnapshot, 0, len(s.downloads))
	for _, dl := range s.downloads {
		snaps = append(snaps, dl.snapshot())
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snaps)
}

// handleDownload enqueues a new download, starts it in the background, and
// immediately returns 202 Accepted with {"id":"…"} for the browser to track.
func (s *server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}

	sourceType  := r.FormValue("source_type")
	outputDir   := strings.TrimSpace(r.FormValue("output_dir"))
	maxPeers, _ := strconv.Atoi(r.FormValue("max_peers"))
	verbose, _  := strconv.Atoi(r.FormValue("verbose"))
	if maxPeers < 1 {
		maxPeers = 50
	}
	if outputDir == "" {
		http.Error(w, "output_dir is required", http.StatusBadRequest)
		return
	}

	id := newID()
	dl := &dlEntry{
		id:         id,
		sourceType: sourceType,
		outputDir:  outputDir,
		status:     "running",
	}

	var source string
	switch sourceType {
	case "file":
		file, header, err := r.FormFile("torrent_file")
		if err != nil {
			http.Error(w, "no torrent file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()
		dl.name = strings.TrimSuffix(header.Filename, ".torrent")

		tmp, err := os.CreateTemp("", "torrent-*.torrent")
		if err != nil {
			http.Error(w, "temp file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := io.Copy(tmp, file); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			http.Error(w, "write temp: "+err.Error(), http.StatusInternalServerError)
			return
		}
		tmp.Close()
		source = tmp.Name()

	case "magnet":
		m := strings.TrimSpace(r.FormValue("magnet_url"))
		if !strings.HasPrefix(m, "magnet:") {
			http.Error(w, "invalid magnet URI", http.StatusBadRequest)
			return
		}
		source = m
		dl.name = magnetDisplayName(m)

	default:
		http.Error(w, "unknown source_type", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	dl.mu.Lock()
	dl.cancel = cancel
	dl.mu.Unlock()

	s.mu.Lock()
	s.downloads[id] = dl
	s.mu.Unlock()

	s.hub.send("queue_add", dl.snapshot())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": id})

	go s.runDownload(ctx, dl, source, maxPeers, verbose)
}

// handleCancel cancels the download identified by the ?id= query parameter.
func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	s.mu.Lock()
	dl, ok := s.downloads[id]
	s.mu.Unlock()
	if ok {
		dl.mu.Lock()
		cancel := dl.cancel
		dl.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleEvents is an SSE endpoint. Newly connected clients receive a replay of
// the current queue state, then live events until they disconnect.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type",                "text/event-stream")
	w.Header().Set("Cache-Control",               "no-cache")
	w.Header().Set("Connection",                  "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(sseClient, 64)
	s.hub.add(ch)
	defer s.hub.remove(ch)

	// Replay existing queue so a reconnecting tab sees current state.
	s.mu.Lock()
	for _, dl := range s.downloads {
		data, _ := json.Marshal(dl.snapshot())
		fmt.Fprintf(w, "event: queue_add\ndata: %s\n\n", data)
	}
	s.mu.Unlock()
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			io.WriteString(w, msg)
			flusher.Flush()
		}
	}
}

// runDownload is the background goroutine that drives one download to completion.
func (s *server) runDownload(ctx context.Context, dl *dlEntry, source string, maxPeers, verbose int) {
	defer func() {
		if dl.sourceType == "file" {
			os.Remove(source)
		}
	}()

	sendLog := func(line string) {
		s.hub.send("log", map[string]string{"id": dl.id, "line": line})
	}

	setErr := func(msg string) {
		dl.mu.Lock()
		dl.status = "error"
		dl.errMsg = msg
		dl.mu.Unlock()
		s.hub.send("queue_update", dl.snapshot())
		s.hub.send("error_event", map[string]string{"id": dl.id, "message": msg})
	}

	if err := os.MkdirAll(dl.outputDir, 0755); err != nil {
		setErr("cannot create output directory: " + err.Error())
		return
	}

	var info *torrent.TorrentInfo
	var err error
	if strings.HasPrefix(source, "magnet:") {
		info, err = torrent.ParseMagnet(source)
	} else {
		info, err = torrent.OpenFile(source)
	}
	if err != nil {
		setErr(err.Error())
		return
	}

	if info.Name != "" {
		dl.mu.Lock()
		dl.name = info.Name
		dl.mu.Unlock()
		s.hub.send("queue_update", dl.snapshot())
	}

	if !info.HasMetadata() {
		sendLog("[gui] magnet link — fetching metadata from peers…")
		peerID, err := newPeerID()
		if err != nil {
			setErr(err.Error())
			return
		}
		peerList, err := gatherPeers(info, peerID)
		if err != nil {
			setErr("no peers found: " + err.Error())
			return
		}
		info, err = metadata.FetchFromPeers(info, peerList, peerID)
		if err != nil {
			setErr("metadata fetch failed: " + err.Error())
			return
		}
		dl.mu.Lock()
		dl.name = info.Name
		dl.mu.Unlock()
		s.hub.send("queue_update", dl.snapshot())
		sendLog(fmt.Sprintf("[gui] metadata ok: %q  %d pieces", info.Name, len(info.PieceHashes)))
	}

	// Engine.Output must be the full file path for single-file torrents and
	// the parent directory for multi-file torrents.
	var outputPath string
	if info.IsMultiFile() {
		outputPath = dl.outputDir
	} else {
		outputPath = filepath.Join(dl.outputDir, info.Name)
	}

	engine := &p2p.Engine{
		Info:     info,
		Output:   outputPath,
		MaxPeers: maxPeers,
		Verbose:  verbose,
		Context:  ctx,
		LogFunc:  sendLog,
		ProgressFunc: func(done, total int, speedMBps float64) {
			dl.mu.Lock()
			dl.done = done
			dl.total = total
			dl.speed = speedMBps
			dl.mu.Unlock()
			pct := float64(done) / float64(total) * 100
			s.hub.send("progress", map[string]any{
				"id":    dl.id,
				"done":  done,
				"total": total,
				"speed": speedMBps,
				"pct":   pct,
			})
		},
	}

	if err := engine.Download(); err != nil {
		if ctx.Err() != nil {
			dl.mu.Lock()
			dl.status = "cancelled"
			dl.mu.Unlock()
			s.hub.send("queue_update", dl.snapshot())
			s.hub.send("cancelled", map[string]string{"id": dl.id})
			return
		}
		setErr(err.Error())
		return
	}

	dl.mu.Lock()
	dl.status = "done"
	if dl.total > 0 {
		dl.done = dl.total
	}
	dl.mu.Unlock()
	s.hub.send("queue_update", dl.snapshot())
	s.hub.send("done", map[string]string{
		"id":     dl.id,
		"name":   info.Name,
		"output": dl.outputDir,
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// magnetDisplayName extracts a human-readable name from a magnet URI.
func magnetDisplayName(m string) string {
	if i := strings.Index(m, "dn="); i >= 0 {
		s := m[i+3:]
		if j := strings.IndexAny(s, "& "); j >= 0 {
			s = s[:j]
		}
		if d, err := url.QueryUnescape(s); err == nil && d != "" {
			return d
		}
	}
	if i := strings.Index(m, "btih:"); i >= 0 {
		h := m[i+5:]
		if len(h) > 8 {
			h = h[:8]
		}
		return "magnet-" + strings.ToLower(h)
	}
	return "magnet link"
}

// openBrowser opens url in the default browser on Linux, macOS, or Windows.
func openBrowser(rawURL string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", rawURL)
	case "darwin":
		cmd = exec.Command("open", rawURL)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", rawURL)
	default:
		fmt.Printf("Open your browser at: %s\n", rawURL)
		return
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Start()
}

func gatherPeers(info *torrent.TorrentInfo, peerID [20]byte) ([]peers.Peer, error) {
	var mu   sync.Mutex
	seen := make(map[string]bool)
	var all []peers.Peer
	var wg  sync.WaitGroup

	for _, u := range trackerURLs(info) {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			t, err := tracker.New(u)
			if err != nil {
				return
			}
			got, err := t.GetPeers(info.InfoHash, peerID, 6881, info.TotalLength())
			if err != nil {
				return
			}
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
		return id, err
	}
	return id, nil
}

func newID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}
