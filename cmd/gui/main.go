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
	htmlpkg "html"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/metadata"
	"github.com/KabilanMA/p2p-torrent-client/internal/p2p"
	"github.com/KabilanMA/p2p-torrent-client/internal/peers"
	"github.com/KabilanMA/p2p-torrent-client/internal/stream"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
	"github.com/KabilanMA/p2p-torrent-client/internal/tracker"
)

//go:embed ui/index.html
var uiFS embed.FS

var (
	anchorMagnetRe = regexp.MustCompile(`(?is)<a[^>]*href\s*=\s*["']?(magnet:[^"'\s>]+)["']?[^>]*>(.*?)</a>`)
	anyMagnetRe    = regexp.MustCompile(`(?i)magnet:\?xt=urn:btih:[a-zA-Z0-9]{32,40}[^\s"'<>]*`)
	htmlTagRe      = regexp.MustCompile(`<[^>]+>`)
	whitespaceRe   = regexp.MustCompile(`\s+`)
	// Matches a regular (non-magnet) anchor whose text is a single clean string.
	titleAnchorRe  = regexp.MustCompile(`(?i)<a\s[^>]*href\s*=\s*["']([^"'#][^"']*)["'][^>]*>([^<]{4,200})</a>`)
)

type magnetResult struct {
	Magnet string `json:"magnet"`
	Name   string `json:"name"`
}

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

// streamEntry holds live state for one streaming session.
type streamEntry struct {
	mu      sync.Mutex
	id      string
	sess    *stream.Session
	name    string
	hasZip  bool // torrent contains archive files that need extraction
}

// streamFileInfo is the JSON-serialisable view of one playable file.
type streamFileInfo struct {
	Idx  int    `json:"idx"`
	Name string `json:"name"`
	Ext  string `json:"ext"`
	Size int64  `json:"size"`
}

// server holds all state shared across HTTP handlers.
type server struct {
	hub          *hub
	mu           sync.Mutex
	downloads    map[string]*dlEntry
	resolvedMu   sync.Mutex
	resolvedMeta map[string]*torrent.TorrentInfo // keyed by lowercase hex info hash
	streamMu     sync.Mutex
	streams      map[string]*streamEntry
}

// resolvedInfo is the JSON response for a successfully resolved magnet.
type resolvedInfo struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	TotalSize int64  `json:"total_size"`
	FileCount int    `json:"file_count"`
}

func toResolvedInfo(info *torrent.TorrentInfo) resolvedInfo {
	fileCount := len(info.Files)
	if fileCount == 0 && info.Length > 0 {
		fileCount = 1
	}
	return resolvedInfo{
		Hash:      strings.ToLower(hex.EncodeToString(info.InfoHash[:])),
		Name:      info.Name,
		TotalSize: int64(info.TotalLength()),
		FileCount: fileCount,
	}
}

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := &server{
		hub:          newHub(),
		downloads:    make(map[string]*dlEntry),
		resolvedMeta: make(map[string]*torrent.TorrentInfo),
		streams:      make(map[string]*streamEntry),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/",                    srv.serveIndex)
	mux.HandleFunc("/api/browse-dir",      srv.handleBrowseDir)
	mux.HandleFunc("/api/clean-magnet",    srv.handleCleanMagnet)
	mux.HandleFunc("/api/scrape-url",      srv.handleScrapeURL)
	mux.HandleFunc("/api/resolve-magnet",  srv.handleResolveMagnet)
	mux.HandleFunc("/api/discard-metadata",srv.handleDiscardMeta)
	mux.HandleFunc("/api/download",        srv.handleDownload)
	mux.HandleFunc("/api/downloads",       srv.handleDownloads)
	mux.HandleFunc("/api/cancel",          srv.handleCancel)
	mux.HandleFunc("/api/events",          srv.handleEvents)
	mux.HandleFunc("/api/stream",          srv.handleStreamStart)
	mux.HandleFunc("/api/stream/",         srv.handleStreamDispatch)

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

// handleScrapeURL fetches the given URL and returns all magnet links found on the page.
func (s *server) handleScrapeURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	rawURL := strings.TrimSpace(string(body))

	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		http.Error(w, "invalid URL: must start with http:// or https://", http.StatusBadRequest)
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	req, _ := http.NewRequest("GET", rawURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TorrentClient/1.0)")
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "fetch failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	pageBytes, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		http.Error(w, "read failed: "+err.Error(), http.StatusBadGateway)
		return
	}

	results := extractMagnetsFromHTML(string(pageBytes))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// genericTexts are strings that carry no useful torrent name — filtered from
// both anchor text and dn= parameter values.
var genericTexts = map[string]bool{
	"magnet": true, "magnet link": true, "[magnet]": true, "(magnet)": true,
	"download": true, "download torrent": true, "torrent": true,
	"get": true, "get torrent": true, "here": true, "link": true,
	"click here": true, "click": true, "dl": true, ".torrent": true,
	"[download]": true, "leechers": true, "seeders": true,
}

// isGeneric reports whether a candidate name is useless as a display name.
func isGeneric(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	return lower == "" || len(lower) <= 3 || genericTexts[lower] ||
		strings.HasPrefix(lower, "magnet-") || strings.HasPrefix(lower, "magnet link")
}

// chooseName returns the best static name for a magnet from anchor text and
// the dn= URI parameter. Returns "" when neither is useful (caller should try
// findRowTitle and then shortHashName as fallbacks).
func chooseName(anchorText, cleaned string) string {
	dnName := magnetDisplayName(cleaned)
	if isGeneric(dnName) {
		dnName = ""
	}

	if isGeneric(anchorText) {
		return dnName // may be ""
	}
	// Both anchor and dn= are non-generic: prefer the longer one.
	if dnName != "" && len(dnName) > len(anchorText)+8 {
		return dnName
	}
	return anchorText
}

// findRowTitle looks backwards from pos in pageHTML, finds the nearest <tr>
// or <li> boundary, and returns the longest non-generic anchor text within
// that row — typically the torrent title link on listing pages.
func findRowTitle(pageHTML string, pos int) string {
	const lookback = 3000
	start := pos - lookback
	if start < 0 {
		start = 0
	}
	window := pageHTML[start:pos]

	// Find the start of the nearest enclosing row / list-item.
	rowStart := 0
	for _, tag := range []string{"<tr", "<TR", "<li ", "<li\t", "<li>", "<LI "} {
		if i := strings.LastIndex(window, tag); i > rowStart {
			rowStart = i
		}
	}
	row := window[rowStart:]

	var best string
	for _, m := range titleAnchorRe.FindAllStringSubmatch(row, -1) {
		href := m[1]
		text := strings.TrimSpace(htmlpkg.UnescapeString(m[2]))
		if strings.HasPrefix(strings.ToLower(href), "magnet:") || isGeneric(text) {
			continue
		}
		if len(text) > len(best) {
			best = text
		}
	}
	return best
}

// shortHashName returns the first 12 hex chars of the info hash as a
// placeholder name (e.g. "23bbca2351ef…"). Used only when everything else fails.
func shortHashName(cleaned string) string {
	if i := strings.Index(cleaned, "btih:"); i >= 0 {
		h := cleaned[i+5:]
		if j := strings.IndexAny(h, "&? "); j >= 0 {
			h = h[:j]
		}
		h = strings.ToLower(h)
		if len(h) > 12 {
			return h[:12] + "…"
		}
		return h
	}
	return "unknown"
}

func extractMagnetsFromHTML(pageHTML string) []magnetResult {
	const maxResults = 100
	seen := make(map[string]bool)
	var results []magnetResult

	addMagnet := func(raw, name string) {
		if len(results) >= maxResults {
			return
		}
		cleaned, err := torrent.ExtractMagnet(htmlpkg.UnescapeString(raw))
		if err != nil || seen[cleaned] {
			return
		}
		seen[cleaned] = true
		if name == "" {
			name = shortHashName(cleaned)
		}
		results = append(results, magnetResult{Magnet: cleaned, Name: name})
	}

	// Pass 1: anchor tags — use position so findRowTitle can look at surrounding HTML.
	for _, idx := range anchorMagnetRe.FindAllStringSubmatchIndex(pageHTML, -1) {
		rawMagnet := pageHTML[idx[2]:idx[3]]
		anchorContent := pageHTML[idx[4]:idx[5]]

		cleaned, err := torrent.ExtractMagnet(htmlpkg.UnescapeString(rawMagnet))
		if err != nil || seen[cleaned] {
			continue
		}
		seen[cleaned] = true

		anchorText := strings.TrimSpace(whitespaceRe.ReplaceAllString(
			htmlpkg.UnescapeString(htmlTagRe.ReplaceAllString(anchorContent, " ")), " "))

		name := chooseName(anchorText, cleaned)
		if name == "" {
			name = findRowTitle(pageHTML, idx[0])
		}
		if name == "" {
			name = shortHashName(cleaned)
		}

		if len(results) < maxResults {
			results = append(results, magnetResult{Magnet: cleaned, Name: name})
		}
	}

	// Pass 2: any remaining magnet links not in anchor tags.
	for _, matchIdx := range anyMagnetRe.FindAllStringIndex(pageHTML, -1) {
		raw := pageHTML[matchIdx[0]:matchIdx[1]]
		cleaned, err := torrent.ExtractMagnet(raw)
		if err != nil || seen[cleaned] {
			continue
		}
		name := findRowTitle(pageHTML, matchIdx[0])
		addMagnet(raw, name) // addMagnet marks seen and uses shortHashName if name=""
	}

	if results == nil {
		results = []magnetResult{}
	}
	return results
}

// handleResolveMagnet fetches BEP 9 metadata for a magnet URI, caches the full
// TorrentInfo by info hash, and returns name/size so the UI can show real names
// before the user starts a download.
func (s *server) handleResolveMagnet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 16*1024))
	magnetURI := strings.TrimSpace(string(body))

	info, err := torrent.ParseMagnet(magnetURI)
	if err != nil {
		http.Error(w, "invalid magnet: "+err.Error(), http.StatusBadRequest)
		return
	}

	hashKey := strings.ToLower(hex.EncodeToString(info.InfoHash[:]))

	// Return immediately if already cached.
	s.resolvedMu.Lock()
	if cached, ok := s.resolvedMeta[hashKey]; ok {
		s.resolvedMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toResolvedInfo(cached))
		return
	}
	s.resolvedMu.Unlock()

	peerID, err := newPeerID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		info *torrent.TorrentInfo
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		peerList, err := gatherPeers(info, peerID)
		if err != nil {
			ch <- result{nil, err}
			return
		}
		resolved, err := metadata.FetchFromPeers(info, peerList, peerID)
		ch <- result{resolved, err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			http.Error(w, res.err.Error(), http.StatusBadGateway)
			return
		}
		s.resolvedMu.Lock()
		s.resolvedMeta[hashKey] = res.info
		s.resolvedMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toResolvedInfo(res.info))
	case <-time.After(35 * time.Second):
		http.Error(w, "metadata fetch timed out", http.StatusGatewayTimeout)
	}
}

// handleDiscardMeta removes pre-resolved metadata for a given info hash,
// freeing memory for magnets the user chose not to download.
func (s *server) handleDiscardMeta(w http.ResponseWriter, r *http.Request) {
	hash := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("hash")))
	if hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	s.resolvedMu.Lock()
	delete(s.resolvedMeta, hash)
	s.resolvedMu.Unlock()
	w.WriteHeader(http.StatusOK)
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

	markCancelled := func() {
		dl.mu.Lock()
		dl.status = "cancelled"
		dl.mu.Unlock()
		s.hub.send("queue_update", dl.snapshot())
		s.hub.send("cancelled", map[string]string{"id": dl.id})
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
		hashKey := strings.ToLower(hex.EncodeToString(info.InfoHash[:]))

		// Use pre-resolved metadata from the URL-scrape flow if available.
		s.resolvedMu.Lock()
		cached, hasCached := s.resolvedMeta[hashKey]
		if hasCached {
			delete(s.resolvedMeta, hashKey) // consume — no longer needed in cache
		}
		s.resolvedMu.Unlock()

		if hasCached {
			info = cached
			dl.mu.Lock()
			dl.name = info.Name
			dl.mu.Unlock()
			s.hub.send("queue_update", dl.snapshot())
			sendLog(fmt.Sprintf("[gui] metadata ready (pre-resolved): %q  %d pieces", info.Name, len(info.PieceHashes)))
		} else {
			sendLog("[gui] magnet link — fetching metadata from peers…")
			peerID, err := newPeerID()
			if err != nil {
				setErr(err.Error())
				return
			}

			// gatherPeers and FetchFromPeers block without context support.
			// Run each in a goroutine and race it against ctx.Done() so cancel works.
			type peersResult struct {
				peers []peers.Peer
				err   error
			}
			pCh := make(chan peersResult, 1)
			go func() {
				pl, e := gatherPeers(info, peerID)
				pCh <- peersResult{pl, e}
			}()

			var peerList []peers.Peer
			select {
			case <-ctx.Done():
				markCancelled()
				return
			case res := <-pCh:
				if res.err != nil {
					setErr("no peers found: " + res.err.Error())
					return
				}
				peerList = res.peers
			}

			type metaResult struct {
				info *torrent.TorrentInfo
				err  error
			}
			mCh := make(chan metaResult, 1)
			go func() {
				i, e := metadata.FetchFromPeers(info, peerList, peerID)
				mCh <- metaResult{i, e}
			}()

			select {
			case <-ctx.Done():
				markCancelled()
				return
			case res := <-mCh:
				if res.err != nil {
					setErr("metadata fetch failed: " + res.err.Error())
					return
				}
				info = res.info
			}

			dl.mu.Lock()
			dl.name = info.Name
			dl.mu.Unlock()
			s.hub.send("queue_update", dl.snapshot())
			sendLog(fmt.Sprintf("[gui] metadata ok: %q  %d pieces", info.Name, len(info.PieceHashes)))
		}
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
			markCancelled()
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

// ── Stream handlers ───────────────────────────────────────────────────────────

// handleStreamStart resolves torrent metadata, detects playable files, and
// starts a streaming session. The download runs to a private temp directory
// so the user's disk stays clean until they choose to save.
//
// Response codes:
//
//	200 — playable files found; includes file list in JSON
//	202 — only archive files found; client should wait for stream_ready SSE
//	422 — no playable files at all; client should offer normal download
//	4xx/5xx — error
func (s *server) handleStreamStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}

	sourceType := r.FormValue("source_type")
	maxPeers, _ := strconv.Atoi(r.FormValue("max_peers"))
	verbose, _ := strconv.Atoi(r.FormValue("verbose"))
	if maxPeers < 1 {
		maxPeers = 50
	}

	// Resolve torrent info (same flow as handleDownload, but no output_dir).
	var source string
	var displayName string
	switch sourceType {
	case "file":
		file, header, err := r.FormFile("torrent_file")
		if err != nil {
			http.Error(w, "no torrent file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()
		displayName = strings.TrimSuffix(header.Filename, ".torrent")
		tmp, err := os.CreateTemp("", "torrent-*.torrent")
		if err != nil {
			http.Error(w, "temp file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := io.Copy(tmp, file); err != nil {
			tmp.Close(); os.Remove(tmp.Name())
			http.Error(w, "write temp: "+err.Error(), http.StatusInternalServerError)
			return
		}
		tmp.Close()
		source = tmp.Name()
		defer os.Remove(source)
	case "magnet":
		m := strings.TrimSpace(r.FormValue("magnet_url"))
		if !strings.HasPrefix(m, "magnet:") {
			http.Error(w, "invalid magnet URI", http.StatusBadRequest)
			return
		}
		source = m
		displayName = magnetDisplayName(m)
	default:
		http.Error(w, "unknown source_type", http.StatusBadRequest)
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if info.Name != "" {
		displayName = info.Name
	}

	// Fetch metadata for magnet links that have none.
	if !info.HasMetadata() {
		hashKey := strings.ToLower(hex.EncodeToString(info.InfoHash[:]))
		s.resolvedMu.Lock()
		cached, ok := s.resolvedMeta[hashKey]
		if ok {
			delete(s.resolvedMeta, hashKey)
		}
		s.resolvedMu.Unlock()

		if ok {
			info = cached
		} else {
			peerID, err := newPeerID()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			peerList, err := gatherPeers(info, peerID)
			if err != nil {
				http.Error(w, "no peers: "+err.Error(), http.StatusBadGateway)
				return
			}
			resolved, err := metadata.FetchFromPeers(info, peerList, peerID)
			if err != nil {
				http.Error(w, "metadata fetch: "+err.Error(), http.StatusBadGateway)
				return
			}
			info = resolved
		}
		displayName = info.Name
	}

	// Detect playable and archive files.
	videos, archives := stream.ScanFiles(info, "")
	hasArchiveOnly := len(videos) == 0 && len(archives) > 0

	if len(videos) == 0 && len(archives) == 0 {
		// Nothing playable — tell the UI to offer a normal download instead.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(map[string]any{
			"no_playable": true,
			"name":        displayName,
		})
		return
	}

	// Create session and start download.
	id := newID()
	sess, err := stream.NewSession(id, info)
	if err != nil {
		http.Error(w, "create session: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Populate physical paths now that tempDir is known.
	videos, _ = stream.ScanFiles(info, sess.TempDir)
	sess.SetFiles(videos)

	entry := &streamEntry{id: id, sess: sess, name: displayName, hasZip: hasArchiveOnly}
	s.streamMu.Lock()
	s.streams[id] = entry
	s.streamMu.Unlock()

	sendLog := func(line string) {
		s.hub.send("stream_log", map[string]string{"id": id, "line": line})
	}
	progressFn := func(done, total int, speedMBps float64) {
		s.hub.send("stream_progress", map[string]any{
			"id":    id,
			"done":  done,
			"total": total,
			"speed": speedMBps,
			"pct":   float64(done) / float64(total) * 100,
		})
	}

	sess.Start(context.Background(), maxPeers, verbose, sendLog, progressFn, nil)

	// If the torrent contains only archive files, wait for full download
	// in a background goroutine, then extract and notify via SSE.
	if hasArchiveOnly {
		_, archivesWithPaths := stream.ScanFiles(info, sess.TempDir)
		go s.runArchiveExtraction(id, sess, archivesWithPaths)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{
			"id":          id,
			"name":        displayName,
			"has_archive": true,
			"files":       []streamFileInfo{},
		})
		return
	}

	// Direct video files — respond immediately with file list.
	fileInfos := toStreamFileInfos(videos)
	go func() {
		<-sess.Done()
		if sess.DownloadErr() != nil {
			s.hub.send("stream_error", map[string]string{"id": id, "message": sess.DownloadErr().Error()})
		} else {
			s.hub.send("stream_done", map[string]string{"id": id})
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":          id,
		"name":        displayName,
		"has_archive": false,
		"files":       fileInfos,
	})
}

// runArchiveExtraction waits for the P2P download to finish, extracts video
// files from each archive, and notifies the browser via SSE.
func (s *server) runArchiveExtraction(id string, sess *stream.Session, archives []stream.ArchiveFile) {
	<-sess.Done()
	if sess.DownloadErr() != nil {
		s.hub.send("stream_error", map[string]string{"id": id, "message": sess.DownloadErr().Error()})
		return
	}

	var allVideos []stream.PlayableFile
	for _, arch := range archives {
		videos, err := sess.ExtractZip(arch)
		if err != nil {
			s.hub.send("stream_log", map[string]string{"id": id, "line": "[stream] zip extract error: " + err.Error()})
			continue
		}
		allVideos = append(allVideos, videos...)
	}

	if len(allVideos) == 0 {
		s.hub.send("stream_error", map[string]string{"id": id, "message": "no playable files found inside archive"})
		return
	}

	sess.SetFiles(allVideos)
	s.hub.send("stream_ready", map[string]any{
		"id":    id,
		"files": toStreamFileInfos(allVideos),
	})
}

func toStreamFileInfos(files []stream.PlayableFile) []streamFileInfo {
	out := make([]streamFileInfo, len(files))
	for i, f := range files {
		out[i] = streamFileInfo{
			Idx:  i,
			Name: f.Name,
			Ext:  f.Extension,
			Size: f.Size,
		}
	}
	return out
}

// handleStreamDispatch routes sub-paths under /api/stream/{id}/...
func (s *server) handleStreamDispatch(w http.ResponseWriter, r *http.Request) {
	// Strip "/api/stream/" prefix.
	rest := strings.TrimPrefix(r.URL.Path, "/api/stream/")
	parts := strings.SplitN(rest, "/", 3)

	if len(parts) < 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]

	s.streamMu.Lock()
	entry, ok := s.streams[id]
	s.streamMu.Unlock()
	if !ok {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	if len(parts) == 1 {
		// DELETE /api/stream/{id} — close session.
		if r.Method == http.MethodDelete {
			entry.sess.Close()
			s.streamMu.Lock()
			delete(s.streams, id)
			s.streamMu.Unlock()
			w.WriteHeader(http.StatusOK)
		} else {
			http.NotFound(w, r)
		}
		return
	}

	switch parts[1] {
	case "file":
		// GET /api/stream/{id}/file/{idx}
		if len(parts) < 3 {
			http.NotFound(w, r)
			return
		}
		idx, err := strconv.Atoi(parts[2])
		if err != nil {
			http.Error(w, "invalid file index", http.StatusBadRequest)
			return
		}
		entry.sess.ServeFile(w, r, idx)

	case "status":
		// GET /api/stream/{id}/status
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         id,
			"downloaded": entry.sess.Waiter.Downloaded(),
			"total":      entry.sess.Waiter.Total(),
		})

	case "save":
		// POST /api/stream/{id}/save — copy files to output_dir.
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			OutputDir string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OutputDir == "" {
			http.Error(w, "output_dir required", http.StatusBadRequest)
			return
		}
		go func() {
			if err := entry.sess.Save(body.OutputDir); err != nil {
				s.hub.send("stream_save_error", map[string]string{"id": id, "message": err.Error()})
			} else {
				s.hub.send("stream_saved", map[string]string{"id": id, "output": body.OutputDir})
			}
		}()
		w.WriteHeader(http.StatusAccepted)

	default:
		http.NotFound(w, r)
	}
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
