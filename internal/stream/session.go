package stream

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/KabilanMA/p2p-torrent-client/internal/p2p"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

// Session holds the state for one active streaming session.
// A session corresponds to one "Watch" action by the user.
type Session struct {
	ID      string
	Info    *torrent.TorrentInfo
	TempDir string
	Waiter  *PieceWaiter

	mu      sync.Mutex
	files   []PlayableFile // may grow after zip extraction
	dlDone  chan struct{}   // closed when the P2P engine finishes
	dlErr   error
	cancel  context.CancelFunc
	hasZip  bool // true when the torrent contained archive files
}

// NewSession creates an ephemeral temp directory and a PieceWaiter for the
// given torrent. Call Start() to begin the P2P download.
func NewSession(id string, info *torrent.TorrentInfo) (*Session, error) {
	tempDir, err := os.MkdirTemp("", "torrent-stream-"+id+"-")
	if err != nil {
		return nil, fmt.Errorf("stream: create temp dir: %w", err)
	}

	numPieces := len(info.PieceHashes)
	waiter := NewPieceWaiter(numPieces, int64(info.PieceLength), int64(info.TotalLength()))

	return &Session{
		ID:      id,
		Info:    info,
		TempDir: tempDir,
		Waiter:  waiter,
		dlDone:  make(chan struct{}),
	}, nil
}

// SetFiles atomically replaces the list of playable files (called after zip extraction).
func (s *Session) SetFiles(files []PlayableFile) {
	s.mu.Lock()
	s.files = files
	s.mu.Unlock()
}

// Files returns a snapshot of the current playable file list.
func (s *Session) Files() []PlayableFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PlayableFile{}, s.files...)
}

// Start launches the P2P engine in a background goroutine.
// pieceReadyCb (optional) is called in addition to the session's own PieceWaiter.
func (s *Session) Start(
	ctx context.Context,
	maxPeers, verbose int,
	logFunc func(string),
	progressFunc func(done, total int, speedMBps float64),
	pieceReadyCb func(int),
) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Engine.Output follows the same convention as handleDownload:
	// full file path for single-file torrents, parent dir for multi-file.
	var output string
	if s.Info.IsMultiFile() {
		output = s.TempDir
	} else {
		output = filepath.Join(s.TempDir, s.Info.Name)
	}

	eng := &p2p.Engine{
		Info:       s.Info,
		Output:     output,
		MaxPeers:   maxPeers,
		Verbose:    verbose,
		Context:    ctx,
		LogFunc:    logFunc,
		Sequential: true,
		PieceReady: func(index int) {
			s.Waiter.MarkReady(index)
			if pieceReadyCb != nil {
				pieceReadyCb(index)
			}
		},
		ProgressFunc: progressFunc,
	}

	go func() {
		err := eng.Download()
		s.mu.Lock()
		s.dlErr = err
		s.mu.Unlock()
		s.Waiter.Close()
		close(s.dlDone)
	}()
}

// Done returns a channel that is closed when the P2P download finishes.
func (s *Session) Done() <-chan struct{} { return s.dlDone }

// DownloadErr returns the error (if any) from the completed download.
// Only valid after Done() is closed.
func (s *Session) DownloadErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dlErr
}

// ExtractZip extracts playable video files from an ArchiveFile that has been
// fully downloaded into s.TempDir. Returns the extracted PlayableFiles.
func (s *Session) ExtractZip(arch ArchiveFile) ([]PlayableFile, error) {
	rc, err := zip.OpenReader(arch.PhysPath)
	if err != nil {
		return nil, fmt.Errorf("stream: open zip %q: %w", arch.Name, err)
	}
	defer rc.Close()

	extractDir := filepath.Join(s.TempDir, "_extracted")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		return nil, fmt.Errorf("stream: mkdir extracted: %w", err)
	}

	var videos []PlayableFile
	for _, f := range rc.File {
		if f.FileInfo().IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(f.Name))
		if !videoExts[ext] {
			continue
		}
		destPath := filepath.Join(extractDir, filepath.Base(f.Name))
		if err := extractEntry(f, destPath); err != nil {
			continue
		}
		videos = append(videos, PlayableFile{
			Name:      f.Name,
			Extension: ext,
			Size:      int64(f.UncompressedSize64),
			PhysPath:  destPath,
			FileIdx:   -1,
			InZip:     true,
		})
	}
	return videos, nil
}

func extractEntry(f *zip.File, dest string) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, rc)
	return err
}

// ServeFile handles an HTTP range request for the i-th playable file.
// For files still being downloaded, reads block until the required pieces arrive.
// For zip-extracted files, they are already fully available.
func (s *Session) ServeFile(w http.ResponseWriter, r *http.Request, fileIdx int) {
	s.mu.Lock()
	if fileIdx < 0 || fileIdx >= len(s.files) {
		s.mu.Unlock()
		http.Error(w, "file index out of range", http.StatusNotFound)
		return
	}
	pf := s.files[fileIdx]
	s.mu.Unlock()

	f, err := os.Open(pf.PhysPath)
	if err != nil {
		http.Error(w, "cannot open: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", MIMEType(pf.Extension))

	if pf.InZip {
		// Already fully extracted — serve directly with no waiting.
		reader := newImmediateReader(f, pf.Size)
		http.ServeContent(w, r, pf.Name, time.Time{}, reader)
	} else {
		// Block reads until pieces are available.
		reader := NewFileReader(f, s.Waiter, pf.Size, pf.FlatOffset)
		http.ServeContent(w, r, pf.Name, time.Time{}, reader)
	}
}

// Save copies every playable file to outputDir (blocking until download is done).
func (s *Session) Save(outputDir string) error {
	<-s.dlDone
	if err := s.DownloadErr(); err != nil {
		return fmt.Errorf("stream: download failed: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	for _, pf := range s.Files() {
		dest := filepath.Join(outputDir, filepath.Base(pf.Name))
		if err := copyFile(pf.PhysPath, dest); err != nil {
			return fmt.Errorf("stream: copy %q: %w", pf.Name, err)
		}
	}
	return nil
}

// Close cancels any ongoing download and removes the temp directory.
func (s *Session) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.Waiter.Close()
	os.RemoveAll(s.TempDir)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
