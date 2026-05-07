// Package stream implements in-memory torrent streaming for video playback.
// It detects playable video files within a torrent, manages an ephemeral temp
// directory for the download, and serves HTTP range requests to the browser.
package stream

import (
	"path/filepath"
	"strings"

	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

// videoExts are file extensions the browser can play or at least attempt to play.
var videoExts = map[string]bool{
	".mp4":  true,
	".mkv":  true,
	".avi":  true,
	".mov":  true,
	".webm": true,
	".m4v":  true,
	".ts":   true,
	".flv":  true,
	".wmv":  true,
	".mpg":  true,
	".mpeg": true,
	".m2ts": true,
	".ogg":  true,
	".ogv":  true,
	".3gp":  true,
}

// archiveExts are compressed container formats that may contain video files.
var archiveExts = map[string]bool{
	".zip": true,
}

// PlayableFile describes one video file that can be served to the browser.
type PlayableFile struct {
	Name       string // display name (relative path within torrent)
	Extension  string // lower-cased extension, e.g. ".mp4"
	Size       int64  // byte size
	FlatOffset int64  // byte offset in the torrent's flat byte space
	PhysPath   string // absolute path to the physical file in the temp dir
	FileIdx    int    // index into TorrentInfo.Files; -1 for single-file torrents
	InZip      bool   // true when this file was extracted from a zip archive
}

// ArchiveFile describes a zip (or similar) file within the torrent.
type ArchiveFile struct {
	Name       string
	Size       int64
	FlatOffset int64
	PhysPath   string
	FileIdx    int
}

// ScanFiles inspects TorrentInfo and returns all video and archive files.
// physDir is the temp directory root used to construct physical file paths;
// pass "" if the temp dir has not been created yet.
func ScanFiles(info *torrent.TorrentInfo, physDir string) (videos []PlayableFile, archives []ArchiveFile) {
	if info.IsMultiFile() {
		var offset int64
		for i, f := range info.Files {
			name := filepath.Join(f.Path...)
			ext := strings.ToLower(filepath.Ext(name))
			size := int64(f.Length)

			var physPath string
			if physDir != "" {
				parts := append([]string{physDir, info.Name}, f.Path...)
				physPath = filepath.Join(parts...)
			}

			switch {
			case videoExts[ext]:
				videos = append(videos, PlayableFile{
					Name:       name,
					Extension:  ext,
					Size:       size,
					FlatOffset: offset,
					PhysPath:   physPath,
					FileIdx:    i,
				})
			case archiveExts[ext]:
				archives = append(archives, ArchiveFile{
					Name:       name,
					Size:       size,
					FlatOffset: offset,
					PhysPath:   physPath,
					FileIdx:    i,
				})
			}
			offset += size
		}
	} else {
		name := info.Name
		ext := strings.ToLower(filepath.Ext(name))
		size := int64(info.Length)

		var physPath string
		if physDir != "" {
			physPath = filepath.Join(physDir, name)
		}

		switch {
		case videoExts[ext]:
			videos = append(videos, PlayableFile{
				Name:       name,
				Extension:  ext,
				Size:       size,
				FlatOffset: 0,
				PhysPath:   physPath,
				FileIdx:    -1,
			})
		case archiveExts[ext]:
			archives = append(archives, ArchiveFile{
				Name:       name,
				Size:       size,
				FlatOffset: 0,
				PhysPath:   physPath,
				FileIdx:    -1,
			})
		}
	}
	return
}

// MIMEType returns the Content-Type for a given video extension.
func MIMEType(ext string) string {
	switch ext {
	case ".mp4", ".m4v":
		return "video/mp4"
	case ".mkv":
		return "video/x-matroska"
	case ".avi":
		return "video/x-msvideo"
	case ".webm":
		return "video/webm"
	case ".ogg", ".ogv":
		return "video/ogg"
	case ".mov":
		return "video/quicktime"
	case ".ts", ".m2ts":
		return "video/mp2t"
	case ".flv":
		return "video/x-flv"
	case ".wmv":
		return "video/x-ms-wmv"
	case ".mpg", ".mpeg":
		return "video/mpeg"
	case ".3gp":
		return "video/3gpp"
	default:
		return "video/mp4"
	}
}
