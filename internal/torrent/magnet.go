package torrent

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"html"
	"net/url"
	"strings"
)

// ExtractMagnet finds and sanitises a magnet URI from arbitrary input.
//
// Handled pollution patterns:
//   - Leading/trailing text ("Download: magnet:?xt=... (seeders: 5)")
//   - HTML character entities (&amp; → &, &#38;, &lt;, &gt;, etc.)
//   - Surrounding quote or bracket pairs ("magnet:?...", [magnet:?...])
//   - Mixed-case scheme (MAGNET:?XT=...)
//   - Trailing sentence punctuation (., ,, ;, !)
//   - Embedded whitespace/newlines (collapsed away from the URI body)
//
// Returns the first valid magnet URI found, or an error if none is present.
func ExtractMagnet(input string) (string, error) {
	// ── Step 1: decode HTML entities ────────────────────────────────────────
	// Webpages and forum posts encode & as &amp; (and sometimes &#38; or &amp;amp;),
	// turning "magnet:?xt=...&dn=..." into "magnet:?xt=...&amp;dn=...".
	// html.UnescapeString handles all standard named and numeric references.
	s := html.UnescapeString(strings.TrimSpace(input))

	// ── Step 2: find the first magnet: prefix ────────────────────────────────
	lower := strings.ToLower(s)
	start := strings.Index(lower, "magnet:")
	if start == -1 {
		return "", fmt.Errorf("no magnet URI found in input")
	}

	raw := s[start:]

	// ── Step 3: extract until a plain-text URI terminator ───────────────────
	// Characters that cannot appear in a URI in a normal text context.
	// Note: single-quote (') and closing-paren are valid URI sub-delimiters
	// and appear in some tracker announce URLs, so we keep them here.
	const terminators = " \t\n\r\"<>{}|^`\\"
	if i := strings.IndexAny(raw, terminators); i >= 0 {
		raw = raw[:i]
	}

	// ── Step 4: strip trailing sentence punctuation ──────────────────────────
	// Periods, commas, semicolons, exclamation marks, and closing brackets/quotes
	// are routinely added by copy-paste from forum posts, README files, etc.
	// Tracker URLs and magnet parameters never end with these characters.
	raw = strings.TrimRight(raw, ".,;!'\")")

	// ── Step 5: strip a lone leading bracket that the URI may have started after
	// e.g. the original text was "(magnet:?...)" — the ) was stripped above
	// but the search started right at 'magnet:' so the leading ( is not in raw;
	// however angle-bracket or quote wrapping (after entity-decode) can remain.
	raw = strings.TrimLeft(raw, "<([\"'")
	// Re-trim the right side in case stripping the left revealed a new closer.
	raw = strings.TrimRight(raw, ">)]\"'")

	// ── Step 6: validate the extracted candidate ─────────────────────────────
	lraw := strings.ToLower(raw)
	if !strings.HasPrefix(lraw, "magnet:") {
		return "", fmt.Errorf("extracted text does not start with magnet: scheme")
	}
	if !strings.Contains(lraw, "xt=urn:btih:") {
		return "", fmt.Errorf("extracted magnet URI is missing required xt=urn:btih parameter")
	}

	return raw, nil
}

// ParseMagnet parses a magnet URI and returns a TorrentInfo.
// The input is automatically cleaned via ExtractMagnet so noisy clipboard
// pastes (HTML entities, surrounding text, quotes, etc.) are handled
// transparently.
// PieceHashes and PieceLength will be zero until metadata is fetched via BEP 9.
func ParseMagnet(input string) (*TorrentInfo, error) {
	uri, err := ExtractMagnet(input)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet URI: %w", err)
	}
	if u.Scheme != "magnet" {
		return nil, fmt.Errorf("not a magnet URI: scheme %q", u.Scheme)
	}

	params := u.Query()

	var infoHash [20]byte
	found := false
	for _, xt := range params["xt"] {
		if !strings.HasPrefix(xt, "urn:btih:") {
			continue
		}
		hash := strings.TrimPrefix(xt, "urn:btih:")
		switch len(hash) {
		case 40: // hex
			b, err := hex.DecodeString(hash)
			if err != nil {
				return nil, fmt.Errorf("invalid info hash hex: %w", err)
			}
			copy(infoHash[:], b)
		case 32: // base32
			b, err := base32.StdEncoding.DecodeString(strings.ToUpper(hash))
			if err != nil {
				return nil, fmt.Errorf("invalid info hash base32: %w", err)
			}
			copy(infoHash[:], b)
		default:
			return nil, fmt.Errorf("info hash has unexpected length %d", len(hash))
		}
		found = true
		break
	}
	if !found {
		return nil, fmt.Errorf("magnet URI missing xt=urn:btih parameter")
	}

	info := &TorrentInfo{InfoHash: infoHash}

	if dn := params.Get("dn"); dn != "" {
		info.Name, _ = url.QueryUnescape(dn)
	}

	for _, tr := range params["tr"] {
		decoded, err := url.QueryUnescape(tr)
		if err != nil {
			decoded = tr
		}
		if info.Announce == "" {
			info.Announce = decoded
		}
		info.AnnounceList = append(info.AnnounceList, []string{decoded})
	}

	return info, nil
}
