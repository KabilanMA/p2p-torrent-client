package torrent

import (
	"testing"
)

const hashSuffix = "abc123def456abc123def456abc123def456abc1"
const base = "magnet:?xt=urn:btih:" + hashSuffix + "&dn=File&tr=http://t.example.com/a"

func TestExtractMagnet(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // empty means error expected
	}{
		{"already clean",         base,                                  base},
		{"leading text",          "Download here: " + base,              base},
		{"trailing period",       base + ".",                            base},
		{"trailing comma",        base + ",",                            base},
		{"trailing exclamation",  base + "!",                            base},
		{"trailing text+space",   base + " (5 seeders)",                 base},
		{"double quotes",         `"` + base + `"`,                      base},
		{"parens",                "(" + base + ")",                      base},
		{"angle brackets",        "<" + base + ">",                      base},
		{"newline terminator",    base + "\nother text",                  base},
		{"html &amp; entities",
			"magnet:?xt=urn:btih:" + hashSuffix + "&amp;dn=File&amp;tr=http://t.example.com/a",
			base},
		{"no magnet",  "just plain text", ""},
		{"no xt param", "magnet:?dn=file", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractMagnet(tt.input)
			if tt.want == "" {
				if err == nil {
					t.Errorf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("\ngot:  %s\nwant: %s", got, tt.want)
			}
		})
	}
}
