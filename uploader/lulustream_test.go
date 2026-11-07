package uploader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLuluStreamFileCode verifies the extractor accepts the shapes the API can
// return (plain code, view link ending in .html, embed link, query strings) and
// rejects structural path segments and filenames.
func TestLuluStreamFileCode(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"AbC123xY", "AbC123xY"},
		{"https://lulustream.com/AbC123xY.html", "AbC123xY"},
		{"https://lulustream.com/e/AbC123xY", "AbC123xY"},
		{"https://lulustream.com/AbC123xY.html?x=1", "AbC123xY"},
		{"https://lulustream.com/e/AbC123xY?redirect=/x", "AbC123xY"},
		{"  https://lulustream.com/e/AbC123xY/  ", "AbC123xY"},
		{"", ""},
		{"https://lulustream.com/", ""},
		{"https://lulustream.com/e/", ""},
		{"video.mp4", ""},     // leaked filename — contains a dot
		{"has space 123", ""}, // whitespace — not a code
		{"ab", ""},            // too short to be a code
	}
	for _, c := range cases {
		if got := luluStreamCode(c.in); got != c.want {
			t.Errorf("luluStreamCode(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestLuluStreamUploadCanonicalLinks drives the upload flow against a fake
// LuluStream API and verifies every documented/observed response shape yields
// the canonical view link.
func TestLuluStreamUploadCanonicalLinks(t *testing.T) {
	shapes := []struct {
		name     string
		response string
		want     string
	}{
		{
			"docs shape: files[] with filecode",
			`{"status":200,"msg":"OK","files":[{"filecode":"AbC123xY","status":"OK"}]}`,
			"https://lulustream.com/AbC123xY.html",
		},
		{
			"files[] with only a view link",
			`{"status":200,"files":[{"url":"https://lulustream.com/AbC123xY.html"}]}`,
			"https://lulustream.com/AbC123xY.html",
		},
		{
			"doodstream-style result[]",
			`{"status":200,"result":[{"filecode":"AbC123xY"}]}`,
			"https://lulustream.com/AbC123xY.html",
		},
		{
			"single result object",
			`{"status":200,"result":{"filecode":"AbC123xY"}}`,
			"https://lulustream.com/AbC123xY.html",
		},
		{
			"bare result string",
			`{"status":200,"result":"AbC123xY"}`,
			"https://lulustream.com/AbC123xY.html",
		},
		{
			"embed URL in filecode",
			`{"status":200,"files":[{"filecode":"https://lulustream.com/e/AbC123xY"}]}`,
			"https://lulustream.com/AbC123xY.html",
		},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/upload/server") {
					fmt.Fprintf(w, `{"msg":"OK","status":200,"result":%q}`, srv.URL)
					return
				}
				fmt.Fprint(w, s.response)
			}))
			defer srv.Close()

			oldBase := luluStreamAPIBase
			luluStreamAPIBase = srv.URL
			defer func() { luluStreamAPIBase = oldBase }()

			dir := t.TempDir()
			filePath := filepath.Join(dir, "test.mp4")
			if err := os.WriteFile(filePath, []byte("fake video bytes"), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			u := NewLuluStreamUploader("test-key")
			link, err := u.Upload(filePath)
			if err != nil {
				t.Fatalf("Upload error: %v", err)
			}
			if link != s.want {
				t.Fatalf("Upload link = %q, want %q", link, s.want)
			}
		})
	}
}
