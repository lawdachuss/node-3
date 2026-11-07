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

// TestVidaraFileCode verifies the extractor handles every shape the Vidara
// API has returned across its lifetime: plain code, view link, embed link,
// query strings, and empty/garbage values.
func TestVidaraFileCode(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"AbC123xY", "AbC123xY"},
		{"https://vidara.so/v/AbC123xY", "AbC123xY"},
		{"https://vidara.to/e/AbC123xY", "AbC123xY"},
		{"https://vidara.to/AbC123xY", "AbC123xY"},
		{"https://vidara.so/v/AbC123xY?x=1", "AbC123xY"},
		{"https://vidara.to/e/AbC123xY?redirect=/x", "AbC123xY"}, // query with a slash must not confuse the split
		{"https://vidara.so/v/https://vidara.to/e/AbC123xY", "AbC123xY"},
		{"  https://vidara.so/e/AbC123xY/  ", "AbC123xY"},
		{"", ""},
		{"https://vidara.so/v/", ""},
		{"https://vidara.so/play", ""},     // future route — structural, not a code
		{"https://vidara.so/download", ""}, // long structural segment
		{"video.mp4", ""},                  // leaked filename — not a code
		{"AbC123xY.v1", ""},                // extension in the segment — not a code
		{"has space 123", ""},              // whitespace — not a code
		{"ab", ""},                         // too short to be a code
	}
	for _, c := range cases {
		if got := vidaraFileCode(c.in); got != c.want {
			t.Errorf("vidaraFileCode(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestVidaraUploadCanonicalLinks drives the real upload flow against a fake
// Vidara API and verifies the returned link is always the canonical view
// link — never a mangled "vidara.so/v/<full embed URL>" value. Each API
// response shape seen in production is covered.
func TestVidaraUploadCanonicalLinks(t *testing.T) {
	shapes := []struct {
		name     string
		url      string
		filecode string
		want     string
	}{
		{
			"docs shape: view link in url, code in filecode",
			"https://vidara.so/v/AbC123xY", "AbC123xY",
			"https://vidara.so/e/AbC123xY",
		},
		{
			"observed shape: embed link in url, code in filecode",
			"https://vidara.to/e/XyZ987abc", "XyZ987abc",
			"https://vidara.so/e/XyZ987abc",
		},
		{
			"observed shape: embed link in filecode, url empty",
			"", "https://vidara.to/e/QwErTy12345",
			"https://vidara.so/e/QwErTy12345",
		},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.Contains(r.URL.Path, "/upload/server"):
					fmt.Fprintf(w, `{"msg":"OK","status":200,"result":{"upload_server":%q}}`, srv.URL)
				default: // the upload POST
					fmt.Fprintf(w, `{"status":200,"msg":"OK","url":%q,"filecode":%q,"title":"test.mp4","video_id":1}`,
						s.url, s.filecode)
				}
			}))
			defer srv.Close()

			// The upload server URL returned by the fake points back at the
			// same server.
			oldBase := vidaraAPIBase
			vidaraAPIBase = srv.URL
			defer func() { vidaraAPIBase = oldBase }()

			dir := t.TempDir()
			filePath := filepath.Join(dir, "test.mp4")
			if err := os.WriteFile(filePath, []byte("fake video bytes"), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			u := NewVidaraUploader("test-key")
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
