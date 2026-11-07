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

// TestDoodStreamCode verifies the extractor accepts the shapes the API can
// return (plain code, embed link, download link, query strings) and rejects
// structural path segments and filenames.
func TestDoodStreamCode(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"AbC123xY", "AbC123xY"},
		{"https://dood.to/e/AbC123xY", "AbC123xY"},
		{"https://dood.to/d/AbC123xY", "AbC123xY"},
		{"https://dood.to/e/AbC123xY?x=1", "AbC123xY"},
		{"https://dood.ws/e/AbC123xY?redirect=/x", "AbC123xY"},
		{"  https://dood.to/e/AbC123xY/  ", "AbC123xY"},
		{"", ""},
		{"https://dood.to/", ""},
		{"https://dood.to/e/", ""},
		{"video.mp4", ""},     // leaked filename — contains a dot
		{"has space 123", ""}, // whitespace — not a code
		{"ab", ""},            // too short to be a code
	}
	for _, c := range cases {
		if got := doodStreamCode(c.in); got != c.want {
			t.Errorf("doodStreamCode(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestDoodStreamUploadCanonicalLinks drives the upload flow against a fake
// DoodStream API and verifies every documented/observed response shape yields
// the canonical embed link. It also asserts the documented multipart api_key
// field (and the key query parameter) actually reach the upload node.
func TestDoodStreamUploadCanonicalLinks(t *testing.T) {
	shapes := []struct {
		name     string
		response string
		want     string
	}{
		{
			"docs shape: result[] with filecode",
			`{"status":200,"msg":"OK","result":[{"filecode":"AbC123xY","status":200}]}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"result[] with protected embed",
			`{"status":200,"result":[{"protected_embed":"https://dood.la/e/AbC123xY"}]}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"result[] with download_url",
			`{"status":200,"result":[{"download_url":"https://dood.to/d/AbC123xY"}]}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"files[] shape",
			`{"status":200,"files":[{"filecode":"AbC123xY"}]}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"single result object",
			`{"status":200,"result":{"filecode":"AbC123xY"}}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"bare result string",
			`{"status":200,"result":"AbC123xY"}`,
			"https://dood.to/e/AbC123xY",
		},
		{
			"quoted status",
			`{"status":"200","result":[{"filecode":"AbC123xY"}]}`,
			"https://dood.to/e/AbC123xY",
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
				if r.Method == http.MethodPost {
					if got := r.URL.Query().Get("api_key"); got != "test-key" {
						t.Errorf("upload URL api_key = %q, want test-key", got)
					}
					if err := r.ParseMultipartForm(1 << 20); err != nil {
						t.Errorf("parse multipart: %v", err)
					} else if got := r.FormValue("api_key"); got != "test-key" {
						t.Errorf("multipart api_key = %q, want test-key", got)
					}
				}
				fmt.Fprint(w, s.response)
			}))
			defer srv.Close()

			oldBase := doodStreamAPIBase
			doodStreamAPIBase = srv.URL
			defer func() { doodStreamAPIBase = oldBase }()

			dir := t.TempDir()
			filePath := filepath.Join(dir, "test.mp4")
			if err := os.WriteFile(filePath, []byte("fake video bytes"), 0o644); err != nil {
				t.Fatalf("write temp file: %v", err)
			}

			u := NewDoodStreamUploader("test-key")
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
