package uploader

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestAnonMP4ChunkedUploadFlow simulates the site's current flow:
// /get-upload-node assigns a node, chunks stream to the node URL, and
// /get-video-node returns the final links.
func TestAnonMP4ChunkedUploadFlow(t *testing.T) {
	var (
		mu        sync.Mutex
		gotChunks = map[int]string{} // chunkIndex -> Content-Range header
	)

	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Any POST to the node URL is a chunk upload.
		q := r.URL.Query()
		idxStr := q.Get("chunkIndex")
		if idxStr == "" {
			t.Errorf("chunk request missing chunkIndex: %s", r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if q.Get("file_id") != "node-tok-123" {
			t.Errorf("chunk missing file_id=node_token, got %q", q.Get("file_id"))
		}
		if q.Get("filename") == "" {
			t.Errorf("chunk missing filename")
		}
		if got := r.Header.Get("Content-Type"); got != "application/octet-stream" {
			t.Errorf("chunk Content-Type = %q", got)
		}
		if got := r.Header.Get("X-Uploading-Mode"); got != "parallel" {
			t.Errorf("chunk X-Uploading-Mode = %q", got)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		var idx int
		fmt.Sscanf(idxStr, "%d", &idx)
		mu.Lock()
		gotChunks[idx] = r.Header.Get("Content-Range")
		mu.Unlock()
		// Mirror the real Worker: 201 Created with a plain-text cumulative
		// bytes-received ack ("0-<received-1>/<total>"), not JSON. Compute the
		// running total from the echoed Content-Range header; the final chunk
		// answers 200 once everything is assembled.
		var total int64
		if n, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %*d-%*d/%d", &total); err == nil && n == 1 && total > 0 {
			received := int64(idx) * anonmp4ChunkSize
			if received > total {
				received = total
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, "0-%d/%d", received-1, total)
			return
		}
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer nodeSrv.Close()

	siteSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get-upload-node":
			fmt.Fprintf(w, `{"status":true,"node_id":"n1","upload_url":%q,"node_token":"node-tok-123"}`, nodeSrv.URL)
		case "/get-video-node":
			fmt.Fprint(w, `{"success":true,"video_id":"v123","watch_url":"https://anonmp4.to/v/v123","embed_url":"https://anonmp4.to/embed/v123"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer siteSrv.Close()

	oldBase := anonmp4SiteBase
	anonmp4SiteBase = siteSrv.URL
	defer func() { anonmp4SiteBase = oldBase }()

	// 3 chunks: 2 MiB + 2 MiB + 100 B
	file := filepath.Join(t.TempDir(), "clip_2026-09-04_05-29-26.mp4")
	data := make([]byte, 2*anonmp4ChunkSize+100)
	if err := os.WriteFile(file, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var (
		progMu    sync.Mutex
		maxReport int64
	)
	up := NewAnonMP4Uploader()
	link, err := up.UploadWithProgress(file, func(host string, current, total int64) {
		progMu.Lock()
		if current > maxReport {
			maxReport = current
		}
		progMu.Unlock()
		if host != "AnonMP4" {
			t.Errorf("progress host = %q", host)
		}
	})
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	if link != "https://anonmp4.help/embed/v123" {
		t.Errorf("link = %q, want normalized embed URL (anonmp4.to -> anonmp4.help)", link)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gotChunks) != 3 {
		t.Fatalf("got %d chunk uploads (%v), want 3 (indices 1..3)", len(gotChunks), gotChunks)
	}
	for i := 1; i <= 3; i++ {
		if _, ok := gotChunks[i]; !ok {
			t.Errorf("chunk %d never uploaded", i)
		}
	}
	if gotChunks[1] != fmt.Sprintf("bytes 0-%d/%d", anonmp4ChunkSize-1, len(data)) {
		t.Errorf("chunk 1 Content-Range = %q", gotChunks[1])
	}
	if !strings.HasPrefix(gotChunks[3], "bytes 4194304-") {
		t.Errorf("chunk 3 Content-Range = %q (last chunk should start at 4 MiB)", gotChunks[3])
	}
	if maxReport != int64(len(data)) {
		t.Errorf("max progress = %d, want %d", maxReport, len(data))
	}
}

// TestAnonMP4Chunk201AckAccepted is the regression test for the seven-day
// fleet-wide AnonMP4 outage: the upload Worker answers 201 Created with a
// plain-text cumulative bytes-received ack ("0-2097151/6291456") for every
// non-final chunk. That response used to be parsed as an error
// ("chunk 1/903 failed: HTTP 201: 0-10485759/1893599352") and every upload
// died on its first chunk. The chunk must be accepted and the upload succeed.
func TestAnonMP4Chunk201AckAccepted(t *testing.T) {
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var total int64
		if n, _ := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", new(int), new(int), &total); n != 3 || total <= 0 {
			t.Errorf("bad Content-Range: %q", r.Header.Get("Content-Range"))
		}
		// 201 on every chunk — the real Worker only switches to 200 on the
		// final one, and 201 alone must never be a failure.
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "0-%d/%d", total-1, total)
	}))
	defer nodeSrv.Close()

	siteSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get-upload-node":
			fmt.Fprintf(w, `{"status":true,"node_id":"n1","upload_url":%q,"node_token":"tok"}`, nodeSrv.URL)
		case "/get-video-node":
			fmt.Fprint(w, `{"success":true,"video_id":"v1","embed_url":"https://anonmp4.to/embed/v1"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer siteSrv.Close()

	oldBase := anonmp4SiteBase
	anonmp4SiteBase = siteSrv.URL
	defer func() { anonmp4SiteBase = oldBase }()

	// One chunk only, so the test isolates the 201-ack handling.
	file := filepath.Join(t.TempDir(), "clip.mp4")
	if err := os.WriteFile(file, make([]byte, 1024), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewAnonMP4Uploader().Upload(file)
	if err != nil {
		t.Fatalf("201-ack chunk upload must succeed, got: %v", err)
	}
}

// TestAnonMP4NodeAssignmentFailure verifies a non-success node assignment is
// reported as an error (fail-fast path leaves it to the fallback chain).
func TestAnonMP4NodeAssignmentFailure(t *testing.T) {
	siteSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":false,"message":"invalid client_token"}`)
	}))
	defer siteSrv.Close()

	oldBase := anonmp4SiteBase
	anonmp4SiteBase = siteSrv.URL
	defer func() { anonmp4SiteBase = oldBase }()

	file := filepath.Join(t.TempDir(), "clip.mp4")
	if err := os.WriteFile(file, make([]byte, 1024), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewAnonMP4Uploader().Upload(file)
	if err == nil {
		t.Fatal("expected error on failed node assignment")
	}
	if !strings.Contains(err.Error(), "node assignment failed") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestNormalizeAnonMP4Link verifies that watch/embed links on any known anonmp4
// view domain are rewritten to the configured working domain, while preserving
// path/query, and that unknown hosts pass through untouched.
func TestNormalizeAnonMP4Link(t *testing.T) {
	oldBase := anonmp4ViewBase
	oldDomains := anonmp4ViewDomains
	defer func() {
		anonmp4ViewBase = oldBase
		anonmp4ViewDomains = oldDomains
	}()
	anonmp4ViewBase = "https://anonmp4.help"
	anonmp4ViewDomains = []string{"anonmp4.to", "anonmp4.help", "anonmp4.art"}

	cases := []struct{ in, want string }{
		{"https://anonmp4.art/embed/2fWaPS7AsdG7uLR", "https://anonmp4.help/embed/2fWaPS7AsdG7uLR"},
		{"https://anonmp4.art/v/2fWaPS7AsdG7uLR?seed=1", "https://anonmp4.help/v/2fWaPS7AsdG7uLR?seed=1"},
		{"https://anonmp4.to/embed/abc123", "https://anonmp4.help/embed/abc123"},
		{"https://anonmp4.help/embed/abc123", "https://anonmp4.help/embed/abc123"}, // idempotent
		{"http://anonmp4.art/embed/x", "https://anonmp4.help/embed/x"},
		{"https://other-host.com/v/xyz", "https://other-host.com/v/xyz"}, // unknown host passes through
		{"", ""},
		{"not a url", "not a url"},
	}
	for _, c := range cases {
		if got := normalizeAnonMP4Link(c.in); got != c.want {
			t.Errorf("normalizeAnonMP4Link(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestSetAnonMP4ViewBase verifies the env override replaces the default and
// that invalid values restore it.
func TestSetAnonMP4ViewBase(t *testing.T) {
	oldBase := anonmp4ViewBase
	defer func() { anonmp4ViewBase = oldBase }()

	SetAnonMP4ViewBase("https://anonmp4.xyz")
	if anonmp4ViewBase != "https://anonmp4.xyz" {
		t.Fatalf("SetAnonMP4ViewBase = %q, want https://anonmp4.xyz", anonmp4ViewBase)
	}
	SetAnonMP4ViewBase("   ")
	if anonmp4ViewBase != "https://anonmp4.help" {
		t.Fatalf("empty override must restore default, got %q", anonmp4ViewBase)
	}
	SetAnonMP4ViewBase("not-a-url")
	if anonmp4ViewBase != "https://anonmp4.help" {
		t.Fatalf("invalid override must restore default, got %q", anonmp4ViewBase)
	}
}
