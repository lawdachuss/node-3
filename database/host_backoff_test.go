package database

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHostBackoffDB serves the upload_host_backoffs collection and records the
// request headers, so the merge-duplicates (upsert) preference is pinned.
type fakeHostBackoffDB struct {
	mu       sync.Mutex
	reqs     []reqRecord
	prefers  []string
	rows     string
	respCode int
}

func (f *fakeHostBackoffDB) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.reqs = append(f.reqs, reqRecord{method: r.Method, path: r.URL.RequestURI(), body: string(body)})
	f.prefers = append(f.prefers, r.Header.Get("Prefer"))
	code, rows := f.respCode, f.rows
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if code != 0 {
		w.WriteHeader(code)
		fmt.Fprint(w, rows)
		return
	}
	if rows == "" {
		rows = "[]"
	}
	fmt.Fprint(w, rows)
}

func newHostBackoffTestClient(t *testing.T, fake *fakeHostBackoffDB) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return &Client{URL: srv.URL, APIKey: "test-key", client: srv.Client()}
}

// TestGetHostBackoffsParsesRows verifies the read path, including the two rows
// that must be ignored: a blank host (would skip nothing but pollutes the cache)
// and an unparseable timestamp (must not become a zero time, which would read as
// "long expired" on some clocks and as "backed off" on others).
func TestGetHostBackoffsParsesRows(t *testing.T) {
	fake := &fakeHostBackoffDB{rows: `[
		{"host":"VoeSX","backoff_until":"2026-09-22T18:00:00Z","reason":"daily api limit"},
		{"host":"Vidara","backoff_until":"2026-09-22T19:30:00Z"},
		{"host":"","backoff_until":"2026-09-22T20:00:00Z"},
		{"host":"Broken","backoff_until":"not-a-timestamp"}
	]`}
	c := newHostBackoffTestClient(t, fake)

	got, err := c.GetHostBackoffs()
	if err != nil {
		t.Fatalf("GetHostBackoffs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("parsed %d backoffs, want 2 (blank host + bad stamp dropped): %v", len(got), got)
	}
	want, _ := time.Parse(time.RFC3339, "2026-09-22T18:00:00Z")
	if !got["VoeSX"].Equal(want) {
		t.Errorf("VoeSX expiry = %v, want %v", got["VoeSX"], want)
	}
	if _, ok := got["Vidara"]; !ok {
		t.Error("Vidara row missing")
	}
	if _, ok := got[""]; ok {
		t.Error("blank host must be dropped")
	}
	if _, ok := got["Broken"]; ok {
		t.Error("unparseable stamp must be dropped rather than stored as zero time")
	}
}

// TestGetHostBackoffsTableMissingIsClassified verifies the degradation path: a
// project without the migration returns PGRST205 and the caller must be able to
// tell that apart from a real failure.
func TestGetHostBackoffsTableMissingIsClassified(t *testing.T) {
	fake := &fakeHostBackoffDB{
		respCode: http.StatusNotFound,
		rows:     `{"code":"PGRST205","message":"Could not find the table 'public.upload_host_backoffs' in the schema cache"}`,
	}
	c := newHostBackoffTestClient(t, fake)

	_, err := c.GetHostBackoffs()
	if err == nil {
		t.Fatal("expected an error for a missing table")
	}
	if !HostBackoffTableMissing(err) {
		t.Fatalf("PGRST205 for upload_host_backoffs must be classified as table-missing: %v", err)
	}
}

// TestSetHostBackoffUpsertsRow pins the upsert: POST to the collection with the
// merge-duplicates preference (so the host primary key keeps one row per host),
// and a body carrying the expiry, reason and reporting node.
func TestSetHostBackoffUpsertsRow(t *testing.T) {
	fake := &fakeHostBackoffDB{rows: `[{"host":"VoeSX"}]`}
	c := newHostBackoffTestClient(t, fake)

	until := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	if err := c.SetHostBackoff("VoeSX", until, "daily api limit", "node-11"); err != nil {
		t.Fatalf("SetHostBackoff: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.reqs) != 1 {
		t.Fatalf("request count = %d, want 1", len(fake.reqs))
	}
	rec := fake.reqs[0]
	if rec.method != "POST" {
		t.Errorf("method = %s, want POST", rec.method)
	}
	if !strings.HasPrefix(rec.path, "/rest/v1/upload_host_backoffs") {
		t.Errorf("unexpected path %q", rec.path)
	}
	if !strings.Contains(fake.prefers[0], "resolution=merge-duplicates") {
		t.Errorf("Prefer = %q, want an upsert (merge-duplicates)", fake.prefers[0])
	}
	for _, want := range []string{
		`"host":"VoeSX"`,
		`"backoff_until":"2026-09-22T18:00:00Z"`,
		`"reason":"daily api limit"`,
		`"updated_by":"node-11"`,
	} {
		if !strings.Contains(rec.body, want) {
			t.Errorf("body missing %s: %s", want, rec.body)
		}
	}
}

// TestSetHostBackoffNoopOnEmptyInput: nothing to report means no request, so a
// caller that lost its host name cannot write a row that skips nothing.
func TestSetHostBackoffNoopOnEmptyInput(t *testing.T) {
	fake := &fakeHostBackoffDB{}
	c := newHostBackoffTestClient(t, fake)

	if err := c.SetHostBackoff("  ", time.Now().Add(time.Hour), "why", "node-1"); err != nil {
		t.Fatalf("SetHostBackoff(blank host): %v", err)
	}
	if err := c.SetHostBackoff("VoeSX", time.Time{}, "why", "node-1"); err != nil {
		t.Fatalf("SetHostBackoff(zero time): %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.reqs) != 0 {
		t.Fatalf("request count = %d, want 0", len(fake.reqs))
	}
}

// TestHostBackoffTableMissingClassifier pins the taxonomy: only a schema-cache
// miss naming THIS table counts.  A generic failure must still surface so a real
// Supabase outage is not mistaken for "migration not applied" and silently
// ignored forever.
func TestHostBackoffTableMissingClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "PGRST205 for the backoff table",
			err:  fmt.Errorf("HTTP 404: {\"code\":\"PGRST205\",\"message\":\"Could not find the table 'public.upload_host_backoffs' in the schema cache\"}"),
			want: true,
		},
		{
			name: "postgres undefined_table",
			err:  fmt.Errorf("HTTP 400: {\"code\":\"42P01\",\"message\":\"relation \\\"public.upload_host_backoffs\\\" does not exist\"}"),
			want: true,
		},
		{
			name: "PGRST205 for a different table",
			err:  fmt.Errorf("HTTP 404: {\"code\":\"PGRST205\",\"message\":\"Could not find the table 'public.recordings' in the schema cache\"}"),
			want: false,
		},
		{name: "auth failure", err: fmt.Errorf("HTTP 401: unauthorized"), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HostBackoffTableMissing(tc.err); got != tc.want {
				t.Errorf("HostBackoffTableMissing(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
