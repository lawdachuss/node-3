package database

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRetentionSupabase mimics the Cloudflare/PostgREST proxy's ~8 KB URI
// limit: ANY request whose RequestURI exceeds maxURILen answers HTTP 414
// ("URI too long") — exactly what production returns when the id=in.(...)
// batch carries too many UUIDs (fleet sweep 2026-10-20: log retention was a
// silent no-op on nodes 1/4/8/12/15 because of this).
//
// GET  <table>?select=id&<field>=lt.<cutoff>&order=<field>.asc&limit=N
//      → the first N remaining old ids.
// DELETE <table>?id=in.(a,b,c)
//      → 414 when the URI is over the limit, otherwise removes those ids.
type fakeRetentionSupabase struct {
	mu        sync.Mutex
	rows      []string // remaining "old" row ids
	maxURILen int
	overLimit int // count of requests rejected with 414
}

func (f *fakeRetentionSupabase) handler(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.RequestURI()) > f.maxURILen {
		f.mu.Lock()
		f.overLimit++
		f.mu.Unlock()
		w.WriteHeader(http.StatusRequestURITooLong)
		fmt.Fprint(w, `{"message":"URI Too Long"}`)
		return
	}

	switch r.Method {
	case "GET":
		limit := 500
		fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
		f.mu.Lock()
		n := len(f.rows)
		if limit < n {
			n = limit
		}
		ids := make([]map[string]string, 0, n)
		for _, id := range f.rows[:n] {
			ids = append(ids, map[string]string{"id": id})
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ids)
	case "DELETE":
		v := r.URL.Query().Get("id")
		if !strings.HasPrefix(v, "in.(") || !strings.HasSuffix(v, ")") {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"message":"expected id=in.(...) filter"}`)
			return
		}
		inner := v[len("in.(") : len(v)-1]
		del := make(map[string]bool)
		for _, id := range strings.Split(inner, ",") {
			del[id] = true
		}
		f.mu.Lock()
		kept := f.rows[:0]
		for _, id := range f.rows {
			if !del[id] {
				kept = append(kept, id)
			}
		}
		f.rows = kept
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func newRetentionClient(t *testing.T, rows int, maxURILen int) (*Client, *fakeRetentionSupabase) {
	t.Helper()
	fake := &fakeRetentionSupabase{maxURILen: maxURILen}
	for i := 0; i < rows; i++ {
		// UUID-shaped ids: 36 chars, exactly what production sends.
		fake.rows = append(fake.rows, fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
	}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	return &Client{URL: srv.URL, APIKey: "test-key", client: srv.Client()}, fake
}

// TestRetentionDeleteBatchStaysUnderProxyURILimit is the 2026-10-20 regression
// guard: deleteRowsOlderThan used to send id=in.(<500 uuids>) — ~18 KB of
// query string — so every DELETE answered HTTP 414 and zero rows were ever
// pruned (only a WARN reached the node log). The delete batches must fit the
// proxy's ~8 KB URI limit while still pruning every old row.
func TestRetentionDeleteBatchStaysUnderProxyURILimit(t *testing.T) {
	const (
		oldRows    = 250 // more than one 100-id batch, fewer than one 500-id page
		maxURILen  = 8192
		cutoffDays = 30
	)
	c, fake := newRetentionClient(t, oldRows, maxURILen)
	cutoff := time.Now().UTC().Add(-cutoffDays * 24 * time.Hour)

	total, err := c.DeleteChannelLogsBefore(cutoff)
	if err != nil {
		t.Fatalf("DeleteChannelLogsBefore: %v", err)
	}
	if total != oldRows {
		t.Errorf("pruned %d rows, want %d — retention must page ids AND batch the deletes under the URI limit", total, oldRows)
	}
	if fake.overLimit != 0 {
		t.Errorf("%d request(s) exceeded the %d-byte URI limit (HTTP 414) — delete batch is still too large", fake.overLimit, maxURILen)
	}
	fake.mu.Lock()
	remaining := len(fake.rows)
	fake.mu.Unlock()
	if remaining != 0 {
		t.Errorf("%d rows left after prune, want 0", remaining)
	}
}

// TestRetentionDiskUsagePrunesToo pins the second telemetry table through the
// same shared helper (different time column, same batch shape).
func TestRetentionDiskUsagePrunesToo(t *testing.T) {
	const oldRows = 120
	c, fake := newRetentionClient(t, oldRows, 8192)
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)

	total, err := c.DeleteDiskUsageBefore(cutoff)
	if err != nil {
		t.Fatalf("DeleteDiskUsageBefore: %v", err)
	}
	if total != oldRows {
		t.Errorf("pruned %d rows, want %d", total, oldRows)
	}
	if fake.overLimit != 0 {
		t.Errorf("%d request(s) hit HTTP 414", fake.overLimit)
	}
}

// TestRetentionEmptyTableIsNoop: a healthy table completes with 0 and issues
// no DELETE at all.
func TestRetentionEmptyTableIsNoop(t *testing.T) {
	c, fake := newRetentionClient(t, 0, 8192)
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)

	total, err := c.DeleteChannelLogsBefore(cutoff)
	if err != nil {
		t.Fatalf("DeleteChannelLogsBefore: %v", err)
	}
	if total != 0 {
		t.Errorf("pruned %d rows, want 0", total)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.overLimit != 0 {
		t.Errorf("%d request(s) hit HTTP 414 on an empty table", fake.overLimit)
	}
}
