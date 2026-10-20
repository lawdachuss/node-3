package uploader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// imgChestQuotaBody403 is the exact live rejection ImgChest returns once the
// shared account's storage quota is blown (captured across the fleet
// 2026-10-20: "You've used 42.4 GB of your 10 GB storage" on 3+ nodes).
const imgChestQuotaBody403 = `{"error":"You've used 42.4 GB of your 10 GB storage"}`

// TestImgChestStorageFullClassifier pins the H3 matcher against the live body
// and the look-alikes it must NOT classify (a plain 403 or an auth failure
// must never cost the host a day-long fleet disable).
func TestImgChestStorageFullClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"live quota body", fmt.Errorf("imgchest: HTTP 403: %s", imgChestQuotaBody403), true},
		{"quota via API-error path", fmt.Errorf("imgchest: API error: You've used 42.4 GB of your 10 GB storage"), true},
		{"plain 403", fmt.Errorf("imgchest: HTTP 403: Forbidden"), false},
		{"auth failure", fmt.Errorf("imgchest: HTTP 401: unauthorized"), false},
		{"rate limit wording", fmt.Errorf("imgchest: HTTP 429: rate limit reached"), false},
		{"generic storage word alone", fmt.Errorf("imgchest: HTTP 500: storage backend unavailable"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isImgChestStorageFull(tc.err); got != tc.want {
			t.Errorf("%s: isImgChestStorageFull(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// newImgChestQuotaServer returns a fake ImgChest that answers every request
// with the given status/body and counts requests.
func newImgChestQuotaServer(t *testing.T, status int, body string) (*httptest.Server, *int64) {
	t.Helper()
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// newImgChestTestUploader points imgChestAPIBase at the fake and builds a
// MultiImageUploader whose only configured host is ImgChest.
func newImgChestTestUploader(t *testing.T, srv *httptest.Server) *MultiImageUploader {
	t.Helper()
	oldBase := imgChestAPIBase
	imgChestAPIBase = srv.URL
	t.Cleanup(func() { imgChestAPIBase = oldBase })
	return &MultiImageUploader{
		imgchest: &ImgChestUploader{client: srv.Client(), token: "test-token"},
	}
}

// TestImgChestStorageFullDisablesHostFleetWide is the H3 regression guard: the
// first quota rejection must (1) fail fast after a single request and (2)
// publish a ~24h disable locally (timed + fleet cache), after which further
// calls skip the host WITHOUT touching the server — the pre-fix behavior was
// that all 18 nodes re-discovered the dead quota once per session.
func TestImgChestStorageFullDisablesHostFleetWide(t *testing.T) {
	defer clearTimedDisablesForTest()

	srv, hits := newImgChestQuotaServer(t, http.StatusForbidden, imgChestQuotaBody403)
	m := newImgChestTestUploader(t, srv)

	_, err := m.tryImgChest(imgbbTempImage(t))
	if err == nil {
		t.Fatal("expected the quota rejection to surface as an error")
	}
	if !isImgChestStorageFull(err) {
		t.Errorf("error should classify as storage-full, got: %v", err)
	}
	if got := atomic.LoadInt64(hits); got != 1 {
		t.Errorf("quota 403 is fail-fast — server saw %d requests, want 1", got)
	}

	if !isHostTimedOut("ImgChest") {
		t.Fatal("ImgChest must be timed-disabled after a storage-quota rejection")
	}
	if remaining := time.Until(hostTimedOutUntil("ImgChest")); remaining < 23*time.Hour {
		t.Errorf("timed disable should be ~24h, got %s", remaining)
	}
	if !isFleetHostBackedOff("ImgChest") {
		t.Fatal("the disable must also enter the fleet-wide backoff cache so peers skip the host")
	}

	// Second call: skipped without a request and with a clear message.
	_, err = m.tryImgChest(imgbbTempImage(t))
	if err == nil || !strings.Contains(err.Error(), "skipped") {
		t.Errorf("second call should be skipped with a clear error, got: %v", err)
	}
	if got := atomic.LoadInt64(hits); got != 1 {
		t.Errorf("a backed-off ImgChest must not be contacted — server saw %d requests, want 1", got)
	}
}

// TestImgChestPlain403DoesNotDisableHost is the over-match guard: a 403 that
// is NOT the storage quota (site block / maintenance) must fail the attempt
// but must NOT disable the host for the fleet for a day.
func TestImgChestPlain403DoesNotDisableHost(t *testing.T) {
	defer clearTimedDisablesForTest()

	srv, _ := newImgChestQuotaServer(t, http.StatusForbidden, `{"error":"Forbidden"}`)
	m := newImgChestTestUploader(t, srv)

	_, err := m.tryImgChest(imgbbTempImage(t))
	if err == nil {
		t.Fatal("expected the 403 to surface as an error")
	}
	if isImgChestStorageFull(err) {
		t.Errorf("a plain 403 must not classify as storage-full, got: %v", err)
	}
	if isHostTimedOut("ImgChest") || isFleetHostBackedOff("ImgChest") {
		t.Error("a plain 403 must not disable ImgChest fleet-wide")
	}
}

// TestImgChestUnconfiguredTokenIsNotAnHTTPCall: no token → the preset error,
// zero requests, no disable.
func TestImgChestUnconfiguredTokenIsNotAnHTTPCall(t *testing.T) {
	defer clearTimedDisablesForTest()

	srv, hits := newImgChestQuotaServer(t, http.StatusForbidden, imgChestQuotaBody403)
	m := newImgChestTestUploader(t, srv)
	m.imgchest.token = ""

	_, err := m.tryImgChest(imgbbTempImage(t))
	if err == nil || !strings.Contains(err.Error(), "IMGCHEST_TOKEN not set") {
		t.Errorf("want the not-configured error, got: %v", err)
	}
	if got := atomic.LoadInt64(hits); got != 0 {
		t.Errorf("server saw %d requests with no token configured, want 0", got)
	}
	if isHostTimedOut("ImgChest") {
		t.Error("a missing token must not disable the host")
	}
}
