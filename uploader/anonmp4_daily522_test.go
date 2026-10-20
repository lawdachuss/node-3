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

// anonmp4522Body is the exact live rejection: Cloudflare answering for a dead
// origin ("error code: 522" = the origin never responded to the edge in time).
const anonmp4522Body = `error code: 522`

// TestAnonMP4OriginDownClassifier pins the C2 matcher to the 522 class only —
// a 500/503/504 or a local file error must never count toward the strike that
// disables the host for the rest of the UTC day.
func TestAnonMP4OriginDownClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"live node-assignment shape", fmt.Errorf("anonmp4: node assignment: HTTP 522: %s", anonmp4522Body), true},
		{"522 from the chunk stage", fmt.Errorf("anonmp4: chunk upload: HTTP 522: %s", anonmp4522Body), true},
		{"522 from finalize", fmt.Errorf("anonmp4: fetch video info: HTTP 522: %s", anonmp4522Body), true},
		{"HTTP 500", fmt.Errorf("anonmp4: node assignment: HTTP 500: internal server error"), false},
		{"HTTP 503", fmt.Errorf("anonmp4: node assignment: HTTP 503: service unavailable"), false},
		{"HTTP 504", fmt.Errorf("anonmp4: node assignment: HTTP 504: gateway timeout"), false},
		{"local stat failure", fmt.Errorf("anonmp4: stat file: open nope.mp4: The system cannot find the file specified."), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isAnonMP4OriginDown(tc.err); got != tc.want {
			t.Errorf("%s: isAnonMP4OriginDown(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestAnonMP4FiveTwentyTwoStrikeCounter drives the day-keyed counter: strikes
// 1-2 change nothing, strike 3 disables exactly once until the next UTC
// midnight, strike 4 does not re-fire, and a new UTC day starts clean.
func TestAnonMP4FiveTwentyTwoStrikeCounter(t *testing.T) {
	defer clearTimedDisablesForTest()
	resetAnonMP4522StrikesForTest()
	defer resetAnonMP4522StrikesForTest()

	if _, disabled := recordAnonMP4OriginDown(); disabled {
		t.Error("strike 1 must not disable")
	}
	if _, disabled := recordAnonMP4OriginDown(); disabled {
		t.Error("strike 2 must not disable (Review-Focus rule 3)")
	}
	if isHostTimedOut("AnonMP4") || isFleetHostBackedOff("AnonMP4") {
		t.Fatal("2 strikes must leave the host enabled")
	}

	strikes, disabled := recordAnonMP4OriginDown()
	if strikes != 3 || !disabled {
		t.Fatalf("strike 3: got (%d, %v), want (3, true)", strikes, disabled)
	}
	if !isHostTimedOut("AnonMP4") {
		t.Fatal("AnonMP4 must be timed-disabled after the 3rd 522 strike")
	}
	if !isFleetHostBackedOff("AnonMP4") {
		t.Fatal("the disable must enter the fleet-wide backoff cache so peers skip the host")
	}
	// Expiry must be the next UTC midnight, not a flat 24h (a fresh day's
	// origin may be fine — the lease must not carry the disable past it).
	wantExpiry := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	if diff := wantExpiry.Sub(hostTimedOutUntil("AnonMP4")); diff < -time.Minute || diff > time.Minute {
		t.Errorf("disable expiry should be the next UTC midnight (%s), got %s (off by %s)",
			wantExpiry.Format(time.RFC3339), hostTimedOutUntil("AnonMP4").Format(time.RFC3339), diff)
	}

	if _, disabled := recordAnonMP4OriginDown(); disabled {
		t.Error("strike 4 must not fire the disable a second time in the same UTC day")
	}

	// A new UTC day starts with a clean slate.
	anonmp4StrikeMu.Lock()
	anonmp4StrikeDay = "2000-01-01"
	anonmp4StrikeMu.Unlock()
	strikes, disabled = recordAnonMP4OriginDown()
	if strikes != 1 || disabled {
		t.Errorf("after the day rolls: got (%d, %v), want (1, false)", strikes, disabled)
	}
}

// TestAnonMP4FiveTwentyTwoWiringDisablesOnThirdStrike is the integration
// guard: two strikes already recorded, then a real upload attempt hits the
// live 522 shape → the 3rd strike disables the host AND bails immediately
// (no 4th attempt against a dead origin).  A later UploadSelected must skip
// AnonMP4 entirely without another request.
func TestAnonMP4FiveTwentyTwoWiringDisablesOnThirdStrike(t *testing.T) {
	defer clearTimedDisablesForTest()
	resetAnonMP4522StrikesForTest()
	defer resetAnonMP4522StrikesForTest()

	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(522) // Cloudflare-specific: origin connection timed out
		fmt.Fprint(w, anonmp4522Body)
	}))
	defer srv.Close()
	oldBase := anonmp4SiteBase
	anonmp4SiteBase = srv.URL
	defer func() { anonmp4SiteBase = oldBase }()

	// Strikes 1-2 arrived earlier today (from other files / peer nodes).
	recordAnonMP4OriginDown()
	recordAnonMP4OriginDown()

	u := &AnonMP4Uploader{client: srv.Client()}
	_, err := u.UploadWithProgress(tempVideoFile(t), nil)
	if err == nil {
		t.Fatal("expected the 522 to surface as an error")
	}
	if !strings.Contains(err.Error(), "HTTP 522") {
		t.Errorf("terminal error must keep AnonMP4's own 522 wording, got: %v", err)
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("the 3rd strike must bail without more attempts — server saw %d requests, want 1", got)
	}
	if !isHostTimedOut("AnonMP4") || !isFleetHostBackedOff("AnonMP4") {
		t.Fatal("AnonMP4 must be disabled (timed + fleet) after the 3rd strike")
	}

	// The multi-host layer must now skip the host without contacting it.
	m := NewMultiHostUploader("", "", "", "", "", "", "test-key", &nilLogger{})
	results := m.UploadSelected(tempVideoFile(t), []string{"AnonMP4"})
	for _, r := range results {
		if r.Host == "AnonMP4" && r.Error == nil && r.DownloadLink != "" {
			t.Error("a disabled AnonMP4 must not produce a result")
		}
	}
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("a disabled AnonMP4 must be skipped without a request — server saw %d total, want 1", got)
	}
}
