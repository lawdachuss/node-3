package uploader

import (
	"errors"
	"testing"
	"time"
)

// TestFleetHostBackoffSkipsHost is the whole point of the shared table: when a
// PEER node reported a host exhausted on the shared credentials, this node must
// not spend its own request rediscovering that. Before the fleet cache, every
// node attempted the host independently.
func TestFleetHostBackoffSkipsHost(t *testing.T) {
	defer clearTimedDisablesForTest()

	SetFleetHostBackoffs(map[string]time.Time{"VoeSX": time.Now().Add(time.Hour)})

	calls := 0
	m := &MultiHostUploader{
		hosts: map[string]uploaderFunc{
			"VoeSX": func(_ string, _ ProgressFunc) (string, error) {
				calls++
				return "https://voe.sx/x", nil
			},
		},
		log: &nilLogger{},
	}

	results := m.UploadSelected("f.mp4", []string{"VoeSX"})
	if calls != 0 {
		t.Fatalf("host inside a fleet-wide backoff must not be attempted, got %d call(s)", calls)
	}
	if len(results) != 0 {
		t.Fatalf("a fleet-backed-off host must be skipped entirely, got %+v", results)
	}

	// Expiry passes -> the peer's backoff must lift on its own.
	SetFleetHostBackoffs(map[string]time.Time{"VoeSX": time.Now().Add(-time.Minute)})
	if isFleetHostBackedOff("VoeSX") {
		t.Fatal("an expired fleet backoff must not still skip the host")
	}
	_ = m.UploadSelected("f.mp4", []string{"VoeSX"})
	if calls != 1 {
		t.Fatalf("host should be attempted again once the fleet backoff expired, got %d call(s)", calls)
	}
}

// TestDisableHostForPublishesFleetWide verifies the report side: when this node
// discovers a shared-credential cap, the backoff must reach the fleet sink (with
// the reason, so operators can see why) and apply locally immediately.
func TestDisableHostForPublishesFleetWide(t *testing.T) {
	defer clearTimedDisablesForTest()

	type report struct {
		host   string
		until  time.Time
		reason string
	}
	reports := make(chan report, 1)
	SetPublishHostBackoff(func(host string, until time.Time, reason string) {
		reports <- report{host, until, reason}
	})
	defer SetPublishHostBackoff(nil)

	before := time.Now()
	disableHostFor("VoeSX", 24*time.Hour, "daily upload limit reached")

	select {
	case got := <-reports:
		if got.host != "VoeSX" {
			t.Errorf("published host = %q, want VoeSX", got.host)
		}
		if got.reason != "daily upload limit reached" {
			t.Errorf("published reason = %q", got.reason)
		}
		if d := got.until.Sub(before); d < 23*time.Hour || d > 25*time.Hour {
			t.Errorf("published expiry is %v from now, want ~24h", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disableHostFor did not publish the backoff to the fleet sink")
	}

	if !isHostTimedOut("VoeSX") {
		t.Fatal("host should be locally timed out")
	}
	if !isFleetHostBackedOff("VoeSX") {
		t.Fatal("host should be in the local fleet cache immediately after reporting")
	}
}

// TestFleetBackoffsReplacedNotMerged pins the refresh semantics: a fresh read of
// the shared table replaces the cache, so a backoff that another node lifted (or
// that this node reported by mistake) cannot linger in memory forever.
func TestFleetBackoffsReplacedNotMerged(t *testing.T) {
	defer clearTimedDisablesForTest()

	SetFleetHostBackoffs(map[string]time.Time{
		"VoeSX":  time.Now().Add(time.Hour),
		"Vidara": time.Now().Add(time.Hour),
	})
	if !isFleetHostBackedOff("Vidara") {
		t.Fatal("Vidara should be backed off")
	}

	SetFleetHostBackoffs(map[string]time.Time{"VoeSX": time.Now().Add(time.Hour)})
	if isFleetHostBackedOff("Vidara") {
		t.Fatal("a host dropped from the shared table must stop being backed off")
	}
	if !isFleetHostBackedOff("VoeSX") {
		t.Fatal("VoeSX should still be backed off")
	}
}

// TestNoFleetBackoffByDefault guards the pre-migration / no-Supabase path: with
// nothing published and nothing read, no host may be skipped, so the feature can
// never silently stop uploads on a deployment without the table.
func TestNoFleetBackoffByDefault(t *testing.T) {
	defer clearTimedDisablesForTest()
	SetFleetHostBackoffs(nil)

	if isFleetHostBackedOff("VoeSX") {
		t.Fatal("no table data must mean no fleet backoff")
	}
	if !fleetHostBackoffUntil("VoeSX").IsZero() {
		t.Fatal("fleetHostBackoffUntil should be zero when nothing is published")
	}
}

// TestFleetBackoffSurvivesUploaderError keeps the fleet skip independent of the
// per-instance fullSend/disabled state: a host backed off by a peer must be
// skipped even when the uploader itself has never seen that host fail.
func TestFleetBackoffSurvivesUploaderError(t *testing.T) {
	defer clearTimedDisablesForTest()
	SetFleetHostBackoffs(map[string]time.Time{"Vidara": time.Now().Add(time.Hour)})

	calls := 0
	m := &MultiHostUploader{
		hosts: map[string]uploaderFunc{
			"Vidara": func(_ string, _ ProgressFunc) (string, error) {
				calls++
				return "", errors.New("should not be called")
			},
		},
		log: &nilLogger{},
	}
	_ = m.UploadSelectedPriority("f.mp4", []string{"Vidara"}, "")
	if calls != 0 {
		t.Fatalf("priority path must honour the fleet backoff, got %d call(s)", calls)
	}
}
