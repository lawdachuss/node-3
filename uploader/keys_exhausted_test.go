package uploader

import (
	"errors"
	"testing"
	"time"
)

// TestIsKeysExhausted pins the predicate to the shared terminal wording
// key-rotating uploaders emit, and confirms it does not fire on ordinary
// transient failures (which the retry loop should keep handling).
func TestIsKeysExhausted(t *testing.T) {
	yes := []string{
		"VOE.sx upload failed: all keys exhausted",
		"Vidara upload failed: all keys exhausted",
	}
	for _, m := range yes {
		if !isKeysExhausted(errors.New(m)) {
			t.Errorf("isKeysExhausted(%q) = false, want true", m)
		}
	}

	no := []string{
		"upload failed with status 500: internal server error",
		"upload failed: rate limited (HTTP 429)",
		"upload failed: daily upload limit reached",
		"context deadline exceeded",
	}
	for _, m := range no {
		if isKeysExhausted(errors.New(m)) {
			t.Errorf("isKeysExhausted(%q) = true, want false", m)
		}
	}
	if isKeysExhausted(nil) {
		t.Error("isKeysExhausted(nil) = true, want false")
	}
}

// TestKeysExhaustedDisablesHost verifies a host whose keys are all dead is put
// on a timed cooldown AND disabled for the run, so every following file stops
// re-walking the dead key ring.
func TestKeysExhaustedDisablesHost(t *testing.T) {
	clearTimedDisablesForTest()
	t.Cleanup(clearTimedDisablesForTest)

	u := &MultiHostUploader{
		log: &nilLogger{},
		hosts: map[string]uploaderFunc{
			"Vidara": func(_ string, _ ProgressFunc) (string, error) {
				return "", errors.New("Vidara upload failed: all keys exhausted")
			},
		},
	}

	results := u.UploadSelected("f.mp4", []string{"Vidara"})
	if len(results) != 1 || results[0].Error == nil {
		t.Fatalf("expected a failure result, got %#v", results)
	}
	if !u.isHostDisabled("Vidara") {
		t.Error("host not disabled after all keys were exhausted")
	}
	if !isHostTimedOut("Vidara") {
		t.Error("host not on a timed cooldown after all keys were exhausted")
	}
	if remaining := time.Until(hostTimedOutUntil("Vidara")); remaining > keysExhaustedCooldown {
		t.Errorf("timed cooldown %s exceeds keysExhaustedCooldown %s", remaining, keysExhaustedCooldown)
	}
}

// TestKeysExhaustedSkipsNextFile is the churn guard: once the keys are known
// dead, the NEXT file must not invoke the host at all.  This is what removes
// the fleet's per-file re-walk of every dead key.
func TestKeysExhaustedSkipsNextFile(t *testing.T) {
	clearTimedDisablesForTest()
	t.Cleanup(clearTimedDisablesForTest)

	calls := 0
	u := &MultiHostUploader{
		log: &nilLogger{},
		hosts: map[string]uploaderFunc{
			"Vidara": func(_ string, _ ProgressFunc) (string, error) {
				calls++
				return "", errors.New("Vidara upload failed: all keys exhausted")
			},
		},
	}

	u.UploadSelected("f1.mp4", []string{"Vidara"})
	u.UploadSelected("f2.mp4", []string{"Vidara"})

	if calls != 1 {
		t.Errorf("host invoked %d times, want 1 (the second file must be skipped inside the cooldown)", calls)
	}
}
