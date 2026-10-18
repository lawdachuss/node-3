package uploader

import "testing"

// TestKeyRingDeathHookReportsRotatedKey verifies the real-time death signal:
// rotating away from a labeled ring entry reports the abandoned key, its host,
// and the reason to the process-wide hook — while unlabeled rings (built
// directly in tests) stay silent.
func TestKeyRingDeathHookReportsRotatedKey(t *testing.T) {
	type report struct{ host, key, reason string }
	var got []report
	SetKeyDeathHook(func(host, key, reason string) {
		got = append(got, report{host, key, reason})
	})
	t.Cleanup(func() { SetKeyDeathHook(nil) })

	kr := newKeyRing("k1,k2")
	kr.host = "Vidara"
	kr.rotateReason("authentication rejected")
	if len(got) != 1 {
		t.Fatalf("hook reports = %d, want 1: %+v", len(got), got)
	}
	if got[0].host != "Vidara" || got[0].key != "k1" || got[0].reason != "authentication rejected" {
		t.Fatalf("got %+v, want {Vidara k1 authentication rejected}", got[0])
	}
	if kr.current() != "k2" {
		t.Fatalf("current = %q, want k2", kr.current())
	}

	kr.rotateReason("quota")
	if len(got) != 2 || got[1].key != "k2" {
		t.Fatalf("second rotate reports = %+v, want one report for k2", got)
	}

	// Unlabeled ring: rotate must not call the hook.
	bare := newKeyRing("a,b")
	bare.rotate()
	bare.rotateReason("authentication rejected")
	if len(got) != 2 {
		t.Fatalf("unlabeled ring reported %d times, want 0 new: %+v", len(got)-2, got)
	}
}

// TestKeyRingDeathHookNilIsSafe verifies the hook can be removed and that a
// nil hook never panics on rotate.
func TestKeyRingDeathHookNilIsSafe(t *testing.T) {
	SetKeyDeathHook(nil)
	kr := newKeyRing("k1,k2")
	kr.host = "VoeSX"
	kr.rotateReason("authentication rejected")
	if kr.current() != "k2" {
		t.Fatalf("current = %q, want k2", kr.current())
	}
}

// TestLabeledConstructorsTagHosts verifies each keyed uploader's ring carries
// its host label so death reports can be attributed.
func TestLabeledConstructorsTagHosts(t *testing.T) {
	t.Setenv("VOESX_API_KEY", "")
	t.Setenv("VIDARA_KEY", "")
	t.Setenv("VIDMOLY_KEY", "")
	t.Setenv("STREAMTAPE_CREDS", "")

	if h := NewVoeSXUploader("k").keys.host; h != "VoeSX" {
		t.Errorf("VoeSX host = %q", h)
	}
	if h := NewVidaraUploader("k").keys.host; h != "Vidara" {
		t.Errorf("Vidara host = %q", h)
	}
	if h := NewVidMolyUploader("k").keys.host; h != "VidMoly" {
		t.Errorf("VidMoly host = %q", h)
	}
	if h := NewStreamtapeUploader("l", "k").creds.host; h != "Streamtape" {
		t.Errorf("Streamtape host = %q", h)
	}
}
