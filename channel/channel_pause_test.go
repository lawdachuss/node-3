package channel

import (
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
)

// TestPauseReasonStickyManual verifies the core rule that protects the user's
// explicit UI pause: a manual pause reason is sticky — automatic re-pauses
// (session boundary, handoff) never overwrite it, so automatic resume paths
// can always tell a user pause apart and leave it alone.
func TestPauseReasonStickyManual(t *testing.T) {
	ch := &Channel{}

	// An automatic pause records its reason normally.
	ch.setPauseReason(entity.PauseReasonBoundary)
	if got := ch.PauseReason(); got != entity.PauseReasonBoundary {
		t.Fatalf("PauseReason after boundary = %q, want %q", got, entity.PauseReasonBoundary)
	}

	// The user pauses — reason becomes manual.
	ch.setPauseReason(entity.PauseReasonManual)
	if got := ch.PauseReason(); got != entity.PauseReasonManual {
		t.Fatalf("PauseReason after manual = %q, want %q", got, entity.PauseReasonManual)
	}

	// Automatic re-pauses (e.g. the next session boundary pausing everything,
	// or a handoff Stop) must NOT downgrade the manual reason.
	ch.setPauseReason(entity.PauseReasonBoundary)
	if got := ch.PauseReason(); got != entity.PauseReasonManual {
		t.Fatalf("boundary re-pause overwrote manual reason: got %q, want %q", got, entity.PauseReasonManual)
	}
	ch.setPauseReason(entity.PauseReasonHandoff)
	if got := ch.PauseReason(); got != entity.PauseReasonManual {
		t.Fatalf("handoff re-pause overwrote manual reason: got %q, want %q", got, entity.PauseReasonManual)
	}

	// Resume clears the reason entirely.
	ch.clearPauseReason()
	if got := ch.PauseReason(); got != "" {
		t.Fatalf("PauseReason after resume = %q, want empty", got)
	}

	// A fresh automatic pause records normally again.
	ch.setPauseReason(entity.PauseReasonHandoff)
	if got := ch.PauseReason(); got != entity.PauseReasonHandoff {
		t.Fatalf("PauseReason after fresh handoff = %q, want %q", got, entity.PauseReasonHandoff)
	}
}
