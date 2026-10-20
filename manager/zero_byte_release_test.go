package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestZeroByteReleaseFiresOnDeadRun pins B2's contract on the dead-run pass: when
// the stuck-recording watchdog clears a DEAD run (monitor gone, zero-byte
// current file past the grace window), the release hook must fire exactly ONCE
// with that channel's username. The DB row behind the hook stops saying
// status='recording' immediately — ReclaimChannels skips 'recording' rows, so
// without the release the cleared wedge keeps burning the slot until the node
// restarts (2026-10-17 sweep: 12/11/8 ghost slots on node-4/8/16).
func TestZeroByteReleaseFiresOnDeadRun(t *testing.T) {
	// Not "alice": TestReapStuckRecordingsDeadMonitorRemovesAgedZeroByteFile
	// runs first in the same package and shares the sweep's global
	// per-username cooldown map — a reused username would be skipped as
	// "handled recently" before reaching the release path.
	ch := newSweepChannel(t, t.TempDir(), "diana")
	path := ch.CurrentRecordingPath()

	var released []string
	m := &Manager{ReleaseHook: func(username string) {
		released = append(released, username)
	}}
	m.Channels.Store("diana", ch)

	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("reaped = %d, want 0 (a dead-run removal is not a restart)", got)
	}
	if len(released) != 1 || released[0] != "diana" {
		t.Errorf("release hook = %v, want exactly [diana]", released)
	}
	// The dead-run file itself is gone (B1's own contract, re-checked here so
	// this test is self-contained about WHY the slot freed).
	if _, err := os.Stat(path); err == nil {
		t.Errorf("dead-run zero-byte file survived the sweep: %v", err)
	}
}

// TestZeroByteReleaseNilHookSafe pins the seam's optional wiring: an unwired manager
// (release never configured — e.g. pooled mode off) must still clear the
// dead-run file without panicking.
func TestZeroByteReleaseNilHookSafe(t *testing.T) {
	ch := newSweepChannel(t, t.TempDir(), "belle")
	path := ch.CurrentRecordingPath()

	m := &Manager{}
	m.Channels.Store("belle", ch)
	m.ReapStuckRecordings()

	if _, err := os.Stat(path); err == nil {
		t.Errorf("dead-run zero-byte file survived a nil-hook sweep: %v", err)
	}
}

// TestZeroByteReleaseOnlyForDeadRuns pins the negative space: a file still
// inside the grace window, one that has data, a paused channel, and a third
// party's stray zero-byte file must never trigger a release — the hook means
// "this node just gave up a recording slot", which is only true for the
// dead-run pass.
func TestZeroByteReleaseOnlyForDeadRuns(t *testing.T) {
	dir := t.TempDir()

	fresh := newSweepChannelFresh(t, dir, "carol")
	grown := newSweepChannelWithSize(t, dir, "dan", 4096)
	paused := newSweepChannel(t, dir, "erin")
	paused.Config.IsPaused.Store(true)

	stray := filepath.Join(dir, "stray_video_file.mp4")
	if err := os.WriteFile(stray, nil, 0o644); err != nil {
		t.Fatalf("WriteFile stray: %v", err)
	}
	old := time.Now().Add(-2 * stuckRecordingGrace)
	if err := os.Chtimes(stray, old, old); err != nil {
		t.Fatalf("Chtimes stray: %v", err)
	}

	var released []string
	m := &Manager{ReleaseHook: func(username string) {
		released = append(released, username)
	}}
	m.Channels.Store("carol", fresh)
	m.Channels.Store("dan", grown)
	m.Channels.Store("erin", paused)
	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("reaped = %d, want 0 — none of these fixtures is a wedged run", got)
	}
	if len(released) != 0 {
		t.Errorf("release hook = %v, want empty (only dead-run passes release)", released)
	}
}
