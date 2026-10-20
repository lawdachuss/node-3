package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/entity"
)

// TestZeroByteReapDue pins the watchdog's threshold rule: only a file that has
// received NOTHING and has been empty for at least the grace window is a wedged
// recording. A file with data, or a freshly created one, must never be reaped.
func TestZeroByteReapDue(t *testing.T) {
	now := time.Date(2026, 9, 22, 5, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		size int64
		mod  time.Time
		want bool
	}{
		{"empty past the grace window", 0, now.Add(-stuckRecordingGrace - time.Minute), true},
		{"empty exactly at the grace window", 0, now.Add(-stuckRecordingGrace), true},
		{"empty but still filling (fresh)", 0, now.Add(-30 * time.Second), false},
		{"empty just inside the grace window", 0, now.Add(-stuckRecordingGrace + time.Second), false},
		{"has data, however old", 4096, now.Add(-3 * time.Hour), false},
		{"has data and fresh", 1, now.Add(-time.Second), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := zeroByteReapDue(c.size, c.mod, now); got != c.want {
				t.Errorf("zeroByteReapDue(%d bytes, %s) = %v, want %v", c.size, now.Sub(c.mod), got, c.want)
			}
		})
	}
}

// TestReapCooldownElapsed verifies a reaped channel is left alone until the
// cooldown passes, so a stream that is genuinely gone falls back to the normal
// retry cadence instead of being restarted on every sweep.
func TestReapCooldownElapsed(t *testing.T) {
	now := time.Date(2026, 9, 22, 5, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		last time.Time
		seen bool
		want bool
	}{
		{"never reaped", time.Time{}, false, true},
		{"reaped long ago", now.Add(-stuckRecordingCooldown - time.Minute), true, true},
		{"reaped exactly at the cooldown", now.Add(-stuckRecordingCooldown), true, true},
		{"reaped a minute ago", now.Add(-time.Minute), true, false},
		{"reaped just inside the cooldown", now.Add(-stuckRecordingCooldown + time.Second), true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := reapCooldownElapsed(c.last, now, c.seen); got != c.want {
				t.Errorf("reapCooldownElapsed(seen=%v, age=%s) = %v, want %v", c.seen, now.Sub(c.last), got, c.want)
			}
		})
	}
}

// TestReapStuckRecordingsEmptyManagerIsSafe guards the sweep against a manager
// with no channels (and against running before any channel exists).
func TestReapStuckRecordingsEmptyManagerIsSafe(t *testing.T) {
	m := &Manager{}
	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("ReapStuckRecordings on an empty manager = %d, want 0", got)
	}
}

// newSweepChannel returns a channel with a real, open, empty recording file aged
// past the grace window — the exact shape the watchdog exists to clear.
func newSweepChannel(t *testing.T, dir, username string) *channel.Channel {
	t.Helper()
	ch := channel.New(&entity.ChannelConfig{
		Username: username,
		Site:     "chaturbate",
		Pattern:  filepath.Join(dir, "{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}"),
	})
	// RecordStream sets FileExt from the playlist before calling NextFile; the
	// path accessor needs it to know the on-disk suffix.
	ch.FileExt = ".mp4"
	if err := ch.NextFile(ch.FileExt); err != nil {
		t.Fatalf("NextFile: %v", err)
	}
	path := ch.CurrentRecordingPath()
	old := time.Now().Add(-2 * stuckRecordingGrace)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	// Close the handle before the test's TempDir cleanup runs: on Windows an
	// open file cannot be unlinked, which fails the test at teardown.
	t.Cleanup(func() { _ = ch.Cleanup(channel.CloseQueue) })
	return ch
}

// TestReapStuckRecordingsDeadMonitorRemovesAgedZeroByteFile verifies the
// dead-run pass. A channel whose monitor already exited (ReapStuckRecording
// reports false) but whose zero-byte current file is past the grace window has
// that file REMOVED: it is the only owner left — the orphan sweep skips current
// paths, and cleanupLocked only runs when the channel closes. `reaped` must
// still report 0: the sweep removed a file, it did not restart anything, and it
// must never resurrect a torn-down or claim restarts it did not perform.
func TestReapStuckRecordingsDeadMonitorRemovesAgedZeroByteFile(t *testing.T) {
	ch := newSweepChannel(t, t.TempDir(), "alice")
	path := ch.CurrentRecordingPath()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if st.Size() != 0 {
		t.Fatalf("test fixture file is %d bytes, want 0", st.Size())
	}
	if !zeroByteReapDue(st.Size(), st.ModTime(), time.Now()) {
		t.Fatal("fixture is not past the reap grace window; the sweep could not reach the reap path")
	}

	m := &Manager{}
	m.Channels.Store("alice", ch)
	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("reaped = %d, want 0 — the dead-run pass removes a file but must not claim a restart", got)
	}

	// The dead-run's empty file must be gone: nothing else will ever clear it.
	if _, err := os.Stat(path); err == nil {
		t.Errorf("dead-run zero-byte file survived the sweep — nothing else deletes it now that" +
			" the monitor is gone")
	}
}

// TestReapStuckRecordingsSkipsPausedChannel verifies a paused channel is never
// touched, even with an aged empty file on disk.
func TestReapStuckRecordingsSkipsPausedChannel(t *testing.T) {
	ch := newSweepChannel(t, t.TempDir(), "bob")
	ch.Config.IsPaused.Store(true)

	m := &Manager{}
	m.Channels.Store("bob", ch)
	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("reaped = %d, want 0 for a paused channel", got)
	}
}

// newSweepChannelFresh is newSweepChannel without aging: its open current file
// is 0 bytes but still INSIDE the grace window — the redemption window a sweep
// must never delete from, because the channel may simply not have received its
// first byte yet.
func newSweepChannelFresh(t *testing.T, dir, username string) *channel.Channel {
	t.Helper()
	ch := channel.New(&entity.ChannelConfig{
		Username: username,
		Site:     "chaturbate",
		Pattern:  filepath.Join(dir, "{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}"),
	})
	ch.FileExt = ".mp4"
	if err := ch.NextFile(ch.FileExt); err != nil {
		t.Fatalf("NextFile: %v", err)
	}
	t.Cleanup(func() { _ = ch.Cleanup(channel.CloseQueue) })
	return ch
}

// newSweepChannelWithSize opens a current recording file and puts `size` bytes
// in it (an actively growing channel, however old its mtime). Its teardown
// closes the handle and REMOVES the file instead of running channel Cleanup: a
// non-empty file would otherwise be queued for post-processing at teardown, and
// the pending-file pipeline needs fixtures this test cannot set up.
func newSweepChannelWithSize(t *testing.T, dir, username string, size int) *channel.Channel {
	t.Helper()
	ch := channel.New(&entity.ChannelConfig{
		Username: username,
		Site:     "chaturbate",
		Pattern:  filepath.Join(dir, "{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}"),
	})
	ch.FileExt = ".mp4"
	if err := ch.NextFile(ch.FileExt); err != nil {
		t.Fatalf("NextFile: %v", err)
	}
	if _, err := ch.File.Write(make([]byte, size)); err != nil {
		t.Fatalf("write %d bytes: %v", size, err)
	}
	t.Cleanup(func() {
		_ = ch.File.Close()
		_ = os.Remove(ch.CurrentRecordingPath())
	})
	return ch
}

// TestReapStuckRecordingsDeadRunPassBounds pins what the dead-run pass must
// leave alone: a file still inside the grace window (it may start growing any
// moment), a file that HAS data, and a zero-byte file on disk that belongs to
// nobody (the orphan sweep's domain, not a live channel's dead run).
func TestReapStuckRecordingsDeadRunPassBounds(t *testing.T) {
	dir := t.TempDir()

	fresh := newSweepChannelFresh(t, dir, "carol")
	freshPath := fresh.CurrentRecordingPath()
	grown := newSweepChannelWithSize(t, dir, "dan", 4096)
	grownPath := grown.CurrentRecordingPath()

	// A stray zero-byte file no channel owns: the orphan sweep's domain.
	stray := filepath.Join(dir, "stray_video_file.mp4")
	if err := os.WriteFile(stray, nil, 0o644); err != nil {
		t.Fatalf("WriteFile stray: %v", err)
	}
	old := time.Now().Add(-2 * stuckRecordingGrace)
	if err := os.Chtimes(stray, old, old); err != nil {
		t.Fatalf("Chtimes stray: %v", err)
	}

	m := &Manager{}
	m.Channels.Store("carol", fresh)
	m.Channels.Store("dan", grown)
	if got := m.ReapStuckRecordings(); got != 0 {
		t.Errorf("reaped = %d, want 0 — no fixture here is a wedged run", got)
	}
	for name, path := range map[string]string{
		"fresh zero-byte file":         freshPath,
		"growing recording":            grownPath,
		"orphan-owned stray zero-byte": stray,
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s must survive an un-owned sweep, stat: %v", name, err)
		}
	}
}
