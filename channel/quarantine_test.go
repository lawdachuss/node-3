package channel

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// pendUser returns a username unlikely to collide with other channel tests.
func pendUser(t *testing.T) string {
	return "quarantine_" + filepath.Base(t.TempDir())
}

func TestQuarantineSegmentMovesToCorruptDir(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "bad_segment.mp4")
	if err := os.WriteFile(src, []byte("garbage-not-a-video"), 0o666); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	if err := quarantineSegment(src); err != nil {
		t.Fatalf("quarantineSegment: %v", err)
	}

	quarantined := filepath.Join(dir, "corrupt", "bad_segment.mp4")
	if _, err := os.Stat(quarantined); err != nil {
		t.Fatalf("expected corrupt file moved to %s: %v", quarantined, err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("original should no longer exist: %v", err)
	}
}

func TestDeletePendingSegmentsCleansCorruptSubdir(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{OutputDir: t.TempDir()}

	user := pendUser(t)
	dir := pendingSegmentsDir(user)
	if err := os.MkdirAll(filepath.Join(dir, "corrupt"), 0o777); err != nil {
		t.Fatalf("mkdir pending: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.mp4"), []byte("x"), 0o666); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupt", "bad.mp4"), []byte("x"), 0o666); err != nil {
		t.Fatalf("write corrupt segment: %v", err)
	}

	deletePendingSegments(user)

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("pending dir should be fully removed including corrupt subdir: %v", err)
	}
}

// TestQuarantineInvalidSegmentsAllCorrupt verifies that when every pending
// segment fails to probe, all of them are moved aside and no "valid" segment
// remains. This runs without requiring ffmpeg/ffprobe: probing garbage always
// fails regardless of whether a binary is installed.
func TestQuarantineInvalidSegmentsAllCorrupt(t *testing.T) {
	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{OutputDir: t.TempDir()}

	user := pendUser(t)
	pending := pendingSegmentsDir(user)
	if err := os.MkdirAll(pending, 0o777); err != nil {
		t.Fatalf("mkdir pending: %v", err)
	}
	paths := []string{
		filepath.Join(pending, "seg1.mp4"),
		filepath.Join(pending, "seg2.mp4"),
	}
	for _, p := range paths {
		if err := os.WriteFile(p, []byte("definitely not a playable video"), 0o666); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	valid, quarantined := quarantineInvalidSegments(user, paths)
	if quarantined != 2 {
		t.Fatalf("expected 2 quarantined, got %d", quarantined)
	}
	if len(valid) != 0 {
		t.Fatalf("expected no valid segments, got %d", len(valid))
	}
	for _, base := range []string{"seg1.mp4", "seg2.mp4"} {
		if _, err := os.Stat(filepath.Join(pending, "corrupt", base)); err != nil {
			t.Fatalf("expected %s quarantined: %v", base, err)
		}
	}
}

// TestQuarantineInvalidSegmentsKeepsValid verifies that a probe-valid segment
// is preserved while a corrupt sibling is quarantined. The valid segment is a
// real mp4 generated with ffmpeg, so the test is skipped when ffmpeg is not
// available.
func TestQuarantineInvalidSegmentsKeepsValid(t *testing.T) {
	ffmpeg := findFFmpeg()
	if ffmpeg == "" {
		t.Skip("ffmpeg not available")
	}
	if !probeWorks(t, ffmpeg) {
		t.Skip("ffprobe not available")
	}

	oldConfig := server.Config
	defer func() { server.Config = oldConfig }()
	server.Config = &entity.Config{OutputDir: t.TempDir()}

	user := pendUser(t)
	pending := pendingSegmentsDir(user)
	if err := os.MkdirAll(pending, 0o777); err != nil {
		t.Fatalf("mkdir pending: %v", err)
	}

	validPath := filepath.Join(pending, "valid_segment.mp4")
	if err := makeValidMp4(ffmpeg, validPath); err != nil {
		t.Fatalf("create valid mp4: %v", err)
	}
	corruptPath := filepath.Join(pending, "corrupt_segment.mp4")
	if err := os.WriteFile(corruptPath, []byte("definitely not a playable video"), 0o666); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	valid, quarantined := quarantineInvalidSegments(user, []string{corruptPath, validPath})
	if quarantined != 1 {
		t.Fatalf("expected 1 quarantined, got %d", quarantined)
	}
	if len(valid) != 1 || valid[0] != validPath {
		t.Fatalf("expected only the valid segment preserved, got %v", valid)
	}
	if _, err := os.Stat(filepath.Join(pending, "corrupt", "corrupt_segment.mp4")); err != nil {
		t.Fatalf("expected corrupt segment quarantined: %v", err)
	}
	if _, err := os.Stat(validPath); err != nil {
		t.Fatalf("valid segment should remain in place: %v", err)
	}
}

func findFFmpeg() string {
	if server.Config != nil && server.Config.FFmpegPath != "" {
		if _, err := os.Stat(server.Config.FFmpegPath); err == nil {
			return server.Config.FFmpegPath
		}
	}
	for _, name := range []string{"ffmpeg", "ffmpeg.exe"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

// probeWorks checks whether an ffprobe binary is reachable so valid-segment
// probing can actually succeed in the test.
func probeWorks(t *testing.T, ffmpeg string) bool {
	t.Helper()
	dir := filepath.Dir(ffmpeg)
	for _, name := range []string{"ffprobe", "ffprobe.exe"} {
		candidate := filepath.Join(dir, name)
		if _, err := os.Stat(candidate); err == nil {
			return true
		}
	}
	_, err1 := exec.LookPath("ffprobe")
	_, err2 := exec.LookPath("ffprobe.exe")
	return err1 == nil || err2 == nil
}

func makeValidMp4(ffmpeg, out string) error {
	cmd := exec.Command(ffmpeg,
		"-y",
		"-f", "lavfi", "-i", "testsrc=duration=2:size=160x90:rate=10",
		"-pix_fmt", "yuv420p",
		out,
	)
	outBytes, err := cmd.CombinedOutput()
	_ = outBytes
	return err
}
