package router

import (
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

func adminDataWithBackoffs(backoffs []HostBackoffView) *AdminData {
	return &AdminData{
		Config:       &entity.Config{FFmpegPath: "/usr/bin/ffmpeg"},
		Disk:         &entity.DiskInfo{Total: "1 TB", Used: "1 GB", Free: "999 GB", Percent: 1},
		Uploads:      &entity.UploadsResponse{},
		HostBackoffs: backoffs,
	}
}

// TestAdminTemplateShowsHostBackoffs pins the operator-facing contract: when a
// host is skipped fleet-wide, the reason and a resumption time must be readable
// on the admin page.  Without it an exhausted shared credential looks like
// uploads silently going missing.
func TestAdminTemplateShowsHostBackoffs(t *testing.T) {
	out := renderTemplate(t, "admin.html", adminDataWithBackoffs([]HostBackoffView{
		{
			Host:       "VoeSX",
			Until:      "2026-09-23 05:32:51",
			Remaining:  "23h14m",
			Reason:     "daily upload limit reached",
			ReportedBy: "node-11",
		},
	}))

	for _, want := range []string{
		"Upload Host Backoffs",
		"skipped fleet-wide",
		"VoeSX",
		"23h14m",
		"daily upload limit reached",
		"node-11",
		"2026-09-23 05:32:51",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("admin.html missing %q in the host-backoff panel", want)
		}
	}
}

// TestAdminTemplateHidesEmptyHostBackoffs keeps the page clean on a healthy
// fleet: no active backoff means no section (and certainly no empty card that
// reads as a fault).
func TestAdminTemplateHidesEmptyHostBackoffs(t *testing.T) {
	out := renderTemplate(t, "admin.html", adminDataWithBackoffs(nil))
	if strings.Contains(out, "Upload Host Backoffs") {
		t.Fatal("host-backoff section must be hidden when nothing is backed off")
	}
}

// TestFormatBackoffRemaining pins the compact rendering, including the
// day-scale case (a host capped for a whole day) and the guard that a lapsed
// entry never renders a negative duration.
func TestFormatBackoffRemaining(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{in: 0, want: "expired"},
		{in: -time.Hour, want: "expired"},
		{in: 12 * time.Minute, want: "12m"},
		{in: 59 * time.Minute, want: "59m"},
		{in: 18*time.Hour + 24*time.Minute, want: "18h24m"},
		{in: 23*time.Hour + 59*time.Minute, want: "23h59m"},
		{in: 24 * time.Hour, want: "1d0h"},
		{in: 3*24*time.Hour + 4*time.Hour, want: "3d4h"},
	}
	for _, tc := range cases {
		if got := formatBackoffRemaining(tc.in); got != tc.want {
			t.Errorf("formatBackoffRemaining(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
