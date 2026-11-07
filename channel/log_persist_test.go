package channel

import (
	"strings"
	"testing"
	"time"
)

// TestShouldPersistLog verifies the channel_logs persistence filter: WARN and
// ERROR lines always persist, diagnostic INFO lines (cleanup/end reasons,
// session/reconnect events, 403/404) persist unless rate-limited, and
// non-diagnostic INFO (per-segment status) is never persisted.
func TestShouldPersistLog(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Second) // inside the 15s INFO rate window
	stale := now.Add(-time.Minute)  // outside it

	cases := []struct {
		name        string
		line        string
		lastPersist time.Time
		wantLevel   string
		wantMsg     string
		wantOK      bool
	}{
		{
			name:      "error always persists",
			line:      "10:05 [ERROR] record stream: get video playlist: get bytes: not found (404)",
			wantLevel: "error",
			wantMsg:   "record stream: get video playlist: get bytes: not found (404)",
			wantOK:    true,
		},
		{
			name:      "warn always persists",
			line:      "10:05 [WARN] retry 1/3 in 30s",
			wantLevel: "warn",
			wantMsg:   "retry 1/3 in 30s",
			wantOK:    true,
		},
		{
			name:        "cleanup end-reason info persists",
			line:        "05:09 [INFO] cleanup: queued alice_2026-08-16_04-12-00.mp4 for post-processing (ended: HLS stream ended: get bytes: not found (404), duration: 53m, size: 1.2 GB)",
			lastPersist: stale,
			wantLevel:   "info",
			wantMsg:     "cleanup: queued alice_2026-08-16_04-12-00.mp4 for post-processing (ended: HLS stream ended: get bytes: not found (404), duration: 53m, size: 1.2 GB)",
			wantOK:      true,
		},
		{
			name:        "session reconnect info persists",
			line:        "05:03 [INFO] stream session expired (HLS session/token) — reconnecting",
			lastPersist: stale,
			wantLevel:   "info",
			wantMsg:     "stream session expired (HLS session/token) — reconnecting",
			wantOK:      true,
		},
		{
			name:        "diagnostic info rate-limited within window",
			line:        "05:09 [INFO] cleanup: queued alice.mp4 for post-processing (ended: channel stopped (handoff))",
			lastPersist: recent,
			wantOK:      false,
		},
		{
			name:        "non-diagnostic info never persists",
			line:        "10:05 [INFO] status: 42 viewers",
			lastPersist: stale,
			wantOK:      false,
		},
		{
			name:        "per-segment status info never persists",
			line:        "10:05 [INFO] duration: 1m30s, filesize: 45.2 MB [v:150 a:150 synced]",
			lastPersist: stale,
			wantOK:      false,
		},
		{
			name:        "malformed line with no level is dropped",
			line:        "garbage without level marker",
			lastPersist: stale,
			wantLevel:   "info",
			wantMsg:     "garbage without level marker",
			wantOK:      false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			level, msg, ok := shouldPersistLog(c.line, c.lastPersist, now)
			if ok != c.wantOK {
				t.Fatalf("shouldPersistLog ok = %v, want %v (line=%q)", ok, c.wantOK, c.line)
			}
			if !ok {
				return
			}
			if level != c.wantLevel {
				t.Errorf("level = %q, want %q", level, c.wantLevel)
			}
			if msg != c.wantMsg {
				t.Errorf("message = %q, want %q", msg, c.wantMsg)
			}
			if strings.Contains(msg, " [") {
				t.Errorf("message still contains time/level prefix: %q", msg)
			}
		})
	}
}
