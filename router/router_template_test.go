package router

import (
	"bytes"
	"html/template"
	"strconv"
	"strings"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/uploadkeys"
)

// loadAllTemplates parses every production template with the exact FuncMap and
// file set used by SetupRouter.
func loadAllTemplates(t *testing.T) *template.Template {
	t.Helper()
	templ := template.New("").Funcs(templateFuncs())
	for _, name := range []string{
		"index.html", "channel_info.html", "videos.html", "video.html",
		"channel.html", "admin.html", "nodes.html", "pool.html", "logs.html",
	} {
		content, err := view.FS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err = templ.New(name).Parse(string(content)); err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
	}
	return templ
}

// renderTemplate executes one template and fails the test if execution errors
// OR the output is truncated (missing the closing tag).  A template action
// referencing a nonexistent field (e.g. the old {{ .VidaraKeyValue }} on
// IndexData) aborts rendering mid-response — the browser then gets a page
// without the trailing <script> block and every inline onclick throws
// "X is not defined".  These tests catch that class of regression.
func renderTemplate(t *testing.T, name string, data any) string {
	t.Helper()
	var buf bytes.Buffer
	if err := loadAllTemplates(t).ExecuteTemplate(&buf, name, data); err != nil {
		t.Fatalf("render %s: %v", name, err)
	}
	out := buf.String()
	if !strings.Contains(out, "</html>") {
		t.Fatalf("%s render truncated: %d bytes, missing </html>\ntail: %s", name, buf.Len(), out[len(out)-300:])
	}
	return out
}

func TestIndexTemplateRendersCompletely(t *testing.T) {
	out := renderTemplate(t, "index.html", &IndexData{
		Config: &entity.Config{
			VidaraKey:       "test-vidara-key",
			VidMolyKey:      "test-vidmoly-key",
			CfClearance:     "test-clearance",
			StreamtapeLogin: "test-login",
			StreamtapeKey:   "test-st-key",
			MixdropEmail:    "test@example.com",
			MixdropToken:    "test-md-token",
		},
		Channels: []*entity.ChannelInfo{
			{
				Username:   "testchannel",
				Site:       "chaturbate",
				IsOnline:   true,
				RoomStatus: "public",
				Filename:   "testchannel_2026-08-05_00-00-00.mp4",
			},
		},
		Disk: &entity.DiskInfo{
			Total:   "256.00 GB",
			Used:    "120.50 GB",
			Free:    "135.50 GB",
			Percent: 47,
		},
	})
	if !strings.Contains(out, `name="vidara_key" value="test-vidara-key"`) {
		t.Fatalf("index.html render missing vidara_key value (template action failed?)\noutput tail:\n%s", out[len(out)-400:])
	}
}

func TestAdminTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "admin.html", &AdminData{
		Config: &entity.Config{FFmpegPath: "/usr/bin/ffmpeg"},
		Disk:   &entity.DiskInfo{Total: "1 TB", Used: "1 GB", Free: "999 GB", Percent: 1},
		Channels: []*entity.ChannelInfo{
			{
				Username:       "test",
				Site:           "chaturbate",
				IsOnline:       true,
				Duration:       "01:00",
				Filesize:       "1.2 GB",
				UploadStatus:   "uploading",
				UploadProgress: 42,
				UploadFilename: "f.mp4",
			},
		},
		Uploads: &entity.UploadsResponse{
			Active: []entity.UploadEntry{
				{Channel: "test", Filename: "f.mp4", Status: "uploading", Progress: 42, HostCount: 2, HostTotal: 5, BytesCurrent: 100, BytesTotal: 1000, Speed: "1 MB/s", Hosts: []entity.HostEntry{{Host: "GoFile", Status: "done", Progress: 100, Speed: ""}}},
			},
			Pending: []entity.PendingEntry{{Channel: "test", Filename: "f.mp4", Stage: "upload", Failed: true, Error: "boom"}},
			History: []entity.PendingEntry{{Channel: "test", Filename: "f.mp4", Stage: "upload"}},
		},
		Nodes: []database.Node{
			{NodeID: "node-test", Hostname: "host", Status: "online", CurrentLoad: 1, WebURL: "https://x", LastHeartbeat: "now"},
		},
		Assignments: []database.ChannelAssignment{
			{Username: "test", Site: "chaturbate", AssignedNode: "node-test", Status: "recording", IsLive: true, AssignedAt: "now", Resolution: 1080, Framerate: 60},
		},
		PipelineMap: map[string]ChannelPipelinesEntry{"test": {Username: "test", Queued: 1}},
		OnlineNodes: 1,
		PoolMode:    "pooled",
		MyNodeID:    "node-test",
	})
}

// TestAdminTemplateRendersUploadKeysPanel verifies the Upload Keys panel
// renders its per-host tables, health badges, per-host add forms, and the
// "never checked" timestamp — the pieces the operator uses to spot a dead key
// at a glance.
func TestAdminTemplateRendersUploadKeysPanel(t *testing.T) {
	out := renderTemplate(t, "admin.html", &AdminData{
		Config:  &entity.Config{},
		Disk:    &entity.DiskInfo{},
		Uploads: &entity.UploadsResponse{},
		UploadKeys: []uploadkeys.HostView{
			{
				Host: "VoeSX", Label: "VOE.sx", Probeable: true, Fields: []string{"key"},
				Keys: []uploadkeys.KeyView{
					{Fingerprint: "fp-dead-0001", Display: "••••wxyz", Status: "dead", Message: "authentication rejected"},
					{Fingerprint: "fp-new-0002", Display: "••••abcd", Status: "untested"},
				},
			},
			{
				Host: "Streamtape", Label: "Streamtape", Probeable: true, Fields: []string{"login", "key"},
				Keys: []uploadkeys.KeyView{
					{Fingerprint: "fp-st-0003", Display: "••••9876", Status: "healthy"},
				},
			},
			{
				Host: "Mixdrop", Label: "Mixdrop", Probeable: false, Fields: []string{"email", "token"},
			},
		},
		UploadKeyTotal: 3,
	})

	for _, want := range []string{
		`data-host="VoeSX"`,
		`data-host="Streamtape"`,
		`data-host="Mixdrop"`,
		`id="uk-total">3<`,
		`<span class="uk-badge uk-dead">dead</span>`,
		`<span class="uk-badge uk-untested">untested</span>`,
		`<span class="uk-badge uk-healthy">healthy</span>`,
		`data-fp="fp-dead-0001"`,
		`authentication rejected`,
		`data-field="key"`,
		`data-field="login"`,
		`data-field="email"`,
		`data-field="token"`,
		`data-act="probe-all"`,
		`—`, // zero LastChecked renders as an em dash via timefmt
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered admin page missing %q", want)
		}
	}
	// The unhealthy Streamtape row must NOT carry the dead badge.
	if strings.Contains(out, `data-fp="fp-st-0003"`+"\n"+`<span class="uk-badge uk-dead">`) {
		t.Error("healthy key wrongly badged dead")
	}
}

func TestVideosTemplateRendersCompletely(t *testing.T) {
	v := &VideoEntry{
		Username:     "test",
		Filename:     "test_2026-08-05.mp4",
		FullPath:     "videos/test.mp4",
		ThumbnailURL: "https://x/thumb.jpg",
		Links:        map[string]string{"GoFile": "https://gofile.io/d/x"},
		Tags:         []string{"tag"},
		Resolution:   "1080p",
		Framerate:    60,
	}
	renderTemplate(t, "videos.html", &VideosData{
		Config:      &entity.Config{},
		Videos:      []*VideoEntry{v},
		Groups:      []VideoGroup{{Username: "test", Videos: []*VideoEntry{v}}},
		Recommended: []*VideoEntry{v},
	})
}

func TestChannelTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "channel.html", &VideosData{
		Config: &entity.Config{},
		Videos: []*VideoEntry{{Username: "test", Filename: "test.mp4"}},
	})
}

func TestNodesTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "nodes.html", &NodesData{
		Nodes: []database.Node{
			{NodeID: "node-1", Hostname: "h", InstanceLabel: "i", SoftwareVersion: "v", Status: "online", CurrentLoad: 2, LastHeartbeat: "now", WebURL: "https://x", CreatedAt: "t"},
		},
		OnlineCount: 1,
		TotalLoad:   2,
		Mode:        "pooled",
		MyNodeID:    "node-1",
	})
}

// TestOfflineNodeLoadCellHidden verifies the per-node Load cell never shows a
// dead node's frozen current_load (only ever written by the node's own
// heartbeat) — it renders a muted dash instead, on both the nodes page and
// the admin page.
//
// The load metric cards are set to a distinct value (999) so they can't be
// confused with the per-node cell values in the assertions. The online node's
// cell renders "280\n...</td>" (template whitespace), so "\u2265280\n" matches
// exactly one load cell; the offline node's cell renders a dash instead.
func TestOfflineNodeLoadCellHidden(t *testing.T) {
	// Load value chosen to not collide with anything in the templates
	// (admin.html contains max-width:280px in its CSS).
	const load = 283
	nodes := []database.Node{
		{NodeID: "node-16", Status: "online", CurrentLoad: load},
		{NodeID: "node-19", Status: "offline", CurrentLoad: load}, // dead — must not show its load
	}

	checkCells := func(name, out string) {
		t.Helper()
		if !strings.Contains(out, ">999<") {
			t.Fatalf("%s: load metric card missing\n%s", name, out[len(out)-800:])
		}
		// The online node's load cell is the ONLY place the load value may
		// appear (metric cards use 999, no timestamps are set, and the offline
		// node's stale value must be replaced by a dash). Exactly one
		// occurrence proves the offline cell no longer renders the frozen load.
		if got := strings.Count(out, strconv.Itoa(load)); got != 1 {
			t.Fatalf("%s: expected exactly one load cell with %d (online node only), got %d\n%s", name, load, got, out[len(out)-1200:])
		}
		if !strings.Contains(out, "\u2014") {
			t.Fatalf("%s: offline node load cell should render a dash", name)
		}
	}

	checkCells("nodes.html", renderTemplate(t, "nodes.html", &NodesData{
		Nodes:       nodes,
		OnlineCount: 1,
		TotalLoad:   999, // distinct metric value, not a load cell
		Mode:        "pooled",
		MyNodeID:    "node-16",
	}))

	checkCells("admin.html", renderTemplate(t, "admin.html", &AdminData{
		Config:        &entity.Config{FFmpegPath: "/usr/bin/ffmpeg"},
		Disk:          &entity.DiskInfo{Total: "1 TB", Used: "1 GB", Free: "999 GB", Percent: 1},
		Uploads:       &entity.UploadsResponse{},
		Nodes:         nodes,
		OnlineNodes:   1,
		TotalNodeLoad: 999, // distinct metric value, not a load cell
		PoolMode:      "pooled",
		MyNodeID:      "node-16",
	}))
}

// TestChannelInfoTemplateShowsAutoResumedBadge verifies the channel card
// renders the "Auto-Resumed" badge only when the channel was automatically
// resumed from a stuck paused-but-still-assigned state — the UI marker for the
// stuck-pause recovery path in manager.CreateChannelFromAssignment.
func TestChannelInfoTemplateShowsAutoResumedBadge(t *testing.T) {
	templ := loadAllTemplates(t)
	render := func(info *entity.ChannelInfo) string {
		t.Helper()
		var buf bytes.Buffer
		if err := templ.ExecuteTemplate(&buf, "channel_info", info); err != nil {
			t.Fatalf("render channel_info: %v", err)
		}
		return buf.String()
	}

	out := render(&entity.ChannelInfo{
		Username:             "stuck_user",
		Site:                 "chaturbate",
		SiteDomain:           "https://www.cb.xxx/",
		IsOnline:             true,
		AutoResumedFromPause: true,
		GlobalConfig:         &entity.Config{},
	})
	if !strings.Contains(out, "Auto-Resumed") {
		t.Fatalf("channel_info render missing Auto-Resumed badge: %s", out)
	}

	normal := render(&entity.ChannelInfo{
		Username:     "normal_user",
		Site:         "chaturbate",
		SiteDomain:   "https://www.cb.xxx/",
		IsOnline:     true,
		GlobalConfig: &entity.Config{},
	})
	if strings.Contains(normal, "Auto-Resumed") {
		t.Fatal("channel_info render must not show Auto-Resumed badge for a normal channel")
	}
}

func TestPoolTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "pool.html", &PoolData{
		Assignments: []PoolEntry{
			{Username: "test", Site: "chaturbate", AssignedNode: "node-1", Status: "unassigned", Resolution: 1080, Framerate: 30, AssignedAt: "now", Source: "pool"},
		},
		Mode: "pooled",
	})
}

func TestLogsTemplateRendersCompletely(t *testing.T) {
	renderTemplate(t, "logs.html", nil)
}

// TestSumNodeLoadExcludesOffline verifies that offline nodes' frozen
// current_load (never corrected after death, since only the node's own
// heartbeat writes it) is excluded from the dashboard's Total Load, while
// online and draining nodes still count.
func TestSumNodeLoadExcludesOffline(t *testing.T) {
	nodes := []database.Node{
		{NodeID: "node-16", Status: "online", CurrentLoad: 280},
		{NodeID: "node-17", Status: "online", CurrentLoad: 280},
		{NodeID: "node-18", Status: "draining", CurrentLoad: 280},
		{NodeID: "node-19", Status: "offline", CurrentLoad: 280}, // dead — must not count
	}
	if got := sumNodeLoad(nodes); got != 840 {
		t.Fatalf("sumNodeLoad = %d, want 840 (offline node excluded)", got)
	}
}

func TestSumNodeLoadAllOffline(t *testing.T) {
	nodes := []database.Node{
		{NodeID: "node-19", Status: "offline", CurrentLoad: 280},
	}
	if got := sumNodeLoad(nodes); got != 0 {
		t.Fatalf("sumNodeLoad = %d, want 0 (single offline node)", got)
	}
}
