package router

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
	"github.com/teacat/chaturbate-dvr/uploadkeys"
)

// IndexData represents the data structure for the index page.
type IndexData struct {
	Config              *entity.Config
	Channels            []*entity.ChannelInfo
	Disk                *entity.DiskInfo
	SessionDeadlineUnix int64 // Unix timestamp when current session ends; 0 = inactive
	SessionDurationSec  int   // Total session duration in seconds
}

type hostPlayer struct {
	Host     string `json:"host"`
	Link     string `json:"link"`
	EmbedURL string `json:"embedUrl,omitempty"`
	VideoURL string `json:"videoUrl,omitempty"`
}

// Index renders the index page with channel information.
func Index(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=30")

	var deadlineUnix int64
	var durSec int
	remaining, active := server.Manager.SessionInfo()
	if active {
		deadlineUnix = time.Now().Add(remaining).Unix()
		durSec = int(server.Config.SessionDurationParsed.Seconds())
	}

	c.HTML(200, "index.html", &IndexData{
		Config:              server.Config,
		Channels:            server.Manager.ChannelInfo(),
		Disk:                server.GetDiskInfo(),
		SessionDeadlineUnix: deadlineUnix,
		SessionDurationSec:  durSec,
	})
}

// proxyInfo is one proxy pool entry rendered by the optional proxy status
// section on the admin page.
type proxyInfo struct {
	IP         string
	Current    bool
	CookiesAge string
	DeadUntil  string
	Refreshing bool
}

// proxyStatus describes the proxy pool state.  AdminPage leaves ProxyStatus
// nil so the (frontend-driven) proxy section stays hidden until the backend
// populates it — a nil pointer keeps the admin page from aborting rendering.
type proxyStatus struct {
	PoolSize   int
	CookiesOK  bool
	Refreshing bool
	Prewarming bool
	DeadCount  int
	Proxies    []proxyInfo
}

// ChannelPipelinesEntry holds per-channel pipeline queue counts for the admin page.
type ChannelPipelinesEntry struct {
	Username string
	Queued   int
	Failed   int
}

// AdminData represents the data structure for the admin page.
type AdminData struct {
	Config   *entity.Config
	Channels []*entity.ChannelInfo
	Disk     *entity.DiskInfo
	Uploads  *entity.UploadsResponse
	Orphans  []orphanEntry

	// QueueBytes is the total size of everything queued + actively uploading
	// (drives the "Upload Queue" metric card).
	QueueBytes int64

	// Session
	SessionActive     bool
	SessionRemaining  string
	SessionDuration   string
	SessionProcessing bool

	// System health
	GoVersion    string
	GoGoroutines int
	GoMemoryMB   string
	GoNumCPU     int
	Uptime       string
	FFmpegFound  bool

	// Tunnel
	TunnelURL string

	// Per-channel pipeline counts (keyed by username for easy template lookup)
	PipelineMap map[string]ChannelPipelinesEntry

	// Optional proxy pool status (nil hides the proxy section on the page).
	ProxyStatus *proxyStatus

	// Distributed shards
	Nodes           []database.Node
	Assignments     []database.ChannelAssignment
	OnlineNodes     int
	DrainingNodes   int
	TotalNodeLoad   int
	PoolMode        string
	MyNodeID        string

	// Recording reconciliation: on-disk recordings vs Supabase metadata, to
	// surface recordings that were produced locally but never reached the cloud.
	Recon *RecordingReconciliation

	// HostBackoffs lists the fleet-wide upload-host backoffs that are currently
	// active, so an operator can see WHY a host is being skipped instead of
	// inferring it from absent uploads. Empty (and the section hidden) when no
	// host is backed off.
	HostBackoffs []HostBackoffView

	// UploadKeys is the central upload-credential store (per host: the
	// configured keys with live health) rendered by the Upload Keys panel.
	UploadKeys []uploadkeys.HostView
	// UploadKeyTotal is the number of configured keys across all hosts (the
	// panel's count badge).
	UploadKeyTotal int
}

// HostBackoffView is one active fleet-wide upload-host backoff rendered on the
// admin page. Upload credentials are shared by every node, so a host that one
// node found exhausted (VidMoly's 50 requests/day API cap, an exhausted key
// ring) is skipped by the whole fleet — which otherwise looks like silent
// missing uploads.
type HostBackoffView struct {
	Host       string
	Until      string // local time the host becomes usable again
	Remaining  string // compact time left, e.g. "18h24m"
	Reason     string // the host's own wording
	ReportedBy string // node that discovered it
}

// formatBackoffRemaining renders a backoff's remaining time compactly
// ("3d4h", "18h24m", "12m").  Any non-positive duration reads as "expired" so a
// row that lapsed between the read and the render never shows a negative time.
func formatBackoffRemaining(d time.Duration) string {
	if d <= 0 {
		return "expired"
	}
	days := int(d / (24 * time.Hour))
	hours := int(d/time.Hour) % 24
	minutes := int(d/time.Minute) % 60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd%dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh%dm", hours, minutes)
	default:
		return fmt.Sprintf("%dm", minutes)
	}
}

// loadHostBackoffs returns the currently-active fleet-wide upload-host
// backoffs, sorted by host.  Best-effort by design: this is a diagnostic panel,
// so an unreachable or not-yet-migrated table yields nothing rather than
// breaking the whole admin page.
func loadHostBackoffs() []HostBackoffView {
	client := server.GetDBClient()
	if client == nil {
		return nil
	}
	rows, err := client.ListHostBackoffs()
	if err != nil {
		if !database.HostBackoffTableMissing(err) {
			fmt.Printf("[WARN] admin: failed to load host backoffs: %v\n", err)
		}
		return nil
	}

	now := time.Now()
	views := make([]HostBackoffView, 0, len(rows))
	for _, r := range rows {
		until, ok := r.UntilTime()
		if !ok || !until.After(now) {
			// Expired rows are not "being skipped" — listing them would make
			// operators chase hosts that are already working again.
			continue
		}
		views = append(views, HostBackoffView{
			Host:       r.Host,
			Until:      until.Local().Format("2006-01-02 15:04:05"),
			Remaining:  formatBackoffRemaining(until.Sub(now)),
			Reason:     r.Reason,
			ReportedBy: r.UpdatedBy,
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Host < views[j].Host })
	return views
}

// RecordingReconciliation compares recordings physically present on THIS node's
// disk against the persistent Supabase metadata store. It exists to answer the
// question "are we actually losing recordings?": any on-disk file with no
// Supabase row that is NOT currently in the upload pipeline is a prime loss
// candidate (GitHub Actions runner disks are ephemeral and wiped between runs).
//
// NOTE: the recordings table is NOT node-scoped (Recording has no node_id), so
// SupabaseTotal is a GLOBAL count across the whole fleet, while the on-disk
// numbers are for this node only. The meaningful loss signal is Stuck, not the
// raw local-vs-global delta.
type RecordingReconciliation struct {
	LocalFiles      int    // completed video files currently on this node's disk
	LocalBytesHuman string // human-readable total size of LocalFiles
	SupabaseTotal   int    // GLOBAL recording rows in Supabase (all nodes)
	HasSupabase     bool   // true when the count query succeeded
	Orphans         int    // on-disk files not provably uploaded (no metadata row, or no upload link)
	OrphanBytesHuman string
	InFlight        int    // files currently queued/active in the upload pipeline
	// Stuck = orphan files not in the upload pipeline AND older than the
	// stuck threshold — the real "permanently lost" candidates.
	Stuck          int
	StuckBytesHuman string
	// ZeroByteStuck is the subset of Stuck that never received a single byte:
	// the channel opened its output file and no segment ever arrived.  Tracked
	// separately because these have no bytes to lose yet are the clearest sign
	// of a wedged recording, and they were previously excluded from Stuck
	// entirely (which is how one node sat on 25 dead files showing HEALTHY).
	ZeroByteStuck int
	Verdict       string // "healthy" | "warning" | "critical"
	VerdictDetail string

	// NoHosts is the GLOBAL count of recordings with zero upload links — rows
	// whose content never reached a host.  A recordings row alone does not mean
	// the video is safe: it is created at enqueue time, before any upload.  The
	// ones whose local copy is already gone can never be re-uploaded, so this is
	// the fleet-wide "upload never landed" tally the orphan list only shows for
	// files still on this node's disk.
	NoHosts    int
	HasNoHosts bool // true when the upload-link index query succeeded

	// LinkWritePath is this node's upload-link write-path canary (see
	// server.VerifyUploadLinkWritePath).  It answers the question NoHosts only
	// raises: can the fleet save a link AT ALL right now?  A database-side rule
	// that rejects the insert makes every recording host-less no matter how well
	// the uploads themselves go, and nothing else in the panel would say so.
	LinkWritePathChecked bool
	LinkWritePathOK      bool
	LinkWritePathDetail  string
}

// stuckOrphanThreshold: an orphan younger than this is assumed still working
// its way through the upload pipeline; older than this with no upload activity
// is treated as stranded.
const stuckOrphanThreshold = 30 * time.Minute

// zeroByteStuckThreshold: how long a zero-byte orphan may sit before it is
// reported as stuck.  Shorter than stuckOrphanThreshold because it is already
// known to be empty — there is no upload to wait on — but long enough that the
// ordinary gap between opening a file and the first segment (and the manager's
// 10-minute reap grace) never trips it.
const zeroByteStuckThreshold = 15 * time.Minute

// isZeroByteStuck reports whether a zero-byte orphan file should be counted as
// a wedged recording: it is not currently being written (so it is not simply
// still filling) and it has been empty past the threshold.  Split out from the
// reconciliation walk so the rule can be tested without a live disk scan.
func isZeroByteStuck(filename string, mod, now time.Time, activeRecs map[string]bool) bool {
	if activeRecs[filename] {
		return false
	}
	return now.Sub(mod) > zeroByteStuckThreshold
}

// computeRecordingReconciliation builds the RecordingReconciliation for the
// admin panel from the on-disk scan helpers and the live upload state.
func computeRecordingReconciliation(uploads *entity.UploadsResponse) *RecordingReconciliation {
	r := &RecordingReconciliation{}

	local := scanAllVideoFiles()
	var localBytes int64
	for _, f := range local {
		localBytes += f.Size
	}
	r.LocalFiles = len(local)
	r.LocalBytesHuman = humanBytes(localBytes)

	if dbClient := server.GetDBClient(); dbClient != nil {
		if n, err := dbClient.CountRecordings(); err == nil {
			r.SupabaseTotal = n
			r.HasSupabase = true
		}
	}
	// Recordings with no host at all (fleet-wide).  Served from the same cached
	// upload-link index scanOrphanFiles uses, so it costs no extra query.
	if all, links, err := server.RecordingUploadLinkIndex(); err == nil {
		r.NoHosts = len(all) - len(links)
		r.HasNoHosts = true
	}
	// Canary result, from the last periodic check (no extra request here: the
	// panel must not turn a page load into a write against the database).
	if st := server.UploadLinkWritePathStatus(); st.Checked {
		r.LinkWritePathChecked = true
		r.LinkWritePathOK = st.OK
		r.LinkWritePathDetail = st.Detail
	}

	// in-flight filenames (queued + actively uploading)
	inFlight := map[string]bool{}
	if uploads != nil {
		for _, p := range uploads.Pending {
			inFlight[p.Filename] = true
		}
		for _, a := range uploads.Active {
			inFlight[a.Filename] = true
		}
	}
	r.InFlight = len(inFlight)

	// Files a channel is recording RIGHT NOW.  A zero-byte file that is still
	// being appended to is normal (the playlist head has not arrived yet), so it
	// must never be reported as a stuck recording; every other zero-byte file
	// past the threshold is a recording that produced nothing.
	activeRecs := map[string]bool{}
	if server.Manager != nil {
		for _, p := range server.Manager.ActiveRecordingFiles() {
			activeRecs[filepath.Base(p)] = true
		}
	}

	orphans := scanOrphanFiles()
	var orphanBytes, stuckBytesAcc int64
	now := time.Now()
	for _, o := range orphans {
		// Session-continuity merge intermediates are held on disk by design
		// (no cloud metadata yet), so they are not a "permanent loss"
		// candidate.
		if strings.Contains(o.Filename, ".merged.") {
			continue
		}
		mod, err := time.Parse(time.RFC3339Nano, o.ModTime)
		if err != nil {
			continue
		}
		if o.Size == 0 {
			// No content to lose, but still the footprint of a recording that
			// never started producing — the channel opened its file and no
			// segment ever arrived (an edge/CF cut at session start, or a
			// wedged HLS poll).  Reporting these is the point: skipping them is
			// what let a node accumulate 25 dead files while reading HEALTHY.
			if !isZeroByteStuck(o.Filename, mod, now, activeRecs) {
				continue
			}
			r.ZeroByteStuck++
			r.Stuck++
			continue
		}
		orphanBytes += o.Size
		if !inFlight[o.Filename] && now.Sub(mod) > stuckOrphanThreshold {
			r.Stuck++
			stuckBytesAcc += o.Size
		}
	}
	r.Orphans = len(orphans)
	r.OrphanBytesHuman = humanBytes(orphanBytes)
	r.StuckBytesHuman = humanBytes(stuckBytesAcc)

	switch {
	case r.Stuck > 0:
		r.Verdict = "critical"
		if r.ZeroByteStuck > 0 {
			r.VerdictDetail = fmt.Sprintf("%d recording(s) on disk have no uploaded copy and are not in the upload pipeline — likely permanent loss (%d never wrote a single byte: the stream was cut before the first segment).", r.Stuck, r.ZeroByteStuck)
		} else {
			r.VerdictDetail = fmt.Sprintf("%d recording(s) on disk have no uploaded copy and are not in the upload pipeline — likely permanent loss.", r.Stuck)
		}
	case r.Orphans > r.InFlight:
		r.Verdict = "warning"
		r.VerdictDetail = fmt.Sprintf("%d orphan file(s) exceed the active upload queue (%d); verify the upload pipeline is draining.", r.Orphans-r.InFlight, r.InFlight)
	default:
		r.Verdict = "healthy"
		r.VerdictDetail = "Every on-disk recording has an uploaded copy or is currently moving through the upload pipeline."
	}
	return r
}

// humanBytes formats a byte count into a short human-readable string.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// AdminPage renders the admin panel with deep upload/orphan matrices.
func AdminPage(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=15")

	channels := server.Manager.ChannelInfo()
	uploads := server.Manager.UploadEntries()

	// ── Orphans ──
	orphans := scanOrphanFiles()

	// ── Session info ──
	var sessionActive bool
	var sessionRemaining, sessionDuration string
	remaining, active := server.Manager.SessionInfo()
	if active {
		sessionActive = true
		sessionRemaining = remaining.Round(time.Second).String()
		sessionDuration = server.Config.SessionDurationParsed.Round(time.Second).String()
	}
	sessionProcessing := server.Manager.IsProcessingSession()

	// ── Queue bytes (pending pipelines + active uploads) ──
	var queueBytes int64
	for _, p := range uploads.Pending {
		queueBytes += p.Size
	}
	for _, a := range uploads.Active {
		queueBytes += a.BytesTotal
	}

	// ── System health ──
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	ffmpegFound := false
	if _, err := exec.LookPath("ffmpeg"); err == nil {
		ffmpegFound = true
	} else if server.Config.FFmpegPath != "" {
		if _, err := exec.LookPath(server.Config.FFmpegPath); err == nil {
			ffmpegFound = true
		} else if _, err := os.Stat(server.Config.FFmpegPath); err == nil {
			ffmpegFound = true
		}
	}
	uptime := time.Since(server.StartTime).Round(time.Second).String()

	// ── Tunnel ──
	tunnelURL, _ := server.LoadCurrentTunnel()

	// ── Per-channel pipeline counts ──
	channelPipelineMap := map[string]ChannelPipelinesEntry{}
	for _, ch := range channels {
		channelPipelineMap[ch.Username] = ChannelPipelinesEntry{Username: ch.Username}
	}
	for _, p := range uploads.Pending {
		if entry, ok := channelPipelineMap[p.Channel]; ok {
			entry.Queued++
			channelPipelineMap[p.Channel] = entry
		}
	}
	for _, h := range uploads.History {
		if entry, ok := channelPipelineMap[h.Channel]; ok {
			if h.Failed {
				entry.Failed++
				channelPipelineMap[h.Channel] = entry
			}
		}
	}

	// ── Nodes ──
	var nodes []database.Node
	var assignments []database.ChannelAssignment
	onlineNodes := 0
	drainingNodes := 0
	totalNodeLoad := 0
	if dbClient := server.GetDBClient(); dbClient != nil {
		var err error
		nodes, err = dbClient.GetAllNodes()
		if err != nil {
			fmt.Printf("[WARN] admin: failed to load nodes: %v\n", err)
		}
		assignments, err = dbClient.GetAllAssignments()
		if err != nil {
			fmt.Printf("[WARN] admin: failed to load assignments: %v\n", err)
		}
	}
	for _, n := range nodes {
		if n.Status == "online" {
			onlineNodes++
		} else if n.Status == "draining" {
			drainingNodes++
		}
	}
	// Same as the nodes page: a dead node's frozen current_load must not
	// inflate the pool load total.
	totalNodeLoad = sumNodeLoad(nodes)

	uploadKeys := uploadkeys.Snapshot()

	c.HTML(200, "admin.html", &AdminData{
		Config:   server.Config,
		Channels: channels,
		Disk:     server.GetDiskInfo(),
		Uploads:  uploads,
		Orphans:  orphans,
		Recon:    computeRecordingReconciliation(uploads),

		SessionActive:    sessionActive,
		SessionRemaining: sessionRemaining,
		SessionDuration:  sessionDuration,
		SessionProcessing: sessionProcessing,
		QueueBytes:        queueBytes,

		GoVersion:    runtime.Version(),
		GoGoroutines: runtime.NumGoroutine(),
		GoMemoryMB:   fmt.Sprintf("%.1f", float64(memStats.Alloc)/1024/1024),
		GoNumCPU:     runtime.NumCPU(),
		Uptime:       uptime,
		FFmpegFound:  ffmpegFound,

		TunnelURL: tunnelURL,

		PipelineMap: channelPipelineMap,

		Nodes:         nodes,
		Assignments:   assignments,
		OnlineNodes:   onlineNodes,
		DrainingNodes: drainingNodes,
		TotalNodeLoad: totalNodeLoad,
		PoolMode:      server.ChannelPoolMode(),
		MyNodeID:      server.NodeID(),

		HostBackoffs: loadHostBackoffs(),

		UploadKeys:     uploadKeys,
		UploadKeyTotal: uploadKeyTotal(uploadKeys),
	})
}

// uploadKeyTotal sums the configured keys across every host for the panel's
// count badge.
func uploadKeyTotal(views []uploadkeys.HostView) int {
	n := 0
	for _, v := range views {
		n += len(v.Keys)
	}
	return n
}

// CreateChannelRequest represents the request body for creating a channel.
type CreateChannelRequest struct {
	Site                    string `form:"site"`
	Username                string `form:"username" binding:"required"`
	Framerate               int    `form:"framerate" binding:"required"`
	Resolution              int    `form:"resolution" binding:"required"`
	Pattern                 string `form:"pattern" binding:"required"`
	MaxDuration             int    `form:"max_duration"`
	MaxFilesize             int    `form:"max_filesize"`
	Compress                bool   `form:"compress"`
	MinDurationBeforeUpload int    `form:"min_duration_before_upload"`
}

// CreateChannel creates a new channel.
func CreateChannel(c *gin.Context) {
	var req *CreateChannelRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	var lastErr error
	for _, username := range strings.Split(req.Username, ",") {
		if err := server.Manager.CreateChannel(&entity.ChannelConfig{
			Site:                    req.Site,
			Username:                username,
			Framerate:               req.Framerate,
			Resolution:              req.Resolution,
			Pattern:                 req.Pattern,
			MaxDuration:             req.MaxDuration,
			MaxFilesize:             req.MaxFilesize,
			Compress:                req.Compress,
			MinDurationBeforeUpload: req.MinDurationBeforeUpload,
			CreatedAt:               time.Now().Unix(),
		}, true); err != nil {
			lastErr = err
			fmt.Printf("[ERROR] create channel %s: %v\n", username, err)
		}
	}
	if lastErr != nil {
		c.String(http.StatusInternalServerError, "Failed to save channel config: %v", lastErr)
		return
	}
	// Ensure the session loop is running after adding a channel
	server.Manager.StartSession(server.Config.SessionDurationParsed)
	c.Redirect(http.StatusFound, "/")
}

// StopChannel stops a channel.
func StopChannel(c *gin.Context) {
	if err := server.Manager.StopChannel(c.Param("username")); err != nil {
		fmt.Printf("[ERROR] stop channel %s: %v\n", c.Param("username"), err)
	}

	if c.GetHeader("HX-Request") == "true" {
		c.Header("HX-Redirect", "/")
		c.Status(http.StatusNoContent)
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	if err := server.Manager.PauseChannel(c.Param("username")); err != nil {
		fmt.Printf("[ERROR] pause channel %s: %v\n", c.Param("username"), err)
	}

	c.Redirect(http.StatusFound, "/")
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	if err := server.Manager.ResumeChannel(c.Param("username")); err != nil {
		fmt.Printf("[ERROR] resume channel %s: %v\n", c.Param("username"), err)
	}

	c.Redirect(http.StatusFound, "/")
}

// Updates handles the SSE connection for updates.
func Updates(c *gin.Context) {
	server.Manager.Subscriber(c.Writer, c.Request)
}

// UpdateConfigRequest represents the request body for updating configuration.
type UpdateConfigRequest struct {
	Cookies         string `json:"cookies" form:"cookies"`
	SessionID       string `json:"sessionid" form:"sessionid"`
	Csrftoken       string `json:"csrftoken" form:"csrftoken"`
	CfClearance     string `json:"cf_clearance" form:"cf_clearance"`
	UserAgent       string `json:"user_agent" form:"user_agent"`
	VoeSXAPIKey     string `json:"voesx_api_key" form:"voesx_api_key"`
	StreamtapeLogin string `json:"streamtape_login" form:"streamtape_login"`
	StreamtapeKey   string `json:"streamtape_key" form:"streamtape_key"`
	MixdropEmail    string `json:"mixdrop_email" form:"mixdrop_email"`
	MixdropToken    string `json:"mixdrop_token" form:"mixdrop_token"`
	VidaraKey       string `json:"vidara_key" form:"vidara_key"`
	VidMolyKey      string `json:"vidmoly_key" form:"vidmoly_key"`
	StripchatPDKey  string `json:"stripchat_pdkey" form:"stripchat_pdkey"`
	AffiliateWM     string `json:"affiliate_wm" form:"affiliate_wm"`
}

// UpdateConfig updates the server configuration from the Web UI form or API POST.
func UpdateConfig(c *gin.Context) {
	var req UpdateConfigRequest
	if err := c.ShouldBind(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	server.ConfigMu.Lock()
	if req.Cookies != "" {
		// Sanitize browser-pasted cookie strings (DevTools exports often
		// wrap values in quotes, which Cloudflare rejects in the header).
		server.Config.Cookies = entity.SanitizeCookieString(req.Cookies)
		// Parse individual fields from the raw cookie string
		if server.Config.CfClearance == "" {
			server.Config.CfClearance = extractCookieValue(server.Config.Cookies, "cf_clearance")
		}
		if server.Config.SessionID == "" {
			server.Config.SessionID = extractCookieValue(server.Config.Cookies, "sessionid")
		}
		if server.Config.Csrftoken == "" {
			server.Config.Csrftoken = extractCookieValue(server.Config.Cookies, "csrftoken")
		}
	}
	if req.SessionID != "" {
		server.Config.SessionID = req.SessionID
	}
	if req.Csrftoken != "" {
		server.Config.Csrftoken = req.Csrftoken
	}
	if req.CfClearance != "" {
		server.Config.CfClearance = entity.SanitizeCookieValue(req.CfClearance)
	}
	if req.UserAgent != "" {
		server.Config.UserAgent = strings.TrimSpace(strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == '\t' || r < 32 {
				return -1
			}
			return r
		}, req.UserAgent))
	}

	parts := make([]string, 0, 3)
	if server.Config.CfClearance != "" {
		parts = append(parts, "cf_clearance="+server.Config.CfClearance)
	}
	if server.Config.SessionID != "" {
		parts = append(parts, "sessionid="+server.Config.SessionID)
	}
	if server.Config.Csrftoken != "" {
		parts = append(parts, "csrftoken="+server.Config.Csrftoken)
	}
	if len(parts) > 0 {
		server.Config.Cookies = strings.Join(parts, "; ")
	}
	server.ConfigMu.Unlock()

	if req.StripchatPDKey != "" {
		server.ConfigMu.Lock()
		server.Config.StripchatPDKey = req.StripchatPDKey
		server.ConfigMu.Unlock()
	}

	// Update the affiliate webmaster code (used for the fast bulk onlinerooms
	// liveness check). Empty values are ignored so the web UI can't wipe a
	// value that is only set in .env / GitHub Actions.
	if req.AffiliateWM != "" {
		server.ConfigMu.Lock()
		server.Config.AffiliateWM = req.AffiliateWM
		server.ConfigMu.Unlock()
	}

	// Update uploader credentials (VOE.sx / Streamtape / Mixdrop / Vidara).
	// SyncLegacyForm saves the edit and verifies every new/rotated credential
	// in this request, so the verdict can be returned to the caller below.
	var keyVerdicts []uploadkeys.Verdict
	if req.VoeSXAPIKey != "" || req.StreamtapeLogin != "" || req.StreamtapeKey != "" || req.MixdropEmail != "" || req.MixdropToken != "" || req.VidaraKey != "" || req.VidMolyKey != "" {
		server.UpdateUploaderCredentials(req.VoeSXAPIKey, req.StreamtapeLogin, req.StreamtapeKey, req.MixdropEmail, req.MixdropToken, req.VidaraKey, req.VidMolyKey)
		// Mirror the submitted credentials into the central upload-key store
		// so this legacy rotation is fleet-wide and is not overwritten by the
		// central set on the next sync.
		keyVerdicts = uploadkeys.SyncLegacyForm(req.VoeSXAPIKey, req.StreamtapeLogin, req.StreamtapeKey, req.MixdropEmail, req.MixdropToken, req.VidaraKey, req.VidMolyKey)
	}

	if err := server.SaveSettings(); err != nil {
		fmt.Printf("[WARN] could not save settings: %v\n", err)
	}

	// JSON clients (the Settings dialog's fetch) get the verdicts of every
	// credential this save added or rotated; plain form posts still redirect.
	if c.ContentType() == "application/json" || strings.Contains(c.GetHeader("Accept"), "application/json") {
		c.JSON(http.StatusOK, gin.H{"ok": true, "keys": keyVerdicts})
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// ── Upload keys API ────────────────────────────────────────────────────────
// The admin page's Upload Keys panel drives these. All four are covered by
// the engine-wide basic auth when configured (SetupAuth).

// UploadKeyAddRequest is the body for adding one credential.
type UploadKeyAddRequest struct {
	Host  string `json:"host" form:"host"`
	Key   string `json:"key" form:"key"`
	Login string `json:"login" form:"login"`
	Email string `json:"email" form:"email"`
	Token string `json:"token" form:"token"`
}

// UploadKeyRemoveRequest is the body for removing one credential.
type UploadKeyRemoveRequest struct {
	Host        string `json:"host" form:"host"`
	Fingerprint string `json:"fingerprint" form:"fingerprint"`
}

// UploadKeyProbeRequest is the body for probing one credential (or, with both
// fields empty, every configured credential).
type UploadKeyProbeRequest struct {
	Host        string `json:"host" form:"host"`
	Fingerprint string `json:"fingerprint" form:"fingerprint"`
}

// GetUploadKeys returns every managed host with its configured keys and
// current health — the panel's data source, polled by the admin page.
func GetUploadKeys(c *gin.Context) {
	views := uploadkeys.Snapshot()
	total := 0
	for _, v := range views {
		total += len(v.Keys)
	}
	c.JSON(http.StatusOK, gin.H{"hosts": views, "total": total})
}

// AddUploadKey replaces a host's central credential list with the submitted
// credential (a rotation — the new key supersedes the old ones everywhere),
// applies it fleet-wide, and returns the verdict of the genuine upload
// round-trip that Add ran before responding, instead of the row starting as
// "untested".
func AddUploadKey(c *gin.Context) {
	var req UploadKeyAddRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}
	cred := uploadkeys.Cred{
		Key:   req.Key,
		Login: req.Login,
		Email: req.Email,
		Token: req.Token,
	}
	// Add saves AND verifies the credential (one tiny test file, deleted
	// again), so the verdict below belongs to this save. The key is already
	// saved, so a failing check reports the host's own error on the row rather
	// than rejecting the save.
	verdict, err := uploadkeys.Add(req.Host, cred)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "status": verdict, "hosts": uploadkeys.Snapshot()})
}

// RemoveUploadKey drops a credential from a host's central list.
func RemoveUploadKey(c *gin.Context) {
	var req UploadKeyRemoveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}
	if err := uploadkeys.Remove(req.Host, req.Fingerprint); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "hosts": uploadkeys.Snapshot()})
}

// ProbeUploadKeys checks one credential (host+fingerprint) or, when both are
// empty, every configured credential, and returns the refreshed snapshot.
func ProbeUploadKeys(c *gin.Context) {
	var req UploadKeyProbeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}
	if req.Host != "" && req.Fingerprint != "" {
		ks, err := uploadkeys.ProbeNow(req.Host, req.Fingerprint)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "status": ks, "hosts": uploadkeys.Snapshot()})
		return
	}
	views, err := uploadkeys.ProbeAllNow()
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	total := 0
	for _, v := range views {
		total += len(v.Keys)
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "hosts": views, "total": total})
}

// extractCookieValue parses a value for the given cookie name from a cookie string.
func extractCookieValue(cookieStr, name string) string {
	for _, pair := range strings.Split(cookieStr, ";") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == name {
			return entity.SanitizeCookieValue(strings.TrimSpace(parts[1]))
		}
	}
	return ""
}

// isPathAllowed checks whether abs is inside the videos/ directory or the
// configured OutputDir.  Returns false for any path outside those roots.
func isPathAllowed(abs string) bool {
	videosAbs, _ := filepath.Abs("videos")
	if videosAbs != "" && strings.HasPrefix(abs, videosAbs+string(filepath.Separator)) || abs == videosAbs {
		return true
	}
	if server.Config != nil && server.Config.OutputDir != "" {
		outAbs, _ := filepath.Abs(server.Config.OutputDir)
		if outAbs != "" && (strings.HasPrefix(abs, outAbs+string(filepath.Separator)) || abs == outAbs) {
			return true
		}
	}
	return false
}

// Download serves a video file for download.
func Download(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	if !isPathAllowed(abs) {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	c.FileAttachment(abs, filepath.Base(abs))
}

// DeleteVideoRecord removes only the Supabase DB records for an uploaded-only video
// (no local file to delete).
func DeleteVideoRecord(c *gin.Context) {
	filename := c.PostForm("filename")
	if filename == "" {
		c.Redirect(http.StatusFound, "/videos")
		return
	}
	// Sanitize: only the base name is accepted, never a path
	filename = filepath.Base(filename)
	if filename == "." || filename == "" {
		c.Redirect(http.StatusFound, "/videos")
		return
	}
	if err := server.DeleteVideoCompletely(filename); err != nil {
		fmt.Printf("[ERROR] delete video DB records for %s: %v\n", filename, err)
	}
	InvalidateVideosCache()
	c.Redirect(http.StatusFound, "/videos")
}

// DeleteVideo removes a video file from disk and all associated data from Supabase.
func DeleteVideo(c *gin.Context) {
	path := c.PostForm("path")
	if path == "" {
		c.Redirect(http.StatusFound, "/videos")
		return
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		c.Redirect(http.StatusFound, "/videos")
		return
	}
	if !isPathAllowed(abs) {
		c.Redirect(http.StatusFound, "/videos")
		return
	}

	// Extract filename for DB cleanup
	filename := filepath.Base(abs)

	// Delete file from disk
	if err := os.Remove(abs); err != nil {
		fmt.Printf("[ERROR] delete video file %s: %v\n", abs, err)
	}

	// Delete all associated data from Supabase
	if err := server.DeleteVideoCompletely(filename); err != nil {
		fmt.Printf("[ERROR] delete video DB records for %s: %v\n", filename, err)
	}

	InvalidateVideosCache()
	c.Redirect(http.StatusFound, "/videos")
}

// Play streams a video file with Range header support for seeking.
func Play(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	if !isPathAllowed(abs) {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	file, err := os.Open(abs)
	if err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	fileSize := stat.Size()

	// Detect MIME type from extension
	mimeType := detectVideoMIME(abs)
	rangeHeader := c.GetHeader("Range")
	c.Header("Accept-Ranges", "bytes")
	c.Header("Cache-Control", "no-cache")
	c.Header("Content-Type", mimeType)

	// Handle HEAD requests
	if c.Request.Method == http.MethodHead {
		c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
		c.Status(http.StatusOK)
		return
	}

	if rangeHeader == "" {
		c.Header("Content-Length", strconv.FormatInt(fileSize, 10))
		c.Status(http.StatusOK)
		io.Copy(c.Writer, file)
		return
	}

	// Parse Range header: "bytes=start-end" or "bytes=start-"
	var start, end int64
	parsed := false
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err == nil {
		parsed = true
	} else if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err == nil {
		parsed = true
		end = fileSize - 1
	}
	if !parsed {
		c.AbortWithStatus(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if start < 0 {
		start = 0
	}
	if end >= fileSize {
		end = fileSize - 1
	}
	if start > end {
		c.AbortWithStatus(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	contentLength := end - start + 1
	c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
	c.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	c.Status(http.StatusPartialContent)

	file.Seek(start, 0)
	io.CopyN(c.Writer, file, contentLength)
}

func detectVideoMIME(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".mp4":
		return "video/mp4"
	case ".ts":
		return "video/MP2T"
	case ".mkv":
		return "video/x-matroska"
	case ".webm":
		return "video/webm"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	default:
		if t := mime.TypeByExtension(ext); t != "" {
			return t
		}
		return "video/mp4"
	}
}

// VideoDetail renders the video detail page with an embedded player.
func VideoDetail(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		c.Redirect(http.StatusFound, "/videos")
		return
	}

	filename := filepath.Base(path)
	username := extractUsername(filename)
	abs := ""
	fileOnDisk := false
	var stat os.FileInfo

	// Try to resolve as a file path. For uploaded-only recordings the path
	// is just a filename, which won't pass isPathAllowed — we fall through
	// to the DB lookup below.
	if resolved, err := filepath.Abs(path); err == nil && isPathAllowed(resolved) {
		abs = resolved
		var statErr error
		stat, statErr = os.Stat(abs)
		fileOnDisk = statErr == nil
	}

	// Load preview URLs from Supabase
	thumbURL, spriteURL, previewURL := server.LoadPreviewLinks(filename)

	// Look up recording metadata from recordings DB
	db := loadRecordings()
	links := map[string]string{}
	tags := []string{}
	roomTitle := ""
	viewers := 0
	gender := ""
	filesize := int64(0)
	embedURL := ""
	dbThumbnailURL := ""
	dbSpriteURL := ""
	dbPreviewURL := ""
	timestamp := ""
	resolution := ""
	framerate := 0
	var related []RecordingEntry
	foundInDB := false
	if db != nil {
		for _, chanData := range db.Channels {
			for _, rec := range chanData.Recordings {
				if rec.Filename == filename {
					foundInDB = true
					if rec.Links != nil {
						links = rec.Links
					}
					tags = rec.Tags
					roomTitle = rec.RoomTitle
					viewers = rec.Viewers
					gender = chanData.Gender
					filesize = rec.Filesize
					embedURL = rec.EmbedURL
					dbThumbnailURL = rec.ThumbnailURL
					dbSpriteURL = rec.SpriteURL
					dbPreviewURL = rec.PreviewURL
					timestamp = rec.Timestamp
					resolution = rec.Resolution
					framerate = rec.Framerate
					break
				}
			}
		}
		// Build same-channel recommendations directly from recordings DB
		// (avoids a full filesystem walk + Supabase scan via scanVideos())
		if db != nil {
			if chanData, ok := db.Channels[username]; ok {
				for _, rec := range chanData.Recordings {
					if rec.Filename == filename {
						continue
					}
					related = append(related, RecordingEntry{
						Filename:     rec.Filename,
						Timestamp:    rec.Timestamp,
						RoomTitle:    rec.RoomTitle,
						Tags:         rec.Tags,
						Viewers:      rec.Viewers,
						Resolution:   rec.Resolution,
						Framerate:    rec.Framerate,
						ThumbnailURL: rec.ThumbnailURL,
						SpriteURL:    rec.SpriteURL,
						PreviewURL:   rec.PreviewURL,
						EndReason:    rec.EndReason,
					})
					if len(related) >= 8 {
						break
					}
				}
			}
		}
	}

	// If file is not on disk AND not in DB, redirect
	if !fileOnDisk && !foundInDB {
		c.Redirect(http.StatusFound, "/videos")
		return
	}

	// Fall back to recordings DB if preview_links table had empty URLs
	if thumbURL == "" && dbThumbnailURL != "" {
		thumbURL = dbThumbnailURL
	}
	if spriteURL == "" && dbSpriteURL != "" {
		spriteURL = dbSpriteURL
	}
	if previewURL == "" && dbPreviewURL != "" {
		previewURL = dbPreviewURL
	}

	hostPlayers := buildHostPlayers(links)

	// If embed URL is empty, try to generate one from upload links
	if embedURL == "" {
		for _, player := range hostPlayers {
			if player.EmbedURL != "" {
				embedURL = player.EmbedURL
				break
			}
		}
	}

	// A Streamtape embed as the page-level <iframe> shows "video not available"
	// (cookie/referer policies). Its playback goes through the same-origin relay
	// (hostPlayer.VideoURL), so never surface a Streamtape embed at page level.
	if isStreamtapeLink(embedURL) {
		embedURL = ""
	}

	hostPlayersJSONBytes, _ := json.Marshal(hostPlayers)
	hostPlayersJSON := template.JS(hostPlayersJSONBytes)

	// Find a direct video URL from upload links (for native player fallback).
	videoURL := ""
	if embedURL == "" {
		for _, player := range hostPlayers {
			if player.VideoURL != "" {
				videoURL = player.VideoURL
				break
			}
		}
	}

	// Build template vars
	fullPath := ""
	size := ""
	modTime := ""
	mimeType := "video/mp4"
	if fileOnDisk {
		fullPath = abs
		size = internal.FormatFilesize(int(stat.Size()))
		modTime = stat.ModTime().Format("2006-01-02 15:04")
		mimeType = detectVideoMIME(abs)
	} else if foundInDB {
		if filesize > 0 {
			size = internal.FormatFilesize(int(filesize))
		} else {
			size = "uploaded"
		}
		if timestamp != "" {
			if t, err := time.Parse("2006-01-02T15:04:05Z", timestamp); err == nil {
				modTime = t.Format("2006-01-02 15:04")
			} else {
				modTime = timestamp
			}
		}
	}

	c.HTML(200, "video.html", gin.H{
		"Config":          server.Config,
		"Filename":        filename,
		"FullPath":        fullPath,
		"VideoURL":        videoURL,
		"Size":            size,
		"ModTime":         modTime,
		"Username":        username,
		"ThumbnailURL":    thumbURL,
		"SpriteURL":       spriteURL,
		"PreviewURL":      previewURL,
		"MimeType":        mimeType,
		"Links":           links,
		"HostPlayers":     hostPlayers,
		"HostPlayersJSON": hostPlayersJSON,
		"Tags":            tags,
		"RoomTitle":       roomTitle,
		"Viewers":         viewers,
		"Gender":          gender,
		"Resolution":      resolution,
		"Framerate":       framerate,
		"Related":         related,
		"EmbedURL":        embedURL,
	})
}

// isStreamtapeLink reports whether u is a Streamtape share/embed/view link.
func isStreamtapeLink(u string) bool {
	u = strings.ToLower(strings.TrimSpace(u))
	return strings.HasPrefix(u, "https://streamtape.com/") || strings.Contains(u, "streamtape.com/")
}

func buildHostPlayers(links map[string]string) []hostPlayer {
	if len(links) == 0 {
		return nil
	}

	hosts := make([]string, 0, len(links))
	for host := range links {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)

	players := make([]hostPlayer, 0, len(hosts))
	for _, host := range hosts {
		link := links[host]
		players = append(players, hostPlayer{
			Host:     host,
			Link:     link,
			EmbedURL: embedURLForHostLink(host, link),
			VideoURL: videoURLForHostLink(host, link),
		})
	}
	return players
}

func embedURLForHostLink(host, link string) string {
	if link == "" {
		return ""
	}
	normalizedHost := strings.ToLower(host)
	normalizedLink := strings.ToLower(link)

	if strings.Contains(normalizedHost, "voe") || strings.Contains(normalizedLink, "voe.sx/") {
		if code := extractFileCode(link); code != "" {
			return "https://voe.sx/e/" + code
		}
	}
	if strings.Contains(normalizedHost, "streamtape") || strings.Contains(normalizedLink, "streamtape.com/") {
		// Streamtape's embed page shows "video not available" from an iframe
		// (cookie/referer policies), so the embed path is disabled and playback
		// goes through the same-origin play relay (videoURLForHostLink).
		return ""
	}
	if strings.Contains(normalizedHost, "mixdrop") || strings.Contains(normalizedLink, "mixdrop.") {
		if code := extractFileCode(link); code != "" {
			return "https://mixdrop.ag/e/" + code
		}
		return link
	}
	if strings.Contains(normalizedHost, "vidara") || strings.Contains(normalizedLink, "vidara.so/") {
		if code := extractFileCode(link); code != "" {
			return "https://vidara.so/e/" + code
		}
		return link
	}
	if strings.Contains(normalizedHost, "gofile") || strings.Contains(normalizedLink, "gofile.io/") {
		return ""
	}
	return ""
}

func videoURLForHostLink(host, link string) string {
	if link == "" {
		return ""
	}

	normalizedHost := strings.ToLower(host)
	normalizedLink := strings.ToLower(link)

	switch {
	case strings.Contains(normalizedHost, "voe") || strings.Contains(normalizedLink, "voe.sx/"):
		if code := extractFileCode(link); code != "" {
			return "https://voe.sx/e/" + code
		}
		return link
	case strings.Contains(normalizedHost, "streamtape") || strings.Contains(normalizedLink, "streamtape.com/"):
		if code := extractFileCode(link); code != "" {
			// Same-origin relay: the node resolves Streamtape's tokenized CDN
			// URL and streams it back, so the browser's <video> element never
			// touches the Streamtape embed (which fails in iframes).
			return "/api/play/streamtape/" + code
		}
		return link
	case strings.Contains(normalizedHost, "mixdrop") || strings.Contains(normalizedLink, "mixdrop."):
		if code := extractFileCode(link); code != "" {
			return "https://mixdrop.ag/e/" + code
		}
		return link
	case strings.Contains(normalizedHost, "vidara") || strings.Contains(normalizedLink, "vidara.so/"):
		if code := extractFileCode(link); code != "" {
			return "https://vidara.so/e/" + code
		}
		return link
	case strings.Contains(normalizedHost, "gofile") || strings.Contains(normalizedLink, "gofile.io/"):
		return ""
	default:
		return ""
	}
}

// ─── Tunnel API ──────────────────────────────────────────────────────────────

type tunnelRequest struct {
	URL   string `json:"url" form:"url"`
	RunID int    `json:"run_id" form:"run_id"`
}

// UpdateTunnel saves a tunnel URL to Supabase.
func UpdateTunnel(c *gin.Context) {
	var req tunnelRequest
	if err := c.ShouldBind(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}
	if req.URL == "" {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	if err := server.SaveTunnelToDB(req.URL, req.RunID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GetTunnel returns the current active tunnel URL.
func GetTunnel(c *gin.Context) {
	url, err := server.LoadCurrentTunnel()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"url": url})
}

func extractFileCode(link string) string {
	parts := strings.Split(strings.TrimRight(link, "/"), "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// ─── Orphan Management API ────────────────────────────────────────────────────

type orphanEntry struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	ModTime  string `json:"modTime"`
	Age      string `json:"age"`
}

// ListOrphans returns a JSON list of orphaned video files found in the
// videos/ and OutputDir directories.  Orphans are files that exist on disk
// but have no Supabase recording entry.
// UploadQueue returns the current upload queue state (active + pending) as JSON.
func UploadQueue(c *gin.Context) {
	resp := server.Manager.UploadEntries()
	c.JSON(http.StatusOK, resp)
}

// TriggerSessionStop manually stops the current recording session early
// and starts the mux/upload/processing phase.
func TriggerSessionStop(c *gin.Context) {
	server.Manager.TriggerSessionStop()
	c.JSON(200, gin.H{"success": true})
}

// SetSessionDuration sets the central recording-session length shared by all
// nodes. The value is persisted to Supabase so every node adopts it on the
// next cycle; the running session is unaffected mid-cycle.
func SetSessionDuration(c *gin.Context) {
	var req struct {
		Duration string `json:"duration"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Duration = strings.TrimSpace(req.Duration)
	if req.Duration == "" || req.Duration == "0" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "duration is required (e.g. \"5h20m0s\", or \"0\" for continuous)"})
		return
	}
	parsed, err := time.ParseDuration(req.Duration)
	if err != nil || parsed < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid duration %q: %v", req.Duration, err)})
		return
	}
	if parsed == 0 {
		// Explicitly disable sessions centrally.
		if err := server.SaveSessionDurationToDB(""); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		server.ConfigMu.Lock()
		server.Config.SessionDuration = ""
		server.Config.SessionDurationParsed = 0
		server.ConfigMu.Unlock()
		c.JSON(http.StatusOK, gin.H{"success": true})
		return
	}

	if err := server.SaveSessionDurationToDB(req.Duration); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	server.ConfigMu.Lock()
	server.Config.SessionDuration = req.Duration
	server.Config.SessionDurationParsed = parsed
	server.ConfigMu.Unlock()
	// Apply it now so a subsequent cycle uses it (no-op if a session is active).
	if server.Manager != nil {
		server.Manager.StartSession(parsed)
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "duration": req.Duration})
}

func ListOrphans(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"orphans": scanOrphanFiles()})
}

// fileEntry describes a video file present on the node's disk.
type fileEntry struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	ModTime  string `json:"modTime"`
}

// scanAllVideoFiles lists every completed video file (mp4/mkv) in videos/ and
// the configured OutputDir, excluding sidecar/scratch/pending artifacts.
// Unlike scanOrphanFiles it does not filter against the recordings table — it
// reports everything on disk so operators can locate recordings that exist
// locally but were never thumbnailed (or never uploaded).
func scanAllVideoFiles() []fileEntry {
	dirs := []string{"videos"}
	if server.Config != nil && server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	var files []fileEntry
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".mp4" && ext != ".mkv" {
				continue
			}
			if strings.Contains(name, ".video.") || strings.Contains(name, ".audio.") || strings.Contains(name, ".muxed.") {
				continue
			}
			if channel.IsFinalizingTemp(name) || strings.Contains(name, ".deleting.") || strings.Contains(name, ".merging") {
				continue
			}
			// Session-continuity merge intermediates (.merged.mp4) are held on
			// disk by design until the live session ends; they are not orphans.
			if strings.Contains(name, ".merged.") {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			files = append(files, fileEntry{
				Path:     filepath.Join(dir, name),
				Filename: name,
				Size:     info.Size(),
				ModTime:  info.ModTime().Format(time.RFC3339),
			})
		}
	}
	return files
}

// ListNodeFiles returns every completed video file present on this node's disk.
func ListNodeFiles(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"files": scanAllVideoFiles()})
}

// orphanNeedsUpload reports whether an on-disk video file still has to be
// uploaded — i.e. its content is not demonstrably in the cloud.
//
// hasRow is every filename with a recordings row; safeInCloud is the subset that
// is proven safe (at least one upload link, or — when the link index is
// unavailable — a recordings row, the old conservative rule).
//
// This replaces a plain "is the filename in the recordings table?" test, which
// answered a different question: the recordings row is written at ENQUEUE time
// (SaveRecordingBasics), long before any host has the file.  So a recording whose
// upload never landed (no embed URL, no upload_links) looked identical to a
// healthy one, kept its local copy out of every consumer of this list —
// including the /api/orphans/retry rescue path — and stayed host-less forever.
//
// Session-continuity merge intermediates (.merged.mp4) are held on disk by design
// until the live session ends and have no recordings row at all; a .merged. file
// that DOES have a row is a published recording, so it is judged like any other.
func orphanNeedsUpload(name string, hasRow, safeInCloud map[string]bool) bool {
	if strings.Contains(name, ".merged.") && !hasRow[name] {
		return false
	}
	return !safeInCloud[name]
}

// orphanIndexWarnThrottle keeps the degraded-mode warning to one line per
// interval: /api/orphans is polled every few seconds while the admin panel is
// open, so a persistent cause (typically the migration that adds the no-host
// function not being applied yet) would otherwise flood the log.
const orphanIndexWarnInterval = 5 * time.Minute

var (
	orphanIndexWarnMu   sync.Mutex
	orphanIndexWarnedAt time.Time
)

func logOrphanIndexUnavailable(err error) {
	orphanIndexWarnMu.Lock()
	defer orphanIndexWarnMu.Unlock()
	if time.Since(orphanIndexWarnedAt) < orphanIndexWarnInterval {
		return
	}
	orphanIndexWarnedAt = time.Now()
	log.Printf("[orphans] upload-link index unavailable (%v) — falling back to recordings-row existence", err)
}

// scanOrphanFiles scans videos/ and the configured OutputDir for video files
// that exist on disk but are not provably in the cloud yet (orphans).  That is
// either a file with no recordings row at all, or one whose recording has zero
// upload links.
// Sidecar parts (.video./.audio./.muxed.) are excluded — they are handled by
// the recovery pipeline (CleanupOrphanedFiles), not the orphan list.
func scanOrphanFiles() []orphanEntry {
	dirs := []string{"videos"}
	if server.Config != nil && server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	// Decide on upload LINKS, not row existence (see orphanNeedsUpload), using
	// the shared cached index — the admin panel polls this list every few seconds
	// and the reconciliation panel consults the same index.
	hasRow, safeInCloud := map[string]bool{}, map[string]bool{}
	if dbClient := server.GetDBClient(); dbClient != nil {
		if all, links, err := server.RecordingUploadLinkIndex(); err == nil {
			hasRow, safeInCloud = all, links
		} else {
			// Degrade to the old row-existence rule: with no link index the only
			// defensible answer is "a recordings row exists", so a Supabase
			// hiccup can never turn this list into every file on disk.
			logOrphanIndexUnavailable(err)
			if allRecs, rErr := dbClient.GetAllRecordings(); rErr == nil {
				for i := range allRecs {
					hasRow[allRecs[i].Filename] = true
					safeInCloud[allRecs[i].Filename] = true
				}
			}
		}
	}

	var orphans []orphanEntry
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".mp4" && ext != ".mkv" {
				continue
			}
			if strings.Contains(name, ".video.") || strings.Contains(name, ".audio.") || strings.Contains(name, ".muxed.") {
				continue
			}
			// Never force-upload ffmpeg finalizer scratch files — a crash
			// mid-finalize leaves a partial "<base>.finalizing.mp4" behind
			// that probes as corrupt and must never reach the hosts.
			if channel.IsFinalizingTemp(name) || strings.Contains(name, ".deleting.") {
				continue
			}
			if !orphanNeedsUpload(name, hasRow, safeInCloud) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			orphans = append(orphans, orphanEntry{
				Path:     filepath.Join(dir, name),
				Filename: name,
				Size:     info.Size(),
				ModTime:  info.ModTime().Format(time.RFC3339),
				Age:      time.Since(info.ModTime()).Round(time.Hour).String(),
			})
		}
	}
	return orphans
}

// RetryOrphan triggers thumbnail generation + upload for one or more orphan
// files.  Expects JSON body: {"paths": ["/path/to/file.mp4", ...]}.
func RetryOrphan(c *gin.Context) {
	var req struct {
		Paths []string `json:"paths"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Paths) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no paths provided"})
		return
	}

	type result struct {
		Path   string `json:"path"`
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}

	results := make([]result, len(req.Paths))
	var wg sync.WaitGroup
	for i, path := range req.Paths {
		abs, err := filepath.Abs(path)
		if err != nil || !isPathAllowed(abs) {
			results[i] = result{Path: path, Status: "failed", Error: "path not allowed"}
			continue
		}
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = result{Path: p, Status: "failed", Error: fmt.Sprintf("panic: %v", r)}
				}
			}()
			if channel.MaybeDeferToPending(p) {
				results[idx] = result{Path: p, Status: "deferred", Error: "below min-duration threshold — moved to pending"}
				return
			}
			thumb := channel.GenerateThumbnailForFile(p)
			if !channel.UploadOrphanedFile(p, thumb.ThumbURL, thumb.SpriteURL, thumb.PreviewURL) {
				results[idx] = result{Path: p, Status: "failed", Error: "upload did not complete successfully"}
				return
			}
			results[idx] = result{Path: p, Status: "success"}
		}(i, path)
	}
	wg.Wait()

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// rescanResult is one file's outcome from a manual output-dir rescan.
type rescanResult struct {
	Path   string `json:"path"`
	Status string `json:"status"` // "uploaded" | "failed"
	Error  string `json:"error,omitempty"`
}

// RescanOutputDir manually rescans the output directories and immediately
// runs every orphaned file through thumbnail generation + upload, without
// waiting for a restart or the periodic orphan-cleanup timer.
//
// This is a rescue action for stuck recordings: unlike the normal pipeline it
// force-uploads files even when a stale in-flight marker is present (the
// "already uploading, skipping duplicate" race that strands a moved file until
// restart).  UploadOrphanedFile re-marks the file in-flight and the upload
// journal dedups hosts that already have it, so a forced re-upload can never
// duplicate work that already completed.
//
// Two deliberate trade-offs:
//   - The request blocks until every upload attempt finishes (up to 3 retries
//     with 60s backoff per file), matching RetryOrphan.  The spawned uploads
//     are not tied to the request context, so they keep running even if the
//     browser or a tunnel drops the connection — only the JSON summary is
//     lost.
//   - A file the pipeline is GENUINELY mid-upload on right now can be
//     double-kicked (both uploaders may read an empty journal before either
//     writes).  Harm is bounded: it is the same file re-uploaded, and
//     SaveRecordingWithLinks upserts by filename.
func RescanOutputDir(c *gin.Context) {
	orphans := scanOrphanFiles()
	if len(orphans) == 0 {
		c.JSON(http.StatusOK, gin.H{"found": 0, "uploaded": 0, "failed": 0, "results": []rescanResult{}})
		return
	}

	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]rescanResult, 0, len(orphans))

	for _, o := range orphans {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			res := rescanResult{Path: path, Status: "uploaded"}
			func() {
				defer func() {
					if r := recover(); r != nil {
						res = rescanResult{Path: path, Status: "failed", Error: fmt.Sprintf("panic: %v", r)}
					}
				}()
				if channel.MaybeDeferToPending(path) {
					res = rescanResult{Path: path, Status: "deferred", Error: "below min-duration threshold — moved to pending"}
					return
				}
				thumb := channel.GenerateThumbnailForFile(path)
				if !channel.UploadOrphanedFile(path, thumb.ThumbURL, thumb.SpriteURL, thumb.PreviewURL) {
					res = rescanResult{Path: path, Status: "failed", Error: "upload did not complete successfully"}
				}
			}()

			mu.Lock()
			results = append(results, res)
			mu.Unlock()
		}(o.Path)
	}
	wg.Wait()

	uploaded, failed := 0, 0
	for _, r := range results {
		if r.Status == "failed" {
			failed++
		} else {
			uploaded++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"found":    len(orphans),
		"uploaded": uploaded,
		"failed":   failed,
		"results":  results,
	})
}

var (
	thumbCacheMu sync.Mutex
	thumbCache   = map[string]thumbCacheEntry{}
)

type thumbCacheEntry struct {
	data        []byte
	contentType string
	expiresAt   time.Time
}

// ServeLiveThumb serves a live thumbnail for a channel.  It always tries to
// extract a frame from the most recent recording file first (like Chaturbate's
// ri/{username}.jpg).  Falls back to the upstream CDN preview URL if no
// recording file is available or ffmpeg extraction fails.
// Cache TTL is kept short (2s ffmpeg, 5s CDN) so the frontend sees near-live
// updates while the stream is active.
func ServeLiveThumb(c *gin.Context) {
	username := c.Param("username")

	// Check cache first — 2s for ffmpeg frames, 5s for CDN proxy.
	thumbCacheMu.Lock()
	entry, cached := thumbCache[username]
	thumbCacheMu.Unlock()
	if cached && time.Now().Before(entry.expiresAt) {
		c.Data(http.StatusOK, entry.contentType, entry.data)
		return
	}

	// Find the most recent recording file.
	videoDir := server.Config.OutputDir
	if videoDir == "" {
		videoDir = "videos"
	}
	var newest string
	var newestMod time.Time
	for _, pat := range []string{
		filepath.Join(videoDir, username+"_*.mp4"),
		filepath.Join(videoDir, username+"_*.video.mp4"),
	} {
		matches, _ := filepath.Glob(pat)
		for _, m := range matches {
			st, err := os.Stat(m)
			if err != nil || st.Size() < 100*1024 {
				continue
			}
			if st.ModTime().After(newestMod) {
				newest = m
				newestMod = st.ModTime()
			}
		}
	}

	if newest != "" {
		cachePath := filepath.Join(os.TempDir(), "opencode-thumb-"+username+".webp")
		var thumbOK bool

		// Try up to 3 approaches:
		// 0 — fragmented MP4 demuxer (works for in-progress fMP4 without moov atom)
		// 1 — no special flags (standard MOV demuxer, works for completed files)
		// 2 — seek near the end (avoid blank first frame in completed files)
		for attempt := 0; attempt < 3 && !thumbOK; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := config.AcquireFFmpegFor(config.FFmpegAcquireTimeout); err != nil {
				// Pool starved — don't hold this request hostage (and don't
				// grab a slot until the whole function returns): fall back to
				// the CDN preview below.
				cancel()
				break
			}
			args := []string{"-y"}
			switch attempt {
			case 0:
				args = append(args,
					"-f", "mp4",
					"-flags", "+genpts",
					"-i", newest,
					"-vframes", "1",
					"-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
					"-c:v", "libwebp",
					"-quality", "80",
					cachePath,
				)
			case 1:
				args = append(args,
					"-i", newest,
					"-vframes", "1",
					"-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
					"-c:v", "libwebp",
					"-quality", "80",
					cachePath,
				)
			case 2:
				args = append(args,
					"-sseof", "-3",
					"-i", newest,
					"-vframes", "1",
					"-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
					"-c:v", "libwebp",
					"-quality", "80",
					cachePath,
				)
			}
			err := config.FFmpegCommandContext(ctx, args...).Run()
			config.ReleaseFFmpeg() // release immediately — the slot must not be held across attempts
			cancel()
			if err == nil {
				data, readErr := os.ReadFile(cachePath)
				if readErr == nil {
					thumbOK = true
					ct := http.DetectContentType(data)
					thumbCacheMu.Lock()
					// Short TTL (2s) so the frontend gets near-live updates
					// while the stream is being recorded.
					thumbCache[username] = thumbCacheEntry{data: data, contentType: ct, expiresAt: time.Now().Add(2 * time.Second)}
					thumbCacheMu.Unlock()
					c.Data(http.StatusOK, ct, data)
				}
			}
		}
		if thumbOK {
			return
		}
	}

	// Fall back to upstream CDN preview.
	var liveThumbURL string
	for _, ch := range server.Manager.ChannelInfo() {
		if ch.Username == username {
			liveThumbURL = ch.LiveThumbURL
			break
		}
	}
	if liveThumbURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	client := internal.NewReq()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := client.GetBytes(ctx, liveThumbURL)
	if err != nil {
		log.Printf("[thumb:proxy] fetch for %s: %v", username, err)
		c.Status(http.StatusGatewayTimeout)
		return
	}

	ct := http.DetectContentType(data)
	thumbCacheMu.Lock()
	thumbCache[username] = thumbCacheEntry{data: data, contentType: ct, expiresAt: time.Now().Add(5 * time.Second)}
	thumbCacheMu.Unlock()
	c.Data(http.StatusOK, ct, data)
}

// proxyAllowedSuffixes are the image-host suffixes we will fetch on behalf of
// the browser.  Anything else is rejected so the DVR cannot be used as an open
// proxy.
var proxyAllowedSuffixes = []string{
	"pixhost.to", "catbox.moe", "freeimage.host", "i.ibb.co", "pimpandhost.com", "imgchest.com", "imgbox.com", "imgbb.com", "iili.io", "s1q2105.com",
}

// ServeImageProxy streams an external thumbnail/sprite/preview image through the
// DVR server itself.  The browser requests a same-origin URL
// (/api/imgproxy?url=...), so broken thumbnails caused by the client being
// unable to reach the upstream image host (firewall / region / DNS / hotlink
// rules) disappear — the server fetches the bytes (which it demonstrably can)
// and relays them.
func ServeImageProxy(c *gin.Context) {
	raw := c.Query("url")
	if raw == "" {
		c.Status(http.StatusBadRequest)
		return
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		c.Status(http.StatusBadRequest)
		return
	}
	allowed := false
	for _, s := range proxyAllowedSuffixes {
		if strings.HasSuffix(u.Host, s) {
			allowed = true
			break
		}
	}
	if !allowed {
		c.Status(http.StatusForbidden)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; chaturbate-dvr/1.0)")
	req.Header.Set("Accept", "image/*,*/*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[imgproxy] fetch failed for %s: %v", raw, err)
		c.Status(http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.Status(http.StatusBadGateway)
		return
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = mime.TypeByExtension(filepath.Ext(u.Path))
	}
	c.Header("Cache-Control", "public, max-age=86400")
	c.DataFromReader(resp.StatusCode, resp.ContentLength, ct, resp.Body, nil)
}

func DeleteOrphans(c *gin.Context) {
	var req struct {
		Paths []string `json:"paths"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Paths) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no paths provided"})
		return
	}

	deleted := 0
	var errors []string
	for _, path := range req.Paths {
		abs, err := filepath.Abs(path)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: bad path", path))
			continue
		}
		if !isPathAllowed(abs) {
			errors = append(errors, fmt.Sprintf("%s: path not allowed", path))
			continue
		}
		if err := os.Remove(abs); err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", path, err))
		} else {
			deleted++
		}
	}

	resp := gin.H{"deleted": deleted}
	if len(errors) > 0 {
		resp["errors"] = errors
	}
	c.JSON(http.StatusOK, resp)
}

// ─── Nodes Dashboard ─────────────────────────────────────────────────────────

// sumNodeLoad totals the current load across nodes that can still hold
// channels (online/draining). A node's current_load is only written by its own
// heartbeat, so an offline node's value is frozen at its last report and counts
// channels that were already reclaimed — including it would inflate the pool's
// Total Load (e.g. a dead node stuck at 280 while holding 0 assignments).
func sumNodeLoad(nodes []database.Node) int {
	total := 0
	for _, n := range nodes {
		if n.Status != "offline" {
			total += n.CurrentLoad
		}
	}
	return total
}

// NodesData represents the data structure for the nodes page.
type NodesData struct {
	Nodes         []database.Node
	OnlineCount   int
	DrainingCount int
	TotalLoad     int
	Mode          string
	MyNodeID      string
}

// NodesPage renders the nodes dashboard.
func NodesPage(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=15")

	client := server.GetDBClient()
	var nodes []database.Node
	if client != nil {
		var err error
		nodes, err = client.GetAllNodes()
		if err != nil {
			fmt.Printf("[WARN] failed to load nodes: %v\n", err)
		}
	}

	onlineCount := 0
	drainingCount := 0
	for _, n := range nodes {
		if n.Status == "online" {
			onlineCount++
		} else if n.Status == "draining" {
			drainingCount++
		}
	}
	// Only online/draining nodes contribute to Total Load. An offline
	// node's current_load is frozen at its last heartbeat and counts
	// channels that were already reclaimed — including it would inflate
	// the metric (e.g. a dead node stuck at 280 while holding 0).
	totalLoad := sumNodeLoad(nodes)

	c.HTML(200, "nodes.html", &NodesData{
		Nodes:         nodes,
		OnlineCount:   onlineCount,
		DrainingCount: drainingCount,
		TotalLoad:     totalLoad,
		Mode:          server.ChannelPoolMode(),
		MyNodeID:      server.NodeID(),
	})
}

// ─── Pool Editor ──────────────────────────────────────────────────────────────

// PoolEntry is a unified row for the pool editor page.  It represents either a
// distributed channel assignment (pooled mode, source="pool") or a locally
// configured channel (isolated mode, source="local").
//
// JSON tags are snake_case to match what pool.html's JavaScript reads
// (a.username, a.site, a.assigned_node, a.is_live, ...). Without them
// encoding/json emits PascalCase keys and every row renders as "undefined".
type PoolEntry struct {
	Username     string `json:"username"`
	Site         string `json:"site"`
	AssignedNode string `json:"assigned_node"`
	Status       string `json:"status"`
	IsLive       bool   `json:"is_live"`
	Resolution   int    `json:"resolution"`
	Framerate    int    `json:"framerate"`
	AssignedAt   string `json:"assigned_at"`
	Source       string `json:"source"` // "pool" (channel_assignments) or "local" (configured channels)

	// Local runtime state — only populated for channels owned by THIS node
	// (pooled mode) or all channels (isolated mode). Paused means the channel
	// is paused locally while still assigned in the DB; Uploading/Pending mean
	// the channel is actively processing uploads. The fleet stuck-pause
	// monitor reads these flags from each node's /api/pool to detect
	// paused-but-still-assigned channels (excluding legitimate pauses during
	// the session processing phase and manual user pauses).
	Paused      bool   `json:"paused"`
	PauseReason string `json:"pause_reason"` // why paused: manual / session-boundary / handoff (empty = unknown)
	Uploading   bool   `json:"uploading"`
	Pending     bool   `json:"pending"`
}

// PoolData represents the data structure for the pool editor page.
type PoolData struct {
	Assignments []PoolEntry
	Mode        string
}

// poolEntries returns the unified pool rows for the editor page.  In pooled
// mode (CHANNEL_POOL_MODE=pooled) the channel_assignments table is the source
// of truth; in isolated mode the locally configured channels (server.Manager)
// are shown so the page is never empty when channels exist.
// hasOnDiskPending reports whether username has files waiting in the on-disk
// .pending queue (segments the pipeline has not finished muxing/uploading).
// Mirrors manager.HasPendingSegments' directory semantics so the pool API and
// the shuffle guard agree on what counts as pending work.
func hasOnDiskPending(username string) bool {
	dir := "videos"
	if server.Config != nil && server.Config.OutputDir != "" {
		dir = server.Config.OutputDir
	}
	entries, err := os.ReadDir(filepath.Join(dir, ".pending", username))
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return true
		}
	}
	return false
}

// localChannelState snapshots the runtime state (paused / uploading / pending)
// of this node's local channels, keyed by username. The flags feed the pool
// API so the fleet stuck-pause monitor can see each node's local pause state
// through its /api/pool endpoint. Uploading/Pending exclude channels that are
// paused only while the session processing phase muxes/uploads their files —
// a channel with active upload work, queued pipeline work, or on-disk .pending
// segments is mid-processing, never "stuck".
func localChannelState() (map[string]*entity.ChannelInfo, map[string]bool) {
	info := map[string]*entity.ChannelInfo{}
	pending := map[string]bool{}
	if server.Manager == nil {
		return info, pending
	}
	for _, ch := range server.Manager.ChannelInfo() {
		info[ch.Username] = ch
		if hasOnDiskPending(ch.Username) {
			pending[ch.Username] = true
		}
	}
	for _, p := range server.Manager.UploadEntries().Pending {
		pending[p.Channel] = true
	}
	return info, pending
}

// poolEntryLocalState fills the paused/uploading/pending flags of a PoolEntry
// from local channel state. Returns true when local state was found.
func poolEntryLocalState(e *PoolEntry, info map[string]*entity.ChannelInfo, pending map[string]bool) bool {
	ch, ok := info[e.Username]
	if !ok {
		return false
	}
	e.Paused = ch.IsPaused
	e.PauseReason = ch.PauseReason
	e.Uploading = ch.UploadStatus != "" || ch.IsCompressing
	e.Pending = pending[e.Username]
	return true
}

func poolEntries() ([]PoolEntry, string) {
	mode := server.ChannelPoolMode()
	var entries []PoolEntry

	localInfo, localPending := localChannelState()

	if server.IsPooledMode() {
		client := server.GetDBClient()
		if client == nil {
			return nil, mode
		}
		assignments, err := client.GetAllAssignments()
		if err != nil {
			fmt.Printf("[WARN] failed to load assignments: %v\n", err)
			return nil, mode
		}
		for _, a := range assignments {
			e := PoolEntry{
				Username:     a.Username,
				Site:         a.Site,
				AssignedNode: a.AssignedNode,
				Status:       a.Status,
				IsLive:       a.IsLive,
				Resolution:   a.Resolution,
				Framerate:    a.Framerate,
				AssignedAt:   a.AssignedAt,
				Source:       "pool",
			}
			// Paused state is only meaningful on the node that owns the
			// assignment — another node's /api/pool must not claim to know it.
			if a.AssignedNode == server.NodeID() {
				poolEntryLocalState(&e, localInfo, localPending)
			}
			entries = append(entries, e)
		}
		return entries, mode
	}

	// Isolated mode — show the locally configured channels.
	if server.Manager != nil {
		for _, ch := range server.Manager.ChannelInfo() {
			status := "offline"
			switch {
			case ch.IsPaused:
				status = "paused"
			case ch.IsOnline:
				status = "recording"
			case ch.IsConnecting:
				status = "connecting"
			}
			e := PoolEntry{
				Username: ch.Username,
				Site:     ch.Site,
				Status:   status,
				IsLive:   ch.IsOnline,
				Source:   "local",
			}
			poolEntryLocalState(&e, localInfo, localPending)
			entries = append(entries, e)
		}
	}
	return entries, mode
}

// PoolPage renders the pool editor page.
func PoolPage(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=15")

	entries, mode := poolEntries()
	c.HTML(200, "pool.html", &PoolData{
		Assignments: entries,
		Mode:        mode,
	})
}

// ─── Nodes API ────────────────────────────────────────────────────────────────

// GetNodesJSON returns all nodes as JSON.
func GetNodesJSON(c *gin.Context) {
	client := server.GetDBClient()
	if client == nil {
		c.JSON(http.StatusOK, gin.H{"nodes": []database.Node{}})
		return
	}

	nodes, err := client.GetAllNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"nodes": nodes})
}

// GetPoolJSON returns all pool entries as JSON (assignments in pooled mode,
// configured channels in isolated mode).
func GetPoolJSON(c *gin.Context) {
	entries, mode := poolEntries()
	c.JSON(http.StatusOK, gin.H{"mode": mode, "assignments": entries})
}

// PoolCheckResponse is the JSON response for the realtime pool channel checker.
type PoolCheckResponse struct {
	Exists  bool   `json:"exists"`
	Source  string `json:"source,omitempty"`
	IsLive  bool   `json:"isLive"`
	Message string `json:"message"`
}

// poolChannelExists reports whether a channel already exists in any known
// location: locally configured channels, the shared pool (channel_assignments,
// pooled mode only), or the Supabase channels table.  Returns the source name
// and a human-readable message describing where the duplicate was found.
func poolChannelExists(username, site string) (bool, string, string) {
	// 1. Locally configured channels (this node / instance).
	if server.Manager != nil {
		for _, ch := range server.Manager.ChannelInfo() {
			if strings.EqualFold(ch.Username, username) {
				return true, "configured",
					fmt.Sprintf("Channel %q already exists — it is already configured on this node", username)
			}
		}
	}

	client := server.GetDBClient()
	if client == nil {
		return false, "", ""
	}

	// 2. Shared pool (channel_assignments) — relevant only in pooled mode.
	if server.IsPooledMode() {
		if a, err := client.GetAssignment(username, site); err == nil && a != nil {
			return true, "pool", fmt.Sprintf("Channel %q is already in the pool", username)
		}
		// The pool key is case-sensitive but cam-site usernames are not, so an
		// exact lookup misses a differently-cased duplicate (the catalogue holds
		// both LunaLeonCoupleWife and lunaleoncouplewife) and the add would create
		// a second row — two nodes recording the same room.
		if fold, err := client.AssignmentExistsFold(username, site); err == nil && fold {
			return true, "pool", fmt.Sprintf("Channel %q is already in the pool", username)
		}
	}

	// 3. Supabase channels table (case-insensitive).  Only a duplicate in
	// isolated mode, where the catalogue is what the node records from.  In
	// pooled mode channel_assignments is authoritative and the catalogue is just
	// "channels we have recorded at some point": refusing to pool those made
	// every previously-recorded channel impossible to re-add (the pool editor
	// answered "already exists in the database" and the pool could never grow
	// past the historical set), even though nothing was recording them.
	if !server.IsPooledMode() {
		if exists, err := client.ChannelExists(username); err == nil && exists {
			return true, "database", fmt.Sprintf("Channel %q already exists in the database", username)
		}
	}

	return false, "", ""
}

// CheckPoolChannel checks in realtime whether a channel already exists
// (configured locally, in the pool, or in the database) so the pool editor can
// skip duplicates before submitting.  It also reports whether the channel is
// currently live so the user can see before adding it.
func CheckPoolChannel(c *gin.Context) {
	username := strings.TrimSpace(c.Query("username"))
	if username == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "username is required"})
		return
	}
	siteName := strings.TrimSpace(c.Query("site"))
	if siteName == "" {
		siteName = "chaturbate"
	}

	exists, source, message := poolChannelExists(username, siteName)

	isLive := false
	if server.Config != nil {
		var siteImpl site.Site
		switch siteName {
		case "stripchat":
			siteImpl = site.NewStripchatSite()
		default:
			siteImpl = site.NewChaturbateSite()
		}
		status, err := siteImpl.GetRoomStatus(context.Background(), internal.NewReq(), username)
		if err == nil {
			isLive = status == site.StatusPublic || status == site.StatusPrivate
		}
	}

	c.JSON(http.StatusOK, PoolCheckResponse{Exists: exists, Source: source, IsLive: isLive, Message: message})
}

// PoolAddRequest is the request body for adding a channel to the pool.
type PoolAddRequest struct {
	Site        string `json:"site" form:"site"`
	Username    string `json:"username" form:"username" binding:"required"`
	Resolution  int    `json:"resolution" form:"resolution"`
	Framerate   int    `json:"framerate" form:"framerate"`
	MaxDuration int    `json:"max_duration" form:"max_duration"`
}

// AddToPool adds a channel to the pool.  In pooled mode this creates a
// channel_assignments row; in isolated mode it adds a locally configured
// channel.  Duplicates are rejected with a 409 so the UI can show why the
// add was skipped.
func AddToPool(c *gin.Context) {
	var req PoolAddRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	if req.Site == "" {
		req.Site = "chaturbate"
	}
	if req.Resolution == 0 {
		req.Resolution = 2160
	}
	if req.Framerate == 0 {
		req.Framerate = 60
	}
	// Default max_duration to the global --max-duration value (90) so newly
	// added pool channels rotate every 1h30m unless explicitly overridden.
	if req.MaxDuration <= 0 {
		req.MaxDuration = 90
	}
	req.Username = strings.TrimSpace(req.Username)

	// Duplicate guard: never add a channel that already exists anywhere.
	if exists, _, message := poolChannelExists(req.Username, req.Site); exists {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": message})
		return
	}

	if !server.IsPooledMode() {
		// Isolated mode: create a locally configured channel.
		if server.Manager == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "manager not initialized"})
			return
		}
		conf := &entity.ChannelConfig{
			Site:        req.Site,
			Username:    req.Username,
			Framerate:   req.Framerate,
			Resolution:  req.Resolution,
			MaxDuration: req.MaxDuration,
			CreatedAt:   time.Now().Unix(),
		}
		if err := server.Manager.CreateChannel(conf, true); err != nil {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}

	client := server.GetDBClient()
	if client == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Supabase not configured"})
		return
	}

	assignment := database.ChannelAssignment{
		Username:    req.Username,
		Site:        req.Site,
		Status:      "unassigned",
		Resolution:  req.Resolution,
		Framerate:   req.Framerate,
		MaxDuration: req.MaxDuration,
	}

	// Register the channel in the channels catalogue (best-effort). The pool is
	// authoritative — the controller no longer deletes assignments against the
	// catalogue — but keeping the row there means external tools and the
	// isolated-mode duplicate check still see every pooled channel.
	if err := client.EnsureChannelExists(req.Username); err != nil {
		fmt.Printf("[WARN] AddToPool: register %q in channels catalogue: %v\n", req.Username, err)
	}

	if err := client.BulkInsertAssignments([]database.ChannelAssignment{assignment}); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// PoolRemoveRequest is the request body for removing a channel from the pool.
type PoolRemoveRequest struct {
	Username string `json:"username" binding:"required"`
	Site     string `json:"site"`
}

// RemoveFromPool removes a channel from the pool.  In pooled mode the
// channel_assignments row is deleted; in isolated mode the locally configured
// channel is stopped and removed.
func RemoveFromPool(c *gin.Context) {
	var req PoolRemoveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	if req.Site == "" {
		req.Site = "chaturbate"
	}

	if !server.IsPooledMode() {
		if server.Manager == nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "manager not initialized"})
			return
		}
		if err := server.Manager.StopChannel(req.Username); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}

	client := server.GetDBClient()
	if client == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Supabase not configured"})
		return
	}

	// Delete the assignment row
	if err := client.DeleteAssignment(req.Username, req.Site); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}
