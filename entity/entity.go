package entity

import (
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// Channel pool mode constants
const (
	PoolModeIsolated = "isolated"
	PoolModePooled   = "pooled"
)

// Event represents the type of event for the channel.
type Event = string

const (
	EventUpdate Event = "update"
	EventLog    Event = "log"
)

// ChannelConfig represents the configuration for a channel.
type ChannelConfig struct {
	IsPaused                atomic.Bool `json:"-"`
	Site                    string      `json:"site"` // "chaturbate" or "stripchat"
	Username                string      `json:"username"`
	Framerate               int         `json:"framerate"`
	Resolution              int         `json:"resolution"`
	Pattern                 string      `json:"pattern"`
	MaxDuration             int         `json:"max_duration"`
	MaxFilesize             int         `json:"max_filesize"`
	Compress                bool        `json:"compress"`
	MinDurationBeforeUpload int         `json:"min_duration_before_upload"` // seconds; 0 = disabled
	CreatedAt               int64       `json:"created_at"`

	// Persisted metadata — captured from the site API even when offline so
	// restarts don't lose them (drives the archive site).
	RoomTitle        string `json:"room_title,omitempty"`
	Gender           string `json:"gender,omitempty"`
	SummaryCardImage string `json:"summary_card_image,omitempty"`
	StreamedAt       int64  `json:"streamed_at,omitempty"`
}

// NormalizeFinalizeMode returns a supported finalization mode
// ("none", "remux", or "transcode"), defaulting to "none".
func NormalizeFinalizeMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "remux":
		return "remux"
	case "transcode":
		return "transcode"
	default:
		return "none"
	}
}

func (c *ChannelConfig) Sanitize() {
	c.Username = regexp.MustCompile(`[^a-zA-Z0-9_-]`).ReplaceAllString(c.Username, "")
	c.Username = strings.TrimSpace(c.Username)
	if c.Site == "" {
		c.Site = "chaturbate"
	}
	if c.Resolution == 0 {
		c.Resolution = 2160
	}
	// MaxDuration 0 means "no rotation" (the web UI shows ∞ and the
	// --max-duration flag help says '0 to disable'). Previously this was
	// silently overridden to 160, so channels configured unbounded actually
	// rotated every 160 minutes — the recordings table shows 704 of 924
	// assignments carrying that phantom 160. Keep 0 as 0.
	if c.Pattern == "" {
		c.Pattern = "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}"
	}
	if c.Framerate == 0 {
		c.Framerate = 60
	}
}

// PauseReason identifies why a channel is paused, so automatic resume paths
// (session boundary, claim/reconcile) can auto-recover automatic pauses
// without overriding the user's explicit UI pause.
type PauseReason string

const (
	PauseReasonManual   PauseReason = "manual"           // user clicked Pause in the web UI
	PauseReasonBoundary PauseReason = "session-boundary" // session stop / processing phase
	PauseReasonHandoff  PauseReason = "handoff"          // channel being moved to another node
)

// ChannelInfo represents the information about a channel,
// mostly used for the template rendering.
type ChannelInfo struct {
	IsOnline      bool
	IsConnecting  bool
	IsPaused      bool
	IsCompressing bool
	RoomStatus    string // public, private, group, away, offline, hidden
	// PauseReason explains WHY a paused channel is paused (empty when not
	// paused or the reason is unknown/legacy). "manual" pauses are sticky and
	// are never auto-resumed or flagged as stuck.
	PauseReason string
	// AutoResumedFromPause is true when the channel was automatically resumed
	// from a stuck paused-but-still-assigned state (the claim cycle found it
	// paused while the DB still assigned it to this node). The node web UI
	// shows a badge so the recovery is visible.
	AutoResumedFromPause bool
	Username             string
	Site                 string // "chaturbate" or "stripchat"
	SiteDomain           string // domain for channel link, e.g. "https://www.cb.xxx/"
	LiveThumbURL         string // live-updating thumbnail; empty = use platform default
	Duration             string
	Filesize             string
	TotalDiskUsage       string // total bytes across all recordings for this channel
	Filename             string
	StreamedAt           string
	MaxDuration          string
	MaxFilesize          string
	CreatedAt            int64
	Logs                 []string
	GlobalConfig         *Config // for nested template to access $.Config
	UploadStatus         string  // human-readable upload status (empty = idle)
	UploadProgress       float64 // 0–100 upload progress estimate
	UploadFilename       string  // file currently being uploaded

	// Persisted room metadata (shown even when offline).
	RoomTitle        string
	Gender           string
	NumViewers       int
	EdgeRegion       string
	SummaryCardImage string
}

// HostEntry holds live upload progress for a single host.
type HostEntry struct {
	Host         string  `json:"host"`          // host name (GoFile, VOE.sx, etc.)
	Status       string  `json:"status"`        // "uploading", "done", "failed"
	Progress     float64 `json:"progress"`      // 0–100
	BytesCurrent int64   `json:"bytes_current"` // bytes uploaded so far
	BytesTotal   int64   `json:"bytes_total"`   // total file size
	Speed        string  `json:"speed"`         // formatted speed, e.g. "2.5 MB/s"
}

// UploadEntry holds upload progress for a single channel's active upload.
type UploadEntry struct {
	Channel      string      `json:"channel"`       // which channel is uploading
	Filename     string      `json:"filename"`      // file being uploaded
	Status       string      `json:"status"`        // human-readable status
	Progress     float64     `json:"progress"`      // 0–100
	HostCount    int         `json:"host_count"`    // how many hosts completed
	HostTotal    int         `json:"host_total"`    // total hosts to upload to
	BytesCurrent int64       `json:"bytes_current"` // total bytes uploaded so far across all hosts
	BytesTotal   int64       `json:"bytes_total"`   // total file size
	Speed        string      `json:"speed"`         // formatted aggregate speed, e.g. "3.2 MB/s"
	Hosts        []HostEntry `json:"hosts"`         // per-host progress
}

// UploadState holds live upload progress data for the global session timer UI.
type UploadState struct {
	Active   bool          `json:"active"`   // true if any channel is uploading
	Channels []UploadEntry `json:"channels"` // all active uploads
}

// PendingEntry describes a file queued for processing but not yet uploading.
type PendingEntry struct {
	Channel  string `json:"channel"`  // username
	Filename string `json:"filename"` // file name
	Stage    string `json:"stage"`    // human-readable current stage
	Failed   bool   `json:"failed"`
	Error    string `json:"error,omitempty"`
	Size     int64  `json:"size"` // final on-disk file size in bytes (0 when unknown)
}

// UploadsResponse is the full JSON body returned by GET /api/uploads.
type UploadsResponse struct {
	Active  []UploadEntry  `json:"active"`  // currently uploading per-channel
	Pending []PendingEntry `json:"pending"` // queued and waiting for processing
	History []PendingEntry `json:"history"` // recently completed or failed pipelines

	// Processing is true while the node is in the post-session drain
	// (finalizing, compressing, uploading queued recordings).
	Processing bool `json:"processing"`
}

// DiskInfo holds disk usage information for the UI.
type DiskInfo struct {
	Total   string // formatted, e.g. "256.00 GB"
	Used    string // formatted, e.g. "120.50 GB"
	Free    string // formatted, e.g. "135.50 GB"
	Percent int    // 0-100
	UsedGB  float64
	TotalGB float64
}

// Config holds the configuration for the application.
type Config struct {
	Version       string
	Username      string
	AdminUsername string
	AdminPassword string
	Framerate     int
	Resolution    int
	Pattern       string
	MaxDuration   int
	MaxFilesize   int
	Compress      bool
	Port          string
	Interval      int
	Cookies       string
	SessionID     string
	Csrftoken     string
	CfClearance   string
	UserAgent     string
	Domain        string

	OutputDir               string
	PerModelFolder          bool
	DeleteLocalAfterUpload  bool
	MinDurationBeforeUpload int // seconds; 0 = disabled; videos shorter than this are deferred for merge
	MinFileSizeBeforeUpload int // MB; 0 = disabled; files smaller than this are crash artifacts and are discarded
	OrphanCleanupInterval   int // minutes between periodic orphan/thumbnail sweeps (0 = disabled)
	LogRetentionDays        int // prune pure telemetry (channel_logs/disk_usage) older than this (0 = disabled, default 3)
	DiskWarningPercent      int // log warning when disk usage exceeds this (0 = disabled, default 80)
	DiskCriticalPercent     int // auto-delete oldest recordings when disk exceeds this (0 = disabled, default 90)
	MaxLocalAgeDays         int // delete local files older than N days if uploaded (0 = disabled)

	VoeSXAPIKey     string
	StreamtapeLogin string
	StreamtapeKey   string
	MixdropEmail    string
	MixdropToken    string
	VidaraKey       string
	CatboxProxyURL  string

	// DisabledUploadHosts lists upload hosts that must never be attempted this
	// run (e.g. permanent/size-cap failures), regardless of whether their API
	// keys are configured. Comma-separated e.g. "AnonMP4,VOE.sx".
	DisabledUploadHosts []string

	// AnonMP4ViewBase is the base URL that AnonMP4 watch/embed links from the
	// uploader are normalized to (the site rotates its public view domain; a
	// returned domain that stopped serving videos would otherwise poison the
	// archive). Empty uses the built-in default.
	AnonMP4ViewBase string

	// Upload throughput tuning.
	UploadMaxConcurrent   int // max video files uploading concurrently (0 = auto, VM-sized)
	UploadHostConcurrency int // max concurrent uploads per host (0 = auto, VM-sized; GoFile gets 1.5×)
	PipelineWorkers       int // concurrent pipelines per channel queue (0 = auto, VM-sized)
	RetryWorkers          int // concurrent upload jobs in the retry pool (0 = auto, VM-sized)

	SupabaseURL            string
	SupabaseAPIKey         string
	SupabaseServiceRoleKey string

	StripchatPDKey string

	// AffiliateWM is the webmaster code for the affiliate onlinerooms API
	// used as a bulk liveness check (single call covers all channels).
	AffiliateWM string

	FFmpegPath string

	// Finalization: how closed recordings are post-processed
	// ("none" = BuildSeekIndex only, "remux" / "transcode" = ffmpeg).
	CompletedDir    string
	FinalizeMode    string
	FFmpegEncoder   string
	FFmpegContainer string
	FFmpegQuality   int
	FFmpegPreset    string
	Debug           bool

	// Notifications (Discord + ntfy) — persisted via settings, editable in web UI.
	NtfyURL             string
	NtfyTopic           string
	NtfyToken           string
	DiscordWebhookURL   string
	CFChannelThreshold  int // consecutive CF blocks before per-channel alert; default 5
	CFGlobalThreshold   int // channels hitting CF in same window for global alert; default 3
	NotifyCooldownHours int // hours between repeated alerts of the same type; default 4
	NotifyStreamOnline  bool

	// CFRetryMinutes is how long a Cloudflare-blocked channel waits before
	// retrying (default 5). Retrying every minute across hundreds of blocked
	// channels hammers Cloudflare and keeps the block alive; a longer backoff
	// gives the automatic cookie refresh time to mint a fresh cf_clearance.
	CFRetryMinutes int
	// CFStarvedThreshold is how many channels must be simultaneously
	// Cloudflare-blocked before the node is considered CF-starved: it stops
	// claiming new channels, sheds its excess claims back to the pool (so
	// healthy nodes can record them), and triggers a cookie re-mint.
	CFStarvedThreshold int
	// CFSessionCutThreshold is how many distinct channels must hit a
	// node-wide session-failure signature within the detection window before
	// the node re-mints cookies (default 3). Both signatures feed the SAME
	// detector: a session cut (CDN HLS 403/404 with a failing site-API probe)
	// or a Cloudflare-block burst. This catches a node-wide Chaturbate
	// session invalidation before every channel 404s and the recordings split.
	CFSessionCutThreshold int
	// CFRefreshMin is the minimum minutes between automatic cookie refresh
	// attempts triggered by persistent Cloudflare blocks (default 10).
	CFRefreshMin int

	SessionDuration       string        // recording session length (e.g. "5h20m0s"); empty = disabled (continuous recording)
	SessionDurationParsed time.Duration // parsed from SessionDuration; 0 = disabled

	// DisableEarlyDrain stops the session-loop's early-final-drain heuristic:
	// channels are never forced to stop before the full session duration
	// elapses, even when the pending upload backlog risks losing files at the
	// runner's hard deadline.  The backlog is drained at the natural boundary
	// instead.  Default false (early drain enabled).
	DisableEarlyDrain bool

	// Distributed shards/nodes configuration
	NodeID          string // unique node identifier (auto-detected if empty)
	ChannelPoolMode string // "isolated" (default) or "pooled"
}
