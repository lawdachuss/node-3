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

	// Session
	SessionActive    bool
	SessionRemaining string
	SessionDuration  string

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

	// Distributed shards
	Nodes           []database.Node
	Assignments     []database.ChannelAssignment
	OnlineNodes     int
	DrainingNodes   int
	TotalNodeLoad   int
	PoolMode        string
	MyNodeID        string
}

// AdminPage renders the admin panel with deep upload/orphan matrices.
func AdminPage(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=15")

	channels := server.Manager.ChannelInfo()
	uploads := server.Manager.UploadEntries()

	// ── Orphans ──
	dirs := []string{"videos"}
	if server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}
	uploaded := map[string]bool{}
	allRecs, _ := server.GetDBClient().GetAllRecordings()
	for i := range allRecs {
		uploaded[allRecs[i].Filename] = true
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
			if uploaded[name] {
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

	// ── Session info ──
	var sessionActive bool
	var sessionRemaining, sessionDuration string
	remaining, active := server.Manager.SessionInfo()
	if active {
		sessionActive = true
		sessionRemaining = remaining.Round(time.Second).String()
		sessionDuration = server.Config.SessionDurationParsed.Round(time.Second).String()
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
		totalNodeLoad += n.CurrentLoad
	}

	c.HTML(200, "admin.html", &AdminData{
		Config:   server.Config,
		Channels: channels,
		Disk:     server.GetDiskInfo(),
		Uploads:  uploads,
		Orphans:  orphans,

		SessionActive:    sessionActive,
		SessionRemaining: sessionRemaining,
		SessionDuration:  sessionDuration,

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
	})
}

// CreateChannelRequest represents the request body for creating a channel.
type CreateChannelRequest struct {
	Site                    string `form:"site"`
	Username                string `form:"username" binding:"required"`
	Framerate               int    `form:"framerate"`
	Resolution              int    `form:"resolution"`
	Pattern                 string `form:"pattern"`
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

	// Default to 4K / 60 FPS / standard pattern when not specified.
	if req.Site == "" {
		req.Site = "chaturbate"
	}
	if req.Resolution == 0 {
		req.Resolution = 2160
	}
	if req.Framerate == 0 {
		req.Framerate = 60
	}
	if req.Pattern == "" {
		req.Pattern = "standard"
	}

	usernames := strings.Split(req.Username, ",")
	if client := server.GetDBClient(); client != nil {
		var conflicts []string
		for _, username := range usernames {
			// Check channels table (isolated mode)
			existing, err := client.GetChannel(username)
			if err == nil && existing != nil {
				conflicts = append(conflicts, username)
				continue
			}
			// Check pool assignments table (pooled mode)
			site := req.Site
			if site == "" {
				site = "chaturbate"
			}
			assignment, aErr := client.GetAssignment(username, site)
			if aErr == nil && assignment != nil {
				conflicts = append(conflicts, username+" (in pool)")
			}
		}
		if len(conflicts) > 0 {
			c.String(http.StatusConflict, "Channel(s) already exist: %s", strings.Join(conflicts, ", "))
			return
		}
	}

	var lastErr error
	for _, username := range usernames {
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
	Cookies          string `json:"cookies" form:"cookies"`
	SessionID        string `json:"sessionid" form:"sessionid"`
	Csrftoken        string `json:"csrftoken" form:"csrftoken"`
	CfClearance      string `json:"cf_clearance" form:"cf_clearance"`
	UserAgent        string `json:"user_agent" form:"user_agent"`
	VoeSXAPIKey      string `json:"voesx_api_key" form:"voesx_api_key"`
	StreamtapeLogin  string `json:"streamtape_login" form:"streamtape_login"`
	StreamtapeKey    string `json:"streamtape_key" form:"streamtape_key"`
	MixdropEmail     string `json:"mixdrop_email" form:"mixdrop_email"`
	MixdropToken     string `json:"mixdrop_token" form:"mixdrop_token"`
	SeekStreamingKey string `json:"seekstreaming_key" form:"seekstreaming_key"`
	VidHideAPIKey    string `json:"vidhide_api_key" form:"vidhide_api_key"`
	StreamWishAPIKey string `json:"streamwish_api_key" form:"streamwish_api_key"`
	StripchatPDKey   string `json:"stripchat_pdkey" form:"stripchat_pdkey"`
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
		server.Config.Cookies = req.Cookies
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
		server.Config.CfClearance = req.CfClearance
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

	// Update uploader credentials
	if req.VoeSXAPIKey != "" || req.StreamtapeLogin != "" || req.StreamtapeKey != "" || req.MixdropEmail != "" || req.MixdropToken != "" ||
		req.SeekStreamingKey != "" || req.VidHideAPIKey != "" || req.StreamWishAPIKey != "" {
		server.UpdateUploaderCredentials(req.VoeSXAPIKey, req.StreamtapeLogin, req.StreamtapeKey, req.MixdropEmail, req.MixdropToken,
			req.SeekStreamingKey, req.VidHideAPIKey, req.StreamWishAPIKey)
	}

	if err := server.SaveSettings(); err != nil {
		fmt.Printf("[WARN] could not save settings: %v\n", err)
	}

	if c.ContentType() == "application/json" {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// extractCookieValue parses a value for the given cookie name from a cookie string.
func extractCookieValue(cookieStr, name string) string {
	for _, pair := range strings.Split(cookieStr, ";") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == name {
			return strings.TrimSpace(parts[1])
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

	if _, err := file.Seek(start, 0); err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	io.CopyN(c.Writer, file, contentLength)
}

func detectVideoMIME(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".mp4":
		return "video/mp4"
	case ".ts":
		return "video/mp2t"
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
		if code := extractFileCode(link); code != "" {
			return "https://streamtape.com/e/" + code + "/"
		}
		return link
	}
	if strings.Contains(normalizedHost, "mixdrop") || strings.Contains(normalizedLink, "mixdrop.") {
		if code := extractFileCode(link); code != "" {
			return "https://mixdrop.ag/e/" + code
		}
		return link
	}
	if strings.Contains(normalizedHost, "gofile") || strings.Contains(normalizedLink, "gofile.io/") {
		return ""
	}
	if strings.Contains(normalizedHost, "vidhide") || strings.Contains(normalizedLink, "morencius.com/") {
		if code := extractFileCode(link); code != "" {
			return "https://morencius.com/embed/" + code
		}
	}
	if strings.Contains(normalizedHost, "streamwish") || strings.Contains(normalizedLink, "hanerix.com/") {
		if code := extractFileCode(link); code != "" {
			return "https://hanerix.com/e/" + code
		}
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
			return "https://streamtape.com/e/" + code + "/"
		}
		return link
	case strings.Contains(normalizedHost, "mixdrop") || strings.Contains(normalizedLink, "mixdrop."):
		if code := extractFileCode(link); code != "" {
			return "https://mixdrop.ag/e/" + code
		}
		return link
	case strings.Contains(normalizedHost, "gofile") || strings.Contains(normalizedLink, "gofile.io/"):
		return ""
	case strings.Contains(normalizedHost, "vidhide") || strings.Contains(normalizedLink, "morencius.com/"):
		if code := extractFileCode(link); code != "" {
			return "https://morencius.com/embed/" + code
		}
		return link
	case strings.Contains(normalizedHost, "streamwish") || strings.Contains(normalizedLink, "hanerix.com/"):
		if code := extractFileCode(link); code != "" {
			return "https://hanerix.com/e/" + code
		}
		return link
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

func ListOrphans(c *gin.Context) {
	dirs := []string{"videos"}
	if server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	// Load all recordings once to avoid N+1 queries
	uploaded := map[string]bool{}
	allRecs, _ := server.GetDBClient().GetAllRecordings()
	for i := range allRecs {
		uploaded[allRecs[i].Filename] = true
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

			if uploaded[name] {
				continue // not orphaned — already uploaded
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

	c.JSON(http.StatusOK, gin.H{"orphans": orphans})
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
			thumbURL, spriteURL, previewURL := channel.GenerateThumbnailForFile(p)
			if !channel.UploadOrphanedFile(p, thumbURL, spriteURL, previewURL) {
				results[idx] = result{Path: p, Status: "failed", Error: "upload did not complete successfully"}
				return
			}
			results[idx] = result{Path: p, Status: "success"}
		}(i, path)
	}
	wg.Wait()

	c.JSON(http.StatusOK, gin.H{"results": results})
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
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				config.AcquireFFmpeg()
				defer config.ReleaseFFmpeg()
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
		}()
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

// NodesData represents the data structure for the nodes page.
type NodesData struct {
	Nodes        []database.Node
	OnlineCount  int
	DrainingCount int
	TotalLoad    int
	Mode         string
	MyNodeID     string
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
	totalLoad := 0
	for _, n := range nodes {
		if n.Status == "online" {
			onlineCount++
		} else if n.Status == "draining" {
			drainingCount++
		}
		totalLoad += n.CurrentLoad
	}

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

// PoolData represents the data structure for the pool editor page.
type PoolData struct {
	Assignments []database.ChannelAssignment
}

// PoolPage renders the pool editor page.
func PoolPage(c *gin.Context) {
	c.Header("Cache-Control", "public, max-age=15")

	client := server.GetDBClient()
	var assignments []database.ChannelAssignment
	if client != nil {
		var err error
		assignments, err = client.GetAllAssignments()
		if err != nil {
			fmt.Printf("[WARN] failed to load assignments: %v\n", err)
		}
	}

	c.HTML(200, "pool.html", &PoolData{
		Assignments: assignments,
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

// GetPoolJSON returns all assignments as JSON.
func GetPoolJSON(c *gin.Context) {
	client := server.GetDBClient()
	if client == nil {
		c.JSON(http.StatusOK, gin.H{"assignments": []database.ChannelAssignment{}})
		return
	}

	assignments, err := client.GetAllAssignments()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"assignments": assignments})
}

// PoolAddRequest is the request body for adding a channel to the pool.
type PoolAddRequest struct {
	Site       string `json:"site" form:"site"`
	Username   string `json:"username" form:"username" binding:"required"`
	Resolution int    `json:"resolution" form:"resolution"`
	Framerate  int    `json:"framerate" form:"framerate"`
}

// AddToPool adds a channel to the shared pool.
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

	client := server.GetDBClient()
	if client == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Supabase not configured"})
		return
	}

	// Check if already exists in the channels table (isolated mode)
	if existingCh, chErr := client.GetChannel(req.Username); chErr == nil && existingCh != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("channel '%s' already exists in database", req.Username)})
		return
	}

	// Check if already in pool assignments
	existing, err := client.GetAssignment(req.Username, req.Site)
	if err == nil && existing != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("channel '%s' already exists in pool", req.Username)})
		return
	}

	assignment := database.ChannelAssignment{
		Username:   req.Username,
		Site:       req.Site,
		Status:     "unassigned",
		Resolution: req.Resolution,
		Framerate:  req.Framerate,
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

// RemoveFromPool removes a channel from the shared pool. It deletes the
// pool assignment, cleans up the isolated-mode channels table, and stops
// the local DVR if the channel is running on this node. Existing recordings
// and previews are kept.
func RemoveFromPool(c *gin.Context) {
	var req PoolRemoveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	if req.Site == "" {
		req.Site = "chaturbate"
	}

	// If the channel is running locally, stop it (releases coordinator,
	// stops ffmpeg, removes from memory, saves config). No-op otherwise.
	if err := server.Manager.StopChannel(req.Username); err != nil {
		fmt.Printf("[ERROR] remove from pool: stop channel %s: %v\n", req.Username, err)
	}

	client := server.GetDBClient()
	if client == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Supabase not configured"})
		return
	}

	// Delete the assignment row (pool mode)
	if err := client.DeleteAssignment(req.Username, req.Site); err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Also delete from channels table (isolated mode leftovers) so the
	// channel is fully removed from the system.
	_ = client.DeleteChannel(req.Username)

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// PoolCheckRequest is the request body for checking if a channel exists.
type PoolCheckRequest struct {
	Site     string `json:"site" form:"site"`
	Username string `json:"username" form:"username" binding:"required"`
}

// CheckPoolChannel checks if a channel already exists in the Supabase database
// (either in the pool assignments table or the channels table). Returns
// {exists: true/false} — used for real-time duplicate detection in the pool
// add form. No external HTTP calls are made.
func CheckPoolChannel(c *gin.Context) {
	var req PoolCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("bind: %v", err)})
		return
	}

	if req.Site == "" {
		req.Site = "chaturbate"
	}
	if req.Username == "" {
		c.JSON(http.StatusOK, gin.H{"exists": false})
		return
	}

	dbClient := server.GetDBClient()
	if dbClient == nil {
		// No DB configured — cannot check, treat as not existing
		c.JSON(http.StatusOK, gin.H{"exists": false})
		return
	}

	// Check pool assignments table (pooled mode)
	if assignment, err := dbClient.GetAssignment(req.Username, req.Site); err == nil && assignment != nil {
		c.JSON(http.StatusOK, gin.H{"exists": true})
		return
	}

	// Check isolated channels table (isolated mode)
	if ch, err := dbClient.GetChannel(req.Username); err == nil && ch != nil {
		c.JSON(http.StatusOK, gin.H{"exists": true})
		return
	}

	c.JSON(http.StatusOK, gin.H{"exists": false})
}
