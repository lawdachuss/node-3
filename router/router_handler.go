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
        "path/filepath"
        "sort"
        "strconv"
        "strings"
        "sync"
        "time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
        "github.com/teacat/chaturbate-dvr/internal"
        "github.com/teacat/chaturbate-dvr/server"
)

// IndexData represents the data structure for the index page.
type IndexData struct {
	Config       *entity.Config
	Channels     []*entity.ChannelInfo
	Disk         *entity.DiskInfo
	SessionDeadlineUnix int64 // Unix timestamp when current session ends; 0 = inactive
	SessionDurationSec   int  // Total session duration in seconds
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
                c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
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
	PixeldrainToken string `json:"pixeldrain_token" form:"pixeldrain_token"`
	StripchatPDKey  string `json:"stripchat_pdkey" form:"stripchat_pdkey"`
}

// UpdateConfig updates the server configuration from the Web UI form or API POST.
func UpdateConfig(c *gin.Context) {
        var req UpdateConfigRequest
        if err := c.ShouldBind(&req); err != nil {
                c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
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

        // Update uploader credentials (VOE.sx / Streamtape / Mixdrop / PixelDrain)
        if req.VoeSXAPIKey != "" || req.StreamtapeLogin != "" || req.StreamtapeKey != "" || req.MixdropEmail != "" || req.MixdropToken != "" || req.PixeldrainToken != "" {
                server.UpdateUploaderCredentials(req.VoeSXAPIKey, req.StreamtapeLogin, req.StreamtapeKey, req.MixdropEmail, req.MixdropToken, req.PixeldrainToken)
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
        if strings.Contains(normalizedHost, "pixeldrain") || strings.Contains(normalizedLink, "pixeldrain.com/") {
                return ""
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
                        return "https://streamtape.com/e/" + code + "/"
                }
                return link
        case strings.Contains(normalizedHost, "mixdrop") || strings.Contains(normalizedLink, "mixdrop."):
                if code := extractFileCode(link); code != "" {
                        return "https://mixdrop.ag/e/" + code
                }
                return link
	case strings.Contains(normalizedHost, "pixeldrain") || strings.Contains(normalizedLink, "pixeldrain.com/"):
		return ""
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
                c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
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
	data      []byte
	contentType string
	expiresAt time.Time
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
