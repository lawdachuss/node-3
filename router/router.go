package router

import (
	"embed"
	"fmt"
	"html/template"
	"log"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/server"
)

// SetupRouter initializes and returns the Gin router.
func SetupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()
	if err := LoadHTMLFromEmbedFS(r, view.FS, "templates/index.html", "templates/channel_info.html", "templates/videos.html", "templates/video.html", "templates/channel.html", "templates/admin.html", "templates/nodes.html", "templates/pool.html", "templates/logs.html"); err != nil {
		log.Fatalf("failed to load HTML templates: %v", err)
	}

	// Apply authentication if configured
	SetupAuth(r)
	// Serve static frontend files
	SetupStatic(r)
	// Register views
	SetupViews(r)

	return r
}

// SetupAuth applies basic authentication if credentials are provided.
func SetupAuth(r *gin.Engine) {
	if server.Config.AdminUsername != "" && server.Config.AdminPassword != "" {
		auth := gin.BasicAuth(gin.Accounts{
			server.Config.AdminUsername: server.Config.AdminPassword,
		})
		r.Use(auth)
	}
}

func init() {
	server.InvalidateVideosCacheFn = InvalidateVideosCache
}

// SetupStatic serves static frontend files with aggressive browser caching.
func SetupStatic(r *gin.Engine) {
	fs, err := view.StaticFS()
	if err != nil {
		log.Fatalf("failed to initialize static files: %v", err)
	}
	// Cache static assets for 24 h — avoids repeat downloads on every page load.
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/static/") {
			c.Header("Cache-Control", "public, max-age=86400")
		}
		c.Next()
	})
	r.StaticFS("/static", fs)
}

// setupViews registers HTML templates and view handlers.
func SetupViews(r *gin.Engine) {
	r.GET("/", Index)
	r.GET("/admin", AdminPage)
	r.GET("/updates", Updates)
	r.GET("/videos", Videos)
	r.GET("/video", VideoDetail)
	r.GET("/channel", ChannelVideos)
	r.GET("/play", Play)
	r.GET("/download", Download)
	r.POST("/delete_video", DeleteVideo)
	r.POST("/delete_video_db", DeleteVideoRecord)
	r.POST("/update_config", UpdateConfig)
	r.POST("/create_channel", CreateChannel)
	r.GET("/stop_channel/:username", StopChannel)
	r.POST("/stop_channel/:username", StopChannel)
	r.GET("/pause_channel/:username", PauseChannel)
	r.POST("/pause_channel/:username", PauseChannel)
	r.GET("/resume_channel/:username", ResumeChannel)
	r.POST("/resume_channel/:username", ResumeChannel)

	// Tunnel API
	r.GET("/api/tunnel", GetTunnel)
	r.POST("/api/tunnel", UpdateTunnel)

	// Orphan management API
	r.GET("/api/orphans", ListOrphans)
	r.POST("/api/orphans/retry", RetryOrphan)
	r.POST("/api/orphans/rescan", RescanOutputDir)
	r.DELETE("/api/orphans", DeleteOrphans)

	// Node-local disk inventory
	r.GET("/api/files", ListNodeFiles)

	// Thumbnail proxy API
	r.GET("/api/thumb/:username", ServeLiveThumb)

	// Same-origin image proxy for external thumbnail/sprite/preview hosts.
	r.GET("/api/imgproxy", ServeImageProxy)

	// Same-origin Streamtape play relay (resolves the tokenized CDN URL on the
	// node and streams it back; the browser's <video> plays it natively).
	r.GET("/api/play/streamtape/:code", StreamtapePlay)

	// Upload queue API
	r.GET("/api/uploads", UploadQueue)

	// Session control API
	r.POST("/api/session/stop", TriggerSessionStop)
	r.POST("/api/session/duration", SetSessionDuration)

	// ── Upload keys API (central credential store + live health) ─────────
	r.GET("/api/upload_keys", GetUploadKeys)
	r.POST("/api/upload_keys/add", AddUploadKey)
	r.POST("/api/upload_keys/remove", RemoveUploadKey)
	r.POST("/api/upload_keys/probe", ProbeUploadKeys)

	// ── Distributed shards / nodes UI ─────────────────────────────────────
	r.GET("/nodes", NodesPage)
	r.GET("/pool", PoolPage)

	// ── Nodes & Pool API ──────────────────────────────────────────────────
	r.GET("/api/nodes", GetNodesJSON)
	r.GET("/api/pool", GetPoolJSON)
	r.GET("/api/pool/check", CheckPoolChannel)
	r.POST("/api/pool/add", AddToPool)
	r.POST("/api/pool/remove", RemoveFromPool)

	// ── Logs API ──────────────────────────────────────────────────────────
	r.GET("/logs", LogsPage)
	r.GET("/api/logs", LogsAPI)

	// ── Health API ────────────────────────────────────────────────────────
	// JSON readiness for monitors: database reachability plus the upload_links
	// write-path canary, so a closed write path is machine-detectable instead of
	// hiding in a log line.  503 when either is bad.
	r.GET("/api/health", HealthAPI)

}

// LoadHTMLFromEmbedFS loads specific HTML templates from an embedded filesystem and registers them with Gin.
// templateFuncs returns the shared FuncMap registered on all HTML templates.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"printf": fmt.Sprintf,
		"subOnline": func(chs []*entity.ChannelInfo) int {
			n := 0
			for _, ch := range chs {
				if ch.IsOnline && !ch.IsPaused {
					n++
				}
			}
			return n
		},
		"subFailed": func(entries []entity.PendingEntry) int {
			n := 0
			for _, e := range entries {
				if e.Failed {
					n++
				}
			}
			return n
		},
		"orphSize": func(orphans []orphanEntry) string {
			var s int64
			for _, o := range orphans {
				s += o.Size
			}
			return fmt.Sprintf("%.1f MB", float64(s)/1024/1024)
		},
		"divBytes": func(b int64) float64 { return float64(b) / 1024 / 1024 },
		"add": func(a, b int) int { return a + b },
		// timefmt renders a timestamp for the admin tables; a zero time (key
		// never checked) reads as an em dash instead of year 1.
		"timefmt": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.Local().Format("2006-01-02 15:04")
		},
		"isWebp": func(url string) bool { return strings.HasSuffix(url, ".webp") },
		// proxy routes an external image URL (thumbnail/sprite/preview on the
		// image hosts) through the DVR server so the browser loads it same-origin.
		// Empty/non-http values pass through unchanged.
		"proxy": func(raw string) string {
			if raw == "" {
				return ""
			}
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return raw
			}
			return "/api/imgproxy?url=" + url.QueryEscape(raw)
		},
	}
}

// LoadHTMLFromEmbedFS loads specific HTML templates from an embedded filesystem and registers them with Gin.
func LoadHTMLFromEmbedFS(r *gin.Engine, embeddedFS embed.FS, files ...string) error {
	templ := template.New("").Funcs(templateFuncs())
	for _, file := range files {
		content, err := embeddedFS.ReadFile(file)
		if err != nil {
			return err
		}
		_, err = templ.New(filepath.Base(file)).Parse(string(content))
		if err != nil {
			return err
		}
	}

	// Set the parsed templates as the HTML renderer for Gin
	r.SetHTMLTemplate(templ)
	return nil
}
