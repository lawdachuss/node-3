package router

import (
        "embed"
        "html/template"
        "log"
        "path/filepath"
        "strings"

        "github.com/gin-gonic/gin"
        "github.com/teacat/chaturbate-dvr/router/view"
        "github.com/teacat/chaturbate-dvr/server"
)

// SetupRouter initializes and returns the Gin router.
func SetupRouter() *gin.Engine {
        gin.SetMode(gin.ReleaseMode)

        r := gin.Default()
        if err := LoadHTMLFromEmbedFS(r, view.FS, "templates/index.html", "templates/channel_info.html", "templates/videos.html", "templates/video.html", "templates/channel.html"); err != nil {
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
	r.DELETE("/api/orphans", DeleteOrphans)

	// Session control API
	r.POST("/api/session/stop", TriggerSessionStop)

}

// LoadHTMLFromEmbedFS loads specific HTML templates from an embedded filesystem and registers them with Gin.
func LoadHTMLFromEmbedFS(r *gin.Engine, embeddedFS embed.FS, files ...string) error {
        templ := template.New("")
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
