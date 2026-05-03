package router

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/HeapOfChaos/goondvr/entity"
	"github.com/HeapOfChaos/goondvr/internal"
	"github.com/HeapOfChaos/goondvr/manager"
	"github.com/HeapOfChaos/goondvr/server"
	"github.com/HeapOfChaos/goondvr/supabase"
	"github.com/HeapOfChaos/goondvr/uploader"
	"github.com/gin-gonic/gin"
)

// IndexData represents the data structure for the index page.
type IndexData struct {
	Config   *entity.Config
	Channels []*entity.ChannelInfo
}

// Index renders the index page with channel information.
func Index(c *gin.Context) {
	c.HTML(200, "index.html", &IndexData{
		Config:   server.Config,
		Channels: server.Manager.ChannelInfo(),
	})
}

// CreateChannelRequest represents the request body for creating a channel.
type CreateChannelRequest struct {
	Username    string `form:"username" binding:"required"`
	Site        string `form:"site"`
	Framerate   int    `form:"framerate" binding:"required"`
	Resolution  int    `form:"resolution" binding:"required"`
	Pattern     string `form:"pattern" binding:"required"`
	MaxDuration int    `form:"max_duration"`
	MaxFilesize int    `form:"max_filesize"`
}

// CreateChannel creates a new channel.
func CreateChannel(c *gin.Context) {
	var req *CreateChannelRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	siteName := entity.NormalizeSite(req.Site)

	var errs []string
	for _, username := range strings.Split(req.Username, ",") {
		username = strings.TrimSpace(username)
		if username == "" {
			continue
		}
		if err := server.Manager.CreateChannel(&entity.ChannelConfig{
			IsPaused:    false,
			Username:    username,
			Site:        siteName,
			Framerate:   req.Framerate,
			Resolution:  req.Resolution,
			Pattern:     req.Pattern,
			MaxDuration: req.MaxDuration,
			MaxFilesize: req.MaxFilesize,
			CreatedAt:   time.Now().Unix(),
		}, true); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("%s", strings.Join(errs, "; ")))
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// StopChannel stops a channel.
func StopChannel(c *gin.Context) {
	server.Manager.StopChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// PauseChannel pauses a channel.
func PauseChannel(c *gin.Context) {
	server.Manager.PauseChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ResumeChannel resumes a paused channel.
func ResumeChannel(c *gin.Context) {
	server.Manager.ResumeChannel(c.Param("channelID"))

	c.Redirect(http.StatusFound, "/")
}

// ThumbProxy proxies the channel's summary card image from the CDN through the server.
// This avoids hotlink-protection issues when the browser requests the image directly.
func ThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	req := internal.NewMediaReq()
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// LiveThumbProxy proxies the channel's live-updating thumbnail from the CDN.
// For Stripchat this uses img.doppiocdn.net; for Chaturbate it falls back to
// the summary card image (the JS handles Chaturbate live thumbs directly).
func LiveThumbProxy(c *gin.Context) {
	imgURL := server.Manager.GetChannelLiveThumb(c.Param("channelID"))
	if imgURL == "" {
		c.Status(http.StatusNotFound)
		return
	}

	req := internal.NewMediaReqWithReferer("https://stripchat.com/")
	imgBytes, err := req.GetBytes(c.Request.Context(), imgURL)
	if err != nil {
		c.Status(http.StatusBadGateway)
		return
	}

	contentType := http.DetectContentType(imgBytes)
	c.Data(http.StatusOK, contentType, imgBytes)
}

// Updates handles the SSE connection for updates.
func Updates(c *gin.Context) {
	server.Manager.Subscriber(c.Writer, c.Request)
}

// Stats returns system stats as JSON for the header stats bar.
func Stats(c *gin.Context) {
	c.JSON(http.StatusOK, server.Manager.GetStats())
}

// UpdateConfigRequest represents the request body for updating configuration.
type UpdateConfigRequest struct {
	Cookies             string `form:"cookies"`
	UserAgent           string `form:"user_agent"`
	CompletedDir        string `form:"completed_dir"`
	FinalizeMode        string `form:"finalize_mode"`
	FFmpegEncoder       string `form:"ffmpeg_encoder"`
	FFmpegContainer     string `form:"ffmpeg_container"`
	FFmpegQuality       int    `form:"ffmpeg_quality"`
	FFmpegPreset        string `form:"ffmpeg_preset"`
	NtfyURL             string `form:"ntfy_url"`
	NtfyTopic           string `form:"ntfy_topic"`
	NtfyToken           string `form:"ntfy_token"`
	DiscordWebhookURL   string `form:"discord_webhook_url"`
	DiskWarningPercent  int    `form:"disk_warning_percent"`
	DiskCriticalPercent int    `form:"disk_critical_percent"`
	CFChannelThreshold  int    `form:"cf_channel_threshold"`
	CFGlobalThreshold   int    `form:"cf_global_threshold"`
	NotifyCooldownHours int    `form:"notify_cooldown_hours"`
	NotifyStreamOnline  bool   `form:"notify_stream_online"`
	EnableGoFileUpload  bool   `form:"enable_gofile_upload"`
	EnableSupabase      bool   `form:"enable_supabase"`
	TurboViPlayAPIKey   string `form:"turboviplay_api_key"`
	VoeSXAPIKey         string `form:"voesx_api_key"`
	StreamtapeLogin     string `form:"streamtape_login"`
	StreamtapeAPIKey    string `form:"streamtape_api_key"`
}

// UpdateConfig updates the server configuration.
func UpdateConfig(c *gin.Context) {
	var req *UpdateConfigRequest
	if err := c.Bind(&req); err != nil {
		c.AbortWithError(http.StatusBadRequest, fmt.Errorf("bind: %w", err))
		return
	}

	server.Config.Cookies = req.Cookies
	server.Config.UserAgent = req.UserAgent
	server.Config.CompletedDir = req.CompletedDir
	server.Config.FinalizeMode = entity.NormalizeFinalizeMode(req.FinalizeMode)
	server.Config.FFmpegEncoder = req.FFmpegEncoder
	if req.FFmpegContainer == "mkv" {
		server.Config.FFmpegContainer = "mkv"
	} else {
		server.Config.FFmpegContainer = "mp4"
	}
	if req.FFmpegQuality > 0 {
		server.Config.FFmpegQuality = req.FFmpegQuality
	} else if server.Config.FFmpegQuality <= 0 {
		server.Config.FFmpegQuality = 23
	}
	server.Config.FFmpegPreset = req.FFmpegPreset
	if server.Config.FFmpegEncoder == "" {
		server.Config.FFmpegEncoder = "libx264"
	}
	if server.Config.FFmpegPreset == "" {
		server.Config.FFmpegPreset = "medium"
	}
	server.Config.NtfyURL = req.NtfyURL
	server.Config.NtfyTopic = req.NtfyTopic
	server.Config.NtfyToken = req.NtfyToken
	server.Config.DiscordWebhookURL = req.DiscordWebhookURL
	server.Config.DiskWarningPercent = req.DiskWarningPercent
	server.Config.DiskCriticalPercent = req.DiskCriticalPercent
	server.Config.CFChannelThreshold = req.CFChannelThreshold
	server.Config.CFGlobalThreshold = req.CFGlobalThreshold
	server.Config.NotifyCooldownHours = req.NotifyCooldownHours
	server.Config.NotifyStreamOnline = req.NotifyStreamOnline
	server.Config.EnableGoFileUpload = req.EnableGoFileUpload
	server.Config.EnableSupabase = req.EnableSupabase
	server.Config.TurboViPlayAPIKey = req.TurboViPlayAPIKey
	server.Config.VoeSXAPIKey = req.VoeSXAPIKey
	server.Config.StreamtapeLogin = req.StreamtapeLogin
	server.Config.StreamtapeAPIKey = req.StreamtapeAPIKey

	if err := manager.SaveSettings(); err != nil {
		c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("save settings: %w", err))
		return
	}
	c.Redirect(http.StatusFound, "/")
}

// GetVideos returns all uploaded video records as JSON
func GetVideos(c *gin.Context) {
	var allRecords []map[string]interface{}
	var supabaseCount, localCount int
	var supabaseError string
	
	// Try Supabase first if credentials are configured
	if server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "" {
		log.Printf("[API] Fetching videos from Supabase...")
		if server.SupabaseClient == nil {
			supabaseError = "Supabase client not initialized"
			log.Printf("[API] ERROR: %s", supabaseError)
		} else {
			records, err := server.SupabaseClient.GetAllUploads()
			if err != nil {
				supabaseError = err.Error()
				log.Printf("[API] Supabase fetch failed: %v, falling back to local database", err)
			} else {
				supabaseCount = len(records)
				log.Printf("[API] Fetched %d videos from Supabase", supabaseCount)
				// Convert to generic map format
				for _, record := range records {
					allRecords = append(allRecords, map[string]interface{}{
						"id":                record.ID,
						"streamer_name":     record.StreamerName,
						"filename":          record.Filename,
						"gofile_link":       record.GofileLink,
						"turboviplay_link":  record.TurboViPlayLink,
						"voesx_link":        record.VoeSXLink,
						"streamtape_link":   record.StreamtapeLink,
						"thumbnail_link":    record.ThumbnailLink,
						"upload_date":       record.UploadDate,
						"source":            "supabase",
					})
				}
			}
		}
	} else {
		supabaseError = "Supabase not configured (missing URL or API key)"
		log.Printf("[API] %s", supabaseError)
	}
	
	// Also read from local JSON database (as backup/supplement)
	log.Printf("[API] Fetching videos from local database...")
	localRecords := readLocalDatabase()
	localCount = len(localRecords)
	log.Printf("[API] Fetched %d videos from local database", localCount)
	allRecords = append(allRecords, localRecords...)
	
	totalCount := len(allRecords)
	log.Printf("[API] Total videos: %d (Supabase: %d, Local: %d)", totalCount, supabaseCount, localCount)
	
	// Enhanced response with debugging info
	response := gin.H{
		"videos": allRecords, 
		"count": totalCount,
		"debug": gin.H{
			"supabase_configured": server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "",
			"supabase_client_initialized": server.SupabaseClient != nil,
			"supabase_count": supabaseCount,
			"supabase_error": supabaseError,
			"local_count": localCount,
			"local_database_path": "database/",
			"total_count": totalCount,
		},
	}
	
	// If no videos found, add helpful message
	if totalCount == 0 {
		response["message"] = "No videos found. Videos will appear here after they are recorded and uploaded."
		if supabaseError != "" {
			response["supabase_status"] = supabaseError
		}
		if localCount == 0 {
			response["local_status"] = "No local database files found"
		}
	}
	
	c.JSON(http.StatusOK, response)
}

// readLocalDatabase reads video records from the local JSON database
func readLocalDatabase() []map[string]interface{} {
	var allRecords []map[string]interface{}
	
	// Walk through database directory
	databaseDir := "database"
	if _, err := os.Stat(databaseDir); os.IsNotExist(err) {
		return allRecords
	}
	
	err := filepath.Walk(databaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		
		// Only process recordings.json files
		if info.IsDir() || filepath.Base(path) != "recordings.json" {
			return nil
		}
		
		// Read the JSON file
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		
		var dbData map[string]interface{}
		if err := json.Unmarshal(data, &dbData); err != nil {
			return nil
		}
		
		// Extract recordings array
		recordings, ok := dbData["recordings"].([]interface{})
		if !ok {
			return nil
		}
		
		// Add each recording to the result with normalized field names
		for _, rec := range recordings {
			if recMap, ok := rec.(map[string]interface{}); ok {
				// Normalize field names to match Supabase format
				normalized := map[string]interface{}{
					"source": "local",
				}
				
				// Map local fields to Supabase field names
				if username, ok := recMap["username"].(string); ok {
					normalized["streamer_name"] = username
				}
				if filename, ok := recMap["filename"].(string); ok {
					normalized["filename"] = filename
				}
				if gofileLink, ok := recMap["gofile_link"].(string); ok {
					normalized["gofile_link"] = gofileLink
				}
				if turboviplayLink, ok := recMap["turboviplay_link"].(string); ok {
					normalized["turboviplay_link"] = turboviplayLink
				}
				if voesxLink, ok := recMap["voesx_link"].(string); ok {
					normalized["voesx_link"] = voesxLink
				}
				if streamtapeLink, ok := recMap["streamtape_link"].(string); ok {
					normalized["streamtape_link"] = streamtapeLink
				}
				if thumbnailLink, ok := recMap["thumbnail_link"].(string); ok {
					normalized["thumbnail_link"] = thumbnailLink
				}
				if uploadedAt, ok := recMap["uploaded_at"].(string); ok {
					normalized["upload_date"] = uploadedAt
				}
				if filesize, ok := recMap["filesize_bytes"]; ok {
					normalized["filesize_bytes"] = filesize
				}
				if duration, ok := recMap["duration_seconds"]; ok {
					normalized["duration_seconds"] = duration
				}
				if site, ok := recMap["site"].(string); ok {
					normalized["site"] = site
				}
				if id, ok := recMap["id"]; ok {
					normalized["id"] = id
				}
				
				allRecords = append(allRecords, normalized)
			}
		}
		
		return nil
	})
	
	if err != nil {
		log.Printf("Error reading local database: %v", err)
	}
	
	return allRecords
}

// GetVideosByUsername returns all uploaded video records for a specific username
func GetVideosByUsername(c *gin.Context) {
	username := c.Param("username")
	var allRecords []map[string]interface{}
	var supabaseCount, localCount int
	var supabaseError string
	
	log.Printf("[API] Fetching videos for username: %s", username)
	
	// Try Supabase first if credentials are configured
	if server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "" {
		log.Printf("[API] Fetching from Supabase for %s...", username)
		if server.SupabaseClient == nil {
			supabaseError = "Supabase client not initialized"
			log.Printf("[API] ERROR: %s", supabaseError)
		} else {
			records, err := server.SupabaseClient.GetUploadsByStreamer(username)
			if err != nil {
				supabaseError = err.Error()
				log.Printf("[API] Supabase fetch failed for %s: %v", username, err)
			} else {
				supabaseCount = len(records)
				log.Printf("[API] Fetched %d videos from Supabase for %s", supabaseCount, username)
				// Convert to generic map format
				for _, record := range records {
					allRecords = append(allRecords, map[string]interface{}{
						"id":                record.ID,
						"streamer_name":     record.StreamerName,
						"filename":          record.Filename,
						"gofile_link":       record.GofileLink,
						"turboviplay_link":  record.TurboViPlayLink,
						"voesx_link":        record.VoeSXLink,
						"streamtape_link":   record.StreamtapeLink,
						"thumbnail_link":    record.ThumbnailLink,
						"upload_date":       record.UploadDate,
						"source":            "supabase",
					})
				}
			}
		}
	} else {
		supabaseError = "Supabase not configured"
	}
	
	// Also read from local JSON database
	log.Printf("[API] Fetching from local database for %s...", username)
	localRecords := readLocalDatabaseByUsername(username)
	localCount = len(localRecords)
	log.Printf("[API] Fetched %d videos from local database for %s", localCount, username)
	allRecords = append(allRecords, localRecords...)
	
	totalCount := len(allRecords)
	log.Printf("[API] Total videos for %s: %d (Supabase: %d, Local: %d)", username, totalCount, supabaseCount, localCount)
	
	response := gin.H{
		"videos": allRecords, 
		"count": totalCount, 
		"username": username,
		"debug": gin.H{
			"supabase_configured": server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "",
			"supabase_client_initialized": server.SupabaseClient != nil,
			"supabase_count": supabaseCount,
			"supabase_error": supabaseError,
			"local_count": localCount,
			"total_count": totalCount,
		},
	}
	
	// If no videos found, add helpful message
	if totalCount == 0 {
		response["message"] = fmt.Sprintf("No videos found for %s. Videos will appear here after they are recorded and uploaded.", username)
	}
	
	c.JSON(http.StatusOK, response)
}

// readLocalDatabaseByUsername reads video records for a specific username from the local JSON database
func readLocalDatabaseByUsername(username string) []map[string]interface{} {
	var allRecords []map[string]interface{}
	
	// Check if user directory exists
	userDir := filepath.Join("database", username)
	if _, err := os.Stat(userDir); os.IsNotExist(err) {
		return allRecords
	}
	
	err := filepath.Walk(userDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		
		if info.IsDir() || filepath.Base(path) != "recordings.json" {
			return nil
		}
		
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		
		var dbData map[string]interface{}
		if err := json.Unmarshal(data, &dbData); err != nil {
			return nil
		}
		
		recordings, ok := dbData["recordings"].([]interface{})
		if !ok {
			return nil
		}
		
		// Add each recording to the result with normalized field names
		for _, rec := range recordings {
			if recMap, ok := rec.(map[string]interface{}); ok {
				// Normalize field names to match Supabase format
				normalized := map[string]interface{}{
					"source": "local",
				}
				
				// Map local fields to Supabase field names
				if user, ok := recMap["username"].(string); ok {
					normalized["streamer_name"] = user
				}
				if filename, ok := recMap["filename"].(string); ok {
					normalized["filename"] = filename
				}
				if gofileLink, ok := recMap["gofile_link"].(string); ok {
					normalized["gofile_link"] = gofileLink
				}
				if turboviplayLink, ok := recMap["turboviplay_link"].(string); ok {
					normalized["turboviplay_link"] = turboviplayLink
				}
				if voesxLink, ok := recMap["voesx_link"].(string); ok {
					normalized["voesx_link"] = voesxLink
				}
				if streamtapeLink, ok := recMap["streamtape_link"].(string); ok {
					normalized["streamtape_link"] = streamtapeLink
				}
				if thumbnailLink, ok := recMap["thumbnail_link"].(string); ok {
					normalized["thumbnail_link"] = thumbnailLink
				}
				if uploadedAt, ok := recMap["uploaded_at"].(string); ok {
					normalized["upload_date"] = uploadedAt
				}
				if filesize, ok := recMap["filesize_bytes"]; ok {
					normalized["filesize_bytes"] = filesize
				}
				if duration, ok := recMap["duration_seconds"]; ok {
					normalized["duration_seconds"] = duration
				}
				if site, ok := recMap["site"].(string); ok {
					normalized["site"] = site
				}
				if id, ok := recMap["id"]; ok {
					normalized["id"] = id
				}
				
				allRecords = append(allRecords, normalized)
			}
		}
		
		return nil
	})
	
	if err != nil {
		log.Printf("Error reading local database for %s: %v", username, err)
	}
	
	return allRecords
}

// GetVideosBySite returns all uploaded video records for a specific site
func GetVideosBySite(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"error": "not implemented", "videos": []interface{}{}})
}

// GetVideoByID returns a specific video record by ID
func GetVideoByID(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": "not implemented"})
}

// GetDatabaseStats returns database statistics
func GetDatabaseStats(c *gin.Context) {
	if !server.Config.EnableSupabase || server.Config.SupabaseURL == "" || server.Config.SupabaseAPIKey == "" {
		c.JSON(http.StatusOK, gin.H{"error": "Supabase is disabled"})
		return
	}

	records, err := server.SupabaseClient.GetAllUploads()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Count by streamer
	streamerCounts := make(map[string]int)
	for _, record := range records {
		streamerCounts[record.StreamerName]++
	}

	c.JSON(http.StatusOK, gin.H{
		"total_videos":    len(records),
		"total_streamers": len(streamerCounts),
		"by_streamer":     streamerCounts,
	})
}

// SearchVideos searches videos by query string
func SearchVideos(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"error": "not implemented", "videos": []interface{}{}})
}

// BackupDatabase creates a backup of the database
func BackupDatabase(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "not implemented"})
}

// UploadCompletedFiles uploads all completed video files to GoFile
func UploadCompletedFiles(c *gin.Context) {
	if !server.Config.EnableGoFileUpload {
		c.JSON(http.StatusBadRequest, gin.H{"error": "GoFile upload is not enabled"})
		return
	}

	// Start upload in background
	go uploadCompletedFilesAsync()

	c.JSON(http.StatusOK, gin.H{"message": "Upload started in background"})
}


// uploadCompletedFilesAsync uploads all completed video files to GoFile in the background
func uploadCompletedFilesAsync() {
	log.Println("Starting upload of completed files...")
	
	completedDir := "videos/completed"
	if server.Config.CompletedDir != "" {
		completedDir = server.Config.CompletedDir
	}

	// Check if directory exists
	if _, err := os.Stat(completedDir); os.IsNotExist(err) {
		log.Printf("Completed directory does not exist: %s", completedDir)
		return
	}

	gofileUploader := uploader.NewGoFileUploader()
	var supabaseClient *supabase.Client
	if server.Config.EnableSupabase && server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "" {
		supabaseClient = supabase.NewClient(server.Config.SupabaseURL, server.Config.SupabaseAPIKey)
	}
	
	uploadCount := 0
	errorCount := 0
	skippedCount := 0

	// Walk through completed directory
	err := filepath.Walk(completedDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files we can't access
		}

		if info.IsDir() {
			return nil // Skip directories
		}

		// Only process video files - CRITICAL: Only .mp4 and .mkv files (NOT .ts)
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".mp4" && ext != ".mkv" {
			// Skip .ts files and other formats
			if ext == ".ts" {
				log.Printf("⚠️  SKIPPED: %s (unconverted .ts file - use automatic recording for proper conversion)", filepath.Base(path))
				skippedCount++
			}
			return nil
		}

		log.Printf("Uploading: %s (%d MB)", filepath.Base(path), info.Size()/(1024*1024))

		// Upload to GoFile
		downloadLink, err := gofileUploader.Upload(path)
		if err != nil {
			log.Printf("ERROR: Upload failed for %s: %v", filepath.Base(path), err)
			errorCount++
			return nil
		}

		log.Printf("SUCCESS: Uploaded %s -> %s", filepath.Base(path), downloadLink)

		// Generate and save thumbnail locally
		var thumbnailPath string
		log.Printf("Generating thumbnail for %s...", filepath.Base(path))
		tempThumbPath, err := generateThumbnailForUpload(path)
		if err != nil {
			log.Printf("WARNING: Thumbnail generation failed for %s: %v", filepath.Base(path), err)
		} else {
			// Save to thumbnails directory
			thumbnailDir := "thumbnails"
			os.MkdirAll(thumbnailDir, 0755)
			
			// Extract username from filename
			filename := filepath.Base(path)
			username := strings.Split(filename, "_")[0]
			thumbnailFilename := fmt.Sprintf("%s_%d.jpg", username, time.Now().Unix())
			thumbnailPath = filepath.Join(thumbnailDir, thumbnailFilename)
			
			if err := os.Rename(tempThumbPath, thumbnailPath); err != nil {
				// Try copy if rename fails
				srcFile, _ := os.Open(tempThumbPath)
				if srcFile != nil {
					dstFile, _ := os.Create(thumbnailPath)
					if dstFile != nil {
						io.Copy(dstFile, srcFile)
						dstFile.Close()
					}
					srcFile.Close()
				}
				os.Remove(tempThumbPath)
			}
			log.Printf("SUCCESS: Thumbnail saved -> %s", thumbnailPath)
		}

		// Extract username from filename (assumes format: username_date_time.ext)
		filename := filepath.Base(path)
		username := strings.Split(filename, "_")[0]

		// Store in Supabase
		if supabaseClient != nil {
			thumbnailLink := thumbnailPath
			if thumbnailPath != "" {
				if link, err := supabaseClient.UploadThumbnail("thumbnails", filepath.Base(thumbnailPath), thumbnailPath, "image/jpeg"); err != nil {
					log.Printf("WARNING: Thumbnail upload failed (keeping local) for %s: %v", filename, err)
				} else {
					thumbnailLink = link
					log.Printf("SUCCESS: Thumbnail uploaded -> %s", thumbnailLink)
				}
			}
			if err := supabaseClient.InsertUploadRecord(username, downloadLink, thumbnailLink); err != nil {
				log.Printf("ERROR: Failed to store in Supabase for %s: %v", filename, err)
			} else {
				log.Printf("SUCCESS: Stored in Supabase for %s", filename)
			}
		}

		// Delete local file after successful upload
		if err := os.Remove(path); err != nil {
			log.Printf("ERROR: Failed to delete %s: %v", filepath.Base(path), err)
		} else {
			log.Printf("SUCCESS: Deleted local file %s", filepath.Base(path))
		}

		uploadCount++
		return nil
	})

	if err != nil {
		log.Printf("ERROR: Failed to walk completed directory: %v", err)
	}

	log.Printf("Upload complete: %d successful, %d failed, %d skipped (.ts files)", uploadCount, errorCount, skippedCount)
}


// generateThumbnailForUpload creates a thumbnail from a video file
func generateThumbnailForUpload(videoPath string) (string, error) {
	thumbnailPath := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + "_thumb.jpg"
	_ = os.Remove(thumbnailPath)

	args := []string{
		"-y",
		"-ss", "2",
		"-i", videoPath,
		"-vframes", "1",
		"-vf", "scale=640:-2",
		"-q:v", "2",
		thumbnailPath,
	}

	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Try without seeking for short videos
		args = []string{
			"-y",
			"-i", videoPath,
			"-vframes", "1",
			"-vf", "scale=640:-2",
			"-q:v", "2",
			thumbnailPath,
		}
		cmd = exec.Command("ffmpeg", args...)
		output, err = cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("ffmpeg failed: %s: %w", string(output), err)
		}
	}

	fileInfo, err := os.Stat(thumbnailPath)
	if os.IsNotExist(err) {
		return "", fmt.Errorf("thumbnail not created")
	}
	if fileInfo.Size() < 1000 {
		os.Remove(thumbnailPath)
		return "", fmt.Errorf("thumbnail corrupted")
	}

	return thumbnailPath, nil
}
