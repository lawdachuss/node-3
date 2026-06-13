package router

import (
        "encoding/json"
        "net/http"
        "os"
        "path/filepath"
        "sort"
        "strings"
        "sync"
        "time"

        "github.com/gin-gonic/gin"
        "github.com/teacat/chaturbate-dvr/entity"
        "github.com/teacat/chaturbate-dvr/internal"
        "github.com/teacat/chaturbate-dvr/server"
)

// videosCache caches the full scanVideos() result to avoid repeated
// filesystem walks and DB calls on every page load.
var (
        videosCacheMu   sync.RWMutex
        cachedVideos    []*VideoEntry
        cachedVideosAt  time.Time
	videosCacheTTL  = 5 * time.Minute
)

// InvalidateVideosCache forces the next /videos request to rescan.
// Call this after a recording is saved or deleted.
func InvalidateVideosCache() {
        videosCacheMu.Lock()
        cachedVideos = nil
        cachedVideosAt = time.Time{}
        videosCacheMu.Unlock()
}

func scanVideosCached() []*VideoEntry {
        videosCacheMu.RLock()
        if cachedVideos != nil && time.Since(cachedVideosAt) < videosCacheTTL {
                result := cachedVideos
                videosCacheMu.RUnlock()
                return result
        }
        videosCacheMu.RUnlock()

        result := scanVideos()

        videosCacheMu.Lock()
        cachedVideos = result
        cachedVideosAt = time.Now()
        videosCacheMu.Unlock()

        return result
}

type RecordingEntry struct {
	Filename     string            `json:"filename"`
	FullPath     string            `json:"full_path,omitempty"`
	Timestamp    string            `json:"timestamp"`
	RoomTitle    string            `json:"room_title"`
	Tags         []string          `json:"tags"`
	Viewers      int               `json:"viewers"`
	Resolution   string            `json:"resolution"`
	Framerate    int               `json:"framerate"`
	Links        map[string]string `json:"links"`
	ThumbnailURL string            `json:"thumbnail_url"`
	SpriteURL    string            `json:"sprite_url"`
	PreviewURL   string            `json:"preview_url"`
	EmbedURL     string            `json:"embed_url"`
	Filesize     int64             `json:"filesize"`
}

type ChannelRecordings struct {
        Gender     string             `json:"gender"`
        Recordings []*RecordingEntry  `json:"recordings"`
}

type RecordingsDB struct {
        Version  int                          `json:"version"`
        Channels map[string]*ChannelRecordings `json:"channels"`
}

type VideoEntry struct {
	Username     string
	Filename     string
	FullPath     string
	Size         string
	ModTime      string
	ModTimeSort  string
	ThumbnailURL string
	SpriteURL    string
	PreviewURL   string
	IsOutputDir  bool
	Links       map[string]string
	Tags        []string
	RoomTitle   string
	Viewers     int
	Gender      string
	Resolution  string
	Framerate   int
}

type VideoGroup struct {
        Username      string
        Gender        string
        Videos        []*VideoEntry
        LatestModTime string
}

type VideosData struct {
        Config      *entity.Config
        Videos      []*VideoEntry
        Groups      []VideoGroup
        Recommended []*VideoEntry
        TagFilter   string
}

func Videos(c *gin.Context) {
        videos := scanVideosCached()

        // Filter by tag if provided
        tagFilter := c.Query("tag")
        if tagFilter != "" {
                filtered := make([]*VideoEntry, 0, len(videos))
                for _, v := range videos {
                        for _, t := range v.Tags {
                                if strings.EqualFold(t, tagFilter) {
                                        filtered = append(filtered, v)
                                        break
                                }
                        }
                }
                videos = filtered
        }

        groups := groupVideos(videos)

        // Show most recently added videos in the "Recommended" slot on the library page.
        var recommended []*VideoEntry
        if len(videos) > 0 && tagFilter == "" {
                sorted := make([]*VideoEntry, len(videos))
                copy(sorted, videos)
                sort.Slice(sorted, func(i, j int) bool {
                        return sorted[i].ModTimeSort > sorted[j].ModTimeSort
                })
                limit := 8
                if len(sorted) < limit {
                        limit = len(sorted)
                }
                recommended = sorted[:limit]
        }

	c.Header("Cache-Control", "public, max-age=60")
	c.HTML(200, "videos.html", &VideosData{
                Config:      server.Config,
                Videos:      videos,
                Groups:      groups,
                Recommended: recommended,
                TagFilter:   tagFilter,
        })
}

func ChannelVideos(c *gin.Context) {
        name := c.Query("name")
        if name == "" {
                c.Redirect(http.StatusFound, "/videos")
                return
        }

        videos := scanVideosCached()
        filtered := make([]*VideoEntry, 0)
        for _, v := range videos {
                if strings.EqualFold(v.Username, name) {
                        filtered = append(filtered, v)
                }
        }

        c.HTML(200, "channel.html", &VideosData{
                Config: server.Config,
                Videos: filtered,
                Groups: nil,
        })
}

var videoExts = map[string]bool{
	".mp4": true,
	".mkv": true,
}

func scanVideos() []*VideoEntry {
        var entries []*VideoEntry
        seen := map[string]bool{}

        dirs := []string{"videos"}
        if server.Config != nil && server.Config.OutputDir != "" {
                dirs = append(dirs, server.Config.OutputDir)
        }

        // Batch-load all preview URLs to avoid N+1 queries
        previewLinks := server.LoadAllPreviewLinks()

        for _, dir := range dirs {
                absDir, err := filepath.Abs(dir)
                if err != nil || seen[absDir] {
                        continue
                }
                seen[absDir] = true
                entries = append(entries, walkDir(dir, previewLinks)...)
        }

        recordings := loadRecordings()
        linked := map[string]bool{}
        for _, e := range entries {
                linked[e.Filename] = true
        }
        for username, chanData := range recordings.Channels {
                for _, rec := range chanData.Recordings {
                        filename := rec.Filename
                        if linked[filename] {
                                for _, e := range entries {
                                        if e.Filename == filename {
                                                e.Links = rec.Links
                                                e.Tags = rec.Tags
                                                e.RoomTitle = rec.RoomTitle
                                                e.Viewers = rec.Viewers
                                                e.Gender = chanData.Gender
                                                e.Resolution = rec.Resolution
                                                e.Framerate = rec.Framerate
						if rec.ThumbnailURL != "" {
							e.ThumbnailURL = rec.ThumbnailURL
						}
						if rec.SpriteURL != "" {
							e.SpriteURL = rec.SpriteURL
						}
						if rec.PreviewURL != "" {
							e.PreviewURL = rec.PreviewURL
						}
                                        }
                                }
                                continue
                        }
                        fs := "uploaded"
                        if rec.Filesize > 0 {
                                fs = internal.FormatFilesize(int(rec.Filesize))
                        }
                        // For uploaded-only entries, supplement thumbnail/sprite from
                        // the preview_images table (previewLinks) if the recordings
                        // table has empty URLs (they are stored separately).
			thumbURL := rec.ThumbnailURL
			spriteURL := rec.SpriteURL
			previewURL := rec.PreviewURL
			if links, ok := previewLinks[filename]; ok {
				if thumbURL == "" && links[0] != "" {
					thumbURL = links[0]
				}
				if spriteURL == "" && links[1] != "" {
					spriteURL = links[1]
				}
				if previewURL == "" && len(links) > 2 && links[2] != "" {
					previewURL = links[2]
				}
			}
                        modTime := rec.Timestamp
                        if t, err := time.Parse(time.RFC3339, rec.Timestamp); err == nil {
                                modTime = t.Format("2006-01-02 15:04")
                        } else if t, err := time.Parse("2006-01-02T15:04:05Z", rec.Timestamp); err == nil {
                                modTime = t.Format("2006-01-02 15:04")
                        }
			entries = append(entries, &VideoEntry{
				Username:     username,
				Filename:     filename,
				FullPath:     "",
				Size:         fs,
				ModTime:      modTime,
				ModTimeSort:  rec.Timestamp,
				IsOutputDir:  false,
				Links:        rec.Links,
				Tags:         rec.Tags,
				RoomTitle:    rec.RoomTitle,
				Viewers:      rec.Viewers,
				Gender:       chanData.Gender,
				Resolution:   rec.Resolution,
				Framerate:    rec.Framerate,
				ThumbnailURL: thumbURL,
				SpriteURL:    spriteURL,
				PreviewURL:   previewURL,
			})
                }
        }

        sort.Slice(entries, func(i, j int) bool {
                return entries[i].ModTimeSort > entries[j].ModTimeSort
        })
        return entries
}

func loadRecordings() *RecordingsDB {
        empty := &RecordingsDB{Version: 2, Channels: map[string]*ChannelRecordings{}}

        dbData := server.LoadRecordingsFromDB()
        if dbData == nil {
                return empty
        }

        var db RecordingsDB
        if err := json.Unmarshal(dbData, &db); err != nil {
                return empty
        }
        return &db
}

func walkDir(dir string, previewLinks map[string][3]string) []*VideoEntry {
        var entries []*VideoEntry

        d, err := os.Open(dir)
        if err != nil {
                return entries
        }
        defer d.Close()

        items, err := d.Readdir(-1)
        if err != nil {
                return entries
        }

        for _, item := range items {
                full := filepath.Join(dir, item.Name())
                if item.IsDir() {
                        entries = append(entries, walkDir(full, previewLinks)...)
                        continue
                }

                ext := strings.ToLower(filepath.Ext(item.Name()))
                if !videoExts[ext] {
                        continue
                }
                if strings.HasSuffix(item.Name(), ".video.mp4") || strings.HasSuffix(item.Name(), ".audio.mp4") {
                        continue
                }

                username := extractUsername(item.Name())
                sizeStr := internal.FormatFilesize(int(item.Size()))
                if sizeStr == "" {
                        sizeStr = "0 B"
                }
                modTime := item.ModTime().Format("2006-01-02 15:04")

                isOutput := false
                if server.Config != nil && server.Config.OutputDir != "" {
                        absPath, _ := filepath.Abs(full)
                        absOut, _ := filepath.Abs(server.Config.OutputDir)
                        isOutput = strings.HasPrefix(absPath, absOut)
                }

		// Look up preview URLs from preloaded map
		var thumbURL, spriteURL, previewURL string
		if links, ok := previewLinks[item.Name()]; ok {
			thumbURL = links[0]
			spriteURL = links[1]
			if len(links) > 2 {
				previewURL = links[2]
			}
		}

		entries = append(entries, &VideoEntry{
			Username:     username,
			Filename:     item.Name(),
			FullPath:     full,
			Size:         sizeStr,
			ModTime:      modTime,
			ModTimeSort:  item.ModTime().Format(time.RFC3339),
			ThumbnailURL: thumbURL,
			SpriteURL:    spriteURL,
			PreviewURL:   previewURL,
			IsOutputDir:  isOutput,
		})
        }

        return entries
}

func extractUsername(filename string) string {
        if idx := strings.Index(filename, "_20"); idx > 0 {
                return filename[:idx]
        }
        if idx := strings.Index(filename, "_"); idx > 0 {
                return filename[:idx]
        }
        return "unknown"
}

func groupVideos(videos []*VideoEntry) []VideoGroup {
        byUser := map[string]*VideoGroup{}
        for _, v := range videos {
                g, ok := byUser[v.Username]
                if !ok {
                        g = &VideoGroup{Username: v.Username, Gender: v.Gender}
                        byUser[v.Username] = g
                }
                g.Videos = append(g.Videos, v)
                if v.ModTimeSort > g.LatestModTime {
                        g.LatestModTime = v.ModTimeSort
                }
        }

        groups := make([]VideoGroup, 0, len(byUser))
        for _, g := range byUser {
                sort.Slice(g.Videos, func(i, j int) bool {
                        return g.Videos[i].ModTimeSort > g.Videos[j].ModTimeSort
                })
                groups = append(groups, *g)
        }

        sort.Slice(groups, func(i, j int) bool {
                return groups[i].LatestModTime > groups[j].LatestModTime
        })
        return groups
}

// getRecommendations returns recommended videos based on the current video
// Recommendations are based on: same channel, similar tags, similar time, same gender
func getRecommendations(currentVideo *VideoEntry, allVideos []*VideoEntry, limit int) []*VideoEntry {
        if currentVideo == nil || len(allVideos) == 0 {
                return nil
        }

        type scoredVideo struct {
                video *VideoEntry
                score float64
        }

        scored := make([]scoredVideo, 0)

        for _, v := range allVideos {
                // Skip the current video itself
                if v.Filename == currentVideo.Filename {
                        continue
                }

                score := 0.0

                // Same channel gets highest priority (50 points)
                if strings.EqualFold(v.Username, currentVideo.Username) {
                        score += 50.0
                }

                // Same gender (10 points)
                if v.Gender != "" && v.Gender == currentVideo.Gender {
                        score += 10.0
                }

                // Similar tags (5 points per matching tag)
                if len(currentVideo.Tags) > 0 && len(v.Tags) > 0 {
                        matchingTags := 0
                        for _, tag1 := range currentVideo.Tags {
                                for _, tag2 := range v.Tags {
                                        if strings.EqualFold(tag1, tag2) {
                                                matchingTags++
                                                break
                                        }
                                }
                        }
                        score += float64(matchingTags) * 5.0
                }

                // Similar resolution (5 points)
                if v.Resolution != "" && v.Resolution == currentVideo.Resolution {
                        score += 5.0
                }

                // Recent videos get bonus (up to 10 points based on recency)
                if v.ModTimeSort != "" {
                        vTime, err1 := time.Parse(time.RFC3339, v.ModTimeSort)
                        cTime, err2 := time.Parse(time.RFC3339, currentVideo.ModTimeSort)
                        if err1 == nil && err2 == nil {
                                daysDiff := vTime.Sub(cTime).Hours() / 24
                                if daysDiff < 0 {
                                        daysDiff = -daysDiff
                                }
                                // Videos within 7 days get bonus points
                                if daysDiff <= 7 {
                                        score += (7 - daysDiff) / 7 * 10.0
                                }
                        }
                }

                // Only include videos with some relevance
                if score > 0 {
                        scored = append(scored, scoredVideo{video: v, score: score})
                }
        }

        // Sort by score descending
        sort.Slice(scored, func(i, j int) bool {
                return scored[i].score > scored[j].score
        })

        // Return top N recommendations
        recommendations := make([]*VideoEntry, 0, limit)
        for i := 0; i < len(scored) && i < limit; i++ {
                recommendations = append(recommendations, scored[i].video)
        }

        return recommendations
}
