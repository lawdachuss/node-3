package channel

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

type recEntry struct {
	Filename     string            `json:"filename"`
	Timestamp    string            `json:"timestamp"`
	RoomTitle    string            `json:"room_title"`
	Tags         []string          `json:"tags"`
	Viewers      int               `json:"viewers"`
	Resolution   string            `json:"resolution"`
	Framerate    int               `json:"framerate"`
	Links        map[string]string `json:"links"`
	ThumbnailURL string            `json:"thumbnail_url"`
	SpriteURL    string            `json:"sprite_url"`
	EmbedURL     string            `json:"embed_url"`
	Filesize     int64             `json:"filesize"`
}

type recChannelData struct {
	Gender     string     `json:"gender"`
	Recordings []recEntry `json:"recordings"`
}

type recDB struct {
	Version  int                        `json:"version"`
	Channels map[string]*recChannelData `json:"channels"`
}

func loadRecDB() *recDB {
	empty := &recDB{Version: 2, Channels: map[string]*recChannelData{}}

	dbData := server.LoadRecordingsFromDB()
	if dbData == nil {
		return empty
	}
	var db recDB
	if err := json.Unmarshal(dbData, &db); err != nil {
		return empty
	}
	return &db
}

func saveRecDB(db *recDB) {
	data, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return
	}

	if err := server.SaveRecordingsToDB(data); err != nil {
		fmt.Printf("[WARN] [db] could not save recordings to Supabase: %v\n", err)
	}
}

func embedURLFromLink(host, link string) string {
	if link == "" {
		return ""
	}

	switch host {
	case "Streamtape":
		if strings.Contains(link, "/v/") {
			parts := strings.SplitN(link, "/v/", 2)
			if len(parts) > 1 {
				code := strings.SplitN(parts[1], "/", 2)[0]
				if code != "" {
					return "https://streamtape.com/e/" + code
				}
			}
		}
	case "VOE.sx", "VoeSX":
		code := link[strings.LastIndex(link, "/")+1:]
		if code != "" {
			return "https://voe.sx/e/" + code
		}
	case "Byse":
		code := link[strings.LastIndex(link, "/")+1:]
		if code != "" {
			return "https://filemoon.sx/e/" + code
		}
	case "SendCM":
		return link
	}
	return ""
}

// uploadFile uploads the given file to all configured hosts.
// It uses the channel's logging so upload events appear in the UI logs.
// GoFile always uploads (no API key needed).
// Other services upload only if their API key is configured.
func (ch *Channel) uploadFile(filePath string) bool {
	cfg := server.Config
	if cfg == nil {
		return false
	}

	filename := filepath.Base(filePath)
	ch.Info("upload: starting upload of %s", filename)

	// Create the uploader with the channel as its logger
	upl := uploader.NewMultiHostUploader(
		cfg.TurboViPlayAPIKey,
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeAPIKey,
		cfg.SendCMAPIKey,
		cfg.ByseAPIKey,
		ch, // Channel implements uploader.Logger
	)

	results := upl.UploadToAll(filePath)
	success := uploader.GetSuccessfulUploads(results)
	if len(results) > 0 {
		ch.Info("upload: finished — %d/%d successful", len(success), len(results))
		if len(success) == 0 {
			ch.Error("upload: all hosts failed for %s", filename)
		}
	}

	// Persist successful upload results to recordings database
	successful := uploader.GetSuccessfulUploads(results)
	if len(successful) > 0 {
		links := map[string]string{}
		var embedURL string
		for _, r := range successful {
			links[r.Host] = r.DownloadLink
			if embedURL == "" {
				embedURL = embedURLFromLink(r.Host, r.DownloadLink)
			}
		}

		stat, _ := os.Stat(filePath)
		var filesize int64
		if stat != nil {
			filesize = stat.Size()
		}

		thumbURL := ""
		if d, e := os.ReadFile(filePath + ".thumb"); e == nil {
			thumbURL = strings.TrimSpace(string(d))
		}
		spriteURL := ""
		if d, e := os.ReadFile(filePath + ".sprite"); e == nil {
			spriteURL = strings.TrimSpace(string(d))
		}

		server.RecMu.Lock()
		db := loadRecDB()
		username := ch.Config.Username
		chanData, ok := db.Channels[username]
		if !ok {
			chanData = &recChannelData{Recordings: []recEntry{}}
			db.Channels[username] = chanData
		}

		found := false
		for i, r := range chanData.Recordings {
			if r.Filename == filename {
				chanData.Recordings[i].Links = links
				if embedURL != "" {
					chanData.Recordings[i].EmbedURL = embedURL
				}
				if thumbURL != "" {
					chanData.Recordings[i].ThumbnailURL = thumbURL
				}
				if spriteURL != "" {
					chanData.Recordings[i].SpriteURL = spriteURL
				}
				if filesize > 0 {
					chanData.Recordings[i].Filesize = filesize
				}
				found = true
				break
			}
		}
		if !found {
			entry := recEntry{
				Filename:     filename,
				Timestamp:    time.Now().UTC().Format("2006-01-02T15:04:05Z"),
				RoomTitle:    ch.RoomTitle,
				Tags:         ch.Tags,
				Viewers:      ch.Viewers,
				Resolution:   ch.Resolution,
				Framerate:    ch.Framerate,
				Links:        links,
				ThumbnailURL: thumbURL,
				SpriteURL:    spriteURL,
				EmbedURL:     embedURL,
				Filesize:     filesize,
			}
			chanData.Recordings = append(chanData.Recordings, entry)
		}
		saveRecDB(db)
		server.RecMu.Unlock()
		ch.Info("upload: saved upload links to database for %s", filename)

		// Save preview links to dedicated table for fast lookup
		if thumbURL != "" || spriteURL != "" {
			if err := server.SavePreviewLinks(filename, thumbURL, spriteURL); err != nil {
				ch.Error("upload: could not save preview links for %s: %v", filename, err)
			} else {
				ch.Info("upload: saved preview links for %s", filename)
			}
		}

		// If configured to delete local files after upload, remove
		// the video and any generated sidecars (thumbnails/sprites).
		// Only delete if we successfully saved the URLs to the database.
		if server.Config != nil && server.Config.DeleteLocalAfterUpload {
			if thumbURL == "" && spriteURL == "" {
				ch.Error("upload: skipping local file deletion for %s — no preview URLs were saved to database", filename)
			} else {
				_ = os.Remove(filePath)
				_ = os.Remove(filePath + ".thumb.jpg")
				_ = os.Remove(filePath + ".sprite.jpg")
				_ = os.Remove(filePath + ".thumb")
				_ = os.Remove(filePath + ".sprite")
				ch.Info("upload: removed local files for %s", filename)
			}
		}
	}

	return len(successful) > 0
}

// Ensure Channel implements uploader.Logger.
var _ uploader.Logger = (*Channel)(nil)
