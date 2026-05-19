package channel

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

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
func (ch *Channel) uploadFile(filePath string, thumbURL, spriteURL string) bool {
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

	// Always save preview links to Supabase first — even if video upload fails,
	// the preview images were already uploaded to image hosts.
	if thumbURL != "" || spriteURL != "" {
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL); err != nil {
			ch.Error("upload: could not save preview links for %s: %v", filename, err)
		} else {
			ch.Info("upload: saved preview links for %s", filename)
		}
	}

	// Persist successful upload results to recordings database
	successful := uploader.GetSuccessfulUploads(results)
	if len(successful) > 0 {
		dbSaved := false
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

		// Save directly to Supabase
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")
		if err := server.SaveRecordingWithLinks(
			ch.Config.Username,
			filename,
			timestamp,
			ch.RoomTitle,
			ch.Tags,
			ch.Viewers,
			ch.Resolution,
			ch.Framerate,
			filesize,
			"", // gender - will be set later if needed
			embedURL,
			links,
		); err != nil {
			ch.Error("upload: failed to save to Supabase: %v", err)
		} else {
			dbSaved = true
			ch.Info("upload: saved recording metadata to Supabase for %s", filename)
		}

		// Only delete local file if at least one DB write succeeded — prevents
		// losing the file when Supabase is down or returns an error.
		if server.Config != nil && server.Config.DeleteLocalAfterUpload && dbSaved {
			_ = os.Remove(filePath)
			// Also clean up any associated preview sidecar files
			for _, suffix := range []string{".thumb.jpg", ".sprite.jpg", ".thumb", ".sprite"} {
				_ = os.Remove(filePath + suffix)
			}
			ch.Info("upload: removed local file for %s", filename)
		}
	}

	return len(successful) > 0
}

// Ensure Channel implements uploader.Logger.
var _ uploader.Logger = (*Channel)(nil)
