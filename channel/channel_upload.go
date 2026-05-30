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
        case "Streamtape":
                return link
        case "Mixdrop":
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
                cfg.SendCMAPIKey,
                cfg.ByseAPIKey,
                cfg.StreamtapeLogin,
                cfg.StreamtapeKey,
                cfg.MixdropEmail,
                cfg.MixdropToken,
                cfg.PixelDrainToken,
                ch, // Channel implements uploader.Logger
        )

        results := upl.UploadToAll(filePath)
        success := uploader.GetSuccessfulUploads(results)
        if len(results) > 0 {
                ch.Info("upload: finished — %d/%d hosts succeeded", len(success), len(results))
                for _, r := range results {
                        if r.Error != nil {
                                ch.Error("upload: [%s] failed: %s", r.Host, r.Error.Error())
                        } else {
                                ch.Info("upload: [%s] done — %s", r.Host, r.DownloadLink)
                        }
                }
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
                        ch.Gender,
                        embedURL,
                        links,
                ); err != nil {
                        ch.Error("upload: failed to save to Supabase: %v", err)
                } else {
                        dbSaved = true
                        ch.Info("upload: saved recording metadata to Supabase for %s", filename)
                }

                // Delete local file once at least one host has the file safely.
                // We do NOT gate deletion on dbSaved — if Supabase is down the
                // file is still on the remote host, and keeping it locally would
                // cause CleanupOrphanedFiles to re-upload it on every restart.
                if server.Config != nil && server.Config.DeleteLocalAfterUpload {
                        if !dbSaved {
                                ch.Warn("upload: Supabase save failed — deleting local copy anyway (file is on remote host)")
                        }
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
