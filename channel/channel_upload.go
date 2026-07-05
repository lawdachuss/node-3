package channel

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/internal"
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
	case "Streamtape":
		return link
	case "Mixdrop":
		return link
	case "SeekStreaming":
		return link
	case "VidHide":
		code := link[strings.LastIndex(link, "/")+1:]
		if code != "" {
			return "https://morencius.com/embed/" + code
		}
	case "StreamWish":
		code := link[strings.LastIndex(link, "/")+1:]
		if code != "" {
			return "https://hanerix.com/e/" + code
		}
	}
	return ""
}

const (
	maxChannelUploadAttempts = 8
	channelUploadRetryDelay  = 5 * time.Second
)

// uploadFile uploads the given file to all configured hosts.
// It uses the channel's logging so upload events appear in the UI logs.
// GoFile always uploads (no API key needed).
// Other services upload only if their API key is configured.
// Retries up to maxChannelUploadAttempts times if all hosts fail, matching
// the orphan-recovery retry policy so active recordings are not lost on
// transient network blips.
//
// Upload Journal: before uploading, computes a fast file hash and checks
// Supabase for any hosts that already received this file (e.g. from a
// previous interrupted run).  Those hosts are skipped.  After each attempt,
// journal entries are upserted so crash recovery is precise.
func (ch *Channel) uploadFile(filePath string, thumbURL, spriteURL, previewURL string) bool {
	cfg := server.Config
	if cfg == nil {
		return false
	}

	filename := filepath.Base(filePath)
	ch.Info("upload: starting upload of %s", filename)

	if _, err := os.Stat(filePath); err != nil {
		ch.Error("upload: file not found %s: %v — skipping upload", filename, err)
		return false
	}

	// Compute file hash for upload journal
	fileHash, hashErr := internal.FastFileHash(filePath)
	if hashErr != nil {
		ch.Warn("upload: could not hash %s (journal skipped): %v", filename, hashErr)
	}

	// Load completed hosts from journal to avoid redundant uploads
	var completedHosts []string
	if fileHash != "" {
		var loadErr error
		completedHosts, loadErr = server.LoadCompletedHosts(fileHash)
		if loadErr != nil {
			ch.Warn("upload: could not load journal for %s: %v", filename, loadErr)
		}
	}

	// Create the uploader with the channel as its logger
	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.SeekStreamingKey,
		cfg.VidHideAPIKey,
		cfg.StreamWishAPIKey,
		ch, // Channel implements uploader.Logger
	)

	allHosts := upl.AvailableHosts()

	// Determine which hosts still need the file
	hostsToTry := allHosts
	if len(completedHosts) > 0 {
		hostsToTry = difference(allHosts, completedHosts)
		if len(hostsToTry) == 0 {
			ch.Info("upload: all hosts already have %s per journal — skipping upload", filename)
			return true
		}
		ch.Info("upload: %d/%d hosts already have this file — uploading to %d remaining",
			len(completedHosts), len(allHosts), len(hostsToTry))
	}

	var results []uploader.UploadResult
	var success []uploader.UploadResult
	for attempt := 1; attempt <= maxChannelUploadAttempts; attempt++ {
		if attempt > 1 && len(hostsToTry) == 0 {
			break
		}
		var attemptResults []uploader.UploadResult
		attemptResults = upl.UploadSelected(filePath, hostsToTry)
		results = append(results, attemptResults...)

		// Save journal entries for each result
		if fileHash != "" {
			stat, _ := os.Stat(filePath)
			var filesize int64
			if stat != nil {
				filesize = stat.Size()
			}
			for _, r := range attemptResults {
				status := "success"
				errMsg := ""
				if r.Error != nil {
					status = "failed"
					errMsg = r.Error.Error()
				}
				if jErr := server.SaveJournalEntry(fileHash, filename, r.Host, status, filesize, errMsg); jErr != nil {
					ch.Warn("upload: could not save journal for %s/%s: %v", r.Host, filename, jErr)
				}
			}
		}

		success = uploader.GetSuccessfulUploads(results)
		if len(success) >= len(allHosts) {
			break
		}

		if attempt < maxChannelUploadAttempts {
			// On retry, only retry hosts that haven't succeeded yet
			failedHosts := failedHostNames(results, completedHosts)
			hostsToTry = failedHosts
			if len(hostsToTry) > 0 {
				ch.Warn("upload: %d hosts still pending — retrying in %ds (attempt %d/%d)",
					len(hostsToTry), int(channelUploadRetryDelay.Seconds()), attempt+1, maxChannelUploadAttempts)
				time.Sleep(channelUploadRetryDelay)
			}
		}
	}

	if len(results) > 0 {
		ch.Info("upload: finished — %d/%d hosts succeeded", len(success), len(allHosts))
		results = deduplicateResults(results)
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
	if thumbURL != "" || spriteURL != "" || previewURL != "" {
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL); err != nil {
			ch.Error("upload: could not save preview links for %s: %v", filename, err)
		} else {
			ch.Info("upload: saved preview links for %s", filename)
		}
	}

	// Persist successful upload results to recordings database
	if len(success) > 0 {
		dbSaved := false
		links := map[string]string{}
		var embedURL string
		var seekPosterURL, seekPreviewURL string
		for _, r := range success {
			links[r.Host] = r.DownloadLink
			if embedURL == "" {
				embedURL = embedURLFromLink(r.Host, r.DownloadLink)
			}
			if r.Host == "SeekStreaming" {
				if r.PosterURL != "" {
					seekPosterURL = r.PosterURL
				}
				if r.PreviewURL != "" {
					seekPreviewURL = r.PreviewURL
				}
			}
		}

		stat, _ := os.Stat(filePath)
		var filesize int64
		if stat != nil {
			filesize = stat.Size()
		}

		// Save directly to Supabase
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")

		dur, probeErr := VideoDurationSeconds(filePath)
		if probeErr != nil {
			ch.Warn("upload: could not probe duration for %s: %v", filename, probeErr)
		}

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
			dur,
			ch.Gender,
			embedURL,
			thumbURL,
			spriteURL,
			previewURL,
			links,
			seekPosterURL,
			seekPreviewURL,
		); err != nil {
			ch.Error("upload: failed to save to Supabase: %v", err)
			// Journal entries were already saved — if we leave them, the upload
			// will be skipped on restart even though the DB has no links.
			// Clean up journals so the upload is retried from scratch.
			if fileHash != "" {
				ch.Warn("upload: removing journal for %s so upload retries", filename)
				if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
					ch.Warn("upload: could not delete journal for %s: %v", filename, jErr)
				}
			}
		} else {
			dbSaved = true
			ch.Info("upload: saved recording metadata to Supabase for %s", filename)
		}

		// Delete local file only once ALL hosts have the file safely AND
		// the metadata is persisted.  If the DB save failed, the journal
		// was cleared above so the upload retries and generates fresh links.
		if server.Config != nil && server.Config.DeleteLocalAfterUpload && len(success) > 0 && dbSaved {
			DeleteSidecarFiles(filePath)
			if removeErr := removeFileWithRetry(filePath); removeErr != nil {
				ch.Warn("upload: could not remove %s — keeping for retry: %v", filename, removeErr)
			}
			// Clean up journal entries since local file is gone
			if fileHash != "" {
				if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
					ch.Warn("upload: could not delete journal for %s: %v", filename, jErr)
				}
			}
			ch.Info("upload: removed local file for %s", filename)
		}
	}

	return len(success) > 0
}

// deduplicateResults returns a slice where each host appears at most once.
// When a host has multiple results (e.g. failed on attempt 1, succeeded on
// attempt 2), the latest result wins.  This prevents misleading log output
// like "[GoFile] failed" followed by "[GoFile] done" for the same file.
func deduplicateResults(results []uploader.UploadResult) []uploader.UploadResult {
	latest := make(map[string]uploader.UploadResult, len(results))
	order := make([]string, 0, len(results))
	for _, r := range results {
		if _, exists := latest[r.Host]; !exists {
			order = append(order, r.Host)
		}
		latest[r.Host] = r
	}
	deduped := make([]uploader.UploadResult, len(order))
	for i, host := range order {
		deduped[i] = latest[host]
	}
	return deduped
}

// failedHostNames returns the deduplicated names of hosts that failed or were
// not attempted, excluding any hosts already completed before this upload session.
func failedHostNames(results []uploader.UploadResult, alreadyCompleted []string) []string {
	completed := make(map[string]bool)
	for _, h := range alreadyCompleted {
		completed[h] = true
	}
	for _, r := range results {
		if r.Error == nil {
			completed[r.Host] = true
		}
	}
	var failed []string
	seen := make(map[string]bool)
	for _, r := range results {
		if !completed[r.Host] && !seen[r.Host] {
			seen[r.Host] = true
			failed = append(failed, r.Host)
		}
	}
	return failed
}

// difference returns the elements in 'a' that are not in 'b'.
func difference(a, b []string) []string {
	setB := make(map[string]bool, len(b))
	for _, s := range b {
		setB[s] = true
	}
	var diff []string
	for _, s := range a {
		if !setB[s] {
			diff = append(diff, s)
		}
	}
	return diff
}

// Ensure Channel implements uploader.Logger.
var _ uploader.Logger = (*Channel)(nil)
