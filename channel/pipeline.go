package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

// Stage represents a processing stage in a file pipeline.
type Stage int

const (
	StageThumbnailUpload Stage = iota // generate thumbnails + upload video (in parallel)
	StageSaveMetadata                 // save recording + links to Supabase
	StageCleanup                      // delete all local files
	StageDone                         // terminal — pipeline finished
)

var stageNames = map[Stage]string{
	StageThumbnailUpload: "thumbnail_upload",
	StageSaveMetadata:    "save_metadata",
	StageCleanup:         "cleanup",
	StageDone:            "done",
}

func (s Stage) String() string { return stageNames[s] }

// maxPipelineRetries is the number of times a failed pipeline will be retried
// across restarts before it is abandoned and its state row is deleted.
const maxPipelineRetries = 3

// pipelineWorkerCount is how many recordings a single channel processes
// concurrently.  The post-record work (mux, thumbnail, upload) is independent
// per file, so running several at once dramatically lowers end-to-end latency
// when a channel produces many recordings.  ffmpeg is still globally bounded by
// config.ffmpegSem (NumCPU*3) and uploads by UploadSem, so raising this only
// adds useful concurrency without exhausting CPU/disk/network.
const pipelineWorkerCount = 4

const (
	// pipelineReapInterval is how often the stale-pipeline reaper runs.
	pipelineReapInterval = 10 * time.Minute
	// pipelineMaxAge is the maximum age of an in-progress pipeline state
	// before the reaper force-deletes it.  Any pipeline still recorded as
	// "in_progress" beyond this is stuck (the owning process crashed, or a
	// stalled upload the stage deadline failed to abort) and must not linger
	// forever showing a green heartbeat with a phantom upload.  Normal
	// pipelines finish well within uploadStageMaxDuration (~90m) plus overhead,
	// so 2h is a safe backstop.
	pipelineMaxAge = 2 * time.Hour
)

func stageFromString(s string) Stage {
	for k, v := range stageNames {
		if v == s {
			return k
		}
	}
	return StageThumbnailUpload
}

// Pipeline processes a single video file through all stages in order.
// Each stage is independently retryable. State is persisted in Supabase
// so interrupted pipelines resume on restart.
type Pipeline struct {
	FilePath string `json:"file_path"`
	FileHash string `json:"file_hash"`
	Filename string `json:"filename"`
	Username string `json:"username"`
	FileSize int64  `json:"file_size"`

	CurrentStage Stage  `json:"current_stage"`
	Failed       bool   `json:"failed"`
	LastError    string `json:"last_error"`
	Retries      int    `json:"retries"`

	// MetadataFailed indicates the video/hosts uploads succeeded but saving the
	// recording's metadata row to Supabase failed (e.g. during a network
	// partition). When set, the local file is preserved and the pipeline keeps
	// retrying stageSaveMetadata on the next resume instead of being abandoned.
	MetadataFailed bool `json:"metadata_failed"`

	// Channel metadata snapshot captured at enqueue time so stageSaveMetadata
	// uses the state from when the file was recorded, not whatever a newer
	// recording session may have written to the Channel struct.
	RoomTitle  string   `json:"room_title"`
	Tags       []string `json:"tags"`
	Viewers    int      `json:"viewers"`
	Gender     string   `json:"gender"`
	Resolution string   `json:"resolution"`
	Framerate  int      `json:"framerate"`

	// Results populated by stages, consumed by downstream stages
	ThumbURL   string            `json:"thumb_url"`
	SpriteURL  string            `json:"sprite_url"`
	PreviewURL string            `json:"preview_url"`
	EmbedURL   string            `json:"embed_url"`
	Links      map[string]string `json:"links"` // host -> download URL

	mu sync.Mutex
}

func newPipeline(filePath, fileHash, filename, username string, fileSize int64) *Pipeline {
	return &Pipeline{
		FilePath:     filePath,
		FileHash:     fileHash,
		Filename:     filename,
		Username:     username,
		FileSize:     fileSize,
		CurrentStage: StageThumbnailUpload,
		Links:        make(map[string]string),
	}
}

// advanceTo moves the pipeline to a new stage.
func (p *Pipeline) advanceTo(s Stage) {
	p.mu.Lock()
	p.CurrentStage = s
	p.mu.Unlock()
}

// toDBState converts the Pipeline to a database.PipelineState for persistence.
func (p *Pipeline) toDBState() *database.PipelineState {
	linksJSON, _ := json.Marshal(p.Links)
	return &database.PipelineState{
		FileHash:     p.FileHash,
		FilePath:     p.FilePath,
		Filename:     p.Filename,
		Username:     p.Username,
		FileSize:     p.FileSize,
		NodeID:       server.NodeID(),
		CurrentStage: p.CurrentStage.String(),
		Failed:       p.Failed,
		LastError:    p.LastError,
		Retries:      p.Retries,
		ThumbURL:     p.ThumbURL,
		SpriteURL:    p.SpriteURL,
		PreviewURL:   p.PreviewURL,
		EmbedURL:     p.EmbedURL,
		LinksJSON:    string(linksJSON),
	}
}

// pipelineFromDBState converts a database.PipelineState back to a Pipeline.
func pipelineFromDBState(s *database.PipelineState) *Pipeline {
	p := &Pipeline{
		FileHash:     s.FileHash,
		FilePath:     s.FilePath,
		Filename:     s.Filename,
		Username:     s.Username,
		FileSize:     s.FileSize,
		CurrentStage: stageFromString(s.CurrentStage),
		Failed:       s.Failed,
		LastError:    s.LastError,
		Retries:      s.Retries,
		ThumbURL:     s.ThumbURL,
		SpriteURL:    s.SpriteURL,
		PreviewURL:   s.PreviewURL,
		EmbedURL:     s.EmbedURL,
		Links:        make(map[string]string),
	}
	if s.LinksJSON != "" {
		json.Unmarshal([]byte(s.LinksJSON), &p.Links)
	}
	return p
}

// stageThumbnail generates thumbnails, sprite, preview and uploads to Pixhost.
// Does NOT advance the pipeline stage — the caller (processPipeline) manages
// stage transitions after both thumbnail and upload finish in parallel.
func (p *Pipeline) stageThumbnail(ch *Channel) error {
	if p.ThumbURL != "" && p.SpriteURL != "" && p.PreviewURL != "" {
		return nil
	}
	thumbURL, spriteURL, previewURL := ch.generateThumbnail(p.FilePath)
	if thumbURL == "" && spriteURL == "" && previewURL == "" {
		return nil
	}
	p.ThumbURL = thumbURL
	p.SpriteURL = spriteURL
	p.PreviewURL = previewURL
	return nil
}

// stageUploadVideos uploads the video file to all configured hosts.
// Uses the upload journal to skip hosts that already have the file.
// Does NOT advance the pipeline stage — the caller manages stage transitions.
func (p *Pipeline) stageUploadVideos(ch *Channel) error {
	cfg := server.Config
	if cfg == nil {
		return fmt.Errorf("server config not loaded")
	}

	filename := p.Filename
	filePath := p.FilePath

	if _, err := os.Stat(filePath); err != nil {
		ch.Error("upload: file not found %s: %v", filename, err)
		return err
	}

	// Reject corrupt/invalid videos before pushing them to any host.
	if ok, reason := validateVideoFile(filePath, false); !ok {
		ch.Error("upload: corrupt/invalid file %s — not uploading: %s", filename, reason)
		return fmt.Errorf("file failed integrity check: %s", reason)
	}

	// Load completed hosts from journal
	var completedHosts []string
	if p.FileHash != "" {
		var loadErr error
		completedHosts, loadErr = server.LoadCompletedHosts(p.FileHash)
		if loadErr != nil {
			ch.Warn("upload: could not load journal for %s: %v", filename, loadErr)
		}
	}

	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.SeekStreamingKey,
		cfg.VidHideAPIKeys,
		cfg.StreamWishAPIKeys,
		ch,
		cfg.UpnshareKeys,
	)

	allHosts := upl.AvailableHosts()
	if len(allHosts) == 0 {
		return fmt.Errorf("no upload hosts configured for %s", filename)
	}

	hostsToTry := allHosts
	if len(completedHosts) > 0 {
		hostsToTry = difference(allHosts, completedHosts)
		if len(hostsToTry) == 0 {
			if len(p.Links) > 0 {
				ch.Info("upload: all hosts already have %s per journal", filename)
				return nil
			}
			ch.Warn("upload: stale journal for %s has no saved links; clearing journal and re-uploading", filename)
			if p.FileHash != "" {
				if jErr := server.DeleteJournalByHash(p.FileHash); jErr != nil {
					ch.Warn("upload: could not clear stale journal for %s: %v", filename, jErr)
				}
			}
			completedHosts = nil
			hostsToTry = allHosts
		}
		ch.Info("upload: %d/%d hosts already have this file — uploading to %d remaining",
			len(completedHosts), len(allHosts), len(hostsToTry))
	}

	var results []uploader.UploadResult
	var success []uploader.UploadResult

	// Bound total time spent retrying this file's uploads so a stalled host
	// (long per-host HTTP timeout) cannot hang the pipeline for hours.
	stageStart := time.Now()
	stageDeadline := stageStart.Add(uploadStageMaxDuration)
	// Hard cap: abort the entire upload stage if it runs past the deadline,
	// even when an individual host is stalled mid-request.  The per-host
	// client timeout bounds a single request, but a stalled host multiplied
	// across retries could otherwise exceed the deadline.  Cancelling the
	// context makes in-flight requests return promptly so we never hang for
	// hours waiting on a dead host.
	stageCtx, stageCancel := context.WithTimeout(context.Background(), uploadStageMaxDuration)
	defer stageCancel()

	// Set up per-upload progress callback for live UI tracking.
	// The callback is called from each uploader's goroutine as bytes are sent.
	hostProgress := make(map[string]struct {
		bytes    int64
		total    int64
		lastTime time.Time
	})
	var hostMu sync.Mutex
	upl.SetProgressCallback(func(host string, current, total int64) {
		hostMu.Lock()
		hp, ok := hostProgress[host]
		if !ok {
			hp = struct {
				bytes    int64
				total    int64
				lastTime time.Time
			}{total: total}
		}
		now := time.Now()
		var speed float64
		if !hp.lastTime.IsZero() && current > hp.bytes {
			dt := now.Sub(hp.lastTime).Seconds()
			if dt > 0 {
				speed = float64(current-hp.bytes) / dt
			}
		}
		hp.bytes = current
		hp.lastTime = now
		hostProgress[host] = hp

		hostCount := len(success)
		uploadedHosts := make(map[string]bool)
		for _, r := range success {
			uploadedHosts[r.Host] = true
		}
		hostMu.Unlock()

		// Build per-host entries
		hostMu.Lock()
		hosts := make([]entity.HostEntry, 0, len(allHosts))
		var totalCur, totalBytes int64
		for _, h := range allHosts {
			state, exists := hostProgress[h]
			entry := entity.HostEntry{Host: h}
			if uploadedHosts[h] {
				entry.Status = "done"
				entry.Progress = 100
				entry.BytesCurrent = state.total
				entry.BytesTotal = state.total
			} else if h == host {
				var pct float64
				if total > 0 {
					pct = float64(current) / float64(total) * 100
				}
				entry.Status = "uploading"
				entry.Progress = pct
				entry.BytesCurrent = current
				entry.BytesTotal = total
				if speed > 0 {
					entry.Speed = formatSpeed(speed)
				}
			} else if !exists || state.bytes == 0 {
				entry.Status = "pending"
				if exists {
					entry.BytesTotal = state.total
				}
			} else {
				entry.Status = "uploading"
				entry.Progress = 100
				if state.total > 0 {
					entry.Progress = float64(state.bytes) / float64(state.total) * 100
				}
				entry.BytesCurrent = state.bytes
				entry.BytesTotal = state.total
			}
			totalCur += entry.BytesCurrent
			totalBytes += entry.BytesTotal
			hosts = append(hosts, entry)
		}
		aggSpeed := formatSpeed(speed)
		hostMu.Unlock()

		var pct float64
		if total > 0 {
			pct = float64(current) / float64(total) * 100
		}
		status := fmt.Sprintf("uploading to %s (%.0f%%) — %d/%d hosts done", host, pct, hostCount, len(allHosts))
		ch.SetUploadProgress(filename, status, pct/float64(len(allHosts)), hostCount, len(allHosts), totalCur, totalBytes, aggSpeed, hosts)
	})

	for attempt := 1; attempt <= maxChannelUploadAttempts; attempt++ {
		if time.Now().After(stageDeadline) {
			ch.Warn("upload: stage deadline (%s) exceeded for %s — accepting partial success (%d/%d hosts)",
				uploadStageMaxDuration, filename, len(success), len(allHosts))
			break
		}
		if attempt > 1 && len(hostsToTry) == 0 {
			break
		}
		attemptCh := make(chan []uploader.UploadResult, 1)
		go func() {
			attemptCh <- upl.UploadSelected(filePath, hostsToTry)
		}()
		var attemptResults []uploader.UploadResult
		select {
		case attemptResults = <-attemptCh:
		case <-stageCtx.Done():
			ch.Warn("upload: stage deadline (%s) reached mid-attempt for %s — aborting upload stage",
				uploadStageMaxDuration, filename)
			goto uploadStageDone
		}
		results = append(results, attemptResults...)

		hostMu.Lock()
		success = uploader.GetSuccessfulUploads(results)
		hostMu.Unlock()
		ch.SetUploadProgress(filename, fmt.Sprintf("uploaded to %d/%d hosts", len(success), len(allHosts)),
			float64(len(success))/float64(len(allHosts))*100, len(success), len(allHosts),
			0, 0, "", nil)

		if p.FileHash != "" {
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
				if jErr := server.SaveJournalEntry(p.FileHash, filename, r.Host, status, filesize, errMsg); jErr != nil {
					ch.Warn("upload: could not save journal for %s/%s: %v", r.Host, filename, jErr)
				}
			}
		}

		if len(success) >= len(allHosts) {
			break
		}

		// Exclude hosts with permanent or proxy errors from retries
		skipHosts := completedHosts
		for _, r := range results {
			if uploader.IsPermanentError(r.Error) || uploader.IsProxyError(r.Error) {
				skipHosts = append(skipHosts, r.Host)
			}
		}

		if attempt < maxChannelUploadAttempts {
			failedHosts := failedHostNames(results, skipHosts)
			hostsToTry = failedHosts
			if len(hostsToTry) > 0 {
				ch.Warn("upload: %d hosts still pending — retrying in %ds (attempt %d/%d)",
					len(hostsToTry), int(channelUploadRetryDelay.Seconds()), attempt+1, maxChannelUploadAttempts)
				select {
				case <-time.After(channelUploadRetryDelay):
				case <-stageCtx.Done():
					goto uploadStageDone
				}
			}
		}
	}

uploadStageDone:

	if len(success) == 0 {
		ch.Error("upload: all hosts failed for %s", filename)
		return fmt.Errorf("all upload hosts failed for %s", filename)
	}

	// Store results
	for _, r := range success {
		p.Links[r.Host] = r.DownloadLink
		if p.EmbedURL == "" {
			p.EmbedURL = embedURLFromLink(r.Host, r.DownloadLink)
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
	}

	p.FileSize, _ = func() (int64, error) {
		stat, err := os.Stat(filePath)
		if err != nil {
			return 0, err
		}
		return stat.Size(), nil
	}()

	return nil
}

// posterFromHosts checks if any host provided an auto-generated poster URL.
func (p *Pipeline) posterFromHosts() string {
	if embedURL, ok := p.Links["SeekStreaming"]; ok {
		videoID := uploader.ExtractSeekStreamingVideoID(embedURL)
		if videoID != "" {
			if cfg := server.Config; cfg != nil && cfg.SeekStreamingKey != "" {
				posterURL, err := uploader.GetSeekStreamingPosterURL(cfg.SeekStreamingKey, videoID)
				if err == nil && posterURL != "" {
					return posterURL
				}
			}
		}
	}
	return ""
}

// seekStreamingMediaFromHosts fetches both poster and preview URLs from SeekStreaming
// using the video ID extracted from the embed URL stored in Links.
func (p *Pipeline) seekStreamingMediaFromHosts() (posterURL, previewURL string) {
	if embedURL, ok := p.Links["SeekStreaming"]; ok {
		videoID := uploader.ExtractSeekStreamingVideoID(embedURL)
		if videoID != "" {
			if cfg := server.Config; cfg != nil && cfg.SeekStreamingKey != "" {
				posterURL, previewURL, _ = uploader.GetSeekStreamingMediaURLs(cfg.SeekStreamingKey, videoID)
			}
		}
	}
	return
}

// stageSaveMetadata persists recording metadata and all links to Supabase.
func (p *Pipeline) stageSaveMetadata(ch *Channel) error {
	// Retry thumbnail generation if it failed during StageThumbnailUpload.
	if p.ThumbURL == "" && p.SpriteURL == "" && p.PreviewURL == "" {
		thumbURL, spriteURL, previewURL := ch.generateThumbnail(p.FilePath)
		if thumbURL != "" || spriteURL != "" || previewURL != "" {
			p.ThumbURL = thumbURL
			p.SpriteURL = spriteURL
			p.PreviewURL = previewURL
			ch.Info("upload: generated thumbnails for %s (retry)", p.Filename)
		} else if pu := p.posterFromHosts(); pu != "" {
			p.ThumbURL = pu
			ch.Info("upload: using auto-generated poster from host as thumbnail for %s", p.Filename)
		} else {
			ch.Warn("upload: thumbnail generation failed for %s (skipped)", p.Filename)
		}
	}

	if p.ThumbURL != "" || p.SpriteURL != "" || p.PreviewURL != "" {
		if err := server.SavePreviewLinks(p.Filename, p.ThumbURL, p.SpriteURL, p.PreviewURL); err != nil {
			ch.Error("upload: could not save preview links for %s: %v", p.Filename, err)
			p.LastError = err.Error()
			return err
		}
		ch.Info("upload: saved preview links for %s", p.Filename)
	}

	if len(p.Links) == 0 {
		return fmt.Errorf("no upload links to save for %s", p.Filename)
	}

	seekPosterURL, seekPreviewURL := p.seekStreamingMediaFromHosts()
	if seekPosterURL != "" || seekPreviewURL != "" {
		ch.Info("upload: found SeekStreaming media — poster=%s preview=%s", seekPosterURL, seekPreviewURL)
	}

	timestamp := extractTimestampFromFilename(p.Filename)
	if timestamp == "" {
		// Fall back to file modification time.
		if st, err := os.Stat(p.FilePath); err == nil {
			timestamp = st.ModTime().UTC().Format("2006-01-02T15:04:05Z")
		} else {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
	}

	// Extract real duration from the video file.
	dur, probeErr := VideoDurationSeconds(p.FilePath)
	if probeErr != nil {
		ch.Warn("upload: could not probe duration for %s: %v", p.Filename, probeErr)
	}

	if err := server.SaveRecordingWithLinks(
		ch.Config.Username,
		p.Filename,
		timestamp,
		p.RoomTitle,
		p.Tags,
		p.Viewers,
		p.Resolution,
		p.Framerate,
		p.FileSize,
		dur,
		p.Gender,
		p.EmbedURL,
		p.ThumbURL,
		p.SpriteURL,
		p.PreviewURL,
		p.Links,
		seekPosterURL,
		seekPreviewURL,
	); err != nil {
		ch.Error("upload: failed to save to Supabase: %v", err)
		// Journal entries prevent retry — clean them so upload generates fresh links.
		if p.FileHash != "" {
			ch.Warn("upload: removing journal for %s so upload retries", p.Filename)
			if jErr := server.DeleteJournalByHash(p.FileHash); jErr != nil {
				ch.Warn("upload: could not delete journal for %s: %v", p.Filename, jErr)
			}
		}
		p.LastError = err.Error()
		return err
	}

	ch.Info("upload: saved recording metadata to Supabase for %s", p.Filename)
	return nil
}

// stageCleanup removes all local files once everything is persisted upstream.
func (p *Pipeline) stageCleanup(ch *Channel) error {
	cfg := server.Config
	if cfg == nil || !cfg.DeleteLocalAfterUpload {
		ch.Info("cleanup: delete after upload disabled — keeping %s", p.Filename)
		return nil
	}

	if len(p.Links) == 0 {
		ch.Info("cleanup: keeping %s because no upload links exist", p.Filename)
		return nil
	}

	ch.Info("cleanup: removing local files for %s", p.Filename)
	DeleteSidecarFiles(p.FilePath)
	if err := removeFileWithRetry(p.FilePath); err != nil {
		ch.Warn("cleanup: could not remove %s (will retry on next run): %v", p.Filename, err)
	} else {
		ch.Info("cleanup: removed %s", p.Filename)
	}
	if p.FileHash != "" {
		if jErr := server.DeleteJournalByHash(p.FileHash); jErr != nil {
			ch.Warn("cleanup: could not delete journal for %s: %v", p.Filename, jErr)
		}
	}
	return nil
}

// PipelineQueue manages a per-channel ordered queue of pipelines.
// Pipelines are processed sequentially (one at a time per channel).
type PipelineQueue struct {
	pipelines []*Pipeline
	mu        sync.Mutex
	cond      *sync.Cond
	wg        sync.WaitGroup
	stopped   bool
	started   bool // tracks whether the worker goroutine has been launched

	ch      *Channel
	history []entity.PendingEntry // last 50 completed/failed pipelines

	reaperStop chan struct{} // closed by Stop() to terminate the reaper goroutine
}

// NewPipelineQueue creates a new pipeline queue for a channel.
func NewPipelineQueue(ch *Channel) *PipelineQueue {
	pq := &PipelineQueue{ch: ch}
	pq.cond = sync.NewCond(&pq.mu)
	pq.reaperStop = make(chan struct{})
	return pq
}

// startOnce launches the worker goroutine on first use, and relaunches it if
// the queue was previously Stop()ed.  This keeps the queue reusable across
// stop/start cycles instead of leaving it permanently dead after the first
// Stop() — a latent footgun where later EnqueueFile calls would silently
// append pipelines that nothing ever processed.
func (pq *PipelineQueue) startOnce() {
	pq.mu.Lock()
	// If the worker was previously stopped, reset so we can launch a fresh one.
	// wg.Wait() in Stop() guarantees the old goroutine has exited by now, so
	// there is no double-launch risk.
	if pq.started && pq.stopped {
		pq.started = false
		pq.stopped = false
	}
	if !pq.started {
		pq.started = true
		pq.reaperStop = make(chan struct{})
		pq.mu.Unlock()
		for i := 0; i < pipelineWorkerCount; i++ {
			pq.wg.Add(1)
			go pq.processLoop()
		}
		go pq.reapLoop()
		return
	}
	pq.mu.Unlock()
}

// Stop signals the worker to finish after draining the queue.
func (pq *PipelineQueue) Stop() {
	pq.mu.Lock()
	pq.stopped = true
	if pq.reaperStop != nil {
		close(pq.reaperStop)
		pq.reaperStop = nil
	}
	pq.mu.Unlock()
	pq.cond.Broadcast()
	pq.wg.Wait()
}

// processLoop is one of pipelineWorkerCount worker goroutines that drain the
// pipeline queue concurrently for a single channel.
func (pq *PipelineQueue) processLoop() {
	defer pq.wg.Done()
	for {
		pq.mu.Lock()
		for len(pq.pipelines) == 0 && !pq.stopped {
			pq.cond.Wait()
		}
		if pq.stopped && len(pq.pipelines) == 0 {
			pq.mu.Unlock()
			return
		}
		p := pq.pipelines[0]
		pq.pipelines = pq.pipelines[1:]
		pq.mu.Unlock()

		pq.processPipeline(p)
	}
}

// reapLoop periodically reclaims pipeline states that are stuck "in_progress"
// forever (e.g. the owning process crashed, or a stalled upload outlived the
// stage deadline).  Without this, a single wedged file shows a green heartbeat
// indefinitely and is never retried or cleaned up.
func (pq *PipelineQueue) reapLoop() {
	ticker := time.NewTicker(pipelineReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pq.reaperStop:
			return
		case <-ticker.C:
			pq.reapStalePipelines()
		}
	}
}

// reapStalePipelines deletes in-progress pipeline states older than
// pipelineMaxAge.  Deletion is idempotent, so every node may run the reaper
// safely (two nodes deleting the same row is a no-op).  The local muxed file is
// left on disk; the orphan scanner re-ingests it if it is still recoverable.
func (pq *PipelineQueue) reapStalePipelines() {
	states, err := server.LoadAllPipelineStatesCached()
	if err != nil {
		pq.ch.Warn("pipeline: reaper could not load states: %v", err)
		return
	}
	if len(states) == 0 {
		return
	}
	cutoff := time.Now().Add(-pipelineMaxAge)
	for _, s := range states {
		if s.Failed {
			continue
		}
		if s.CurrentStage == StageDone.String() {
			continue
		}
		// Only reap this node's own stale states. Reaping a healthy node's
		// in-progress state (e.g. a >2h upload) would discard real progress.
		if s.NodeID != "" && s.NodeID != server.NodeID() {
			continue
		}
		// Never reap a state that is merely waiting on the DB (uploads done,
		// stageSaveMetadata failing — e.g. during a partition). The pipeline
		// retries the metadata save on the next resume; deleting it would lose
		// an already-uploaded recording.
		if s.CurrentStage == StageSaveMetadata.String() {
			continue
		}
		if s.CreatedAt == "" {
			continue
		}
		created, perr := time.Parse(time.RFC3339Nano, s.CreatedAt)
		if perr != nil {
			created, perr = time.Parse(time.RFC3339, s.CreatedAt)
		}
		if perr != nil {
			// Unknown timestamp format — leave it alone rather than risk
			// deleting a row we can't age.
			continue
		}
		if created.After(cutoff) {
			continue
		}
		pq.ch.Warn("pipeline: reaping stalled %s (stage=%s, created=%s) — deleting phantom in-progress state",
			s.Filename, s.CurrentStage, s.CreatedAt)
		if delErr := server.DeletePipelineState(s.FileHash); delErr != nil {
			pq.ch.Warn("pipeline: reaper could not delete %s: %v", s.Filename, delErr)
		}
	}
}

// QueuedEntries returns info about all pending pipelines in the queue.
func (pq *PipelineQueue) QueuedEntries() []entity.PendingEntry {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	entries := make([]entity.PendingEntry, 0, len(pq.pipelines))
	for _, p := range pq.pipelines {
		entries = append(entries, entity.PendingEntry{
			Channel:  p.Username,
			Filename: p.Filename,
			Stage:    p.CurrentStage.String(),
			Failed:   p.Failed,
			Error:    p.LastError,
		})
	}
	return entries
}

// HistoryEntries returns the recent pipeline history.
func (pq *PipelineQueue) HistoryEntries() []entity.PendingEntry {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	out := make([]entity.PendingEntry, len(pq.history))
	copy(out, pq.history)
	return out
}

// pushHistory appends a completed/failed pipeline to the ring buffer.
func (pq *PipelineQueue) pushHistory(e entity.PendingEntry) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	const maxHistory = 50
	pq.history = append(pq.history, e)
	if len(pq.history) > maxHistory {
		pq.history = pq.history[len(pq.history)-maxHistory:]
	}
}

// processPipeline runs a single pipeline through all stages.
// Thumbnail generation and video upload run in parallel goroutines to
// minimize wall-clock time per file.  Both must finish before metadata
// can be saved.
func (pq *PipelineQueue) processPipeline(p *Pipeline) {
	ch := pq.ch
	filename := p.Filename
	p.Failed = false
	p.LastError = ""
	ch.SetUploadProgress(filename, "queued for processing", 0, 0, 0, 0, 0, "", nil)
	ch.Info("pipeline: processing %s (starting at stage %s)", filename, p.CurrentStage)

	defer func() {
		if r := recover(); r != nil {
			ch.Error("pipeline: panic processing %s: %v", filename, r)
			p.Failed = true
			p.LastError = fmt.Sprintf("panic: %v", r)
		}
		ch.UploadWg.Done()
		MarkUploadDone(p.FilePath)
		// Record history
		stageStr := p.CurrentStage.String()
		if p.Failed || p.CurrentStage == StageDone {
			pq.pushHistory(entity.PendingEntry{
				Channel:  ch.Config.Username,
				Filename: filename,
				Stage:    stageStr,
				Failed:   p.Failed,
				Error:    p.LastError,
			})
		}
		if p.CurrentStage == StageDone || p.Failed {
			if p.CurrentStage == StageDone {
				if delErr := server.DeletePipelineState(p.FileHash); delErr != nil {
					ch.Warn("pipeline: could not delete state for %s: %v", filename, delErr)
				}
			} else if p.Retries < maxPipelineRetries {
				p.Retries++
				if saveErr := server.SavePipelineState(p.toDBState()); saveErr != nil {
					ch.Warn("pipeline: could not persist state for %s: %v", filename, saveErr)
				}
			} else {
				// Retries exhausted.
				if p.MetadataFailed && len(p.Links) > 0 {
					// Uploads succeeded but the Supabase metadata row could not
					// be persisted (e.g. network partition / fenced node). Keep
					// the local file and the pipeline state so stageSaveMetadata
					// is retried on the next resume once the DB is reachable
					// again, rather than discarding a successfully-uploaded
					// recording.
					ch.Error("pipeline: %s metadata save failed %d times; keeping file for later retry", filename, p.Retries+1)
					if saveErr := server.SavePipelineState(p.toDBState()); saveErr != nil {
						ch.Warn("pipeline: could not persist metadata-pending state for %s: %v", filename, saveErr)
					}
				} else {
					// Retries exhausted — abandon the pipeline and clean up.
					ch.Error("pipeline: %s failed %d times, abandoning", filename, p.Retries+1)
					if delErr := server.DeletePipelineState(p.FileHash); delErr != nil {
						ch.Warn("pipeline: could not delete abandoned state for %s: %v", filename, delErr)
					}
					deleteLocalFile(ch, filename, p.FilePath)
				}
			}
			if m := server.Manager; m != nil {
				m.PublishLog(ch.Config.Username, fmt.Sprintf("[pipeline] %s finished (stage=%s, failed=%v, retries=%d)", filename, p.CurrentStage, p.Failed, p.Retries))
			}
		}
	}()

	defer func() {
		if p.CurrentStage != StageDone {
			if err := server.SavePipelineState(p.toDBState()); err != nil {
				ch.Warn("pipeline: could not persist state for %s: %v", filename, err)
			}
		}
	}()

	// ── Stage: Thumbnail + Video Upload (parallel) ───────────────────────
	if p.CurrentStage == StageThumbnailUpload {
		// On graceful shutdown (workflow cancel/timeout) skip all uploads and
		// leave the muxed file on disk — do not push to any host.
		if server.IsShuttingDown() {
			ch.Info("pipeline: skipping upload for %s (shutting down)", filename)
			p.advanceTo(StageDone)
		} else {
		ch.Info("pipeline: stage thumbnail_upload for %s", filename)
		ch.SetUploadProgress(filename, "generating thumbnails and uploading to hosts", 5, 0, 0, 0, 0, "", nil)

		var wg sync.WaitGroup
		var thumbErr error
		var uploadErr error

		// Start thumbnail generation + Pixhost upload in background
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					ch.Error("pipeline: thumbnail panicked for %s: %v", filename, r)
					thumbErr = fmt.Errorf("thumbnail panic: %v", r)
				}
			}()
			thumbErr = p.stageThumbnail(ch)
		}()

		// Start video upload in background (acquires UploadSem)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case UploadSem <- struct{}{}:
			case <-time.After(30 * time.Second):
				ch.Warn("pipeline: upload slot unavailable for %s after 30s — skipping upload", filename)
				uploadErr = fmt.Errorf("upload semaphore timeout")
				return
			}
			defer func() { <-UploadSem }()
			defer func() {
				if r := recover(); r != nil {
					ch.Error("pipeline: upload goroutine panicked for %s: %v", filename, r)
					uploadErr = fmt.Errorf("upload panic: %v", r)
				}
			}()
			uploadErr = p.stageUploadVideos(ch)
		}()

		// Wait for both to finish
		wg.Wait()

		if thumbErr != nil {
			ch.Error("pipeline: thumbnail stage failed for %s: %v", filename, thumbErr)
		}
		if uploadErr != nil {
			ch.Error("pipeline: upload stage failed for %s: %v", filename, uploadErr)
			p.Failed = true
			p.LastError = uploadErr.Error()
			return
		}
		if len(p.Links) == 0 {
			ch.Error("pipeline: upload stage produced no links for %s", filename)
			p.Failed = true
			p.LastError = "upload produced no links"
			return
		}

		if _, statErr := os.Stat(p.FilePath); statErr == nil {
			p.advanceTo(StageSaveMetadata)
		} else {
			ch.Error("pipeline: file %s disappeared during processing: %v", filename, statErr)
			p.Failed = true
			p.LastError = statErr.Error()
			return
		}
		}
	}

	// ── Stage: Save Metadata ─────────────────────────────────────────────
	if p.CurrentStage == StageSaveMetadata {
		ch.Info("pipeline: stage save_metadata for %s", filename)
		ch.SetUploadProgress(filename, "saving recording metadata", 90, len(p.Links), len(p.Links), 0, 0, "", nil)
		if err := p.stageSaveMetadata(ch); err != nil {
			ch.Error("pipeline: metadata stage failed for %s: %v", filename, err)
			p.Failed = true
			p.MetadataFailed = true
			p.LastError = err.Error()
			return
		}
		p.advanceTo(StageCleanup)
	}

	// ── Stage: Cleanup ───────────────────────────────────────────────────
	if p.CurrentStage == StageCleanup {
		ch.Info("pipeline: stage cleanup for %s", filename)
		ch.SetUploadProgress(filename, "cleaning up local files", 95, len(p.Links), len(p.Links), 0, 0, "", nil)
		if err := p.stageCleanup(ch); err != nil {
			ch.Error("pipeline: cleanup stage failed for %s: %v", filename, err)
			p.Failed = true
			p.LastError = err.Error()
			return
		}
		p.advanceTo(StageDone)
	}

	if p.CurrentStage == StageDone {
		ch.Info("pipeline: completed %s successfully", filename)
	} else if !p.Failed {
		ch.Info("pipeline: %s paused at stage %s (will retry)", filename, p.CurrentStage)
	}
	ch.ClearUploadProgress(filename)
}

// containsHash returns true if a pipeline with the given file hash is already
// waiting in the queue.  Caller must hold pq.mu.
func (pq *PipelineQueue) containsHash(fileHash string) bool {
	if fileHash == "" {
		return false
	}
	for _, p := range pq.pipelines {
		if p.FileHash == fileHash {
			return true
		}
	}
	return false
}

// EnqueueFile creates a pipeline for a finalized video file and adds it to the queue.
func (pq *PipelineQueue) EnqueueFile(filePath string) {
	base := filepath.Base(filePath)
	if !videoExt(base) || isSidecar(base) {
		return
	}

	// Reject files where the model username cannot be extracted from the
	// filename — these are corrupted recordings (e.g. ".video.muxed.mp4"
	// when the DVR crashed before setting the channel username).
	if user := extractUsernameFromFilename(base); user == "" {
		pq.ch.Warn("pipeline: %s has no extractable username — skipping corrupted file", base)
		_ = os.Remove(filePath)
		return
	}

	MarkUploadInFlight(filePath)

	fileHash, hashErr := internal.FastFileHash(filePath)
	if hashErr != nil {
		pq.ch.Warn("pipeline: could not hash %s (state persistence limited): %v", base, hashErr)
	}

	// Phase 2: When hashing fails, use a deterministic fallback key so
	// pipeline state can still be persisted and recovered.
	if fileHash == "" {
		fileHash = "fallback-" + base
	}

	var fileSize int64
	if stat, err := os.Stat(filePath); err == nil {
		fileSize = stat.Size()
	}

	pq.startOnce()

	// Under lock: check stopped, track upload, create pipeline, enqueue — atomic.
	// This prevents Stop() from racing between the stopped check and add-to-queue.
	pq.mu.Lock()
	if pq.stopped {
		pq.mu.Unlock()
		pq.ch.Warn("pipeline: queue stopped, saving %s for recovery on next start", base)
		recoveryPipeline := newPipeline(filePath, fileHash, base, pq.ch.Config.Username, fileSize)
		if saveErr := server.SavePipelineState(recoveryPipeline.toDBState()); saveErr != nil {
			pq.ch.Warn("pipeline: could not save recovery state for %s: %v", base, saveErr)
		}
		MarkUploadDone(filePath)
		return
	}
	// Dedup: a pipeline for this exact file is already queued.  Drop the
	// duplicate so two pipelines can't race on the same upload journal.
	if pq.containsHash(fileHash) {
		pq.mu.Unlock()
		MarkUploadDone(filePath)
		pq.ch.Warn("pipeline: %s already queued (hash=%s), skipping duplicate", base, fileHash)
		return
	}

	pq.ch.UploadWg.Add(1)
	p := newPipeline(filePath, fileHash, base, pq.ch.Config.Username, fileSize)

	// Snapshot channel metadata under stateMu, then pq.mu — safe lock ordering.
	pq.ch.stateMu.Lock()
	p.RoomTitle = pq.ch.RoomTitle
	p.Tags = append([]string{}, pq.ch.Tags...)
	p.Viewers = pq.ch.Viewers
	p.Gender = pq.ch.Gender
	p.Resolution = pq.ch.Resolution
	p.Framerate = pq.ch.Framerate
	roomTitle := p.RoomTitle
	tags := make([]string, len(p.Tags))
	copy(tags, p.Tags)
	viewers := p.Viewers
	gender := p.Gender
	resolution := p.Resolution
	framerate := p.Framerate
	pq.ch.stateMu.Unlock()

	// Persist initial state for crash recovery (best-effort) while we
	// still hold pq.mu, before the worker can race on p's fields.
	if hErr := server.SavePipelineState(p.toDBState()); hErr != nil {
		pq.ch.Warn("pipeline: could not persist initial state for %s: %v", p.Filename, hErr)
	}

	pq.pipelines = append(pq.pipelines, p)
	pq.mu.Unlock()
	pq.cond.Signal()

	// Phase 1: Save basic recording metadata immediately so it's never lost
	// even if the process is killed during upload. stageSaveMetadata later
	// overwrites this with full data (thumbnails, upload links) via upsert.
	timestamp := extractTimestampFromFilename(base)
	if timestamp == "" {
		if st, statErr := os.Stat(filePath); statErr == nil {
			timestamp = st.ModTime().UTC().Format("2006-01-02T15:04:05Z")
		} else {
			timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
		}
	}
	dur, _ := VideoDurationSeconds(filePath)
	func() {
		defer func() {
			if r := recover(); r != nil {
				pq.ch.Error("pipeline: SaveRecordingBasics panicked for %s: %v", base, r)
			}
		}()
		if saveErr := server.SaveRecordingBasics(
			pq.ch.Config.Username, base, timestamp,
			roomTitle, tags, viewers,
			gender, resolution, framerate,
			fileSize, dur,
		); saveErr != nil {
			pq.ch.Warn("pipeline: could not save early metadata for %s: %v", base, saveErr)
		} else {
			pq.ch.Info("pipeline: saved early metadata for %s", base)
		}
	}()

}

// ResumePending loads incomplete pipelines from Supabase and re-queues them.
func (pq *PipelineQueue) ResumePending() {
	states, err := server.LoadAllPipelineStates()
	if err != nil {
		pq.ch.Warn("pipeline: could not load pending states: %v", err)
		return
	}
	if len(states) == 0 {
		return
	}
	pq.startOnce()
	for _, s := range states {
		if s.FileHash == "" {
			continue
		}
		username := s.Username
		if username == "" {
			username = extractUsernameFromFilename(s.Filename)
		}
		if username != "" && username != pq.ch.Config.Username {
			continue
		}
		// Only resume pipeline states owned by this node. States from other
		// nodes reference files on their disks and would fail or double-upload.
		// Legacy states with an empty NodeID are still resumed for compatibility.
		if s.NodeID != "" && s.NodeID != server.NodeID() {
			continue
		}
		// Check file still exists
		if _, statErr := os.Stat(s.FilePath); os.IsNotExist(statErr) {
			if delErr := server.DeletePipelineState(s.FileHash); delErr != nil {
				pq.ch.Warn("pipeline: could not delete stale state for %s: %v", s.Filename, delErr)
			}
			continue
		}
		// Skip pipelines that have exhausted their retry budget.
		if s.Retries >= maxPipelineRetries {
			pq.ch.Warn("pipeline: skipping %s — %d retries exhausted (last error: %s)",
				s.Filename, s.Retries, s.LastError)
			if delErr := server.DeletePipelineState(s.FileHash); delErr != nil {
				pq.ch.Warn("pipeline: could not delete exhausted state for %s: %v", s.Filename, delErr)
			}
			continue
		}
		// Dedup: skip if a pipeline for this hash is already queued (e.g.
		// ResumePending called twice, or the file was re-enqueued manually).
		pq.mu.Lock()
		if pq.containsHash(s.FileHash) {
			pq.mu.Unlock()
			continue
		}
		p := pipelineFromDBState(&s)
		MarkUploadInFlight(s.FilePath)
		pq.ch.UploadWg.Add(1)
		pq.ch.Info("pipeline: resuming %s at stage %s (retry %d)", s.Filename, s.CurrentStage, s.Retries)
		pq.pipelines = append(pq.pipelines, p)
		pq.mu.Unlock()
		pq.cond.Signal()
	}
}

func formatSpeed(bytesPerSec float64) string {
	switch {
	case bytesPerSec >= 1_000_000_000:
		return fmt.Sprintf("%.1f GB/s", bytesPerSec/1_000_000_000)
	case bytesPerSec >= 1_000_000:
		return fmt.Sprintf("%.1f MB/s", bytesPerSec/1_000_000)
	case bytesPerSec >= 1_000:
		return fmt.Sprintf("%.0f KB/s", bytesPerSec/1_000)
	default:
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
}

// deleteLocalFile attempts to remove a local video file with retry.  Used by
// pipeline failure paths to prevent disk filling when uploads fail or the
// pipeline is abandoned.
func deleteLocalFile(ch *Channel, filename, filePath string) {
	if filePath == "" {
		return
	}
	DeleteSidecarFiles(filePath)
	if err := removeFileWithRetry(filePath); err != nil {
		ch.Warn("pipeline: could not remove %s after failure: %v", filename, err)
	} else {
		ch.Info("pipeline: removed %s despite pipeline failure", filename)
	}
}
