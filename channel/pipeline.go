package channel

import (
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

// pipelineThumbnailTimeout bounds how long the thumbnail leg of the
// thumbnail_upload stage may run before the pipeline proceeds WITHOUT it.
//
// Thumbnail generation is normally bounded by per-ffmpeg-call context
// timeouts and image-host HTTP client timeouts, so a healthy node finishes in
// minutes. But the stage must never hang FOREVER: the node-13 production
// wedge froze every pipeline at thumbnail_upload with updated==created (video
// uploads in the parallel goroutine still succeeded, so recordings had links
// yet files were never cleaned and the disk filled at ~13 GB/h) because a
// After upload succeeds, wait this long for thumbnail generation. If exceeded,
// proceed without thumbnails — the file is kept (cleanup checks for thumbURL)
// and ScanThumbnails/backfill fills it in later. 15 minutes is generous:
// normal thumbnails take minutes; if it's been 15min something is wrong
// (deadlocked ffmpeg, image-host outage) and we shouldn't block the pipeline.
const pipelineThumbnailTimeout = 15 * time.Minute

// pipelineUploadTimeout bounds how long the video-upload leg of the
// thumbnail_upload stage may run before the pipeline gives up and fails for
// retry.  Unlike the thumbnail leg we CANNOT proceed without the upload —
// stageSaveMetadata needs the download links — but a wedged upload goroutine
// (upload-semaphore starvation, a deadlocked host HTTP client) must never
// freeze the pipeline FOREVER, which is exactly the production wedge seen on
// node-13 (pipelines stuck at thumbnail_upload for days, disk filling while
// recordings never saved metadata).
//
// A healthy upload is bounded far tighter: per-host HTTP clients cap at 120
// minutes and DoWithRetry caps at maxChannelUploadAttempts, so a multi-GB
// file normally finishes in tens of minutes.  3 hours is generous headroom
// that still guarantees a wedged upload cannot hold the pipeline overnight.
// On expiry the pipeline fails and the retry re-queues later; the upload
// journal keeps already-succeeded hosts from ever being re-pushed.
const pipelineUploadTimeout = 3 * time.Hour

// defaultPipelineWorkers is how many pipelines one channel's queue processes
// concurrently.  More workers means a channel with a backlog of recordings
// uploads several files at once instead of serially.  The global UploadSem
// and the retry-worker pool still cap total concurrent file uploads across
// all channels, so this only increases parallelism, never violates the
// global cap.
const defaultPipelineWorkers = 6

var (
	pipelineWorkers = defaultPipelineWorkers
)

// SetPipelineWorkers configures how many pipelines a channel queue processes
// concurrently.  Call at startup before any queues are created.
func SetPipelineWorkers(n int) {
	if n > 0 {
		pipelineWorkers = n
	}
}

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

	// Duration is the video length in seconds, captured at enqueue time so
	// stageSaveMetadata can reuse it instead of spawning a second ffprobe.
	// Not persisted (PipelineState has no column); resumed pipelines re-probe.
	Duration float64 `json:"-"`

	CurrentStage Stage  `json:"current_stage"`
	Failed       bool   `json:"failed"`
	LastError    string `json:"last_error"`
	Retries      int    `json:"retries"`

	// retried is set when the pipeline is re-queued after a failure.  The
	// UploadWg token is released only on the first processing of a pipeline,
	// so re-queued attempts never double-decrement it.
	retried bool

	// Channel metadata snapshot captured at enqueue time so stageSaveMetadata
	// uses the state from when the file was recorded, not whatever a newer
	// recording session may have written to the Channel struct.
	RoomTitle  string   `json:"room_title"`
	Tags       []string `json:"tags"`
	Viewers    int      `json:"viewers"`
	Gender     string   `json:"gender"`
	Resolution string   `json:"resolution"`
	Framerate  int      `json:"framerate"`
	// EndReason is why the recording stopped (captured from closeReason at
	// file-close time); persisted to the recordings row in Supabase.
	EndReason string `json:"end_reason,omitempty"`

	// Results populated by stages, consumed by downstream stages
	ThumbURL       string            `json:"thumb_url"`
	SpriteURL      string            `json:"sprite_url"`
	PreviewURL     string            `json:"preview_url"`
	ThumbMirrors   map[string]string `json:"thumb_mirrors,omitempty"`   // host -> URL
	SpriteMirrors  map[string]string `json:"sprite_mirrors,omitempty"`  // host -> URL
	PreviewMirrors map[string]string `json:"preview_mirrors,omitempty"` // host -> URL
	EmbedURL       string            `json:"embed_url"`
	Links          map[string]string `json:"links"`   // host -> download URL
	NodeID         string            `json:"node_id"` // node that owns this pipeline

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
		NodeID:       server.NodeID(),
	}
}

// setEndReason records why the recording stopped. Called right after creation
// (and on resume when persisted state carries it) so stageSaveMetadata can
// persist it to the recordings row.
func (p *Pipeline) setEndReason(reason string) {
	p.mu.Lock()
	p.EndReason = reason
	p.mu.Unlock()
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
		FileHash:       p.FileHash,
		FilePath:       p.FilePath,
		Filename:       p.Filename,
		Username:       p.Username,
		FileSize:       p.FileSize,
		CurrentStage:   p.CurrentStage.String(),
		Failed:         p.Failed,
		LastError:      p.LastError,
		Retries:        p.Retries,
		ThumbURL:       p.ThumbURL,
		SpriteURL:      p.SpriteURL,
		PreviewURL:     p.PreviewURL,
		ThumbMirrors:   p.ThumbMirrors,
		SpriteMirrors:  p.SpriteMirrors,
		PreviewMirrors: p.PreviewMirrors,
		EmbedURL:       p.EmbedURL,
		LinksJSON:      string(linksJSON),
		NodeID:         p.NodeID,
	}
}

// pipelineFromDBState converts a database.PipelineState back to a Pipeline.
func pipelineFromDBState(s *database.PipelineState) *Pipeline {
	p := &Pipeline{
		FileHash:       s.FileHash,
		FilePath:       s.FilePath,
		Filename:       s.Filename,
		Username:       s.Username,
		FileSize:       s.FileSize,
		CurrentStage:   stageFromString(s.CurrentStage),
		Failed:         s.Failed,
		LastError:      s.LastError,
		Retries:        s.Retries,
		ThumbURL:       s.ThumbURL,
		SpriteURL:      s.SpriteURL,
		PreviewURL:     s.PreviewURL,
		ThumbMirrors:   s.ThumbMirrors,
		SpriteMirrors:  s.SpriteMirrors,
		PreviewMirrors: s.PreviewMirrors,
		EmbedURL:       s.EmbedURL,
		Links:          make(map[string]string),
		NodeID:         s.NodeID,
	}
	if s.LinksJSON != "" {
		json.Unmarshal([]byte(s.LinksJSON), &p.Links)
	}
	return p
}

// stageThumbnail generates thumbnails, sprite, preview and uploads to image
// hosts, then persists the URLs to Supabase IMMEDIATELY.
//
// Persisting here (rather than only in stageSaveMetadata) is deliberate: the
// thumbnail generation runs in parallel with the video upload, and the video
// upload can take many minutes (multi-GB files, multiple hosts) or fail
// entirely (all hosts down).  If the preview links were only saved after the
// upload, a slow or failed upload would leave the recording with NO thumbnail
// in the UI for a long time — or forever (a failed pipeline never reaches
// stageSaveMetadata).  Saving as soon as generation finishes makes the
// thumbnail appear the moment the ffmpeg work completes, independent of the
// video upload.  stageSaveMetadata later re-saves (idempotent upsert by
// filename), and the retry path's early-return below avoids re-generating
// pieces that already succeeded.
func (p *Pipeline) stageThumbnail(ch *Channel) error {
	if p.ThumbURL != "" && p.SpriteURL != "" && p.PreviewURL != "" {
		return nil
	}

	// onHostSave is called the instant a host succeeds for thumb/sprite/preview.
	// It persists the URL to Supabase immediately so the thumbnail appears in
	// the UI without waiting for slower hosts to finish.
	//
	// mirrors accumulates each host URL as it succeeds, so the DB write
	// always includes every mirror seen so far (never loses a host).
	mirrors := map[string]map[string]string{
		"thumb":   {},
		"sprite":  {},
		"preview": {},
	}
	// primary records whether we've picked the primary URL for each asset
	// (first host by priority: Catbox > Pixhost > freeimage.host > …).
	primary := map[string]bool{"thumb": false, "sprite": false, "preview": false}
	onHostSave := func(assetType, host, url string) {
		p.mu.Lock()
		// Set primary URL on first success.
		if !primary[assetType] {
			switch assetType {
			case "thumb":
				p.ThumbURL = url
			case "sprite":
				p.SpriteURL = url
			case "preview":
				p.PreviewURL = url
			}
			primary[assetType] = true
		}
		// Accumulate mirror.
		mirrors[assetType][host] = url

		// Snapshot under lock — including mirror count for the log line.
		thumbURL, spriteURL, previewURL := p.ThumbURL, p.SpriteURL, p.PreviewURL
		thumbMirrors := copyMap(mirrors["thumb"])
		spriteMirrors := copyMap(mirrors["sprite"])
		previewMirrors := copyMap(mirrors["preview"])
		mirrorCount := len(mirrors[assetType])
		// Keep p.*Mirrors authoritative (monotonic) so any LATER re-save —
		// stageSaveMetadata runs minutes later, after mirrors finished — carries
		// the full mirror set and never shrinks what earlier callbacks persisted.
		// Mirrors finish in the background after generateThumbnail returns, so
		// without this sync a future save could clobber the DB's grown mirror set
		// with the partial set known at generation time.
		switch assetType {
		case "thumb":
			p.ThumbMirrors = copyMap(mirrors["thumb"])
		case "sprite":
			p.SpriteMirrors = copyMap(mirrors["sprite"])
		case "preview":
			p.PreviewMirrors = copyMap(mirrors["preview"])
		}
		p.mu.Unlock()

		// Save primary + ALL mirrors seen so far.  Each host success
		// overwrites with one more mirror — the DB always has the
		// latest complete set, not a stale partial one.
		if err := server.SavePreviewLinks(p.Filename, thumbURL, spriteURL, previewURL, thumbMirrors, spriteMirrors, previewMirrors); err != nil {
			ch.Warn("pipeline: could not save %s URL from %s for %s: %v", assetType, host, p.Filename, err)
		} else {
			ch.Info("pipeline: saved %s URL from %s for %s (%d mirrors)", assetType, host, p.Filename, mirrorCount)
		}
	}

	thumb := ch.generateThumbnail(p.FilePath, onHostSave)
	p.mu.Lock()
	// Fill in only the pieces still missing so a partial failure (e.g. the
	// preview generated but the thumbnail did not) never discards work that
	// already succeeded.
	// Always prefer the priority-ordered primary URL from generateThumbnail
	// (Catbox > Pixhost > freeimage.host) over the non-deterministic first-
	// to-succeed value that onHostSave may have set.
	if thumb.ThumbURL != "" {
		p.ThumbURL = thumb.ThumbURL
	}
	if thumb.SpriteURL != "" {
		p.SpriteURL = thumb.SpriteURL
	}
	if thumb.PreviewURL != "" {
		p.PreviewURL = thumb.PreviewURL
	}
	// Union the generator's mirror results into the onHostSave accumulation —
	// the callback set is the authoritative superset (it keeps firing after
	// generateThumbnail returns as background mirrors land), but the union
	// loses nothing if a callback was dropped.  Never replace: a replace could
	// shrink the DB's mirror set on a later stageSaveMetadata re-save.
	p.ThumbMirrors = mergeMirrorMaps(mirrors["thumb"], thumb.ThumbMirrors)
	p.SpriteMirrors = mergeMirrorMaps(mirrors["sprite"], thumb.SpriteMirrors)
	p.PreviewMirrors = mergeMirrorMaps(mirrors["preview"], thumb.PreviewMirrors)
	p.mu.Unlock()

	// Final persist — covers any URLs the onHostSave callback missed
	// (e.g. if all hosts failed and we got nothing).
	if p.ThumbURL != "" || p.SpriteURL != "" || p.PreviewURL != "" {
		if err := server.SavePreviewLinks(p.Filename, p.ThumbURL, p.SpriteURL, p.PreviewURL, p.ThumbMirrors, p.SpriteMirrors, p.PreviewMirrors); err != nil {
			ch.Warn("pipeline: could not save preview links early for %s: %v", p.Filename, err)
		} else {
			ch.Info("pipeline: saved preview links early for %s", p.Filename)
		}
	}

	// Return an error when the critical thumbnail is missing so the pipeline
	// retries later instead of silently proceeding with no thumbnail.
	if p.ThumbURL == "" {
		if thumb.Unavailable {
			// The node's tools could not run at all — a different fault from an
			// unthumbnailable file, and one that clears on its own.  Report it as
			// such so the pipeline delays its retry instead of spending the
			// budget inside the same outage.
			return nodeToolFailureError(p.Filename)
		}
		return fmt.Errorf("thumbnail generation/upload failed for %s — will retry later", p.Filename)
	}
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
		cfg.VidaraKey,
		ch,
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
			// The journal says every host succeeded but p.Links is empty
			// (typically after a crash/restart before state was persisted).
			// Try to recover the download links from the journal instead
			// of destroying them and re-uploading to every host.
			if p.FileHash != "" {
				if recovered := server.LoadJournalLinks(p.FileHash); len(recovered) > 0 {
					ch.Info("upload: recovered %d links from journal for %s", len(recovered), filename)
					p.Links = recovered
					for host, link := range recovered {
						if p.EmbedURL == "" {
							p.EmbedURL = embedURLFromLink(host, link)
						}
					}
					return nil
				}
				ch.Warn("upload: stale journal for %s has no recoverable links; clearing and re-uploading", filename)
				if jErr := server.DeleteJournalByHash(p.FileHash); jErr != nil {
					ch.Warn("upload: could not clear stale journal for %s: %v", filename, jErr)
				}
			}
			completedHosts = nil
			hostsToTry = allHosts
		} else if p.FileHash != "" {
			// Partial skip: some hosts were skipped because the journal already
			// has them "done", but only the all-skipped branch above restores
			// their links into p.Links.  Pull them back here too so they report
			// as "done" in the upload matrix (and survive into stageSaveMetadata)
			// instead of showing "pending" for a host that already has the file.
			if recovered := server.LoadJournalLinks(p.FileHash); len(recovered) > 0 {
				for host, link := range recovered {
					if p.Links[host] == "" {
						p.Links[host] = link
						if p.EmbedURL == "" {
							p.EmbedURL = embedURLFromLink(host, link)
						}
					}
				}
			}
		}
	ch.Info("upload: %d/%d hosts already have this file — uploading to %d remaining",
		len(completedHosts), len(allHosts), len(hostsToTry))
	}

	var results []uploader.UploadResult
	var success []uploader.UploadResult
	// success is reassigned by the retry worker (below) while the per-host
	// uploader goroutines' progress callback reads it — guard both sides.
	var successMu sync.Mutex

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
		// Feed the node-wide throughput estimate (early final-drain decision).
		// Delta must be captured BEFORE hp.bytes is overwritten below.
		if delta := current - hp.bytes; delta > 0 {
			RecordUploadBytes(delta)
		}
		hp.bytes = current
		hp.lastTime = now
		hostProgress[host] = hp
		hostMu.Unlock()

		successMu.Lock()
		hostCount := len(success)
		uploadedHosts := make(map[string]bool)
		for _, r := range success {
			uploadedHosts[r.Host] = true
		}
		successMu.Unlock()

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

	// Use RetryManager to handle upload retries in background
	err := DoWithRetry("upload-"+p.FileHash, func() error {
		// recordingID is resolved once (via sync.Once) and reused for all per-host
		// saves to avoid N+1 GetRecording queries (one per host succeeding).
		var recordingID string
		var recordingIDOnce sync.Once
		attemptResults := upl.UploadSelectedWithCallback(filePath, hostsToTry, func(host, url string) {
		// Persist the journal entry FIRST — while the upload result and its
		// download link are in hand — then save the link to the recordings
		// row.  If the runner dies between the two, or the DB save exhausts
		// its retries, the journal carries the link so ReconcileMissingUploadLinks
		// can restore it on the next boot.  Previously the journal was written
		// only for the empty-recording-ID path (and wholesale after all hosts),
		// so a crash in between left a host "done" in the journal with its link
		// permanently unpersisted.
		if jErr := server.SaveJournalEntry(p.FileHash, filename, host, "success", url, 0, ""); jErr != nil {
			ch.Warn("upload: could not save journal entry for %s/%s: %v", host, filename, jErr)
		}
		// Save the upload link to Supabase the instant a host succeeds —
		// don't wait for all hosts.  If the runner VM dies before other
		// hosts finish, this link survives.
		recordingIDOnce.Do(func() {
			var lookupErr error
			recordingID, lookupErr = server.GetRecordingID(filename)
			if lookupErr != nil {
				// Recording row doesn't exist yet (SaveRecordingBasics failed
				// or was skipped).  Create it now so the upload link can be
				// persisted immediately instead of waiting for stageSaveMetadata.
				ch.Warn("upload: recording row missing for %s, creating basics", filename)
				if bErr := server.SaveRecordingBasics(
					extractUsernameFromFilename(filename),
					filename,
					time.Now().UTC().Format("2006-01-02T15:04:05Z"),
					"", nil, 0, "", "", "", 0, 0, 0,
				); bErr != nil {
					ch.Warn("upload: could not create recording basics for %s: %v", filename, bErr)
					return
				}
				recordingID, lookupErr = server.GetRecordingID(filename)
				if lookupErr != nil {
					ch.Warn("upload: could not find recording ID for %s after creating basics: %v", filename, lookupErr)
				}
			}
		})
		if recordingID == "" {
			// Journal entry above preserves the link; stageSaveMetadata
			// will reconcile later.  This should be extremely rare.
			return
		}
		if saveErr := server.SaveUploadLinkByIDWithFilename(recordingID, host, url, filename); saveErr != nil {
			ch.Warn("upload: could not save link from %s for %s immediately: %v", host, filename, saveErr)
		} else {
			ch.Info("upload: saved link from %s for %s immediately", host, filename)
		}
	})
		results = append(results, attemptResults...)

		// Save journal entries for each result
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
				if jErr := server.SaveJournalEntry(p.FileHash, filename, r.Host, status, r.DownloadLink, filesize, errMsg); jErr != nil {
					ch.Warn("upload: could not save journal for %s/%s: %v", r.Host, filename, jErr)
				}
			}
		}

		successMu.Lock()
		success = uploader.GetSuccessfulUploads(results)
		successMu.Unlock()
		if len(success) >= len(allHosts) {
			return nil
		}

		// Some hosts failed — update hostsToTry for next retry
		failedHosts := failedHostNames(results, completedHosts)
		hostsToTry = failedHosts
		if len(hostsToTry) == 0 {
			return nil
		}

		return fmt.Errorf("%d hosts still pending", len(hostsToTry))
	}, WithUploadSem(), WithMaxAttempts(maxChannelUploadAttempts), WithBaseBackoff(channelUploadRetryDelay))

	// Persist links from every host that DID succeed, even if another host is
	// down.  A single failing host (e.g. an expired API key returning 403)
	// must not discard the successful uploads — that left recordings as bare
	// rows with no embed URL and no upload_links.
	for _, r := range success {
		p.Links[r.Host] = r.DownloadLink
		if p.EmbedURL == "" {
			p.EmbedURL = embedURLFromLink(r.Host, r.DownloadLink)
		}
	}

	if err != nil {
		if len(p.Links) == 0 {
			return err
		}
		ch.Warn("upload: %d/%d hosts succeeded for %s despite errors (%v) — persisting partial links",
			len(p.Links), len(allHosts), filename, err)
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

	// Emit an accurate terminal upload state the instant the video transfer
	// finishes. Previously the last per-host progress callback (often a host
	// at 100% with 0/6 marked "done") was the final update until the NEXT
	// stage advanced, so a file whose upload had actually completed could sit
	// in the admin upload matrix looking mid-transfer for minutes — or
	// forever while the thumbnail leg of the stage ran on (or wedged, as in
	// the node-13 ffmpegSem deadlock). The matrix now shows the real outcome:
	// hosts that got links are "done", the rest are "pending"/"failed", and
	// the status names the current phase instead of a stale byte count.
	{
		failedHosts := map[string]bool{}
		for _, r := range results {
			if r.Error != nil {
				failedHosts[r.Host] = true
			}
		}
		hosts := make([]entity.HostEntry, 0, len(allHosts))
		for _, h := range allHosts {
			e := entity.HostEntry{Host: h}
			e.BytesTotal = p.FileSize
			switch {
			case p.Links[h] != "":
				// Uploaded now, or already complete per the journal (skipped hosts
				// may not appear in this run's results).
				e.Status = "done"
				e.Progress = 100
				e.BytesCurrent = p.FileSize
			case failedHosts[h]:
				e.Status = "failed"
				e.Progress = 100
			default:
				e.Status = "pending"
			}
			hosts = append(hosts, e)
		}
		status := fmt.Sprintf("uploaded %d/%d hosts — processing", len(p.Links), len(allHosts))
		ch.SetUploadProgress(filename, status, 80, len(p.Links), len(allHosts), p.FileSize, p.FileSize, "", hosts)
	}

	return nil
}

// stageSaveMetadata persists recording metadata and all links to Supabase.
func (p *Pipeline) stageSaveMetadata(ch *Channel) error {
	// Retry thumbnail generation if the THUMBNAIL — the asset actually shown
	// on the video card — is still missing.  Gate on the thumbnail only, NOT
	// on the sprite/preview: the animated preview frequently fails (Catbox
	// down, ImgBB rate-limited), and re-generating all three pieces for a
	// file whose thumbnail already succeeded would re-upload the working
	// thumbnail and sprite to the image hosts on every retry — exactly the
	// host-hammering loop that causes the fleet-wide rate-limit failures.
	// A missing sprite/preview is cosmetic; a missing thumbnail is not.
	if p.ThumbURL == "" {
		thumb := ch.generateThumbnail(p.FilePath, nil)
		generated := false
		if p.ThumbURL == "" && thumb.ThumbURL != "" {
			p.ThumbURL = thumb.ThumbURL
			generated = true
		}
		if p.SpriteURL == "" && thumb.SpriteURL != "" {
			p.SpriteURL = thumb.SpriteURL
			generated = true
		}
		if p.PreviewURL == "" && thumb.PreviewURL != "" {
			p.PreviewURL = thumb.PreviewURL
			generated = true
		}
		// Union mirrors from the retry generation into what we already have (the
		// early onHostSave writes may have persisted a larger set).
		p.mu.Lock()
		p.ThumbMirrors = mergeMirrorMaps(p.ThumbMirrors, thumb.ThumbMirrors)
		p.SpriteMirrors = mergeMirrorMaps(p.SpriteMirrors, thumb.SpriteMirrors)
		p.PreviewMirrors = mergeMirrorMaps(p.PreviewMirrors, thumb.PreviewMirrors)
		p.mu.Unlock()
		switch {
		case generated:
			ch.Info("upload: generated missing presentation assets for %s (retry)", p.Filename)
		case thumb.Unavailable:
			// Node-level tool outage: fail with the distinguishable error so the
			// pipeline backs off instead of immediately re-running the same
			// ~28 ffmpeg invocations inside the same broken window.
			ch.Error("upload: %s: ffmpeg could not run on this node — keeping recording for a later retry instead of saving it without a thumbnail", p.Filename)
			err := nodeToolFailureError(p.Filename)
			p.LastError = err.Error()
			return err
		default:
			ch.Warn("upload: thumbnail generation failed for %s (skipped — local file kept for later retry)", p.Filename)
		}
		// If the critical thumbnail is still missing after this retry, fail the
		// metadata stage so the pipeline retries (rather than writing a row
		// with a NULL thumbnail that the UI would show without a preview).
		// The local file is kept for the later retry per stageCleanup.
		if p.ThumbURL == "" {
			ch.Error("upload: no thumbnail for %s after retry — keeping recording for later retry", p.Filename)
			p.LastError = "thumbnail generation failed after retry"
			return fmt.Errorf("thumbnail generation failed after retry for %s", p.Filename)
		}
	}

	if p.ThumbURL != "" || p.SpriteURL != "" || p.PreviewURL != "" {
		if err := server.SavePreviewLinks(p.Filename, p.ThumbURL, p.SpriteURL, p.PreviewURL, p.ThumbMirrors, p.SpriteMirrors, p.PreviewMirrors); err != nil {
			ch.Error("upload: could not save preview links for %s: %v", p.Filename, err)
			p.LastError = err.Error()
			return err
		}
		ch.Info("upload: saved preview links for %s", p.Filename)
	}

	if len(p.Links) == 0 {
		return fmt.Errorf("no upload links to save for %s", p.Filename)
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

	// Reuse the duration probed at enqueue time — no second ffprobe spawn
	// (each holds a global ffmpeg slot).  Only re-probe when it wasn't
	// captured, e.g. a pipeline resumed from persisted state.
	dur := p.Duration
	if dur <= 0 {
		var probeErr error
		dur, probeErr = VideoDurationSeconds(p.FilePath)
		if probeErr != nil {
			ch.Warn("upload: could not probe duration for %s: %v", p.Filename, probeErr)
		}
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
		p.EndReason,
		p.EmbedURL,
		p.ThumbURL,
		p.SpriteURL,
		p.PreviewURL,
		p.Links,
	); err != nil {
		ch.Error("upload: failed to save to Supabase: %v", err)
		// Do NOT delete the journal here — it contains the download links
		// that succeeded during upload.  Deleting it would force a full
		// re-upload on retry, and if hosts are now down the links are lost
		// forever.  The pipeline retries stageSaveMetadata with the same
		// p.Links, and the journal is only cleared after a successful save
		// or when the upload itself needs to be retried.
		p.LastError = err.Error()
		return err
	}

	ch.Info("upload: saved recording metadata to Supabase for %s", p.Filename)

	// Safety net: if the RPC saved the recording but the thumbnail_url
	// didn't make it (schema mismatch, partial write), explicitly patch it.
	// This is cheap (single PATCH) and prevents the "recording saved but
	// no thumbnail" gap that forces ScanThumbnails to backfill later.
	if p.ThumbURL != "" {
		dbThumb, _, _ := server.VerifyRecordingThumbnails(p.Filename)
		if !dbThumb {
			ch.Warn("upload: thumbnail missing in DB after save for %s — re-saving", p.Filename)
			if err := server.UpdateRecordingThumbnails(p.Filename, p.ThumbURL, p.SpriteURL, p.PreviewURL); err != nil {
				ch.Warn("upload: could not re-save thumbnails for %s: %v", p.Filename, err)
			}
		}
	}
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

	// Keep the local file whenever the THUMBNAIL — the asset actually shown
	// on the video card — is missing, even if the sprite and/or preview
	// succeeded.  Gating on all three being empty (the old check) meant a
	// file whose sprite uploaded but whose thumbnail failed was deleted, and
	// the thumbnail became un-recoverable forever (ScanThumbnails needs the
	// source video).  The startup/periodic ScanThumbnails pass picks these
	// files up and retries generation; deleting here would lose that chance,
	// while the video itself is already safe in the cloud.
	if p.ThumbURL == "" || p.SpriteURL == "" || p.PreviewURL == "" {
		ch.Info("cleanup: keeping %s — thumbnail missing (queued for thumbnail retry)", p.Filename)
		return nil
	}

	// Verify thumbnails actually persisted to the database before deleting
	// local files.  The pipeline object holds URLs in memory, but the DB
	// write may have failed silently (RLS, timeout, network error).  If the
	// thumbnails didn't make it to Supabase, keeping the local file lets
	// ScanThumbnails regenerate them later.
	dbThumb, dbSprite, dbPreview := server.VerifyRecordingThumbnails(p.Filename)
	if !dbThumb || !dbSprite || !dbPreview {
		ch.Warn("cleanup: keeping %s — thumbnails not in recordings table (thumb=%v sprite=%v preview=%v), will retry", p.Filename, dbThumb, dbSprite, dbPreview)
		return nil
	}

	// Also verify preview_images has the thumbnail.  The legacy fallback path
	// writes recordings but may skip preview_images; without this check the
	// local file would be deleted and ScanThumbnails could never regenerate
	// the missing preview_images row.
	if !server.VerifyPreviewImage(p.Filename) {
		ch.Warn("cleanup: keeping %s — thumbnail missing in preview_images table, will retry", p.Filename)
		return nil
	}

	ch.Info("cleanup: removing local files for %s", p.Filename)
	DeleteSidecarFiles(p.FilePath)
	if err := removeFileWithRetry(p.FilePath); err != nil {
		// KEEP the upload journal when the local file could not be removed.
		// The next orphan scan sees the file, finds the journal (all hosts
		// completed), and removes the local copy WITHOUT re-uploading.  Deleting
		// the journal here would destroy the dedup record and trigger a
		// duplicate upload of all hosts on the next scan.
		ch.Warn("cleanup: could not remove %s (will retry on next run, journal kept): %v", p.Filename, err)
		return nil
	}
	ch.Info("cleanup: removed %s", p.Filename)
	if p.FileHash != "" {
		if jErr := server.DeleteJournalByHash(p.FileHash); jErr != nil {
			ch.Warn("cleanup: could not delete journal for %s: %v", p.Filename, jErr)
		}
	}
	return nil
}

// PipelineQueue manages a per-channel ordered queue of pipelines.
// Pipelines are processed concurrently by a small worker pool so a channel
// with a backlog uploads several files in parallel.  Order within a channel is
// largest-file-first: the queue stays sorted by FileSize descending, so
// workers always pick up the biggest recording next (workers may finish out
// of order).
type PipelineQueue struct {
	pipelines []*Pipeline
	mu        sync.Mutex
	cond      *sync.Cond
	wg        sync.WaitGroup
	stopped   bool
	started   bool          // tracks whether the worker goroutine has been launched
	stopCh    chan struct{} // closed on Stop() to cancel pending retry timers
	enqueued  int           // total pipelines accepted (after dedup), incl. currently processing

	// processingBytes is the FileSize of the pipeline currently being worked
	// by each worker (popped but not finished).  PendingBytes() sums this with
	// the queued sizes so the session loop can estimate remaining upload time.
	processingBytes int64

	ch      *Channel
	history []entity.PendingEntry // last 50 completed/failed pipelines
}

// NewPipelineQueue creates a new pipeline queue for a channel.
func NewPipelineQueue(ch *Channel) *PipelineQueue {
	pq := &PipelineQueue{ch: ch, stopCh: make(chan struct{})}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// startOnce launches the worker pool on first use, and relaunches it if
// the queue was previously Stop()ed.  This keeps the queue reusable across
// stop/start cycles instead of leaving it permanently dead after the first
// Stop() — a latent footgun where later EnqueueFile calls would silently
// append pipelines that nothing ever processed.
func (pq *PipelineQueue) startOnce() {
	pq.mu.Lock()
	// If the worker was previously stopped, reset so we can launch a fresh one.
	// wg.Wait() in Stop() guarantees the old goroutines have exited by now, so
	// there is no double-launch risk.
	if pq.started && pq.stopped {
		pq.started = false
		pq.stopped = false
		pq.stopCh = make(chan struct{})
	}
	if !pq.started {
		pq.started = true
		pq.mu.Unlock()
		for i := 0; i < pipelineWorkers; i++ {
			pq.wg.Add(1)
			go pq.processLoop()
		}
		return
	}
	pq.mu.Unlock()
}

// Stop signals the worker to finish after draining the queue.
func (pq *PipelineQueue) Stop() {
	pq.mu.Lock()
	pq.stopped = true
	close(pq.stopCh)
	pq.mu.Unlock()
	pq.cond.Broadcast()
	pq.wg.Wait()
}

// scheduleRetry re-queues a failed pipeline after a backoff delay so a
// transient outage (all hosts down, Supabase hiccup) self-heals without manual
// intervention.  The recording is always kept on disk — retries never delete
// it.  The UploadWg token is NOT re-counted: processPipeline releases it only
// on the first processing (p.retried), so re-queued attempts are free.
//
// If the queue is Stop()ed while the retry is pending, the retry is dropped;
// the persisted state row and the on-disk file are both preserved, so
// ResumePending (restart) and the orphan scan recover it.
func (pq *PipelineQueue) scheduleRetry(p *Pipeline) bool {
	if _, err := os.Stat(p.FilePath); err != nil {
		// Recording vanished externally — nothing left to retry.
		if delErr := server.DeletePipelineState(p.FileHash); delErr != nil {
			pq.ch.Warn("pipeline: could not delete state for vanished %s: %v", p.Filename, delErr)
		}
		return false
	}

	retries := p.Retries
	delay := pipelineRetryDelay(retries, p.LastError)
	p.retried = true
	pq.ch.Warn("pipeline: %s will retry in %s (retry %d/%d)", p.Filename, delay, retries, maxPipelineRetries)

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-pq.stopCh:
			return // queue shutting down — state was persisted, file kept on disk
		}
		pq.mu.Lock()
		if pq.stopped {
			pq.mu.Unlock()
			return
		}
		// Dedup: if ResumePending (or a prior retry) already re-queued this
		// same file while we were waiting out the backoff, don't append a
		// second pipeline that would race the first on the same upload
		// journal.
		if pq.containsHash(p.FileHash) {
			pq.mu.Unlock()
			return
		}
		MarkUploadInFlight(p.FilePath)
		pq.enqueueByPriority(p)
		pq.cond.Broadcast()
		pq.mu.Unlock()
	}()
	return true
}

// processLoop is a single worker goroutine that processes pipelines from the
// shared queue.  A PipelineQueue runs pipelineWorkers of these concurrently.
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
		pq.processingBytes += p.FileSize
		pq.mu.Unlock()

		pq.processPipeline(p)
	}
}

// PendingBytes returns the total size of files still awaiting upload: queued
// pipelines plus the file currently being processed.  The session loop uses
// this to decide when the final drain must start — a multi-GB backlog needs
// the drain to begin early, before the runner's hard deadline.
func (pq *PipelineQueue) PendingBytes() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var total int64
	for _, p := range pq.pipelines {
		total += p.FileSize
	}
	return total + pq.processingBytes
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
				Size:     p.FileSize,
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

	// Release the processing-bytes counter used by the early-drain estimate
	// once the pipeline fully finishes (success, failure, or panic).
	defer func() {
		pq.mu.Lock()
		pq.processingBytes -= p.FileSize
		pq.mu.Unlock()
	}()

	defer func() {
		// Snapshot whether this is the pipeline's first processing BEFORE any
		// retry logic runs: scheduleRetry marks p.retried synchronously, so
		// the UploadWg token must be released based on this snapshot, not the
		// flag's value after a retry has been scheduled.
		firstRun := !p.retried

		if r := recover(); r != nil {
			ch.Error("pipeline: panic processing %s: %v", filename, r)
			p.Failed = true
			p.LastError = fmt.Sprintf("panic: %v", r)
		}

		stageStr := p.CurrentStage.String()
		if p.CurrentStage == StageDone || p.Failed {
			switch {
			case p.CurrentStage == StageDone:
				if delErr := server.DeletePipelineState(p.FileHash); delErr != nil {
					ch.Warn("pipeline: could not delete state for %s: %v", filename, delErr)
				}
			case p.Retries < maxPipelineRetries:
				// Keep the recording and retry in-process with backoff.  The
				// file is NEVER deleted on failure — losing a recording is
				// worse than a retry loop, and stageUploadVideos skips hosts
				// that already succeeded via the upload journal.
				p.Retries++
				if saveErr := server.SavePipelineState(p.toDBState()); saveErr != nil {
					ch.Warn("pipeline: could not persist state for %s: %v", filename, saveErr)
				}
				if p.Failed {
					pq.scheduleRetry(p)
				}
			default:
				// Retries exhausted — keep the recording for recovery instead
				// of deleting it.  Dropping the state row lets the startup and
				// periodic orphan scan pick the file up and retry the upload.
				if IsFFmpegSpawnFailureText(p.LastError) {
					// Not a bad recording: this node could not run ffmpeg across
					// every retry, so an operator needs to look at the node.
					ch.Error("pipeline: %s: ffmpeg could not run on this node across all %d retries — node-level fault, NOT a bad recording (file kept for the recovery scan)", filename, maxPipelineRetries)
				}
				ch.Error("pipeline: %s failed %d times, keeping file for recovery", filename, maxPipelineRetries)
				if delErr := server.DeletePipelineState(p.FileHash); delErr != nil {
					ch.Warn("pipeline: could not delete abandoned state for %s: %v", filename, delErr)
				}
			}
			if m := server.Manager; m != nil {
				m.PublishLog(ch.Config.Username, fmt.Sprintf("[pipeline] %s finished (stage=%s, failed=%v, retries=%d)", filename, p.CurrentStage, p.Failed, p.Retries))
			}
		}

		// Release the upload token only on the first processing of a pipeline.
		// Re-queued retries carry no new token, so they must not Done again.
		if firstRun {
			ch.UploadWg.Done()
		}
		MarkUploadDone(p.FilePath)

		// Record history
		if p.Failed || p.CurrentStage == StageDone {
			pq.pushHistory(entity.PendingEntry{
				Channel:  ch.Config.Username,
				Filename: filename,
				Stage:    stageStr,
				Failed:   p.Failed,
				Error:    p.LastError,
				Size:     p.FileSize,
			})
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
		ch.Info("pipeline: stage thumbnail_upload for %s", filename)
		ch.SetUploadProgress(filename, "generating thumbnails and uploading to hosts", 5, 0, 0, 0, 0, "", nil)

		var thumbErr error
		var uploadErr error
		thumbDone := make(chan error, 1)
		uploadDone := make(chan error, 1)

		// Start thumbnail generation + Pixhost upload in background. The result
		// is ALWAYS delivered on thumbDone (deferred after recover) so a panic
		// can never leave the stage waiting on a channel that never fires.
		go func() {
			var err error
			defer func() {
				if r := recover(); r != nil {
					ch.Error("pipeline: thumbnail panicked for %s: %v", filename, r)
					err = fmt.Errorf("thumbnail panic: %v", r)
				}
				thumbDone <- err
			}()
			err = p.stageThumbnail(ch)
		}()

		// Start video upload in background.
		//
		// UploadSem is NOT acquired here: stageUploadVideos runs its upload
		// inside DoWithRetry(..., WithUploadSem()), and the retry manager
		// worker acquires UploadSem right before executing the job.  An outer
		// acquire here would hold a slot for the entire wait on the retry
		// result channel while the worker waits for a DIFFERENT slot — with
		// enough concurrent pipelines the semaphore saturates, every pipeline
		// waits on its own held slot, the workers starve, and the whole node
		// deadlocks at the thumbnail_upload stage (seen in production: all
		// pipeline states frozen at thumbnail_upload with updated==created).
		go func() {
			var err error
			defer func() {
				if r := recover(); r != nil {
					ch.Error("pipeline: upload goroutine panicked for %s: %v", filename, r)
					err = fmt.Errorf("upload panic: %v", r)
				}
				uploadDone <- err
			}()
			err = p.stageUploadVideos(ch)
		}()

		// Await the video upload FIRST and with a hard cap: abandoning an
		// in-flight upload could let a later stage delete a file that is
		// still being pushed to hosts. The upload is bounded anyway (retry
		// workers with attempt caps and per-host HTTP client timeouts), and
		// pipelineUploadTimeout guarantees a wedged upload goroutine (semaphore
		// starvation, deadlocked host) cannot freeze this stage forever the
		// way the node-13 production wedge did.
		select {
		case uploadErr = <-uploadDone:
		case <-time.After(pipelineUploadTimeout):
			ch.Error("pipeline: upload stage for %s exceeded %s — failing for retry (journal preserves completed hosts)", filename, pipelineUploadTimeout)
			uploadErr = fmt.Errorf("upload stage timed out after %s", pipelineUploadTimeout)
		}

		// If the video upload failed there is nothing to advance — fail fast
		// and let the retry re-queue; do not wait out the thumbnail cap first.
		if uploadErr != nil {
			ch.Error("pipeline: upload stage failed for %s: %v — keeping recording for retry", filename, uploadErr)
			p.Failed = true
			p.LastError = uploadErr.Error()
			return
		}
		if len(p.Links) == 0 {
			ch.Error("pipeline: upload stage produced no links for %s — keeping recording for retry", filename)
			p.Failed = true
			p.LastError = "upload produced no links"
			return
		}

		// Persist state immediately after upload succeeds so that p.Links
		// survives a crash before the deferred state save runs.  Without
		// this, a crash between upload completion and stageSaveMetadata
		// would lose all download links (only in memory) and force a full
		// re-upload on resume.
		if err := server.SavePipelineState(p.toDBState()); err != nil {
			ch.Warn("pipeline: could not persist state with links for %s: %v", filename, err)
		}

		// Upload succeeded: links are persisted. Now await the thumbnails with
		// a generous cap (pipelineThumbnailTimeout) so a deadlocked or starved
		// thumbnail goroutine can never freeze this stage forever (the node-13
		// wedge: pipelines stuck at thumbnail_upload with updated==created,
		// uploads succeeded but files were never cleaned and disk filled). If
		// they overrun, proceed without them — cleanup keeps the file when the
		// thumbnail is missing, and ScanThumbnails/backfill fills it in later.
		select {
		case thumbErr = <-thumbDone:
		case <-time.After(pipelineThumbnailTimeout):
			ch.Error("pipeline: thumbnail stage for %s exceeded %s — proceeding without thumbnails (backfill will retry)", filename, pipelineThumbnailTimeout)
			thumbErr = fmt.Errorf("thumbnail stage timed out after %s", pipelineThumbnailTimeout)
		}

		if thumbErr != nil {
			ch.Warn("pipeline: thumbnail stage failed for %s: %v — will retry in stageSaveMetadata", filename, thumbErr)
			// …unless the node itself is broken.  Finalizing metadata now would
			// write the recording without its thumbnail, and the immediate
			// stageSaveMetadata retry would just re-run the identical work
			// inside the same outage.  Fail the pipeline instead: the file is
			// kept, the retry is delayed (pipelineRetryDelay), and the log names
			// the node rather than the recording.
			if IsFFmpegUnavailable(thumbErr) {
				ch.Error("pipeline: %s: ffmpeg could not run on this node — not finalizing metadata without a thumbnail, will retry after the node recovers", filename)
				p.Failed = true
				p.LastError = thumbErr.Error()
				return
			}
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

	// ── Stage: Save Metadata ─────────────────────────────────────────────
	if p.CurrentStage == StageSaveMetadata {
		ch.Info("pipeline: stage save_metadata for %s", filename)
		ch.SetUploadProgress(filename, "saving recording metadata", 90, len(p.Links), len(p.Links), 0, 0, "", nil)
		if err := p.stageSaveMetadata(ch); err != nil {
			ch.Error("pipeline: metadata stage failed for %s: %v", filename, err)
			p.Failed = true
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
	ch.SetUploadProgress("", "", 0, 0, 0, 0, 0, "", nil)
}

// enqueueByPriority inserts p into the queue keeping it sorted by FileSize
// descending, so workers always pop the LARGEST pending recording first.  A
// channel with a backlog then uploads its biggest files first instead of FIFO
// — multi-GB recordings stop being the stragglers that get killed at the run
// boundary.  Caller must hold pq.mu.
func (pq *PipelineQueue) enqueueByPriority(p *Pipeline) {
	// Linear scan is fine: the per-channel queue holds only that channel's
	// pending files, typically a handful.
	i := 0
	for i < len(pq.pipelines) && pq.pipelines[i].FileSize >= p.FileSize {
		i++
	}
	pq.pipelines = append(pq.pipelines, nil)
	copy(pq.pipelines[i+1:], pq.pipelines[i:])
	pq.pipelines[i] = p
}

// pathQueued returns true if a pipeline for this exact file path is already
// waiting in the queue (not yet popped by a worker).  Uses the path rather than
// the hash so the dedup diagnostic can run without hashing a large file.
func (pq *PipelineQueue) pathQueued(filePath string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for _, p := range pq.pipelines {
		if p.FilePath == filePath {
			return true
		}
	}
	return false
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
	pq.enqueueFile(filePath, false, "")
}

// EnqueueFileClaimed enqueues a file whose in-flight marker has ALREADY been
// set by the caller.  Used by MoveToOutputDir, which marks the destination
// in-flight before the move so the OutputDir watcher can never double-claim
// the freshly relocated recording.  Skipping the IsUploadInFlight early-out
// is safe because the marker was set by us moments ago and no pipeline for
// this brand-new path can exist yet; the queue's hash-based duplicate check
// (containsHash) still drops genuine duplicates.
// endReason is why the recording stopped; it is persisted with the recording
// metadata in Supabase.
func (pq *PipelineQueue) EnqueueFileClaimed(filePath, endReason string) {
	pq.enqueueFile(filePath, true, endReason)
}

func (pq *PipelineQueue) enqueueFile(filePath string, alreadyClaimed bool, endReason string) {
	base := filepath.Base(filePath)
	if !videoExt(base) || isSidecar(base) {
		return
	}

	// Dedup: this exact file is already queued or being processed (possibly by
	// another worker in the pool).  Check before hashing so the second enqueue
	// of a large file doesn't waste time on FastFileHash.
	//
	// alreadyClaimed bypasses the check: the caller set the marker itself (so
	// the watcher skips the file) and must not have that same marker mistaken
	// for an existing pipeline — otherwise the recording is never enqueued.
	if !alreadyClaimed && IsUploadInFlight(filePath) {
		// Log the actual upload state so dedup issues are diagnosable: if a
		// marker is set but no pipeline owns the file (not queued, not being
		// processed), the marker is stale and the file may be stranded until
		// restart or a manual rescan.
		inQueue := pq.pathQueued(filePath)
		beingProcessed := pq.ch.UploadEntry().Filename == base
		pq.ch.Warn("pipeline: %s already uploading — skipping duplicate "+
			"(in-queue=%v being-processed=%v in-flight-markers=%d)",
			base, inQueue, beingProcessed, InFlightCount())
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

	// Belt-and-suspenders min-size guard (the duration/egress paths also check
	// this): never create a recordings row or pipeline for a crash artifact.
	// A file below min-file-size-before-upload is a truncated DVR segment, not
	// a recording, and skipping it here prevents a permanent un-uploadable row
	// (the observed 1,112-byte "bright_diamonds_054" became a recording with
	// duration 0, no thumbnail, and an AnonMP4 embed that could never encode).
	if server.Config != nil && server.Config.MinFileSizeBeforeUpload > 0 {
		minBytes := int64(server.Config.MinFileSizeBeforeUpload) * 1024 * 1024
		if fileSize > 0 && fileSize < minBytes {
			pq.ch.Warn("pipeline: discarding crash-artifact %s (%d bytes < %d)", base, fileSize, minBytes)
			if rmErr := os.Remove(filePath); rmErr != nil && !os.IsNotExist(rmErr) {
				pq.ch.Warn("pipeline: could not delete under-sized %s: %v", base, rmErr)
			}
			MarkUploadDone(filePath)
			return
		}
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
	p.setEndReason(endReason)

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

	pq.enqueueByPriority(p)
	pq.enqueued++
	pq.mu.Unlock()
	pq.cond.Broadcast()

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
	p.Duration = dur // reuse in stageSaveMetadata — skip the second ffprobe
	func() {
		defer func() {
			if r := recover(); r != nil {
				pq.ch.Error("pipeline: SaveRecordingBasics panicked for %s: %v", base, r)
			}
		}()
		if saveErr := server.SaveRecordingBasics(
			pq.ch.Config.Username, base, timestamp,
			roomTitle, tags, viewers,
			gender, p.EndReason, resolution, framerate,
			fileSize, dur,
		); saveErr != nil {
			pq.ch.Warn("pipeline: could not save early metadata for %s: %v", base, saveErr)
		} else {
			pq.ch.Info("pipeline: saved early metadata for %s", base)
		}
	}()

	// Persist initial state for crash recovery (best-effort).
	if hErr := server.SavePipelineState(p.toDBState()); hErr != nil {
		pq.ch.Warn("pipeline: could not persist initial state for %s: %v", p.Filename, hErr)
	}
}

// EnqueuedCount returns the total number of pipelines accepted by the queue
// (after dedup), including those currently being processed.  It is monotonic.
func (pq *PipelineQueue) EnqueuedCount() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.enqueued
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
		// ResumePending called twice, or the file was re-enqueued manually),
		// or if this exact file is currently being processed by a worker.
		pq.mu.Lock()
		if pq.containsHash(s.FileHash) || IsUploadInFlight(s.FilePath) {
			pq.mu.Unlock()
			continue
		}
		p := pipelineFromDBState(&s)
		MarkUploadInFlight(s.FilePath)
		pq.ch.UploadWg.Add(1)
		pq.ch.Info("pipeline: resuming %s at stage %s (retry %d)", s.Filename, s.CurrentStage, s.Retries)
		pq.enqueueByPriority(p)
		pq.enqueued++
		pq.mu.Unlock()
		pq.cond.Broadcast()
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

// copyMap returns a shallow copy of m so the caller can snapshot a mirror
// map without holding the lock during the DB write.
func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// mergeMirrorMaps returns a new map containing every key from a and b (b wins
// on collision).  Neither input is mutated, so it is safe to union a
// snapshot taken under lock with a result map written by background
// goroutines.
func mergeMirrorMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
