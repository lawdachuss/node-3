package channel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
)

// pendingFile tracks a closed recording file awaiting post-processing
// (finalize → move to output dir → thumbnail → upload → DB save → deletion).
type pendingFile struct {
	videoPath string
	// endReason is why the recording stopped (captured from closeReason before
	// cleanupLocked consumes it); persisted to the recordings row in Supabase.
	endReason string
}

// Channel represents a channel instance.
type Channel struct {
	CancelFunc      context.CancelFunc
	PauseCancelFunc context.CancelFunc
	cancelMu        sync.Mutex // guards CancelFunc and PauseCancelFunc writes
	LogCh           chan string
	// VerboseCh carries high-frequency per-segment status lines (duration,
	// filesize). They are shown in the UI exactly like LogCh lines but are NOT
	// forwarded to Supabase channel_logs — persisting them would flood the
	// table thousands of rows per channel per hour.
	VerboseCh chan string
	UpdateCh  chan bool
	done      chan struct{} // closed when channel is torn down
	closeDone sync.Once     // ensures done is closed exactly once

	// logPersistLast is the last time an INFO log line was persisted to
	// Supabase, used to rate-limit them. Written only from the Publisher
	// goroutine (single-threaded), so it needs no lock.
	logPersistLast time.Time

	IsOnline        bool
	IsConnecting    bool   // true during retry/reconnect, shown as "Reconnecting..." in UI
	RoomStatus      string // public, private, group, away, offline, hidden
	StreamedAt      int64
	Duration        float64 // Seconds
	Filesize        int     // Bytes
	Sequence        int
	FileExt         string // ".ts" or ".mp4", set per-stream
	CurrentFilename string

	CompressingCount int32 // atomic: number of active compression goroutines

	// autoResumedFromPause is set when the channel was automatically resumed
	// from a stuck paused-but-still-assigned state (manager's
	// CreateChannelFromAssignment). ExportInfo surfaces it as a UI badge so the
	// node web UI makes the recovery visible.
	autoResumedFromPause atomic.Bool

	stateMu sync.Mutex // protects IsOnline, IsConnecting, RoomStatus, metadata fields

	// pauseReasonMu guards pauseReason — why the channel is paused (manual,
	// session-boundary, or handoff). A manual (user) pause is sticky: automatic
	// re-pauses never overwrite it, so automatic resume paths (session
	// boundary restart, claim/reconcile) can tell a user pause apart and leave
	// it alone instead of fighting it.
	pauseReasonMu sync.Mutex
	pauseReason   string

	RoomTitle        string   // captured from API, persisted even when offline
	Tags             []string // captured from API at recording start
	Viewers          int      // captured from API at recording start
	Gender           string   // broadcaster_gender from API ("m", "f", "c", "t", …)
	Resolution       string   // actual stream resolution (e.g. "1920x1080")
	Framerate        int      // actual stream framerate (e.g. 30)
	LiveThumbURL     string   // live thumbnail URL for the current stream
	SummaryCardImage string   // static profile card image; persisted even when offline
	CFBlockCount     int      // consecutive Cloudflare-blocked responses

	Logs   []string
	logsMu sync.Mutex

	File           *os.File
	mp4InitSegment []byte
	Config         *entity.ChannelConfig

	fileMu sync.RWMutex // protects File, mp4InitSegment, Duration, Filesize, TotalDiskUsageBytes

	// closeReason records why the current recording is ending (model offline,
	// stream session expired, max duration/filesize rotation, pause/stop,
	// session boundary). Set before cleanupLocked closes the file so the
	// end-reason can be logged with the final duration.
	closeReason             string
	profileMu               sync.Mutex
	lastProfileScrape       time.Time // when the full profile was last fetched (on-demand only)
	monitorMu               sync.Mutex
	monitorRunning          bool
	monitorRestartRequested bool
	monitorRunID            uint64
	monitorDone             chan struct{}

	cleanupMu     sync.Mutex // serialises Cleanup() calls from concurrent goroutines
	pendingFiles  []pendingFile
	pendingWg     sync.WaitGroup // tracks async pending-file finalization goroutines
	UploadWg      sync.WaitGroup // tracks in-flight upload goroutines for graceful shutdown
	monitorWg     sync.WaitGroup // tracks the Monitor goroutine lifetime
	uploadSem     chan struct{}  // per-channel upload semaphore (1 at a time)
	PipelineQueue *PipelineQueue // ordered pipeline for thumbnails → upload → metadata → cleanup

	TotalDiskUsageBytes int64 // total bytes across all recordings for this channel

	// Upload progress tracking — updated by the pipeline worker goroutine.
	// Thread-safe via uploadStatusMu; visible in the UI via ExportInfo().
	uploadStatusMu   sync.Mutex
	UploadStatus     string             // human-readable status: "", "generating thumbnails…", "uploading (2/5 hosts)…"
	UploadProgress   float64            // 0–100, best-effort estimate
	UploadFilename   string             // file currently being processed by the pipeline
	UploadHostCount  int                // how many hosts have completed
	UploadHostTotal  int                // total hosts to upload to
	UploadBytesCur   int64              // bytes uploaded so far
	UploadBytesTotal int64              // total file size
	UploadSpeed      string             // formatted aggregate speed
	UploadHosts      []entity.HostEntry // per-host progress
}

// New creates a new channel instance with the given manager and configuration.
func New(conf *entity.ChannelConfig) *Channel {
	ch := &Channel{
		LogCh:            make(chan string, 256),
		VerboseCh:        make(chan string, 256),
		UpdateCh:         make(chan bool, 64),
		done:             make(chan struct{}),
		Config:           conf,
		CancelFunc:       func() {},
		PauseCancelFunc:  func() {},
		uploadSem:        make(chan struct{}, 1),
		RoomStatus:       "offline",
		RoomTitle:        conf.RoomTitle,
		Gender:           conf.Gender,
		SummaryCardImage: conf.SummaryCardImage,
		StreamedAt:       conf.StreamedAt,
	}
	ch.PipelineQueue = NewPipelineQueue(ch)
	go ch.Publisher()

	return ch
}

// Publisher listens for log messages and updates from the channel.
// Progress updates are coalesced so busy channels do not repaint the UI more
// often than a person can read it.
func (ch *Channel) Publisher() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC [%s] publisher: %v", ch.Config.Username, r)
			// Restart the publisher so the channel stays responsive.
			go ch.Publisher()
		}
	}()
	updateTimer := time.NewTimer(0)
	if !updateTimer.Stop() {
		<-updateTimer.C
	}
	var pendingUpdate bool
	for {
		select {
		case v := <-ch.LogCh:
			ch.publishLine(v)
			ch.persistLog(v)

		case v := <-ch.VerboseCh:
			// UI-only: never persisted (per-segment status would flood Supabase).
			ch.publishLine(v)

		case <-ch.UpdateCh:
			if !pendingUpdate {
				pendingUpdate = true
				updateTimer.Reset(2 * time.Second)
			}
		case <-updateTimer.C:
			pendingUpdate = false
			server.Manager.Publish(entity.EventUpdate, ch.ExportStatusInfo())
		case <-ch.done:
			updateTimer.Stop()
			return
		}
	}
}

// publishLine appends a log line to the channel's in-memory ring buffer and
// broadcasts it over SSE so the web UI shows it. Called only from Publisher.
func (ch *Channel) publishLine(v string) {
	ch.logsMu.Lock()
	ch.Logs = append(ch.Logs, v)
	if len(ch.Logs) > 100 {
		ch.Logs = ch.Logs[len(ch.Logs)-100:]
	}
	ch.logsMu.Unlock()
	server.Manager.PublishLog(ch.Config.Username, v)
}

// persistLogInfoInterval rate-limits INFO lines persisted to Supabase per
// channel. WARN/ERROR lines always pass; diagnostic INFO lines (cleanup/end
// reasons, session/reconnect events) pass when not rate-limited. This bounds
// the channel_logs table to a few rows per minute per channel even while a
// node is chatty, so bursts stay queryable without flooding the table.
const persistLogInfoInterval = 15 * time.Second

// diagnosticLogMarkers are the INFO-line substrings worth persisting. They
// capture the artifacts that make recording bursts diagnosable from the
// channel_logs table: why each recording ended, session/CF/reconnect events,
// and stop/handoff transitions.
var diagnosticLogMarkers = []string{
	"cleanup:", "ended:", "session", "reconnect", "expired", "stalled",
	"blocked", "cloudflare", "handoff", "paused", "stopped", "stalled",
	"403", "404", "max duration", "private show", "went offline",
}

// shouldPersistLog decides whether a channel log line should be persisted to
// Supabase. It returns the parsed log level and the message with the time
// prefix stripped. lastPersist/now drive the INFO rate limit (pure, so the
// filter is unit-testable; the Publisher owns the actual state).
func shouldPersistLog(line string, lastPersist time.Time, now time.Time) (level, message string, ok bool) {
	// Message format: "15:04 [LEVEL] message".
	level = "info"
	message = line
	if open := strings.Index(line, "["); open >= 0 {
		if close := strings.Index(line[open:], "]"); close > 1 {
			lvl := strings.ToLower(line[open+1 : open+close])
			switch lvl {
			case "error", "warn", "info":
				level = lvl
			}
			if rest := line[open+close+1:]; rest != "" {
				message = strings.TrimSpace(strings.TrimPrefix(rest, ": "))
				message = strings.TrimSpace(strings.TrimPrefix(message, " "))
			}
		}
	}

	// WARN/ERROR always persist — they are rare and always diagnostic.
	if level != "info" {
		return level, message, true
	}

	// INFO persists only when it carries a diagnostic marker AND is not
	// rate-limited. High-frequency INFO (e.g. per-segment status) never
	// matches a marker and is dropped entirely.
	marker := false
	lower := strings.ToLower(message)
	for _, m := range diagnosticLogMarkers {
		if strings.Contains(lower, m) {
			marker = true
			break
		}
	}
	if !marker {
		return level, message, false
	}
	if now.Sub(lastPersist) < persistLogInfoInterval {
		return level, message, false
	}
	return level, message, true
}

// persistLog forwards a channel log line to Supabase channel_logs so bursts
// (synchronized HLS 404s, CF blocks, handoffs) are diagnosable from the DB
// instead of being lost with the node's rotating in-memory log buffer. The
// write is fire-and-forget and never blocks the Publisher; failures just drop
// the line. Called only from Publisher.
func (ch *Channel) persistLog(line string) {
	if ch.Config == nil || ch.Config.Username == "" {
		return
	}
	now := time.Now()
	level, msg, ok := shouldPersistLog(line, ch.logPersistLast, now)
	if !ok {
		return
	}
	ch.logPersistLast = now

	username := ch.Config.Username
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [%s] persist log: %v", username, r)
			}
		}()
		db := server.GetDBClient()
		if db == nil {
			return // Supabase not configured (local/isolated mode) — nothing to do
		}
		logEntry := &database.ChannelLog{
			Username:   username,
			LogLevel:   level,
			Message:    msg,
			InstanceID: server.NodeID(),
		}
		if err := db.SaveLogBestEffort(logEntry); err != nil && server.Config != nil && server.Config.Debug {
			log.Printf(" WARN [%s] persist log to Supabase failed (dropped): %v", username, err)
		}
	}()
}

// WithCancel creates a new context with a cancel function,
// then stores the cancel function in the channel's CancelFunc field.
//
// This is used to cancel the context when the channel is stopped or paused.
func (ch *Channel) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	ch.cancelMu.Lock()
	ctx, ch.CancelFunc = context.WithCancel(ctx)
	cancel := ch.CancelFunc
	ch.cancelMu.Unlock()
	return ctx, cancel
}

// Info logs an informational message.
func (ch *Channel) Info(format string, a ...any) {
	msg := fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	select {
	case ch.LogCh <- msg:
	default:
		log.Printf(" WARN [%s] log queue full, dropped: %s", ch.Config.Username, msg)
	}
	log.Printf(" INFO [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// Verbose logs a message to the browser log always, and to stdout only when --debug is enabled.
// Use this for high-frequency events (e.g. per-segment updates) that would clutter the console.
// Verbose lines are routed through VerboseCh (UI-only) so they are never
// persisted to Supabase channel_logs.
func (ch *Channel) Verbose(format string, a ...any) {
	msg := fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	select {
	case ch.VerboseCh <- msg:
	default:
	}
	if server.Config != nil && server.Config.Debug {
		log.Printf(" INFO [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
	}
}

// Warn logs a warning message.
func (ch *Channel) Warn(format string, a ...any) {
	msg := fmt.Sprintf("%s [WARN] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	select {
	case ch.LogCh <- msg:
	default:
		log.Printf(" WARN [%s] log queue full, dropped: %s", ch.Config.Username, msg)
	}
	log.Printf(" WARN [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// Error logs an error message.
func (ch *Channel) Error(format string, a ...any) {
	msg := fmt.Sprintf("%s [ERROR] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
	select {
	case ch.LogCh <- msg:
	default:
		log.Printf(" WARN [%s] log queue full, dropped: %s", ch.Config.Username, msg)
	}
	log.Printf("ERROR [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// SetUploadProgress updates live upload status visible in the UI.
// Safe for concurrent calls from pipeline worker goroutines.
func (ch *Channel) SetUploadProgress(filename, status string, progress float64, hostCount, hostTotal int, bytesCur, bytesTotal int64, speed string, hosts []entity.HostEntry) {
	ch.uploadStatusMu.Lock()
	ch.UploadFilename = filename
	ch.UploadStatus = status
	ch.UploadProgress = progress
	ch.UploadHostCount = hostCount
	ch.UploadHostTotal = hostTotal
	ch.UploadBytesCur = bytesCur
	ch.UploadBytesTotal = bytesTotal
	ch.UploadSpeed = speed
	ch.UploadHosts = hosts
	ch.uploadStatusMu.Unlock()
	// Trigger a UI update so progress is reflected immediately.
	select {
	case ch.UpdateCh <- true:
	default:
	}
	// Broadcast aggregated upload state for the session timer UI.
	if server.Manager != nil {
		server.Manager.PublishUploadState()
	}
}

// UploadEntry returns the current upload progress entry for this channel.
func (ch *Channel) UploadEntry() entity.UploadEntry {
	ch.uploadStatusMu.Lock()
	defer ch.uploadStatusMu.Unlock()
	hosts := ch.UploadHosts
	if hosts == nil {
		hosts = []entity.HostEntry{}
	}
	return entity.UploadEntry{
		Channel:      ch.Config.Username,
		Filename:     ch.UploadFilename,
		Status:       ch.UploadStatus,
		Progress:     ch.UploadProgress,
		HostCount:    ch.UploadHostCount,
		HostTotal:    ch.UploadHostTotal,
		BytesCurrent: ch.UploadBytesCur,
		BytesTotal:   ch.UploadBytesTotal,
		Speed:        ch.UploadSpeed,
		Hosts:        hosts,
	}
}

// ExportInfo exports the channel information as a ChannelInfo struct.
func (ch *Channel) ExportInfo() *entity.ChannelInfo {
	return ch.exportInfo(true)
}

// ExportStatusInfo exports the channel state without copying logs. SSE status
// swaps do not render historical logs, so this keeps hot updates cheap.
func (ch *Channel) ExportStatusInfo() *entity.ChannelInfo {
	return ch.exportInfo(false)
}

func (ch *Channel) exportInfo(includeLogs bool) *entity.ChannelInfo {
	ch.fileMu.RLock()
	duration := ch.Duration
	filesize := ch.Filesize
	totalDiskUsageBytes := ch.TotalDiskUsageBytes
	currentFilename := ch.CurrentFilename
	fileExt := ch.FileExt
	var fileName string
	if ch.File != nil {
		fileName = ch.File.Name()
	}
	ch.fileMu.RUnlock()

	ch.stateMu.Lock()
	var streamedAt string
	if ch.StreamedAt != 0 {
		streamedAt = time.Unix(ch.StreamedAt, 0).Format("2006-01-02 15:04 AM")
	}
	isOnline := ch.IsOnline
	isConnecting := ch.IsConnecting
	roomStatus := ch.RoomStatus
	liveThumbURL := ch.LiveThumbURL
	roomTitle := ch.RoomTitle
	gender := ch.Gender
	viewers := ch.Viewers
	summaryCardImage := ch.SummaryCardImage
	ch.stateMu.Unlock()

	var filename string
	switch {
	case fileName != "":
		filename = fileName
	case currentFilename != "":
		filename = currentFilename + fileExt
	}

	var logsCopy []string
	if includeLogs {
		ch.logsMu.Lock()
		logsCopy = make([]string, len(ch.Logs))
		copy(logsCopy, ch.Logs)
		ch.logsMu.Unlock()
	}

	ch.uploadStatusMu.Lock()
	uploadStatus := ch.UploadStatus
	uploadProgress := ch.UploadProgress
	uploadFilename := ch.UploadFilename
	ch.uploadStatusMu.Unlock()

	siteName := ch.Config.Site
	if siteName == "" {
		siteName = "chaturbate"
	}
	siteDomain := server.Config.Domain
	if siteName == "stripchat" {
		siteDomain = "https://stripchat.com/"
	}

	return &entity.ChannelInfo{
		IsOnline:             isOnline,
		IsConnecting:         isConnecting,
		IsPaused:             ch.Config.IsPaused.Load(),
		PauseReason:          string(ch.PauseReason()),
		IsCompressing:        atomic.LoadInt32(&ch.CompressingCount) > 0,
		AutoResumedFromPause: ch.autoResumedFromPause.Load(),
		RoomStatus:           roomStatus,
		Username:             ch.Config.Username,
		Site:                 siteName,
		SiteDomain:           siteDomain,
		LiveThumbURL:         liveThumbURL,
		MaxDuration:          internal.FormatDuration(float64(ch.Config.MaxDuration * 60)),
		MaxFilesize:          internal.FormatFilesize(ch.Config.MaxFilesize * 1024 * 1024),
		StreamedAt:           streamedAt,
		CreatedAt:            ch.Config.CreatedAt,
		Duration:             internal.FormatDuration(duration),
		Filesize:             internal.FormatFilesize(filesize),
		TotalDiskUsage:       internal.FormatFilesize(int(totalDiskUsageBytes)),
		Filename:             filename,
		Logs:                 logsCopy,
		GlobalConfig:         server.Config,
		UploadStatus:         uploadStatus,
		UploadProgress:       uploadProgress,
		UploadFilename:       uploadFilename,
		RoomTitle:            roomTitle,
		Gender:               gender,
		NumViewers:           viewers,
		SummaryCardImage:     summaryCardImage,
	}
}

// MarkAutoResumedFromPause records that this channel was automatically resumed
// from a stuck paused-but-still-assigned state. The flag is sticky for the
// channel's lifetime (until it is re-created after a release/re-claim) and
// shows as an "Auto-Resumed" badge in the web UI.
func (ch *Channel) MarkAutoResumedFromPause() {
	ch.autoResumedFromPause.Store(true)
}

// PauseReason returns why the channel is paused (empty when not paused, or
// when the reason is unknown/legacy — e.g. a channel paused before pause
// reasons were tracked, which automatic resume treats as recoverable).
func (ch *Channel) PauseReason() entity.PauseReason {
	ch.pauseReasonMu.Lock()
	defer ch.pauseReasonMu.Unlock()
	return entity.PauseReason(ch.pauseReason)
}

// setPauseReason records why the channel is being paused. A manual (user)
// pause is sticky: automatic re-pauses (session boundary, handoff) never
// overwrite it, so the user's intent survives session cycles and automatic
// resume paths can detect it.
func (ch *Channel) setPauseReason(reason entity.PauseReason) {
	ch.pauseReasonMu.Lock()
	defer ch.pauseReasonMu.Unlock()
	if ch.pauseReason == string(entity.PauseReasonManual) && reason != entity.PauseReasonManual {
		return // user pause is sticky — never downgrade it to an automatic pause
	}
	ch.pauseReason = string(reason)
}

// clearPauseReason resets the recorded pause reason once the channel is no
// longer paused.
func (ch *Channel) clearPauseReason() {
	ch.pauseReasonMu.Lock()
	ch.pauseReason = ""
	ch.pauseReasonMu.Unlock()
}

// Pause pauses the channel and cancels the context, recording the pause as a
// manual (user-initiated) pause.
func (ch *Channel) Pause() {
	ch.PauseWithReason(entity.PauseReasonManual)
}

// PauseWithReason pauses the channel and records WHY it was paused. Automatic
// callers (session boundary, handoff) pass their reason so resume paths can
// auto-recover automatic pauses while leaving manual user pauses alone.
func (ch *Channel) PauseWithReason(reason entity.PauseReason) {
	ch.setPauseReason(reason)

	// Stop the monitoring loop. `context.Canceled` → `ch.Monitor()` →
	// `onRetry` → `ch.UpdateOnlineStatus(false)`.
	ch.monitorMu.Lock()
	ch.monitorRestartRequested = false
	ch.monitorMu.Unlock()

	// Record WHY before canceling: the monitor's deferred cleanup closes the
	// file the moment the context is canceled, and cleanupLocked logs this
	// reason. Log the EFFECTIVE reason — for a manually-paused channel
	// re-paused by an automatic boundary, the sticky rule keeps "manual" and
	// that is what the log should say.
	ch.setCloseReason(fmt.Sprintf("paused (%s)", ch.PauseReason()))
	ch.Config.IsPaused.Store(true)
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.cancelMu.Unlock()
	ch.Update()
	ch.Info("channel paused (%s)", ch.PauseReason())

	// Finalize any in-progress files immediately so they can be uploaded
	// and removed when `DeleteLocalAfterUpload` is enabled.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch.Error("pause cleanup panic recovered: %v", r)
			}
		}()
		if err := ch.Cleanup(CloseProcess); err != nil {
			ch.Error("cleanup on pause: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch.cancelMu.Lock()
	ch.PauseCancelFunc = cancel
	ch.cancelMu.Unlock()
	go ch.CheckOnlineWhilePaused(ctx, 0)
}

// Cancel safely calls the channel's CancelFunc under the cancelMu lock.
func (ch *Channel) Cancel() {
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.cancelMu.Unlock()
}

// setCloseReason records why the current recording is ending so cleanupLocked
// can log it when it closes the file. Callers should set it BEFORE invoking
// Cleanup/cleanupLocked (or before canceling the context that triggers them).
func (ch *Channel) setCloseReason(reason string) {
	ch.fileMu.Lock()
	ch.closeReason = reason
	ch.fileMu.Unlock()
}

// Stop stops the channel and cancels the context. The pause reason is recorded
// as "handoff" (the channel is being torn down for reassignment or deletion).
func (ch *Channel) Stop() {
	ch.monitorMu.Lock()
	ch.monitorRestartRequested = false
	ch.Config.IsPaused.Store(true)
	ch.setPauseReason(entity.PauseReasonHandoff)
	ch.monitorMu.Unlock()

	ch.setCloseReason("channel stopped (handoff)")
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.PauseCancelFunc()
	ch.cancelMu.Unlock()

	ch.waitForMonitorStop()
	ch.ProcessPending()
	ch.Info("channel stopped")
	ch.Close()
}

// ReapStuckRecording abandons the current monitor run and starts a fresh one.
//
// The stuck-recording watchdog calls this when a channel has held a ZERO-BYTE
// recording file well past the grace window.  Such a run is wedged, not slow:
// the HLS poll never returns, so the 30-second stall detector never fires, the
// retry loop never rotates the file, and the empty file stays open forever
// (cleanupLocked only deletes a zero-byte file when it closes).  Cancelling the
// run is what unwedges it — the monitor's deferred Cleanup then CLOSES and
// DELETES the empty file.
//
// The restart is requested BEFORE the cancel: finishMonitor sees the flag and
// spawns a new monitor (clearing the pause state), so the channel re-resolves
// the stream instead of stopping.  Returns false when no monitor is running, so
// the watchdog skips a channel that is already torn down rather than
// resurrecting it.
func (ch *Channel) ReapStuckRecording(reason string) bool {
	ch.monitorMu.Lock()
	if !ch.monitorRunning {
		ch.monitorMu.Unlock()
		return false
	}
	ch.monitorRestartRequested = true
	ch.monitorMu.Unlock()

	// Set the reason before cancelling: Monitor skips its own classification for
	// context.Canceled, so this is what cleanupLocked logs and persists.
	ch.setCloseReason(reason)

	ch.cancelMu.Lock()
	if ch.CancelFunc != nil {
		ch.CancelFunc()
	}
	ch.cancelMu.Unlock()
	return true
}

// Close stops non-recording background goroutines after recording/upload work
// has been processed.
func (ch *Channel) Close() {
	ch.PipelineQueue.Stop()
	ch.closeDone.Do(func() { close(ch.done) })
}

// Resume resumes channel monitoring immediately. API pacing is handled by the
// shared adaptive limiter, not by delaying whole channels.
func (ch *Channel) Resume(_ int) {
	select {
	case <-ch.done:
		return // Channel already stopped, do not resume
	default:
	}

	ch.cancelMu.Lock()
	ch.PauseCancelFunc()
	ch.cancelMu.Unlock()
	ch.Config.IsPaused.Store(false)
	ch.clearPauseReason()

	ch.Update()
	ch.Info("channel resumed")

	ch.monitorWg.Add(1)
	go func() {
		defer ch.monitorWg.Done()
		defer func() {
			if r := recover(); r != nil {
				ch.Error("monitor panic recovered: %v", r)
			}
		}()
		// Check again right before starting monitor
		select {
		case <-ch.done:
			return
		default:
		}
		runID, ok := ch.requestMonitorStart()
		if !ok {
			return
		}
		ch.Monitor(runID)
	}()
}

// WaitMonitor blocks until the Monitor goroutine has fully exited.
// By the time it returns, Cleanup() has already run and any pending
// files have been queued.
func (ch *Channel) WaitMonitor() {
	ch.monitorWg.Wait()
}

// ProcessPending waits for any in-flight async finalization from Cleanup(CloseProcess)
// to finish, then processes queued pending files and waits for all uploads.
// Blocks until all uploads (including those from previous file rotations)
// complete.  Call after WaitMonitor when Cleanup was called with CloseQueue.
func (ch *Channel) ProcessPending() {
	// Wait for the async finalize goroutines so no pending files are missed.
	ch.pendingWg.Wait()

	ch.cleanupMu.Lock()
	if len(ch.pendingFiles) > 0 {
		ch.processPendingQueue()
	}
	ch.cleanupMu.Unlock()

	// Release the session-merge hold, if any, BEFORE awaiting uploads: a held
	// file whose session produced no further cycle (offline after token
	// expiry, empty/deleted final file) would otherwise stay parked forever —
	// in-flight-marked, never enqueued, no recordings row — until the runner's
	// disk was wiped.  After pendingFiles have been processed there is no
	// upcoming cycle that could merge into the hold on this process, so the
	// only safe release is an individual upload.  Every stop path (handoff
	// Stop, session-boundary drain) funnels through here.
	ch.FlushHeldSessionMerge()

	ch.UploadWg.Wait()
}

// UpdateOnlineStatus updates the online status of the channel.
func (ch *Channel) UpdateOnlineStatus(isOnline bool) {
	ch.stateMu.Lock()
	ch.IsOnline = isOnline
	ch.IsConnecting = false
	if !isOnline {
		ch.Viewers = 0
	}
	ch.stateMu.Unlock()
	ch.Update()
}

// SetConnecting sets the connecting/reconnecting state without changing IsOnline.
// Used during retry to show "Reconnecting..." in the UI while the channel is
// temporarily re-fetching a fresh CDN session token.
func (ch *Channel) SetConnecting(connecting bool) {
	ch.stateMu.Lock()
	ch.IsConnecting = connecting
	ch.stateMu.Unlock()
	ch.Update()
}

// resolveSite returns the appropriate site.Site implementation for the channel.
func resolveSite(ch *Channel) site.Site {
	switch ch.Config.Site {
	case "stripchat":
		return site.NewStripchatSite()
	default:
		return site.NewChaturbateSite()
	}
}

// CheckOnlineWhilePaused periodically refreshes room status for paused channels
// so the UI can still distinguish online/private/offline states.
func (ch *Channel) CheckOnlineWhilePaused(ctx context.Context, startSeq int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC [%s] check-online: %v", ch.Config.Username, r)
		}
	}()
	siteImpl := resolveSite(ch)
	req := internal.NewReq()
	baseIntervalMinutes := max(server.Config.Interval, 15)

	initialDelay := time.Duration(startSeq*5) * time.Second
	if initialDelay > 0 {
		timer := time.NewTimer(initialDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}

	for {
		waitInterval := time.Duration(baseIntervalMinutes) * time.Minute

		status, err := siteImpl.GetRoomStatus(ctx, req, ch.Config.Username)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
		} else if status != "" {
			isOnline := status != site.StatusAway && status != site.StatusOffline
			ch.stateMu.Lock()
			changed := ch.IsOnline != isOnline || ch.RoomStatus != status || ch.IsConnecting
			if changed {
				ch.IsOnline = isOnline
				ch.IsConnecting = false
				ch.RoomStatus = status
			}
			ch.stateMu.Unlock()
			if changed {
				ch.Info("channel status: %s (paused)", status)
				ch.Update()
			}
		}

		timer := time.NewTimer(waitInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// requestMonitorStart starts a monitor immediately when possible, or records
// a pending restart if a previous monitor is still shutting down.
func (ch *Channel) requestMonitorStart() (uint64, bool) {
	ch.monitorMu.Lock()
	defer ch.monitorMu.Unlock()

	if ch.monitorRunning {
		if ch.Config.IsPaused.Load() {
			ch.monitorRestartRequested = true
		}
		return 0, false
	}

	return ch.startMonitorLocked(), true
}

// finishMonitor clears the running flag when a monitor loop exits.
func (ch *Channel) finishMonitor() {
	ch.monitorMu.Lock()
	done := ch.monitorDone
	shouldRestart := ch.monitorRestartRequested
	ch.monitorRunning = false
	ch.monitorRestartRequested = false
	var runID uint64
	if shouldRestart {
		runID = ch.startMonitorLocked()
	} else {
		ch.monitorDone = nil
	}
	ch.monitorMu.Unlock()

	if done != nil {
		close(done)
	}

	if shouldRestart {
		ch.Update()
		ch.Info("channel resumed")
		ch.monitorWg.Add(1)
		go func() {
			defer ch.monitorWg.Done()
			defer func() {
				if r := recover(); r != nil {
					ch.Error("monitor panic recovered: %v", r)
				}
			}()
			ch.Monitor(runID)
		}()
	}
}

// startMonitorLocked marks a monitor as active and allocates a new run ID.
// monitorMu must already be held by the caller.
func (ch *Channel) startMonitorLocked() uint64 {
	ch.Config.IsPaused.Store(false)
	ch.clearPauseReason()
	ch.monitorRunning = true
	ch.monitorRestartRequested = false
	ch.monitorDone = make(chan struct{})
	ch.monitorRunID++
	return ch.monitorRunID
}

// waitForMonitorStop blocks until the current monitor run has finished cleanup.
func (ch *Channel) waitForMonitorStop() {
	ch.monitorMu.Lock()
	done := ch.monitorDone
	ch.monitorMu.Unlock()

	if done != nil {
		<-done
	}
}

// Update sends an update signal to the channel's update channel.
func (ch *Channel) Update() {
	select {
	case ch.UpdateCh <- true:
	default:
	}
}
