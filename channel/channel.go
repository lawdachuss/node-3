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

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
	"github.com/teacat/chaturbate-dvr/stripchat"
)

// pendingFile tracks a closed recording file awaiting post-processing
// (mux, move to output dir, thumbnail, upload, DB save, deletion).
type pendingFile struct {
	videoPath        string
	audioPath        string // empty if no separate audio
	hasSeparateAudio bool   // captured at queue-time so file-level A/V pairing survives stream config changes
	skipMinDuration  bool   // when true, bypass the minimum-duration threshold (used on pause)
}

// Channel represents a channel instance.
type Channel struct {
	CancelFunc      context.CancelFunc
	PauseCancelFunc context.CancelFunc
	cancelMu        sync.Mutex // guards CancelFunc and PauseCancelFunc writes
	LogCh           chan string
	UpdateCh        chan bool
	done            chan struct{} // closed when channel is torn down
	closeDone       sync.Once     // ensures done is closed exactly once

	IsOnline     bool
	IsConnecting bool   // true during retry/reconnect, shown as "Reconnecting..." in UI
	RoomStatus   string // public, private, group, away, offline
	StreamedAt   int64
	Duration     float64 // Seconds
	Filesize     int     // Bytes
	Sequence     atomic.Int64

	CompressingCount int32 // atomic: number of active compression goroutines

	stateMu sync.Mutex // protects IsOnline, IsConnecting, RoomStatus, Duration, Filesize

	// LastError holds the most recent API/recording error message for diagnostic display.
	// Set by the Monitor retry loop whenever an attempt fails.
	LastError string

	RoomTitle    string   // captured from API at recording start
	Tags         []string // captured from API at recording start
	Viewers      int      // captured from API at recording start
	Gender       string   // broadcaster_gender from Chaturbate API ("m", "f", "c", "t", …)
	Resolution   string   // actual stream resolution (e.g. "1920x1080")
	Framerate    int      // actual stream framerate (e.g. 30)
	LiveThumbURL string   // live thumbnail URL for the current stream

	Logs   []string
	logsMu sync.Mutex

	File              *os.File
	AudioFile         *os.File
	Config            *entity.ChannelConfig
	CurrentFilename   string
	InitSegment       []byte // fMP4 video init segment for LL-HLS streams
	AudioInitSegment  []byte // fMP4 audio init segment for LL-HLS streams
	HasSeparateAudio  bool
	switchRequested   bool       // set by HandleSegment, consumed by OnPollComplete
	videoSegmentCount int        // tracks video segments written to current file
	audioSegmentCount int        // tracks audio segments written to current file
	cleanupMu         sync.Mutex // serialises Cleanup() calls from concurrent goroutines
	skipMinDurOnExit  bool       // set when context is cancelled; skip min-duration on cleanup
	pendingFiles      []pendingFile
	pendingWg         sync.WaitGroup // tracks async pending-file processing goroutine
	UploadWg          sync.WaitGroup // tracks in-flight upload goroutines for graceful shutdown
	monitorWg         sync.WaitGroup // tracks the Monitor goroutine lifetime
	pauseWg           sync.WaitGroup // tracks the CheckOnlineWhilePaused goroutine
	PipelineQueue     *PipelineQueue // ordered pipeline for thumbnails → upload → metadata → cleanup

	// Upload progress tracking — updated by the pipeline worker goroutines.
	// Thread-safe via uploadStatusMu; visible in the UI via ExportInfo().
	// Multiple in-flight uploads per channel are tracked (keyed by filename)
	// so pipelineWorkerCount concurrent pipelines each report independently
	// instead of overwriting a single shared snapshot.
	uploadStatusMu sync.Mutex
	activeUploads  map[string]*uploadSnapshot // keyed by filename
}

// uploadSnapshot is one in-flight upload for a channel.
type uploadSnapshot struct {
	Channel      string
	Filename     string
	Status       string
	Progress     float64
	HostCount    int
	HostTotal    int
	BytesCurrent int64
	BytesTotal   int64
	Speed        string
	Hosts        []entity.HostEntry
}

// New creates a new channel instance with the given manager and configuration.
func New(conf *entity.ChannelConfig) *Channel {
	ch := &Channel{
		LogCh:           make(chan string, 256),
		UpdateCh:        make(chan bool, 64),
		done:            make(chan struct{}),
		Config:          conf,
		CancelFunc:      func() {},
		PauseCancelFunc: func() {},
		RoomStatus:      "offline",
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
			ch.logsMu.Lock()
			ch.Logs = append(ch.Logs, v)
			if len(ch.Logs) > 100 {
				ch.Logs = ch.Logs[len(ch.Logs)-100:]
			}
			ch.logsMu.Unlock()
			if server.Manager != nil {
				server.Manager.PublishLog(ch.Config.Username, v)
			}

		case <-ch.UpdateCh:
			if !pendingUpdate {
				pendingUpdate = true
				updateTimer.Reset(2 * time.Second)
			}
		case <-updateTimer.C:
			pendingUpdate = false
			if server.Manager != nil {
				server.Manager.Publish(entity.EventUpdate, ch.ExportStatusInfo())
			}
		case <-ch.done:
			updateTimer.Stop()
			return
		}
	}
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

// SetLastError records the most recent recording error for diagnostic display.
// Safe for concurrent calls from the Monitor retry loop.
func (ch *Channel) SetLastError(err error) {
	ch.stateMu.Lock()
	if err != nil {
		ch.LastError = err.Error()
	} else {
		ch.LastError = ""
	}
	ch.stateMu.Unlock()
	ch.Update()
}

// SetUploadProgress updates live upload status for a single in-flight file,
// visible in the UI. Safe for concurrent calls from pipeline worker
// goroutines — each call is keyed by filename so multiple concurrent uploads
// on the same channel are tracked independently.
func (ch *Channel) SetUploadProgress(filename, status string, progress float64, hostCount, hostTotal int, bytesCur, bytesTotal int64, speed string, hosts []entity.HostEntry) {
	ch.uploadStatusMu.Lock()
	if ch.activeUploads == nil {
		ch.activeUploads = make(map[string]*uploadSnapshot)
	}
	if filename == "" {
		// Legacy global clear (no specific file) — wipe everything.
		ch.activeUploads = make(map[string]*uploadSnapshot)
	} else {
		ch.activeUploads[filename] = &uploadSnapshot{
			Channel:      ch.Config.Username,
			Filename:     filename,
			Status:       status,
			Progress:     progress,
			HostCount:    hostCount,
			HostTotal:    hostTotal,
			BytesCurrent: bytesCur,
			BytesTotal:   bytesTotal,
			Speed:        speed,
			Hosts:        hosts,
		}
	}
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

// ClearUploadProgress removes a single file's live upload status. Called by
// the pipeline's deferred cleanup so finishing one of several concurrent
// uploads does not wipe the others on the same channel.
func (ch *Channel) ClearUploadProgress(filename string) {
	if filename == "" {
		return
	}
	ch.uploadStatusMu.Lock()
	if ch.activeUploads != nil {
		delete(ch.activeUploads, filename)
	}
	ch.uploadStatusMu.Unlock()
	select {
	case ch.UpdateCh <- true:
	default:
	}
	if server.Manager != nil {
		server.Manager.PublishUploadState()
	}
}

// primaryUpload returns the most relevant in-flight upload for the channel
// card (prefers an actively-uploading file, else the most-progressed). Used by
// ExportInfo, which shows a single upload summary per channel.
func (ch *Channel) primaryUpload() *uploadSnapshot {
	ch.uploadStatusMu.Lock()
	defer ch.uploadStatusMu.Unlock()
	var best *uploadSnapshot
	for _, s := range ch.activeUploads {
		if best == nil {
			best = s
			continue
		}
		if strings.Contains(s.Status, "uploading") && !strings.Contains(best.Status, "uploading") {
			best = s
		} else if s.Progress > best.Progress {
			best = s
		}
	}
	return best
}

// UploadEntry returns one UploadEntry per in-flight upload for this channel
// (so concurrent pipelineWorkerCount uploads are all visible in the UI).
func (ch *Channel) UploadEntry() []entity.UploadEntry {
	ch.uploadStatusMu.Lock()
	defer ch.uploadStatusMu.Unlock()
	if len(ch.activeUploads) == 0 {
		return nil
	}
	out := make([]entity.UploadEntry, 0, len(ch.activeUploads))
	for _, s := range ch.activeUploads {
		hosts := s.Hosts
		if hosts == nil {
			hosts = []entity.HostEntry{}
		}
		out = append(out, entity.UploadEntry{
			Channel:      s.Channel,
			Filename:     s.Filename,
			Status:       s.Status,
			Progress:     s.Progress,
			HostCount:    s.HostCount,
			HostTotal:    s.HostTotal,
			BytesCurrent: s.BytesCurrent,
			BytesTotal:   s.BytesTotal,
			Speed:        s.Speed,
			Hosts:        hosts,
		})
	}
	return out
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
	ch.stateMu.Lock()
	var streamedAt string
	if ch.StreamedAt != 0 {
		streamedAt = time.Unix(ch.StreamedAt, 0).Format("2006-01-02 15:04 AM")
	}
	isOnline := ch.IsOnline
	isConnecting := ch.IsConnecting
	roomStatus := ch.RoomStatus
	duration := ch.Duration
	filesize := ch.Filesize
	currentFilename := ch.CurrentFilename
	hasSeparateAudio := ch.HasSeparateAudio
	hasFile := ch.File != nil
	liveThumbURL := ch.LiveThumbURL
	lastError := ch.LastError
	var fileName string
	if hasFile {
		fileName = ch.File.Name()
	}
	ch.stateMu.Unlock()

	var filename string
	if currentFilename != "" && hasSeparateAudio {
		filename = currentFilename + ".mp4"
	} else if hasFile {
		filename = fileName
	}

	var logsCopy []string
	if includeLogs {
		ch.logsMu.Lock()
		logsCopy = make([]string, len(ch.Logs))
		copy(logsCopy, ch.Logs)
		ch.logsMu.Unlock()
	}

	primary := ch.primaryUpload()
	var uploadStatus, uploadFilename string
	var uploadProgress float64
	if primary != nil {
		uploadStatus = primary.Status
		uploadProgress = primary.Progress
		uploadFilename = primary.Filename
	}

	siteName := ch.Config.Site
	if siteName == "" {
		siteName = "chaturbate"
	}
	siteDomain := server.Config.Domain
	if siteName == "stripchat" {
		siteDomain = "https://stripchat.com/"
	}

	return &entity.ChannelInfo{
		IsOnline:       isOnline,
		IsConnecting:   isConnecting,
		IsPaused:       ch.Config.IsPaused.Load(),
		IsCompressing:  atomic.LoadInt32(&ch.CompressingCount) > 0,
		RoomStatus:     roomStatus,
		Username:       ch.Config.Username,
		Site:           siteName,
		SiteDomain:     siteDomain,
		LiveThumbURL:   liveThumbURL,
		MaxDuration:    internal.FormatDuration(float64(ch.Config.MaxDuration * 60)),
		MaxFilesize:    internal.FormatFilesize(ch.Config.MaxFilesize * 1024 * 1024),
		StreamedAt:     streamedAt,
		CreatedAt:      ch.Config.CreatedAt,
		Duration:       internal.FormatDuration(duration),
		Filesize:       internal.FormatFilesize(filesize),
		Filename:       filename,
		Logs:           logsCopy,
		GlobalConfig:   server.Config,
		UploadStatus:   uploadStatus,
		UploadProgress: uploadProgress,
		UploadFilename: uploadFilename,
		LastError:      lastError,
	}
}

// Pause pauses the channel and cancels the context.
func (ch *Channel) Pause() {
	// Stop the monitoring loop and hand over to CheckOnlineWhilePaused
	// which will poll the API to keep RoomStatus and IsOnline up to date.
	ch.Config.IsPaused.Store(true)
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.cancelMu.Unlock()
	ch.Update()
	ch.Info("channel paused")

	// Finalize any in-progress files immediately so they can be uploaded
	// and removed when `DeleteLocalAfterUpload` is enabled.
	go func() {
		if err := ch.Cleanup(CloseProcess); err != nil {
			ch.Error("cleanup on pause: %s", err.Error())
		}
	}()

	// Cancel any previous pause context to prevent goroutine leaks on double Pause.
	ch.cancelMu.Lock()
	ch.PauseCancelFunc()
	ctx, cancel := context.WithCancel(context.Background())
	ch.PauseCancelFunc = cancel
	ch.cancelMu.Unlock()
	ch.pauseWg.Add(1)
	go func() {
		defer ch.pauseWg.Done()
		ch.CheckOnlineWhilePaused(ctx, 0)
	}()
}

// Cancel safely calls the channel's CancelFunc under the cancelMu lock.
func (ch *Channel) Cancel() {
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.cancelMu.Unlock()
}

// Stop stops the channel and cancels the context.
func (ch *Channel) Stop() {
	ch.cancelMu.Lock()
	ch.CancelFunc()
	ch.PauseCancelFunc()
	ch.cancelMu.Unlock()
	ch.WaitMonitor()
	ch.pauseWg.Wait()
	ch.ProcessPending()
	ch.Info("channel stopped")
	ch.Close()
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

	// Wait for the previous Monitor goroutine to fully exit before starting
	// a new one. Pause() already cancelled the Monitor's context via
	// CancelFunc(), so the old Monitor is on its way out.  Waiting avoids
	// a TOCTOU race where Resume() runs before the Monitor goroutine's defer
	// has a chance to finish cleanup, leaving the channel without any Monitor.
	// Bounded: if the old Monitor is hung (e.g. on a stalled proxy/CDN request
	// that ignores context cancellation), never block the HTTP handler forever
	// — start the new Monitor anyway and let the stale one exit on its own.
	waitDone := make(chan struct{})
	go func() {
		ch.monitorWg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(20 * time.Second):
		ch.Warn("resume: previous monitor did not exit within 20s (likely hung on a stalled proxy/CDN request); starting new monitor anyway")
	}

	ch.Config.IsPaused.Store(false)
	ch.Update()
	ch.Info("channel resumed")

	// Create the cancellable context and store CancelFunc BEFORE starting
	// the goroutine so Pause() always has a valid target to cancel.
	ctx, cancel := context.WithCancel(context.Background())
	ch.cancelMu.Lock()
	ch.CancelFunc = cancel
	ch.cancelMu.Unlock()

	ch.monitorWg.Add(1)
	go func() {
		defer ch.monitorWg.Done()
		select {
		case <-ch.done:
			cancel()
			return
		default:
		}
		ch.Monitor(ctx)
	}()
}

// WaitMonitor blocks until the Monitor goroutine has fully exited.
// By the time it returns, Cleanup() has already run and any pending
// files have been queued into UploadWg. It is bounded so a Monitor hung on a
// stalled proxy/CDN request that ignores context cancellation cannot block
// Stop() forever (mirrors the bounded wait already used by Resume).
func (ch *Channel) WaitMonitor() {
	done := make(chan struct{})
	go func() {
		ch.monitorWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		ch.Warn("WaitMonitor: monitor did not exit within 60s (likely hung on a stalled request); proceeding with stop")
	}
}

// ProcessPending waits for any in-flight async processing from Cleanup(CloseProcess)
// to finish, then muxes/compresses/uploads any queued pending files.
// Blocks until all uploads (including those from previous file rotations)
// complete.  Call after WaitMonitor when Cleanup was called with CloseQueue.
func (ch *Channel) ProcessPending() {
	// Wait for the async Cleanup goroutine to finish processing files
	// that were dispatched in CloseProcess mode.  This ensures all
	// CompressFile / MoveToOutputDir calls have already done UploadWg.Add(1)
	// before we check UploadWg below (no missed Add/Wait race).
	ch.pendingWg.Wait()

	ch.cleanupMu.Lock()
	if len(ch.pendingFiles) > 0 {
		ch.processPendingQueue()
	}
	ch.cleanupMu.Unlock()
	ch.UploadWg.Wait()
}

// UpdateOnlineStatus updates the online status of the channel.
func (ch *Channel) UpdateOnlineStatus(isOnline bool) {
	ch.stateMu.Lock()
	ch.IsOnline = isOnline
	ch.IsConnecting = false
	if isOnline {
		ch.LastError = ""
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
		return stripchat.NewStripchatSite()
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
