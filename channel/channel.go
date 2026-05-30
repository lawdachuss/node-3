package channel

import (
        "context"
        "errors"
        "fmt"
        "log"
        "os"
        "sync"
        "sync/atomic"
        "time"

        "github.com/teacat/chaturbate-dvr/chaturbate"
        "github.com/teacat/chaturbate-dvr/entity"
        "github.com/teacat/chaturbate-dvr/internal"
        "github.com/teacat/chaturbate-dvr/server"
)

// pendingFile tracks a closed recording file awaiting post-processing
// (mux, move to output dir, thumbnail, upload, DB save, deletion).
type pendingFile struct {
        videoPath string
        audioPath string // empty if no separate audio
}

// Channel represents a channel instance.
type Channel struct {
        CancelFunc      context.CancelFunc
        PauseCancelFunc context.CancelFunc
        LogCh           chan string
        UpdateCh        chan bool
        done            chan struct{} // closed when channel is torn down
        closeDone       sync.Once    // ensures done is closed exactly once

        IsOnline   bool
        RoomStatus string // public, private, group, away, offline
        StreamedAt int64
        Duration   float64 // Seconds
        Filesize   int     // Bytes
        Sequence   int

        CompressingCount int32 // atomic: number of active compression goroutines

        stateMu sync.Mutex // protects IsOnline, RoomStatus, Duration, Filesize

        RoomTitle  string   // captured from API at recording start
        Tags       []string // captured from API at recording start
        Viewers    int      // captured from API at recording start
        Gender     string   // broadcaster_gender from Chaturbate API ("m", "f", "c", "t", …)
        Resolution string   // actual stream resolution (e.g. "1920x1080")
        Framerate  int      // actual stream framerate (e.g. 30)

        Logs    []string
        logsMu  sync.Mutex

        File             *os.File
        AudioFile        *os.File
        Config           *entity.ChannelConfig
        CurrentFilename  string
        InitSegment      []byte // fMP4 video init segment for LL-HLS streams
        AudioInitSegment []byte // fMP4 audio init segment for LL-HLS streams
        HasSeparateAudio bool
        switchRequested  bool       // set by HandleSegment, consumed by OnPollComplete
        cleanupMu        sync.Mutex // serialises Cleanup() calls from concurrent goroutines
        pendingFiles     []pendingFile
        UploadWg         sync.WaitGroup // tracks in-flight upload goroutines for graceful shutdown
        monitorWg        sync.WaitGroup // tracks the Monitor goroutine lifetime
}

// New creates a new channel instance with the given manager and configuration.
func New(conf *entity.ChannelConfig) *Channel {
        ch := &Channel{
                LogCh:           make(chan string),
                UpdateCh:        make(chan bool),
                done:            make(chan struct{}),
                Config:          conf,
                CancelFunc:      func() {},
                PauseCancelFunc: func() {},
        }
        go ch.Publisher()

        return ch
}

// Publisher listens for log messages and updates from the channel
// and publishes once received.
func (ch *Channel) Publisher() {
        for {
                select {
                case v := <-ch.LogCh:
                        ch.logsMu.Lock()
                        ch.Logs = append(ch.Logs, v)
                        if len(ch.Logs) > 100 {
                                ch.Logs = ch.Logs[len(ch.Logs)-100:]
                        }
                        ch.logsMu.Unlock()
                        server.Manager.Publish(entity.EventLog, ch.ExportInfo())

                case <-ch.UpdateCh:
                        server.Manager.Publish(entity.EventUpdate, ch.ExportInfo())
                case <-ch.done:
                        return
                }
        }
}

// WithCancel creates a new context with a cancel function,
// then stores the cancel function in the channel's CancelFunc field.
//
// This is used to cancel the context when the channel is stopped or paused.
func (ch *Channel) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
        ctx, ch.CancelFunc = context.WithCancel(ctx)
        return ctx, ch.CancelFunc
}

// Info logs an informational message.
func (ch *Channel) Info(format string, a ...any) {
        ch.LogCh <- fmt.Sprintf("%s [INFO] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
        log.Printf(" INFO [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// Warn logs a warning message.
func (ch *Channel) Warn(format string, a ...any) {
        ch.LogCh <- fmt.Sprintf("%s [WARN] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
        log.Printf(" WARN [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// Error logs an error message.
func (ch *Channel) Error(format string, a ...any) {
        ch.LogCh <- fmt.Sprintf("%s [ERROR] %s", time.Now().Format("15:04"), fmt.Sprintf(format, a...))
        log.Printf("ERROR [%s] %s", ch.Config.Username, fmt.Sprintf(format, a...))
}

// ExportInfo exports the channel information as a ChannelInfo struct.
func (ch *Channel) ExportInfo() *entity.ChannelInfo {
        var filename string
        if ch.CurrentFilename != "" && ch.HasSeparateAudio {
                filename = ch.CurrentFilename + ".mp4"
        } else if ch.File != nil {
                filename = ch.File.Name()
        }
        var streamedAt string
        if ch.StreamedAt != 0 {
                streamedAt = time.Unix(ch.StreamedAt, 0).Format("2006-01-02 15:04 AM")
        }
        ch.stateMu.Lock()
        isOnline := ch.IsOnline
        roomStatus := ch.RoomStatus
        duration := ch.Duration
        filesize := ch.Filesize
        ch.stateMu.Unlock()

        ch.logsMu.Lock()
        logsCopy := make([]string, len(ch.Logs))
        copy(logsCopy, ch.Logs)
        ch.logsMu.Unlock()

        return &entity.ChannelInfo{
                IsOnline:      isOnline,
                IsPaused:      ch.Config.IsPaused.Load(),
                IsCompressing: atomic.LoadInt32(&ch.CompressingCount) > 0,
                RoomStatus:    roomStatus,
                Username:      ch.Config.Username,
                MaxDuration:   internal.FormatDuration(float64(ch.Config.MaxDuration * 60)),
                MaxFilesize:   internal.FormatFilesize(ch.Config.MaxFilesize * 1024 * 1024),
                StreamedAt:    streamedAt,
                CreatedAt:     ch.Config.CreatedAt,
                Duration:      internal.FormatDuration(duration),
                Filesize:      internal.FormatFilesize(filesize),
                Filename:      filename,
                Logs:          logsCopy,
                GlobalConfig:  server.Config,
        }
}

// Pause pauses the channel and cancels the context.
func (ch *Channel) Pause() {
        // Stop the monitoring loop, this also updates `ch.IsOnline` to false
        // `context.Canceled` → `ch.Monitor()` → `onRetry` → `ch.UpdateOnlineStatus(false)`.
        ch.CancelFunc()

        ch.Config.IsPaused.Store(true)
        ch.Update()
        ch.Info("channel paused")

        // Finalize any in-progress files immediately so they can be uploaded
        // and removed when `DeleteLocalAfterUpload` is enabled.
        go func() {
                if err := ch.Cleanup(false); err != nil {
                        ch.Error("cleanup on pause: %s", err.Error())
                }
        }()

        ctx, cancel := context.WithCancel(context.Background())
        ch.PauseCancelFunc = cancel
        go ch.CheckOnlineWhilePaused(ctx, 0)
}

// Stop stops the channel and cancels the context.
func (ch *Channel) Stop() {
        ch.CancelFunc()
        ch.PauseCancelFunc()
        ch.closeDone.Do(func() { close(ch.done) })
        ch.Info("channel stopped")
}

// Resume resumes the channel monitoring.
//
// `startSeq` is used to prevent all channels from starting at the same time, preventing TooManyRequests errors.
// It's only be used when program starting and trying to resume all channels at once.
func (ch *Channel) Resume(startSeq int) {
        ch.PauseCancelFunc()
        ch.Config.IsPaused.Store(false)

        ch.Update()
        ch.Info("channel resumed")

        ch.monitorWg.Add(1)
        go func() {
                defer ch.monitorWg.Done()
                if startSeq > 0 {
                        <-time.After(time.Duration(startSeq) * time.Second)
                }
                ch.Monitor()
        }()
}

// WaitMonitor blocks until the Monitor goroutine has fully exited.
// By the time it returns, Cleanup() has already run and any pending
// files have been queued into UploadWg.
func (ch *Channel) WaitMonitor() {
        ch.monitorWg.Wait()
}

// UpdateOnlineStatus updates the online status of the channel.
func (ch *Channel) UpdateOnlineStatus(isOnline bool) {
        ch.stateMu.Lock()
        ch.IsOnline = isOnline
        ch.stateMu.Unlock()
        ch.Update()
}

// CheckOnlineWhilePaused periodically refreshes room status for paused channels
// so the UI can still distinguish online/private/offline states.
func (ch *Channel) CheckOnlineWhilePaused(ctx context.Context, startSeq int) {
        client := chaturbate.NewClient()
        baseIntervalMinutes := max(server.Config.Interval, 15)
        cfBlockCount := 0

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

                status, err := client.GetRoomStatus(ctx, ch.Config.Username)
                if err != nil {
                        if errors.Is(err, context.Canceled) {
                                return
                        }
                        if isCFBlock(err) {
                                cfBlockCount++
                                delayMinutes := cfBackoffMinutes(cfBlockCount, baseIntervalMinutes)
                                waitInterval = time.Duration(delayMinutes) * time.Minute
                                ch.Info("paused status check blocked by Cloudflare (attempt %d); retry in %d min(s)", cfBlockCount, delayMinutes)
                        } else {
                                cfBlockCount = 0
                        }
                } else if status != "" {
                        cfBlockCount = 0
                        isOnline := status != chaturbate.StatusAway && status != chaturbate.StatusOffline
                        ch.stateMu.Lock()
                        changed := ch.IsOnline != isOnline || ch.RoomStatus != status
                        if changed {
                                ch.IsOnline = isOnline
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
