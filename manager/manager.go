package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/r3labs/sse/v2"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/watcher"
)

// renderCacheEntry holds the last-rendered HTML and a fingerprint of the
// ChannelInfo fields that affect template output. When the fingerprint
// matches a previous publish, the SSE event is skipped entirely.
type renderCacheEntry struct {
	html        []byte
	fingerprint string
}

// channelInfoFingerprint produces a string that changes whenever any
// field displayed in the channel_info template changes. This is used
// to skip redundant template renders + SSE pushes.
func channelInfoFingerprint(info *entity.ChannelInfo) string {
	return fmt.Sprintf("%t|%t|%t|%t|%s|%s|%s|%s|%s",
		info.IsOnline,
		info.IsConnecting,
		info.IsPaused,
		info.IsCompressing,
		info.RoomStatus,
		info.Duration,
		info.Filesize,
		info.Filename,
		info.StreamedAt,
	)
}

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	SSE      *sse.Server

	// watcherDone is closed during graceful shutdown so the fsnotify
	// watcher can release its file handles promptly.
	WatcherDone chan struct{}

	// saveDebounce coalesces rapid SaveConfig calls into a single
	// Supabase PATCH.  The first call starts a 1 s timer; subsequent
	// calls reset it.  When the timer fires, the actual save runs.
	// This prevents API hammering when many channels are paused,
	// resumed, or stopped in quick succession.
	saveDebounce   *time.Timer
	saveDebounceMu sync.Mutex

	// logRateLimit rate-limits SSE log events to at most 1 per second
	// per channel. Log lines still go to the in-memory buffer and
	// terminal output; only the SSE broadcast is throttled to prevent
	// browser lag when many channels are recording simultaneously.
	logRateLimit   map[string]time.Time
	logRateLimitMu sync.Mutex

	// renderCache caches the last-rendered channel_info HTML per
	// channel.  Publish() skips the SSE push when the fingerprint
	// is unchanged, which eliminates redundant template execution
	// and browser DOM replacements for offline/paused channels.
	renderCache   map[string]*renderCacheEntry
	renderCacheMu sync.Mutex

	// watcherMu guards WatcherDone close + reset so the session loop
	// and the SIGTERM handler cannot panic on double-close.
	watcherMu     sync.Mutex
	watcherClosed bool

	// sessionDeadline tracks when the current recording session will end
	// (zero = no active session, recording or between cycles).
	sessionDeadline   time.Time
	sessionDeadlineMu sync.Mutex
	sessionDuration   time.Duration

	// sessionStopCh is created each sessionLoop iteration; TriggerSessionStop
	// sends on it to break out of the timer early and start processing.
	sessionStopCh   chan struct{}
	sessionStopMu   sync.Mutex

	// sessionMu prevents multiple concurrent sessionLoop goroutines when
	// StartSession is called more than once (e.g. from create-channel handler).
	sessionMu       sync.Mutex
	sessionStarted  bool
}

// TriggerSessionStop signals the session loop to stop recording now and
// begin the mux/upload/processing phase early. No-op if no active session.
func (m *Manager) TriggerSessionStop() {
	m.sessionStopMu.Lock()
	ch := m.sessionStopCh
	m.sessionStopMu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SessionInfo returns the remaining recording time and whether a session
// is currently active (recording phase, not processing phase).
func (m *Manager) SessionInfo() (remaining time.Duration, active bool) {
	m.sessionDeadlineMu.Lock()
	defer m.sessionDeadlineMu.Unlock()
	if m.sessionDeadline.IsZero() {
		return 0, false
	}
	remaining = time.Until(m.sessionDeadline)
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// New initializes a new Manager instance with an SSE server.
func New() (*Manager, error) {

	server := sse.New()
	server.SplitData = true

	updateStream := server.CreateStream("updates")
	updateStream.AutoReplay = false

	return &Manager{
		SSE:          server,
		logRateLimit: make(map[string]time.Time),
		renderCache:  make(map[string]*renderCacheEntry),
		WatcherDone:  make(chan struct{}),
	}, nil
}

// debouncedSave is a non-blocking request to persist channel state.
// Multiple calls within 1 s are coalesced into a single Supabase write.
func (m *Manager) debouncedSave() {
	m.saveDebounceMu.Lock()
	defer m.saveDebounceMu.Unlock()
	if m.saveDebounce != nil {
		m.saveDebounce.Stop()
	}
	m.saveDebounce = time.AfterFunc(time.Second, func() {
		if err := m.SaveConfig(); err != nil {
			fmt.Printf("[WARN] debounced save: %v\n", err)
		}
	})
}

// SaveConfig saves the current channels to Supabase.
func (m *Manager) SaveConfig() error {
	// Initialize as empty slice (not nil) so MarshalIndent produces "[]"
	// rather than "null" when all channels are deleted. Supabase's
	// app_settings.value column has a NOT NULL constraint.
	config := make([]*entity.ChannelConfig, 0)

	m.Channels.Range(func(key, value any) bool {
		config = append(config, value.(*channel.Channel).Config)
		return true
	})

	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := server.SaveChannelsToDB(b); err != nil {
		return fmt.Errorf("save channels to database: %w", err)
	}
	return nil
}

// LoadConfig loads the channels from Supabase and starts them.
// All channels are automatically resumed on startup, regardless of their paused state.
func (m *Manager) LoadConfig() error {
	// Restore persisted cookies/user-agent before starting channels
	if err := server.LoadSettings(); err != nil {
		fmt.Printf("[WARN] could not load settings: %v\n", err)
	}

	// Load channels from Supabase
	b := server.LoadChannelsFromDB()
	if b == nil {
		return nil
	}

	var config []*entity.ChannelConfig
	if err := json.Unmarshal(b, &config); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if len(config) == 0 {
		return nil
	}

	for _, conf := range config {
		conf.Sanitize()
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)

		// Automatically resume all channels on startup
		if ch.Config.IsPaused.Load() {
			ch.Info("channel was paused, automatically resuming on startup")
			ch.Config.IsPaused.Store(false)
		}

		ch.Resume(0)
	}

	// Save the updated config to persist the resumed state.
	// This is best-effort — if Supabase is down, the web UI should still start
	// and channels will save their state on the next config change.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[WARN] could not persist channel state to Supabase: %v\n", err)
		fmt.Println("[WARN] channels are running but state changes will be lost if the container restarts")
	}

	// Clean up orphaned sidecar files from previous interrupted runs
	go func() {
		channel.CleanupOrphanedFiles()
		m.ScanThumbnails()
	}()

	// Periodic orphan cleanup + thumbnail scan
	if server.Config.OrphanCleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(server.Config.OrphanCleanupInterval) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				channel.CleanupOrphanedFiles()
				m.ScanThumbnails()
			}
		}()
	}

	// File watcher for real-time orphan detection.
	// Only watch the output directory — files in the temp "videos/"
	// directory are either active recordings (which the watcher must
	// not touch) or are already handled by MoveToOutputDir's upload
	// goroutine after a failed move.
	go func() {
		dirs := []string{}
		if server.Config.OutputDir != "" {
			dirs = append(dirs, server.Config.OutputDir)
		}
		if len(dirs) == 0 {
			return
		}
		fw, err := watcher.New(dirs)
		if err != nil {
			log.Printf("[watcher] failed to start: %v", err)
			return
		}
		log.Printf("[watcher] watching %d directories for new video files", len(dirs))
		fw.Start(m.WatcherDone)
	}()

	return nil
}

// ScanThumbnails walks the videos directory and generates thumbnails for any
// video file that is missing preview URLs in Supabase.
func (m *Manager) ScanThumbnails() {
	videoExts := map[string]bool{".mp4": true, ".mkv": true}
	dirs := []string{"videos"}

	for _, dir := range dirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(path))
			if !videoExts[ext] {
				return nil
			}
			// Skip A/V track sidecars (but keep .video.muxed.mp4 which is the final muxed output)
			if strings.HasSuffix(info.Name(), ".video.mp4") || strings.HasSuffix(info.Name(), ".audio.mp4") {
				return nil
			}
			// Only process files that are missing any preview URLs in Supabase
			thumbURL, spriteURL, previewURL := server.LoadPreviewLinks(info.Name())
			if thumbURL != "" && spriteURL != "" && previewURL != "" {
				return nil
			}
			newThumb, newSprite, newPreview := channel.GenerateThumbnailForFile(path)
			if newThumb != "" || newSprite != "" || newPreview != "" {
				if err := server.SavePreviewLinks(info.Name(), newThumb, newSprite, newPreview); err != nil {
					log.Printf("[thumb] failed to save preview links for %s: %v", info.Name(), err)
				}
			}
			return nil
		})
	}
}

// CreateChannel starts monitoring an M3U8 stream
func (m *Manager) CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error {
	conf.Sanitize()
	ch := channel.New(conf)

	// prevent duplicate channels
	_, ok := m.Channels.Load(conf.Username)
	if ok {
		return fmt.Errorf("channel %s already exists", conf.Username)
	}
	m.Channels.Store(conf.Username, ch)

	ch.Resume(0)

	if shouldSave {
		m.debouncedSave()
	}
	return nil
}

// StopChannel deletes a channel permanently.
//
// Execution order:
//  1. Remove from the in-memory map immediately — the channel disappears from
//     the UI on the very next page load, and duplicate requests are no-ops.
//  2. Persist synchronously via SaveConfig (PATCH to app_settings blob) —
//     this is the only call that MUST complete before the HTTP redirect so the
//     deletion survives a subsequent app restart.
//  3. Stop the ffmpeg recording process in a goroutine — gracefully terminating
//     ffmpeg can take several seconds, and blocking the HTTP handler for that
//     long causes browser timeouts and duplicate click events. The OS will clean
//     up any still-running ffmpeg processes if the app exits before Stop returns.
//  4. Delete the secondary channels-table row in a goroutine — best-effort FK
//     cleanup that never needs to block the response.
func (m *Manager) StopChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}

	// Step 1: remove from memory so subsequent requests are immediate no-ops
	// and the UI reflects the deletion on the next GET /.
	m.Channels.Delete(username)

	// Step 2: synchronous PATCH to the authoritative app_settings blob.
	// Must complete before we redirect so the deletion survives a restart.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[ERROR] SaveConfig after delete of %q: %v\n", username, err)
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf(" INFO [manager] channel %q deleted and persisted to Supabase\n", username)

	// Step 3: non-blocking cleanup — stop the ffmpeg process.
	// The channels table row is intentionally left orphaned because it is shared
	// across instances and no longer read by LoadChannelsFromDB.
	go func() {
		thing.(*channel.Channel).Stop()
	}()

	return nil
}

// StopWatcher signals the fsnotify file watcher to shut down, releasing
// its file handles.  Call during graceful shutdown after all channels have
// been stopped so the watcher cannot race with deferred Cleanup goroutines.
// Safe to call multiple times — the close is guarded by watcherMu.
func (m *Manager) StopWatcher() {
	m.watcherMu.Lock()
	defer m.watcherMu.Unlock()
	if !m.watcherClosed {
		close(m.WatcherDone)
		m.watcherClosed = true
	}
}

// WaitForUploads blocks until all in-flight upload goroutines across every
// channel have finished. Call this during graceful shutdown so recordings
// are not lost when the container receives SIGTERM.
func (m *Manager) WaitForUploads() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).UploadWg.Wait()
		return true
	})
}

// StopAllChannels cancels all active channel Monitor goroutines without
// removing them from the map. Used during graceful shutdown so recordings
// can be finalized and uploaded before the process exits.
func (m *Manager) StopAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).Stop()
		return true
	})
}

// WaitForAllChannels blocks until every channel's Monitor goroutine has
// fully exited. By the time this returns, Cleanup() has run for each
// channel, meaning all pending files have been queued into UploadWg.
// Always call this before WaitForUploads() during graceful shutdown.
func (m *Manager) WaitForAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).WaitMonitor()
		return true
	})
}

// CancelAllChannels cancels the recording context for every channel.
// Unlike StopAllChannels, this does NOT close ch.done, so channels
// can be resumed later via ResumeAllChannels.  The deferred Cleanup
// inside RecordStream still runs, queuing pending files into UploadWg.
func (m *Manager) CancelAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		ch := value.(*channel.Channel)
		ch.Cancel()
		return true
	})
}

// ResumeAllChannels resumes every channel in the map.  Skips any
// channel whose ch.done is already closed (permanently stopped).
func (m *Manager) ResumeAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).Resume(0)
		return true
	})
}

// StartWatcher creates a new fsnotify watcher on the output directory
// and runs it in a background goroutine.  Used by the session loop to
// re-enable orphan detection after the processing phase completes.
func (m *Manager) StartWatcher() {
	dirs := []string{}
	if server.Config != nil && server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}
	if len(dirs) == 0 {
		return
	}

	m.watcherMu.Lock()
	m.WatcherDone = make(chan struct{})
	m.watcherClosed = false
	m.watcherMu.Unlock()

	go func() {
		fw, err := watcher.New(dirs)
		if err != nil {
			log.Printf("[watcher] failed to start: %v", err)
			return
		}
		log.Printf("[watcher] watching %d directories for new video files", len(dirs))
		fw.Start(m.WatcherDone)
	}()
}

// StartSession begins the automatic recording-session lifecycle.
// If duration is <= 0 this is a no-op (continuous recording).
// The session loop: record for duration → cancel all channels →
// wait for mux/upload/Supabase → resume all channels → repeat.
func (m *Manager) StartSession(d time.Duration) {
	if d <= 0 {
		return
	}
	m.sessionMu.Lock()
	if m.sessionStarted {
		m.sessionMu.Unlock()
		return
	}
	m.sessionStarted = true
	m.sessionMu.Unlock()
	go m.sessionLoop(d)
}

func (m *Manager) sessionLoop(d time.Duration) {
	for {
		channels := m.channelCount()
		if channels == 0 {
			m.sessionDeadlineMu.Lock()
			m.sessionDeadline = time.Time{}
			m.sessionDuration = 0
			m.sessionDeadlineMu.Unlock()
			log.Println("[session] no channels to record — stopping session loop")
			m.sessionMu.Lock()
			m.sessionStarted = false
			m.sessionMu.Unlock()
			return
		}
		log.Printf("[session] recording session started — next stop in %s with %d channel(s)", d, channels)

		deadline := time.Now().Add(d)
		m.sessionDeadlineMu.Lock()
		m.sessionDeadline = deadline
		m.sessionDuration = d
		m.sessionDeadlineMu.Unlock()

		stopCh := make(chan struct{}, 1)
		m.sessionStopMu.Lock()
		m.sessionStopCh = stopCh
		m.sessionStopMu.Unlock()

		timer := time.NewTimer(d)
		progress := time.NewTicker(30 * time.Minute)

	sessionWait:
		for {
			select {
			case <-timer.C:
				progress.Stop()
				break sessionWait
			case <-stopCh:
				progress.Stop()
				if !timer.Stop() {
					<-timer.C
				}
				log.Println("[session] manual stop triggered")
				break sessionWait
			case <-progress.C:
				remaining := time.Until(deadline)
				if remaining > 0 {
					log.Printf("[session] %s remaining in recording session", remaining.Round(time.Second))
				}
			}
		}

		m.sessionStopMu.Lock()
		m.sessionStopCh = nil
		m.sessionStopMu.Unlock()

		log.Println("[session] duration reached — stopping all channels")

		m.sessionDeadlineMu.Lock()
		m.sessionDeadline = time.Time{}
		m.sessionDuration = 0
		m.sessionDeadlineMu.Unlock()

		m.CancelAllChannels()
		m.StopWatcher()

		log.Println("[session] waiting for recordings to finalize...")
		m.WaitForAllChannels()

		processingDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			elapsed := 30
			for {
				select {
				case <-ticker.C:
					log.Printf("[session] still finalizing... (%ds elapsed)", elapsed)
					elapsed += 30
				case <-processingDone:
					return
				}
			}
		}()

		log.Println("[session] waiting for mux/thumbnail/upload/Supabase to complete...")
		waitDone := make(chan struct{})
		go func() {
			m.WaitForUploads()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-time.After(10 * time.Minute):
			log.Println("[session] WARN: WaitForUploads timed out after 10 minutes — proceeding anyway")
		}
		close(processingDone)

		log.Println("[session] all processing complete — restarting recording session")

		m.ResumeAllChannels()
		m.StartWatcher()
	}
}

// IsFileUploadInFlight returns true if the given file path is currently
// being uploaded by any channel's upload goroutine.
func (m *Manager) IsFileUploadInFlight(filePath string) bool {
	return channel.IsUploadInFlight(filePath)
}

// channelCount returns the number of channels currently in the map.
func (m *Manager) channelCount() int {
	count := 0
	m.Channels.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// PauseChannel pauses the channel and persists the state.
func (m *Manager) PauseChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Pause()
	m.debouncedSave()
	return nil
}

// ResumeChannel resumes the channel and persists the state.
func (m *Manager) ResumeChannel(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}
	thing.(*channel.Channel).Resume(0)
	m.debouncedSave()
	return nil
}

// ChannelInfo returns a list of channel information for the web UI.
func (m *Manager) ChannelInfo() []*entity.ChannelInfo {
	var channels []*entity.ChannelInfo

	// Iterate over the channels and append their information to the slice
	m.Channels.Range(func(key, value any) bool {
		channels = append(channels, value.(*channel.Channel).ExportInfo())
		return true
	})

	sort.Slice(channels, func(i, j int) bool {
		pi, pj := channelSortPriority(channels[i]), channelSortPriority(channels[j])
		if pi != pj {
			return pi < pj
		}
		// Same priority: sort by username alphabetically
		return strings.ToLower(channels[i].Username) < strings.ToLower(channels[j].Username)
	})

	return channels
}

func channelSortPriority(c *entity.ChannelInfo) int {
	switch {
	case !c.IsPaused && c.IsOnline:
		return 0 // Recording
	case c.IsPaused:
		return 1 // Paused, whether currently online or offline
	case c.IsConnecting:
		return 2 // Reconnecting / actively watching
	default:
		return 3 // Offline
	}
}

// Publish sends an SSE event to the specified channel.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) {
	switch evt {
	case entity.EventUpdate:
		fp := channelInfoFingerprint(info)

		m.renderCacheMu.Lock()
		cached := m.renderCache[info.Username]
		if cached != nil && cached.fingerprint == fp {
			m.renderCacheMu.Unlock()
			return // nothing changed since last publish
		}

		var b bytes.Buffer
		if err := view.InfoTpl.ExecuteTemplate(&b, "channel_info", info); err != nil {
			m.renderCacheMu.Unlock()
			fmt.Println("Error executing template:", err)
			return
		}
		html := b.Bytes()
		m.renderCache[info.Username] = &renderCacheEntry{html: html, fingerprint: fp}
		m.renderCacheMu.Unlock()

		m.SSE.Publish("updates", &sse.Event{
			Event: []byte(info.Username + "-info"),
			Data:  html,
		})
	case entity.EventLog:
		if len(info.Logs) > 0 {
			m.logRateLimitMu.Lock()
			last := m.logRateLimit[info.Username]
			now := time.Now()
			if now.Sub(last) < time.Second {
				m.logRateLimit[info.Username] = now
				m.logRateLimitMu.Unlock()
				return
			}
			m.logRateLimit[info.Username] = now
			m.logRateLimitMu.Unlock()
			m.SSE.Publish("updates", &sse.Event{
				Event: []byte(info.Username + "-log"),
				Data:  []byte(info.Logs[len(info.Logs)-1]),
			})
		}
	}
}

func (m *Manager) PublishLog(username, line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	m.logRateLimitMu.Lock()
	last := m.logRateLimit[username]
	now := time.Now()
	if now.Sub(last) < time.Second {
		m.logRateLimit[username] = now
		m.logRateLimitMu.Unlock()
		return
	}
	m.logRateLimit[username] = now
	m.logRateLimitMu.Unlock()
	m.SSE.Publish("updates", &sse.Event{
		Event: []byte(username + "-log"),
		Data:  []byte(line),
	})
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.SSE.ServeHTTP(w, r)
}
