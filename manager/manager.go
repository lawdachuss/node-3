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
	"github.com/teacat/chaturbate-dvr/coordinator"
	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/router/view"
	"github.com/teacat/chaturbate-dvr/server"
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
	return fmt.Sprintf("%t|%t|%t|%t|%s|%s|%s|%s|%s|%s|%.0f|%s|%s",
		info.IsOnline,
		info.IsConnecting,
		info.IsPaused,
		info.IsCompressing,
		info.RoomStatus,
		info.Duration,
		info.Filesize,
		info.Filename,
		info.StreamedAt,
		info.UploadStatus,
		info.UploadProgress,
		info.UploadFilename,
		info.LastError,
	)
}

// Manager is responsible for managing channels and their states.
type Manager struct {
	Channels sync.Map
	// draining tracks channels that are currently stopping (after a
	// reassignment) so we never run two Channel objects for the same username
	// at once (which would write to the same output paths and double-upload).
	draining sync.Map
	SSE      *sse.Server

	// Coordinator for distributed shards/nodes mode (nil in isolated mode).
	Coordinator *coordinator.Coordinator

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

	// sessionDeadline tracks when the current recording session will end
	// (zero = no active session, recording or between cycles).
	sessionDeadline   time.Time
	sessionDeadlineMu sync.Mutex
	sessionDuration   time.Duration

	// sessionStopCh is created each sessionLoop iteration; TriggerSessionStop
	// sends on it to break out of the timer early and start processing.
	sessionStopCh chan struct{}
	sessionStopMu sync.Mutex

	// sessionMu prevents multiple concurrent sessionLoop goroutines when
	// StartSession is called more than once (e.g. from create-channel handler).
	sessionMu       sync.Mutex
	sessionStarted  bool
	sessionStopped  bool // set by StopSession to permanently stop the loop

	// cooldowns prevents channels from being immediately re-created after a
	// forced stop (e.g. CDN/proxy hang that caused WaitMonitor to time out).
	// Without a cooldown, the coordinator's reconciliation loop restarts the
	// channel on the next cycle, hitting the same broken CDN edge and creating
	// a vicious stop-start loop that produces short recordings.
	// Map key: username, value: time.Time until which re-creation is blocked.
	cooldowns   map[string]time.Time
	cooldownsMu sync.Mutex
}

// setCooldown records a restart cooldown for the given username.
// If forced is true, the cooldown is 5 minutes (hung CDN/proxy likely still
// broken); otherwise it is a brief 30-second debounce to prevent thundering
// herd restarts from the reconciliation loop.
func (m *Manager) setCooldown(username string, forced bool) {
	m.cooldownsMu.Lock()
	defer m.cooldownsMu.Unlock()
	if m.cooldowns == nil {
		m.cooldowns = make(map[string]time.Time)
	}
	var dur time.Duration
	if forced {
		dur = 5 * time.Minute
	} else {
		dur = 30 * time.Second
	}
	m.cooldowns[username] = time.Now().Add(dur)
	log.Printf("[manager] cooldown set for %q: %s (forced=%v)", username, dur, forced)
}

// inCooldown checks whether the given username is currently in cooldown.
// Returns the remaining duration and whether the cooldown is active.
func (m *Manager) inCooldown(username string) (time.Duration, bool) {
	m.cooldownsMu.Lock()
	defer m.cooldownsMu.Unlock()
	if m.cooldowns == nil {
		return 0, false
	}
	until, ok := m.cooldowns[username]
	if !ok {
		return 0, false
	}
	remaining := time.Until(until)
	if remaining <= 0 {
		delete(m.cooldowns, username)
		return 0, false
	}
	return remaining, true
}

// StopSession permanently stops the session loop so it won't restart
// after the current cycle finishes.  Call before StopAllChannels during
// graceful shutdown to prevent the loop from racing with teardown.
func (m *Manager) StopSession() {
	m.TriggerSessionStop()
	m.sessionMu.Lock()
	m.sessionStopped = true
	m.sessionMu.Unlock()
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
// In pooled mode, saves to the shared channel_pool instead of instance-scoped key.
func (m *Manager) SaveConfig() error {
	config := make([]*entity.ChannelConfig, 0)

	m.Channels.Range(func(key, value any) bool {
		config = append(config, value.(*channel.Channel).Config)
		return true
	})

	b, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if server.IsPooledMode() {
		return server.SavePoolToDB(b)
	}
	return server.SaveChannelsToDB(b)
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
		ch.PipelineQueue.ResumePending()

		// Automatically resume all channels on startup
		if ch.Config.IsPaused.Load() {
			ch.Info("channel was paused, automatically resuming on startup")
			ch.Config.IsPaused.Store(false)
		}

		ch.Resume(0)

		// Recover any segments left in the channel's .pending directory from a
		// previous run or a session that ended with unmerged short clips.  The
		// background fsnotify watcher that used to re-scan these directories
		// has been removed, so this is the only path that prevents those
		// recordings from being orphaned forever.
		go ch.RecoverPendingSegments()
	}

	// Save the updated config to persist the resumed state.
	// This is best-effort — if Supabase is down, the web UI should still start
	// and channels will save their state on the next config change.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[WARN] could not persist channel state to Supabase: %v\n", err)
		fmt.Println("[WARN] channels are running but state changes will be lost if the container restarts")
	}

	// Scan thumbnails for the UI
	go func() {
		m.ScanThumbnails()
	}()

	// Periodic thumbnail scan
	if server.Config.OrphanCleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(server.Config.OrphanCleanupInterval) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				m.ScanThumbnails()
			}
		}()
	}

	return nil
}

// ============================================================================
// Pooled mode (distributed shards/nodes)
// ============================================================================

// LoadPooledConfig creates local channel objects from the channel_assignments
// rows assigned to this node.  Called instead of LoadConfig() when
// CHANNEL_POOL_MODE=pooled.  The channel_assignments table is the sole
// source of truth — the legacy channel_pool app_settings blob is ignored.
func (m *Manager) LoadPooledConfig() error {
	// Restore persisted cookies/user-agent
	if err := server.LoadSettings(); err != nil {
		fmt.Printf("[WARN] could not load settings: %v\n", err)
	}

	client := server.GetDBClient()
	if client == nil {
		return fmt.Errorf("supabase not configured")
	}

	// Fetch assignments that belong to this node (status != unassigned).
	myAssignments, err := client.GetNodeAssignments(server.NodeID())
	if err != nil {
		return fmt.Errorf("get node assignments: %w", err)
	}

	// If channel_assignments is empty, migrate from old isolated-mode app_settings blob
	if len(myAssignments) == 0 {
		migrated, err := m.migrateLegacyChannels(client)
		if err != nil {
			fmt.Printf("[manager] legacy migration: %v\n", err)
		} else if migrated > 0 {
			fmt.Printf("[manager] migrated %d channel(s) from legacy app_settings\n", migrated)
			myAssignments, err = client.GetNodeAssignments(server.NodeID())
			if err != nil {
				return fmt.Errorf("get node assignments after migration: %w", err)
			}
		}
	}

	created := 0
	for _, a := range myAssignments {
		if a.Status == "unassigned" || a.AssignedNode == "" {
			continue
		}
		conf := coordinator.ConfigFromAssignment(&a)
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)
		ch.PipelineQueue.ResumePending()
		ch.Resume(0)
		created++
	}

	fmt.Printf("[manager] LoadPooledConfig: loaded %d channel(s) for node %q\n",
		created, server.NodeID())

	// Scan thumbnails (same as LoadConfig)
	go func() {
		m.ScanThumbnails()
	}()

	if server.Config.OrphanCleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(server.Config.OrphanCleanupInterval) * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				m.ScanThumbnails()
			}
		}()
	}

	return nil
}

// migrateLegacyChannels reads the old isolated-mode channel list from
// app_settings (key: channels_<INSTANCE_ID>) and inserts each channel as
// an unassigned row in channel_assignments. This lets existing setups
// transition to pooled mode without losing their channel configuration.
func (m *Manager) migrateLegacyChannels(client *database.Client) (int, error) {
	data := server.LoadChannelsFromDB()
	if data == nil {
		return 0, nil
	}

	var configs []*entity.ChannelConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return 0, fmt.Errorf("unmarshal legacy channels: %w", err)
	}
	if len(configs) == 0 {
		return 0, nil
	}

	var assignments []database.ChannelAssignment
	for _, conf := range configs {
		conf.Sanitize()
		assignments = append(assignments, database.ChannelAssignment{
			Username:                conf.Username,
			Site:                    conf.Site,
			Status:                  "unassigned",
			IsLive:                  false,
			Framerate:               conf.Framerate,
			Resolution:              conf.Resolution,
			Pattern:                 conf.Pattern,
			MaxDuration:             conf.MaxDuration,
			MaxFilesize:             conf.MaxFilesize,
			Compress:                conf.Compress,
			MinDurationBeforeUpload: conf.MinDurationBeforeUpload,
		})
	}

	if err := client.BulkInsertAssignments(assignments); err != nil {
		return 0, fmt.Errorf("bulk insert assignments: %w", err)
	}

	if err := server.ClearLegacyChannelsBlob(); err != nil {
		fmt.Printf("[manager] warning: could not clear legacy channels blob: %v\n", err)
	}

	fmt.Printf("[manager] migrateLegacyChannels: migrated %d channel(s) to channel_assignments\n", len(assignments))
	return len(assignments), nil
}

// CreateChannelFromAssignment implements coordinator.ChannelManager.
// Creates a channel from a channel_assignments row (claimed by coordinator).
// If the channel is in cooldown (from a recent forced stop), the creation
// is deferred until the cooldown expires to prevent vicious restart loops.
func (m *Manager) CreateChannelFromAssignment(ca *database.ChannelAssignment) error {
	conf := coordinator.ConfigFromAssignment(ca)
	conf.Sanitize()

	// Check restart cooldown — if the channel was force-stopped recently, wait
	// for the cooldown to expire before creating a new Monitor.  Without this,
	// a broken CDN edge would trigger stop→restart cycles on every 60s
	// reconciliation tick, producing short recordings and failing immediately.
	if remaining, ok := m.inCooldown(conf.Username); ok {
		log.Printf("[manager] channel %q is in cooldown (%v remaining) — deferring creation", conf.Username, remaining)
		return fmt.Errorf("channel %q in cooldown (%v remaining)", conf.Username, remaining)
	}

	// If a previous instance of this channel is still draining its uploads
	// after a reassignment, wait for it to finish before starting a new one so
	// we never run two Channel objects for the same username concurrently.
	if wg, ok := m.draining.Load(conf.Username); ok {
		wg.(*sync.WaitGroup).Wait()
		m.draining.Delete(conf.Username)
	}

	// Check for duplicate
	if _, loaded := m.Channels.LoadOrStore(conf.Username, channel.New(conf)); loaded {
		return nil // already exists
	}

	// Load the stored channel and start it
	thing, _ := m.Channels.Load(conf.Username)
	ch := thing.(*channel.Channel)
	ch.PipelineQueue.ResumePending()
	ch.Resume(0)

	// Restart the session loop if it exited (e.g. when channels were claimed after
	// an empty startup).  If the session is already active this is a no-op.
	// Newly claimed channels then participate in the next session boundary
	// (stop → process → upload → resume cycle).
	m.sessionDeadlineMu.Lock()
	dur := m.sessionDuration
	m.sessionDeadlineMu.Unlock()
	m.StartSession(dur)

	fmt.Printf("[manager] created channel from assignment: %s/%s\n", ca.Site, ca.Username)
	return nil
}

// RemoveChannelForReassignment implements coordinator.ChannelManager.
// Removes a channel from this node when it's been reassigned to another node.
// If the stop was forced (WaitMonitor timeout), sets a restart cooldown to
// prevent the reconciliation loop from immediately re-creating the channel.
func (m *Manager) RemoveChannelForReassignment(username string) error {
	thing, ok := m.Channels.Load(username)
	if !ok {
		return nil
	}

	ch := thing.(*channel.Channel)
	m.Channels.Delete(username)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	m.draining.Store(username, wg)
	go func() {
		defer m.draining.Delete(username)
		defer wg.Done()
		ch.Stop()
		if ch.WasForcedStop() {
			m.setCooldown(username, true)
		}
	}()
	return nil
}

// GetLocalChannels implements coordinator.ChannelManager.
// Returns the list of usernames of channels active on this node.
func (m *Manager) GetLocalChannels() []string {
	var list []string
	m.Channels.Range(func(key, value interface{}) bool {
		if username, ok := key.(string); ok {
			list = append(list, username)
		}
		return true
	})
	return list
}

// ScanThumbnails walks the videos directory and generates thumbnails for any
// video file that is missing preview URLs in Supabase.
func (m *Manager) ScanThumbnails() {
	videoExts := map[string]bool{".mp4": true, ".mkv": true}
	dirs := []string{"videos"}
	if server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

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

// CreateChannel starts monitoring an M3U8 stream.
// If the channel is in cooldown (from a recent forced stop), creation is
// rejected to prevent immediate restart cycles.
func (m *Manager) CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error {
	conf.Sanitize()

	// Check restart cooldown — if the channel was force-stopped recently,
	// reject the creation so the broken CDN/proxy can recover.
	if remaining, ok := m.inCooldown(conf.Username); ok {
		return fmt.Errorf("channel %q is in cooldown (%v remaining) — try again later", conf.Username, remaining)
	}

	// In pooled mode, create the assignment and try to claim for this node
	if server.IsPooledMode() && m.Coordinator != nil {
		if err := m.Coordinator.CreateChannelAssignment(conf); err != nil {
			return fmt.Errorf("create assignment: %w", err)
		}
		shouldSave = false // pool save is handled by coordinator
	}

	// prevent duplicate channels
	_, ok := m.Channels.Load(conf.Username)
	if ok {
		return fmt.Errorf("channel %s already exists", conf.Username)
	}

	ch := channel.New(conf)
	m.Channels.Store(conf.Username, ch)
	ch.PipelineQueue.ResumePending()
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

	ch := thing.(*channel.Channel)

	// Step 1: remove from memory so subsequent requests are immediate no-ops
	// and the UI reflects the deletion on the next GET /.
	m.Channels.Delete(username)

	// Step 2: in pooled mode, release the assignment first
	if server.IsPooledMode() && m.Coordinator != nil {
		m.Coordinator.ReleaseChannel(username, ch.Config.Site)
	}

	// Step 3: synchronous PATCH to the authoritative app_settings blob.
	// In pooled mode, this saves to the shared pool.
	if err := m.SaveConfig(); err != nil {
		fmt.Printf("[ERROR] SaveConfig after delete of %q: %v\n", username, err)
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf(" INFO [manager] channel %q deleted and persisted to Supabase\n", username)

	// Step 4: non-blocking cleanup — stop the ffmpeg process.
	// If the stop was forced (WaitMonitor timeout), set a restart cooldown
	// to prevent immediate re-creation.
	go func() {
		ch.Stop()
		if ch.WasForcedStop() {
			m.setCooldown(username, true)
		}
	}()

	return nil
}

// WaitForUploads processes queued recordings and blocks until their uploads
// and metadata saves have finished. Call this during graceful shutdown so
// recordings are not lost when the container receives SIGTERM.
func (m *Manager) WaitForUploads() {
	var chs []*channel.Channel
	m.Channels.Range(func(key, value any) bool {
		chs = append(chs, value.(*channel.Channel))
		return true
	})
	if len(chs) == 0 {
		return
	}

	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, ch := range chs {
		sem <- struct{}{}
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			ch.ProcessPending()
		}(ch)
	}
	wg.Wait()
}

// StopAllChannels cancels all active channel Monitor goroutines without
// removing them from the map. Used during graceful shutdown so recordings
// can be finalized and uploaded before the process exits.
func (m *Manager) StopAllChannels() {
	m.Channels.Range(func(key, value any) bool {
		value.(*channel.Channel).Cancel()
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

// StopWithProcessingQueue cancels all channels and processes their queued
// recordings in batches using a limited number of workers.  Each worker
// processes one channel at a time (mux all pending files, wait for all
// uploads) so CPU, disk, and network contention is minimised.
func (m *Manager) StopWithProcessingQueue(workers int) {
	var chs []*channel.Channel
	m.Channels.Range(func(key, value any) bool {
		chs = append(chs, value.(*channel.Channel))
		return true
	})

	m.CancelAllChannels()

	log.Printf("[session] waiting for %d channels to close recordings...", len(chs))
	m.WaitForAllChannels()

	if len(chs) == 0 {
		return
	}

	log.Printf("[session] processing %d channels with %d worker(s)...", len(chs), workers)

	// Publish a status log to each channel so the UI shows what's happening.
	for _, ch := range chs {
		ch.Info("session stopped — processing pending files (mux, compress, upload)")
	}

	// Broadcast upload state so the frontend activates the upload bar.
	m.PublishUploadState()

	// Progress ticker
	processingDone := make(chan struct{})
	defer close(processingDone)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		elapsed := 0
		for {
			select {
			case <-ticker.C:
				elapsed += 30
				log.Printf("[session] still processing... (%ds elapsed)", elapsed)
			case <-processingDone:
				return
			}
		}
	}()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for _, ch := range chs {
		sem <- struct{}{}
		wg.Add(1)
		go func(ch *channel.Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[session] PANIC processing channel %s: %v", ch.Config.Username, r)
				}
			}()
			ch.Info("processing pending files...")
			ch.ProcessPending()

			// Pause the channel so it moves to the "Paused" section in the UI
			// and logs reflect the completed state.
			ch.Pause()
			ch.Info("channel paused — ready for next session")
		}(ch)
	}

	wg.Wait()

	// Final broadcast so upload bar hides when all processing is done.
	m.PublishUploadState()
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
	m.sessionDuration = d // persist so CreateChannelFromAssignment can restart the session later
	if m.sessionStarted {
		m.sessionMu.Unlock()
		return
	}
	m.sessionStarted = true
	m.sessionStopped = false
	m.sessionMu.Unlock()
	go m.sessionLoop(d)
}

func (m *Manager) sessionLoop(d time.Duration) {
	log.Printf("[session] recording session started — next stop in %s with %d channel(s)", d, m.channelCount())

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

	m.StopWithProcessingQueue(10)

	log.Println("[session] all processing complete — session ended")

	// Signal workflow that all uploads are done so it can safely exit.
	if err := os.WriteFile("upload-complete.flag", []byte("done"), 0644); err != nil {
		log.Printf("[session] WARNING: could not write upload-complete.flag: %v", err)
	} else {
		log.Println("[session] upload-complete.flag written")
	}

	// Restart the session: resume channels, restart the watcher, and begin
	// the next recording cycle.  We keep sessionStarted = true so no other
	// caller can start a duplicate session loop.  If StopSession() was called
	// (e.g. during graceful shutdown), skip the restart.
	m.sessionMu.Lock()
	stopped := m.sessionStopped
	m.sessionMu.Unlock()
	if stopped {
		log.Println("[session] session permanently stopped — not restarting")
		m.sessionMu.Lock()
		m.sessionStarted = false
		m.sessionMu.Unlock()
		return
	}

	log.Println("[session] restarting recording session")
	m.ResumeAllChannels()
	m.sessionLoop(d)
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

// PublishUploadState aggregates upload progress from all channels and
// broadcasts it as an SSE "upload" event for the session timer UI.
func (m *Manager) PublishUploadState() {
	state := entity.UploadState{Active: false}
	var entries []entity.UploadEntry
	m.Channels.Range(func(_, value any) bool {
		ch := value.(*channel.Channel)
		es := ch.UploadEntry()
		if len(es) == 0 {
			return true
		}
		state.Active = true
		entries = append(entries, es...)
		return true
	})
	if len(entries) == 0 {
		entries = nil
	}
	state.Channels = entries

	payload, err := json.Marshal(state)
	if err != nil {
		return
	}
	m.SSE.Publish("updates", &sse.Event{
		Event: []byte("upload"),
		Data:  payload,
	})
}

func (m *Manager) PublishLog(username, line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	m.logRateLimitMu.Lock()
	last := m.logRateLimit[username]
	now := time.Now()
	if now.Sub(last) < time.Second {
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

// UploadEntries returns the full uploads response (active + pending + history) for the API.
func (m *Manager) UploadEntries() *entity.UploadsResponse {
	resp := &entity.UploadsResponse{}
	m.Channels.Range(func(_, value any) bool {
		ch := value.(*channel.Channel)
		es := ch.UploadEntry()
		if len(es) > 0 {
			resp.Active = append(resp.Active, es...)
		}
		queued := ch.PipelineQueue.QueuedEntries()
		resp.Pending = append(resp.Pending, queued...)
		hist := ch.PipelineQueue.HistoryEntries()
		resp.History = append(resp.History, hist...)
		return true
	})
	if resp.Active == nil {
		resp.Active = []entity.UploadEntry{}
	}
	if resp.Pending == nil {
		resp.Pending = []entity.PendingEntry{}
	}
	if resp.History == nil {
		resp.History = []entity.PendingEntry{}
	}
	return resp
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
	m.SSE.ServeHTTP(w, r)
}
