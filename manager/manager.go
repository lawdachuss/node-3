package manager

import (
        "bytes"
        "context"
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
        "github.com/teacat/chaturbate-dvr/internal"
        "github.com/teacat/chaturbate-dvr/router/view"
        "github.com/teacat/chaturbate-dvr/server"
)

// Manager is responsible for managing channels and their states.
type Manager struct {
        Channels sync.Map
        SSE      *sse.Server
}

// New initializes a new Manager instance with an SSE server.
func New() (*Manager, error) {

        server := sse.New()
        server.SplitData = true

        updateStream := server.CreateStream("updates")
        updateStream.AutoReplay = false

        return &Manager{
                SSE: server,
        }, nil
}

// SaveConfig saves the current channels to Supabase.
func (m *Manager) SaveConfig() error {
	var config []*entity.ChannelConfig

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

// StartCookieRefresher launches a background goroutine that calls
// Byparr/FlareSolverr periodically to obtain fresh cf_clearance cookies
// and pushes them into the running config automatically.
// On success it sleeps 30 minutes; on failure it retries after 15 seconds.
func (m *Manager) StartCookieRefresher() {
        go func() {
                // Short delay so the rest of the app finishes initialising first.
                time.Sleep(10 * time.Second)
                for {
                        if ok := m.refreshCookiesOnce(); ok {
                                time.Sleep(30 * time.Minute)
                        } else {
                                time.Sleep(15 * time.Second)
                        }
                }
        }()
        fmt.Println(" INFO [cookie-refresher] started — will refresh Cloudflare cookies every 30 minutes")
}

// refreshCookiesOnce returns true when cookies were successfully refreshed.
func (m *Manager) refreshCookiesOnce() bool {
        ctx, cancel := context.WithTimeout(context.Background(), 330*time.Second)
        defer cancel()

        fmt.Println(" INFO [cookie-refresher] fetching fresh cookies from Byparr...")
        cookies, userAgent, err := internal.GetFreshCookiesViaFlareSolverr(ctx, "https://chaturbate.com/")
        if err != nil {
                fmt.Printf("[WARN] [cookie-refresher] %v\n", err)
                return false
        }
        if cookies == "" {
                fmt.Println("[WARN] [cookie-refresher] Byparr returned no cookies")
                return false
        }

	server.UpdateByparrCredentials(cookies, userAgent)
	if err := server.SaveSettings(); err != nil {
		fmt.Printf("[WARN] [cookie-refresher] could not persist cookies: %v\n", err)
		return false
	}
        fmt.Println(" INFO [cookie-refresher] cookies refreshed and saved — recording will resume shortly")
        return true
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

	seq := 0
	for _, conf := range config {
		ch := channel.New(conf)
		m.Channels.Store(conf.Username, ch)

		// Automatically resume all channels on startup
		if ch.Config.IsPaused.Load() {
			ch.Info("channel was paused, automatically resuming on startup")
			ch.Config.IsPaused.Store(false)
		}
		go ch.Resume(seq)
		seq++
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
                        // Skip segment sidecars
                        if strings.Contains(info.Name(), ".video.") || strings.Contains(info.Name(), ".audio.") {
                                return nil
                        }
                        // Only process files that are missing preview URLs in Supabase
                        thumbURL, spriteURL := server.LoadPreviewLinks(info.Name())
                        if thumbURL != "" && spriteURL != "" {
                                return nil
                        }
                        newThumb, newSprite := channel.GenerateThumbnailForFile(path)
                        if newThumb != "" || newSprite != "" {
                                if err := server.SavePreviewLinks(info.Name(), newThumb, newSprite); err != nil {
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

        go ch.Resume(0)

        if shouldSave {
                if err := m.SaveConfig(); err != nil {
                        return fmt.Errorf("save config: %w", err)
                }
        }
        return nil
}

// StopChannel stops the channel.
func (m *Manager) StopChannel(username string) error {
        thing, ok := m.Channels.Load(username)
        if !ok {
                return nil
        }
        thing.(*channel.Channel).Stop()
        m.Channels.Delete(username)

        if err := m.SaveConfig(); err != nil {
                return fmt.Errorf("save config: %w", err)
        }
        return nil
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

// PauseChannel pauses the channel.
func (m *Manager) PauseChannel(username string) error {
        thing, ok := m.Channels.Load(username)
        if !ok {
                return nil
        }
        thing.(*channel.Channel).Pause()

        if err := m.SaveConfig(); err != nil {
                return fmt.Errorf("save config: %w", err)
        }
        return nil
}

// ResumeChannel resumes the channel.
func (m *Manager) ResumeChannel(username string) error {
        thing, ok := m.Channels.Load(username)
        if !ok {
                return nil
        }
        thing.(*channel.Channel).Resume(0)

        if err := m.SaveConfig(); err != nil {
                return fmt.Errorf("save config: %w", err)
        }
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
                // Paused channels always sort to the bottom.
                getPriority := func(c *entity.ChannelInfo) int {
                        switch {
                        case !c.IsPaused && c.IsOnline:
                                return 0 // Recording
                        case !c.IsPaused:
                                return 1 // Offline, actively watching
                        case c.IsOnline:
                                return 2 // Paused, currently online
                        default:
                                return 3 // Paused, offline
                        }
                }

                pi, pj := getPriority(channels[i]), getPriority(channels[j])
                if pi != pj {
                        return pi < pj
                }
                // Same priority: sort by username alphabetically
                return strings.ToLower(channels[i].Username) < strings.ToLower(channels[j].Username)
        })

        return channels
}

// Publish sends an SSE event to the specified channel.
func (m *Manager) Publish(evt entity.Event, info *entity.ChannelInfo) {
        switch evt {
        case entity.EventUpdate:
                var b bytes.Buffer
                if err := view.InfoTpl.ExecuteTemplate(&b, "channel_info", info); err != nil {
                        fmt.Println("Error executing template:", err)
                        return
                }
                m.SSE.Publish("updates", &sse.Event{
                        Event: []byte(info.Username + "-info"),
                        Data:  b.Bytes(),
                })
        case entity.EventLog:
                if len(info.Logs) > 0 {
                        m.SSE.Publish("updates", &sse.Event{
                                Event: []byte(info.Username + "-log"),
                                Data:  []byte(info.Logs[len(info.Logs)-1]),
                        })
                }
        }
}

// Subscriber handles SSE subscriptions for the specified channel.
func (m *Manager) Subscriber(w http.ResponseWriter, r *http.Request) {
        m.SSE.ServeHTTP(w, r)
}
