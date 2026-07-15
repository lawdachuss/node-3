package watcher

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/server"
)

const (
	debounceWindow = 5 * time.Second // how long to wait for writes to settle
)

// FileWatcher monitors directories for new video files and processes them.
// It uses fsnotify for native file system notifications with a debounce
// window to avoid processing files while they're still being written.
type FileWatcher struct {
	watcher *fsnotify.Watcher

	// mu protects pending and stopped
	mu      sync.Mutex
	pending map[string]*debounceState // file path -> debounce state
	stopped bool
}

type debounceState struct {
	timer    *time.Timer
	path     string
	canceled chan struct{} // closed when this state is superseded
}

// New creates a FileWatcher and starts watching the given directories.
func New(dirs []string) (*FileWatcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	fw := &FileWatcher{
		watcher: w,
		pending: make(map[string]*debounceState),
	}

	for _, dir := range dirs {
		if err := fw.addDir(dir); err != nil {
			log.Printf("[watcher] could not watch %s: %v", dir, err)
		}
	}

	return fw, nil
}

// addDir adds a directory and all nested subdirectories to the watch list.
func (fw *FileWatcher) addDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // directory doesn't exist yet — skip silently
		}
		return err
	}
	if !info.IsDir() {
		return nil
	}

	return filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			log.Printf("[watcher] cannot inspect %s: %v", path, walkErr)
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if err := fw.watcher.Add(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			log.Printf("[watcher] could not watch %s: %v", path, err)
		}
		return nil
	})
}

// videoExt returns true if the extension is a known video extension.
func videoExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".mp4" || ext == ".mkv"
}

// isSidecar returns true if the filename appears to be a sidecar/preview file.
// Note: .video.muxed.mp4 is the final muxed output (not a sidecar), while
// .video.mp4 and .audio.mp4 are raw A/V track files (sidecars).
func isSidecar(name string) bool {
	return strings.HasSuffix(name, ".thumb.webp") ||
		strings.HasSuffix(name, ".thumb.jpg") ||
		strings.HasSuffix(name, ".sprite.webp") ||
		strings.HasSuffix(name, ".sprite.jpg") ||
		strings.HasSuffix(name, ".preview.webp") ||
		strings.HasSuffix(name, ".preview.mp4") ||
		strings.HasSuffix(name, ".thumb") ||
		strings.HasSuffix(name, ".sprite") ||
		strings.HasSuffix(name, ".video.mp4") ||
		strings.HasSuffix(name, ".audio.mp4")
}

// Start begins watching for new files. It blocks until the context is done.
// When a new video file is detected and settles (no writes for debounceWindow),
// it generates thumbnails and uploads it, skipping files already in the journal.
func (fw *FileWatcher) Start(done <-chan struct{}) {
	for {
		select {
		case <-done:
			fw.mu.Lock()
			fw.stopped = true
			for _, state := range fw.pending {
				state.timer.Stop()
			}
			fw.mu.Unlock()
			fw.watcher.Close()
			return

		case event, ok := <-fw.watcher.Events:
			if !ok {
				return
			}
			fw.handleEvent(event)

		case err, ok := <-fw.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[watcher] error: %v", err)
		}
	}
}

func (fw *FileWatcher) handleEvent(event fsnotify.Event) {
	name := event.Name
	base := filepath.Base(name)

	if event.Op&fsnotify.Create != 0 {
		if info, err := os.Stat(name); err == nil && info.IsDir() {
			if err := fw.addDir(name); err != nil {
				log.Printf("[watcher] could not watch new directory %s: %v", name, err)
			}
			return
		}
	}

	// Only interested in new/updated video files
	if !videoExt(base) || isSidecar(base) {
		return
	}

	// Ignore REMOVE and CHMOD
	if event.Op&(fsnotify.Create|fsnotify.Write) == 0 {
		return
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.stopped {
		return
	}

	// Cancel any existing debounce state and start fresh.
	// Closing canceled signals the old timer callback to skip.
	if state, exists := fw.pending[name]; exists {
		close(state.canceled)
		state.timer.Stop()
	}

	state := &debounceState{
		path:     name,
		canceled: make(chan struct{}),
	}
	state.timer = time.AfterFunc(debounceWindow, func() {
		select {
		case <-state.canceled:
			return // superseded by a later event
		default:
		}
		fw.processFile(name)
	})
	fw.pending[name] = state
}

// processFile handles a settled video file: checks journal, generates
// thumbnails, uploads, and cleans up.
func (fw *FileWatcher) processFile(filePath string) {
	fw.mu.Lock()
	delete(fw.pending, filePath)
	fw.mu.Unlock()

	base := filepath.Base(filePath)

	// File might have been deleted since the event
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return
	}

	// Skip if the channel system is currently uploading this file
	if channel.IsUploadInFlight(filePath) {
		return
	}

	// Skip if all hosts already have this file per the upload journal
	if channel.IsAlreadyFullyUploaded(filePath) {
		if server.Config == nil || !server.Config.DeleteLocalAfterUpload {
			log.Printf("[watcher] %s already fully uploaded; keeping local copy", base)
			return
		}
		log.Printf("[watcher] %s already fully uploaded — removing local copy", base)
		os.Remove(filePath)
		channel.DeleteSidecarFiles(filePath)
		return
	}

	log.Printf("[watcher] processing new file %s", base)

	// If min-duration is enabled, check threshold before uploading
	if channel.MaybeDeferToPending(filePath) {
		log.Printf("[watcher] %s deferred to pending (below min-duration threshold)", base)
		return
	}

	// Claim the file via the in-flight marker so the concurrently-running
	// orphan scanner does not also upload it. The orphan scanner checks
	// IsUploadInFlight and skips files already claimed here.
	channel.MarkUploadInFlight(filePath)
	defer channel.MarkUploadDone(filePath)

	// Generate thumbnails and upload
	thumbURL, spriteURL, previewURL := channel.GenerateThumbnailForFile(filePath)
	if !channel.UploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL) {
		log.Printf("[watcher] upload failed for %s; file will remain for retry", base)
	}
}
