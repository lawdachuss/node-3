package channel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var pendingDirMu sync.Mutex

type Pattern struct {
	Username string
	Sequence int
	Year     string
	Month    string
	Day      string
	Hour     string
	Minute   string
	Second   string
}

// CloseMode controls whether Cleanup processes pending files immediately
// or defers processing for later (batched session stop).
type CloseMode int

const (
	CloseProcess CloseMode = iota // close files + process pending (rotation, pause, stream error)
	CloseQueue                    // close files only, defer processing to ProcessPending (session stop)
)

// NextFile prepares the next file to be created, by cleaning up the last file and generating a new one
func (ch *Channel) NextFile() error {
	if err := ch.Cleanup(CloseProcess); err != nil {
		return err
	}
	filename, err := ch.GenerateFilename()
	if err != nil {
		return err
	}
	ch.stateMu.Lock()
	ch.CurrentFilename = filename
	ch.videoSegmentCount = 0
	ch.audioSegmentCount = 0
	ch.stateMu.Unlock()

	if err := ch.CreateNewFile(filename); err != nil {
		return err
	}

	// Increment the sequence number for the next file
	ch.Sequence++
	return nil
}

// Cleanup closes any open recording files.
// CloseProcess: also mux/compress/upload pending files asynchronously (rotation, pause, stream error).
// CloseQueue:   only close and queue files; caller must call ProcessPending() later (session stop).
func (ch *Channel) Cleanup(mode CloseMode) error {
	ch.cleanupMu.Lock()
	defer ch.cleanupMu.Unlock()

	if ch.File == nil && ch.AudioFile == nil && len(ch.pendingFiles) == 0 {
		return nil
	}

	// Close any open files and add them to the pending queue (or remove empty ones).
	// Errors from closeTrackedFile are logged but not returned — aborting
	// would strand ALL previously queued pendingFiles permanently.
	if ch.File != nil || ch.AudioFile != nil {
		videoPath, _, err := closeTrackedFile(ch.File)
		if err != nil {
			ch.Error("cleanup: video file close: %s", err.Error())
		}
		audioPath, _, err := closeTrackedFile(ch.AudioFile)
		if err != nil {
			ch.Error("cleanup: audio file close: %s", err.Error())
		}

		ch.File = nil
		ch.AudioFile = nil
		ch.stateMu.Lock()
		ch.CurrentFilename = ""
		ch.Filesize = 0
		ch.Duration = 0
		hasVideo := ch.videoSegmentCount > 0
		hasAudio := ch.audioSegmentCount > 0
		ch.stateMu.Unlock()

		// Remove files that contain only init segments with no media data.
		if ch.HasSeparateAudio && !hasVideo && !hasAudio {
			if videoPath != "" {
				os.Remove(videoPath)
			}
			if audioPath != "" {
				os.Remove(audioPath)
			}
		} else if !ch.HasSeparateAudio && !hasVideo {
			if videoPath != "" {
				os.Remove(videoPath)
			}
			if mode == CloseQueue && len(ch.pendingFiles) > 10 {
				ch.Warn("cleanup: %d pending files accumulated during rotation — will be processed when recording ends", len(ch.pendingFiles))
			}
		} else {
			ch.stateMu.Lock()
			hasSeparateAudio := ch.HasSeparateAudio
			ch.stateMu.Unlock()
			ch.pendingFiles = append(ch.pendingFiles, pendingFile{
				videoPath:        videoPath,
				audioPath:        audioPath,
				hasSeparateAudio: hasSeparateAudio,
				skipMinDuration:  ch.Config.IsPaused.Load(),
			})
			if videoPath != "" {
				ch.Info("cleanup: queued %s for post-processing (%d pending)", filepath.Base(videoPath), len(ch.pendingFiles))
			} else if audioPath != "" {
				ch.Info("cleanup: queued %s for post-processing (%d pending)", filepath.Base(audioPath), len(ch.pendingFiles))
			}
		}
	}

	if mode == CloseProcess && len(ch.pendingFiles) > 0 {
		files := ch.pendingFiles
		ch.pendingFiles = nil
		ch.pendingWg.Add(1)
		go func() {
			defer ch.pendingWg.Done()
			for _, pf := range files {
				ch.processPendingFile(pf)
			}
		}()
	}
	return nil
}

// processPendingQueue processes all pending files: mux A/V if needed, move to
// output dir, generate previews, upload, save metadata, and delete local files.
// Must be called with cleanupMu held.
func (ch *Channel) processPendingQueue() {
	if len(ch.pendingFiles) == 0 {
		return
	}
	ch.Info("cleanup: processing %d pending file(s)", len(ch.pendingFiles))

	for _, pf := range ch.pendingFiles {
		ch.processPendingFile(pf)
	}
	ch.pendingFiles = nil
}

func (ch *Channel) processPendingFile(pf pendingFile) {
	videoPath := pf.videoPath
	audioPath := pf.audioPath

	if pf.hasSeparateAudio {
		ch.processPendingMuxPair(videoPath, audioPath, pf.skipMinDuration)
		return
	}

	// Single-stream file — move to output dir (triggers preview + upload).
	if _, err := os.Stat(videoPath); err == nil {
		if ch.Config.Compress {
			if !pf.skipMinDuration && ch.handleMinDurationAndMerge(videoPath) {
				return // video was deferred to pending or merged+uploaded
			}
			ch.CompressFile(videoPath)
			return
		} else if !pf.skipMinDuration && ch.handleMinDurationAndMerge(videoPath) {
			return // video was deferred to pending or merged+uploaded
		} else {
			// Normalize fMP4 timestamps: Stripchat's LL-HLS segments carry
			// absolute server timestamps (e.g. start at 5044s), making the
			// file appear hours long.  A fast ffmpeg stream-copy remux resets
			// the timeline.
			normalized, _ := normalizeFMP4Timestamps(videoPath)
			ch.MoveToOutputDir(normalized)
		}
	}
}

func (ch *Channel) processPendingMuxPair(videoPath, audioPath string, skipMinDuration bool) {
	videoInfo, _ := os.Stat(videoPath)
	audioInfo, _ := os.Stat(audioPath)

	switch {
	case videoInfo == nil && audioInfo == nil:
		return
	case videoInfo == nil:
		if muxedFileFromSidecar(audioPath) != "" {
			ch.Info("mux: stale audio sidecar %s (muxed version exists) — removing", filepath.Base(audioPath))
			os.Remove(audioPath)
			return
		}
		ch.Info("mux: video track missing; preserving audio-only file %s", filepath.Base(audioPath))
		if !skipMinDuration && ch.handleMinDurationAndMerge(audioPath) {
			return
		}
		if ch.Config.Compress {
			ch.CompressFile(audioPath)
		} else {
			ch.MoveToOutputDir(audioPath)
		}
		return
	case audioInfo == nil:
		if muxedFileFromSidecar(videoPath) != "" {
			ch.Info("mux: stale video sidecar %s (muxed version exists) — removing", filepath.Base(videoPath))
			os.Remove(videoPath)
			return
		}
		ch.Info("mux: audio track missing; preserving video-only file %s", filepath.Base(videoPath))
		if !skipMinDuration && ch.handleMinDurationAndMerge(videoPath) {
			return
		}
		if ch.Config.Compress {
			ch.CompressFile(videoPath)
		} else {
			ch.MoveToOutputDir(videoPath)
		}
		return
	}

	// Both tracks exist — mux them together.
	finalOutput := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".muxed.mp4"
	if err := ch.MuxAV(videoPath, audioPath, finalOutput); err != nil {
		ch.Info("mux: ffmpeg mux failed, trying native fallback: %s", err.Error())
		if nativeErr := ch.MuxAVNative(videoPath, audioPath, finalOutput); nativeErr != nil {
			ch.Error("mux failed for %s: %v — uploading tracks separately", filepath.Base(videoPath), nativeErr)
			_ = os.Remove(finalOutput)
			ch.MoveToOutputDir(videoPath)
			ch.MoveToOutputDir(audioPath)
			return
		}
	}

	if ok, reason := muxOutputLooksValid(finalOutput, videoInfo, audioInfo); !ok {
		ch.Error("mux: output looks corrupt (%s); uploading sidecars %s and %s separately", reason, filepath.Base(videoPath), filepath.Base(audioPath))
		_ = os.Remove(finalOutput)
		ch.MoveToOutputDir(videoPath)
		ch.MoveToOutputDir(audioPath)
		return
	}

	_ = os.Remove(videoPath)
	_ = os.Remove(audioPath)
	ch.Info("delete: removed sidecar %s", filepath.Base(videoPath))
	ch.Info("delete: removed sidecar %s", filepath.Base(audioPath))

	if ch.Config.Compress {
		if !skipMinDuration && ch.handleMinDurationAndMerge(finalOutput) {
			return // video was deferred to pending or merged+uploaded
		}
		ch.CompressFile(finalOutput)
	} else if !skipMinDuration && ch.handleMinDurationAndMerge(finalOutput) {
		return // video was deferred to pending or merged+uploaded
	} else {
		ch.MoveToOutputDir(finalOutput)
	}
}

// muxOutputLooksValid returns true if the muxed MP4 exists and contains data.
// With `-c copy -shortest` the output is intentionally truncated to the shorter
// track's duration, so we cannot compare file sizes against the input sum.
// Trust ffmpeg's exit code — if it returned 0 the file is valid.
func muxOutputLooksValid(outputPath string, _ /*videoInfo*/, _ /*audioInfo*/ os.FileInfo) (bool, string) {
	finalInfo, err := os.Stat(outputPath)
	if err != nil {
		return false, fmt.Sprintf("stat: %s", err.Error())
	}
	if finalInfo.Size() == 0 {
		return false, "empty output"
	}
	return true, ""
}

// muxedFileFromSidecar checks if a .video.muxed.mp4 file exists for the
// given sidecar path (.video.mp4 or .audio.mp4).  Returns the muxed path if
// it exists, or "" otherwise.
func muxedFileFromSidecar(sidecarPath string) string {
	base := sidecarPath
	// Strip .video.mp4 or .audio.mp4 suffix.
	for _, suf := range []string{".video.mp4", ".audio.mp4"} {
		if strings.HasSuffix(base, suf) {
			muxedPath := strings.TrimSuffix(base, suf) + ".video.muxed.mp4"
			if _, err := os.Stat(muxedPath); err == nil {
				return muxedPath
			}
			return ""
		}
	}
	return ""
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

// MoveToOutputDir relocates a finalized recording into server.Config.OutputDir.
// Errors are non-fatal: the recording is already safely written at srcPath.
func (ch *Channel) MoveToOutputDir(srcPath string) string {
	// Enqueue the file into the pipeline for thumbnail → upload → metadata → cleanup.
	// The pipeline handles all lifecycle (semaphore, waitgroup, state persistence).
	enqueue := func(filePath string) {
		ch.PipelineQueue.EnqueueFile(filePath)
	}

	if server.Config == nil || server.Config.OutputDir == "" {
		enqueue(srcPath)
		return srcPath
	}

	destDir := server.Config.OutputDir
	if server.Config.PerModelFolder {
		destDir = filepath.Join(destDir, ch.Config.Username)
	}
	if err := os.MkdirAll(destDir, 0777); err != nil {
		ch.Error("output-dir: mkdir %s: %s", destDir, err.Error())
		return srcPath
	}

	destPath := uniqueDestPath(filepath.Join(destDir, filepath.Base(srcPath)))
	ch.Info("output-dir: moving %s (%s) -> %s", filepath.Base(srcPath), resolvePathForLog(srcPath), destPath)
	// Mark in-flight before moveFile so the watcher's fsnotify handler
	// sees the file as already claimed by the pipeline.
	MarkUploadInFlight(destPath)
	if err := moveFile(srcPath, destPath); err != nil {
		ch.Error("output-dir: move %s to %s: %s — uploading from original location (%s)", filepath.Base(srcPath), destDir, err.Error(), resolvePathForLog(srcPath))
		MarkUploadDone(destPath) // release the failed-dest marker before marking src
		MarkUploadInFlight(srcPath)
		enqueue(srcPath)
		return srcPath
	}
	// Verify the destination actually exists after the move — on some
	// Windows configurations os.Rename can return nil without moving
	// the file (e.g. when src and dest resolve to the same path via
	// symlinks or junctions).  If the dest is missing, fall back to
	// the original location.
	if _, statErr := os.Stat(destPath); statErr != nil {
		ch.Error("output-dir: post-move stat of dest %s failed: %v — uploading from original location (%s)", destPath, statErr, resolvePathForLog(srcPath))
		MarkUploadDone(destPath) // release the failed-dest marker before marking src
		MarkUploadInFlight(srcPath)
		enqueue(srcPath)
		return srcPath
	}
	ch.Info("output-dir: moved %s -> %s", filepath.Base(srcPath), destPath)
	enqueue(destPath)
	return destPath
}

// resolvePathForLog resolves a path to its absolute form for logging.
// If resolution fails, returns the original path unchanged.
func resolvePathForLog(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

func (ch *Channel) generatePreviewAndUpload(filePath string) {
	ch.PipelineQueue.EnqueueFile(filePath)
}

// uniqueDestPath returns path if it does not exist, otherwise appends
// " (n)" before the extension until an unused path is found.
func uniqueDestPath(path string) string {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}
	ext := filepath.Ext(path)
	base := path[:len(path)-len(ext)]
	for i := 1; i < 100000; i++ {
		candidate := fmt.Sprintf("%s (%d)%s", base, i, ext)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
	return fmt.Sprintf("%s (99999)%s", base, ext)
}

func moveFile(src, dest string) error {
	// Retry rename with backoff for transient Windows locks (AV, Search Indexer, etc.).
	for i := 0; i < 3; i++ {
		if err := os.Rename(src, dest); err == nil {
			return nil
		}
		time.Sleep(time.Duration(50*(1<<i)) * time.Millisecond)
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		in.Close()
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		in.Close()
		out.Close()
		os.Remove(dest)
		return err
	}
	// Sync before close so a crash between close and os.Remove(src) can't
	// leave a truncated destination alongside a deleted source.
	if err := out.Sync(); err != nil {
		in.Close()
		out.Close()
		os.Remove(dest)
		return err
	}
	if err := out.Close(); err != nil {
		in.Close()
		os.Remove(dest)
		return err
	}
	// Close the source handle BEFORE removing the file.  On Windows,
	// DeleteFileW fails with ERROR_ACCESS_DENIED when any handle is
	// still open, so defer in.Close() would keep the file busy.
	in.Close()

	// Retry remove with backoff.  If it still fails, the copy succeeded —
	// the file was effectively moved.  The leftover source will be cleaned
	// up on the next orphan run.
	for i := 0; i < 5; i++ {
		if err := os.Remove(src); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// GenerateFilename creates a filename based on the configured pattern and the current timestamp
func (ch *Channel) GenerateFilename() (string, error) {
	var buf bytes.Buffer

	// Parse the filename pattern defined in the channel's config
	tpl, err := template.New("filename").Parse(ch.Config.Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error: %w", err)
	}

	// Get the current time based on the Unix timestamp when the stream was started
	t := time.Unix(ch.StreamedAt, 0)
	pattern := &Pattern{
		Username: ch.Config.Username,
		Sequence: ch.Sequence,
		Year:     t.Format("2006"),
		Month:    t.Format("01"),
		Day:      t.Format("02"),
		Hour:     t.Format("15"),
		Minute:   t.Format("04"),
		Second:   t.Format("05"),
	}

	if err := tpl.Execute(&buf, pattern); err != nil {
		return "", fmt.Errorf("template execution error: %w", err)
	}
	return buf.String(), nil
}

// CreateNewFile creates a new file for the channel using the given filename
func (ch *Channel) CreateNewFile(filename string) error {
	// Ensure the directory exists before creating the file
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	videoPath := ch.videoPath(filename)
	file, err := os.OpenFile(videoPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("cannot open file: %s: %w", filename, err)
	}
	ch.File = file

	if len(ch.InitSegment) > 0 {
		n, err := ch.File.Write(ch.InitSegment)
		if err != nil {
			ch.File.Close()
			ch.File = nil
			return fmt.Errorf("write init segment: %w", err)
		}
		ch.stateMu.Lock()
		ch.Filesize += n
		ch.stateMu.Unlock()
	}

	if ch.HasSeparateAudio {
		audioPath := ch.audioPath(filename)
		audioFile, err := os.OpenFile(audioPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
		if err != nil {
			_ = ch.File.Close()
			ch.File = nil
			return fmt.Errorf("cannot open audio file: %s: %w", filename, err)
		}
		ch.AudioFile = audioFile

		if len(ch.AudioInitSegment) > 0 {
			if _, err := ch.AudioFile.Write(ch.AudioInitSegment); err != nil {
				_ = ch.File.Close()
				_ = ch.AudioFile.Close()
				ch.File = nil
				ch.AudioFile = nil
				return fmt.Errorf("write audio init segment: %w", err)
			}
		}
	}

	return nil
}

func (ch *Channel) videoPath(filename string) string {
	if ch.HasSeparateAudio {
		return filename + ".video.mp4"
	}
	return filename + ".mp4"
}

func (ch *Channel) audioPath(filename string) string {
	return filename + ".audio.mp4"
}

func closeTrackedFile(file *os.File) (string, os.FileInfo, error) {
	if file == nil {
		return "", nil, nil
	}

	filename := file.Name()
	if err := file.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		file.Close()
		return filename, nil, fmt.Errorf("sync file: %w", err)
	}
	if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return filename, nil, fmt.Errorf("close file: %w", err)
	}

	fileInfo, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return filename, nil, fmt.Errorf("stat file: %w", err)
	}
	if fileInfo != nil && fileInfo.Size() == 0 {
		if err := os.Remove(filename); err != nil {
			return filename, nil, fmt.Errorf("remove zero file: %w", err)
		}
		fileInfo = nil
	}

	return filename, fileInfo, nil
}

// maybeDeferToPending checks whether min-duration is enabled and, if so,
// whether filePath is short enough to be deferred.  When the file should be
// deferred (or on probe failure — we'd rather be safe) it is moved into
// .pending/<user>/ and the function returns true so callers skip upload.
func MaybeDeferToPending(filePath string) bool {
	minDur := 0
	if server.Config != nil {
		minDur = server.Config.MinDurationBeforeUpload
	}
	if minDur <= 0 {
		return false // feature disabled — upload directly
	}

	username := extractUsernameFromFilename(filepath.Base(filePath))
	if username == "" {
		// Can't determine the user; fall back to "unknown"
		username = "unknown"
	}

	dur, err := VideoDurationSeconds(filePath)
	if err != nil {
		log.Printf("[cleanup] min-duration: could not probe %s (%v) — deferring to pending", filepath.Base(filePath), err)
		_ = moveToPendingDir(filePath, username)
		return true
	}

	if dur < float64(minDur) {
		log.Printf("[cleanup] min-duration: %s = %.1fs (< %ds) — deferring to pending",
			filepath.Base(filePath), dur, minDur)
		_ = moveToPendingDir(filePath, username)
		return true
	}

	return false // meets threshold — upload normally
}

// moveToPendingDir moves a file into the .pending/<username>/ directory.
// Acquires pendingDirMu so it cannot race with handleMinDurationAndMerge or
// processAllPendingSegments, which may call deletePendingSegments concurrently.
func moveToPendingDir(filePath, username string) error {
	pendingDirMu.Lock()
	defer pendingDirMu.Unlock()

	pendingDir := pendingSegmentsDir(username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}
	dest := filepath.Join(pendingDir, filepath.Base(filePath))
	return os.Rename(filePath, dest)
}

// CleanupOrphanedFiles processes orphaned sidecar files left behind by
// cancelled or crashed post-processing runs. Instead of deleting them,
// it runs them through the full pipeline: mux (if split A/V), generate
// thumbnails, upload to hosts, save metadata to Supabase, then delete.
func CleanupOrphanedFiles() {
	if server.Config == nil {
		return
	}

	dirs := []string{"videos"}
	if server.Config.OutputDir != "" {
		dirs = append(dirs, server.Config.OutputDir)
	}

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		// Classify files by type
		type fileInfo struct {
			path string
			name string
		}
		mainVideos := map[string]fileInfo{} // stem -> info
		muxedFiles := map[string]fileInfo{} // stem -> info
		videoParts := map[string]fileInfo{} // stem -> info (.video.mp4)
		audioParts := map[string]fileInfo{} // stem -> info (.audio.mp4)

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			path := filepath.Join(dir, name)
			ext := strings.ToLower(filepath.Ext(name))

			switch {
			case strings.HasSuffix(name, ".video.muxed.mp4"):
				stem := strings.TrimSuffix(name, ".video.muxed.mp4")
				muxedFiles[stem] = fileInfo{path, name}
			case strings.HasSuffix(name, ".video.mp4"):
				stem := strings.TrimSuffix(name, ".video.mp4")
				videoParts[stem] = fileInfo{path, name}
			case strings.HasSuffix(name, ".audio.mp4"):
				stem := strings.TrimSuffix(name, ".audio.mp4")
				audioParts[stem] = fileInfo{path, name}
			case (ext == ".mp4" || ext == ".mkv" || ext == ".ts") &&
				!strings.Contains(name, ".video.") &&
				!strings.Contains(name, ".audio.") &&
				!strings.Contains(name, ".muxed.") &&
				!strings.Contains(name, ".preview."):
				stem := strings.TrimSuffix(name, filepath.Ext(name))
				mainVideos[stem] = fileInfo{path, name}
			}
		}

		// Process orphaned muxed files (output from a mux that was never uploaded)
		sem := make(chan struct{}, 5)
		for stem, info := range muxedFiles {
			if _, hasMain := mainVideos[stem]; hasMain {
				continue
			}
			stem, info := stem, info
			sem <- struct{}{}
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[PANIC] processing orphaned muxed file %s: %v", info.name, r)
					}
					<-sem
				}()
				recoveryLogf(info.name, "processing orphaned muxed file %s", info.name)

				// Check journal to skip files that were already fully uploaded
				if IsAlreadyFullyUploaded(info.path) {
					recoveryLogf(info.name, "all hosts already have this file per journal — removing local copy")
					os.Remove(info.path)
					DeleteSidecarFiles(info.path)
					_ = stem
					return
				}

				if MaybeDeferToPending(info.path) {
					_ = stem
					return
				}
				thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(info.path)
				UploadOrphanedFile(info.path, thumbURL, spriteURL, previewURL)
				DeleteSidecarFiles(info.path)
				_ = stem
			}()
		}

		// Process orphaned split A/V pairs (mux them first, then upload)
		for stem, vInfo := range videoParts {
			if _, hasMain := mainVideos[stem]; hasMain {
				continue
			}
			aInfo, hasAudio := audioParts[stem]
			if !hasAudio {
				// No matching audio sidecar — this video part is stale.
				// If a muxed result exists for this stem, delete the stale video part.
				if _, hasMuxed := muxedFiles[stem]; hasMuxed {
					recoveryLogf(vInfo.name, "recovery: deleting stale video sidecar %s (muxed version exists)", vInfo.name)
					os.Remove(vInfo.path)
					continue
				}
				// No muxed result either — upload the video part on its own.
				if !MaybeDeferToPending(vInfo.path) {
					thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(vInfo.path)
					UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL)
				}
				DeleteSidecarFiles(vInfo.path)
				continue
			}

			stem, vInfo, aInfo := stem, vInfo, aInfo
			sem <- struct{}{}
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[PANIC] muxing orphaned split A/V pair %s: %v", stem, r)
					}
					<-sem
				}()

				// Mux the pair
				muxedPath := filepath.Join(dir, stem+".video.muxed.mp4")
				recoveryLogf(vInfo.name, "recovery: muxing orphaned split A/V pair %s", stem)
				if err := muxVideoAudio(vInfo.path, aInfo.path, muxedPath); err != nil {
					recoveryLogf(vInfo.name, "recovery: mux failed for %s: %v — uploading video-only", stem, err)
					// Fall back to uploading just the video track
					if !MaybeDeferToPending(vInfo.path) {
						thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(vInfo.path)
						UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL)
					}
					DeleteSidecarFiles(vInfo.path)
					return
				}

				// Delete source sidecars
				os.Remove(vInfo.path)
				os.Remove(aInfo.path)

				// Generate thumbnails, upload, and clean up
				if !MaybeDeferToPending(muxedPath) {
					thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(muxedPath)
					UploadOrphanedFile(muxedPath, thumbURL, spriteURL, previewURL)
				}
				DeleteSidecarFiles(muxedPath)
				os.Remove(muxedPath)
			}()
		}

		// Wait for all orphan processing to complete
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}

		// Process any pending segments (short videos awaiting merge).
		// Pending segments are stored under .pending/{username}/.
		processAllPendingSegments()

		// Clean up orphaned sidecar files whose main video no longer exists
		sidecarExts := []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			path := filepath.Join(dir, name)
			for _, suffix := range sidecarExts {
				if !strings.HasSuffix(name, suffix) {
					continue
				}
				base := strings.TrimSuffix(name, suffix)
				hasMain := false
				for ext := range map[string]bool{".mp4": true, ".mkv": true, ".ts": true} {
					if _, ok := mainVideos[base+ext]; ok {
						hasMain = true
						break
					}
				}
				if !hasMain {
					os.Remove(path)
				}
				break
			}
		}
	}
}

// DeleteSidecarFiles removes preview sidecar files associated with a video path.
func DeleteSidecarFiles(videoPath string) {
	for _, suffix := range []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"} {
		os.Remove(videoPath + suffix)
	}
}

// muxVideoAudio combines a separate video and audio file into a single MP4.
// Uses a 5-minute timeout so a hung ffmpeg cannot leak the caller's goroutine.
func muxVideoAudio(videoPath, audioPath, outputPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := config.FFmpegCommandContext(ctx, "-y",
		"-i", videoPath,
		"-i", audioPath,
		"-c", "copy",
		"-movflags", "+faststart",
		outputPath,
	)
	return cmd.Run()
}

// normalizeFMP4Timestamps remuxes an fMP4 recording to reset the timeline.
// Stripchat's LL-HLS segments carry absolute server timestamps (e.g. start at
// 5044s), which makes the file appear hours long.  A fast stream-copy remux
// with -movflags +faststart normalises the timestamps and moves the moov atom
// to the front for immediate playback.  The original file is replaced.
func normalizeFMP4Timestamps(videoPath string) (string, error) {
	tmpPath := videoPath + ".normalized.mp4"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	config.AcquireFFmpeg()
	defer config.ReleaseFFmpeg()
	err := config.FFmpegCommandContext(ctx,
		"-y",
		"-i", videoPath,
		"-c", "copy",
		"-movflags", "+faststart",
		tmpPath,
	).Run()
	if err != nil {
		os.Remove(tmpPath)
		return videoPath, err
	}
	if err := os.Rename(tmpPath, videoPath); err != nil {
		os.Remove(tmpPath)
		return videoPath, err
	}
	return videoPath, nil
}

// extractUsernameFromFilename parses "username_YYYY-MM-DD_HH-MM-SS.ext" to get the username.
// Uses a tighter pattern: looks for "_20" followed immediately by two digits, a hyphen, two digits,
// a hyphen, and two digits (i.e. the date portion YYYY-MM-DD).  This avoids false matches when
// a username itself contains "_20" (e.g. "alice_20_fan_2025-01-01...").
func extractUsernameFromFilename(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Strip "merged-" prefix that the merge system prepends.
	stem := strings.TrimPrefix(base, "merged-")

	// Find the timestamp separator (_YYYY-MM-DD_ or _YYYY-MM-DD-)
	idx := strings.Index(stem, "_20")
	if idx < 0 {
		return ""
	}
	rest := stem[idx+1:]
	if len(rest) < 10 || rest[4] != '-' || rest[7] != '-' {
		return ""
	}

	candidate := stem[:idx]

	// Deduplicate: merged filenames become "<user>-<user>" via the merge
	// system.  Usernames may contain hyphens (e.g. "Awesome-sona"), so we
	// try every split point and check whether left == right.
	if hyphen := strings.Index(candidate, "-"); hyphen > 0 {
		rightSide := candidate[hyphen+1:]
		if candidate[:hyphen] == rightSide {
			return candidate[:hyphen]
		}
		// Username might contain a hyphen — try later split positions.
		for h := strings.Index(candidate[hyphen+1:], "-"); h >= 0; h = strings.Index(candidate[hyphen+1:], "-") {
			hyphen += 1 + h
			rightSide = candidate[hyphen+1:]
			if candidate[:hyphen] == rightSide {
				return candidate[:hyphen]
			}
		}
	}

	return candidate
}

// extractTimestampFromFilename parses the standard recording timestamp from a
// filename like "username_2025-01-01_12-00-00.mp4" and returns it in Supabase
// format ("2025-01-01T12:00:00Z").  Returns "" if the pattern is not found.
func extractTimestampFromFilename(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	if idx := strings.Index(base, "_20"); idx > 0 {
		ts := base[idx+1:]
		if len(ts) >= 19 && ts[4] == '-' && ts[7] == '-' && ts[10] == '_' && ts[13] == '-' && ts[16] == '-' {
			return ts[:10] + "T" + ts[11:13] + ":" + ts[14:16] + ":" + ts[17:19] + "Z"
		}
	}
	return ""
}

// recoveryLogf logs to both stdout and the channel's SSE log stream if the
// file can be associated with an active channel via its Manager.
func recoveryLogf(filename, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	username := extractUsernameFromFilename(filename)
	log.Printf("recovery [%s]: %s", username, msg)
	if m := server.Manager; m != nil && username != "" {
		m.PublishLog(username, "[recovery] "+msg)
	}
}

// isAlreadyFullyUploaded checks the upload journal to determine if a file has
// been successfully uploaded to all configured hosts.
func IsAlreadyFullyUploaded(filePath string) bool {
	if !server.RecordingExists(filepath.Base(filePath)) {
		return false
	}
	fileHash, err := internal.FastFileHash(filePath)
	if err != nil || fileHash == "" {
		return false
	}
	completed, err := server.LoadCompletedHosts(fileHash)
	if err != nil {
		return false
	}
	// Build the set of all configured hosts
	hosts := configuredUploadHosts()
	if len(hosts) == 0 {
		return false
	}
	done := make(map[string]bool, len(completed))
	for _, h := range completed {
		done[h] = true
	}
	for _, h := range hosts {
		if !done[h] {
			return false
		}
	}
	return true
}

// configuredUploadHosts returns the list of upload hosts that have their
// API keys configured in the server config.
func configuredUploadHosts() []string {
	cfg := server.Config
	if cfg == nil {
		return nil
	}
	var hosts []string
	hosts = append(hosts, "GoFile")
	if cfg.VoeSXAPIKey != "" {
		hosts = append(hosts, "VOE.sx")
	}
	if cfg.StreamtapeLogin != "" && cfg.StreamtapeKey != "" {
		hosts = append(hosts, "Streamtape")
	}
	if cfg.MixdropEmail != "" && cfg.MixdropToken != "" {
		hosts = append(hosts, "Mixdrop")
	}
	return hosts
}

// UploadOrphanedFile uploads a file to all configured hosts and saves metadata
// to Supabase. Unlike Channel.uploadFile, this doesn't require an active channel.
// Username is extracted from the filename; metadata fields are left empty.
//
// If every configured host fails on the first attempt, it retries up to 2 more
// times with a 60-second delay between attempts.  This handles transient network
// or API outages that can occur when the app restarts after a crash.
func UploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL string) bool {
	MarkUploadInFlight(filePath)
	defer MarkUploadDone(filePath)
	cfg := server.Config
	if cfg == nil {
		return false
	}

	UploadSem <- struct{}{}
	defer func() { <-UploadSem }()

	filename := filepath.Base(filePath)

	recoveryLogf(filename, "uploading %s", filename)

	// Compute file hash for upload journal
	fileHash, hashErr := internal.FastFileHash(filePath)
	if hashErr != nil {
		recoveryLogf(filename, "could not hash (journal skipped): %v", hashErr)
	}

	// Load completed hosts from journal
	var completedHosts []string
	if fileHash != "" {
		var loadErr error
		completedHosts, loadErr = server.LoadCompletedHosts(fileHash)
		if loadErr != nil {
			recoveryLogf(filename, "could not load journal: %v", loadErr)
		}
	}

	// Save preview links first
	if thumbURL != "" || spriteURL != "" || previewURL != "" {
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL); err != nil {
			recoveryLogf(filename, "could not save preview links: %v", err)
		}
	}

	// Upload to all configured hosts — retry up to 3 times if all hosts fail.
	const maxUploadAttempts = 3
	const retryDelay = 60 * time.Second

	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.SeekStreamingKey,
		nil, // no logger for orphan recovery
	)

	allHosts := upl.AvailableHosts()

	// Determine which hosts still need the file
	hostsToTry := allHosts
	if len(completedHosts) > 0 {
		hostsToTry = difference(allHosts, completedHosts)
		if len(hostsToTry) == 0 {
			if server.RecordingExists(filename) {
				recoveryLogf(filename, "all hosts already have this file per journal — skipping upload")
				return true
			}
			recoveryLogf(filename, "stale journal has no Supabase recording; clearing journal and re-uploading")
			if fileHash != "" {
				if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
					recoveryLogf(filename, "could not clear stale journal: %v", jErr)
				}
			}
			completedHosts = nil
			hostsToTry = allHosts
		}
		recoveryLogf(filename, "%d/%d hosts already have this file — uploading to %d remaining",
			len(completedHosts), len(allHosts), len(hostsToTry))
	}

	var results []uploader.UploadResult
	var success []uploader.UploadResult
	for attempt := 1; attempt <= maxUploadAttempts; attempt++ {
		if attempt > 1 && len(hostsToTry) == 0 {
			break
		}
		attemptResults := upl.UploadSelected(filePath, hostsToTry)
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
					recoveryLogf(filename, "could not save journal for %s: %v", r.Host, jErr)
				}
			}
		}

		success = uploader.GetSuccessfulUploads(results)
		recoveryLogf(filename, "upload attempt %d/%d — %d/%d successful", attempt, maxUploadAttempts, len(success), len(allHosts))
		if len(success) >= len(allHosts) {
			break
		}

		if attempt < maxUploadAttempts {
			failedHosts := failedHostNames(results, completedHosts)
			hostsToTry = failedHosts
			if len(hostsToTry) > 0 {
				recoveryLogf(filename, "%d hosts still pending — retrying in %s...", len(hostsToTry), retryDelay)
				time.Sleep(retryDelay)
			}
		}
	}

	if len(success) == 0 {
		recoveryLogf(filename, "[WARN] all upload attempts exhausted — file will be retried on next restart")
		return false
	}

	// Build links map
	links := map[string]string{}
	var embedURL string
	for _, r := range success {
		links[r.Host] = r.DownloadLink
		if embedURL == "" {
			embedURL = embedURLFromLink(r.Host, r.DownloadLink)
		}
	}

	stat, _ := os.Stat(filePath)
	var filesize int64
	if stat != nil {
		filesize = stat.Size()
	}

	timestamp := extractTimestampFromFilename(filename)
	if timestamp == "" {
		timestamp = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}

	dur, probeErr := VideoDurationSeconds(filePath)
	if probeErr != nil {
		recoveryLogf(filename, "could not probe duration: %v", probeErr)
	}

	dbSaved := false
	if err := server.SaveRecordingWithLinks(
		extractUsernameFromFilename(filename), filename, timestamp,
		"", nil, 0, "", 0, filesize, dur, "", embedURL, thumbURL, spriteURL, previewURL, links,
	); err != nil {
		recoveryLogf(filename, "failed to save recording to Supabase: %v", err)
		if fileHash != "" {
			if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
				recoveryLogf(filename, "could not delete journal after DB failure: %v", jErr)
			}
		}
	} else {
		dbSaved = true
		recoveryLogf(filename, "saved recording metadata")
	}

	// Delete local file only once ALL hosts have the file safely and metadata
	// is persisted. Otherwise the file remains available for retry.
	if cfg.DeleteLocalAfterUpload && len(success) > 0 && dbSaved {
		os.Remove(filePath)
		DeleteSidecarFiles(filePath)
		if fileHash != "" {
			if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
				recoveryLogf(filename, "could not delete journal: %v", jErr)
			}
		}
		recoveryLogf(filename, "removed local file")
	}

	return true
}

// pendingSegmentsDir returns the directory where short video segments are stored
// awaiting merge with the next recording.  A subdirectory per channel keeps
// segments from different models separate.
func pendingSegmentsDir(username string) string {
	dir := "videos"
	if server.Config != nil && server.Config.OutputDir != "" {
		dir = server.Config.OutputDir
	}
	return filepath.Join(dir, ".pending", username)
}

// collectPendingSegments returns sorted absolute paths of all pending segments
// for a given channel.
func collectPendingSegments(username string) []string {
	dir := pendingSegmentsDir(username)
	return collectPendingSegmentsInDir(dir)
}

// collectPendingSegmentsInDir returns sorted absolute paths of actual video
// files in dir, filtering out sidecar files, zero-byte files, and
// merged-*.mp4 files that were already consolidated.
func collectPendingSegmentsInDir(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Skip sidecar files — they are not video segments.
		if isSidecar(name) {
			continue
		}
		// Skip zero-byte files — they are corrupt/empty segments.
		info, err := e.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		paths = append(paths, filepath.Join(dir, name))
	}
	sort.Strings(paths)
	return paths
}

// deletePendingSegments removes all pending segments for a channel and cleans
// up the (now empty) directory.
func deletePendingSegments(username string) {
	dir := pendingSegmentsDir(username)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	_ = os.Remove(dir)
}

// VideoDurationSeconds probes a video file and returns its duration in seconds.
// Falls back to parsing ffmpeg stderr output when ffprobe is unavailable or fails.
func VideoDurationSeconds(videoPath string) (float64, error) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer probeCancel()
	config.AcquireFFmpeg()
	defer config.ReleaseFFmpeg()
	out, err := config.FFprobeCommandContext(probeCtx,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		videoPath,
	).Output()
	if err == nil {
		dur, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if parseErr == nil {
			return dur, nil
		}
	}

	// Fallback: ask ffmpeg to decode null and parse "Duration:" from stderr.
	fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer fallbackCancel()
	cmd := config.FFmpegCommandContext(fallbackCtx, "-i", videoPath, "-f", "null", "-")
	stderr, fbErr := cmd.CombinedOutput()
	if fbErr == nil && len(stderr) > 0 {
		line := string(stderr)
		const durationPrefix = "Duration: "
		if idx := strings.Index(line, durationPrefix); idx >= 0 {
			rest := line[idx+len(durationPrefix):]
			if end := strings.IndexAny(rest, "., "); end > 0 {
				rest = rest[:end]
			}
			parts := strings.SplitN(rest, ":", 3)
			if len(parts) == 3 {
				hours, _ := strconv.ParseFloat(parts[0], 64)
				minutes, _ := strconv.ParseFloat(parts[1], 64)
				seconds, _ := strconv.ParseFloat(parts[2], 64)
				if hours >= 0 || minutes >= 0 || seconds >= 0 {
					return hours*3600 + minutes*60 + seconds, nil
				}
			}
		}
	}

	if err != nil {
		return 0, fmt.Errorf("probe %s: %w", filepath.Base(videoPath), err)
	}
	return 0, fmt.Errorf("probe %s: could not parse duration from ffprobe or ffmpeg", filepath.Base(videoPath))
}

// mergeVideos concatenates multiple video files into a single output using the
// ffmpeg concat demuxer, then normalizes timestamps.  All source files must
// share the same codec parameters (same encoding, resolution, etc.) for a
// plain stream-copy to work.
//
// The post-merge normalization is critical: pending segments are raw fMP4 with
// absolute server timestamps (e.g. PTS=5044s) from LL-HLS streams.  The concat
// demuxer copies packets verbatim, so the merged output inherits those broken
// timestamps -> laggy playback, wrong duration, corrupt seeking.
// A fast stream-copy remux with -fflags +genpts regenerates PTS from DTS.
func mergeVideos(inputs []string, outputPath string) error {
	listFile, err := os.CreateTemp("", "concat-*.txt")
	if err != nil {
		return fmt.Errorf("create concat list: %w", err)
	}
	defer os.Remove(listFile.Name())

	for _, p := range inputs {
		abs, aErr := filepath.Abs(p)
		if aErr != nil {
			abs = p
		}
		// Escape single quotes for ffmpeg's concat demuxer.
		escaped := strings.ReplaceAll(abs, "'", "'\\''")
		if _, wErr := fmt.Fprintf(listFile, "file '%s'\n", escaped); wErr != nil {
			listFile.Close()
			return fmt.Errorf("write concat list: %w", wErr)
		}
	}
	listFile.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	config.AcquireFFmpegHeavy()
	defer config.ReleaseFFmpegHeavy()

	// Step 1: Concat merge (stream copy).
	if err := config.FFmpegCommandContext(ctx,
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		"-fflags", "+genpts",
		"-movflags", "+faststart",
		outputPath,
	).Run(); err != nil {
		os.Remove(outputPath)
		return fmt.Errorf("merge video: %w", err)
	}

	// Step 2: Normalize timestamps.  The concat step preserves the input
	// segments' original fMP4 absolute timestamps (e.g. PTS=5044s from
	// LL-HLS).  A stream-copy remux with -fflags +genpts regenerates clean
	// PTS values starting from 0, so the output plays correctly everywhere.
	tmpPath := outputPath + ".normalized.mp4"
	if err := config.FFmpegCommandContext(ctx,
		"-y",
		"-i", outputPath,
		"-c", "copy",
		"-fflags", "+genpts",
		"-movflags", "+faststart",
		tmpPath,
	).Run(); err != nil {
		os.Remove(tmpPath)
		// If normalization fails, return the concat output as-is (best-effort).
		return nil
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		os.Remove(tmpPath)
	}
	return nil
}

// handleMinDurationAndMerge checks whether a finalized video file meets the
// minimum-duration threshold.  If the feature is disabled the check is skipped
// and the caller proceeds to upload normally.  Callers should skip this
// function entirely when skipMinDuration is set (channel pause).
//
// When a video is shorter than the threshold it is moved into a pending
// directory.  If pending segments already exist (including the one just moved),
// they are merged together and the merged result is uploaded via
// MoveToOutputDir.
//
// Returns true if the video was handled (deferred to pending or merged+uploaded)
// so the caller should stop processing it.  Returns false when the caller
// should proceed with its normal upload logic.
func (ch *Channel) handleMinDurationAndMerge(videoPath string) bool {
	pendingDirMu.Lock()

	minDur := ch.Config.MinDurationBeforeUpload
	if minDur <= 0 {
		if server.Config != nil && server.Config.MinDurationBeforeUpload > 0 {
			minDur = server.Config.MinDurationBeforeUpload
		} else {
			pendingDirMu.Unlock()
			return false // feature disabled — proceed normally
		}
	}

	dur, err := VideoDurationSeconds(videoPath)
	if err != nil {
		ch.Warn("min-duration: could not probe %s: %v — deferring to pending", filepath.Base(videoPath), err)
		// On probe failure, keep the video pending rather than uploading a corrupt/short file
		pendingDir := pendingSegmentsDir(ch.Config.Username)
		if mErr := os.MkdirAll(pendingDir, 0777); mErr == nil {
			destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
			if rErr := os.Rename(videoPath, destPath); rErr == nil {
				pendingDirMu.Unlock()
				return true
			}
		}
		pendingDirMu.Unlock()
		return false
	}

	if dur >= float64(minDur) {
		// Video is long enough. Before uploading, check if there are
		// pending segments to merge with.
		segments := collectPendingSegments(ch.Config.Username)
		if len(segments) == 0 {
			pendingDirMu.Unlock()
			return false // no pending — proceed normally
		}

		// Merge pending segments with the current video.
		// Release the lock during the potentially long ffmpeg encode.
		mergedPath := videoPath + ".merged.mp4"
		// Snapshot the segment paths we intend to merge before releasing the lock,
		// so deletePendingSegments below only removes these exact files.
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		allInputs := append(mergeInputs, videoPath)
		ch.Info("min-duration: merging %d pending segment(s) with %s", len(mergeInputs), filepath.Base(videoPath))
		pendingDirMu.Unlock()
		mErr := mergeVideos(allInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			ch.Error("min-duration: merge failed: %v — uploading current video separately, clearing pending segments", mErr)
			// Remove the stale pending segments so they don't accumulate.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			return false
		}

		// Verify the merged output duration — fMP4 timestamp issues can
		// cause the concat merge to produce a valid file that is shorter
		// than the sum of its parts.
		mergedDur, probeErr := VideoDurationSeconds(mergedPath)
		if probeErr != nil || mergedDur < float64(minDur)*0.9 {
			ch.Warn("min-duration: merged output for %s is %.1fs (expected >= %ds) — uploading current video separately",
				filepath.Base(mergedPath), mergedDur, minDur)
			os.Remove(mergedPath)
			// Remove the stale pending segments.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			return false
		}

		// Re-acquire lock to remove only the segments we merged.
		pendingDirMu.Lock()
		for _, s := range mergeInputs {
			os.Remove(s)
		}
		_ = os.Remove(videoPath)
		pendingDirMu.Unlock()

		if ch.Config.Compress {
			ch.Info("min-duration: merged -> %s (%.1fs), compressing before upload", filepath.Base(mergedPath), mergedDur)
			ch.CompressFile(mergedPath)
		} else {
			ch.Info("min-duration: merged -> %s (%.1fs), proceeding with upload", filepath.Base(mergedPath), mergedDur)
			ch.MoveToOutputDir(mergedPath)
		}
		return true
	}

	// Video is too short — move to pending
	pendingDir := pendingSegmentsDir(ch.Config.Username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		pendingDirMu.Unlock()
		ch.Error("min-duration: cannot create pending dir %s: %v — uploading short video", pendingDir, err)
		return false
	}

	destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
	if err := os.Rename(videoPath, destPath); err != nil {
		pendingDirMu.Unlock()
		ch.Error("min-duration: cannot move %s to pending: %v — uploading short video", filepath.Base(videoPath), err)
		return false
	}
	ch.Info("min-duration: %s is %.1fs (< %ds) — deferred to pending", filepath.Base(videoPath), dur, minDur)

	// If multiple segments have now accumulated, merge them and check the
	// combined duration. Only upload if the merged result meets the threshold.
	segments := collectPendingSegments(ch.Config.Username)
	if len(segments) > 1 {
		mergedPath := filepath.Join(pendingDir, "merged-"+filepath.Base(destPath))
		// Snapshot segment paths before releasing lock.
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		ch.Info("min-duration: merging %d pending segment(s)", len(mergeInputs))
		// Release lock during the ffmpeg encode so other channels aren't blocked.
		pendingDirMu.Unlock()
		mErr := mergeVideos(mergeInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			ch.Error("min-duration: merge failed: %v — segments remain pending for next recording", mErr)
			return true
		}
		pendingDirMu.Lock()

		// Check if the merged video is long enough
		mergedDur, mErr := VideoDurationSeconds(mergedPath)
		if mErr != nil {
			ch.Warn("min-duration: could not probe merged result, uploading anyway: %v", mErr)
			// Remove only the exact files we merged (not any files added during merge).
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			pendingDirMu.Unlock()
			ch.MoveToOutputDir(mergedPath)
			return true
		}

		if mergedDur >= float64(minDur) {
			// Remove only the exact files we merged.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			ch.Info("min-duration: merged %d segments = %.1fs (>= %ds) — uploading", len(mergeInputs), mergedDur, minDur)
			pendingDirMu.Unlock()

			if ch.Config.Compress {
				ch.CompressFile(mergedPath)
			} else {
				ch.MoveToOutputDir(mergedPath)
			}
		} else {
			// Still too short — move merged result back to pending as a single file
			ch.Info("min-duration: merged %d segments = %.1fs (< %ds) — still pending", len(mergeInputs), mergedDur, minDur)
			// Remove only the exact files we merged.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			mergedDest := filepath.Join(pendingDir, "merged-"+filepath.Base(destPath))
			if mErr := os.MkdirAll(pendingDir, 0777); mErr == nil {
				if rErr := os.Rename(mergedPath, mergedDest); rErr != nil {
					pendingDirMu.Unlock()
					ch.Error("min-duration: cannot keep merged result pending: %v — uploading anyway", rErr)
					ch.MoveToOutputDir(mergedPath)
					return true
				}
			} else {
				pendingDirMu.Unlock()
				ch.Error("min-duration: cannot recreate pending dir: %v — uploading anyway", mErr)
				ch.MoveToOutputDir(mergedPath)
				return true
			}
			pendingDirMu.Unlock()
		}
	} else {
		pendingDirMu.Unlock()
	}

	return true // video was deferred to pending (or merged+uploaded)
}

// processAllPendingSegments scans all .pending/* subdirectories and processes any
// accumulated segments.  If segments exist they are merged together and uploaded.
// This is called during startup orphan cleanup so short segments from a previous
// run don't stay pending forever when no new recording arrives.
func processAllPendingSegments() {
	minDur := 0
	if server.Config != nil {
		minDur = server.Config.MinDurationBeforeUpload
	}

	dirs := []string{"videos"}
	if server.Config != nil && server.Config.OutputDir != "" && server.Config.OutputDir != "videos" {
		dirs = append(dirs, server.Config.OutputDir)
	}
	for _, dir := range dirs {
		pendingRoot := filepath.Join(dir, ".pending")
		userDirs, err := os.ReadDir(pendingRoot)
		if err != nil {
			continue
		}
		for _, ud := range userDirs {
			if !ud.IsDir() {
				continue
			}
			username := ud.Name()

			// Lock per-user to avoid holding the global lock during ffmpeg calls.
			pendingDirMu.Lock()
			segments := collectPendingSegmentsInDir(filepath.Join(pendingRoot, username))
			if len(segments) < 1 {
				pendingDirMu.Unlock()
				continue
			}

			// If min-duration is disabled, upload everything directly (legacy behavior).
			if minDur <= 0 {
				for _, s := range segments {
					recoveryLogf(s, "recovery: uploading pending segment %s", filepath.Base(s))
					// Release lock during potentially long operations.
					pendingDirMu.Unlock()
					thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(s)
					UploadOrphanedFile(s, thumbURL, spriteURL, previewURL)
					_ = os.Remove(s)
					pendingDirMu.Lock()
				}
				_ = os.Remove(pendingSegmentsDir(username))
				pendingDirMu.Unlock()
				continue
			}

			// Min-duration is enabled — merge segments and only upload if threshold met.
			// Snapshot segments and release lock before ffmpeg.
			segCopy := make([]string, len(segments))
			copy(segCopy, segments)
			pendingDirMu.Unlock()

			mergedPath := filepath.Join(pendingSegmentsDir(username), "merged-"+filepath.Base(segments[0]))
			recoveryLogf(segments[0], "recovery: merging %d pending segments for %s", len(segments), username)
			if err := mergeVideos(segCopy, mergedPath); err != nil {
				os.Remove(mergedPath) // clean up partial output
				recoveryLogf(segments[0], "recovery: merge failed for %s: %v — leaving segments pending", username, err)
				continue
			}

			mergedDur, durErr := VideoDurationSeconds(mergedPath)
			pendingDir := pendingSegmentsDir(username)
			if durErr != nil || mergedDur < float64(minDur) {
				// Merged result still below threshold — keep it pending for next recording
				mergedName := "merged-" + filepath.Base(segments[0])
				if durErr != nil {
					recoveryLogf(mergedPath, "recovery: could not probe merged duration (%v) — keeping pending", durErr)
				} else {
					recoveryLogf(mergedPath, "recovery: merged = %.1fs (< %ds) — keeping pending", mergedDur, minDur)
				}
				pendingDirMu.Lock()
				// Remove only the exact files we merged (not any files added during merge).
				for _, s := range segCopy {
					os.Remove(s)
				}
				_ = os.MkdirAll(pendingDir, 0777)
				_ = os.Rename(mergedPath, filepath.Join(pendingDir, mergedName))
				pendingDirMu.Unlock()
				continue
			}

			// Sanity check: the merged output should not be significantly shorter
			// than the sum of input durations.  If it is, the concat demuxer may
			// have silently dropped incompatible streams (encoding mismatch across
			// different recording sessions), producing a shorter-than-expected file.
			var totalInputDur float64
			for _, s := range segCopy {
				if d, e := VideoDurationSeconds(s); e == nil {
					totalInputDur += d
				}
			}
			if totalInputDur > 0 && mergedDur < totalInputDur*0.5 {
				recoveryLogf(mergedPath, "recovery: merged output %.1fs is <50%% of total input %.1fs — streams may be incompatible, keeping pending",
					mergedDur, totalInputDur)
				pendingDirMu.Lock()
				for _, s := range segCopy {
					os.Remove(s)
				}
				_ = os.MkdirAll(pendingDir, 0777)
				_ = os.Rename(mergedPath, filepath.Join(pendingDir, "merged-"+filepath.Base(segments[0])))
				pendingDirMu.Unlock()
				continue
			}

			pendingDirMu.Lock()
			// Remove only the exact files we merged.
			for _, s := range segCopy {
				os.Remove(s)
			}
			pendingDirMu.Unlock()
			recoveryLogf(mergedPath, "recovery: merged = %.1fs (>= %ds) — uploading", mergedDur, minDur)
			thumbURL, spriteURL, previewURL := GenerateThumbnailForFile(mergedPath)
			UploadOrphanedFile(mergedPath, thumbURL, spriteURL, previewURL)
			_ = os.Remove(mergedPath)
		}
	}
}

// ShouldSwitchFile determines whether a new file should be created.
func (ch *Channel) ShouldSwitchFile() bool {
	maxFilesizeBytes := ch.Config.MaxFilesize * 1024 * 1024
	maxDurationSeconds := ch.Config.MaxDuration * 60

	ch.stateMu.Lock()
	dur := ch.Duration
	fsize := ch.Filesize
	ch.stateMu.Unlock()

	return (dur >= float64(maxDurationSeconds) && ch.Config.MaxDuration > 0) ||
		(fsize >= maxFilesizeBytes && ch.Config.MaxFilesize > 0)
}
