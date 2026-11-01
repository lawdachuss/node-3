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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var pendingDirMu sync.Map // map[string]*sync.Mutex, keyed by channel username

// pendingMu returns the per-channel mutex for the given username.
// Each channel has its own lock so one channel's ffmpeg encode does not
// block another channel's pending directory operations.
func pendingMu(username string) *sync.Mutex {
	v, _ := pendingDirMu.LoadOrStore(username, &sync.Mutex{})
	return v.(*sync.Mutex)
}

var mergeDirMu sync.Map // map[string]*sync.Mutex, keyed by channel username

// mergeMu returns the per-channel mutex that serializes pending-segment merges.
// handleMinDurationAndMerge and processAllPendingSegments can otherwise merge
// the SAME segments concurrently (channel rotation vs. the startup orphan
// scan), and both would renameOverwriting the shared stable "merged-*" name —
// deleting the file out from under the other's upload ("could not hash" /
// "0/5 successful"). Holding this lock for the whole merge lets both flows
// upload from the clean stable name instead of a unique scratch path.
func mergeMu(username string) *sync.Mutex {
	v, _ := mergeDirMu.LoadOrStore(username, &sync.Mutex{})
	return v.(*sync.Mutex)
}

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

// NextFile prepares the next file to be created, by cleaning up the last file
// and generating a new one. ext is the file extension to use (e.g. ".ts" or ".mp4").
func (ch *Channel) NextFile(ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	if err := ch.cleanupLocked(); err != nil {
		return err
	}
	filename, err := ch.generateFilenameLocked()
	if err != nil {
		return err
	}
	if err := ch.createNewFileLocked(filename, ext); err != nil {
		return err
	}

	// Increment the sequence number for the next file.
	ch.Sequence++
	return nil
}

// Cleanup closes any open recording files and queues them for post-processing.
// CloseProcess also starts processing the queue immediately (rotation, pause,
// stream error); CloseQueue defers processing to ProcessPending (session stop).
func (ch *Channel) Cleanup(mode CloseMode) error {
	ch.fileMu.Lock()
	err := ch.cleanupLocked()
	ch.fileMu.Unlock()
	if err != nil {
		return err
	}

	if mode == CloseProcess {
		ch.flushPending()
	}
	return nil
}

// flushPending starts async post-processing of all currently queued pending files.
func (ch *Channel) flushPending() {
	ch.cleanupMu.Lock()
	files := ch.pendingFiles
	ch.pendingFiles = nil
	ch.cleanupMu.Unlock()

	if len(files) == 0 {
		return
	}
	ch.Info("cleanup: processing %d pending file(s)", len(files))
	ch.pendingWg.Add(1)
	go func() {
		defer ch.pendingWg.Done()
		defer func() {
			if r := recover(); r != nil {
				ch.Error("flush pending panic recovered: %v", r)
			}
		}()
		for _, pf := range files {
			ch.processPendingFile(pf)
		}
	}()
}

// processPendingQueue processes all pending files synchronously.
// Caller must hold cleanupMu (see ProcessPending in channel.go).
func (ch *Channel) processPendingQueue() {
	if len(ch.pendingFiles) == 0 {
		return
	}
	ch.Info("cleanup: processing %d pending file(s)", len(ch.pendingFiles))
	files := ch.pendingFiles
	ch.pendingFiles = nil
	for _, pf := range files {
		ch.processPendingFile(pf)
	}
}

// processPendingFile finalizes a closed recording (seek index or ffmpeg),
// optionally relocates it to the completed dir, and routes it through the
// output pipeline (preview → upload → metadata → cleanup).
func (ch *Channel) processPendingFile(pf pendingFile) {
	videoPath := pf.videoPath
	if _, err := os.Stat(videoPath); err != nil {
		return
	}

	// goondvr-style finalization: BuildSeekIndex or ffmpeg remux/transcode.
	finalPath, err := ch.finalizeRecordingFile(videoPath)
	if err != nil {
		ch.Error("finalize %s: %s — keeping original recording", filepath.Base(videoPath), err.Error())
		finalPath = videoPath
	} else if finalPath != videoPath {
		// The finalizer produced a new file (e.g. .ts -> .mp4); drop the original.
		if rmErr := os.Remove(videoPath); rmErr != nil && !os.IsNotExist(rmErr) {
			ch.Error("remove original after finalization `%s`: %s", filepath.Base(videoPath), rmErr.Error())
		}
	}

	if _, err := os.Stat(finalPath); err != nil {
		return
	}

	// Join consecutive same-session recording cycles into one continuous video
	// (the HLS token refresh would otherwise split every ~20 minutes).  When
	// the file is consumed into a merge it is NOT uploaded individually here.
	if ch.trySessionMerge(finalPath, pf.endReason) {
		return
	}

	// If no output dir is configured, recordings can be relocated to the
	// completed dir (goondvr semantics) before the pipeline runs in place.
	if server.Config.OutputDir == "" && server.Config.CompletedDir != "" {
		if dst, err := moveRecordingToDir(finalPath, recordingDirFromPattern(ch.Config.Pattern), server.Config.CompletedDir); err != nil {
			ch.Error("move completed recording `%s`: %s", finalPath, err.Error())
		} else {
			finalPath = dst
		}
	}

	if ch.Config.Compress {
		if ch.handleMinDurationAndMerge(finalPath, pf.endReason) {
			return
		}
		ch.CompressFile(finalPath, pf.endReason)
	} else if ch.handleMinDurationAndMerge(finalPath, pf.endReason) {
		return
	} else {
		ch.MoveToOutputDir(finalPath, pf.endReason)
	}

	// Refresh the disk-usage counter after finalization changes file sizes.
	go ch.ScanTotalDiskUsage()
}

// finalizeRecordingFile post-processes a closed recording according to the
// configured FinalizeMode:
//   - "none":      only build the in-place seek index for fMP4 files
//   - "remux":     ffmpeg stream-copy remux (+faststart)
//   - "transcode": ffmpeg re-encode with the configured encoder/quality
func (ch *Channel) finalizeRecordingFile(filename string) (string, error) {
	if server.Config.FinalizeMode == "none" {
		if strings.HasSuffix(filename, ".mp4") {
			if err := chaturbate.BuildSeekIndex(filename); err != nil {
				ch.Error("seek index %s: %v", filename, err)
			}
		}
		return filename, nil
	}
	return ch.runFFmpegFinalizer(filename)
}

// cleanupLocked closes the active recording file, removes empty files, and
// queues non-empty files for post-processing. Callers must hold fileMu.
func (ch *Channel) cleanupLocked() error {
	if ch.File == nil {
		return nil
	}
	filename := ch.File.Name()
	endReason := ch.closeReason
	if endReason == "" {
		endReason = "unknown"
	}

	defer func() {
		ch.Filesize = 0
		ch.Duration = 0
		ch.closeReason = "" // consumed; next file starts with a fresh reason
	}()

	// Sync the file to ensure data is written to disk.
	if err := ch.File.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("sync file: %w", err)
	}
	if err := ch.File.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("close file: %w", err)
	}
	ch.File = nil
	ch.CurrentFilename = ""

	// Delete the empty file.
	fileInfo, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat file delete zero file: %w", err)
	}
	if fileInfo != nil && fileInfo.Size() == 0 {
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("remove zero file: %w", err)
		}
		go ch.ScanTotalDiskUsage()
	} else if fileInfo != nil {
		ch.cleanupMu.Lock()
		ch.pendingFiles = append(ch.pendingFiles, pendingFile{
			videoPath: filename,
			endReason: endReason,
		})
		ch.cleanupMu.Unlock()
		ch.Info("cleanup: queued %s for post-processing (ended: %s, duration: %s, size: %s)",
			filepath.Base(filename), endReason, internal.FormatDuration(ch.Duration), internal.FormatFilesize(ch.Filesize))
	}

	return nil
}

// GenerateFilename creates a filename based on the configured pattern and the current timestamp.
func (ch *Channel) GenerateFilename() (string, error) {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.generateFilenameLocked()
}

// LiveFileSize returns the current size of the active recording file in bytes
// (0 when nothing is being recorded).  Used by the session loop's early-drain
// estimate so live recordings — which close and join the upload queue the
// moment the drain stops channels — are counted.
func (ch *Channel) LiveFileSize() int64 {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()
	return int64(ch.Filesize)
}

// CurrentRecordingPath returns the absolute path of the file this channel is
// currently recording, or "" when no recording is active.  The orphan scan
// walks the same directory (videos/ and OutputDir) and must never treat a
// live recording as a stranded orphan.
func (ch *Channel) CurrentRecordingPath() string {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	if ch.File == nil || ch.CurrentFilename == "" {
		return ""
	}
	return filepath.Clean(ch.CurrentFilename + ch.FileExt)
}

func (ch *Channel) generateFilenameLocked() (string, error) {
	var buf bytes.Buffer

	// Parse the filename pattern defined in the channel's config.
	tpl, err := template.New("filename").Parse(ch.Config.Pattern)
	if err != nil {
		return "", fmt.Errorf("filename pattern error: %w", err)
	}

	// Get the current time based on the Unix timestamp when the stream was started.
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

// CreateNewFile creates a new file for the channel using the given filename and extension.
func (ch *Channel) CreateNewFile(filename, ext string) error {
	ch.fileMu.Lock()
	defer ch.fileMu.Unlock()

	return ch.createNewFileLocked(filename, ext)
}

func (ch *Channel) createNewFileLocked(filename, ext string) error {
	// Ensure the directory exists before creating the file.
	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return fmt.Errorf("mkdir all: %w", err)
	}

	// Open the file in append mode, create it if it doesn't exist.
	file, err := os.OpenFile(filename+ext, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("cannot open file: %s: %w", filename, err)
	}

	ch.File = file
	ch.CurrentFilename = filename
	return nil
}

// recordingDirFromPattern extracts the base directory from a filename pattern
// like "videos/{{.Username}}_..._..." → "videos".
func recordingDirFromPattern(pattern string) string {
	idx := strings.Index(pattern, "{{")
	if idx == -1 {
		return "."
	}
	dir := filepath.Dir(pattern[:idx])
	if dir == "" || dir == "." {
		return "."
	}
	return dir
}

func finalOutputExt(filename string) string {
	if server.Config.FFmpegContainer == "mkv" {
		return ".mkv"
	}
	if server.Config.FinalizeMode == "none" {
		return filepath.Ext(filename)
	}
	return ".mp4"
}

func finalOutputPath(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	return base + finalOutputExt(filename)
}

// ScanTotalDiskUsage calculates the total bytes of all recordings for this
// channel by walking the recording directory for files whose name starts with
// the username. The result is stored in TotalDiskUsageBytes.
func (ch *Channel) ScanTotalDiskUsage() {
	recordingDir := filepath.Clean(recordingDirFromPattern(ch.Config.Pattern))
	prefix := ch.Config.Username
	var total int64
	_ = filepath.WalkDir(recordingDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), prefix) {
			if info, err2 := d.Info(); err2 == nil {
				total += info.Size()
			}
		}
		return nil
	})
	ch.fileMu.Lock()
	ch.TotalDiskUsageBytes = total
	ch.fileMu.Unlock()
}

// ShouldSwitchFile determines whether a new file should be created.
func (ch *Channel) ShouldSwitchFile() bool {
	ch.fileMu.RLock()
	defer ch.fileMu.RUnlock()

	return ch.shouldSwitchFileLocked()
}

func (ch *Channel) shouldSwitchFileLocked() bool {
	maxFilesizeBytes := ch.Config.MaxFilesize * 1024 * 1024
	maxDurationSeconds := ch.Config.MaxDuration * 60

	return (ch.Duration >= float64(maxDurationSeconds) && ch.Config.MaxDuration > 0) ||
		(ch.Filesize >= maxFilesizeBytes && ch.Config.MaxFilesize > 0)
}

// isMP4InitSegment reports whether b looks like an fMP4 init segment containing
// top-level ftyp/moov boxes and no media fragments yet.
func isMP4InitSegment(b []byte) bool {
	var hasFtyp bool
	var hasMoov bool

	for pos := 0; pos+8 <= len(b); {
		size := int(uint32(b[pos])<<24 | uint32(b[pos+1])<<16 | uint32(b[pos+2])<<8 | uint32(b[pos+3]))
		if size < 8 || pos+size > len(b) {
			return false
		}

		switch string(b[pos+4 : pos+8]) {
		case "ftyp":
			hasFtyp = true
		case "moov":
			hasMoov = true
		case "moof", "mdat", "mfra":
			return false
		}
		pos += size
	}

	return hasFtyp && hasMoov
}

func moveRecordingToDir(src, recordingRoot, completedDir string) (string, error) {
	dstDir := completedDir

	srcDir := filepath.Dir(src)
	cleanRoot := filepath.Clean(recordingRoot)
	cleanSrcDir := filepath.Clean(srcDir)
	if relDir, err := filepath.Rel(cleanRoot, cleanSrcDir); err == nil && relDir != ".." && !strings.HasPrefix(relDir, ".."+string(os.PathSeparator)) {
		if relDir != "." {
			dstDir = filepath.Join(completedDir, relDir)
		}
	}

	if err := os.MkdirAll(dstDir, 0777); err != nil {
		return "", fmt.Errorf("mkdir completed dir: %w", err)
	}

	dst := filepath.Join(dstDir, filepath.Base(src))
	if src == dst {
		return dst, nil
	}

	if err := os.Rename(src, dst); err == nil {
		return dst, nil
	} else if !isCrossDeviceRename(err) {
		return "", fmt.Errorf("rename completed file: %w", err)
	}

	if err := copyFile(src, dst); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", fmt.Errorf("remove source after copy: %w", err)
	}
	return dst, nil
}

func isCrossDeviceRename(err error) bool {
	linkErr := &os.LinkError{}
	return errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync destination file: %w", err)
	}
	return nil
}

func (ch *Channel) runFFmpegFinalizer(filename string) (string, error) {
	outExt := finalOutputExt(filename)
	finalPath := finalOutputPath(filename)
	tempOutput := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".finalizing" + outExt
	_ = os.Remove(tempOutput)

	// buildArgs assembles the ffmpeg arguments for the current FinalizeMode.
	// tolerant enables the corrupt-tail recovery flags (-err_detect ignore_err
	// and -fflags +genpts+igndts) so ffmpeg skips damaged/truncated packets
	// instead of aborting.  This rescues recordings whose final HLS fragment
	// was cut short when the stream or session ended.
	buildArgs := func(tolerant bool) []string {
		args := []string{"-nostdin", "-y"}
		if tolerant {
			args = append(args, "-err_detect", "ignore_err", "-fflags", "+genpts+igndts")
		}
		args = append(args, "-i", filename)
		switch server.Config.FinalizeMode {
		case "remux":
			args = append(args, "-c", "copy")
		case "transcode":
			encoder := strings.TrimSpace(server.Config.FFmpegEncoder)
			if encoder == "" {
				encoder = "libx264"
			}
			args = append(args, "-c:v", encoder)
			args = append(args, qualityArgsForEncoder(encoder, server.Config.FFmpegQuality)...)
			if preset := strings.TrimSpace(server.Config.FFmpegPreset); preset != "" {
				args = append(args, "-preset", preset)
			}
			args = append(args, "-c:a", "copy")
		default:
			return nil // unsupported mode — handled before any command runs
		}
		if outExt == ".mp4" {
			args = append(args, "-movflags", "+faststart")
		}
		args = append(args, tempOutput)
		return args
	}

	if buildArgs(false) == nil {
		return "", fmt.Errorf("unsupported finalization mode %q", server.Config.FinalizeMode)
	}

	// run executes one finalization pass with its own full 30-minute budget,
	// so the rescue pass never inherits a nearly-expired context.
	run := func(tolerant bool) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		return config.FFmpegCommandContext(ctx, buildArgs(tolerant)...).CombinedOutput()
	}

	ch.Info("running ffmpeg %s for `%s`", server.Config.FinalizeMode, filepath.Base(filename))
	if err := config.AcquireFFmpegHeavyFor(config.FFmpegHeavyAcquireTimeout); err != nil {
		return "", fmt.Errorf("could not acquire ffmpeg slot to finalize: %w", err)
	}
	defer config.ReleaseFFmpegHeavy()

	// Attempt 1: strict finalization.  A truncated tail (the last fragment
	// cut short when the stream/session ends) makes ffmpeg abort here even
	// though the file is 99% fine.
	outputBytes, err := run(false)

	// Attempt 2: rescue pass with tolerant flags.  A slightly shortened
	// recording beats a completely unplayable one; if even this fails, the
	// original raw file is kept (caller handles the error).
	rescueUsed := false
	if err != nil {
		_ = os.Remove(tempOutput)
		outStr := strings.TrimSpace(string(outputBytes))
		if len(outStr) > 500 {
			outStr = outStr[len(outStr)-500:]
		}
		ch.Warn("finalize %s: %s — retrying with corrupt-tail recovery", filepath.Base(filename), outStr)
		outputBytes, err = run(true)
		rescueUsed = err == nil
	}

	if err != nil {
		_ = os.Remove(tempOutput)
		msg := strings.TrimSpace(string(outputBytes))
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}

	// Validate the output before it replaces (and deletes) the original:
	// ffmpeg can exit 0 while writing garbage when the source was too
	// damaged.  An unreadable output always keeps the original; a rescue
	// pass is only accepted when it still probes to at least half the
	// source duration, so a broken recording can never be destroyed.
	outDur, probeErr := VideoDurationSeconds(tempOutput)
	if probeErr != nil {
		_ = os.Remove(tempOutput)
		return "", fmt.Errorf("finalize output unreadable: %v", probeErr)
	}
	if rescueUsed {
		inDur, inErr := VideoDurationSeconds(filename)
		if inErr == nil && inDur > 0 && outDur < inDur*0.5 {
			_ = os.Remove(tempOutput)
			ch.Warn("finalize %s: rescue output too short (%.1fs vs source %.1fs) — keeping original recording",
				filepath.Base(filename), outDur, inDur)
			return "", fmt.Errorf("rescue output too short (%.1fs < 50%% of source %.1fs)", outDur, inDur)
		}
		ch.Warn("finalize %s: rescue pass recovered %.1fs of %.1fs source",
			filepath.Base(filename), outDur, inDur)
	}
	if finalPath == filename {
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tempOutput)
			return "", fmt.Errorf("remove original before replace: %w", err)
		}
	}
	if err := os.Rename(tempOutput, finalPath); err != nil {
		_ = os.Remove(tempOutput)
		return "", fmt.Errorf("rename finalized output: %w", err)
	}
	return finalPath, nil
}

func qualityArgsForEncoder(encoder string, quality int) []string {
	if quality <= 0 {
		quality = 23
	}
	lower := strings.ToLower(strings.TrimSpace(encoder))
	switch {
	case strings.Contains(lower, "nvenc"):
		return []string{"-cq", fmt.Sprintf("%d", quality)}
	case strings.Contains(lower, "qsv"), strings.Contains(lower, "vaapi"), strings.Contains(lower, "amf"):
		return []string{"-global_quality", fmt.Sprintf("%d", quality)}
	default:
		return []string{"-crf", fmt.Sprintf("%d", quality)}
	}
}

// ─── Output pipeline (MoveToOutputDir → thumbnail → upload → metadata → cleanup) ───

// MoveToOutputDir relocates a finalized recording into server.Config.OutputDir.
// Errors are non-fatal: the recording is already safely written at srcPath.
// endReason (why the recording stopped) is forwarded to the pipeline so it is
// persisted to the recordings row in Supabase.
func (ch *Channel) MoveToOutputDir(srcPath, endReason string) string {
	// Central min-duration gate: never upload a sub-threshold recording.
	// Same-session merge cycles are combined upstream; if the final (merged)
	// result is still below the threshold it is a genuinely short stream and is
	// discarded rather than pushed as a small clip.
	if discardIfBelowMinDuration(ch.Config.Username, srcPath) {
		return ""
	}

	// Enqueue the file into the pipeline for thumbnail → upload → metadata → cleanup.
	// The pipeline handles all lifecycle (semaphore, waitgroup, state persistence).
	//
	// EnqueueFileClaimed is used because every path here marks the file
	// in-flight before enqueueing (to keep the OutputDir watcher from
	// double-claiming it).  The plain EnqueueFile would see that marker and
	// drop the enqueue as a "duplicate" — leaving the freshly moved recording
	// permanently stuck, never uploaded until the next restart.
	enqueue := func(filePath string) {
		ch.PipelineQueue.EnqueueFileClaimed(filePath, endReason)
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

	// Retry remove with aggressive backoff (up to ~50s total) so transient
	// Windows locks (AV scanner, Search Indexer) have time to release.
	// If still locked, try rename + delete as a fallback.
	for i := 0; i < 20; i++ {
		if err := os.Remove(src); err == nil {
			return nil
		}
		if i >= 10 {
			tmpPath := fmt.Sprintf("%s.deleting.%d", src, i)
			if renameErr := os.Rename(src, tmpPath); renameErr == nil {
				if removeErr := os.Remove(tmpPath); removeErr == nil {
					return nil
				}
				os.Rename(tmpPath, src)
			}
		}
		backoff := time.Duration(100*(1<<uint(min(i, 8)))) * time.Millisecond // 100ms, 200ms, 400ms, … 25.6s
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		time.Sleep(backoff)
	}
	// Source could not be removed — return the error so the caller can handle
	// the duplicate without silently leaving orphaned files.
	return fmt.Errorf("could not remove source after copy: %w", os.Remove(src))
}

// ─── Min-duration pending segments ─────────────────────────────────────────

// resolveMinDurationBeforeUpload returns the effective min-duration-before-
// upload threshold for a channel, mirroring handleMinDurationAndMerge:
// the per-channel setting (pool DB / channels.json) wins, falling back to the
// server-global flag.  Orphan and pending-segment flows MUST use the same
// value the channel flow gates with — otherwise a channel configured with a
// 1200s threshold in the pool would have its short segments parked by the
// channel flow and then uploaded directly by the orphan/pending flows on any
// node whose global MIN_DURATION_BEFORE_UPLOAD env var is unset.
func resolveMinDurationBeforeUpload(username string) int {
	if server.Manager != nil {
		if v := server.Manager.ChannelMinDurationBeforeUpload(username); v > 0 {
			return v
		}
	}
	if server.Config != nil {
		return server.Config.MinDurationBeforeUpload
	}
	return 0
}

// discardIfBelowMinDuration is the single enforcement point for the
// "longer videos only" policy: a finalized recording shorter than the
// min-duration-before-upload threshold is deleted (never uploaded).  Every
// egress path — the normal upload, the merge flush, and orphan recovery —
// routes through this so no code path can leak a sub-threshold clip.
//
// Without this, the min-duration gate only blocked at PARK time; the
// definitive-stop flush (handleMinDurationAndMerge) and the lone-segment
// recovery still uploaded sub-threshold files (with empty end_reason), which
// is exactly what produced the 16s/21s recordings in the archive.
//
// A probe failure is treated as "keep" — we never delete content whose
// duration we cannot confirm.  When the threshold is disabled (<=0) this is a
// no-op.
func discardIfBelowMinDuration(username, path string) bool {
	// Hard floor: a file smaller than min-file-size-before-upload (default 1MB)
	// can only be a crash artifact — a truncated/failed segment written by a
	// dying DVR, not a real recording.  Such a file cannot probe a valid
	// duration or be encoded into a thumbnail, so the duration gate above
	// would keep it forever (probe failure => keep) and it would become a
	// permanent, un-backfillable recordings row like the observed 1,112-byte
	// "bright_diamonds_054" that AnonMP4 could never encode.  Discard these
	// unconditionally, independent of the min-duration setting.
	if server.Config != nil && server.Config.MinFileSizeBeforeUpload > 0 {
		minBytes := int64(server.Config.MinFileSizeBeforeUpload) * 1024 * 1024
		if st, statErr := os.Stat(path); statErr == nil && st.Size() < minBytes {
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				recoveryLogf(path, "min-size: could not delete under-sized file %d bytes (< %d): %v — leaving it un-uploaded", st.Size(), minBytes, rmErr)
				return true // still treated as discarded: do not upload
			}
			recoveryLogf(path, "min-size: discarded crash-artifact recording %d bytes (< %d bytes)", st.Size(), minBytes)
			return true
		}
	}

	minDur := resolveMinDurationBeforeUpload(username)
	if minDur <= 0 {
		return false
	}
	dur, err := VideoDurationSeconds(path)
	if err != nil {
		return false
	}
	if dur < float64(minDur) {
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			recoveryLogf(path, "min-duration: could not delete sub-threshold file %.1fs (< %ds): %v — leaving it un-uploaded", dur, minDur, rmErr)
			return true // still treated as discarded: do not upload
		}
		recoveryLogf(path, "min-duration: discarded sub-threshold recording %.1fs (< %ds)", dur, minDur)
		return true
	}
	return false
}

// MaybeDeferToPending checks whether min-duration is enabled and, if so,
// whether filePath is short enough to be deferred.  When the file should be
// deferred (or on probe failure — we'd rather be safe) it is moved into
// .pending/<user>/ and the function returns true so callers skip upload.
func MaybeDeferToPending(filePath string) bool {
	username := extractUsernameFromFilename(filepath.Base(filePath))
	if username == "" {
		// Can't determine the user; fall back to "unknown"
		username = "unknown"
	}

	minDur := resolveMinDurationBeforeUpload(username)
	if minDur <= 0 {
		return false // feature disabled — upload directly
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
	mu := pendingMu(username)
	mu.Lock()
	defer mu.Unlock()

	pendingDir := pendingSegmentsDir(username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}
	dest := filepath.Join(pendingDir, filepath.Base(filePath))
	return os.Rename(filePath, dest)
}

// recoverConsumedMergeInput handles a stray "<stable video>.consumed" file left
// by a re-merge that failed to publish AND restore.  It returns the name to
// classify as a normal main video ("" = handled/deferred, no classification):
//   - restore: renames the stray back to its original name when that name is
//     free (the publish rename failed, so a is the only copy of that content);
//   - dedup: removes the stray when the published merge already occupies the
//     stable name — the published merge is a strict SUPERSET of it (old merged
//     file + the next cycle), so dropping it is safe, never a loss;
//   - defer: leaves it in place when it is too recent (a live re-merge may be
//     staging it right now) or locked, for a later scan.
//
// Renaming onto an occupied destination is never done: on POSIX os.Rename
// would silently OVERWRITE the freshly published merge, destroying it.
func recoverConsumedMergeInput(dir, name string) string {
	bare := strings.TrimSuffix(name, ".consumed")
	if bare == "" {
		return ""
	}
	path := filepath.Join(dir, name)

	if st, serr := os.Stat(path); serr == nil && time.Since(st.ModTime()) < orphanSettleWindow {
		return "" // a live re-merge staged this file a moment ago — don't race it
	}

	dest := filepath.Join(dir, bare)
	if _, err := os.Stat(dest); err == nil {
		// Superseded by the published merge (whose content contains it): drop
		// the duplicate once the OS releases it.
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			recoveryLogf(bare, "recovery: consumed duplicate of %s could not be removed (locked?) — leaving for the next scan: %v", bare, rmErr)
		} else {
			recoveryLogf(bare, "recovery: removed consumed duplicate of published merge %s", bare)
		}
		return ""
	} else if !os.IsNotExist(err) {
		// Cannot confirm the stable name is free — never risk renaming over a
		// possibly-present published merge.
		recoveryLogf(bare, "recovery: could not stat destination %s — leaving stranded input for the next scan: %v", bare, err)
		return ""
	}

	if err := os.Rename(path, dest); err != nil {
		recoveryLogf(bare, "recovery: stranded merge input %s could not be restored — leaving for the next scan: %v", name, err)
		return ""
	}
	recoveryLogf(bare, "recovery: restored stranded merge input %s -> %s; uploading normally", name, bare)
	return bare
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

		// Remove ffmpeg finalizer scratch files ("<base>.finalizing.<ext>")
		// left behind by a crash mid-finalize.  The original recording is
		// still on disk — the finalizer only deletes it after a successful
		// rename — so the partial scratch is pure garbage.  Only age them out
		// past the finalize timeout (30 min): a live finalize may still be
		// writing one.
		removeStaleFinalizingScratch(dir)

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

			// Recover stray re-merge inputs parked under a ".consumed" suffix
			// (a re-merge that failed to publish AND restore).  Restored files
			// fall through to the normal main-video classification and are
			// uploaded like any stranded recording; duplicates of a published
			// merge are dropped.
			if strings.HasSuffix(name, ".consumed") {
				if recovered := recoverConsumedMergeInput(dir, name); recovered != "" {
					name = recovered
					path = filepath.Join(dir, recovered)
				} else {
					continue
				}
			}

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
				// Never race another flow that owns this file right now (an active
				// channel pipeline or the OutputDir watcher).  Their in-flight
				// marker is cleared when they finish; the periodic scan (or the
				// manual rescan) picks the file back up afterwards.
				if IsUploadInFlight(info.path) {
					return
				}
				thumb := GenerateThumbnailForFile(info.path)
				thumbURL := thumb.ThumbURL
				spriteURL := thumb.SpriteURL
				previewURL := thumb.PreviewURL
				// Keep sidecars unless the upload fully completed: a deferred
				// save (missing thumbnail) or failed upload leaves everything
				// for the next scan's retry.
				if UploadOrphanedFile(info.path, thumbURL, spriteURL, previewURL) {
					DeleteSidecarFiles(info.path)
				}
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
				if !MaybeDeferToPending(vInfo.path) && !IsUploadInFlight(vInfo.path) {
					thumb := GenerateThumbnailForFile(vInfo.path)
					thumbURL := thumb.ThumbURL
					spriteURL := thumb.SpriteURL
					previewURL := thumb.PreviewURL
					// Keep the local file unless the upload fully completed: a
					// deferred save (missing thumbnail) or failed upload must
					// leave the source for the next scan's retry.
					if UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL) {
						DeleteSidecarFiles(vInfo.path)
						_ = os.Remove(vInfo.path)
					}
				}
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
					if !MaybeDeferToPending(vInfo.path) && !IsUploadInFlight(vInfo.path) {
					thumb := GenerateThumbnailForFile(vInfo.path)
					thumbURL := thumb.ThumbURL
					spriteURL := thumb.SpriteURL
					previewURL := thumb.PreviewURL
					// Keep the local file unless the upload fully completed: a
					// deferred save (missing thumbnail) or failed upload must
					// leave the source for the next scan's retry.
					if UploadOrphanedFile(vInfo.path, thumbURL, spriteURL, previewURL) {
						DeleteSidecarFiles(vInfo.path)
						_ = os.Remove(vInfo.path)
					}
				}
				return
				}

				// Delete source sidecars
				os.Remove(vInfo.path)
				os.Remove(aInfo.path)

				// Generate thumbnails, upload, and clean up
				if MaybeDeferToPending(muxedPath) {
					DeleteSidecarFiles(muxedPath)
					os.Remove(muxedPath)
					return
				}
				// Another flow (watcher/pipeline) already owns the freshly muxed
				// file — let it finish; never upload or delete it out from
				// under that upload.
				if IsUploadInFlight(muxedPath) {
					return
				}
				thumb := GenerateThumbnailForFile(muxedPath)
				thumbURL := thumb.ThumbURL
				spriteURL := thumb.SpriteURL
				previewURL := thumb.PreviewURL
				// Only drop the muxed output when the upload fully completed —
				// a deferred save (missing thumbnail) or failed upload leaves
				// it for the next scan instead of losing the just-muxed file.
				if UploadOrphanedFile(muxedPath, thumbURL, spriteURL, previewURL) {
					DeleteSidecarFiles(muxedPath)
					_ = os.Remove(muxedPath)
				}
			}()
		}

		// Wait for all orphan processing to complete
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}

		// Process orphaned MAIN videos (fully finalized recordings that were
		// never uploaded — e.g. a pipeline retry dropped when the channel
		// stopped, a Supabase outage lost the journal, or a crash left the file
		// stranded).  Previously the scan built this map but never acted on it,
		// so such recordings had no recovery path and sat on disk forever.
		activeRecs := map[string]bool{}
		if m := server.Manager; m != nil {
			for _, p := range m.ActiveRecordingFiles() {
				if abs, err := filepath.Abs(p); err == nil {
					activeRecs[filepath.Clean(abs)] = true
				} else {
					activeRecs[filepath.Clean(p)] = true
				}
			}
		}
		for stem, info := range mainVideos {
			stem := stem
			// Never touch a file a channel is recording RIGHT NOW: the orphan
			// scan walks the recording directory too, and a live recording is
			// not marked in-flight (it is only being appended to).
			absPath, absErr := filepath.Abs(info.path)
			if absErr == nil {
				if activeRecs[filepath.Clean(absPath)] {
					continue
				}
			} else if activeRecs[filepath.Clean(info.path)] {
				continue
			}
			if IsUploadInFlight(info.path) {
				continue // another flow (pipeline/watcher) owns it right now
			}
			if IsFinalizingTemp(info.name) || strings.Contains(info.name, ".deleting.") {
				continue // scratch/in-flight finalize artifact, not a recording
			}
			// Settle guard: a channel that just stopped recording is about to
			// finalize this file (ffmpeg reads the source in videos/ while
			// writing the .finalizing scratch, and only marks the OUTPUT
			// in-flight after the rename).  Skipping recently-touched files
			// prevents the scan from uploading the pre-finalize source while
			// the pipeline then uploads the finalized output — a duplicate.
			if st, serr := os.Stat(info.path); serr == nil && time.Since(st.ModTime()) < orphanSettleWindow {
				continue
			}

			// Skip files that were already fully uploaded AND have their
			// thumbnail: content is safe in the cloud, so drop the local copy.
			// If the thumbnail is missing, KEEP the file so ScanThumbnails can
			// retry generation — deleting now would make the thumbnail
			// un-recoverable forever.
			if IsAlreadyFullyUploaded(info.path) {
				thumbURL, _, _ := server.LoadPreviewLinks(info.name)
				if thumbURL == "" {
					recoveryLogf(info.name, "recovery: fully uploaded but thumbnail missing — keeping for thumbnail retry")
					continue
				}
				recoveryLogf(info.name, "recovery: fully uploaded main video — removing local copy")
				os.Remove(info.path)
				DeleteSidecarFiles(info.path)
				continue
			}

			if MaybeDeferToPending(info.path) {
				continue // short segment parked for merge
			}

			recoveryLogf(info.name, "recovery: uploading orphaned main video %s", info.name)
			thumb := GenerateThumbnailForFile(info.path)
			thumbURL := thumb.ThumbURL
			spriteURL := thumb.SpriteURL
			previewURL := thumb.PreviewURL
			// Keep sidecars unless the upload fully completed (same rule as
			// above: deferred saves and failed uploads retry on the next scan).
			if UploadOrphanedFile(info.path, thumbURL, spriteURL, previewURL) {
				DeleteSidecarFiles(info.path)
			}
			_ = stem
		}

		// Process any pending segments (short videos awaiting merge).
		// Pending segments are stored under .pending/{username}/.
		processAllPendingSegments(false)

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
				// base may or may not already carry a video extension. A main video's
				// stem is its name minus its own extension, so match BOTH forms:
				//   - base IS a main video (e.g. "X.mp4.merged.mp4" -> stem "X.mp4.merged")
				//   - base needs an extension appended (e.g. "recording" sidecar whose
				//     main is "recording.mp4" -> stem "recording")
				// Previously this only tested mainVideos[base+ext], which never matched
				// previews/thumbnails of .merged.mp4 (or other already-.mp4) outputs —
				// the cleanup deleted their freshly-generated sidecars mid-upload.
				if _, ok := mainVideos[strings.TrimSuffix(base, filepath.Ext(base))]; ok {
					hasMain = true
				}
				if !hasMain {
					for ext := range map[string]bool{".mp4": true, ".mkv": true, ".ts": true} {
						if _, ok := mainVideos[strings.TrimSuffix(base+ext, filepath.Ext(base+ext))]; ok {
							hasMain = true
							break
						}
					}
				}
				if !hasMain {
					// Same ordering rule as DeleteSidecarFiles: an abandoned asset
					// upload goroutine may still be reading this sidecar (the sidecar
					// name is "<videoPath>.<kind>", so base is that video path).
					// Leave it to that goroutine's own deferred os.Remove instead of
					// deleting it under a live reader.
					if !IsThumbnailAssetUploadInFlight(filepath.Join(dir, base)) {
						os.Remove(path)
					}
				}
				break
			}
		}
	}
}

// removeStaleFinalizingScratch deletes transient scratch files whose mtime is
// older than the finalize timeout:
//   - ffmpeg finalizer scratch files ("<base>.finalizing.<ext>") — a crash
//     mid-finalize (process kill, RDP node restart) leaves the partial scratch
//     behind while the original recording is still present, so the scratch is
//     pure garbage that must never be uploaded or thumbnailed;
//   - deletion-in-progress leftovers ("<base>.deleting.N") from
//     removeFileWithRetry's rename-then-delete strategy — a crash between the
//     rename and the remove strands a dead copy of an already-uploaded file.
//
// The 35-minute age threshold is safe because ffmpeg writes the scratch file
// continuously while it runs, keeping its mtime fresh — a scratch older than
// ~35 minutes means ffmpeg has not written to it for that long, i.e. the
// finalize is dead (the strict + rescue passes each have a 30-minute budget,
// but an in-progress pass keeps the mtime current until its last write).
// Even in the one adversarial case — a rescue pass waiting on the
// process-wide FFmpegHeavy semaphore with a stale scratch — the rescue pass
// itself calls os.Remove(tempOutput) before running, so ffmpeg simply
// recreates the file; the original recording is never touched.
func removeStaleFinalizingScratch(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	const maxAge = 35 * time.Minute
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !IsFinalizingTemp(name) && !strings.Contains(name, ".deleting.") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if time.Since(info.ModTime()) > maxAge {
			path := filepath.Join(dir, e.Name())
			if os.Remove(path) == nil {
				log.Printf("[cleanup] removed stale scratch file %s", e.Name())
			}
		}
	}
}

// DeleteSidecarFiles removes preview sidecar files associated with a video path.
//
// It refuses to remove them while a presentation-asset upload for the same video
// may still be reading them.  generateThumbnailForFile abandons an asset
// goroutine after thumbnailAssetTimeout and deliberately lets it keep uploading
// mirrors in the background, so deleting the sidecar here pulled the file out
// from under that goroutine: every remaining attempt then failed with "imgpile:
// open file: ..." / "imgbb: read file: ..." MINUTES after the pipeline had
// logged "completed ... successfully" (the files were already gone).  The
// guard is what keeps the two flows ordered — deletion waits for the upload
// that owns the file.  Each asset goroutine removes its own sidecar when it
// finishes, and the orphan-sidecar sweep at the end of CleanupOrphanedFiles
// applies the same guard, so a skipped deletion is never a leak.
func DeleteSidecarFiles(videoPath string) {
	if IsThumbnailAssetUploadInFlight(videoPath) {
		log.Printf("delete: keeping sidecars for %s — thumb/sprite/preview upload still in flight", filepath.Base(videoPath))
		return
	}
	for _, suffix := range []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"} {
		os.Remove(videoPath + suffix)
	}
}

// removeFileWithRetry attempts to remove a file, retrying up to 5 times
// with exponential backoff.  This handles transient Windows file locks
// from AV scanners, Search Indexer, upload handles still closing, etc.
//
// After 2 attempts, it tries a rename-then-delete strategy: renaming
// the file can succeed even when deletion is blocked by a reader (common with
// Windows Defender), which often releases the lock on the original path.
// Returns nil if the file was removed (or didn't exist).
func removeFileWithRetry(path string) error {
	for i := 0; i < 5; i++ {
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			return nil
		}

		if i >= 2 {
			tmpPath := fmt.Sprintf("%s.deleting.%d", path, i)
			if renameErr := os.Rename(path, tmpPath); renameErr == nil {
				if removeErr := os.Remove(tmpPath); removeErr == nil {
					return nil
				}
				os.Rename(tmpPath, path)
			}
		}

		backoff := time.Duration(500*(1<<uint(min(i, 6)))) * time.Millisecond // 0.5s, 1s, 2s, 4s, 8s, 16s, 32s
		if backoff > 15*time.Second {
			backoff = 15 * time.Second
		}
		time.Sleep(backoff)
	}
	return os.Remove(path) // final attempt, return the error
}

// muxVideoAudio combines a separate video and audio file into a single MP4.
// Used only for recovering legacy split A/V sidecars from previous versions.
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

// dateSeparatorRe matches the "_YYYY-MM-DD_" / "_YYYY-MM-DD-" timestamp separator
// the recorder writes between the username and the time portion.  Anchoring on
// the full date (not just "_20") is what keeps usernames that themselves contain
// "_20" (e.g. "alice_20_fan_2025-01-01_...") from being mis-split: the regex
// skips the "_20" inside the username and lands on the real date.
var dateSeparatorRe = regexp.MustCompile(`_(20\d{2}-\d{2}-\d{2})[_-]`)

// findDateSeparatorIndex returns the byte index in stem of the "_" that begins
// the "_YYYY-MM-DD_" timestamp separator, or -1 if no date separator is found.
// Both extractUsernameFromFilename and extractTimestampFromFilename use it so
// they always agree on where the username ends.
func findDateSeparatorIndex(stem string) int {
	loc := dateSeparatorRe.FindStringSubmatchIndex(stem)
	if loc == nil {
		return -1
	}
	return loc[0] // index of the leading "_"
}

// extractUsernameFromFilename parses "username_YYYY-MM-DD_HH-MM-SS.ext" to get the username.
// It locates the "_YYYY-MM-DD_" timestamp separator (not merely the substring
// "_20", which can legitimately appear inside a username such as
// "alice_20_fan_2025-01-01_...") so the username portion is split correctly.
func extractUsernameFromFilename(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Strip "merged-" prefix that the merge system prepends.
	stem := strings.TrimPrefix(base, "merged-")

	// Find the real timestamp separator (_YYYY-MM-DD_ or _YYYY-MM-DD-).
	idx := findDateSeparatorIndex(stem)
	if idx < 0 {
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
	idx := findDateSeparatorIndex(base)
	if idx < 0 {
		return ""
	}
	ts := base[idx+1:] // "YYYY-MM-DD_HH-MM-SS..."
	if len(ts) >= 19 && ts[4] == '-' && ts[7] == '-' && ts[10] == '_' && ts[13] == '-' && ts[16] == '-' {
		return ts[:10] + "T" + ts[11:13] + ":" + ts[14:16] + ":" + ts[17:19] + "Z"
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
//
// This delegates to uploader.NewMultiHostUploader(...).AvailableHosts() so that
// the set of hosts checked by IsAlreadyFullyUploaded is always identical to the
// set the pipeline actually uploads to.  Previously this maintained a separate
// hand-written list that drifted out of sync, which caused the watcher to
// consider a file "fully uploaded" — and delete the local copy — before every
// configured host had received it.
func configuredUploadHosts() []string {
	cfg := server.Config
	if cfg == nil {
		return nil
	}
	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.VidaraKey,
		nil,
	)
	return upl.AvailableHosts()
}

// UploadOrphanedFile uploads a file to all configured hosts and saves metadata
// to Supabase. Unlike Channel.uploadFile, this doesn't require an active channel.
// Username is extracted from the filename; metadata fields are left empty.
//
// If every configured host fails on the first attempt, it retries up to 2 more
// times with a 60-second delay between attempts.  This handles transient network
// or API outages that can occur when the app restarts after a crash.
func UploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL string) bool {
	return uploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL, true)
}

// UploadOrphanedFileEvict uploads a file whose thumbnails can never be
// generated (permanently corrupt recording) and then deletes the local copy
// entirely once its metadata is saved — the user's chosen policy for evicting
// files that would otherwise burn disk + CPU forever on thumbnail retries.
func UploadOrphanedFileEvict(filePath string) bool {
	return uploadOrphanedFile(filePath, "", "", "", false)
}

func uploadOrphanedFile(filePath, thumbURL, spriteURL, previewURL string, thumbMustExist bool) bool {
	MarkUploadInFlight(filePath)
	defer MarkUploadDone(filePath)
	cfg := server.Config
	if cfg == nil {
		return false
	}

	filename := filepath.Base(filePath)

	// Safety net: if the file vanished since the caller generated thumbnails
	// (e.g. a concurrent flow finalized/removed it), uploading it would only
	// thrash every host for 3 attempts. Bail out — the next scan or merge
	// re-creates it.
	if _, err := os.Stat(filePath); err != nil {
		recoveryLogf(filename, "file vanished before upload (%v) — skipping; will be retried", err)
		return false
	}

	// Central min-duration gate: orphan recovery must obey the same "longer
	// videos only" policy as the channel flow, otherwise sub-threshold clips
	// (e.g. a lone parked segment flushed on a definitive stop) slip through.
	if discardIfBelowMinDuration(extractUsernameFromFilename(filename), filePath) {
		return false
	}

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
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL, nil, nil, nil); err != nil {
			recoveryLogf(filename, "could not save preview links: %v", err)
		}
	}

	upl := uploader.NewMultiHostUploader(
		cfg.VoeSXAPIKey,
		cfg.StreamtapeLogin,
		cfg.StreamtapeKey,
		cfg.MixdropEmail,
		cfg.MixdropToken,
		cfg.VidaraKey,
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
				// Eviction mode: the file is corrupt (no thumbnail possible)
				// but already safe in the cloud — delete the local copy even
				// though nothing new was uploaded this pass.
				if !thumbMustExist && cfg.DeleteLocalAfterUpload {
					DeleteSidecarFiles(filePath)
					if err := removeFileWithRetry(filePath); err != nil {
						recoveryLogf(filename, "could not remove local file: %v — will retry on next restart", err)
						return true
					}
					if fileHash != "" {
						if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
							recoveryLogf(filename, "could not delete journal: %v", jErr)
						}
					}
					recoveryLogf(filename, "removed local file (evicted corrupt file)")
				}
				return true
			}
			// All hosts have the file per the journal but the recording row is
			// missing — the original metadata save failed (e.g. during a
			// Supabase outage).  Recover the metadata from the local journal's
			// stored links instead of re-uploading, which would duplicate the
			// video on every host.
			//
			// Tri-state: false can mean "stale journal" (nothing recoverable —
			// clear the journal and re-upload) OR "thumbnail still missing"
			// (journal is GOOD — never clear it, that would trigger a wasteful
			// duplicate re-upload to every host).  Distinguish via the flag so
			// a thumbnail-defer leaves the journal intact for the next scan.
			if ok, journalGood := recoverOrphanMetadataFromJournal(filePath, filename, fileHash); ok {
				return true
			} else if journalGood {
				recoveryLogf(filename, "recovery deferred: thumbnail still missing — journal kept, file kept for thumbnail retry")
				return false
			}
			recoveryLogf(filename, "stale journal has no Supabase recording and no recoverable links; clearing journal and re-uploading")
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

	// Use RetryManager for upload retries
	var allResults []uploader.UploadResult
	err := DoWithRetry("orphan-"+filename, func() error {
		attemptResults := upl.UploadSelected(filePath, hostsToTry)
		allResults = append(allResults, attemptResults...)

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
				if jErr := server.SaveJournalEntry(fileHash, filename, r.Host, status, r.DownloadLink, filesize, errMsg); jErr != nil {
					recoveryLogf(filename, "could not save journal for %s: %v", r.Host, jErr)
				}
			}
		}

		success := uploader.GetSuccessfulUploads(attemptResults)
		recoveryLogf(filename, "upload attempt — %d/%d successful", len(success), len(allHosts))
		if len(success) >= len(allHosts) {
			return nil
		}

		failedHosts := failedHostNames(attemptResults, completedHosts)
		hostsToTry = failedHosts
		if len(hostsToTry) == 0 {
			return nil
		}

		return fmt.Errorf("%d hosts still pending", len(hostsToTry))
	}, WithUploadSem(), WithMaxAttempts(3), WithBaseBackoff(60*time.Second))

	// Build links map from all accumulated results — even if one host is down,
	// the hosts that DID receive the file must be persisted so the recording
	// gets an embed URL and is playable.
	links := map[string]string{}
	var embedURL string
	for _, r := range allResults {
		if r.Error == nil && r.DownloadLink != "" {
			links[r.Host] = r.DownloadLink
			if embedURL == "" {
				embedURL = embedURLFromLink(r.Host, r.DownloadLink)
			}
		}
	}

	// Thumbnail gate (mirrors the pipeline's stageSaveMetadata rule): never
	// persist a recording row whose THUMBNAIL is missing while the local
	// source could still produce one.  A NULL-thumbnail row that outlives the
	// file can never be repaired — ScanThumbnails needs the local video — so
	// retry generation once here and, if it still fails, defer the whole
	// save: the journal keeps every host's completion, the file stays on
	// disk, and the next orphan scan resumes via the journal-recovery path
	// (which regenerates and saves).  Returning false tells callers to keep
	// the local copy.
	// Placed AFTER the all-hosts-failed early return: when the video itself
	// failed to upload there is nothing to save yet, and burning an ffmpeg
	// generation pass (up to the 3-hour asset timeout) would only stall the
	// retry loop.
	if thumbMustExist && thumbURL == "" && len(links) > 0 {
		recoveryLogf(filename, "thumbnail missing before metadata save — regenerating once")
		thumb := GenerateThumbnailForFile(filePath)
		if thumb.ThumbURL != "" {
			thumbURL = thumb.ThumbURL
			spriteURL = thumb.SpriteURL
			previewURL = thumb.PreviewURL
			if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL, thumb.ThumbMirrors, thumb.SpriteMirrors, thumb.PreviewMirrors); err != nil {
				recoveryLogf(filename, "could not save preview links: %v", err)
			}
		}
	}
	if thumbMustExist && thumbURL == "" && len(links) > 0 {
		recoveryLogf(filename, "thumbnail generation failed — deferring metadata save, keeping file for thumbnail retry")
		return false
	}

	if err != nil {
		if len(links) == 0 {
			recoveryLogf(filename, "[WARN] all upload attempts exhausted — file will be retried on next restart")
			return false
		}
		recoveryLogf(filename, "[WARN] %d/%d hosts succeeded despite errors (%v) — persisting partial links",
			len(links), len(allHosts), err)
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

	return saveOrphanMetadataAndCleanup(filePath, filename, fileHash, embedURL, filesize, dur, timestamp, thumbURL, spriteURL, previewURL, links, thumbMustExist)
}

// saveOrphanMetadataAndCleanup persists recording metadata to Supabase and
// removes the local file once the recording is safely stored.  Returns true
// when the upload was handled (metadata saved OR the file intentionally kept),
// false only when the local copy must be retried on the next scan.
//
// thumbMustExist gates local deletion on a generated thumbnail — the same rule
// the pipeline cleanup uses: a file whose thumbnail is missing is kept so a
// later ScanThumbnails pass can retry generation (the video itself is already
// safe on the hosts).  Corrupt-file eviction passes false so the file is
// deleted even without a thumbnail.
func saveOrphanMetadataAndCleanup(filePath, filename, fileHash, embedURL string, filesize int64, dur float64, timestamp, thumbURL, spriteURL, previewURL string, links map[string]string, thumbMustExist bool) bool {
	cfg := server.Config
	dbSaved := false
	if err := server.SaveRecordingWithLinks(
		extractUsernameFromFilename(filename), filename, timestamp,
		"", nil, 0, "", 0, filesize, dur, "", "", embedURL, thumbURL, spriteURL, previewURL, links,
	); err != nil {
		// Keep the local journal on DB failure: it holds the dedup record AND
		// the download links, so the next scan can retry metadata recovery
		// without re-uploading to any host.  Deleting it here (the old
		// behavior) destroyed the dedup proof and caused duplicate uploads.
		recoveryLogf(filename, "failed to save recording to Supabase: %v (journal kept for retry)", err)
	} else {
		dbSaved = true
		recoveryLogf(filename, "saved recording metadata")
	}

	// Delete local file only once ALL hosts have the file safely, metadata
	// is persisted, and (when thumbMustExist) the THUMBNAIL exists.  Gating on
	// "any of the three assets" (the old check) let a file whose sprite
	// uploaded but whose thumbnail failed be deleted — making the thumbnail
	// un-recoverable forever, because ScanThumbnails needs the source video.
	// Keep the file whenever the thumbnail is missing so a later
	// ScanThumbnails pass can retry generation and fill in the missing URL —
	// the video itself is already safe on the hosts.
	if cfg.DeleteLocalAfterUpload && dbSaved && (thumbURL != "" || !thumbMustExist) {
		DeleteSidecarFiles(filePath)
		if err := removeFileWithRetry(filePath); err != nil {
			recoveryLogf(filename, "could not remove local file: %v — will retry on next restart", err)
			return true
		}
		if fileHash != "" {
			if jErr := server.DeleteJournalByHash(fileHash); jErr != nil {
				recoveryLogf(filename, "could not delete journal: %v", jErr)
			}
		}
		recoveryLogf(filename, "removed local file")
	}

	return true
}

// recoverOrphanMetadataFromJournal rebuilds a missing recording row from the
// local upload journal after a Supabase outage lost the original metadata
// save.  The file was already fully uploaded (all hosts recorded as success in
// the journal), so we persist the stored links instead of re-uploading — which
// would duplicate the video on every host.
//
// Thumbnails: the journal stores links only, so the saved row would otherwise
// have a NULL thumbnail_url (the exact bug that stranded thumbnail-less rows —
// e.g. nicolle_mitchelle 2026-09-05, eve_kiwi100/liaglamour/terrryliq earlier
// — with no preview_images row and no way back once the local file was
// deleted).  We reuse any thumbnail already stored in preview_images, else
// generate one now from the local file, and save it to preview_images so the
// periodic thumb-sync copies it onto the recordings row.  When generation
// fails, the save is DEFERRED: the local file and journal stay and the next
// scan retries — a NULL-thumbnail row that outlives the file can never be
// repaired.
//
// Returns (true, _) when the metadata was saved (or intentionally skipped),
// (false, false) when the journal is stale/unrecoverable and the caller may
// clear it, and (false, true) when the journal is valid but the save was
// deferred on thumbnail generation — the caller must keep the journal.
func recoverOrphanMetadataFromJournal(filePath, filename, fileHash string) (bool, bool) {
	if fileHash == "" {
		return false, false
	}
	links := server.LoadJournalLinks(fileHash)
	if len(links) == 0 {
		return false, false
	}

	// Reuse an already-stored thumbnail before spending an ffmpeg+upload pass:
	// preview_images is the authoritative store the pipeline and ScanThumbnails
	// write, the recordings row is checked too (it may hold a thumb even when
	// preview_images lost its row).
	thumbURL, spriteURL, previewURL := server.LoadPreviewLinks(filename)
	if thumbURL == "" {
		if t, s, p := server.LoadRecordingThumbnails(filename); t != "" {
			thumbURL, spriteURL, previewURL = t, s, p
		}
	}
	if thumbURL == "" {
		recoveryLogf(filename, "recovery: no stored thumbnail — generating from local file")
		thumb := GenerateThumbnailForFile(filePath)
		thumbURL = thumb.ThumbURL
		spriteURL = thumb.SpriteURL
		previewURL = thumb.PreviewURL
		if thumbURL == "" {
			// Keep file + journal; the next orphan scan retries this recovery.
			recoveryLogf(filename, "recovery: thumbnail generation failed — deferring metadata save, keeping file for thumbnail retry")
			return false, true
		}
		if err := server.SavePreviewLinks(filename, thumbURL, spriteURL, previewURL, thumb.ThumbMirrors, thumb.SpriteMirrors, thumb.PreviewMirrors); err != nil {
			recoveryLogf(filename, "recovery: could not save preview links: %v", err)
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
	dur, _ := VideoDurationSeconds(filePath)

	var embedURL string
	for _, host := range []string{"VOE.sx", "Vidara", "Mixdrop", "Streamtape"} {
		if link, ok := links[host]; ok && link != "" {
			embedURL = embedURLFromLink(host, link)
			if embedURL != "" {
				break
			}
		}
	}

	recoveryLogf(filename, "recovery: restoring metadata for fully-uploaded file from journal (%d hosts)", len(links))
	return saveOrphanMetadataAndCleanup(filePath, filename, fileHash, embedURL, filesize, dur, timestamp, thumbURL, spriteURL, previewURL, links, true), true
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

// pendingMaxAge is how long a sub-threshold pending segment may sit in
// .pending before it is deleted outright.  Segments land there when they are
// below the min-duration-before-upload threshold (the user does not want
// sub-threshold recordings uploaded); if no new recording arrives to merge
// them up to the threshold within this window, the stream is not coming back
// and the segments would leak disk forever.  Age is measured by file
// modification time — a merge refreshes the mtime, so a segment still being
// extended by newer recordings is never deleted.
const pendingMaxAge = 24 * time.Hour

// orphanSettleWindow is how recently a main-video file may have been modified
// before the orphan scan will consider it "settled".  A channel that just
// stopped recording finalizes the source in place (ffmpeg reads it while
// writing the .finalizing scratch and only marks the OUTPUT in-flight), so a
// recently-touched file is likely mid-finalize — skip it this pass and let
// the pipeline own it.
const orphanSettleWindow = 5 * time.Minute

// deleteStalePendingSegments removes pending segments older than pendingMaxAge.
// Callers must hold the channel's pending mutex.
func deleteStalePendingSegments(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	// Remove crash-left ".merging-*" merge scratch files whose mtime is older
	// than the merge timeout.  A live merge keeps the scratch mtime fresh while
	// it runs, and a user's merges are serialized by mergeMu, so an old scratch
	// is a crash leftover, never a live encode.  (They are excluded from
	// segment collection via isSidecar, so the segment loop below would
	// otherwise never clean them.)
	const scratchMaxAge = 35 * time.Minute
	scratchCutoff := time.Now().Add(-scratchMaxAge)
	for _, e := range entries {
		if e.IsDir() || !strings.Contains(e.Name(), ".merging-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(scratchCutoff) {
			log.Printf("[min-duration] removing stale merge scratch %s (mod %s)", e.Name(), info.ModTime().Format(time.RFC3339))
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	cutoff := time.Now().Add(-pendingMaxAge)
	for _, e := range entries {
		if e.IsDir() || isSidecar(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			path := filepath.Join(dir, e.Name())
			log.Printf("[min-duration] deleting stale pending segment %s (mod %s, older than %s)", path, info.ModTime().Format(time.RFC3339), pendingMaxAge)
			_ = os.Remove(path)
		}
	}
}

// recoverMergeScratch reconciles crash-left ".merging-*" merge scratch files in
// a user's pending dir:
//   - If any real pending segment still exists alongside a scratch, the scratch
//     is mid-merge garbage — its inputs still hold all the content — so it is
//     deleted and the segments are re-merged by the normal flow.
//   - If a scratch is the ONLY file, its inputs were already consumed by a
//     completed merge that crashed before the output was renamed to the stable
//     "merged-*" name.  The scratch IS the finished content, so rename it to
//     its stable name and let the normal flow merge/upload it exactly once.
//
// This closes the crash window where a finished merge's inputs were removed but
// the output was never finalized (content would otherwise be stranded in a
// scratch file that isSidecar now excludes), while guaranteeing a partial
// mid-merge scratch can never be treated as a real segment.
//
// A merge is serialized per user by mergeMu, so a scratch observed here can
// never belong to a live encode.  Callers must hold the channel's pending mutex.
func recoverMergeScratch(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var scratches []os.DirEntry
	for _, e := range entries {
		if e.IsDir() || !strings.Contains(e.Name(), ".merging-") {
			continue
		}
		scratches = append(scratches, e)
	}
	if len(scratches) == 0 {
		return
	}
	// Ambiguous leftovers (e.g. repeated crashes) — leave them for
	// deleteStalePendingSegments to age out rather than guessing.
	if len(scratches) > 1 {
		return
	}
	if len(collectPendingSegmentsInDir(dir)) > 0 {
		// Inputs still present — the scratch is mid-merge garbage.
		_ = os.Remove(filepath.Join(dir, scratches[0].Name()))
		return
	}
	// Sole scratch with no segments left: finished merge waiting to be
	// finalized.  Rename ".merging-<nano>-merged-<base>" to "merged-<base>".
	name := filepath.Base(scratches[0].Name())
	name = strings.TrimPrefix(name, ".merging-") // "<nano>-merged-<base>"
	if i := strings.Index(name, "-"); i >= 0 {
		name = name[i+1:] // "merged-<base>"
	}
	if !strings.HasPrefix(name, "merged-") {
		return // unrecognized — leave for stale aging
	}
	log.Printf("[min-duration] recovering finished merge scratch %s", filepath.Base(scratches[0].Name()))
	if rErr := os.Rename(filepath.Join(dir, scratches[0].Name()), filepath.Join(dir, name)); rErr != nil {
		log.Printf("[min-duration] could not recover merge scratch: %v", rErr)
	}
}

// collectPendingSegments returns sorted absolute paths of all pending segments
// for a given channel.
func collectPendingSegments(username string) []string {
	dir := pendingSegmentsDir(username)
	return collectPendingSegmentsInDir(dir)
}

// collectPendingSegmentsInDir returns sorted absolute paths of actual video
// files in dir, filtering out sidecar files and zero-byte files.  Previously
// consolidated "merged-*.mp4" files ARE intentionally included: merging a
// consolidated segment with newer ones preserves all of the recording content.
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
// up the (now empty) directory, including any quarantined corrupt segments.
func deletePendingSegments(username string) {
	dir := pendingSegmentsDir(username)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() && e.Name() == "corrupt" {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
			continue
		}
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	_ = os.Remove(dir)
}

// quarantineSegment moves a corrupt pending segment into the corrupt/
// subdirectory of its pending dir, preserving it for manual inspection while
// ensuring it can never block a future merge.
func quarantineSegment(path string) error {
	corruptDir := filepath.Join(filepath.Dir(path), "corrupt")
	if err := os.MkdirAll(corruptDir, 0777); err != nil {
		return err
	}
	dest := filepath.Join(corruptDir, filepath.Base(path))
	_ = os.Remove(dest) // overwrite a previously quarantined copy
	return os.Rename(path, dest)
}

// quarantineInvalidSegments probes each pending segment and moves any that
// cannot be probed (corrupt, truncated, or unreadable) into the corrupt/
// subdirectory so a single bad segment can never poison an entire merge.  It
// returns the probe-valid segments and the number quarantined.  Callers must
// hold the channel's pending mutex.
func quarantineInvalidSegments(username string, segments []string) ([]string, int) {
	valid := make([]string, 0, len(segments))
	quarantined := 0
	for _, s := range segments {
		dur, err := VideoDurationSeconds(s)
		if err != nil || dur <= 0 {
			if err == nil {
				err = fmt.Errorf("zero duration")
			}
			log.Printf("[min-duration] quarantining corrupt pending segment %s (%v)", filepath.Base(s), err)
			if qErr := quarantineSegment(s); qErr != nil {
				log.Printf("[min-duration] could not quarantine %s: %v — excluding it from merge anyway", filepath.Base(s), qErr)
			} else {
				quarantined++
			}
			continue
		}
		valid = append(valid, s)
	}
	return valid, quarantined
}

// mergedPendingName returns a stable name for a consolidated pending segment:
// "merged-" plus the oldest segment's base name, stripping any accumulated
// "merged-" prefixes so repeated consolidations don't grow the filename
// indefinitely (which could eventually exceed the Windows path length limit).
func mergedPendingName(segments []string) string {
	base := filepath.Base(segments[0])
	base = strings.TrimPrefix(base, "merged-")
	return "merged-" + base
}

// renameOverwriting moves src to dst, removing any existing dst first.  On
// Windows os.Rename refuses to overwrite, and a stable merged-pending name can
// already exist from a previous consolidation.
func renameOverwriting(src, dst string) error {
	_ = os.Remove(dst)
	return os.Rename(src, dst)
}

// IsUnreadableVideo reports whether a video file cannot be probed by ffprobe
// (or reports zero duration).  Used by the thumbnail-failure eviction path to
// distinguish a permanently-corrupt recording from a healthy file whose
// thumbnail upload failed transiently (image-host outage) — only genuinely
// unreadable files are evicted.
func IsUnreadableVideo(videoPath string) bool {
	dur, err := VideoDurationSeconds(videoPath)
	return err != nil || dur <= 0
}

// VideoDurationSeconds probes the duration of a video file using ffprobe, falling back
// to parsing ffmpeg's "Duration:" stderr output when ffprobe fails.
func VideoDurationSeconds(videoPath string) (float64, error) {
	var out []byte
	var err error
	doProbe := func() {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer probeCancel()
		out, err = config.FFprobeCommandContext(probeCtx,
			"-v", "error",
			"-show_entries", "format=duration",
			"-of", "default=noprint_wrappers=1:nokey=1",
			videoPath,
		).Output()
	}
	if err := config.AcquireFFmpegFor(config.FFmpegAcquireTimeout); err != nil {
		return 0, fmt.Errorf("could not acquire ffmpeg slot to probe: %w", err)
	}
	doProbe()
	config.ReleaseFFmpeg()
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

// mergeVideos concatenates multiple video files into a single output.
//
// Strategy (two-phase):
//  1. Fast path — concat demuxer with stream copy.  Works when all segments
//     share the same codec parameters and each (except the first) starts with
//     a keyframe.  If it succeeds AND the output contains all expected streams
//     with reasonable duration, we are done.
//  2. Fallback — concat demuxer with re-encode (libx264 + AAC).  Fixes
//     codec-parameter mismatch, missing-keyframe, and broken-timestamp issues
//     that the stream-copy path cannot handle.
//
// In both phases the output is always normalized: pending segments carry
// absolute server timestamps (PTS=5044s from LL-HLS) that would otherwise
// make the merged file unseekable and break playback after the first segment
// boundary.
func mergeVideos(inputs []string, outputPath string) error {
	if len(inputs) < 2 {
		return fmt.Errorf("need at least 2 inputs, got %d", len(inputs))
	}

	// ── Pre-flight: validate every segment ──────────────────────────────
	for _, p := range inputs {
		dur, err := VideoDurationSeconds(p)
		if err != nil || dur <= 0 {
			if err == nil {
				err = fmt.Errorf("zero duration")
			}
			return fmt.Errorf("segment %s is invalid: %w", filepath.Base(p), err)
		}
	}

	// ── Build concat list for both phases ────────────────────────────────
	listFile, err := os.CreateTemp("", "concat-*.txt")
	if err != nil {
		return fmt.Errorf("create concat list: %w", err)
	}
	defer listFile.Close()
	defer os.Remove(listFile.Name())

	for _, p := range inputs {
		abs, aErr := filepath.Abs(p)
		if aErr != nil {
			abs = p
		}
		escaped := strings.ReplaceAll(abs, "'", "'\\''")
		if _, wErr := fmt.Fprintf(listFile, "file '%s'\n", escaped); wErr != nil {
			return fmt.Errorf("write concat list: %w", wErr)
		}
	}

	// Compute total input duration for validation later.
	var totalInputDur float64
	for _, p := range inputs {
		if d, e := VideoDurationSeconds(p); e == nil {
			totalInputDur += d
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := config.AcquireFFmpegHeavyFor(config.FFmpegHeavyAcquireTimeout); err != nil {
		return err
	}
	defer config.ReleaseFFmpegHeavy()

	// ── Phase 1: Fast stream-copy concat ────────────────────────────────
	streamCopyOK := config.FFmpegCommandContext(ctx,
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c", "copy",
		"-fflags", "+genpts+igndts",
		"-movflags", "+faststart",
		outputPath,
	).Run()

	if streamCopyOK == nil {
		// Probe the output — check it has reasonable duration and at least a video track.
		outputDur, probeErr := VideoDurationSeconds(outputPath)
		if probeErr == nil && outputDur > 0 && (totalInputDur <= 0 || outputDur >= totalInputDur*0.5) {
			tmpPath := outputPath + ".normalized.mp4"
			if err := config.FFmpegCommandContext(ctx,
				"-y",
				"-fflags", "+genpts+igndts",
				"-i", outputPath,
				"-c", "copy",
				"-fflags", "+genpts",
				"-movflags", "+faststart",
				tmpPath,
			).Run(); err != nil {
				os.Remove(tmpPath)
				return nil // best-effort — return the concat output as-is
			}
			os.Remove(outputPath)
			if rErr := os.Rename(tmpPath, outputPath); rErr != nil {
				os.Remove(tmpPath)
			}
			return nil
		}
	}

	// Stream-copy failed or produced a bad output — clean up and re-try.
	os.Remove(outputPath)

	// ── Phase 2: Re-encode concat ───────────────────────────────────────
	reEncodeArgs := []string{
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile.Name(),
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "23",
		"-c:a", "aac",
		"-b:a", "128k",
	}

	// Force timestamps to start at zero on every segment boundary.
	// The setpts/asetpts filters reset the timeline so there are no gaps.
	reEncodeArgs = append(reEncodeArgs,
		"-vf", "setpts=PTS-STARTPTS",
		"-af", "asetpts=PTS-STARTPTS",
		"-movflags", "+faststart",
		outputPath,
	)

	if err := config.FFmpegCommandContext(ctx, reEncodeArgs...).Run(); err != nil {
		os.Remove(outputPath)
		return fmt.Errorf("merge re-encode: %w", err)
	}

	return nil
}

// handleMinDurationAndMerge checks whether a finalized video file meets the
// minimum-duration threshold.  If the feature is disabled the check is skipped
// and the caller proceeds to upload normally.
//
// When a video is shorter than the threshold it is moved into a pending
// directory.  If pending segments already exist (including the one just moved),
// they are merged together and the merged result is uploaded via
// MoveToOutputDir.
//
// A failed merge (or a merged output that is still below the threshold) never
// triggers an upload: the current video — or the consolidated merged output —
// is parked with the pending segments instead, so the next recording can try
// merging everything again.
//
// Returns true if the video was handled (deferred to pending or merged+uploaded)
// so the caller should stop processing it.  Returns false when the caller
// should proceed with its normal upload logic (only when the feature is
// disabled or the video meets the threshold with no pending segments).
// isDefinitiveStop reports whether the recording ended for good (channel went
// offline / entered a private show / hit max duration / was handed off /
// paused) rather than pausing to reconnect a briefly-expired HLS session.
// Chaturbate rotates its HLS token every ~20 minutes, which shows up as a stall
// reason of "stream session expired (no new segments)" or "...— reconnecting".
// Those are continuations, not ends: sub-threshold fragments must be held and
// merged with the next cycle instead of being uploaded as lone ~20-minute clips
// (matching streamlink/yt-dlp/hls.js token-refresh behaviour).
func isDefinitiveStop(endReason string) bool {
	if endReason == "" {
		return false
	}
	if isContinuationReason(endReason) {
		return false
	}
	return true
}

func (ch *Channel) handleMinDurationAndMerge(videoPath, endReason string) bool {
	// The fragment's end reason is only knowable right now — once a short
	// fragment is parked in .pending (and later merged) the reason would be
	// discarded, hiding WHY the recording was dropped. Log it on every park
	// so the node logs expose the real drop cause instead of the merged
	// recording's empty end_reason in Supabase.
	if endReason == "" {
		endReason = "unknown"
	}
	// Serialize this channel's merges against processAllPendingSegments (which
	// runs at startup/periodic orphan cleanup) so the shared stable merged-*
	// name can never be renamed out from under a concurrent upload. The unique
	// scratch name alone was not enough: two flows merging the SAME segments
	// both renameOverwriting the stable path, deleting the file between the
	// hash and the upload ("could not hash" / "0/5 successful").
	mmu := mergeMu(ch.Config.Username)
	mmu.Lock()
	defer mmu.Unlock()

	mu := pendingMu(ch.Config.Username)
	mu.Lock()

	minDur := ch.Config.MinDurationBeforeUpload
	if minDur <= 0 {
		if server.Config != nil && server.Config.MinDurationBeforeUpload > 0 {
			minDur = server.Config.MinDurationBeforeUpload
		} else {
			mu.Unlock()
			return false // feature disabled — proceed normally
		}
	}

	dur, err := VideoDurationSeconds(videoPath)
	if err != nil {
		// On probe failure, keep the video pending rather than uploading a corrupt/short file
		pendingDir := pendingSegmentsDir(ch.Config.Username)
		if mErr := os.MkdirAll(pendingDir, 0777); mErr == nil {
			destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
			if rErr := os.Rename(videoPath, destPath); rErr == nil {
				mu.Unlock()
				ch.Warn("min-duration: could not probe %s (%v) — deferred to pending (ended: %s)", filepath.Base(videoPath), err, endReason)
				return true
			}
		}
		mu.Unlock()
		ch.Error("min-duration: probe failed and could not move %s to pending: %v — keeping it in place, NOT uploading",
			filepath.Base(videoPath), err)
		return true
	}

	if dur >= float64(minDur) {
		// Video is long enough. Before uploading, check if there are
		// pending segments to merge with.
		segments := collectPendingSegments(ch.Config.Username)
		if len(segments) == 0 {
			mu.Unlock()
			return false // no pending — proceed normally
		}

		// Quarantine any corrupt pending segments so a single bad file cannot
		// poison the merge. If nothing probe-valid remains, upload the current
		// (already >= min-duration) video normally instead of parking it.
		segments, quarantined := quarantineInvalidSegments(ch.Config.Username, segments)
		if quarantined > 0 {
			ch.Warn("min-duration: quarantined %d corrupt pending segment(s) for %s", quarantined, ch.Config.Username)
		}
		if len(segments) == 0 {
			mu.Unlock()
			return false // no valid pending — proceed with normal upload
		}

		// Merge pending segments with the current video.  The output is written
		// to a unique ".merging-*" scratch name in the pending dir — never
		// "videoPath + .merged.mp4" in the recording dir, which a crash
		// mid-merge left behind as a stray file the orphan scan later uploaded
		// while the (still-present) pending segments were merged again into the
		// next recording, uploading the same content twice.  Scratch files are
		// excluded from segment collection, so a crash here is recoverable
		// (recoverMergeScratch) and never double-uploads.
		// Release the lock during the potentially long ffmpeg encode.
		pendingDir := pendingSegmentsDir(ch.Config.Username)
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		allInputs := append(mergeInputs, videoPath)
		stableName := mergedPendingName(allInputs)
		mergedPath := filepath.Join(pendingDir, fmt.Sprintf(".merging-%d-%s", time.Now().UnixNano(), stableName))
		ch.Info("min-duration: merging %d pending segment(s) with %s (ended: %s)", len(mergeInputs), filepath.Base(videoPath), endReason)
		mu.Unlock()
		mErr := mergeVideos(allInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			// Hold, don't upload: a failed merge must never push any of this
			// content out as a separate upload that bypasses the threshold.
			// Park the current video with the pending segments so the next
			// recording can attempt the merge again.
			if pkErr := moveToPendingDir(videoPath, ch.Config.Username); pkErr != nil {
				ch.Error("min-duration: merge failed (%v) and could not park %s in pending: %v — keeping it in place, NOT uploading",
					mErr, filepath.Base(videoPath), pkErr)
			} else {
				ch.Error("min-duration: merge failed: %v — holding %s with %d pending segment(s) for a future merge",
					mErr, filepath.Base(videoPath), len(mergeInputs))
			}
			return true
		}

		mergedDur, probeErr := VideoDurationSeconds(mergedPath)
		holdPending := false
		if probeErr != nil {
			ch.Warn("min-duration: could not probe merged output (%v) — holding it in pending, NOT uploading", probeErr)
			holdPending = true
		} else if mergedDur < float64(minDur) && !isDefinitiveStop(endReason) {
			ch.Warn("min-duration: merged output for %s is %.1fs (< %ds) — holding it in pending, NOT uploading",
				filepath.Base(mergedPath), mergedDur, minDur)
			holdPending = true
		}
		if holdPending {
			// The current video is already incorporated into the merged output:
			// drop the original and park the consolidated file with the pending
			// segments so nothing below the threshold is ever uploaded.
			_ = os.Remove(videoPath)
			// Remove the inputs BEFORE renaming the scratch to its stable name,
			// so a crash here leaves the finished content as a sole scratch
			// that recoverMergeScratch finalizes — never a stable merged-*
			// file sitting next to segments that already contain its content
			// (which would double-merge).
			mu.Lock()
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			mu.Unlock()
			if rErr := renameOverwriting(mergedPath, filepath.Join(pendingDir, stableName)); rErr != nil {
				ch.Error("min-duration: could not park merged output in pending: %v — keeping it in place, NOT uploading",
					rErr)
			}
			return true
		}

		// Consume the inputs first: the finished merge is now the only copy of
		// this content.  A crash before the rename leaves a sole scratch that
		// recoverMergeScratch finalizes exactly once.
		mu.Lock()
		for _, s := range mergeInputs {
			os.Remove(s)
		}
		_ = os.Remove(videoPath)
		mu.Unlock()

		// Rename the unique scratch output to the STABLE merged-* name and
		// upload from there, so the archive keeps a clean
		// "<user>_YYYY-MM-DD_....mp4" filename and username extraction stays
		// correct.  Safe because mergeMu serializes this channel's merges — no
		// concurrent flow can renameOverwrite the stable path out from under
		// this upload.
		stablePath := filepath.Join(pendingDir, stableName)
		if rErr := renameOverwriting(mergedPath, stablePath); rErr != nil {
			ch.Warn("min-duration: could not rename merged output to %s: %v — uploading from scratch name", stableName, rErr)
		} else {
			mergedPath = stablePath
		}

		if ch.Config.Compress {
			ch.Info("min-duration: merged -> %s (%.1fs), compressing before upload", filepath.Base(mergedPath), mergedDur)
			ch.CompressFile(mergedPath, endReason)
		} else {
			ch.Info("min-duration: merged -> %s (%.1fs), proceeding with upload", filepath.Base(mergedPath), mergedDur)
			ch.MoveToOutputDir(mergedPath, endReason)
		}
		return true
	}

	// Video is too short — move to pending
	pendingDir := pendingSegmentsDir(ch.Config.Username)
	if err := os.MkdirAll(pendingDir, 0777); err != nil {
		mu.Unlock()
		ch.Error("min-duration: cannot create pending dir %s: %v — keeping %s in place, NOT uploading short video",
			pendingDir, err, filepath.Base(videoPath))
		return true
	}

	destPath := filepath.Join(pendingDir, filepath.Base(videoPath))
	if err := os.Rename(videoPath, destPath); err != nil {
		mu.Unlock()
		ch.Error("min-duration: cannot move %s to pending: %v — keeping it in place, NOT uploading short video", filepath.Base(videoPath), err)
		return true
	}
	ch.Info("min-duration: %s is %.1fs (< %ds) — deferred to pending (ended: %s)", filepath.Base(videoPath), dur, minDur, endReason)

	// If multiple segments have now accumulated, merge them and check the
	// combined duration. Only upload if the merged result meets the threshold.
	segments := collectPendingSegments(ch.Config.Username)
	segments, quarantined := quarantineInvalidSegments(ch.Config.Username, segments)
	if quarantined > 0 {
		ch.Warn("min-duration: quarantined %d corrupt pending segment(s) for %s", quarantined, ch.Config.Username)
	}
	if len(segments) > 1 {
		// Write to a unique scratch name first so the output can never collide
		// with an existing "merged-*" input segment, then finalize below.
		stableName := mergedPendingName(segments)
		mergedPath := filepath.Join(pendingDir, fmt.Sprintf(".merging-%d-%s", time.Now().UnixNano(), stableName))
		mergeInputs := make([]string, len(segments))
		copy(mergeInputs, segments)
		ch.Info("min-duration: merging %d pending segment(s) (ended: %s)", len(mergeInputs), endReason)
		mu.Unlock()
		mErr := mergeVideos(mergeInputs, mergedPath)
		if mErr != nil {
			os.Remove(mergedPath) // clean up partial output
			ch.Error("min-duration: merge failed: %v — segments remain pending for next recording", mErr)
			return true
		}
		mu.Lock()

		mergedDur, mErr := VideoDurationSeconds(mergedPath)
		if mErr != nil {
			// Consume the inputs first, then keep the merged result pending
			// under the stable name (best-effort) rather than risking an
			// upload of unconfirmed duration — the min-duration guarantee must
			// never be violated just because probing failed.  A crash between
			// the removal and the rename leaves a sole scratch that
			// recoverMergeScratch finalizes.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			stablePath := filepath.Join(pendingDir, stableName)
			if rErr := renameOverwriting(mergedPath, stablePath); rErr == nil {
				mergedPath = stablePath
			}
			ch.Warn("min-duration: could not probe merged result (%v) — keeping pending", mErr)
			mu.Unlock()
			return true
		}

		if mergedDur >= float64(minDur) || isDefinitiveStop(endReason) {
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			ch.Info("min-duration: merged %d segments = %.1fs — uploading (definitive stop flush)", len(mergeInputs), mergedDur)
			mu.Unlock()

			// Rename the unique scratch output to the STABLE merged-* name and
			// upload from there, so the archive keeps a clean
			// "<user>_YYYY-MM-DD_....mp4" filename and username extraction stays
			// correct. Safe because mergeMu serializes this channel's merges —
			// no concurrent flow can renameOverwrite the stable path out from
			// under this upload. (The scratch name is still used for the ffmpeg
			// encode so the output never collides with a "merged-*" input.)
			stablePath := filepath.Join(pendingDir, stableName)
			if rErr := renameOverwriting(mergedPath, stablePath); rErr == nil {
				mergedPath = stablePath
			}
			if ch.Config.Compress {
				ch.CompressFile(mergedPath, endReason)
			} else {
				ch.MoveToOutputDir(mergedPath, endReason)
			}
		} else {
			// Keep pending under the stable name so the next merge dedupes it.
			// Consume the inputs first: a crash before the rename leaves a sole
			// scratch that recoverMergeScratch finalizes — never a stable
			// merged-* file next to segments that already contain its content.
			for _, s := range mergeInputs {
				os.Remove(s)
			}
			stablePath := filepath.Join(pendingDir, stableName)
			if rErr := renameOverwriting(mergedPath, stablePath); rErr == nil {
				mergedPath = stablePath
			}
			ch.Info("min-duration: merged %d segments = %.1fs (< %ds) — still pending", len(mergeInputs), mergedDur, minDur)
			mu.Unlock()
		}
	} else {
		var lone string
		if len(segments) == 1 {
			lone = segments[0]
		}
		mu.Unlock()
		if lone != "" && isDefinitiveStop(endReason) {
			ch.Info("min-duration: definitive stop — uploading lone pending segment %s", filepath.Base(lone))
			thumb := GenerateThumbnailForFile(lone)
			thumbURL := thumb.ThumbURL
			spriteURL := thumb.SpriteURL
			previewURL := thumb.PreviewURL
			if UploadOrphanedFile(lone, thumbURL, spriteURL, previewURL) {
				_ = os.Remove(lone)
			}
		}
	}

	return true // video was deferred to pending (or merged+uploaded)
}

// FlushPendingSegmentsAtBoundary merges/uploads any short segments parked in
// .pending after a session drain. force=true (the FINAL drain before a runner
// is torn down, or a graceful StopSession) uploads sub-threshold residual
// segments instead of deleting or re-parking them — a short clip uploaded
// beats content lost forever with the VM.
func FlushPendingSegmentsAtBoundary(force bool) {
	processAllPendingSegments(force)
}

// processAllPendingSegments scans all .pending/* subdirectories and processes any
// accumulated segments.  If segments exist they are merged together and uploaded.
// This is called during startup orphan cleanup so short segments from a previous
// run don't stay pending forever when no new recording arrives, and again after
// every session drain (force=true on the final drain before VM teardown).
func processAllPendingSegments(force bool) {
	// Scan exactly the pending root the min-duration system actually uses:
	// pendingSegmentsDir resolves to OutputDir/.pending when OutputDir is set,
	// otherwise "videos/.pending".  Scanning BOTH roots (as an older version
	// did) could process a user's segments as two separate sets, and legacy
	// segments stranded in videos/.pending would never be touched.
	root := "videos"
	if server.Config != nil && server.Config.OutputDir != "" {
		root = server.Config.OutputDir
	}
	pendingRoot := filepath.Join(root, ".pending")
	userDirs, err := os.ReadDir(pendingRoot)
	if err != nil {
		return
	}
	for _, ud := range userDirs {
		if !ud.IsDir() {
			continue
		}
		processPendingUserSegments(ud.Name(), force)
	}
}

// processPendingUserSegments processes one user's .pending directory: it
// quarantines corrupt segments, merges the rest (when min-duration is enabled),
// and uploads anything that meets the threshold. It takes the per-user mergeMu
// so it can never run concurrently with handleMinDurationAndMerge on the same
// segments — that concurrency previously let both flows renameOverwriting the
// shared stable merged-* name out from under each other's upload.
// force=true uploads sub-threshold residual segments (final drain).
func processPendingUserSegments(username string, force bool) {
	mmu := mergeMu(username)
	mmu.Lock()
	defer mmu.Unlock()

	minDur := resolveMinDurationBeforeUpload(username)

	mu := pendingMu(username)

	mu.Lock()
	// Reconcile crash-left ".merging-*" scratch before anything else, so a
	// finished merge whose inputs were consumed is finalized (not stranded) and
	// a partial mid-merge scratch is dropped while its inputs still exist.
	recoverMergeScratch(pendingSegmentsDir(username))
	segments := collectPendingSegmentsInDir(pendingSegmentsDir(username))
	if len(segments) < 1 {
		mu.Unlock()
		return
	}

	// Age out sub-threshold segments that have been pending too long.  The
	// user does not want <threshold uploads, and a segment that has not been
	// extended by newer recordings within pendingMaxAge means the stream is
	// not coming back — keeping it would leak disk forever with no upload
	// ever possible.  Delete it outright.
	// On the final drain (force) we never delete: everything uploads, so the
	// pending dir is emptied by the upload path below, not by age.
	if !force {
		deleteStalePendingSegments(pendingSegmentsDir(username))
		segments = collectPendingSegmentsInDir(pendingSegmentsDir(username))
		if len(segments) < 1 {
			mu.Unlock()
			return
		}
	}

	// Quarantine corrupt segments so they can never poison a merge.
	segments, quarantined := quarantineInvalidSegments(username, segments)
	if quarantined > 0 {
		recoveryLogf(pendingSegmentsDir(username), "quarantined %d corrupt pending segment(s) for %s", quarantined, username)
	}
	if len(segments) < 1 {
		mu.Unlock()
		return
	}

	// If min-duration is disabled, upload everything directly (legacy behavior).
	if minDur <= 0 {
		for _, s := range segments {
			if IsUploadInFlight(s) {
				continue // the pipeline owns this file right now
			}
			recoveryLogf(s, "recovery: uploading pending segment %s", filepath.Base(s))
			mu.Unlock()
			thumb := GenerateThumbnailForFile(s)
			thumbURL := thumb.ThumbURL
			spriteURL := thumb.SpriteURL
			previewURL := thumb.PreviewURL
			// Keep the segment on failure: UploadOrphanedFile already removes the
			// local copy on success (when DeleteLocalAfterUpload is set); a failed
			// upload must leave the file in .pending for the next retry instead of
			// discarding content that was never uploaded.
			if UploadOrphanedFile(s, thumbURL, spriteURL, previewURL) {
				_ = os.Remove(s)
			}
			mu.Lock()
		}
		_ = os.Remove(pendingSegmentsDir(username))
		mu.Unlock()
		return
	}

	// Min-duration is enabled — merge segments and only upload if threshold met.
	segCopy := make([]string, len(segments))
	copy(segCopy, segments)
	mu.Unlock()

	// A single remaining segment can still be uploaded if it alone meets
	// the threshold (e.g. all other segments were quarantined as corrupt).
	// On the final drain (force) upload it regardless of duration.
	if len(segCopy) == 1 {
		if !IsUploadInFlight(segCopy[0]) {
			singleDur, dErr := VideoDurationSeconds(segCopy[0])
			if dErr == nil && (force || singleDur >= float64(minDur)) {
				if force && singleDur < float64(minDur) {
					recoveryLogf(segCopy[0], "recovery: final drain — single pending segment = %.1fs, uploading (below threshold allowed)", singleDur)
				} else {
					recoveryLogf(segCopy[0], "recovery: single pending segment = %.1fs (>= %ds) — uploading", singleDur, minDur)
				}
				thumb := GenerateThumbnailForFile(segCopy[0])
				thumbURL := thumb.ThumbURL
				spriteURL := thumb.SpriteURL
				previewURL := thumb.PreviewURL
				// Keep on failure so the next scan can retry (see above).
				if UploadOrphanedFile(segCopy[0], thumbURL, spriteURL, previewURL) {
					_ = os.Remove(segCopy[0])
				}
			} else {
				if dErr != nil {
					recoveryLogf(segCopy[0], "recovery: could not probe single pending segment (%v) — keeping pending", dErr)
				} else {
					recoveryLogf(segCopy[0], "recovery: single pending segment = %.1fs (< %ds) — keeping pending", singleDur, minDur)
				}
			}
		}
		return
	}

	pendingDir := pendingSegmentsDir(username)
	stableName := mergedPendingName(segments)
	// Write to a unique scratch name first so the output can never
	// collide with an existing "merged-*" input, then finalize.
	mergedPath := filepath.Join(pendingDir, fmt.Sprintf(".merging-%d-%s", time.Now().UnixNano(), stableName))
	recoveryLogf(segments[0], "recovery: merging %d pending segments for %s", len(segments), username)
	if err := mergeVideos(segCopy, mergedPath); err != nil {
		os.Remove(mergedPath)
		recoveryLogf(segments[0], "recovery: merge failed for %s: %v — leaving segments pending", username, err)
		return
	}

	mergedDur, durErr := VideoDurationSeconds(mergedPath)
	if durErr != nil || (!force && mergedDur < float64(minDur)) {
		// Keep pending under the stable name so the next merge dedupes it.
		// Consume the inputs first: a crash between the removal and the rename
		// leaves a sole scratch that recoverMergeScratch finalizes.
		mu.Lock()
		for _, s := range segCopy {
			os.Remove(s)
		}
		mu.Unlock()
		if rErr := renameOverwriting(mergedPath, filepath.Join(pendingDir, stableName)); rErr == nil {
			mergedPath = filepath.Join(pendingDir, stableName)
		}
		if durErr != nil {
			recoveryLogf(mergedPath, "recovery: could not probe merged duration (%v) — keeping pending", durErr)
		} else {
			recoveryLogf(mergedPath, "recovery: merged = %.1fs (< %ds) — keeping pending", mergedDur, minDur)
		}
		return
	}

	var totalInputDur float64
	for _, s := range segCopy {
		if d, e := VideoDurationSeconds(s); e == nil {
			totalInputDur += d
		}
	}
	if totalInputDur > 0 && !force && mergedDur < totalInputDur*0.5 {
		// Keep pending under the stable name too.  Remove the inputs first so
		// a crash here leaves a sole scratch that recoverMergeScratch
		// finalizes.
		mu.Lock()
		for _, s := range segCopy {
			os.Remove(s)
		}
		mu.Unlock()
		if rErr := renameOverwriting(mergedPath, filepath.Join(pendingDir, stableName)); rErr == nil {
			mergedPath = filepath.Join(pendingDir, stableName)
		}
		recoveryLogf(mergedPath, "recovery: merged output %.1fs is <50%% of total input %.1fs — streams may be incompatible, keeping pending",
			mergedDur, totalInputDur)
		return
	}

	mu.Lock()
	for _, s := range segCopy {
		os.Remove(s)
	}
	mu.Unlock()
	if force {
		recoveryLogf(mergedPath, "recovery: final drain — merged = %.1fs, uploading (below threshold allowed)", mergedDur)
	} else {
		recoveryLogf(mergedPath, "recovery: merged = %.1fs (>= %ds) — uploading", mergedDur, minDur)
	}
	// Rename the unique scratch output to the STABLE merged-* name and
	// upload from there so the archive keeps a clean filename and correct
	// username (see handleMinDurationAndMerge). mergeMu guarantees no
	// concurrent merge can renameOverwrite it out from under this upload.
	stablePath := filepath.Join(pendingDir, stableName)
	if rErr := renameOverwriting(mergedPath, stablePath); rErr == nil {
		mergedPath = stablePath
	}
	thumb := GenerateThumbnailForFile(mergedPath)
	thumbURL := thumb.ThumbURL
	spriteURL := thumb.SpriteURL
	previewURL := thumb.PreviewURL
	// Keep the merged file on failure: UploadOrphanedFile removes the local
	// copy itself on success; a failed upload must leave the stable merged-*
	// file in .pending so the next scan merges/retries it instead of discarding
	// content that never reached any host.
	if UploadOrphanedFile(mergedPath, thumbURL, spriteURL, previewURL) {
		_ = os.Remove(mergedPath)
	}
}

// videoExt returns true if the extension is a known video extension.
func videoExt(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".mp4" || ext == ".mkv" || ext == ".ts"
}

// isSidecar returns true if the filename appears to be a sidecar/preview file.
// Note: .video.muxed.mp4 is the final muxed output (not a sidecar), while
// .video.mp4 and .audio.mp4 are raw A/V track files (sidecars).
// IsFinalizingTemp reports whether name is an ffmpeg finalizer scratch file
// ("<base>.finalizing.<ext>").  The finalizer writes to this temporary name
// and only renames it to the final output on success; a crash mid-finalize
// leaves a partial file behind that must never be uploaded, thumbnailed, or
// merged into pending segments.  (ext alone is not enough — a "foo.finalizing.mp4"
// has extension .mp4 and would otherwise be treated as a real video.)
func IsFinalizingTemp(name string) bool {
	return strings.Contains(name, ".finalizing")
}

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
		strings.HasSuffix(name, ".audio.mp4") ||
		IsFinalizingTemp(name) ||
		strings.Contains(name, ".deleting.") ||
		// Merge scratch: mergeVideos writes to ".merging-<nano>-<stable>" in the
		// pending dir (and a "<output>.normalized.mp4" temp).  A crash mid-merge
		// leaves a partial file that must never be uploaded, thumbnailed, or
		// merged as a real segment.  Recovered finished merges are renamed to
		// the stable "merged-*" name by recoverMergeScratch.
		strings.Contains(name, ".merging-")
}
