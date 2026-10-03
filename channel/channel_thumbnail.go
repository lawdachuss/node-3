package channel

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	thumbWidth      = 1280
	thumbHeight     = 720
	spriteFrames    = 16
	spriteCols      = 4
	spriteRows      = 4
	spriteFrameW    = 640
	spriteFrameH    = 360
	previewWidth    = 320
	previewDuration = 6.0 // seconds
	previewSegments = 12  // number of smooth clips to stitch (each ~0.5s)

	// thumbnailAssetTimeout caps the TOTAL time generateThumbnailForFile waits
	// for its three asset goroutines (thumbnail/sprite/preview).  The assets
	// run in parallel and are collected concurrently under this one budget,
	// and each goroutine signals done the instant its FIRST host succeeds
	// (mirrors finish in the background): generation is fast keyframe seeks,
	// so a healthy file completes in well under a minute.  A goroutine that
	// exceeds the cap keeps running — its onHost callback still persists late
	// URLs to the DB — the local result just stops waiting on it.  Keeping
	// this far below the pipeline's 15-minute thumbnail stage cap guarantees
	// the stage can never again stall out (the seen "timed out after 10m0s"
	// / "exceeded 15m0s" node-13 wheels came from a sequential 3×10-min wait
	// PLUS a mirrored-upload wait for the slowest host).
	thumbnailAssetTimeout = 6 * time.Minute

	// assetTimeoutCap bounds how long ONE asset goroutine may spend on its
	// ffmpeg work on a slow host.  It is the longest internal budget an asset
	// can burn without having produced anything, so anything watching an
	// abandoned asset from the outside (see lateAssetTrackBound in
	// asset_late.go) must allow longer than this before concluding the asset
	// is never coming.
	assetTimeoutCap = 90 * time.Minute

	// thumbnailFFmpegAcquireTimeout bounds how long thumbnail-scoped work
	// waits for a free lightweight ffmpeg slot.  Much shorter than the global
	// FFmpegAcquireTimeout (5 min): a wave of sprite/preview extractions can
	// spike the pool, and 5 minutes × (16 tiles + 12 clips + concat) per
	// video turns a transient burst into a 10+ minute stall.  30s rides out a
	// brief contention wave; if the pool stays saturated, individual
	// tiles/clips degrade via the existing blank-frame fallbacks instead of
	// stalling the pipeline.
	thumbnailFFmpegAcquireTimeout = 30 * time.Second
)

// ThumbnailResult holds the generated thumbnail, sprite, and preview URLs
// along with their mirror URLs from all hosts.
type ThumbnailResult struct {
	ThumbURL       string
	SpriteURL      string
	PreviewURL     string
	ThumbMirrors   map[string]string // host -> URL
	SpriteMirrors  map[string]string // host -> URL
	PreviewMirrors map[string]string // host -> URL

	// Unavailable is set when NOTHING was produced because this node could not
	// run ffmpeg at all (see ErrFFmpegUnavailable) — as opposed to the file
	// being unthumbnailable.  Callers must keep the recording and retry later
	// rather than finalizing it without its thumbnail.
	Unavailable bool
}

// OnHostUploadFunc is called the instant a single host finishes uploading
// a thumbnail/sprite/preview asset.  The caller can persist the URL to the
// database immediately instead of waiting for all hosts to finish.
type OnHostUploadFunc func(assetType, host, url string)

// generateThumbnail is the channel-scoped wrapper — logs go to the channel log.
// If onHost is non-nil it is called the instant each host succeeds for each
// asset (thumb, sprite, preview) so the caller can save to DB immediately.
func (ch *Channel) generateThumbnail(videoPath string, onHost OnHostUploadFunc) ThumbnailResult {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { ch.Info(f, a...) },
		func(f string, a ...interface{}) { ch.Warn(f, a...) },
		func(f string, a ...interface{}) { ch.Error(f, a...) },
		onHost,
	)
}

// GenerateThumbnailForFile is a standalone thumbnail generator that can be
// called outside of a channel context (e.g. for pre-existing video files).
func GenerateThumbnailForFile(videoPath string) ThumbnailResult {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { log.Printf("[thumb] "+f, a...) },
		func(f string, a ...interface{}) { log.Printf("[thumb:warn] "+f, a...) },
		func(f string, a ...interface{}) { log.Printf("[thumb:err] "+f, a...) },
		nil,
	)
}

// ThumbnailExists reports whether the THUMBNAIL — the asset actually shown on
// the video card — is already stored for this file, either in preview_images or
// on the recordings row.  Used by cleanup paths (watcher, orphan scans) to
// decide whether the local video source may be deleted: without a stored
// thumbnail the local file is the only way to ever (re)generate one.
func ThumbnailExists(filePath string) bool {
	name := filepath.Base(filePath)
	if thumb, _, _ := server.LoadPreviewLinks(name); thumb != "" {
		return true
	}
	if thumb, _, _ := server.LoadRecordingThumbnails(name); thumb != "" {
		return true
	}
	return false
}

// generateThumbnailForFile creates a static thumbnail (JPEG), a multi-frame sprite
// sheet (JPEG), and an animated WEBP hover preview (6 seconds of smooth clips
// from across the full video).  All three are uploaded to remote hosts and the
// URLs returned.  Local temp files are always cleaned up.
//
// JPEG is used for thumbnail and sprite because:
//   - All image hosts support it (Pixhost, ImgBB, Catbox)
//   - mjpeg encoder is fast (minimal encoding lag)
//   - Small filesize with good visual quality
//
// Animated WEBP is used for the preview because:
//   - ~90% smaller than GIF at same quality, full 24-bit color
//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
//   - Hosted by Catbox (primary) and ImgBB (fallback) — both accept WEBP
//     (the IamAPTBA/ImgBB API no-op, image hosts all accept it) so the
//     preview never depends on MP4-specific hosts like PixelDrain.
//
// The preview uses filter_complex to extract 12 short clips (~0.5s each)
// from evenly-spaced points across the full video and stitch them together.
// Each clip has consecutive frames for fully smooth motion, unlike a
// frame-sampled timelapse where every frame is a jarring jump.
//
// Thumbnail, sprite, and preview run in parallel with independent timeouts:
//   - thumbnail: 5 min  (single-frame seek)
//   - sprite:    15 min (seeks through full video for long recordings)
//   - preview:   15 min (12× trim + stitch, H.264 encode)
//
// Using separate contexts prevents one task from being killed prematurely
// when a long video causes another to exceed a shared short timeout.
// fileExists returns true if the path exists and is a regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}

// waitForOutputFile polls with backoff until the file is confirmed to exist.
// On Windows an AV scanner (Defender, etc.) can briefly hold an exclusive lock
// on a freshly-created file, making os.Stat return ERROR_FILE_NOT_FOUND even
// though ffmpeg exited successfully. Retrying with a short delay resolves it.
func waitForOutputFile(path string) bool {
	for delay := 0; delay < 5; delay++ {
		if fileExists(path) {
			return true
		}
		time.Sleep(time.Duration(50*(1<<delay)) * time.Millisecond) // 50, 100, 200, 400, 800 ms
	}
	return false
}

// runFFmpegParallel runs fn for each index in [0, n) with up to workers
// goroutines running at once.  Each fn is responsible for acquiring its own
// ffmpeg slot (AcquireFFmpeg/ReleaseFFmpeg) so the global ffmpegSem bounds
// total concurrency across all channels.  Returns the first error (others are
// still awaited; their results are discarded).
func runFFmpegParallel(workers, n int, fn func(i int) error) error {
	if workers < 1 {
		workers = 1
	}
	if n < 1 {
		return nil
	}
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	slots := make(chan struct{}, workers)
	for i := 0; i < n; i++ {
		wg.Add(1)
		slots <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := fn(idx); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

// acquireFFmpegSlot is an indirection over config.AcquireFFmpegFor so tests can
// simulate a saturated pool without waiting out the real 30s budget or reaching
// into the config package's unexported semaphore.  Production always uses the
// direct call; the point of the seam is that runFFmpegFresh's mapping from a
// pool wait to `errFFmpegSlotStarved` is itself testable — if that mapping ever
// regresses, a starved pool silently goes back to being reported (and retried)
// as a seek failure.
var acquireFFmpegSlot = config.AcquireFFmpegFor

// runFFmpegFresh runs an ffmpeg command with a fresh timeout context of the
// given duration, acquiring a global ffmpeg slot for the duration.  Each
// invocation gets its own context so a retry (slow seek, blank fallback,
// regenerate, assembly) is never killed instantly by a shared per-task context
// whose budget was already fully consumed by a failed fast seek — otherwise
// the retry fails with an immediate "context deadline exceeded" even though it
// never got a chance to run.
func runFFmpegFresh(timeout time.Duration, args ...string) error {
	if err := acquireFFmpegSlot(thumbnailFFmpegAcquireTimeout); err != nil {
		// Name the real cause.  AcquireFFmpegFor returns a bare ctx.Err()
		// ("context deadline exceeded"), and every caller below used to report
		// that as "fast seek failed … retrying with slow seek" — which is not
		// just a misleading log line: it sent callers into a slow-seek retry
		// that needs the same slot, so a starved pool spent 30s+ per tile
		// discovering nothing.  Wrapping it as both the specific reason and
		// the node-tool class lets callers skip those fallbacks and lets the
		// pipeline treat it as "retry after the node recovers".
		return ffmpegSlotWaitError(thumbnailFFmpegAcquireTimeout)
	}
	defer config.ReleaseFFmpeg()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return config.FFmpegCommandContext(ctx, args...).Run()
}

func generateThumbnailForFile(videoPath string, info, warn, errFn func(string, ...interface{}), onHost OnHostUploadFunc) ThumbnailResult {
	var result ThumbnailResult
	ext := strings.ToLower(filepath.Ext(videoPath))
	if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
		return result
	}

	// Watch every error this run reports for the signature of ffmpeg failing to
	// START, so the caller can tell a broken node from an unthumbnailable file
	// (see ffmpeg_unavailable.go).  Wrapping errFn covers every call site at
	// once, including the three asset goroutines that close over it.
	var spawnFailures int32
	rawErrFn := errFn
	errFn = func(format string, a ...interface{}) {
		if IsFFmpegSpawnFailureText(fmt.Sprintf(format, a...)) {
			atomic.AddInt32(&spawnFailures, 1)
		}
		rawErrFn(format, a...)
	}

	st, err := os.Stat(videoPath)
	if err != nil {
		errFn("thumb: file not found %s: %v", filepath.Base(videoPath), err)
		return result
	}
	// Skip files too small to contain video frames — ffmpeg returns
	// exit code -22 (EINVAL) on header-only fMP4 from failed streams.
	if st.Size() < 100*1024 {
		errFn("thumb: skipping %s: too small (%d bytes)", filepath.Base(videoPath), st.Size())
		return result
	}

	baseName := filepath.Base(videoPath)

	// Probe video duration — short dedicated timeout.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()

	var dur float64
	if err := config.AcquireFFmpegFor(thumbnailFFmpegAcquireTimeout); err != nil {
		errFn("thumb: could not acquire ffmpeg slot to probe %s: %v — continuing without duration", filepath.Base(videoPath), err)
	} else {
		probeOut, probeErr := config.FFprobeCommandContext(probeCtx,
			"-v", "error",
			"-show_entries", "format=duration",
			"-of", "default=noprint_wrappers=1:nokey=1",
			videoPath,
		).Output()
		config.ReleaseFFmpeg() // release immediately — the 3 goroutines below also need slots
		if probeErr == nil {
			var parseErr error
			dur, parseErr = strconv.ParseFloat(strings.TrimSpace(string(probeOut)), 64)
			if parseErr != nil {
				log.Printf("WARN: could not parse probe duration %q: %v", strings.TrimSpace(string(probeOut)), parseErr)
			}
		} else {
			// Was logged nowhere before, so a node where ffprobe cannot run at
			// all left only the downstream "duration unknown" traces — which
			// read as a property of the FILE.  Report the probe failure itself,
			// and let the wrapper above classify it (it is the earliest and
			// cheapest signal that this node's tools are unusable).
			errFn("thumb: duration probe failed for %s: %v — continuing with unknown duration", filepath.Base(videoPath), probeErr)
		}
	}

	// MPEG-TS has no seek index: every -ss before -i still demuxes from byte 0,
	// so extracting 16 sprite tiles + 12 preview clips from a long .ts
	// recording means up to ~28 full-file scans (minutes to tens of minutes
	// for 2-4 h videos on shared hosts).  Remux ONCE to a seekable temp .mp4
	// (stream copy — no re-encode, purely I/O-bound) and extract from that:
	// the single remux read/write replaces all 28 scans, and the subsequent
	// keyframe seeks are instant index-based jumps.
	workPath := videoPath
	if ext == ".ts" {
		remuxTimeout := 15 * time.Minute
		if dur > 0 {
			extra := time.Duration(dur/3600.) * 10 * time.Minute
			if extra > 20*time.Minute {
				extra = 20 * time.Minute
			}
			remuxTimeout += extra
		}
		remuxCtx, remuxCancel := context.WithTimeout(context.Background(), remuxTimeout)
		defer remuxCancel()
		workDir, mkErr := os.MkdirTemp("", "thumb-seekable-")
		if mkErr != nil {
			errFn("thumb: mkdir temp for seekable remux: %v", mkErr)
		} else {
			defer os.RemoveAll(workDir)
			seekablePath := filepath.Join(workDir, "seekable.mp4")
			if err := config.AcquireFFmpegFor(thumbnailFFmpegAcquireTimeout); err != nil {
				errFn("thumb: could not acquire ffmpeg slot for seekable remux of %s: %v — extracting directly", baseName, err)
			} else {
				remuxErr := config.FFmpegCommandContext(remuxCtx,
					"-y",
					"-i", videoPath,
					"-c", "copy",
					"-movflags", "+faststart",
					seekablePath,
				).Run()
				config.ReleaseFFmpeg()
				if remuxErr == nil && fileExists(seekablePath) {
					workPath = seekablePath
					info("thumb: remuxed %s to seekable temp for fast extraction", baseName)
				} else {
					errFn("thumb: seekable remux failed for %s: %v — extracting directly", baseName, remuxErr)
				}
			}
		}
	}

	// A failed first probe (dur == 0) would make the sprite sample only the
	// first 150 s and the preview take the bounded fallback path.  The remuxed
	// faststart .mp4 probes reliably — re-probe it so long .ts recordings still
	// get full-cover sprites and properly segmented previews.
	if dur <= 0 && workPath != videoPath {
		if err := config.AcquireFFmpegFor(thumbnailFFmpegAcquireTimeout); err != nil {
			errFn("thumb: could not acquire ffmpeg slot to re-probe remuxed %s: %v", baseName, err)
		} else {
			reprobeCtx, reprobeCancel := context.WithTimeout(context.Background(), 60*time.Second)
			reprobeOut, reprobeErr := config.FFprobeCommandContext(reprobeCtx,
				"-v", "error",
				"-show_entries", "format=duration",
				"-of", "default=noprint_wrappers=1:nokey=1",
				workPath,
			).Output()
			reprobeCancel()
			config.ReleaseFFmpeg()
			if reprobeErr == nil {
				if d, parseErr := strconv.ParseFloat(strings.TrimSpace(string(reprobeOut)), 64); parseErr == nil && d > 0 {
					dur = d
					info("thumb: re-probed remuxed seekable %s duration: %.0fs", baseName, dur)
				}
			}
		}
	}

	thumbDone := make(chan string, 1)
	spriteDone := make(chan string, 1)
	previewDone := make(chan string, 1)

	// Timeouts scale with video duration so very long recordings (2-4h)
	// never get silently dropped for exceeding a fixed timeout on a slow
	// host.  Fast keyframe seeks mean these are usually quick, but a short
	// .ts remux or a heavily-loaded shared ffmpeg slot can push a long
	// extract over the old fixed 15-minute ceiling.
	base := 5 * time.Minute
	if dur > 0 {
		// scale linearly: 5 min at 0s → up to 25 min at 4h
		extra := time.Duration(dur/3600.) * 5 * time.Minute
		if extra > 20*time.Minute {
			extra = 20 * time.Minute
		}
		base += extra
	}
	thumbTimeout := base
	spriteTimeout := base * 3
	if spriteTimeout > assetTimeoutCap {
		spriteTimeout = assetTimeoutCap
	}
	previewTimeout := base * 3
	if previewTimeout > assetTimeoutCap {
		previewTimeout = assetTimeoutCap
	}

	// Mirror URL maps — populated by goroutines, read after they complete.
	var thumbMirrors, spriteMirrors, previewMirrors map[string]string
	var mirrorsMu sync.Mutex

	// ── Single thumbnail (static frame near the 10% mark) ──────────────────
	// Independent 90-second context: seeking to a single frame is always fast.
	go func() {
		// Hold this asset's upload lifetime open for DeleteSidecarFiles: the
		// collect below stops waiting for this goroutine after
		// thumbnailAssetTimeout while it keeps uploading mirrors in the
		// background, and deleting the sidecar then broke every one of those
		// remaining attempts ("imgpile: open file: ...").
		markThumbnailAssetUploadStart(videoPath)
		defer markThumbnailAssetUploadDone(videoPath)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [thumb] generating thumbnail for %s: %v", baseName, r)
				select {
				case thumbDone <- "":
				default:
				}
			}
		}()
		thumbJPG := videoPath + ".thumb.jpg"
		defer os.Remove(thumbJPG)

		seekPos := "00:00:03"
		if dur > 0 && dur < 3 {
			seekPos = fmt.Sprintf("%.2f", dur*0.5)
		} else if dur > 0 {
			seekPos = fmt.Sprintf("%.2f", dur*0.1)
		}

		// generateThumb extracts the single thumbnail frame (slow=true uses
		// slow seek for codecs where fast seek crashes ffmpeg).  It uses a
		// fresh context per attempt so the slow-seek retry is never killed
		// instantly by a shared context exhausted by a failed fast seek.
		generateThumb := func(slow bool) error {
			args := []string{"-y"}
			if slow {
				args = append(args, "-i", workPath, "-ss", seekPos)
			} else {
				args = append(args, "-ss", seekPos, "-i", workPath)
			}
			args = append(args,
				"-vframes", "1",
				"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					thumbWidth, thumbHeight, thumbWidth, thumbHeight),
				"-c:v", "mjpeg",
				"-q:v", "5",
				thumbJPG,
			)
			return runFFmpegFresh(thumbTimeout, args...)
		}

		// NO outer ffmpeg slot is held here: runFFmpegFresh acquires (and
		// releases) its own slot per invocation, so an extra acquire held for
		// the goroutine's whole lifetime — including the multi-minute image-
		// host uploads below — would nest a second acquire inside the first.
		// When enough thumbnail goroutines overlapped (a file-rotation wave on
		// a loaded node), every ffmpegSem slot became such an outer hold and
		// each holder blocked forever on its inner acquire: a permanent
		// hold-and-wait deadlock that froze every pipeline at
		// stage=thumbnail_upload with updated==created (video uploads in the
		// parallel goroutine still succeeded; files were never cleaned up and
		// disk filled). Seen on node-13 after b16ac44 made this call route
		// through runFFmpegFresh.
		err := generateThumb(false)

		if IsFFmpegSlotStarved(err) {
			// No ffmpeg ran, so this is not a seek failure: the slow-seek retry
			// would need the same slot and time out identically.  Report the
			// pool, not the seek.
			errFn("thumb: %s: %v", baseName, err)
		} else if err != nil {
			// Fast seek failed; retry with slow seek (-ss after -i).
			// This handles certain codecs/formats where fast seek causes
			// ffmpeg to crash (exit 0xffffffea on Windows).
			errFn("thumb: fast seek failed for %s: %v, retrying with slow seek", baseName, err)
			err = generateThumb(true)
		}

		// The freshly-written .thumb.jpg can be briefly invisible to os.Stat
		// (Windows AV scanners) OR deleted by a concurrent flow processing the
		// same video (a second pipeline's DeleteSidecarFiles). Either way a
		// missing file at upload time showed up as "pixhost: stat file".
		// Poll briefly before uploading.
		if err == nil && !waitForOutputFile(thumbJPG) {
			err = fmt.Errorf("thumbnail file %s never appeared", filepath.Base(thumbJPG))
		}

		if err != nil {
			errFn("thumb: failed for %s: %v", baseName, err)
			thumbDone <- ""
			return
		}

		// StartAll launches every host in parallel; WaitForPrimary returns the
		// instant the primary URL is known.  The remaining mirrors finish in
		// the background, accumulating into the result maps and firing onHost
		// per success.  sess.Wait() then keeps the temp file alive until every
		// mirror has read it, so it can be deleted safely (mainly on
		// Windows); the pipeline waits for this only up to the shared
		// thumbnailAssetTimeout deadline in the collect below.
		sess := uploader.NewMultiImageUploader().StartAll(thumbJPG, func(host, url string) {
			mirrorsMu.Lock()
			if thumbMirrors == nil {
				thumbMirrors = make(map[string]string)
			}
			thumbMirrors[host] = url
			mirrorsMu.Unlock()
			if onHost != nil {
				onHost("thumb", host, url)
			}
		})
		primaryURL, primaryHost, primaryOK := sess.WaitForPrimary()
		sess.Wait()
		if primaryOK {
			info("thumb: ✓ %s (primary host: %s)", baseName, primaryHost)
			thumbDone <- primaryURL
			return
		}
		errFn("thumb: upload failed for %s — all hosts rejected or saturated", baseName)
		thumbDone <- ""
	}()

	// ── Sprite sheet (4×4 grid covering the full video duration) ───────────
	// Each frame is spriteFrameW×spriteFrameH px; total image is
	// (spriteCols*spriteFrameW) × (spriteRows*spriteFrameH) = 2560×1440.
	// Using 640×360 frames so HiDPI/Retina displays get sharp previews.
	//
	// FAST PATH: instead of decoding the ENTIRE video with a sequential
	// fps=1/INTERVAL filter (which costs one full decode pass — minutes to
	// tens of minutes for 2-4 h recordings on shared hosts), each tile is
	// extracted with an input keyframe seek (-ss before -i).  A seek decodes
	// only ~1 GOP (~1-4 s of frames) instead of the whole file, so the sprite
	// takes ~1 s regardless of recording length.  Tiles land on the keyframe
	// nearest each target position, which is visually identical in a 640×360
	// contact-sheet tile.
	//
	// Independent 15-minute context: extraction runs N quick seeks; a short
	// shared context would cause SIGKILL ("signal: killed") and silently skip
	// sprite generation on slow hosts.
	go func() {
		// Same lifetime hold as the thumbnail goroutine above — see the
		// markThumbnailAssetUploadStart comment in upload_tracker.go.
		markThumbnailAssetUploadStart(videoPath)
		defer markThumbnailAssetUploadDone(videoPath)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [sprite] generating sprite for %s: %v", baseName, r)
				select {
				case spriteDone <- "":
				default:
				}
			}
		}()
		spriteJPG := videoPath + ".sprite.jpg"
		defer os.Remove(spriteJPG)

		tileDir, err := os.MkdirTemp("", "sprite-tiles-")
		if err != nil {
			errFn("sprite: mkdir temp for %s: %v", baseName, err)
			spriteDone <- ""
			return
		}
		defer os.RemoveAll(tileDir)

		// Positions for the 16 tiles, evenly spaced across the video.
		// Clamp so the last tile never seeks past the end.
		positions := make([]float64, spriteFrames)
		if dur > 0 {
			spacing := dur / float64(spriteFrames)
			for i := range positions {
				positions[i] = spacing * float64(i)
			}
		} else {
			// No duration available — fall back to fixed 10 s spacing like
			// the old fps=1/10 filter did.  For a long recording this samples
			// only the first ~2.5 minutes; duration probing (including the
			// remuxed re-probe above) normally succeeds, so this is a last
			// resort that keeps the grid usable rather than skipping the
			// sprite entirely.
			errFn("sprite: duration unknown for %s — tiles use fixed 10 s spacing (covers only the first %.0fs", baseName, 10.0*float64(spriteFrames))
			for i := range positions {
				positions[i] = 10.0 * float64(i)
			}
		}

		// generateSprite extracts all 16 tiles via keyframe seeks (in parallel,
		// bounded by the global ffmpeg semaphore), then assembles them into the
		// contact sheet with one tile=4x4 pass.
		generateSprite := func() error {
			// Extract one tile via a fast keyframe seek.  Each tile acquires its
			// own ffmpeg slot so tiles run concurrently across the pool; -threads 1
			// keeps a single seek from grabbing the whole CPU when N run at once.
			extractTile := func(i int) error {
				pos := positions[i]
				tilePath := filepath.Join(tileDir, fmt.Sprintf("t%d.jpg", i))
				vf := fmt.Sprintf(
					"scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					spriteFrameW, spriteFrameH,
					spriteFrameW, spriteFrameH,
				)
				// Fast keyframe seek: -ss before -i.  Decodes only the GOP
				// containing the target position.
				seekArgs := []string{
					"-y",
					"-threads", "1",
					"-ss", fmt.Sprintf("%.3f", pos),
					"-i", workPath,
					"-frames:v", "1",
					"-vf", vf,
					"-c:v", "mjpeg",
					"-q:v", "5",
					tilePath,
				}
				err := runFFmpegFresh(spriteTimeout, seekArgs...)
				if IsFFmpegSlotStarved(err) {
					// Nothing ran.  Both remaining steps need a slot, so skip them,
					// but do NOT fall through to a missing tile: the assembly below
					// reads t0..tN with the image2 demuxer, which STOPS at the first
					// missing index — a hole would truncate the 4×4 grid into a
					// nearly-empty sheet that still looks like a real sprite.  For
					// genuine seek failures the blank-frame fallback exists exactly
					// to keep the sequence contiguous; on pool starvation that is not
					// worth another 30s wait, so fail the sprite with the real cause.
					errFn("sprite: tile %d for %s: %v", i, baseName, err)
					return err
				}
				if err != nil || !fileExists(tilePath) {
					// Fast seek failed (some codecs crash with -ss before -i on
					// Windows, exit 0xffffffea).  Retry with slow seek (-ss after
					// -i); the decode cost is bounded by the GOP, not the file.
					// Fresh context so the retry gets its own full budget.
					errFn("sprite: tile %d fast seek failed for %s: %v — retrying with slow seek", i, baseName, err)
					slowArgs := []string{
						"-y",
						"-threads", "1",
						"-i", workPath,
						"-ss", fmt.Sprintf("%.3f", pos),
						"-frames:v", "1",
						"-vf", vf,
						"-c:v", "mjpeg",
						"-q:v", "5",
						tilePath,
					}
					err = runFFmpegFresh(spriteTimeout, slowArgs...)
					if IsFFmpegSlotStarved(err) {
						// The slow seek hit the same wall.  Do not spend one more
						// acquire wait on a blank frame that cannot be produced
						// either; report the pool as the cause, same as above.
						errFn("sprite: tile %d for %s: %v", i, baseName, err)
						return err
					}
				}
				// If the tile still failed, generate a blank (black) frame so
				// the 4×4 grid is complete and the sprite can still be used.
				// This happens on certain keyframes where the codec/container
				// is corrupted (exit 1 even with slow seek) — e.g. node-4 and
				// node-13 show "tile 9 at 6076s: exit status 1" on long
				// recordings where a mid-stream IDR frame is unreadable.
				if err != nil || !fileExists(tilePath) {
					errFn("sprite: tile %d at %.0fs skipped (both seeks failed): %v", i, pos, err)
					blankArgs := []string{
						"-y",
						"-f", "lavfi",
						"-i", fmt.Sprintf("color=c=black:s=%dx%d:d=0.04:r=1", spriteFrameW, spriteFrameH),
						"-frames:v", "1",
						"-c:v", "mjpeg",
						"-q:v", "5",
						tilePath,
					}
					blankErr := runFFmpegFresh(spriteTimeout, blankArgs...)
					if !fileExists(tilePath) {
						if IsFFmpegSlotStarved(blankErr) {
							// The pool is the problem, so a partially-built grid is
							// the honest outcome — fail rather than return a sheet
							// full of holes as if it were complete.
							return fmt.Errorf("sprite: tile %d at %.0fs: %w", i, pos, blankErr)
						}
						// Even the blank-frame fallback failed — log but do not
						// abort the entire sprite; the grid will have a missing
						// tile but is still usable.
						errFn("sprite: tile %d at %.0fs: blank fallback also failed", i, pos)
					}
					return nil // non-fatal: grid is still usable with a missing tile
				}
				return nil
			}

			// Spawn tiles concurrently — 16 sequential ffmpeg spawns (each with
			// process startup + GOP decode) is the sprite's dominant cost.
			// Cap workers at NumCPU so we don't thrash; ffmpegSem bounds the
			// true concurrent ffmpeg count across the whole fleet of channels.
			workers := runtime.NumCPU()
			if workers > spriteFrames {
				workers = spriteFrames
			}
			if err := runFFmpegParallel(workers, spriteFrames, extractTile); err != nil {
				return err
			}

			// Assemble the 16 tiles into the 4×4 contact sheet via the image2
			// demuxer + tile filter (one cheap pass over the tiny JPEGs).
			pattern := filepath.ToSlash(filepath.Join(tileDir, "t%d.jpg"))
			err := runFFmpegFresh(spriteTimeout,
				"-y",
				"-framerate", "1",
				"-start_number", "0",
				"-i", pattern,
				"-vf", fmt.Sprintf("tile=%dx%d", spriteCols, spriteRows),
				"-frames:v", "1",
				"-c:v", "mjpeg",
				"-q:v", "5",
				spriteJPG,
			)
			return err
		}

		err = generateSprite()

		// Same missing-file-at-upload-time race as the thumbnail: a concurrent
		// flow can DeleteSidecarFiles on this video while we're between ffmpeg
		// and the upload. Poll + regenerate before uploading.
		if err == nil && !fileExists(spriteJPG) {
			errFn("sprite: %s missing after generation — regenerating", filepath.Base(spriteJPG))
			err = generateSprite()
		}
		if err == nil && !fileExists(spriteJPG) {
			err = fmt.Errorf("sprite file %s never appeared", filepath.Base(spriteJPG))
		}

		if err != nil {
			errFn("sprite: failed for %s: %v", baseName, err)
			spriteDone <- ""
			return
		}

		sess := uploader.NewMultiImageUploader().StartAll(spriteJPG, func(host, url string) {
			mirrorsMu.Lock()
			if spriteMirrors == nil {
				spriteMirrors = make(map[string]string)
			}
			spriteMirrors[host] = url
			mirrorsMu.Unlock()
			if onHost != nil {
				onHost("sprite", host, url)
			}
		})
		primaryURL, primaryHost, primaryOK := sess.WaitForPrimary()
		sess.Wait()
		if primaryOK {
			info("sprite: ✓ %s (primary host: %s)", baseName, primaryHost)
			spriteDone <- primaryURL
			return
		}
		errFn("sprite: upload failed for %s — all hosts rejected or saturated", baseName)
		spriteDone <- ""
	}()

	// ── Animated WEBP hover preview (smooth clips from across the video, 6s) ─
	// Animated WEBP is used instead of GIF because:
	//   - ~90% smaller file size for the same visual quality
	//   - Full 24-bit color (vs 256-color palette in GIF)
	//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
	//   - Catbox (primary) and ImgBB (fallback) both accept WEBP files, so no
	//     MP4-only host (PixelDrain) is required.
	//
	// Instead of isolated frame sampling (which produces a jerky slideshow),
	// we extract 12 short continuous clips (~0.5s each) from evenly-spaced
	// points across the video and stitch them together.  Each clip has fully
	// smooth motion because frames within it are consecutive.
	//
	//   <6 sec:  no segmenting, plays whole video at normal speed
	//   1 min:   12 clips × 0.5s = 6s (5s between clips)
	//   60 min:  12 clips × 0.5s = 6s (5 min between clips)
	//
	// FAST PATH: the old implementation decoded the ENTIRE video once through
	// a filter_complex (every trim=start=… branch forced a full sequential
	// decode from frame 0), which took minutes to tens of minutes for 2-4 h
	// recordings and even hit the 15-minute timeout — producing truncated
	// ~1.7 s previews.  Each clip is now extracted with an input keyframe
	// seek (-ss before -i), which decodes only ~1 GOP (~1-4 s of frames)
	// before the clip start, so the whole preview takes ~1-2 s regardless of
	// recording length.  Clips land on the keyframe nearest each target
	// position — visually identical for 0.5 s hover clips.
	//
	// Uploaded to Catbox.moe (free, permanent, CDN-backed) with ImgBB
	// as fallback — both return direct file URLs suitable for embedding.
	go func() {
		// Same lifetime hold as the thumbnail goroutine above — the preview is
		// the asset that actually gets abandoned by the collect deadline (its
		// 12-clip encode + upload is the slowest), and its late ImgBB/ImgPile
		// mirrors are what failed on the deleted sidecar.
		markThumbnailAssetUploadStart(videoPath)
		defer markThumbnailAssetUploadDone(videoPath)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [preview] generating preview for %s: %v", baseName, r)
				select {
				case previewDone <- "":
				default:
				}
			}
		}()
		previewPath := videoPath + ".preview.webp"
		// Remove on the final return, but NOT if ffmpeg failed — leave the
		// file on disk so a later restart or manual retry can pick it up.
		var previewGenerated bool
		defer func() {
			if previewGenerated {
				os.Remove(previewPath)
			}
		}()

		// waitForPreviewFile polls with backoff until the preview file is
		// confirmed to exist.  On Windows, an AV scanner (Defender, etc.) can
		// briefly hold an exclusive lock on a newly-created file, causing
		// os.Stat to return ERROR_FILE_NOT_FOUND even though ffmpeg exited
		// successfully.  Retrying with a short delay resolves this.
		waitForPreviewFile := func() bool {
			for delay := 0; delay < 5; delay++ {
				if fileExists(previewPath) {
					return true
				}
				time.Sleep(time.Duration(50*(1<<delay)) * time.Millisecond) // 50, 100, 200, 400, 800 ms
			}
			return false
		}

		var err error
		if dur <= previewDuration || dur <= 0 {
			// Short known video (0 < dur <= previewDuration) — play the whole
			// thing at normal speed.  libwebp needs a constant frame rate, so
			// -r 15 forces CFR.
			// Unknown duration (probe failed): NEVER transcode a possibly
			// multi-hour recording in full just because the probe failed —
			// that turned into a 45-minute full-file webp encode.  Bound the
			// sample to the first previewDuration seconds: identical for
			// genuinely short videos, and it caps a long recording at 6 s of
			// work.
			args := []string{"-y", "-i", workPath}
			if dur <= 0 {
				errFn("preview: duration unknown for %s — bounding preview to the first %.0fs", baseName, previewDuration)
				args = append(args, "-t", fmt.Sprintf("%.2f", previewDuration))
			}
			args = append(args,
				"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
				"-c:v", "libwebp",
				"-lossless", "0",
				"-q:v", "60",
				"-r", "15",
				"-an",
				previewPath,
			)
			err = runFFmpegFresh(previewTimeout, args...)
		} else {
			// Extract 12 short clips via keyframe seeks into a temp dir, then
			// concat them and run ONE final WEBP encode.  Each clip is tiny
			// (0.5 s at 320 px), so the intermediate re-encode cost is
			// negligible compared to the full-decode the old filter_complex
			// approach required.
			segDuration := previewDuration / float64(previewSegments)
			step := dur / float64(previewSegments)

			clipDir, mkErr := os.MkdirTemp("", "preview-clips-")
			if mkErr != nil {
				err = fmt.Errorf("mkdir temp clips: %w", mkErr)
			} else {
				defer os.RemoveAll(clipDir)

				// Extract all 12 clips concurrently (bounded by the global ffmpeg
				// semaphore).  The clips are independent keyframe seeks, so 12
				// sequential ffmpeg spawns become ~1 round-trip each.
				extractClip := func(i int) error {
					midpoint := step * (float64(i) + 0.5)
					start := midpoint - segDuration/2
					if start+segDuration > dur {
						start = dur - segDuration
					}
					if start < 0 {
						start = 0
					}

					clipPath := filepath.Join(clipDir, fmt.Sprintf("c%d.mp4", i))
					seekArgs := []string{
						"-y",
						"-threads", "1",
						"-ss", fmt.Sprintf("%.3f", start),
						"-i", workPath,
						"-t", fmt.Sprintf("%.3f", segDuration),
						"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos,setpts=PTS-STARTPTS", previewWidth),
						"-c:v", "libx264",
						"-preset", "ultrafast",
						"-crf", "23",
						"-r", "15",
						"-an",
						clipPath,
					}
					seekErr := runFFmpegFresh(previewTimeout, seekArgs...)
					if IsFFmpegSlotStarved(seekErr) {
						// Same reasoning as the sprite tiles: every remaining step
						// (slow seek, black placeholder, concat, WEBP encode) needs a
						// slot, so bail with the pool as the cause instead of
						// reporting a seek failure that never happened.
						errFn("preview: clip %d for %s: %v", i, baseName, seekErr)
						return seekErr
					}
					if seekErr != nil || !fileExists(clipPath) {
						if seekErr != nil {
							errFn("preview: clip %d fast seek failed for %s: %v — retrying with slow seek", i, baseName, seekErr)
						}
						slowArgs := []string{
							"-y",
							"-threads", "1",
							"-i", workPath,
							"-ss", fmt.Sprintf("%.3f", start),
							"-t", fmt.Sprintf("%.3f", segDuration),
							"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos,setpts=PTS-STARTPTS", previewWidth),
							"-c:v", "libx264",
							"-preset", "ultrafast",
							"-crf", "23",
							"-r", "15",
							"-an",
							clipPath,
						}
						seekErr = runFFmpegFresh(previewTimeout, slowArgs...)
						if IsFFmpegSlotStarved(seekErr) {
							// Nothing ran, so the black placeholder below would only
							// wait on the pool again.  Fail with the real cause.
							errFn("preview: clip %d for %s: %v", i, baseName, seekErr)
							return seekErr
						}
						if seekErr != nil || !fileExists(clipPath) {
							// Both fast and slow seek failed — generate a black
							// placeholder clip so the concat step can still run.
							// This happens on corrupted keyframes in long recordings.
							errFn("preview: clip %d at %.2fs skipped (both seeks failed): %v", i, start, seekErr)
							blackArgs := []string{
								"-y",
								"-f", "lavfi",
								"-i", fmt.Sprintf("color=c=black:s=%dx%d:d=%.3f:r=15", previewWidth, previewWidth*9/16, segDuration),
								"-c:v", "libx264",
								"-preset", "ultrafast",
								"-crf", "23",
								"-an",
								clipPath,
							}
							_ = runFFmpegFresh(previewTimeout, blackArgs...)
							if !fileExists(clipPath) {
								return fmt.Errorf("clip %d at %.2fs: both seeks and blank fallback failed", i, start)
							}
							return nil // non-fatal: preview still usable with blank segment
						}
					}
					if !fileExists(clipPath) {
						return fmt.Errorf("clip %d at %.2fs never appeared", i, start)
					}
					return nil
				}

				workers := runtime.NumCPU()
				if workers > previewSegments {
					workers = previewSegments
				}
				if cErr := runFFmpegParallel(workers, previewSegments, extractClip); cErr != nil {
					err = cErr
				}

				if err == nil {
					// Concat the clips with the concat demuxer, then run ONE
					// final WEBP encode over the stitched 6 s.
					listPath := filepath.Join(clipDir, "list.txt")
					var list strings.Builder
					for i := 0; i < previewSegments; i++ {
						clipPath := filepath.ToSlash(filepath.Join(clipDir, fmt.Sprintf("c%d.mp4", i)))
						list.WriteString(fmt.Sprintf("file '%s'\n", clipPath))
					}
					if werr := os.WriteFile(listPath, []byte(list.String()), 0o666); werr != nil {
						err = fmt.Errorf("write concat list: %w", werr)
					} else {
						err = runFFmpegFresh(previewTimeout,
							"-y",
							"-f", "concat",
							"-safe", "0",
							"-i", listPath,
							"-c:v", "libwebp",
							"-lossless", "0",
							"-q:v", "60",
							"-r", "15",
							"-an",
							previewPath,
						)
					}
				}
			}

			// If extraction or the concat encode failed, fall back to a simple
			// single-clip preview from the middle of the video.  The old
			// filter_complex could also silently produce no output on some
			// videos (e.g. unusual stream timing), so keep the fallback.
			//
			// Use a fresh context so the fallback gets its own 5-minute
			// timeout instead of inheriting a nearly-expired shared context.
			if IsFFmpegSlotStarved(err) {
				// The single-clip fallback is another ffmpeg run against the same
				// saturated pool, so it cannot succeed — skip it and keep the
				// pool's fault as the reason.
				errFn("preview: %s: skipping the single-clip fallback — %v", baseName, err)
			} else if err != nil || !fileExists(previewPath) {
				if err != nil {
					errFn("preview: clip extraction failed for %s: %v, trying simple fallback", baseName, err)
				} else {
					errFn("preview: clip concat produced no output for %s, trying simple fallback", baseName)
				}
				err = runFFmpegFresh(5*time.Minute,
					"-y",
					"-ss", fmt.Sprintf("%.2f", dur*0.3),
					"-i", workPath,
					"-t", fmt.Sprintf("%.2f", previewDuration),
					"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
					"-c:v", "libwebp",
					"-lossless", "0",
					"-q:v", "60",
					"-r", "15",
					"-an",
					previewPath,
				)
			}
		}

		if err != nil {
			errFn("preview: failed for %s: %v", baseName, err)
			previewDone <- ""
			return
		}

		if !waitForPreviewFile() {
			errFn("preview: ffmpeg exited successfully but produced no output file for %s", baseName)
			previewDone <- ""
			return
		}

		previewGenerated = true

		sess := uploader.NewMultiImageUploader().StartAll(previewPath, func(host, url string) {
			mirrorsMu.Lock()
			if previewMirrors == nil {
				previewMirrors = make(map[string]string)
			}
			previewMirrors[host] = url
			mirrorsMu.Unlock()
			if onHost != nil {
				onHost("preview", host, url)
			}
		})
		primaryURL, primaryHost, primaryOK := sess.WaitForPrimary()
		sess.Wait()
		if primaryOK {
			info("preview: ✓ %s (primary host: %s)", baseName, primaryHost)
			previewDone <- primaryURL
			return
		}
		errFn("preview: all hosts failed or saturated for %s (cosmetic — thumbnail+sprite still saved)", baseName)
		previewDone <- ""
	}()

	// Collect the three assets under ONE shared deadline.  An asset that
	// overruns it is abandoned, not failed: the goroutine keeps uploading in
	// the background and its onHost callback still persists the URL, so the
	// abandon is logged as informational and the real outcome is recorded
	// separately.  See asset_late.go — the error-level version of this line
	// was 6,160 phantom preview "failures" against ~22 actually-missing
	// thumbnails.
	assets := []assetCollect{
		{"thumbnail", &result.ThumbURL, thumbDone},
		{"sprite", &result.SpriteURL, spriteDone},
		{"preview", &result.PreviewURL, previewDone},
	}
	collectAssets(baseName, assets, thumbnailAssetTimeout, info, warn, errFn)

	mirrorsMu.Lock()
	result.ThumbMirrors = copyMap(thumbMirrors)
	result.SpriteMirrors = copyMap(spriteMirrors)
	result.PreviewMirrors = copyMap(previewMirrors)
	mirrorsMu.Unlock()

	// Nothing at all was produced AND ffmpeg failed to start at least once:
	// this is a node fault, not an unthumbnailable recording.  Say so loudly
	// and let the caller keep the file for a later retry instead of finalizing
	// a row with no thumbnail — which is how 22 recordings lost theirs.
	failures := atomic.LoadInt32(&spawnFailures)
	if thumbnailNodeToolUnavailable(result.ThumbURL, failures) {
		result.Unavailable = true
		errFn("thumb: ffmpeg could not run on this node (%d failed invocation(s)) — treating %s as retry-later, NOT as an unthumbnailable file",
			failures, baseName)
	}

	return result
}
