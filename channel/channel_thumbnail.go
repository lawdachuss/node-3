package channel

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
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
	previewHeight   = 180 // 16:9 of previewWidth — fixed so concat never fails on aspect drift
	previewDuration = 18.0 // total seconds of the stitched montage
	previewSegments = 12   // number of smooth clips to stitch (each ~1.5s)
)

// generateThumbnail is the channel-scoped wrapper — logs go to the channel log.
func (ch *Channel) generateThumbnail(videoPath string) (thumbURL, spriteURL, previewURL string) {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { ch.Info(f, a...) },
		func(f string, a ...interface{}) { ch.Error(f, a...) },
	)
}

// GenerateThumbnailForFile is a standalone thumbnail generator that can be
// called outside of a channel context (e.g. for pre-existing video files).
func GenerateThumbnailForFile(videoPath string) (thumbURL, spriteURL, previewURL string) {
	return generateThumbnailForFile(videoPath,
		func(f string, a ...interface{}) { log.Printf("[thumb] "+f, a...) },
		func(f string, a ...interface{}) { log.Printf("[thumb:err] "+f, a...) },
	)
}

// generateThumbnailForFile creates a static thumbnail (JPEG), a multi-frame sprite
// sheet (JPEG), and an MP4 hover preview (6 seconds of smooth clips from
// across the full video).  All three are uploaded to remote hosts and the
// URLs returned.  Local temp files are always cleaned up.
//
// JPEG is used for thumbnail and sprite because:
//   - All image hosts support it (Pixhost, Catbox)
//   - mjpeg encoder is fast (minimal encoding lag)
//   - Small filesize with good visual quality
//
// MP4 is used for the animated preview because:
//   - ~90% smaller than GIF at same quality
//   - Full 24-bit color (no 256-color palette limit)
//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
//   - Catbox accepts MP4 files (free, permanent, CDN-backed)
//
// The preview uses filter_complex to extract 12 short clips (~1.5s each)
// spanning the full duration — anchored at the start and end of the stream
// with the rest evenly spaced 0%..100% — and stitches them together.
// Each clip has consecutive frames for fully smooth motion, unlike a
// frame-sampled timelapse where every frame is a jarring jump.
//
// Thumbnail, sprite, and preview run in parallel with independent timeouts:
//   - thumbnail: 5 min  (single-frame seek)
//   - sprite:    15 min (16 input seeks — instant per frame, not full decode)
//   - preview:   15 min (12 input seeks × 1.5s clip, not full decode)
//
// All three use input seeking (-ss before -i) so ffmpeg jumps to the target
// time and reads only what it needs — a 1-hour recording no longer requires
// decoding the whole file just to make a thumbnail.
//
// Using separate contexts prevents one task from being killed prematurely
// when a long video causes another to exceed a shared short timeout.
// fileExists returns true if the path exists and is a regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}

// runFFmpeg runs ffmpeg, returning any stderr output (capped to the last 2 KB)
// alongside the error.  ffmpeg writes its diagnostics to stderr, so calling
// .Run() directly only gives the caller "exit status 1" and the real cause
// (missing codec, invalid filter, image2 muxer requiring -update, etc.) — and
// therefore the actual fix — is lost.
func runFFmpeg(ctx context.Context, args ...string) (string, error) {
	cmd := config.FFmpegCommandContext(ctx, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		out := stderr.String()
		if len(out) > 2000 {
			out = out[len(out)-2000:]
		}
		return out, err
	}
	return "", nil
}

func generateThumbnailForFile(videoPath string, info, errFn func(string, ...interface{})) (thumbURL, spriteURL, previewURL string) {
	ext := strings.ToLower(filepath.Ext(videoPath))
	if ext != ".mp4" && ext != ".mkv" && ext != ".ts" {
		return "", "", ""
	}

	st, err := os.Stat(videoPath)
	if err != nil {
		errFn("thumb: file not found %s: %v", filepath.Base(videoPath), err)
		return "", "", ""
	}
	// Skip files too small to contain video frames — ffmpeg returns
	// exit code -22 (EINVAL) on header-only fMP4 from failed streams.
	if st.Size() < 100*1024 {
		errFn("thumb: skipping %s: too small (%d bytes)", filepath.Base(videoPath), st.Size())
		return "", "", ""
	}

	baseName := filepath.Base(videoPath)

	// Probe video duration — short dedicated timeout.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer probeCancel()

	var dur float64
	config.AcquireFFmpeg()
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
	}

	// Compute the interval so we get exactly spriteFrames frames spread
	// evenly across the whole video.  Clamp to at least 0.1 s.
	interval := 10.0
	if dur > 0 {
		interval = dur / float64(spriteFrames)
		if interval < 0.1 {
			interval = 0.1
		}
	}

	// Probe the first-frame PTS offset once so the sprite/preview fast paths
	// can seek correctly in inputs that carry absolute timestamps (LL-HLS fMP4).
	// Without it, -ss would seek into the file's real timeline incorrectly.
	var ptsOffset float64
	if dur > 0 {
		ptsOffset = probeFirstPTSOffset(videoPath)
	}

	thumbDone := make(chan string, 1)
	spriteDone := make(chan string, 1)
	previewDone := make(chan string, 1)

	// ── Single thumbnail (static frame near the 10% mark) ──────────────────
	// Independent 90-second context: seeking to a single frame is always fast.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [thumb] generating thumbnail for %s: %v", baseName, r)
				select {
				case thumbDone <- "":
				default:
				}
			}
		}()
		thumbCtx, thumbCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer thumbCancel()

		thumbJPG := videoPath + ".thumb.jpg"
		defer os.Remove(thumbJPG)

		seekPos := "00:00:03"
		if dur > 0 && dur < 3 {
			seekPos = fmt.Sprintf("%.2f", dur*0.5)
		} else if dur > 0 {
			seekPos = fmt.Sprintf("%.2f", dur*0.1)
		}

		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()
		stderr, err := runFFmpeg(thumbCtx,
			"-y",
			"-ss", seekPos,
			"-i", videoPath,
			"-vframes", "1",
			"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
				thumbWidth, thumbHeight, thumbWidth, thumbHeight),
			"-c:v", "mjpeg",
			"-q:v", "5",
			"-update", "1",
			thumbJPG,
		)

		// Fallback: if the initial seek missed (very short clips, or an
		// unknown duration that fell back to a fixed 00:00:03 seek beyond the
		// end of the file), grab the first available frame instead.
		if err != nil || !fileExists(thumbJPG) {
			if err != nil {
				errFn("thumb: seek failed for %s: %v (ffmpeg: %s) — trying first-frame fallback", baseName, err, stderr)
			} else {
				errFn("thumb: seek produced no output for %s — trying first-frame fallback", baseName)
			}
			fbCtx, fbCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer fbCancel()
			_, fbErr := runFFmpeg(fbCtx,
				"-y",
				"-ss", "0",
				"-i", videoPath,
				"-vframes", "1",
				"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					thumbWidth, thumbHeight, thumbWidth, thumbHeight),
				"-c:v", "mjpeg",
				"-q:v", "5",
				"-update", "1",
				thumbJPG,
			)
			if fbErr != nil {
				errFn("thumb: fallback also failed for %s: %v", baseName, fbErr)
				thumbDone <- ""
				return
			}
		}

		imgUploader := uploader.NewMultiImageUploader()
		if remoteURL, _, uploadErr := imgUploader.Upload(thumbJPG); uploadErr == nil {
			info("thumb: ✓ %s", baseName)
			thumbDone <- remoteURL
		} else {
			errFn("thumb: upload failed for %s: %v", baseName, uploadErr)
			thumbDone <- ""
		}
	}()

	// ── Sprite sheet (4×4 grid covering the full video duration) ───────────
	// Each frame is spriteFrameW×spriteFrameH px; total image is
	// (spriteCols*spriteFrameW) × (spriteRows*spriteFrameH) = 2560×1440.
	// Using 640×360 frames so HiDPI/Retina displays get sharp previews.
	//
	// Uses input seeking (-ss before -i): ffmpeg jumps to each of the 16
	// evenly-spaced positions and reads a single frame, so generation is
	// O(frames) not O(video_duration).  The 15-minute context is a generous
	// ceiling for very slow/resource-constrained hosts, not a typical cost.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [sprite] generating sprite for %s: %v", baseName, r)
				select {
				case spriteDone <- "":
				default:
				}
			}
		}()
		spriteCtx, spriteCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer spriteCancel()

		spriteJPG := videoPath + ".sprite.jpg"
		defer os.Remove(spriteJPG)

		// Fast path: seek to spriteFrames evenly-spaced points via input
		// seeking (-ss before -i) instead of decoding the whole file.  For a
		// 1-hour recording this turns a full-file decode into spriteFrames
		// instant seeks, dropping generation from minutes to seconds.
		config.AcquireFFmpeg()
		defer config.ReleaseFFmpeg()

		var stderr string
		var err error
		if dur > 0 {
			args := []string{"-y"}
			for i := 0; i < spriteFrames; i++ {
				t := (float64(i) + 0.5) / float64(spriteFrames) * dur
				if t > dur-0.04 {
					t = dur - 0.04
				}
				if t < 0 {
					t = 0
				}
				args = append(args, "-ss", fmt.Sprintf("%.3f", ptsOffset+t), "-i", videoPath)
			}
			var fp []string
			for i := 0; i < spriteFrames; i++ {
				fp = append(fp, fmt.Sprintf("[%d:v]scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2[s%d]",
					i, spriteFrameW, spriteFrameH, spriteFrameW, spriteFrameH, i))
			}
			var tileIns []string
			for i := 0; i < spriteFrames; i++ {
				tileIns = append(tileIns, fmt.Sprintf("[s%d]", i))
			}
			// concat merges the 16 single-frame streams into one 16-frame
			// stream; tile then arranges those frames into the 4×4 grid.
			// (tile takes a single input, so concat is required first.)
			fp = append(fp, fmt.Sprintf("%s%s", strings.Join(tileIns, ""),
				fmt.Sprintf("concat=n=%d:v=1:a=0,tile=%dx%d", spriteFrames, spriteCols, spriteRows)))
			args = append(args, "-filter_complex", strings.Join(fp, ";"), "-frames:v", "1", "-update", "1", "-q:v", "5", spriteJPG)
			stderr, err = runFFmpeg(spriteCtx, args...)
		} else {
			// Duration unknown: fall back to sequential fps extraction.
			vf := fmt.Sprintf(
				"fps=1/%.4f,scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,tile=%dx%d",
				interval,
				spriteFrameW, spriteFrameH,
				spriteFrameW, spriteFrameH,
				spriteCols, spriteRows,
			)
			stderr, err = runFFmpeg(spriteCtx,
				"-y",
				"-i", videoPath,
				"-vf", vf,
				"-frames:v", "1",
				"-c:v", "mjpeg",
				"-q:v", "5",
				"-update", "1",
				spriteJPG,
			)
		}

		// Fallback: if the contact sheet fails, fall back to a single
		// representative frame so sprite generation still yields a usable image.
		if err != nil || !fileExists(spriteJPG) {
			if err != nil {
				errFn("sprite: sheet failed for %s: %v (ffmpeg: %s) — trying single-frame fallback", baseName, err, stderr)
			} else {
				errFn("sprite: sheet produced no output for %s — trying single-frame fallback", baseName)
			}
			fbCtx, fbCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer fbCancel()
			_, fbErr := runFFmpeg(fbCtx,
				"-y",
				"-ss", fmt.Sprintf("%.2f", ptsOffset+dur*0.1),
				"-i", videoPath,
				"-vframes", "1",
				"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
					spriteCols*spriteFrameW, spriteRows*spriteFrameH, spriteCols*spriteFrameW, spriteRows*spriteFrameH),
				"-c:v", "mjpeg",
				"-q:v", "5",
				"-update", "1",
				spriteJPG,
			)
			if fbErr != nil {
				errFn("sprite: fallback also failed for %s: %v", baseName, fbErr)
				spriteDone <- ""
				return
			}
		}

		imgUploader := uploader.NewMultiImageUploader()
		if remoteURL, _, uploadErr := imgUploader.Upload(spriteJPG); uploadErr == nil {
			info("sprite: ✓ %s", baseName)
			spriteDone <- remoteURL
		} else {
			errFn("sprite: upload failed for %s: %v", baseName, uploadErr)
			spriteDone <- ""
		}
	}()

	// ── MP4 hover preview (smooth clips from across the video, 18s total) ──
	// H.264 MP4 is used instead of GIF because:
	//   - ~90% smaller file size for the same visual quality
	//   - Full 24-bit color (vs 256-color palette in GIF)
	//   - Smooth native-framerate playback (GIF was variable ~1-8fps)
	//   - Catbox accepts MP4 files (200MB limit, permanent storage)
	//
	// Instead of isolated frame sampling (which produces a jerky slideshow),
	// we extract 12 short continuous clips (~1.5s each) spanning the FULL
	// duration — anchored at the very start (i=0) and very end (i=n-1) so the
	// beginning and end of the stream are always included, with the rest
	// evenly spaced 0%..100%.  Each clip has fully smooth motion because the
	// frames within it are consecutive.
	//
	//   <18 sec: no segmenting, plays whole video at normal speed
	//   1 min:   12 clips × 1.5s = 18s (4.5s between clips)
	//   60 min:  12 clips × 1.5s = 18s (5 min between clips)
	//
	// Uploaded to Catbox.moe (free, permanent, CDN-backed) with LobFile as fallback.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC [preview] generating preview for %s: %v", baseName, r)
				select {
				case previewDone <- "":
				default:
				}
			}
		}()
		previewCtx, previewCancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer previewCancel()

		previewMP4 := videoPath + ".preview.mp4"
		var previewGenerated bool
		defer func() {
			if previewGenerated {
				os.Remove(previewMP4)
			}
		}()

		waitForPreviewFile := func() bool {
			for delay := 0; delay < 8; delay++ {
				if fileExists(previewMP4) {
					return true
				}
				time.Sleep(time.Duration(50*(1<<delay)) * time.Millisecond)
			}
			return false
		}

		// generatePreview runs ffmpeg with input-seek + concat filter_complex
		// (or a simple single-segment fallback).  Returns true if the preview
		// file was successfully created.
		generatePreview := func(ctx context.Context) bool {
			var err error
			var stderr string
			if dur <= 0 {
				// Duration unknown (probe failed). Encoding the whole file as
				// the preview would produce a multi-GB clip for long videos, so
				// cap to the first previewDuration seconds from the start.
				stderr, err = runFFmpeg(ctx,
					"-y",
					"-ss", "0",
					"-i", videoPath,
					"-t", fmt.Sprintf("%.2f", previewDuration),
					"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
					"-c:v", "libx264",
					"-preset", "ultrafast",
					"-crf", "28",
					"-threads", "1",
					"-an",
					previewMP4,
				)
			} else if dur <= previewDuration {
				stderr, err = runFFmpeg(ctx,
					"-y",
					"-i", videoPath,
					"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
					"-c:v", "libx264",
					"-preset", "fast",
					"-crf", "23",
					"-movflags", "+faststart",
					"-an",
					previewMP4,
				)
			} else {
				// Fast path: seek to previewSegments clips via input seeking
				// (-ss/-t before -i) instead of decoding the whole file with
				// trim.  For a 1-hour recording this cuts decode work from
				// ~full video to previewSegments×segDuration seconds.
				segDuration := previewDuration / float64(previewSegments)

				args := []string{"-y"}
				for i := 0; i < previewSegments; i++ {
					var center float64
					if previewSegments == 1 {
						center = dur / 2
					} else {
						center = (float64(i) / float64(previewSegments-1)) * dur
					}
					start := center - segDuration/2
					if start < 0 {
						start = 0
					}
					if start+segDuration > dur {
						start = dur - segDuration
					}
					if start < 0 {
						start = 0
					}
					args = append(args, "-ss", fmt.Sprintf("%.3f", ptsOffset+start), "-t", fmt.Sprintf("%.3f", segDuration), "-i", videoPath)
				}

				// Concatenate the raw clips first (no scaling per segment),
				// then scale the whole 18s montage once — avoids 11 redundant
				// scale passes.  Encode with ultrafast + single thread + no
				// faststart: at 320×180 the frame is tiny, so libx264 thread
				// spin-up dominates; a single thread is actually faster here,
				// and faststart only helps playback start, not generation time.
				var fp []string
				for i := 0; i < previewSegments; i++ {
					fp = append(fp, fmt.Sprintf("[%d:v]setpts=PTS-STARTPTS[v%d]", i, i))
				}
				var concatIns []string
				for i := 0; i < previewSegments; i++ {
					concatIns = append(concatIns, fmt.Sprintf("[v%d]", i))
				}
				fp = append(fp, fmt.Sprintf("%s%s", strings.Join(concatIns, ""),
					fmt.Sprintf("concat=n=%d:v=1:a=0,scale=%d:%d:force_original_aspect_ratio=decrease:flags=lanczos,pad=%d:%d:(ow-iw)/2:(oh-ih)/2[out]",
						previewSegments, previewWidth, previewHeight, previewWidth, previewHeight)))

				args = append(args,
					"-filter_complex", strings.Join(fp, ";"),
					"-map", "[out]",
					"-c:v", "libx264",
					"-preset", "ultrafast",
					"-crf", "28",
					"-threads", "1",
					"-an",
					previewMP4,
				)
				stderr, err = runFFmpeg(ctx, args...)

				if err != nil || !fileExists(previewMP4) {
					if err != nil {
						errFn("preview: seek-concat failed for %s: %v (ffmpeg: %s), trying simple fallback", baseName, err, stderr)
					} else {
						errFn("preview: seek-concat produced no output for %s, trying simple fallback", baseName)
					}
					fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 5*time.Minute)
					defer fallbackCancel()
					stderr, err = runFFmpeg(fallbackCtx,
						"-y",
						"-ss", fmt.Sprintf("%.2f", ptsOffset+dur*0.3),
						"-i", videoPath,
						"-t", fmt.Sprintf("%.2f", previewDuration),
						"-vf", fmt.Sprintf("scale=%d:-2:flags=lanczos", previewWidth),
						"-c:v", "libx264",
						"-preset", "ultrafast",
						"-crf", "28",
						"-threads", "1",
						"-an",
						previewMP4,
					)
				}
			}

			if err != nil {
				errFn("preview: ffmpeg failed for %s: %v", baseName, err)
				return false
			}

			if !waitForPreviewFile() {
				errFn("preview: ffmpeg exited successfully but produced no output file for %s", baseName)
				return false
			}

			return true
		}

		config.AcquireFFmpeg()
		previewOK := generatePreview(previewCtx)
		config.ReleaseFFmpeg()
		if !previewOK {
			previewDone <- ""
			return
		}
		previewGenerated = true

		catboxUploader := uploader.NewCatboxUploader()
		lobfileUploader := uploader.NewLobFileUploader(os.Getenv("LOBFILE_API_KEY"))
		var remoteURL string
		var uploadErr error

		maxPreviewAttempts := 2
		for attempt := 0; attempt < maxPreviewAttempts; attempt++ {
			if attempt > 0 {
				info("preview: regenerating %s (attempt %d/%d)", baseName, attempt+1, maxPreviewAttempts)
				config.AcquireFFmpeg()
				regenCtx, regenCancel := context.WithTimeout(context.Background(), 5*time.Minute)
				ok := generatePreview(regenCtx)
				regenCancel()
				config.ReleaseFFmpeg()
				if !ok {
					uploadErr = fmt.Errorf("preview regeneration failed")
					break
				}
			}

			// Try hosts in order: Catbox → LobFile
			remoteURL, uploadErr = catboxUploader.Upload(previewMP4)
			if uploadErr == nil {
				break
			}
			errFn("preview: catbox failed for %s: %v, trying LobFile", baseName, uploadErr)

			remoteURL, uploadErr = lobfileUploader.Upload(previewMP4)
			if uploadErr == nil {
				break
			}
			errFn("preview: LobFile failed for %s: %v", baseName, uploadErr)

			errStr := uploadErr.Error()
			if strings.Contains(errStr, "no such file") ||
				strings.Contains(errStr, "cannot find") ||
				strings.Contains(errStr, "stat file") ||
				strings.Contains(errStr, "open file") {
				continue
			}

			break
		}

		if uploadErr == nil {
			info("preview: ✓ %s", baseName)
			previewDone <- remoteURL
		} else {
			errFn("preview: Catbox and LobFile both failed for %s: %v", baseName, uploadErr)
			previewDone <- ""
		}
	}()

	// Hard overall deadline so a stuck sub-step (a blocked ffmpeg semaphore
	// slot, a hung upload, a wedged ffmpeg) can never hang the caller forever.
	// The old code waited on the three done-channels with no timeout, which is
	// exactly the "shows a little preview then got stuck" hang. We wait up to
	// 15 minutes and return whatever completed; the goroutines self-terminate
	// via their own contexts.
	overallCtx, overallCancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer overallCancel()

	recv := func(ch chan string) string {
		select {
		case v := <-ch:
			return v
		case <-overallCtx.Done():
			return ""
		}
	}

	thumbURL = recv(thumbDone)
	spriteURL = recv(spriteDone)
	previewURL = recv(previewDone)

	if thumbURL == "" || spriteURL == "" || previewURL == "" {
		errFn("thumb: generation incomplete (thumb=%q sprite=%q preview=%q) — returning partial result", thumbURL, spriteURL, previewURL)
	}

	return thumbURL, spriteURL, previewURL
}

// probeFirstPTSOffset returns the PTS of the first video frame, or 0 if it
// cannot be determined.  LL-HLS fMP4 segments may carry absolute server
// timestamps (e.g. starting at 5044s), which causes trim=start=X to select
// wrong frames since trim uses PTS values.
func probeFirstPTSOffset(videoPath string) float64 {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer probeCancel()
	config.AcquireFFmpeg()
	defer config.ReleaseFFmpeg()
	out, err := config.FFprobeCommandContext(probeCtx,
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "frame=pkt_pts_time",
		"-of", "default=noprint_wrappers=1:nokey=1",
		"-read_intervals", "%+#1",
		videoPath,
	).Output()
	if err != nil {
		return 0
	}
	pts, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if parseErr != nil {
		return 0
	}
	if pts <= 0 {
		return 0
	}
	return pts
}
