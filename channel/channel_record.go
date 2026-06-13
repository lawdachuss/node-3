package channel

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/teacat/chaturbate-dvr/chaturbate"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
	"github.com/teacat/chaturbate-dvr/stripchat"
)

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor() {
	siteImpl := resolveSite(ch)
	req := internal.NewReq()
	ch.Info("starting to record `%s` (%s)", ch.Config.Username, ch.Config.Site)

	ctx, _ := ch.WithCancel(context.Background())

	ch.stateMu.Lock()
	ch.RoomStatus = "offline"
	ch.stateMu.Unlock()

	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}

		pipeline := func() error {
			return ch.RecordStream(ctx, siteImpl, req)
		}

		onRetry := func(_ uint, err error) {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			switch {
			case errors.Is(err, internal.ErrChannelOffline) || errors.Is(err, internal.ErrPrivateStream):
				ch.stateMu.Lock()
				ch.IsOnline = false
				ch.IsConnecting = false
				roomStatus := ch.RoomStatus
				ch.stateMu.Unlock()
				ch.Update()
				ch.Info("channel is %s, try again in %d min(s)", roomStatus, server.Config.Interval)

			case errors.Is(err, internal.ErrStreamStalled):
				ch.SetConnecting(true)
				ch.Info("stream stalled (CDN session expired) — reconnecting in 15s")

			default:
				ch.SetConnecting(true)
				ch.Error("on retry: %s: retrying", err.Error())
			}
		}

		customDelay := func(n uint, err error, _ *retry.Config) time.Duration {
			switch {
			case errors.Is(err, internal.ErrStreamStalled):
				return 15*time.Second + time.Duration(rand.Int63n(16))*time.Second
			case errors.Is(err, internal.ErrChannelOffline) || errors.Is(err, internal.ErrPrivateStream):
				base := time.Duration(server.Config.Interval) * time.Minute
				return base + time.Duration(rand.Int63n(31))*time.Second
			default:
				backoff := time.Duration(min(1<<n, 32)) * 10 * time.Second
				return backoff + time.Duration(rand.Int63n(11))*time.Second
			}
		}

		if err = retry.Do(
			pipeline,
			retry.Context(ctx),
			retry.Attempts(0),
			retry.DelayType(customDelay),
			retry.OnRetry(onRetry),
		); err != nil {
			break
		}
	}

	if err := ch.Cleanup(CloseQueue); err != nil {
		ch.Error("cleanup on monitor exit: %s", err.Error())
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		ch.Error("record stream: %s", err.Error())
	}
}

func (ch *Channel) Update() {
	select {
	case ch.UpdateCh <- true:
	default:
	}
}

const defaultGracePeriod = 3 * time.Minute

// RecordStream fetches stream info via the site interface, then dispatches
// to the site-specific recording implementation.
func (ch *Channel) RecordStream(ctx context.Context, siteImpl site.Site, req *internal.Req) error {
	info, err := siteImpl.FetchStream(ctx, req, ch.Config.Username)
	if err != nil {
		if info != nil {
			ch.stateMu.Lock()
			ch.RoomStatus = info.RoomStatus
			ch.stateMu.Unlock()
		}
		return fmt.Errorf("get stream: %w", err)
	}

	ch.stateMu.Lock()
	ch.RoomTitle = info.RoomTitle
	ch.Tags = info.Tags
	ch.Viewers = info.NumUsers
	ch.Gender = info.Gender
	ch.LiveThumbURL = info.LiveThumbURL
	ch.stateMu.Unlock()

	if len(ch.Tags) == 0 && ch.RoomTitle != "" {
		ch.Tags = extractHashtags(ch.RoomTitle)
	}

	if ch.Config.Site == "stripchat" {
		return ch.recordStreamSC(ctx, req, info)
	}
	return ch.recordStreamCB(ctx, req, info)
}

// recordStreamCB handles recording for Chaturbate.
func (ch *Channel) recordStreamCB(ctx context.Context, req *internal.Req, info *site.StreamInfo) error {
	playlist, err := chaturbate.FetchPlaylist(ctx, info.HLSSource, ch.Config.Resolution, ch.Config.Framerate)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}

	ch.Resolution = fmt.Sprintf("%dp", playlist.Resolution)
	ch.Framerate = playlist.Framerate
	ch.initRecordingState(playlist.AudioPlaylistURL != "")

	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	defer ch.cleanupOnExit(ctx)

	ch.stateMu.Lock()
	ch.RoomStatus = site.StatusPublic
	ch.stateMu.Unlock()
	ch.UpdateOnlineStatus(true)

	ch.logStreamQuality(playlist.Resolution, playlist.Framerate)

	return ch.watchWithGraceCB(ctx, req, playlist)
}

// recordStreamSC handles recording for Stripchat with MOUFLON v2 support.
func (ch *Channel) recordStreamSC(ctx context.Context, req *internal.Req, info *site.StreamInfo) error {
	playlist, err := stripchat.FetchPlaylist(ctx, req, info.HLSSource, server.Config.StripchatPDKey, ch.Config.Resolution, ch.Config.Framerate)
	if err != nil {
		return fmt.Errorf("get playlist: %w", err)
	}

	ch.Info("stripchat playlist: url=%s pdkey=%q res=%d", playlist.PlaylistURL, playlist.PDKey, playlist.Resolution)
	ch.Resolution = fmt.Sprintf("%dp", playlist.Resolution)
	ch.Framerate = playlist.Framerate
	ch.initRecordingState(playlist.AudioPlaylistURL != "")

	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}

	defer ch.cleanupOnExit(ctx)

	ch.stateMu.Lock()
	ch.RoomStatus = site.StatusPublic
	ch.stateMu.Unlock()
	ch.UpdateOnlineStatus(true)

	ch.logStreamQuality(playlist.Resolution, playlist.Framerate)

	return ch.watchWithGraceSC(ctx, req, playlist)
}

func (ch *Channel) initRecordingState(hasSeparateAudio bool) {
	ch.stateMu.Lock()
	ch.StreamedAt = time.Now().Unix()
	ch.Sequence = 0
	ch.HasSeparateAudio = hasSeparateAudio
	ch.videoSegmentCount = 0
	ch.audioSegmentCount = 0
	ch.stateMu.Unlock()
	ch.InitSegment = nil
	ch.AudioInitSegment = nil
	ch.switchRequested = false
}

func (ch *Channel) cleanupOnExit(ctx context.Context) {
	mode := CloseProcess
	if ctx.Err() != nil {
		mode = CloseQueue
	}
	if err := ch.Cleanup(mode); err != nil {
		ch.Error("cleanup on record stream exit: %s", err.Error())
	}
}

func (ch *Channel) logStreamQuality(resolution, framerate int) {
	ch.Info("stream quality - %dp @ %dfps (target: %dp @ %dfps)", resolution, framerate, ch.Config.Resolution, ch.Config.Framerate)
	if ch.HasSeparateAudio {
		ch.Info("mux: separate audio track detected — will mux audio/video after recording")
	}
	if ch.Viewers > 0 {
		ch.Info("status: %d viewers", ch.Viewers)
	}
	if ch.RoomTitle != "" {
		title := ch.RoomTitle
		if len(title) > 80 {
			title = title[:80] + "…"
		}
		ch.Info("status: room title: %s", title)
	}
}

// ─── Chaturbate grace/watch ──────────────────────────────────────────────

func (ch *Channel) watchWithGraceCB(ctx context.Context, client *internal.Req, p *chaturbate.Playlist) error {
	const pollInterval = 30 * time.Second

	siteImpl := site.NewChaturbateSite()
	origErr := ch.watchLoopCB(ctx, client, p)
	if origErr == nil {
		return nil
	}
	if errors.Is(origErr, context.Canceled) || errors.Is(origErr, context.DeadlineExceeded) {
		return origErr
	}

	ch.Info("recording: segment fetch interrupted (%s); %s grace period starts", origErr, defaultGracePeriod)

	deadline := time.Now().Add(defaultGracePeriod)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}

		info, apiErr := siteImpl.FetchStream(ctx, client, ch.Config.Username)
		if apiErr != nil {
			ch.Info("recording: API still unavailable (%s); %s remaining", apiErr, time.Until(deadline).Round(time.Second))
			continue
		}

		newPlaylist, apiErr := chaturbate.FetchPlaylist(ctx, info.HLSSource, ch.Config.Resolution, ch.Config.Framerate)
		if apiErr != nil {
			continue
		}

		ch.Info("recording: channel back online — resuming with fresh playlist")
		ch.stateMu.Lock()
		ch.RoomStatus = site.StatusPublic
		ch.stateMu.Unlock()
		ch.UpdateOnlineStatus(true)

		ch.Resolution = fmt.Sprintf("%dp", newPlaylist.Resolution)
		ch.Framerate = newPlaylist.Framerate

		loopErr := ch.watchLoopCB(ctx, client, newPlaylist)
		if loopErr == nil {
			return nil
		}
		if errors.Is(loopErr, context.Canceled) || errors.Is(loopErr, context.DeadlineExceeded) {
			return loopErr
		}
		ch.Info("recording: segment fetch interrupted again during grace period (%s)", loopErr)
	}

	ch.Info("recording: grace period expired — finalizing file")
	return fmt.Errorf("channel offline after grace period: %w", origErr)
}

func (ch *Channel) watchLoopCB(ctx context.Context, client *internal.Req, p *chaturbate.Playlist) error {
	const maxStalls = 10
	siteImpl := site.NewChaturbateSite()
	for stall := 0; stall < maxStalls; stall++ {
		err := p.WatchAVSegments(ctx, ch.HandleSegment, ch.HandleInitSegment, ch.HandleAudioSegment, ch.HandleAudioInitSegment, ch.OnPollComplete)
		if err == nil || !errors.Is(err, internal.ErrStreamStalled) {
			return err
		}
		ch.Info("recording: CDN session expired — fetching fresh playlist URL")
		info, apiErr := siteImpl.FetchStream(ctx, client, ch.Config.Username)
		if apiErr != nil {
			return apiErr
		}
		newPlaylist, apiErr := chaturbate.FetchPlaylist(ctx, info.HLSSource, ch.Config.Resolution, ch.Config.Framerate)
		if apiErr != nil {
			return apiErr
		}
		p = newPlaylist
		ch.Resolution = fmt.Sprintf("%dp", newPlaylist.Resolution)
		ch.Framerate = newPlaylist.Framerate
		ch.stateMu.Lock()
		ch.RoomStatus = site.StatusPublic
		ch.stateMu.Unlock()
		ch.UpdateOnlineStatus(true)
	}
	return fmt.Errorf("too many consecutive CDN stalls (%d): %w", maxStalls, internal.ErrStreamStalled)
}

// ─── Stripchat grace/watch ──────────────────────────────────────────────

func (ch *Channel) watchWithGraceSC(ctx context.Context, client *internal.Req, p *stripchat.Playlist) error {
	const pollInterval = 30 * time.Second

	var siteImpl site.Site = site.NewChaturbateSite()
	if ch.Config.Site == "stripchat" {
		siteImpl = stripchat.NewStripchatSite()
	}

	// Periodically refresh LiveThumbURL during Stripchat recording.
	// Stripchat's API returns a signed preview URL that can expire; refreshing
	// it ensures the thumbnail in the UI stays current rather than going stale.
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	defer refreshCancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				info, err := siteImpl.FetchStream(refreshCtx, client, ch.Config.Username)
				if err == nil && info.LiveThumbURL != "" {
					ch.stateMu.Lock()
					ch.LiveThumbURL = info.LiveThumbURL
					ch.stateMu.Unlock()
				}
			}
		}
	}()

	origErr := ch.watchLoopSC(ctx, client, p)
	if origErr == nil {
		return nil
	}
	if errors.Is(origErr, context.Canceled) || errors.Is(origErr, context.DeadlineExceeded) {
		return origErr
	}

	ch.Info("recording: segment fetch interrupted (%s); %s grace period starts", origErr, defaultGracePeriod)

	deadline := time.Now().Add(defaultGracePeriod)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}

		info, apiErr := siteImpl.FetchStream(ctx, client, ch.Config.Username)
		if apiErr != nil {
			ch.Info("recording: API still unavailable (%s); %s remaining", apiErr, time.Until(deadline).Round(time.Second))
			continue
		}

		if info.LiveThumbURL != "" {
			ch.stateMu.Lock()
			ch.LiveThumbURL = info.LiveThumbURL
			ch.stateMu.Unlock()
		}

		newPlaylist, apiErr := stripchat.FetchPlaylist(ctx, client, info.HLSSource, "", ch.Config.Resolution, ch.Config.Framerate)
		if apiErr != nil {
			continue
		}

		ch.Info("recording: channel back online — resuming with fresh playlist")
		ch.stateMu.Lock()
		ch.RoomStatus = site.StatusPublic
		ch.stateMu.Unlock()
		ch.UpdateOnlineStatus(true)

		ch.Resolution = fmt.Sprintf("%dp", newPlaylist.Resolution)
		ch.Framerate = newPlaylist.Framerate

		loopErr := ch.watchLoopSC(ctx, client, newPlaylist)
		if loopErr == nil {
			return nil
		}
		if errors.Is(loopErr, context.Canceled) || errors.Is(loopErr, context.DeadlineExceeded) {
			return loopErr
		}
		ch.Info("recording: segment fetch interrupted again during grace period (%s)", loopErr)
	}

	ch.Info("recording: grace period expired — finalizing file")
	return fmt.Errorf("channel offline after grace period: %w", origErr)
}

func (ch *Channel) watchLoopSC(ctx context.Context, client *internal.Req, p *stripchat.Playlist) error {
	const maxStalls = 10
	for stall := 0; stall < maxStalls; stall++ {
		err := p.WatchAVSegments(ctx, ch.HandleSegment, ch.HandleInitSegment, ch.HandleAudioSegment, ch.HandleAudioInitSegment, ch.OnPollComplete)
		if err == nil || !errors.Is(err, internal.ErrStreamStalled) {
			return err
		}
		ch.Info("recording: CDN session expired — refreshing master playlist (refresh #%d)", stall+1)

		// The variant playlist URL has a short-lived ?pkey= token (~20s).
		// The master playlist URL has no token — re-fetch it to get a fresh
		// pkey embedded in the response, then build a new variant URL.
		// This is much cheaper than calling the Stripchat API.
		//
		// Pass pdkey="" so FetchPlaylist extracts the fresh pkey from the
		// new master body and resolves a new pdkey for MOUFLON decryption.
		newPlaylist, apiErr := stripchat.FetchPlaylist(ctx, client, p.MasterURL, "", ch.Config.Resolution, ch.Config.Framerate)
		if apiErr != nil {
			// Master fetch failed — the stream may have ended.
			// Fall back to the Stripchat API to check online status.
			ch.Info("recording: master playlist unavailable (%s) — checking API", apiErr)
			siteImpl := stripchat.NewStripchatSite()
			info, fetchErr := ch.fetchStreamWithRetry(ctx, client, siteImpl)
			if fetchErr != nil {
				ch.Info("recording: channel appears offline after CDN expiry (%s)", fetchErr)
				return fetchErr
			}
			newPlaylist, apiErr = stripchat.FetchPlaylist(ctx, client, info.HLSSource, "", ch.Config.Resolution, ch.Config.Framerate)
			if apiErr != nil {
				return apiErr
			}
		}
		p = newPlaylist
		ch.Resolution = fmt.Sprintf("%dp", newPlaylist.Resolution)
		ch.Framerate = newPlaylist.Framerate
		ch.stateMu.Lock()
		ch.RoomStatus = site.StatusPublic
		ch.stateMu.Unlock()
		ch.UpdateOnlineStatus(true)
	}
	return fmt.Errorf("too many consecutive CDN stalls (%d): %w", maxStalls, internal.ErrStreamStalled)
}

// fetchStreamWithRetry calls FetchStream and retries transient API failures
// with a short delay so a brief blip during CDN token rollover doesn't trigger
// the full 3-minute grace period.
func (ch *Channel) fetchStreamWithRetry(ctx context.Context, client *internal.Req, siteImpl site.Site) (*site.StreamInfo, error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		info, err := siteImpl.FetchStream(ctx, client, ch.Config.Username)
		if err == nil {
			return info, nil
		}
		lastErr = err
		if attempt < maxAttempts-1 {
			ch.Info("recording: API unavailable during CDN refresh (%s) — retrying in 15s (attempt %d/%d)", err, attempt+1, maxAttempts)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(15 * time.Second):
			}
		}
	}
	return nil, lastErr
}

// HandleInitSegment stores the fMP4 init segment and writes it to the file.
func (ch *Channel) HandleInitSegment(initData []byte) error {
	if ch.InitSegment != nil {
		return nil
	}
	ch.InitSegment = initData

	if ch.File == nil {
		return nil
	}

	n, err := ch.File.Write(initData)
	if err != nil {
		return fmt.Errorf("write init segment: %w", err)
	}
	ch.stateMu.Lock()
	ch.Filesize += n
	ch.stateMu.Unlock()
	return nil
}

// HandleAudioInitSegment stores the fMP4 audio init segment and writes it to the file.
func (ch *Channel) HandleAudioInitSegment(initData []byte) error {
	if ch.AudioInitSegment != nil {
		return nil
	}
	ch.AudioInitSegment = initData

	if ch.AudioFile == nil {
		return nil
	}

	n, err := ch.AudioFile.Write(initData)
	if err != nil {
		return fmt.Errorf("write audio init segment: %w", err)
	}
	ch.stateMu.Lock()
	ch.Filesize += n
	ch.stateMu.Unlock()
	return nil
}

// HandleSegment processes and writes segment data to a file.
func (ch *Channel) HandleSegment(b []byte, duration float64) error {
	if ch.Config.IsPaused.Load() {
		return retry.Unrecoverable(internal.ErrPaused)
	}

	if ch.File == nil {
		return fmt.Errorf("HandleSegment: ch.File is nil")
	}

	n, err := ch.File.Write(b)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	ch.stateMu.Lock()
	ch.Filesize += n
	ch.Duration += duration
	ch.videoSegmentCount++
	dur := ch.Duration
	fs := ch.Filesize
	vSegCount := ch.videoSegmentCount
	aSegCount := ch.audioSegmentCount
	hasSeparateAudio := ch.HasSeparateAudio
	ch.stateMu.Unlock()

	if hasSeparateAudio {
		segDiff := vSegCount - aSegCount
		if segDiff != 0 {
			ch.Info("duration: %s, filesize: %s [v:%d a:%d Δ%+d]", internal.FormatDuration(dur), internal.FormatFilesize(fs), vSegCount, aSegCount, segDiff)
		} else {
			ch.Info("duration: %s, filesize: %s [v:%d a:%d synced]", internal.FormatDuration(dur), internal.FormatFilesize(fs), vSegCount, aSegCount)
		}
	} else {
		ch.Info("duration: %s, filesize: %s", internal.FormatDuration(dur), internal.FormatFilesize(fs))
	}

	ch.Update()

	if !ch.ShouldSwitchFile() {
		return nil
	}

	if hasSeparateAudio {
		ch.switchRequested = true
		return nil
	}

	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}
	ch.Info("max filesize or duration exceeded, new file created: %s", ch.File.Name())
	return nil
}

// OnPollComplete performs any file rotation requested during the poll cycle.
func (ch *Channel) OnPollComplete() error {
	if !ch.switchRequested {
		return nil
	}
	ch.switchRequested = false
	if err := ch.NextFile(); err != nil {
		return fmt.Errorf("next file: %w", err)
	}
	ch.Info("max filesize or duration exceeded, new file created: %s", ch.File.Name())
	return nil
}

// HandleAudioSegment processes and writes audio segment data to a sidecar file.
func (ch *Channel) HandleAudioSegment(b []byte, duration float64) error {
	if ch.AudioFile == nil {
		return nil
	}
	if ch.Config.IsPaused.Load() {
		return retry.Unrecoverable(internal.ErrPaused)
	}

	if _, err := ch.AudioFile.Write(b); err != nil {
		return fmt.Errorf("write audio file: %w", err)
	}

	ch.stateMu.Lock()
	ch.audioSegmentCount++
	ch.stateMu.Unlock()

	return nil
}

// extractHashtags pulls #word tokens out of a room title and returns them as
// a clean tag list. Used as a fallback when the API returns an empty tags array.
func extractHashtags(title string) []string {
	var tags []string
	for _, word := range strings.Fields(title) {
		if !strings.HasPrefix(word, "#") {
			continue
		}
		tag := strings.TrimPrefix(word, "#")
		tag = strings.Trim(tag, ".,!?;:")
		if tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags
}
