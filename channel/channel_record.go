package channel

import (
        "context"
        "errors"
        "fmt"
        "strings"
        "time"

        "github.com/avast/retry-go/v4"
        "github.com/teacat/chaturbate-dvr/chaturbate"
        "github.com/teacat/chaturbate-dvr/internal"
        "github.com/teacat/chaturbate-dvr/server"
)

// Monitor starts monitoring the channel for live streams and records them.
func (ch *Channel) Monitor() {
        client := chaturbate.NewClient()
        ch.Info("starting to record `%s`", ch.Config.Username)

        // Create a new context with a cancel function,
        // the CancelFunc will be stored in the channel's CancelFunc field
        // and will be called by `Pause` or `Stop` functions
        ctx, _ := ch.WithCancel(context.Background())

        var err error
        cfBlockCount := 0
        for {
                if err = ctx.Err(); err != nil {
                        break
                }

                pipeline := func() error {
                        return ch.RecordStream(ctx, client)
                }

                onRetry := func(_ uint, err error) {
                        ch.UpdateOnlineStatus(false)

                        if isCFBlock(err) {
                                cfBlockCount++
                                delay := cfBackoffMinutes(cfBlockCount, server.Config.Interval)
                                ch.Info("blocked by Cloudflare (attempt %d); try with `-cookies` and `-user-agent`? try again in %d min(s)", cfBlockCount, delay)
                        } else if errors.Is(err, internal.ErrChannelOffline) || errors.Is(err, internal.ErrPrivateStream) {
                                cfBlockCount = 0
                                ch.stateMu.Lock()
                                ch.RoomStatus = client.LastRoomStatus
                                ch.stateMu.Unlock()
                                ch.Update()
                                if client.LastRoomStatus == chaturbate.StatusPublic && errors.Is(err, internal.ErrChannelOffline) {
                                        ch.Info("channel is live but stream URL unavailable (check Byparr/cookies); try again in %d min(s)", server.Config.Interval)
                                } else {
                                        ch.Info("channel is %s, try again in %d min(s)", ch.RoomStatus, server.Config.Interval)
                                }

                                // NOTE: no extra Cleanup call here.
                                // RecordStream's deferred Cleanup(false) always runs before
                                // onRetry is called (defers execute before the function returns
                                // to the retry loop). ch.File is therefore always nil at this
                                // point. Launching a goroutine here was redundant dead-code and
                                // caused a race: if the goroutine was delayed by the scheduler
                                // it could fire after the next RecordStream had already opened
                                // new files, closing and uploading them prematurely.
                        } else if errors.Is(err, internal.ErrStreamStalled) {
                                // CDN session expired mid-stream (common with LL-HLS tokens).
                                // The current file has already been finalised by the deferred
                                // Cleanup in RecordStream.  Just re-fetch a fresh HLS URL.
                                cfBlockCount = 0
                                ch.Info("stream stalled (CDN session expired) — re-fetching fresh URL in %d min(s)", server.Config.Interval)
                        } else if errors.Is(err, context.Canceled) {
                                cfBlockCount = 0
                        } else {
                                cfBlockCount = 0
                                ch.Error("on retry: %s: retrying in %d min(s)", err.Error(), server.Config.Interval)
                        }
                }

                customDelay := func(_ uint, err error, _ *retry.Config) time.Duration {
                        if isCFBlock(err) {
                                return time.Duration(cfBackoffMinutes(cfBlockCount, server.Config.Interval)) * time.Minute
                        }
                        return time.Duration(server.Config.Interval) * time.Minute
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

        // Always cleanup when monitor exits, regardless of error
        if err := ch.Cleanup(false); err != nil {
                ch.Error("cleanup on monitor exit: %s", err.Error())
        }

        // Log error if it's not a context cancellation
        if err != nil && !errors.Is(err, context.Canceled) {
                ch.Error("record stream: %s", err.Error())
        }
}

// Update sends an update signal to the channel's update channel.
// This notifies the Server-sent Event to boradcast the channel information to the client.
func (ch *Channel) Update() {
        ch.UpdateCh <- true
}

// RecordStream records the stream of the channel using the provided client.
// It retrieves the stream information and starts watching the segments.
func (ch *Channel) RecordStream(ctx context.Context, client *chaturbate.Client) error {
        stream, err := client.GetStream(ctx, ch.Config.Username)
        if err != nil {
                return fmt.Errorf("get stream: %w", err)
        }
        playlist, err := stream.GetPlaylist(ctx, ch.Config.Resolution, ch.Config.Framerate)
        if err != nil {
                return fmt.Errorf("get playlist: %w", err)
        }

        // Capture room metadata cached on the client from GetStream.
        ch.RoomTitle = client.LastRoomTitle
        ch.Tags = client.LastTags
        ch.Viewers = client.LastViewers
        ch.Gender = client.LastGender

        // Fallback: if the API returned no tags array, extract hashtags from
        // the room title (e.g. "title #tag1 #tag2").
        if len(ch.Tags) == 0 && ch.RoomTitle != "" {
                ch.Tags = extractHashtags(ch.RoomTitle)
        }

        // Capture actual stream quality from the playlist
        ch.Resolution = fmt.Sprintf("%dp", playlist.Resolution)
        ch.Framerate = playlist.Framerate

        ch.StreamedAt = time.Now().Unix()
        ch.Sequence = 0
        ch.InitSegment = nil
        ch.AudioInitSegment = nil
        ch.HasSeparateAudio = playlist.AudioPlaylistURL != ""
        ch.switchRequested = false

        if err := ch.NextFile(); err != nil {
                return fmt.Errorf("next file: %w", err)
        }

        // Ensure file is cleaned up when this function exits in any case
        defer func() {
                if err := ch.Cleanup(false); err != nil {
                        ch.Error("cleanup on record stream exit: %s", err.Error())
                }
        }()

        ch.stateMu.Lock()
        ch.RoomStatus = chaturbate.StatusPublic
        ch.stateMu.Unlock()
        ch.UpdateOnlineStatus(true) // after GetPlaylist succeeds

        ch.Info("stream quality - %dp @ %dfps (target: %dp @ %dfps)", playlist.Resolution, playlist.Framerate, ch.Config.Resolution, ch.Config.Framerate)
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

        return playlist.WatchAVSegments(ctx, ch.HandleSegment, ch.HandleInitSegment, ch.HandleAudioSegment, ch.HandleAudioInitSegment, ch.OnPollComplete)
}

// HandleInitSegment stores the fMP4 init segment and writes it to the file.
func (ch *Channel) HandleInitSegment(initData []byte) error {
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
        ch.AudioInitSegment = initData

        if ch.AudioFile == nil {
                return nil
        }

        if _, err := ch.AudioFile.Write(initData); err != nil {
                return fmt.Errorf("write audio init segment: %w", err)
        }
        return nil
}

func isCFBlock(err error) bool {
        return errors.Is(err, internal.ErrCloudflareBlocked) || errors.Is(err, internal.ErrAgeVerification)
}

// cfBackoffMinutes returns the delay in minutes for Cloudflare block retries.
// Uses exponential backoff: interval * 2^(n-1), capped at 30 minutes.
// consecutiveBlocks must be >= 1.
func cfBackoffMinutes(consecutiveBlocks, baseInterval int) int {
        shift := min(consecutiveBlocks-1, 4) // max multiplier: 16x
        delay := baseInterval * (1 << shift)
        return min(delay, 30)
}

// HandleSegment processes and writes segment data to a file.
func (ch *Channel) HandleSegment(b []byte, duration float64) error {
        if ch.Config.IsPaused.Load() {
                return retry.Unrecoverable(internal.ErrPaused)
        }

        n, err := ch.File.Write(b)
        if err != nil {
                return fmt.Errorf("write file: %w", err)
        }

        ch.stateMu.Lock()
        ch.Filesize += n
        ch.Duration += duration
        dur := ch.Duration
        fs := ch.Filesize
        ch.stateMu.Unlock()
        ch.Info("duration: %s, filesize: %s", internal.FormatDuration(dur), internal.FormatFilesize(fs))

        // Send an SSE update to update the view
        ch.Update()

        if !ch.ShouldSwitchFile() {
                return nil
        }

        // For LL-HLS streams with separate audio, defer the rotation until the
        // current poll cycle finishes so the paired audio segments land in the
        // same file as the video ones. Single-stream recordings have no pairing
        // risk, and deferring would let processMediaPlaylist keep appending a
        // backlog of catch-up segments past the MaxFilesize/MaxDuration limit.
        if ch.HasSeparateAudio {
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
// Called by WatchAVSegments after both video and audio playlists have been
// processed, guaranteeing that rotation never splits an A/V pair.
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
        return nil
}

// extractHashtags pulls #word tokens out of a Chaturbate room title and
// returns them as a clean tag list.  Used as a fallback when the API
// returns an empty tags array.
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
