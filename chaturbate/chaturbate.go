package chaturbate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/grafov/m3u8"
	"github.com/samber/lo"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/stripchat"
)

// Room status constants from the Chaturbate API.
const (
	StatusPublic  = "public"
	StatusPrivate = "private"
	StatusAway    = "away"
	StatusOffline = "offline"
)

// edgeRegionRegexp extracts edge region from URL like "edge14-sin.live.mmcdn.com"
var edgeRegionRegexp = regexp.MustCompile(`edge\d+-([a-z]+)`)

// edgeRegions is the list of CDN edge regions to try when geo-blocked
var edgeRegions = []string{"lax", "fra", "ams", "sin", "hnd"}

// APIResponse represents the response from /api/chatvideocontext/ and get_edge_hls_url_ajax/ endpoints.
// The POST endpoint returns the stream URL in the "url" field; the GET endpoint uses "hls_source".
type APIResponse struct {
	HLSSource         string   `json:"hls_source"`
	URL               string   `json:"url"`
	RoomStatus        string   `json:"room_status"`
	RoomTitle         string   `json:"room_title"`
	Tags              []string `json:"tags"`
	NumUsers          int      `json:"num_users"`
	BroadcasterGender string   `json:"broadcaster_gender"`
	SummaryCardImage  string   `json:"summary_card_image"`
	EdgeRegion        string   `json:"edge_region"`
	Code              string   `json:"code"`
}

// StreamURL returns the HLS source URL, preferring hls_source and falling back to url.
func (r *APIResponse) StreamURL() string {
	if r.HLSSource != "" {
		return r.HLSSource
	}
	return r.URL
}

// Client represents an API client for interacting with Chaturbate.
type Client struct {
	Req            *internal.Req
	LastRoomStatus string   // cached from the most recent API call
	LastRoomTitle  string   // cached room metadata for recording entry
	LastTags       []string // cached room metadata for recording entry
	LastViewers    int      // cached room metadata for recording entry
	LastGender     string   // cached broadcaster_gender ("m", "f", "c", "t", …)
	SkipEdgeCheck  bool     // if true, skip HEAD validation in FetchStream (used after stream stall for fast reconnect)
}

// NewClient initializes and returns a new Client instance.
func NewClient() *Client {
	return &Client{
		Req: internal.NewReq(),
	}
}

// GetStream fetches the stream information for a given username.
// Room metadata (title, tags, viewers, gender) is cached on the Client for use
// when building the recording entry.
func (c *Client) GetStream(ctx context.Context, username string) (*Stream, error) {
	var roomInfo APIResponse
	// Use the internal helper so SkipEdgeCheck is passed through.
	stream, roomStatus, err := fetchStream(ctx, c.Req, username, &roomInfo, c.SkipEdgeCheck)
	c.SkipEdgeCheck = false // one-shot; reset after use
	c.LastRoomStatus = roomStatus
	c.LastRoomTitle = roomInfo.RoomTitle
	c.LastTags = roomInfo.Tags
	c.LastViewers = roomInfo.NumUsers
	c.LastGender = roomInfo.BroadcasterGender
	return stream, err
}

// GetRoomStatus returns the room status string (public, private, away, offline, etc.)
func (c *Client) GetRoomStatus(ctx context.Context, username string) (string, error) {
	resp, err := fetchAPIResponse(ctx, c.Req, username)
	if err != nil {
		return "", err
	}
	return resp.RoomStatus, nil
}

func fetchAPIResponse(ctx context.Context, client *internal.Req, username string) (*APIResponse, error) {
	apiURL := fmt.Sprintf("%sapi/chatvideocontext/%s/", server.Config.Domain, username)

	// NOTE: No circuit-breaker check here on purpose. The GET (chatvideocontext)
	// endpoint only needs cookies (and is frequently answered cookie-less with a
	// clean JSON body) and is the critical fallback when the POST API fails or
	// the breaker opens. The breaker only gates the POST API — it must NOT block
	// this fallback, or the node can never recover and record live channels
	// during transient auth/CF issues (e.g. a fresh GitHub runner IP that has no
	// minted cf_clearance yet).

	var body string
	err := retry.Do(func() error {
		if err := internal.WaitForChaturbateRateLimit(ctx); err != nil {
			return err
		}

		var e error
		body, e = client.Get(ctx, apiURL)
		if e != nil {
			internal.ReportChaturbateFailureUnlessExpected(e)
			return e
		}
		if body == "" {
			internal.ReportChaturbateFailure()
			return fmt.Errorf("empty response body")
		}
		internal.ReportChaturbateSuccess()
		return nil
	},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.MaxDelay(10*time.Second),
		retry.DelayType(retry.BackOffDelay),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get API response: %w", err)
	}

	var resp APIResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	return &resp, nil
}

// FetchStream retrieves the streaming data using the Chaturbate API.
// Returns the stream, the room status string, and any error.
// If roomInfo is non-nil, it is populated with room metadata (title, tags,
// viewers) from whichever API call succeeded.
func FetchStream(ctx context.Context, client *internal.Req, username string, roomInfo *APIResponse) (*Stream, string, error) {
	return fetchStream(ctx, client, username, roomInfo, false)
}

// fetchStream is the internal implementation of FetchStream.  When skipEdgeCheck
// is true, HEAD validation of the HLS edge URL is skipped — used after a stream
// stall to reconnect as fast as possible.
func fetchStream(ctx context.Context, client *internal.Req, username string, roomInfo *APIResponse, skipEdgeCheck bool) (*Stream, string, error) {
	// Try POST API first
	body, err := internal.PostChaturbateAPI(ctx, username)
	if err != nil {
		// Try the GET API as fallback
		resp, apiErr := fetchAPIResponse(ctx, client, username)
		if apiErr != nil {
			return nil, "", apiErr
		}

		if roomInfo != nil {
			roomInfo.RoomTitle = resp.RoomTitle
			roomInfo.Tags = resp.Tags
			roomInfo.NumUsers = resp.NumUsers
			roomInfo.SummaryCardImage = resp.SummaryCardImage
			roomInfo.EdgeRegion = resp.EdgeRegion
			if resp.BroadcasterGender != "" {
				roomInfo.BroadcasterGender = resp.BroadcasterGender
			}
		}

		if resp.Code == "unauthorized" {
			return nil, resp.RoomStatus, internal.ErrRoomPasswordRequired
		}

		// NOTE: no "hidden" case here on purpose — hidden (limitcam) rooms ARE
		// streaming on Chaturbate and expose an hls_source just like public rooms.
		// Rejecting them here (as this file did) meant limitcam channels were
		// claimed as live by liveChecker.IsLive but never recorded. Hidden rooms
		// fall through to the stream-URL check below; if the room has no URL it
		// is correctly treated as offline.
		switch resp.RoomStatus {
		case StatusPrivate:
			return nil, resp.RoomStatus, internal.ErrPrivateStream
		case StatusAway, StatusOffline:
			return nil, resp.RoomStatus, internal.ErrChannelOffline
		}

		if resp.StreamURL() == "" {
			return nil, resp.RoomStatus, internal.ErrChannelOffline
		}

		workingURL := resp.StreamURL()
		if !skipEdgeCheck {
			url, edgeErr := findWorkingEdgeURL(ctx, client, workingURL)
			if edgeErr != nil {
				return nil, resp.RoomStatus, edgeErr
			}
			workingURL = url
		}

		return &Stream{HLSSource: workingURL}, resp.RoomStatus, nil
	}

	// Parse POST API response
	var resp APIResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, "", fmt.Errorf("failed to parse POST API response: %w", err)
	}

	if roomInfo != nil {
		roomInfo.RoomTitle = resp.RoomTitle
		roomInfo.Tags = resp.Tags
		roomInfo.NumUsers = resp.NumUsers
		roomInfo.SummaryCardImage = resp.SummaryCardImage
		roomInfo.EdgeRegion = resp.EdgeRegion
		if resp.BroadcasterGender != "" {
			roomInfo.BroadcasterGender = resp.BroadcasterGender
		}
	}

	if server.Config.Debug {
		fmt.Printf("[DEBUG] %s POST API response: status=%s url=%s\n", username, resp.RoomStatus, resp.StreamURL())
	}

	if resp.Code == "unauthorized" {
		return nil, resp.RoomStatus, internal.ErrRoomPasswordRequired
	}

	// Enrich metadata from the GET API (chatvideocontext) which reliably
	// returns tags, room_title, num_users, and broadcaster_gender even when
	// the POST endpoint only returns the HLS URL.
	if getResp, getErr := fetchAPIResponse(ctx, client, username); getErr == nil {
		if roomInfo != nil {
			if getResp.RoomTitle != "" {
				roomInfo.RoomTitle = getResp.RoomTitle
			}
			if len(getResp.Tags) > 0 {
				roomInfo.Tags = getResp.Tags
			}
			if getResp.NumUsers > 0 {
				roomInfo.NumUsers = getResp.NumUsers
			}
			if getResp.BroadcasterGender != "" {
				roomInfo.BroadcasterGender = getResp.BroadcasterGender
			}
			if getResp.SummaryCardImage != "" {
				roomInfo.SummaryCardImage = getResp.SummaryCardImage
			}
			if getResp.EdgeRegion != "" {
				roomInfo.EdgeRegion = getResp.EdgeRegion
			}
		}
		if resp.RoomStatus == "" && getResp.RoomStatus != "" {
			resp.RoomStatus = getResp.RoomStatus
		}
	}

	// Hidden (limitcam) rooms stream like public ones — do not reject (see above).
	switch resp.RoomStatus {
	case StatusPrivate:
		return nil, resp.RoomStatus, internal.ErrPrivateStream
	case StatusAway, StatusOffline:
		return nil, resp.RoomStatus, internal.ErrChannelOffline
	}

	// If POST API returned a public room but no HLS source, fall back to GET API.
	if resp.StreamURL() == "" {
		if server.Config.Debug {
			fmt.Printf("[WARN] %s: POST API returned empty URL, trying GET API fallback (check cookies if this persists)\n", username)
		}
		getResp, apiErr := fetchAPIResponse(ctx, client, username)
		if apiErr == nil && getResp.StreamURL() != "" {
			resp = *getResp
			if roomInfo != nil {
				roomInfo.RoomTitle = getResp.RoomTitle
				roomInfo.Tags = getResp.Tags
				roomInfo.NumUsers = getResp.NumUsers
				roomInfo.SummaryCardImage = getResp.SummaryCardImage
				if getResp.BroadcasterGender != "" {
					roomInfo.BroadcasterGender = getResp.BroadcasterGender
				}
			}
		} else {
			if apiErr == nil {
				switch getResp.RoomStatus {
				case StatusPrivate:
					return nil, getResp.RoomStatus, internal.ErrPrivateStream
				default:
					return nil, getResp.RoomStatus, internal.ErrChannelOffline
				}
			}
			return nil, resp.RoomStatus, internal.ErrChannelOffline
		}
	}

	workingURL := resp.StreamURL()
	if !skipEdgeCheck {
		url, edgeErr := findWorkingEdgeURL(ctx, client, workingURL)
		if edgeErr != nil {
			return nil, resp.RoomStatus, edgeErr
		}
		workingURL = url
	}

	return &Stream{HLSSource: workingURL}, resp.RoomStatus, nil
}

// findWorkingEdgeURL validates the HLS URL and tries alternative edge regions if geo-blocked.
func findWorkingEdgeURL(ctx context.Context, client *internal.Req, hlsSource string) (string, error) {
	// LL-HLS URLs use token-based sessions; HEAD requests consume the token
	// and cause subsequent GET requests to fail with "session_duplicated".
	// Skip HEAD validation for these URLs.
	if strings.Contains(hlsSource, "llhls.m3u8") {
		return hlsSource, nil
	}

	// 1. Validate original URL
	statusCode, err := client.Head(ctx, hlsSource)
	if err == nil && statusCode == 200 {
		return hlsSource, nil
	}
	if server.Config.Debug {
		fmt.Printf("[DEBUG] findWorkingEdgeURL: original HEAD -> status=%d err=%v\n", statusCode, err)
	}

	// 2. Extract current region from URL
	matches := edgeRegionRegexp.FindStringSubmatch(hlsSource)
	if len(matches) < 2 {
		return hlsSource, nil
	}
	currentRegion := matches[1]

	// 3. Try alternative edge regions: lax, fra, ams, sin, hnd
	for _, region := range edgeRegions {
		if region == currentRegion {
			continue
		}
		altURL := strings.Replace(hlsSource, "-"+currentRegion+".", "-"+region+".", 1)

		statusCode, err := client.Head(ctx, altURL)
		if err == nil && statusCode == 200 {
			return altURL, nil
		}
		if server.Config.Debug {
			fmt.Printf("[DEBUG] findWorkingEdgeURL: alt region %s -> status=%d err=%v\n", region, statusCode, err)
		}
	}

	// 4. If we couldn't validate any edge, return the original URL anyway
	//    so the recorder can try GETing it directly (HEAD may be blocked by CDN).
	return hlsSource, nil
}

// Stream represents an HLS stream source.
type Stream struct {
	HLSSource string
}

// GetPlaylist retrieves the playlist corresponding to the given resolution and framerate.
func (s *Stream) GetPlaylist(ctx context.Context, resolution, framerate int) (*Playlist, error) {
	return FetchPlaylist(ctx, s.HLSSource, resolution, framerate, "", "")
}

// FetchPlaylist fetches and decodes the HLS playlist file.
//
// cdnReferer, when non-empty, is the Referer/Origin sent for CDN media requests
// (e.g. Stripchat requires a stripchat.com referer).  mouflonPDKey is the
// Stripchat MOUFLON v2 decryption key; pass "auto" to resolve it from the
// master playlist, or "" when not applicable.
func FetchPlaylist(ctx context.Context, hlsSource string, resolution, framerate int, cdnReferer, mouflonPDKey string) (*Playlist, error) {
	if hlsSource == "" {
		return nil, internal.ErrChannelOffline
	}

	var client *internal.Req
	if cdnReferer != "" {
		client = internal.NewMediaReqWithReferer(cdnReferer)
	} else {
		client = internal.NewMediaReq()
	}
	resp, source, err := fetchPlaylistSource(ctx, client, hlsSource)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch HLS source: %w", err)
	}

	if server.Config.Debug {
		fmt.Printf("[DEBUG] master playlist response for %s:\n%s\n", source, resp)
	}

	// Extract Stripchat's custom MOUFLON tag which carries the CDN pkey.
	// Format: #EXT-X-MOUFLON:PSCH:v2:{pkey}
	// The variant URLs in the master omit the pkey; it must be appended when fetching.
	pkey := stripchat.ParsePKeyFromMaster(resp)
	if pkey != "" {
		if mouflonPDKey == "auto" {
			mouflonPDKey = stripchat.ResolvePDKey(ctx, pkey)
			switch mouflonPDKey {
			case "pending":
				if server.Config.Debug {
					fmt.Println("[DEBUG] mouflon: candidate keys extracted; will test against first encrypted segment")
				}
			case "":
				fmt.Printf("[WARN] mouflon: no pdkey for pkey=%s; segments will 404. Use --stripchat-pdkey to set manually.\n", pkey)
			default:
				if server.Config.Debug {
					fmt.Printf("[DEBUG] mouflon: pdkey resolved for pkey=%s (%d chars)\n", pkey, len(mouflonPDKey))
				}
			}
		}
	}

	playlist, err := ParsePlaylist(resp, source, resolution, framerate)
	if err != nil {
		return nil, err
	}
	if pkey != "" {
		playlist.PlaylistURL = withMouflonParams(playlist.PlaylistURL, pkey)
		if playlist.AudioPlaylistURL != "" {
			playlist.AudioPlaylistURL = withMouflonParams(playlist.AudioPlaylistURL, pkey)
		}
	}
	playlist.Client = client
	playlist.MouflonPDKey = mouflonPDKey
	playlist.MasterURL = source
	return playlist, nil
}

func fetchPlaylistSource(ctx context.Context, client *internal.Req, hlsSource string) (string, string, error) {
	resp, err := retry.DoWithData(
		func() (string, error) {
			return client.Get(ctx, hlsSource)
		},
		retry.Context(ctx),
		retry.Attempts(3),
		retry.Delay(500*time.Millisecond),
		retry.MaxDelay(3*time.Second),
		retry.DelayType(retry.BackOffDelay),
	)
	if err == nil {
		return resp, hlsSource, nil
	}

	// LL-HLS session URLs should not be probed with HEAD because that can
	// consume the token. If the first GET cannot connect to the assigned
	// CDN edge, try the same URL on alternate regions using GET only.
	if !strings.Contains(hlsSource, "llhls.m3u8") {
		return "", hlsSource, err
	}
	for _, altURL := range alternateEdgeURLs(hlsSource) {
		altResp, altErr := client.Get(ctx, altURL)
		if altErr == nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] FetchPlaylist: recovered via alternate edge %s\n", edgeHostForLog(altURL))
			}
			return altResp, altURL, nil
		}
		if server.Config.Debug {
			fmt.Printf("[DEBUG] FetchPlaylist: alternate edge %s failed: %v\n", edgeHostForLog(altURL), altErr)
		}
	}

	return "", hlsSource, err
}

func alternateEdgeURLs(hlsSource string) []string {
	matches := edgeRegionRegexp.FindStringSubmatch(hlsSource)
	if len(matches) < 2 {
		return nil
	}
	currentRegion := matches[1]
	urls := make([]string, 0, len(edgeRegions)-1)
	for _, region := range edgeRegions {
		if region == currentRegion {
			continue
		}
		urls = append(urls, strings.Replace(hlsSource, "-"+currentRegion+".", "-"+region+".", 1))
	}
	return urls
}

func edgeHostForLog(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "unknown"
	}
	return u.Host
}

// ParsePlaylist decodes the M3U8 playlist and extracts the variant streams.
func ParsePlaylist(resp, hlsSource string, resolution, framerate int) (*Playlist, error) {
	p, _, err := m3u8.DecodeFrom(strings.NewReader(resp), true)
	if err != nil {
		if server.Config.Debug {
			fmt.Printf("[DEBUG] master playlist parse failed: %v\n", err)
			fmt.Printf("[DEBUG]   HLS source URL: %s\n", hlsSource)
			end := len(resp)
			if end > 2000 {
				end = 2000
			}
			fmt.Printf("[DEBUG]   Response (first 2000 chars):\n%s\n", resp[:end])
		}
		return nil, fmt.Errorf("failed to decode m3u8 playlist: %w", err)
	}

	masterPlaylist, ok := p.(*m3u8.MasterPlaylist)
	if !ok {
		return nil, errors.New("invalid master playlist format")
	}

	return PickPlaylist(masterPlaylist, hlsSource, resolution, framerate)
}

// Playlist represents an HLS playlist containing variant streams.
type Playlist struct {
	PlaylistURL      string
	AudioPlaylistURL string
	RootURL          string // base for resolving video segment URIs
	MasterURL        string // original master HLS source URL (for re-fetching)
	Resolution       int
	Framerate        int
	FileExt          string        // ".ts" for legacy HLS, ".mp4" for LL-HLS fMP4
	Client           *internal.Req // reuse the same client that fetched the master playlist
	MouflonPDKey     string        // Stripchat MOUFLON v2 decryption key; empty for Chaturbate

	// RefreshURL re-fetches fresh HLS playlist URLs after a token/session
	// expiry (CDN 403). When set, the watch loop calls it on a media-forbidden
	// error and keeps the SAME open recording file (continuing to append
	// segments) instead of finalising the current fragment and starting a new
	// one. This is the root-cause fix for ~20-minute fragmentation: Chaturbate
	// rotates its HLS token every ~20 minutes, which surfaces as a 403. By
	// re-fetching a fresh token and continuing — exactly what streamlink,
	// yt-dlp (--hls-use-mpegts) and hls.js do — a live session stays one
	// continuous recording up to max duration. May be nil (legacy callers keep
	// the old finalise-and-reconnect behaviour).
	RefreshURL func(ctx context.Context) (videoURL, audioURL, rootURL string, err error)
}

// Resolution represents a video resolution and its corresponding framerate.
type Resolution struct {
	Framerate map[int]string // [framerate]url
	Width     int
}

// PickPlaylist selects the best matching variant stream based on resolution and framerate.
func PickPlaylist(masterPlaylist *m3u8.MasterPlaylist, baseURL string, resolution, framerate int) (*Playlist, error) {
	resolutions := map[int]*Resolution{}

	// Extract available resolutions and framerates from the master playlist
	for _, v := range masterPlaylist.Variants {
		parts := strings.Split(v.Resolution, "x")
		if len(parts) != 2 {
			continue
		}
		width, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("parse resolution: %w", err)
		}
		framerateVal := 30
		if v.FrameRate >= 59.0 || strings.Contains(v.Name, "FPS:60.0") {
			framerateVal = 60
		}
		if _, exists := resolutions[width]; !exists {
			resolutions[width] = &Resolution{Framerate: map[int]string{}, Width: width}
		}
		resolutions[width].Framerate[framerateVal] = v.URI
	}

	// Find exact match for requested resolution
	variant, exists := resolutions[resolution]
	if !exists {
		// Filter resolutions below the requested resolution
		candidates := lo.Filter(lo.Values(resolutions), func(r *Resolution, _ int) bool {
			return r.Width < resolution
		})
		// Pick the highest resolution among the candidates
		variant = lo.MaxBy(candidates, func(a, b *Resolution) bool {
			return a.Width > b.Width
		})
	}
	if variant == nil {
		return nil, fmt.Errorf("resolution not found")
	}

	var (
		finalResolution = variant.Width
		finalFramerate  = framerate
	)
	// Select the desired framerate, or fallback to the first available framerate
	playlistURL, exists := variant.Framerate[framerate]
	if !exists {
		for fr, u := range variant.Framerate {
			playlistURL = u
			finalFramerate = fr
			break
		}
	}

	fileExt := ".ts"
	if strings.Contains(playlistURL, "llhls") || strings.HasSuffix(strings.SplitN(playlistURL, "?", 2)[0], ".m4s") {
		fileExt = ".mp4"
	}

	// Stripchat uses fMP4 segments (.mp4) even though the playlist URL
	// doesn't contain "llhls" or end in ".m4s". Detect by checking the
	// master playlist for EXT-X-MAP (init segment indicator) in any variant.
	if fileExt == ".ts" && strings.Contains(baseURL, "doppiocdn") {
		fileExt = ".mp4"
	}

	// For LL-HLS streams, find the audio rendition from the selected variant's EXT-X-MEDIA alternatives.
	var audioPlaylistURL string
	if fileExt == ".mp4" {
		for _, v := range masterPlaylist.Variants {
			if v.URI == playlistURL {
				for _, alt := range v.Alternatives {
					if alt != nil && alt.Type == "AUDIO" && alt.URI != "" {
						audioPlaylistURL = resolveHLSURL(baseURL, alt.URI)
						if alt.Default {
							break
						}
					}
				}
				break
			}
		}
		if server.Config.Debug {
			if audioPlaylistURL != "" {
				fmt.Printf("[DEBUG] LL-HLS audio rendition: %s\n", audioPlaylistURL)
			} else {
				fmt.Printf("[DEBUG] LL-HLS stream has no separate audio rendition\n")
			}
		}
	}

	resolvedPlaylist := resolveHLSURL(baseURL, playlistURL)
	return &Playlist{
		PlaylistURL:      resolvedPlaylist,
		AudioPlaylistURL: audioPlaylistURL,
		RootURL:          strings.SplitN(resolvedPlaylist, "?", 2)[0],
		Resolution:       finalResolution,
		Framerate:        finalFramerate,
		FileExt:          fileExt,
	}, nil
}

// resolveHLSURL resolves a potentially relative or absolute URI against a base
// URL. The base's query string is stripped so pkey/token suffixes can be added
// cleanly by withMouflonParams.
func resolveHLSURL(base, ref string) string {
	baseClean := strings.SplitN(base, "?", 2)[0]
	baseURL, err := url.Parse(baseClean)
	if err != nil {
		return base + ref
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return base + ref
	}
	return baseURL.ResolveReference(refURL).String()
}

// withMouflonParams appends the psch=v2&pkey= query parameters needed for
// Stripchat MOUFLON-protected segment playlists.
func withMouflonParams(u, pkey string) string {
	if pkey == "" || u == "" {
		return u
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	q := parsed.Query()
	q.Set("psch", "v2")
	q.Set("pkey", pkey)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

// WatchHandler is a function type that processes video segments.
type WatchHandler func(b []byte, duration float64) error

// WatchSegments continuously fetches and processes video segments.
// For LL-HLS streams with a separate audio rendition it automatically muxes
// audio and video into a single fragmented MP4 output stream.
func (p *Playlist) WatchSegments(ctx context.Context, handler WatchHandler) error {
	if p.AudioPlaylistURL != "" {
		return p.watchMuxedSegments(ctx, handler)
	}
	return p.watchVideoOnlySegments(ctx, handler)
}

// streamStallPolls is the number of consecutive watch-loop polls that return no
// new HLS segments before the session is treated as stalled (ErrStreamStalled).
// Polls run at ~1s each, so this is roughly 30s of silence — long enough to
// ride out the CDN's natural segment cadence (2-4s) plus minor lag, short
// enough to notice an expired session token quickly. The Monitor loop treats
// ErrStreamStalled as a soft error: it finalises the current file and re-fetches
// a fresh HLS URL so recording resumes immediately.
var streamStallPolls = 30

// safeDecodeFrom wraps m3u8.DecodeFrom with a recover so that library panics
// (e.g. nil-pointer on unknown LL-HLS tags) are returned as errors instead of
// crashing the process.
func safeDecodeFrom(r io.Reader) (pl m3u8.Playlist, listType m3u8.ListType, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("m3u8 decode panic: %v", rec)
		}
	}()
	return m3u8.DecodeFrom(r, true)
}

// decodeMouflon rewrites a Stripchat media playlist that uses the proprietary
// #EXT-X-MOUFLON:URI: tag to hide real segment URLs behind a generic placeholder
// (e.g. https://.../media.mp4). Each MOUFLON URI tag is consumed and its real
// URL replaces the following non-comment placeholder line.
//
// When pdkey is non-empty, the encrypted token in each URI is decrypted using
// the MOUFLON v2 algorithm (reverse -> base64-decode -> XOR SHA256(pdkey)).
// If pdkey is "pending", the first encrypted URI is used to brute-force the
// correct key from candidate strings extracted from the player JS.
func decodeMouflon(resp, pdkey string) string {
	if !strings.Contains(resp, "#EXT-X-MOUFLON:URI:") {
		return resp
	}

	// If pdkey is "pending", try to find the working key from candidates
	// using the first MOUFLON URI as a test sample.
	if pdkey == "pending" {
		for _, line := range strings.Split(resp, "\n") {
			trimmed := strings.TrimRight(line, "\r")
			if strings.HasPrefix(trimmed, "#EXT-X-MOUFLON:URI:") {
				sampleURI := strings.TrimPrefix(trimmed, "#EXT-X-MOUFLON:URI:")
				found := stripchat.TryFindWorkingKey(sampleURI)
				if found != "" {
					pdkey = found
				} else {
					pdkey = "" // no working key found
				}
				break
			}
		}
	}

	lines := strings.Split(resp, "\n")
	out := make([]string, 0, len(lines))
	var pendingURI string
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		if strings.HasPrefix(trimmed, "#EXT-X-MOUFLON:URI:") {
			uri := strings.TrimPrefix(trimmed, "#EXT-X-MOUFLON:URI:")
			if pdkey != "" {
				decrypted, err := stripchat.DecryptMouflonURI(uri, pdkey)
				if err != nil {
					if server.Config.Debug {
						fmt.Printf("[DEBUG] mouflon decrypt failed for URI: %v\n", err)
					}
				} else {
					uri = decrypted
				}
			}
			pendingURI = uri
			continue // drop the MOUFLON tag line entirely
		}
		if pendingURI != "" && !strings.HasPrefix(trimmed, "#") && trimmed != "" {
			out = append(out, pendingURI) // real URI replaces placeholder
			pendingURI = ""
			continue // drop the placeholder line
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, "\n")
}

// normalizeM3U8 fixes non-standard #EXTINF lines that lack a trailing comma,
// and strips LL-HLS extension tags that cause the m3u8 library to panic.
// Some CDNs (e.g. Stripchat) emit "#EXTINF:2.000" instead of "#EXTINF:2.000,".
func normalizeM3U8(resp string) string {
	// LL-HLS tags the grafov/m3u8 library cannot handle without panicking.
	stripPrefixes := []string{
		"#EXT-X-PART:",
		"#EXT-X-PART-INF:",
		"#EXT-X-PRELOAD-HINT:",
		"#EXT-X-SERVER-CONTROL:",
		"#EXT-X-RENDITION-REPORT:",
		"#EXT-X-MOUFLON:",
	}
	lines := strings.Split(resp, "\n")
	out := lines[:0]
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		skip := false
		for _, pfx := range stripPrefixes {
			if strings.HasPrefix(trimmed, pfx) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if strings.HasPrefix(trimmed, "#EXTINF:") && !strings.Contains(trimmed, ",") {
			trimmed = trimmed + ","
		}
		out = append(out, trimmed)
	}
	return strings.Join(out, "\n")
}

// watchVideoOnlySegments is the original single-track segment loop.
func (p *Playlist) watchVideoOnlySegments(ctx context.Context, handler WatchHandler) error {
	client := p.Client
	if client == nil {
		client = internal.NewMediaReq()
	}
	lastSeq := -1
	lastSegURI := ""
	lastMapURI := ""
	consecutiveErrors := 0
	stalledPolls := 0

	// For fMP4 streams, normalise tfdt timestamps so the recording starts
	// at 0:00 instead of the CDN's absolute stream uptime. Always attempt
	// this — extractAllTrackBaseTimes returns nil on non-fMP4 (.ts) data.
	var trackBaseTimes map[uint32]uint64

	for {
		sawNewSegment := false
		resp, err := client.Get(ctx, p.PlaylistURL)
		if err != nil {
			if errors.Is(err, internal.ErrMediaForbidden) && p.RefreshURL != nil {
				// Token/session expired (CDN 403). Re-fetch a fresh HLS URL and
				// keep recording into the SAME open file — do NOT finalise and
				// start a new fragment. This is what keeps a live session
				// continuous instead of splitting every ~20 minutes.
				if vURL, aURL, rURL, rerr := p.RefreshURL(ctx); rerr == nil && vURL != "" {
					p.PlaylistURL = vURL
					p.AudioPlaylistURL = aURL
					p.RootURL = rURL
					consecutiveErrors = 0
					<-time.After(300 * time.Millisecond)
					continue
				}
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}

		pl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(resp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] variant playlist parse failed: %v\n", err)
				fmt.Printf("[DEBUG]   Playlist URL: %s\n", p.PlaylistURL)
				end := len(resp)
				if end > 2000 {
					end = 2000
				}
				fmt.Printf("[DEBUG]   Response (first 2000 chars):\n%s\n", resp[:end])
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode from: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		playlist, ok := pl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast to media playlist")
		}
		consecutiveErrors = 0

		for _, v := range playlist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			// Fall back to the HLS media sequence number (v.SeqId) when the URI
			// doesn't contain a parseable sequence (e.g. Stripchat .mp4 segments).
			if seq == -1 && v.SeqId > 0 {
				seq = int(v.SeqId)
			}
			if seq != -1 {
				if seq <= lastSeq {
					continue
				}
				lastSeq = seq
			} else {
				if v.URI == lastSegURI {
					continue
				}
			}
			lastSegURI = v.URI
			sawNewSegment = true
			if v.Map != nil && v.Map.URI != lastMapURI {
				mapURL := resolveHLSURL(p.RootURL, v.Map.URI)
				initBytes, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get init segment: %w", err)
				}
				if err := handler(initBytes, 0); err != nil {
					return fmt.Errorf("handler init segment: %w", err)
				}
				lastMapURI = v.Map.URI
			}

			pipeline := func() ([]byte, error) {
				return client.GetBytes(ctx, resolveHLSURL(p.RootURL, v.URI))
			}
			resp, err := retry.DoWithData(
				pipeline,
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool {
					return !errors.Is(err, internal.ErrNotFound)
				}),
			)
			if err != nil {
				if errors.Is(err, internal.ErrNotFound) {
					if server.Config.Debug {
						fmt.Printf("[DEBUG] segment 404 (skipping): seq=%d %s\n", seq, resolveHLSURL(p.RootURL, v.URI))
					}
					continue // segment expired on CDN; move on to next
				}
				if server.Config.Debug {
					fmt.Printf("[DEBUG] segment error (breaking inner loop): seq=%d err=%v\n", seq, err)
				}
				break
			}
			// Normalise fMP4 tfdt so playback starts at 0:00 (all tracks).
			if trackBaseTimes == nil {
				trackBaseTimes = extractAllTrackBaseTimes(resp)
			}
			if trackBaseTimes != nil {
				resp = shiftSegmentAllTracks(resp, trackBaseTimes)
			}
			if err := handler(resp, v.Duration); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		}

		if !sawNewSegment {
			stalledPolls++
			if stalledPolls >= streamStallPolls {
				return fmt.Errorf("stream stalled: %w", internal.ErrStreamStalled)
			}
		} else {
			stalledPolls = 0
		}

		<-time.After(1 * time.Second)
	}
}

// watchMuxedSegments polls both video and audio LL-HLS playlists, combines their
// init segments into a single fMP4 init, then writes interleaved video and
// audio moof+mdat fragments. Audio track_id is renumbered to 2.
// tfdt decode times are normalised to start from zero so players display the
// correct recording position rather than the CDN stream uptime offset.
func (p *Playlist) watchMuxedSegments(ctx context.Context, handler WatchHandler) error {
	client := p.Client
	if client == nil {
		client = internal.NewMediaReq()
	}

	lastVideoSeq := -1
	lastAudioSeq := -1
	lastVideoURI := ""
	lastAudioURI := ""
	lastVideoMapURI := ""
	lastAudioMapURI := ""
	var videoInitBytes []byte
	var audioInitBytes []byte
	initWritten := false
	consecutiveErrors := 0
	stalledPolls := 0

	// Per-track tfdt base times captured from the first segment of each track.
	// Subtracting these normalises timestamps to start from zero.
	var videoTimeBase uint64
	var audioTimeBase uint64
	videoBaseSet := false
	audioBaseSet := false

	for {
		// Fetch video playlist
		videoResp, err := client.Get(ctx, p.PlaylistURL)
		if err != nil {
			if errors.Is(err, internal.ErrMediaForbidden) && p.RefreshURL != nil {
				if vURL, aURL, rURL, rerr := p.RefreshURL(ctx); rerr == nil && vURL != "" {
					p.PlaylistURL = vURL
					p.AudioPlaylistURL = aURL
					p.RootURL = rURL
					consecutiveErrors = 0
					<-time.After(300 * time.Millisecond)
					continue
				}
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get video playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		vpl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(videoResp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] muxed: video playlist parse failed: %v\n", err)
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode video playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		videoPlaylist, ok := vpl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast video playlist to media playlist")
		}

		// Fetch audio playlist
		audioResp, err := client.Get(ctx, p.AudioPlaylistURL)
		if err != nil {
			if errors.Is(err, internal.ErrMediaForbidden) && p.RefreshURL != nil {
				if vURL, aURL, rURL, rerr := p.RefreshURL(ctx); rerr == nil && vURL != "" {
					p.PlaylistURL = vURL
					p.AudioPlaylistURL = aURL
					p.RootURL = rURL
					consecutiveErrors = 0
					<-time.After(300 * time.Millisecond)
					continue
				}
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("get audio playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		apl, _, err := safeDecodeFrom(strings.NewReader(normalizeM3U8(decodeMouflon(audioResp, p.MouflonPDKey))))
		if err != nil {
			if server.Config.Debug {
				fmt.Printf("[DEBUG] muxed: audio playlist parse failed: %v\n", err)
			}
			if consecutiveErrors++; consecutiveErrors >= 5 {
				return fmt.Errorf("decode audio playlist: %w", err)
			}
			<-time.After(2 * time.Second)
			continue
		}
		audioPlaylist, ok := apl.(*m3u8.MediaPlaylist)
		if !ok {
			return fmt.Errorf("cast audio playlist to media playlist")
		}
		consecutiveErrors = 0

		// Collect video init segment (EXT-X-MAP)
		for _, v := range videoPlaylist.Segments {
			if v == nil {
				continue
			}
			if v.Map != nil && v.Map.URI != lastVideoMapURI {
				mapURL := resolveHLSURL(p.RootURL, v.Map.URI)
				b, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get video init segment: %w", err)
				}
				videoInitBytes = b
				lastVideoMapURI = v.Map.URI
				initWritten = false
			}
			break
		}

		// Collect audio init segment (EXT-X-MAP)
		for _, v := range audioPlaylist.Segments {
			if v == nil {
				continue
			}
			if v.Map != nil && v.Map.URI != lastAudioMapURI {
				mapURL := resolveHLSURL(p.AudioPlaylistURL, v.Map.URI)
				b, err := client.GetBytes(ctx, mapURL)
				if err != nil {
					return fmt.Errorf("get audio init segment: %w", err)
				}
				audioInitBytes = b
				lastAudioMapURI = v.Map.URI
				initWritten = false
			}
			break
		}

		// Write combined init once we have both init segments
		if !initWritten && videoInitBytes != nil && audioInitBytes != nil {
			combined, err := buildCombinedInit(videoInitBytes, audioInitBytes)
			if err != nil {
				return fmt.Errorf("build combined init: %w", err)
			}
			if err := handler(combined, 0); err != nil {
				return fmt.Errorf("handler combined init: %w", err)
			}
			initWritten = true
		}
		if !initWritten {
			// Never got a usable combined init (e.g. playlists without
			// EXT-X-MAP, or a stalled session whose maps rotated away). This
			// used to spin forever without recording — count it as a stall so
			// the Monitor can re-fetch a fresh HLS URL instead of hanging.
			stalledPolls++
			if stalledPolls >= streamStallPolls {
				return fmt.Errorf("stream stalled: %w", internal.ErrStreamStalled)
			}
			<-time.After(1 * time.Second)
			continue
		}

		// Collect new segment URLs. Pre-resolve URLs to avoid closure capture
		// issues, and fall back to URI-string dedup when seq is unavailable.
		type segInfo struct {
			url      string
			duration float64
		}
		var newVideoSegs []segInfo
		for _, v := range videoPlaylist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			if seq != -1 {
				if seq <= lastVideoSeq {
					continue
				}
				lastVideoSeq = seq
			} else {
				if v.URI == lastVideoURI {
					continue
				}
			}
			lastVideoURI = v.URI
			newVideoSegs = append(newVideoSegs, segInfo{
				url:      resolveHLSURL(p.RootURL, v.URI),
				duration: v.Duration,
			})
		}
		var newAudioSegs []segInfo
		for _, v := range audioPlaylist.Segments {
			if v == nil {
				continue
			}
			seq := internal.SegmentSeq(v.URI)
			if seq != -1 {
				if seq <= lastAudioSeq {
					continue
				}
				lastAudioSeq = seq
			} else {
				if v.URI == lastAudioURI {
					continue
				}
			}
			lastAudioURI = v.URI
			newAudioSegs = append(newAudioSegs, segInfo{
				url:      resolveHLSURL(p.AudioPlaylistURL, v.URI),
				duration: v.Duration,
			})
		}

		if server.Config.Debug {
			fmt.Printf("[DEBUG] muxed: cycle video=%d audio=%d\n", len(newVideoSegs), len(newAudioSegs))
		}

		if len(newVideoSegs) == 0 && len(newAudioSegs) == 0 {
			stalledPolls++
			if stalledPolls >= streamStallPolls {
				return fmt.Errorf("stream stalled: %w", internal.ErrStreamStalled)
			}
		} else {
			stalledPolls = 0
		}

		isStripchatMux := strings.Contains(p.PlaylistURL, "doppiocdn") || strings.Contains(p.AudioPlaylistURL, "doppiocdn")

		// Stripchat can expose video/audio playlists with different cadence,
		// and index-based pairing can produce files that begin with a long
		// video-only run after a split. Keep Chaturbate on the original paired
		// write order because it was already behaving correctly there.
		if !isStripchatMux {
			maxLen := len(newVideoSegs)
			if len(newAudioSegs) > maxLen {
				maxLen = len(newAudioSegs)
			}
			for i := 0; i < maxLen; i++ {
				var chunk []byte
				var chunkDuration float64

				if i < len(newVideoSegs) {
					vseg := newVideoSegs[i]
					vsegURL := vseg.url
					segBytes, err := retry.DoWithData(
						func() ([]byte, error) { return client.GetBytes(ctx, vsegURL) },
						retry.Context(ctx),
						retry.Attempts(3),
						retry.Delay(600*time.Millisecond),
						retry.DelayType(retry.FixedDelay),
					)
					if err == nil {
						if !videoBaseSet {
							if t, ok := extractMoofFirstTfdt(segBytes); ok {
								videoTimeBase = t
								videoBaseSet = true
							}
						}
						segBytes = shiftSegmentTfdt(segBytes, 1, videoTimeBase)
						chunk = append(chunk, segBytes...)
						chunkDuration = vseg.duration
					}
				}
				if i < len(newAudioSegs) {
					aseg := newAudioSegs[i]
					asegURL := aseg.url
					segBytes, err := retry.DoWithData(
						func() ([]byte, error) { return client.GetBytes(ctx, asegURL) },
						retry.Context(ctx),
						retry.Attempts(3),
						retry.Delay(600*time.Millisecond),
						retry.DelayType(retry.FixedDelay),
					)
					if err != nil {
						fmt.Printf("[WARN] audio seg download failed: %v\n", err)
					} else {
						if !audioBaseSet {
							if t, ok := extractMoofFirstTfdt(segBytes); ok {
								audioTimeBase = t
								audioBaseSet = true
								if server.Config.Debug {
									fmt.Printf("[DEBUG] muxed: audio base=%d\n", audioTimeBase)
								}
							}
						}
						segBytes = rewriteAudioMoofTrackID(segBytes)
						segBytes = shiftSegmentTfdt(segBytes, 2, audioTimeBase)
						chunk = append(chunk, segBytes...)
					}
				}
				if len(chunk) > 0 {
					if err := handler(chunk, chunkDuration); err != nil {
						return fmt.Errorf("handler muxed segment group: %w", err)
					}
				}
			}

			<-time.After(1 * time.Second)
			continue
		}

		// Merge Stripchat by actual fragment decode time rather than playlist index.
		type pendingSeg struct {
			track    string
			time     uint64
			duration float64
			data     []byte
		}
		var pending []pendingSeg

		for _, vseg := range newVideoSegs {
			vsegURL := vseg.url
			segBytes, err := retry.DoWithData(
				func() ([]byte, error) { return client.GetBytes(ctx, vsegURL) },
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
			)
			if err != nil {
				fmt.Printf("[WARN] video seg download failed: %v\n", err)
				continue
			}

			rawTfdt, ok := extractMoofFirstTfdt(segBytes)
			if !videoBaseSet && ok {
				videoTimeBase = rawTfdt
				videoBaseSet = true
			}

			normalisedTime := rawTfdt
			if videoBaseSet && rawTfdt >= videoTimeBase {
				normalisedTime = rawTfdt - videoTimeBase
			}
			segBytes = shiftSegmentTfdt(segBytes, 1, videoTimeBase)
			pending = append(pending, pendingSeg{
				track:    "video",
				time:     normalisedTime,
				duration: vseg.duration,
				data:     segBytes,
			})
		}

		for _, aseg := range newAudioSegs {
			asegURL := aseg.url
			segBytes, err := retry.DoWithData(
				func() ([]byte, error) { return client.GetBytes(ctx, asegURL) },
				retry.Context(ctx),
				retry.Attempts(3),
				retry.Delay(600*time.Millisecond),
				retry.DelayType(retry.FixedDelay),
			)
			if err != nil {
				fmt.Printf("[WARN] audio seg download failed: %v\n", err)
				continue
			}

			rawTfdt, ok := extractMoofFirstTfdt(segBytes)
			if !audioBaseSet && ok {
				audioTimeBase = rawTfdt
				audioBaseSet = true
				if server.Config.Debug {
					fmt.Printf("[DEBUG] muxed: audio base=%d\n", audioTimeBase)
				}
			}

			normalisedTime := rawTfdt
			if audioBaseSet && rawTfdt >= audioTimeBase {
				normalisedTime = rawTfdt - audioTimeBase
			}
			if server.Config.Debug && ok {
				fmt.Printf("[DEBUG] muxed: audio seg dur=%.3f raw_tfdt=%d norm=%d\n", aseg.duration, rawTfdt, normalisedTime)
			}

			segBytes = rewriteAudioMoofTrackID(segBytes)
			segBytes = shiftSegmentTfdt(segBytes, 2, audioTimeBase)
			pending = append(pending, pendingSeg{
				track:    "audio",
				time:     normalisedTime,
				duration: 0,
				data:     segBytes,
			})
		}

		sort.SliceStable(pending, func(i, j int) bool {
			if pending[i].time != pending[j].time {
				return pending[i].time < pending[j].time
			}
			return pending[i].track < pending[j].track
		})

		for _, seg := range pending {
			if err := handler(seg.data, seg.duration); err != nil {
				return fmt.Errorf("handler muxed segment: %w", err)
			}
		}

		<-time.After(1 * time.Second)
	}
}
