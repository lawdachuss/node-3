package site

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
)

type StripchatSite struct{}

func NewStripchatSite() *StripchatSite {
	return &StripchatSite{}
}

// scModelInfo holds the per-model fields extracted from a Stripchat page state
// or v2 cam API response. The two sources expose the same model object, so a
// single shared struct is used for both.
type scModelInfo struct {
	Username           string   `json:"username"`
	Status             string   `json:"status"`
	IsLive             bool     `json:"isLive"`
	IsOnline           bool     `json:"isOnline"`
	BroadcastGender    string   `json:"broadcastGender"`
	Gender             string   `json:"gender"`
	PreviewUrlThumbBig string   `json:"previewUrlThumbBig"`
	SnapshotTimestamp  int64    `json:"snapshotTimestamp"`
	// Tag-bearing profile fields (see stripchatTags). The v2 API exposes all of
	// these; the legacy SSR page state exposes them on the same model object.
	Specifics        []string `json:"specifics"`
	Interests        []string `json:"interests"`
	PublicActivities []string `json:"publicActivities"`
	Subculture       string   `json:"subculture"`
	BodyType         string   `json:"bodyType"`
	Ethnicity        string   `json:"ethnicity"`
	HairColor        string   `json:"hairColor"`
	EyeColor         string   `json:"eyeColor"`
}

// scPageState holds the model page state needed to build an HLS recording URL.
// It normalises two data sources:
//  1. The v2 API: /api/front/v2/models/username/{username}/cam
//  2. The legacy SSR: window.__PRELOADED_STATE__ embedded in the HTML page.
type scPageState struct {
	ViewCamBase struct {
		Model scModelInfo `json:"model"`
	} `json:"viewCamBase"`
	ViewCam struct {
		StreamName  string            `json:"streamName"`
		ViewServers map[string]string `json:"viewServers"`
		IsCamActive bool              `json:"isCamActive"`
		Topic       string            `json:"topic"`
	} `json:"viewCam"`
}

// scV2CamResponse is the JSON structure returned by the Stripchat v2 cam API:
// /api/front/v2/models/username/{username}/cam?uniq={random}
type scV2CamResponse struct {
	User struct {
		User scModelInfo `json:"user"`
	} `json:"user"`
	// Cam is json.RawMessage because Stripchat sometimes returns an object and
	// sometimes an empty array (e.g. "cam": [] for an offline/unavailable model).
	// A concrete struct here made json.Unmarshal fail with "cannot unmarshal
	// array into Go struct field scV2CamResponse.cam of type struct {...}".
	Cam json.RawMessage `json:"cam"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// scV2Cam holds the parsed cam object fields (see scV2CamResponse.Cam).
type scV2Cam struct {
	StreamName     string            `json:"streamName"`
	ViewServers    map[string]string `json:"viewServers"`
	IsCamActive    bool              `json:"isCamActive"`
	IsCamAvailable bool              `json:"isCamAvailable"`
	Topic          string            `json:"topic"`
	PreviewURL     string            `json:"previewUrl"`
	SnapshotTs     int64             `json:"snapshotTs"`
}

// scUniq generates a random alphanumeric string to defeat CDN caching on
// Stripchat API calls (same approach as StreaMonitor).
func scUniq() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}

// fetchStripchatPage fetches model data from Stripchat. It tries two sources
// in order:
//  1. The v2 cam API (JSON, fast, reliable under httpcloak).
//  2. Legacy SSR page parsing (window.__PRELOADED_STATE__ as fallback).
//
// The v2 API is tried up to 2 times (with a brief backoff) because datacenter
// IPs can receive a transient 418/403 that succeeds on the next attempt.
// Returns a normalised scPageState and HTTP status.
func fetchStripchatPage(ctx context.Context, req *internal.Req, username string) (*scPageState, int, error) {
	// Try the v2 cam API (up to 2 attempts — transient 418s from Cloudflare
	// datacenter detection succeed on retry because httpcloak's TLS fingerprint
	// rotates per connection).
	var lastV2Err error
	var lastV2Status int
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			// Brief backoff before retry (100ms, then 250ms).
			time.Sleep(time.Duration(100+150*attempt) * time.Millisecond)
		}
		state, status, err := fetchStripchatV2API(ctx, req, username)
		if err == nil {
			return state, status, nil
		}
		lastV2Err = err
		lastV2Status = status
	}

	// Fall back to SSR page parsing.
	// NOTE: Stripchat removed __PRELOADED_STATE__ from SSR output in August
	// 2026, so this fallback always fails now. The error is surfaced only if
	// the caller needs it for diagnostics — callers should treat SSR failure
	// as "model is not publicly streamable" rather than a hard error.
	state, status, ssrErr := fetchStripchatSSRPage(ctx, req, username)
	if ssrErr == nil {
		return state, status, nil
	}

	// Both methods failed. Return the v2 error (primary method) rather than
	// the SSR error (expected to always fail now).
	return nil, lastV2Status, fmt.Errorf("stripchat: v2 api failed: %w; ssr fallback also failed: %w", lastV2Err, ssrErr)
}

// fetchStripchatModelID resolves a Stripchat username to a numeric model ID
// via the hu.stripchat.com user-ids endpoint. This endpoint is on a separate
// subdomain that is NOT blocked by Cloudflare's bot detection (unlike the
// main stripchat.com API).
func fetchStripchatModelID(ctx context.Context, req *internal.Req, username string) (int, error) {
	apiURL := fmt.Sprintf("https://hu.stripchat.com/api/front/users/user-ids/%s", username)

	body, statusCode, err := req.GetBytesWithStatus(ctx, apiURL)
	if err != nil {
		return 0, fmt.Errorf("stripchat: model ID lookup: %w", err)
	}
	if statusCode == http.StatusNotFound {
		return 0, internal.ErrNotFound
	}
	if statusCode != http.StatusOK {
		return 0, fmt.Errorf("stripchat: model ID lookup: HTTP %d", statusCode)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("stripchat: model ID parse: %w", err)
	}
	if result.ID == 0 {
		return 0, fmt.Errorf("stripchat: model ID not found for %s", username)
	}
	return result.ID, nil
}

// fetchStripchatV2API queries the Stripchat v2 cam endpoint and normalises
// the response into an scPageState. Uses the two-step flow:
//  1. Resolve username → model ID via hu.stripchat.com (not blocked by Cloudflare)
//  2. Fetch cam data via /api/front/v2/models/{modelId}/cam (model-ID-based, works)
//
// The username-based endpoint (/username/{username}/cam) returns HTTP 418 from
// datacenter IPs even with httpcloak — the model-ID-based endpoint does not.
func fetchStripchatV2API(ctx context.Context, req *internal.Req, username string) (*scPageState, int, error) {
	// Step 1: Resolve username to model ID.
	modelID, err := fetchStripchatModelID(ctx, req, username)
	if err != nil {
		return nil, 0, fmt.Errorf("stripchat: v2 api: %w", err)
	}

	// Step 2: Fetch cam data by model ID.
	apiURL := fmt.Sprintf("https://stripchat.com/api/front/v2/models/%d/cam?uniq=%s", modelID, scUniq())

	body, statusCode, err := req.GetBytesWithStatus(ctx, apiURL)
	if err != nil {
		return nil, statusCode, fmt.Errorf("stripchat: v2 api: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, statusCode, fmt.Errorf("stripchat: v2 api: HTTP %d", statusCode)
	}

	var resp scV2CamResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, statusCode, fmt.Errorf("stripchat: v2 api parse: %w", err)
	}

	// Check for API-level errors.
	if resp.Error != nil {
		if resp.Error.Code == "NOT_FOUND" || resp.Error.Message == "Not Found" {
			return nil, 404, internal.ErrNotFound
		}
		return nil, statusCode, fmt.Errorf("stripchat: v2 api error: %s: %s", resp.Error.Code, resp.Error.Message)
	}

	// Parse the cam object. Stripchat returns "cam": [] (empty array) for
	// offline/unavailable models — treat that as an offline model rather than
	// failing the whole fetch.
	var cam scV2Cam
	if len(resp.Cam) > 0 && !bytes.Equal(bytes.TrimSpace(resp.Cam), []byte("[]")) {
		if err := json.Unmarshal(resp.Cam, &cam); err != nil {
			return nil, statusCode, fmt.Errorf("stripchat: v2 api parse: %w", err)
		}
	}

	// Normalise to scPageState.
	state := &scPageState{}
	state.ViewCamBase.Model = resp.User.User
	if state.ViewCamBase.Model.BroadcastGender == "" {
		state.ViewCamBase.Model.BroadcastGender = resp.User.User.Gender
	}
	applyV2CamFields(state, &cam)

	return state, statusCode, nil
}

// applyV2CamFields overlays the cam-object fields onto the normalised page
// state. The cam object stopped exposing previewUrl/snapshotTs (verified absent
// for live models as of 2026-10); the preview data now lives only on
// user.user. Only overwrite when the cam object actually provides a value,
// otherwise the valid user.user preview URL would be wiped.
func applyV2CamFields(state *scPageState, cam *scV2Cam) {
	state.ViewCam.StreamName = cam.StreamName
	state.ViewCam.ViewServers = cam.ViewServers
	state.ViewCam.IsCamActive = cam.IsCamActive
	state.ViewCam.Topic = cam.Topic
	if cam.PreviewURL != "" {
		state.ViewCamBase.Model.PreviewUrlThumbBig = cam.PreviewURL
	}
	if cam.SnapshotTs != 0 {
		state.ViewCamBase.Model.SnapshotTimestamp = cam.SnapshotTs
	}
}

// fetchStripchatSSRPage parses the model page HTML for window.__PRELOADED_STATE__.
// This is the legacy method — Stripchat removed __PRELOADED_STATE__ from their
// SSR output around August 2026, so this now serves as a fallback only.
func fetchStripchatSSRPage(ctx context.Context, req *internal.Req, username string) (*scPageState, int, error) {
	pageURL := "https://stripchat.com/" + username

	body, statusCode, err := req.GetBytesWithStatus(ctx, pageURL)
	if err != nil {
		return nil, 0, fmt.Errorf("stripchat: fetch page: %w", err)
	}

	html := string(body)

	// Try multiple known SSR state variable names.
	for _, marker := range []string{"window.__PRELOADED_STATE__", "window.__INITIAL_STATE__", "window.__APP_STATE__"} {
		idx := strings.Index(html, marker)
		if idx < 0 {
			continue
		}
		start := idx + len(marker)
		for start < len(html) && (html[start] == ' ' || html[start] == '=') {
			start++
		}
		end := findJSONObjectEnd(html, start)
		if end < 0 {
			continue
		}
		var state scPageState
		if err := json.Unmarshal([]byte(html[start:end]), &state); err != nil {
			continue
		}
		return &state, statusCode, nil
	}

	return nil, statusCode, fmt.Errorf("stripchat: no SSR state found in page (HTTP %d, len %d)", statusCode, len(html))
}

// findJSONObjectEnd returns the index after the closing } of the JSON object
// starting at pos in s. Returns -1 if not found or pos does not point at '{'.
func findJSONObjectEnd(s string, pos int) int {
	if pos >= len(s) || s[pos] != '{' {
		return -1
	}
	depth := 0
	inStr := false
	escape := false
	for i := pos; i < len(s); i++ {
		c := s[i]
		if escape {
			escape = false
			continue
		}
		if c == '\\' && inStr {
			escape = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

func mapGender(g string) string {
	switch g {
	case "female":
		return "f"
	case "male":
		return "m"
	case "couple":
		return "c"
	case "trans":
		return "t"
	default:
		return g
	}
}

// scDisplayGender returns the model's broadcast gender, falling back to the
// plain gender field when broadcastGender is absent.
func scDisplayGender(m scModelInfo) string {
	if m.BroadcastGender != "" {
		return m.BroadcastGender
	}
	return m.Gender
}

// mouflonPDKey returns the manual Stripchat PD key if configured, otherwise
// "auto" so FetchPlaylist resolves the pkey from the master playlist and
// extracts/verifies the pdkey automatically.
func mouflonPDKey() string {
	if server.Config != nil && server.Config.StripchatPDKey != "" {
		return server.Config.StripchatPDKey
	}
	return "auto"
}

// camelSplitRegexp inserts a space before each capital letter that follows a
// lowercase letter or digit ("sexToys" -> "sex toys").
var camelSplitRegexp = regexp.MustCompile(`([a-z0-9])([A-Z])`)

// scActivityTag converts a Stripchat public-activity slug (e.g. "doBlowjob",
// "doSexToys") into a human-readable tag (e.g. "blowjob", "sex toys").
func scActivityTag(slug string) string {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return ""
	}
	slug = strings.TrimPrefix(slug, "do")
	slug = camelSplitRegexp.ReplaceAllString(slug, "${1} ${2}")
	return strings.ToLower(slug)
}

// scEnumPrefixes are the value prefixes Stripchat uses for profile-selector
// enums. e.g. "ethnicityWhite", "subcultureRomantic", "bodyTypeAverage",
// "hairColorBlack", "eyeColorBrown". Stored lowercased for matching.
var scEnumPrefixes = []string{"subculture", "bodytype", "ethnicity", "haircolor", "eyecolor"}

// stripchatEnumTag converts a Stripchat profile-enum value (e.g.
// "ethnicityWhite") into a plain tag (e.g. "white"). Values without a known
// prefix are passed through lowercased.
func stripchatEnumTag(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	for _, p := range scEnumPrefixes {
		if strings.HasPrefix(lower, p) {
			return strings.ToLower(strings.TrimSpace(v[len(p):]))
		}
	}
	return lower
}

// stripchatTags builds the tag list for a Stripchat model. Stripchat exposes
// tags in several places: #hashtags in the room topic, the profile "specifics"
// picks (its canonical tag vocabulary), free-text interests, public show
// activities and the profile-selector enums. All tags are lowercased and
// deduplicated to match the mergeHashtags normalisation used downstream.
func stripchatTags(m scModelInfo, topic string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(m.Specifics)+len(m.Interests)+len(m.PublicActivities)+8)
	add := func(tag string) {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}

	for _, word := range strings.Fields(topic) {
		if strings.HasPrefix(word, "#") {
			tag := strings.TrimPrefix(word, "#")
			tag = strings.Trim(tag, ".,!?;:")
			add(tag)
		}
	}
	for _, s := range m.Specifics {
		add(s)
	}
	for _, s := range m.Interests {
		add(s)
	}
	for _, s := range m.PublicActivities {
		add(scActivityTag(s))
	}
	add(stripchatEnumTag(m.Subculture))
	add(stripchatEnumTag(m.BodyType))
	add(stripchatEnumTag(m.Ethnicity))
	add(stripchatEnumTag(m.HairColor))
	add(stripchatEnumTag(m.EyeColor))

	return out
}

func (s *StripchatSite) FetchStream(ctx context.Context, req *internal.Req, username string) (*StreamInfo, error) {
	state, httpStatus, err := fetchStripchatPage(ctx, req, username)
	if err != nil {
		return nil, fmt.Errorf("stripchat: fetch stream: %w", err)
	}

	m := state.ViewCamBase.Model
	cam := state.ViewCam

	// 404 page = model does not exist
	if httpStatus == 404 {
		return nil, internal.ErrNotFound
	}

	// Build room status: Stripchat page state uses "off", "public", "private", etc.
	roomStatus := m.Status
	if roomStatus == "off" || (!m.IsOnline && !m.IsLive) {
		if roomStatus == "" || roomStatus == "off" {
			roomStatus = StatusOffline
		}
	}

	// Parse tags from the room topic, profile specifics/interests, activities
	// and profile-selector enums.
	tags := stripchatTags(m, cam.Topic)

	// Thumbnail URL with cache-buster
	thumbURL := m.PreviewUrlThumbBig
	if thumbURL != "" {
		if strings.Contains(thumbURL, "?") {
			thumbURL = fmt.Sprintf("%s&t=%d", thumbURL, m.SnapshotTimestamp)
		} else {
			thumbURL = fmt.Sprintf("%s?t=%d", thumbURL, m.SnapshotTimestamp)
		}
	}

	info := &StreamInfo{
		RoomStatus:   roomStatus,
		RoomTitle:    cam.Topic,
		Tags:         tags,
		Gender:       mapGender(scDisplayGender(m)),
		LiveThumbURL: thumbURL,
		// Stripchat CDNs require a stripchat.com Referer/Origin for media
		// requests, and MOUFLON segment decryption needs a pdkey.
		CDNReferer:   "https://stripchat.com/",
		MouflonPDKey: mouflonPDKey(),
	}

	// Model must be live and in a public show to record
	if !m.IsOnline && !m.IsLive {
		return info, internal.ErrChannelOffline
	}
	if !cam.IsCamActive {
		return info, internal.ErrChannelOffline
	}
	if roomStatus != "public" {
		return info, internal.ErrChannelOffline
	}

	// Build HLS URL from page-embedded stream data
	streamName := cam.StreamName
	if streamName == "" {
		return info, internal.ErrChannelOffline
	}

	var hlsURL string
	if svr, ok := cam.ViewServers["flashphoner-hls"]; ok && svr != "" {
		hlsURL = fmt.Sprintf(
			"https://b-%s.doppiocdn.com/hls/%s/master_%s.m3u8",
			svr, streamName, streamName,
		)
	} else {
		hlsURL = fmt.Sprintf(
			"https://edge-hls.doppiocdn.net/hls/%s/master/%s_auto.m3u8?playlistType=lowLatency",
			streamName, streamName,
		)
	}

	info.HLSSource = hlsURL
	return info, nil
}

// FetchLastBroadcast implements site.Site. Stripchat does not expose a
// last_broadcast timestamp in a usable form, so this returns 0.
func (s *StripchatSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	return 0, nil
}

// FetchProfile implements site.Site. Stripchat exposes no public biocontext-style
// profile endpoint, so this returns nil, nil and callers skip the profile DB write.
func (s *StripchatSite) FetchProfile(ctx context.Context, req *internal.Req, username string) (*database.ChannelProfile, error) {
	return nil, nil
}

func (s *StripchatSite) GetRoomStatus(ctx context.Context, req *internal.Req, username string) (string, error) {
	state, httpStatus, err := fetchStripchatPage(ctx, req, username)
	if err != nil {
		return "", fmt.Errorf("stripchat: get room status: %w", err)
	}

	if httpStatus == 404 {
		return StatusOffline, nil
	}

	m := state.ViewCamBase.Model
	status := m.Status
	if status == "off" || status == "" {
		if !m.IsOnline && !m.IsLive {
			return StatusOffline, nil
		}
		return "unknown", nil
	}
	return status, nil
}

var _ Site = (*StripchatSite)(nil)
