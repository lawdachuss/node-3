package server

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Config *entity.Config
var ConfigMu sync.RWMutex
var StartTime = time.Now()

type persistedSettings struct {
	Cookies         string `json:"cookies"`
	SessionID       string `json:"sessionid,omitempty"`
	Csrftoken       string `json:"csrftoken,omitempty"`
	CfClearance     string `json:"cf_clearance,omitempty"`
	UserAgent       string `json:"user_agent"`
	VoeSXAPIKey     string `json:"voesx_api_key,omitempty"`
	StreamtapeLogin string `json:"streamtape_login,omitempty"`
	StreamtapeKey   string `json:"streamtape_key,omitempty"`
	MixdropEmail    string `json:"mixdrop_email,omitempty"`
	MixdropToken    string `json:"mixdrop_token,omitempty"`
	VidaraKey       string `json:"vidara_key,omitempty"`
	StripchatPDKey  string `json:"stripchat_pdkey,omitempty"`
	AffiliateWM     string `json:"affiliate_wm,omitempty"`
}

// SaveSettings persists the shared upload credentials and the effective
// AFFILIATE_WM to the global "dvr_settings" key, and each node's IP-bound
// cookies + user-agent under its per-node key (dvr_settings:<node_id>).
//
// Called at every startup after LoadSettings, so whichever value was effective
// (env/GitHub secret, or the already-stored central one) is written back — the
// global key is the single source of truth that all nodes converge on.
func SaveSettings() error {
	ConfigMu.RLock()
	cookies := persistedSettings{
		// Sanitize on save so previously-quoted browser-pasted values never
		// get re-persisted (keeps the stored blob well-formed for all nodes).
		Cookies:     entity.SanitizeCookieString(Config.Cookies),
		SessionID:   entity.SanitizeCookieValue(Config.SessionID),
		Csrftoken:   entity.SanitizeCookieValue(Config.Csrftoken),
		CfClearance: entity.SanitizeCookieValue(Config.CfClearance),
		UserAgent:   Config.UserAgent,
	}
	creds := persistedSettings{
		VoeSXAPIKey:     validPersistedValue(Config.VoeSXAPIKey),
		StreamtapeLogin: validPersistedValue(Config.StreamtapeLogin),
		StreamtapeKey:   validPersistedValue(Config.StreamtapeKey),
		MixdropEmail:    validPersistedValue(Config.MixdropEmail),
		MixdropToken:    validPersistedValue(Config.MixdropToken),
		VidaraKey:       validPersistedValue(Config.VidaraKey),
		StripchatPDKey:  validPersistedValue(Config.StripchatPDKey),
		AffiliateWM:     validPersistedValue(Config.AffiliateWM),
	}
	ConfigMu.RUnlock()

	if err := persistSettings(CookieSettingsKey(), &cookies); err != nil {
		return fmt.Errorf("save per-node cookies (%s): %w", CookieSettingsKey(), err)
	}
	return persistSettings("dvr_settings", &creds)
}

// persistSettings marshals a settings snapshot and writes it under the given
// app_settings key.
func persistSettings(key string, s *persistedSettings) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	return SaveSettingsToDBForKey(key, b)
}

// LoadSettings reads each node's persisted cookies from its per-node key
// (dvr_settings:<node_id>) and the shared upload credentials from the global
// "dvr_settings" key. For backwards compatibility, if the per-node key has no
// cookie blob yet, it falls back to the legacy cookie fields stored in the
// global key (so a node that already ran never finds itself with no cookies).
//
// Upload credentials: a non-empty local value (.env / CLI flag / GitHub
// secret) always wins over the central blob — the central value only seeds
// fields the local config left empty (see applyUploadCredential). This is the
// secret-rotation path: update the GitHub secret or .env, restart, and the
// next SaveSettings converges the whole fleet onto the new key.
func LoadSettings() error {
	// 1) Credentials are always read from the global key.
	if b := LoadSettingsFromDB(); b != nil {
		var s persistedSettings
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("unmarshal global settings: %w", err)
		}
		ConfigMu.Lock()
		// Upload credentials: local (env/flag/GitHub-secret) value wins; the
		// central blob only seeds fields the local config left empty.
		Config.VoeSXAPIKey = applyUploadCredential(Config.VoeSXAPIKey, s.VoeSXAPIKey, "voesx_api_key")
		Config.StreamtapeLogin = applyUploadCredential(Config.StreamtapeLogin, s.StreamtapeLogin, "streamtape_login")
		Config.StreamtapeKey = applyUploadCredential(Config.StreamtapeKey, s.StreamtapeKey, "streamtape_key")
		Config.MixdropEmail = applyUploadCredential(Config.MixdropEmail, s.MixdropEmail, "mixdrop_email")
		Config.MixdropToken = applyUploadCredential(Config.MixdropToken, s.MixdropToken, "mixdrop_token")
		Config.VidaraKey = applyUploadCredential(Config.VidaraKey, s.VidaraKey, "vidara_key")
		if v := validPersistedValue(s.StripchatPDKey); v != "" {
			Config.StripchatPDKey = v
		}
		// Central value wins over .env/GitHub-secret AFFILIATE_WM: SaveSettings
		// persists whatever value was effective at startup, so a node can seed
		// Supabase once and every other node adopts it (an empty stored value
		// is skipped, so it never wipes a locally-set one).
		if v := validPersistedValue(s.AffiliateWM); v != "" {
			Config.AffiliateWM = v
		}
		ConfigMu.Unlock()
	}

	// 2) node-scoped cookies: per-node key first, legacy global cookie fallback.
	blob := LoadSettingsFromDBKey(CookieSettingsKey())
	legacyFallback := false
	if blob == nil {
		blob = LoadSettingsFromDB() // legacy: cookies once lived in dvr_settings
		legacyFallback = true
	}
	if blob == nil {
		return nil
	}

	var s persistedSettings
	if err := json.Unmarshal(blob, &s); err != nil {
		return fmt.Errorf("unmarshal cookie settings: %w", err)
	}
	if legacyFallback {
		// Only take the cookie fields from a legacy global blob, never the
		// credentials (already read above).
		s.VoeSXAPIKey = ""
		s.StreamtapeLogin = ""
		s.StreamtapeKey = ""
		s.MixdropEmail = ""
		s.MixdropToken = ""
		s.VidaraKey = ""
		s.StripchatPDKey = ""
		s.AffiliateWM = ""
	}

	ConfigMu.Lock()
	if s.Cookies != "" {
		// Strip quotes/invalid bytes (browser-pasted cookie strings often
		// wrap values in quotes, which Cloudflare rejects in the header).
		Config.Cookies = entity.SanitizeCookieString(s.Cookies)
	}
	if s.SessionID != "" {
		Config.SessionID = entity.SanitizeCookieValue(s.SessionID)
	}
	if s.Csrftoken != "" {
		Config.Csrftoken = entity.SanitizeCookieValue(s.Csrftoken)
	}
	if s.CfClearance != "" {
		Config.CfClearance = entity.SanitizeCookieValue(s.CfClearance)
	}
	if s.UserAgent != "" {
		Config.UserAgent = s.UserAgent
	}

	// Parse Config.Cookies back into individual fields if they are empty.
	if Config.Cookies != "" {
		if Config.CfClearance == "" {
			Config.CfClearance = extractCookie(Config.Cookies, "cf_clearance")
		}
		if Config.SessionID == "" {
			Config.SessionID = extractCookie(Config.Cookies, "sessionid")
		}
		if Config.Csrftoken == "" {
			Config.Csrftoken = extractCookie(Config.Cookies, "csrftoken")
		}
	}
	ConfigMu.Unlock()

	return nil
}

func extractCookie(cookieStr, name string) string {
	for _, pair := range strings.Split(cookieStr, ";") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == name {
			return entity.SanitizeCookieValue(strings.TrimSpace(parts[1]))
		}
	}
	return ""
}

// applyUploadCredential resolves one upload credential at settings-load time.
//
// Precedence (fleet secret-rotation fix): a non-empty LOCAL value (from .env,
// CLI flag, or GitHub secret) always wins over the persisted central value;
// the central value only seeds fields the local config left empty. Previously
// the central blob won unconditionally, so rotating a key in GitHub
// secrets/.env never reached the fleet: every node's LoadSettings overwrote
// the fresh env value with the stale central one and SaveSettings immediately
// re-persisted it — uploads kept failing with 401/403 auth errors (e.g.
// Streamtape "Authentication failed") until the blob was fixed by hand.
func applyUploadCredential(local, central, name string) string {
	if v := validPersistedValue(local); v != "" {
		if prev := validPersistedValue(central); prev != "" && prev != v {
			log.Printf("[startup] upload credential %s: local (env/flag) value overrides the central Supabase value — next save re-persists it fleet-wide", name)
		}
		return v
	}
	return validPersistedValue(central)
}

// validPersistedValue returns s trimmed, or "" when s is empty, whitespace-only,
// or the placeholder dash ("-"). The web UI and settings blob have previously
// stored "-" for unset API keys; treating it as a real value made startup
// (LoadSettings) overwrite working .env credentials and all uploads failed with
// 401 "invalid api_key". Callers must skip the value when this returns "".
func validPersistedValue(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return ""
	}
	return s
}

// Session-deadline stagger. Nodes that start at the same time with the same
// session duration end up with session_deadlines only minutes apart, so every
// node enters its 15-minute pre-deadline migration window at roughly the same
// moment. With the whole fleet imminent at once, the deadline-migration cycle
// has NO healthy non-imminent node to move channels to, so channels flap
// between imminent nodes — every hop force-splits the in-progress recording
// (the fleet-wide "channel stopped (handoff)" pattern). The stagger gives each
// node a deterministic, node-ID-derived offset so deadlines are spread across
// the fleet and migration always has healthy targets.
const (
	// ciSessionStaggerSpread bounds the deadline offset between CI runners.
	// On CI the offset is SUBTRACTED from the session duration (see
	// staggerSessionDuration): deadlines spread across the fleet while the
	// shortest-drain node keeps the full 360m-kill headroom (25m for the
	// 335m fallback, 40m for a 320m central value).  "Imminent" is monotone
	// — a node stays in the migration set until its deadline passes — so the
	// win is not fewer nodes inside the window but ROLLOVER: with a 30-minute
	// spread the 18 runners' teardown/re-registration cycles roll over 20+
	// minutes instead of arriving as one synchronized wave.  The old
	// 10-minute ADD spread fit every deadline inside the 15-minute window at
	// the same minute, so the whole fleet entered migration together, every
	// reborn node became imminent again within minutes, and deadline
	// migration never had a healthy non-imminent target to hand channels to
	// (the fleet-wide "channel stopped (handoff)" wave + disk wipes).
	ciSessionStaggerSpread = 30 * time.Minute
	// permanentSessionStaggerSpread bounds the offset on permanent nodes (no
	// hard kill). 240 minutes spreads the 15-minute migration windows of a
	// ~16-node fleet so they essentially never overlap, keeping healthy
	// migration targets available at all times.
	permanentSessionStaggerSpread = 240 * time.Minute
)

// staggerNodeKey returns the identifier that drives the per-node deadline
// offset.  NODE_ID is the fleet contract; when it is unset the hostname is
// preferred over detectNodeID() because on CI every runner shares the same
// GITHUB_REPOSITORY — hashing that would give every runner of a repo the SAME
// offset, silently reintroducing the synchronized-deadline wave the stagger
// exists to break.  GitHub runner hostnames are unique per runner.  The
// timestamp fallback in detectNodeID() is avoided here too: an offset that
// changes every restart would destabilize the persisted session deadline.
func staggerNodeKey() string {
	if id := strings.TrimSpace(os.Getenv("NODE_ID")); id != "" && id != "-" {
		return id
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return detectNodeID()
}

// staggerSessionDuration returns d plus or minus a deterministic per-node
// offset derived from the node id (fnv hash mod the spread). The offset is
// stable across restarts and session cycles, so the fleet's relative deadline
// spacing never collapses back to synchronized.
//
// On permanent nodes the offset is ADDED: there is no hard kill, so extra
// session length costs nothing.  On CI runners it is SUBTRACTED: runners are
// hard-killed 6h after start regardless of the session duration, so adding an
// offset would silently eat the graceful-drain window before the kill.
// Subtracting spreads the deadlines across [d-spread, d] while the worst-case
// headroom before the kill stays 360m-d — the longest-recording node also has
// the most drain time.  The result is floored at d/2 so a tiny configured
// duration can never go negative.
func staggerSessionDuration(d time.Duration) time.Duration {
	spread := permanentSessionStaggerSpread
	ci := os.Getenv("GITHUB_RUN_ID") != ""
	if ci {
		spread = ciSessionStaggerSpread
	}
	h := fnv.New32a()
	h.Write([]byte(staggerNodeKey()))
	offset := time.Duration(h.Sum32()%uint32(spread/time.Minute)) * time.Minute
	if ci {
		if offset > d/2 {
			offset = d / 2
		}
		return d - offset
	}
	return d + offset
}

// ApplyCentralSessionDuration reconciles the session length so every node
// follows the same value. Precedence:
//  1. A locally-set SESSION_DURATION (env/flag) always wins — per-node override.
//  2. Otherwise the central value stored in Supabase (app_settings
//     "session_duration") is adopted, so nodes need no env at all.
//  3. If neither is set, a CI runner (GITHUB_RUN_ID) falls back to a ~5m-35m
//     buffer (5h35m) before the workflow's hard kill so it still stops,
//     processes and uploads instead of being force-flushed mid-recording.
//
// Finally, the resolved duration is staggered per node (see
// staggerSessionDuration). Both the manager's session stop and the
// coordinator's persisted session_deadline read SessionDurationParsed, so a
// single stagger here keeps them in agreement.
func ApplyCentralSessionDuration() {
	ConfigMu.Lock()
	defer ConfigMu.Unlock()

	// Local env value already parsed — keep it (per-node override).
	if Config.SessionDurationParsed <= 0 && Config.SessionDuration == "" {
		if central := LoadSessionDurationFromDB(); central != "" {
			if parsed, err := time.ParseDuration(central); err == nil && parsed > 0 {
				Config.SessionDuration = central
				Config.SessionDurationParsed = parsed
			}
		}

		if Config.SessionDurationParsed <= 0 && os.Getenv("GITHUB_RUN_ID") != "" {
			Config.SessionDuration = "335m"
			Config.SessionDurationParsed = 335 * time.Minute
		}
	}

	// Stagger whatever duration was resolved (env, central, or CI fallback)
	// so fleet deadlines desynchronize instead of collapsing into one window.
	if Config.SessionDurationParsed > 0 {
		staggered := staggerSessionDuration(Config.SessionDurationParsed)
		if staggered != Config.SessionDurationParsed {
			log.Printf("[startup] session duration staggered for node %q: %s -> %s (offset %s)",
				detectNodeID(), Config.SessionDurationParsed, staggered, staggered-Config.SessionDurationParsed)
			Config.SessionDurationParsed = staggered
			Config.SessionDuration = staggered.String()
		}
	}
}

// UpdateUploaderCredentials updates upload service credentials and protects concurrent access with a mutex.
func UpdateUploaderCredentials(voeSXAPIKey, streamtapeLogin, streamtapeKey, mixdropEmail, mixdropToken, vidaraKey string) {
	// Ignore placeholder dash values so the web UI can't wipe real .env keys.
	voeSXAPIKey = validPersistedValue(voeSXAPIKey)
	streamtapeLogin = validPersistedValue(streamtapeLogin)
	streamtapeKey = validPersistedValue(streamtapeKey)
	mixdropEmail = validPersistedValue(mixdropEmail)
	mixdropToken = validPersistedValue(mixdropToken)
	vidaraKey = validPersistedValue(vidaraKey)

	ConfigMu.Lock()
	if voeSXAPIKey != "" {
		Config.VoeSXAPIKey = voeSXAPIKey
	}
	if streamtapeLogin != "" {
		Config.StreamtapeLogin = streamtapeLogin
	}
	if streamtapeKey != "" {
		Config.StreamtapeKey = streamtapeKey
	}
	if mixdropEmail != "" {
		Config.MixdropEmail = mixdropEmail
	}
	if mixdropToken != "" {
		Config.MixdropToken = mixdropToken
	}
	if vidaraKey != "" {
		Config.VidaraKey = vidaraKey
	}
	ConfigMu.Unlock()
}
