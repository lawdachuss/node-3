package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Config *entity.Config
var ConfigMu sync.RWMutex
var StartTime = time.Now()

type persistedSettings struct {
	Cookies          string `json:"cookies"`
	SessionID        string `json:"sessionid,omitempty"`
	Csrftoken        string `json:"csrftoken,omitempty"`
	CfClearance      string `json:"cf_clearance,omitempty"`
	UserAgent        string `json:"user_agent"`
	VoeSXAPIKey      string `json:"voesx_api_key,omitempty"`
	StreamtapeLogin  string `json:"streamtape_login,omitempty"`
	StreamtapeKey    string `json:"streamtape_key,omitempty"`
	MixdropEmail     string `json:"mixdrop_email,omitempty"`
	MixdropToken     string `json:"mixdrop_token,omitempty"`
	SeekStreamingKey string `json:"seekstreaming_key,omitempty"`
	VidHideAPIKey    string `json:"vidhide_api_key,omitempty"`
	StreamWishAPIKey string `json:"streamwish_api_key,omitempty"`
	StripchatPDKey   string `json:"stripchat_pdkey,omitempty"`
}

// SaveSettings writes the runtime cookies and user-agent to Supabase.
func SaveSettings() error {
	ConfigMu.RLock()
	s := persistedSettings{
		Cookies:          Config.Cookies,
		SessionID:        Config.SessionID,
		Csrftoken:        Config.Csrftoken,
		CfClearance:      Config.CfClearance,
		UserAgent:        Config.UserAgent,
		VoeSXAPIKey:      Config.VoeSXAPIKey,
		StreamtapeLogin:  Config.StreamtapeLogin,
		StreamtapeKey:    Config.StreamtapeKey,
		MixdropEmail:     Config.MixdropEmail,
		MixdropToken:     Config.MixdropToken,
		SeekStreamingKey: Config.SeekStreamingKey,
		VidHideAPIKey:    Config.VidHideAPIKey,
		StreamWishAPIKey: Config.StreamWishAPIKey,
		StripchatPDKey:   Config.StripchatPDKey,
	}
	ConfigMu.RUnlock()

	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}

	if err := SaveSettingsToDB(b); err != nil {
		return fmt.Errorf("save settings to Supabase: %w", err)
	}
	return nil
}

// LoadSettings reads persisted cookies and user-agent from Supabase.
func LoadSettings() error {
	b := LoadSettingsFromDB()
	if b == nil {
		return nil
	}

	var s persistedSettings
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("unmarshal settings: %w", err)
	}

	ConfigMu.Lock()
	if s.Cookies != "" {
		Config.Cookies = s.Cookies
	}
	if s.SessionID != "" {
		Config.SessionID = s.SessionID
	}
	if s.Csrftoken != "" {
		Config.Csrftoken = s.Csrftoken
	}
	if s.CfClearance != "" {
		Config.CfClearance = s.CfClearance
	}
	if s.UserAgent != "" {
		Config.UserAgent = s.UserAgent
	}
	if s.VoeSXAPIKey != "" {
		Config.VoeSXAPIKey = s.VoeSXAPIKey
	}
	if s.StreamtapeLogin != "" {
		Config.StreamtapeLogin = s.StreamtapeLogin
	}
	if s.StreamtapeKey != "" {
		Config.StreamtapeKey = s.StreamtapeKey
	}
	if s.MixdropEmail != "" {
		Config.MixdropEmail = s.MixdropEmail
	}
	if s.MixdropToken != "" {
		Config.MixdropToken = s.MixdropToken
	}
	if s.SeekStreamingKey != "" {
		Config.SeekStreamingKey = s.SeekStreamingKey
	}
	if s.VidHideAPIKey != "" {
		Config.VidHideAPIKey = s.VidHideAPIKey
	}
	if s.StreamWishAPIKey != "" {
		Config.StreamWishAPIKey = s.StreamWishAPIKey
	}
	if s.StripchatPDKey != "" {
		Config.StripchatPDKey = s.StripchatPDKey
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
			return strings.TrimSpace(parts[1])
		}
	}
	return ""
}

// UpdateUploaderCredentials updates upload service credentials and protects concurrent access with a mutex.
func UpdateUploaderCredentials(voeSXAPIKey, streamtapeLogin, streamtapeKey, mixdropEmail, mixdropToken, seekStreamingKey, vidHideAPIKey, streamWishAPIKey string) {
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
	if seekStreamingKey != "" {
		Config.SeekStreamingKey = seekStreamingKey
	}
	if vidHideAPIKey != "" {
		Config.VidHideAPIKey = vidHideAPIKey
	}
	if streamWishAPIKey != "" {
		Config.StreamWishAPIKey = streamWishAPIKey
	}
	ConfigMu.Unlock()
}
