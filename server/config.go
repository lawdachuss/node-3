package server

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/teacat/chaturbate-dvr/entity"
)

var Config *entity.Config
var configMu sync.RWMutex

type persistedSettings struct {
	Cookies   string `json:"cookies"`
	UserAgent string `json:"user_agent"`
	ByparrURL string `json:"byparr_url"`
}

// SaveSettings writes the runtime cookies and user-agent to Supabase.
func SaveSettings() error {
	configMu.RLock()
	s := persistedSettings{
		Cookies:   Config.Cookies,
		UserAgent: Config.UserAgent,
		ByparrURL: Config.ByparrURL,
	}
	configMu.RUnlock()

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

	configMu.Lock()
	if s.Cookies != "" {
		Config.Cookies = s.Cookies
	}
	if s.UserAgent != "" {
		Config.UserAgent = s.UserAgent
	}
	if s.ByparrURL != "" {
		Config.ByparrURL = s.ByparrURL
	}
	configMu.Unlock()

	return nil
}

// UpdateByparrCredentials safely updates cookies and user-agent
// with mutex protection for concurrent access.
func UpdateByparrCredentials(cookies, userAgent string) {
	configMu.Lock()
	if cookies != "" {
		Config.Cookies = cookies
	}
	if userAgent != "" {
		Config.UserAgent = userAgent
	}
	configMu.Unlock()
}
