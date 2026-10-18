// Package uploadkeys is the fleet-wide upload-credential manager: it keeps a
// central set of upload-host API keys in Supabase (app_settings
// "upload_host_keys"), applies it over every node's env-derived config, and
// tracks per-key health so dead keys surface on the admin page in real time.
//
// Why central: upload credentials are shared by every node (see the
// upload_host_backoffs comment in database/supabase.go) — when a key dies on
// node-7 it is dead for the whole fleet, so rotating it once from ANY node's
// admin page must reach every other node without editing .env / GitHub
// secrets on 18 runners.
package uploadkeys

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// app_settings keys for the central blobs.
const (
	keysKey   = "upload_host_keys"
	statusKey = "upload_host_keys_status"
)

// Status values a key can carry.
const (
	StatusHealthy  = "healthy"
	StatusDead     = "dead"
	StatusQuota    = "quota"   // valid key, account capped (rotates away but not dead)
	StatusUnknown  = "unknown" // probe could not decide (host down, WAF, timeout)
	StatusUntested = "untested"
)

// Cred is one credential entry for an upload host. Which fields are required
// depends on the host: keyed hosts use Key; Streamtape uses Login+Key; Mixdrop
// uses Email+Token. One entry = one account.
type Cred struct {
	Key   string `json:"key,omitempty"`
	Login string `json:"login,omitempty"`
	Email string `json:"email,omitempty"`
	Token string `json:"token,omitempty"`
}

// Set is the central credential store: every host's ordered credential list.
type Set struct {
	// Managed marks that the store has been taken ownership of (by the first
	// node's env seeding or by any admin edit). Once managed, a host with an
	// EMPTY credential list is actively cleared on every node's config and env
	// — without this flag a node's env-derived fallback would silently
	// resurrect keys the operator deliberately deleted. Unmanaged (blob absent
	// or Supabase unreachable) sets only ever overlay non-empty lists, so a
	// transient DB failure can never wipe live credentials.
	Managed    bool   `json:"managed,omitempty"`
	VoeSX      []Cred `json:"voesx,omitempty"`
	Streamtape []Cred `json:"streamtape,omitempty"`
	Mixdrop    []Cred `json:"mixdrop,omitempty"`
	Vidara     []Cred `json:"vidara,omitempty"`
	VidMoly    []Cred `json:"vidmoly,omitempty"`
}

// KeyStatus is one credential's health as last determined by the prober or
// reported live by an upload rotation.
type KeyStatus struct {
	Host        string    `json:"host"`
	Fingerprint string    `json:"fingerprint"`
	Status      string    `json:"status"`
	LastChecked time.Time `json:"last_checked"`
	Message     string    `json:"message,omitempty"`
}

// KeyView is one key row as rendered on the admin page / returned by the API.
type KeyView struct {
	Fingerprint string    `json:"fingerprint"`
	Display     string    `json:"display"`
	Status      string    `json:"status"`
	LastChecked time.Time `json:"last_checked"`
	Message     string    `json:"message,omitempty"`
}

// HostView is one upload host and its configured keys for the admin page.
type HostView struct {
	Host      string    `json:"host"`
	Label     string    `json:"label"`
	Probeable bool      `json:"probeable"`
	Fields    []string  `json:"fields"`
	Keys      []KeyView `json:"keys"`
}

// hostSpec describes one managed upload host: how it is probed and which
// input fields its credential consists of.
type hostSpec struct {
	name      string
	label     string
	probeable bool
	fields    []string
	get       func(*Set) []Cred
	set       func(*Set, []Cred)
}

var hostSpecs = []hostSpec{
	{
		name: "VoeSX", label: "VOE.sx", probeable: true, fields: []string{"key"},
		get: func(s *Set) []Cred { return s.VoeSX },
		set: func(s *Set, v []Cred) { s.VoeSX = v },
	},
	{
		name: "Streamtape", label: "Streamtape", probeable: true, fields: []string{"login", "key"},
		get: func(s *Set) []Cred { return s.Streamtape },
		set: func(s *Set, v []Cred) { s.Streamtape = v },
	},
	{
		// Mixdrop has no cheap account-verify endpoint, so it is managed here
		// but judged only from live upload outcomes (passive). Only the FIRST
		// listed account is applied — the uploader takes a single email/token.
		name: "Mixdrop", label: "Mixdrop", probeable: false, fields: []string{"email", "token"},
		get: func(s *Set) []Cred { return s.Mixdrop },
		set: func(s *Set, v []Cred) { s.Mixdrop = v },
	},
	{
		name: "Vidara", label: "Vidara", probeable: true, fields: []string{"key"},
		get: func(s *Set) []Cred { return s.Vidara },
		set: func(s *Set, v []Cred) { s.Vidara = v },
	},
	{
		name: "VidMoly", label: "VidMoly", probeable: true, fields: []string{"key"},
		get: func(s *Set) []Cred { return s.VidMoly },
		set: func(s *Set, v []Cred) { s.VidMoly = v },
	},
}

func specByName(name string) (hostSpec, bool) {
	for _, h := range hostSpecs {
		if strings.EqualFold(h.name, name) {
			return h, true
		}
	}
	return hostSpec{}, false
}

// Storage hooks are package vars so tests can stub the Supabase round-trip.
var (
	loadBlob = server.LoadSettingsFromDBKey
	saveBlob = server.SaveSettingsToDBForKey
)

var (
	mu       sync.RWMutex
	current  *Set                 // last loaded central set (nil until first load)
	statuses map[string]KeyStatus // "host:fingerprint" → status (merged fleet view)
	started  bool

	// lastLoadAttempt throttles failed ensureLoaded retries (guarded by mu).
	lastLoadAttempt time.Time
)

// Fingerprint returns the stable identifier for a credential — a short hash
// of the secret, identical on every node (so statuses written by one node are
// readable by all) without exposing the secret itself.
func Fingerprint(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])[:12]
}

// display masks a secret down to its last 4 characters for the admin page.
func display(secret string) string {
	if len(secret) <= 4 {
		return "••••"
	}
	return "••••" + secret[len(secret)-4:]
}

// secretOf returns the identifying secret a cred's fingerprint is derived
// from. Pair hosts combine both halves so changing either half is a new key.
func secretOf(h hostSpec, c Cred) string {
	switch h.name {
	case "Streamtape":
		return c.Login + ":" + c.Key
	case "Mixdrop":
		return c.Email + ":" + c.Token
	default:
		return c.Key
	}
}

// statusKeyOf builds the statuses-map key for one credential. The fingerprint
// alone is NOT unique across hosts (the same API key string could be configured
// for two different services), so statuses are namespaced per host — otherwise
// two hosts sharing a key would overwrite each other's health verdicts.
func statusKeyOf(host, fingerprint string) string {
	return host + ":" + fingerprint
}

// Load reads the central credential set from Supabase.
func Load() (*Set, error) {
	blob := loadBlob(keysKey)
	if len(blob) == 0 {
		return &Set{}, nil
	}
	var s Set
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, fmt.Errorf("decode %s: %w", keysKey, err)
	}
	return &s, nil
}

// Save writes the central credential set to Supabase. Any set we persist is by
// definition operator-owned, so it is stamped Managed: from then on empty host
// lists mean "removed on purpose" and are cleared on every node.
func Save(s *Set) error {
	s.Managed = true
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return saveBlob(keysKey, data)
}

func (s *Set) nonEmpty() bool {
	for _, h := range hostSpecs {
		if len(h.get(s)) > 0 {
			return true
		}
	}
	return false
}

func cloneSet(s *Set) *Set {
	if s == nil {
		return nil
	}
	cp := *s
	cp.VoeSX = append([]Cred(nil), s.VoeSX...)
	cp.Streamtape = append([]Cred(nil), s.Streamtape...)
	cp.Mixdrop = append([]Cred(nil), s.Mixdrop...)
	cp.Vidara = append([]Cred(nil), s.Vidara...)
	cp.VidMoly = append([]Cred(nil), s.VidMoly...)
	return &cp
}

// applyToConfig overlays the central set onto the live config AND the process
// environment. The env write matters: the uploaders' key rings prefer their
// env var over the config value passed in (buildRingFromEnv), so on nodes
// seeded from GitHub secrets the central value would otherwise never take
// effect. Mutating this process's env is contained to this process and makes
// the central set authoritative everywhere.
func applyToConfig(s *Set) {
	if s == nil || server.Config == nil {
		return
	}
	joinKeys := func(creds []Cred) string {
		parts := make([]string, 0, len(creds))
		for _, c := range creds {
			if c.Key != "" {
				parts = append(parts, c.Key)
			}
		}
		return strings.Join(parts, ",")
	}

	server.ConfigMu.Lock()
	// Overlay non-empty host lists. A Managed set additionally CLEARS hosts it
	// carries no keys for: the operator deleted the last key of that host, and
	// the node's env-derived fallback (GitHub secrets) must not keep uploading
	// with the dead credential. Unmanaged sets never clear (a missing/failed
	// central read is indistinguishable from "not configured yet").
	overlay := func(dst *string, envVar, v string) {
		if v != "" {
			*dst = v
			os.Setenv(envVar, v)
		} else if s.Managed {
			*dst = ""
			os.Setenv(envVar, "")
		}
	}
	overlay(&server.Config.VoeSXAPIKey, "VOESX_API_KEY", joinKeys(s.VoeSX))
	overlay(&server.Config.VidaraKey, "VIDARA_KEY", joinKeys(s.Vidara))
	overlay(&server.Config.VidMolyKey, "VIDMOLY_KEY", joinKeys(s.VidMoly))
	{
		logins := make([]string, 0, len(s.Streamtape))
		keys := make([]string, 0, len(s.Streamtape))
		creds := make([]string, 0, len(s.Streamtape))
		for _, c := range s.Streamtape {
			if c.Login == "" || c.Key == "" {
				continue
			}
			logins = append(logins, c.Login)
			keys = append(keys, c.Key)
			creds = append(creds, c.Login+":"+c.Key)
		}
		if len(creds) > 0 {
			joined := strings.Join(creds, ",")
			// STREAMTAPE_CREDS wins inside buildStreamtapeCredList, so keep it
			// in sync with the login/key lists for the same reason as above.
			server.Config.StreamtapeLogin = strings.Join(logins, ",")
			server.Config.StreamtapeKey = strings.Join(keys, ",")
			os.Setenv("STREAMTAPE_CREDS", joined)
		} else if s.Managed {
			server.Config.StreamtapeLogin = ""
			server.Config.StreamtapeKey = ""
			os.Setenv("STREAMTAPE_CREDS", "")
		}
	}
	if len(s.Mixdrop) > 0 && s.Mixdrop[0].Email != "" && s.Mixdrop[0].Token != "" {
		// The Mixdrop uploader takes a single email/token pair — first wins.
		server.Config.MixdropEmail = s.Mixdrop[0].Email
		server.Config.MixdropToken = s.Mixdrop[0].Token
	} else if s.Managed {
		server.Config.MixdropEmail = ""
		server.Config.MixdropToken = ""
	}
	server.ConfigMu.Unlock()
}

// loadRetryDelay throttles ensureLoaded's retries after a failed load: without
// it, every 45s admin poll would hang on a Supabase round-trip that is known
// to be failing.
const loadRetryDelay = 30 * time.Second

// ensureLoaded lazily loads the central set + statuses on first use (so the
// admin page works even if Start() has not run or Supabase was down at boot).
func ensureLoaded() {
	mu.RLock()
	loaded := current != nil
	ready := loaded || time.Since(lastLoadAttempt) >= loadRetryDelay
	mu.RUnlock()
	if !ready {
		return
	}
	mu.Lock()
	lastLoadAttempt = time.Now()
	mu.Unlock()
	if s, err := Load(); err == nil && s != nil {
		mu.Lock()
		if current == nil {
			current = s
		}
		mu.Unlock()
	}
	if st, err := loadStatuses(); err == nil {
		mergeStatuses(st)
	}
}

func loadStatuses() ([]KeyStatus, error) {
	blob := loadBlob(statusKey)
	if len(blob) == 0 {
		return nil, nil
	}
	var rows []KeyStatus
	if err := json.Unmarshal(blob, &rows); err != nil {
		return nil, fmt.Errorf("decode %s: %w", statusKey, err)
	}
	return rows, nil
}

// mergeStatuses folds persisted rows into the in-memory map; the newer
// LastChecked always wins so a stale central read cannot clobber a fresh
// local result.
func mergeStatuses(rows []KeyStatus) {
	if len(rows) == 0 {
		return
	}
	mu.Lock()
	if statuses == nil {
		statuses = map[string]KeyStatus{}
	}
	for _, r := range rows {
		if r.Fingerprint == "" {
			continue
		}
		k := statusKeyOf(r.Host, r.Fingerprint)
		if old, ok := statuses[k]; ok && old.LastChecked.After(r.LastChecked) {
			continue
		}
		statuses[k] = r
	}
	mu.Unlock()
}

var persistMu sync.Mutex

// persistDelay is how long a status write waits for the current burst of
// updates to settle (several rotations in one upload must not produce several
// writes). A var so tests can push it past their runtime and keep a late
// background write from leaking into another test.
var persistDelay = 5 * time.Second

// persistScheduled makes persistStatusesAsync single-flight: a burst of key
// deaths (one upload rotating through a whole ring) collapses into ONE write
// per persistDelay window instead of one goroutine — and one Supabase write —
// per rotation.
var persistScheduled atomic.Bool

// persistStatusesAsync writes the merged status map to Supabase shortly after
// the current burst of updates settles.
func persistStatusesAsync() {
	if !persistScheduled.CompareAndSwap(false, true) {
		return // a flush is already scheduled; it will pick up this update too
	}
	go func() {
		time.Sleep(persistDelay)
		persistScheduled.Store(false) // updates during the write schedule a fresh flush
		persistStatuses()
	}()
}

func persistStatuses() {
	persistMu.Lock()
	defer persistMu.Unlock()
	mu.RLock()
	rows := make([]KeyStatus, 0, len(statuses))
	for _, r := range statuses {
		rows = append(rows, r)
	}
	mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Host != rows[j].Host {
			return rows[i].Host < rows[j].Host
		}
		return rows[i].Fingerprint < rows[j].Fingerprint
	})
	data, err := json.Marshal(rows)
	if err != nil {
		return
	}
	_ = saveBlob(statusKey, data)
}

// recordStatus upserts one key's health in the merged view and schedules a
// persist. Used by the prober and by the live key-death hook.
func recordStatus(host, fingerprint, status, message string) {
	if fingerprint == "" {
		return
	}
	mu.Lock()
	if statuses == nil {
		statuses = map[string]KeyStatus{}
	}
	statuses[statusKeyOf(host, fingerprint)] = KeyStatus{
		Host:        host,
		Fingerprint: fingerprint,
		Status:      status,
		LastChecked: time.Now(),
		Message:     message,
	}
	mu.Unlock()
	persistStatusesAsync()
}

// onKeyDeath is the uploader key-death hook: an upload just rotated away from
// a credential, which is the earliest possible signal that a key stopped
// working. Quota-flavoured reasons are recorded as "quota" (valid key, cap
// hit) rather than "dead" — the periodic probe will re-classify either way.
func onKeyDeath(host, secret, reason string) {
	status := StatusDead
	low := strings.ToLower(reason)
	if strings.Contains(low, "quota") || strings.Contains(low, "limit") {
		status = StatusQuota
	}
	recordStatus(host, Fingerprint(secret), status, reason)
}

// Snapshot returns every managed host with its configured keys and health —
// the payload for the admin page and GET /api/upload_keys.
func Snapshot() []HostView {
	ensureLoaded()
	mu.RLock()
	s := cloneSet(current)
	st := make(map[string]KeyStatus, len(statuses))
	for k, v := range statuses {
		st[k] = v
	}
	mu.RUnlock()

	views := make([]HostView, 0, len(hostSpecs))
	for _, h := range hostSpecs {
		hv := HostView{
			Host:      h.name,
			Label:     h.label,
			Probeable: h.probeable,
			Fields:    h.fields,
			Keys:      []KeyView{},
		}
		if s != nil {
			for _, c := range h.get(s) {
				secret := secretOf(h, c)
				fp := Fingerprint(secret)
				kv := KeyView{
					Fingerprint: fp,
					Display:     display(secret),
					Status:      StatusUntested,
				}
				if ks, ok := st[statusKeyOf(h.name, fp)]; ok {
					kv.Status = ks.Status
					kv.LastChecked = ks.LastChecked
					kv.Message = ks.Message
				}
				hv.Keys = append(hv.Keys, kv)
			}
		}
		views = append(views, hv)
	}
	return views
}

// Add appends one credential to a host's central list, applies it, and kicks
// off an immediate probe so its status appears without waiting for the sweep.
func Add(host string, cred Cred) error {
	h, ok := specByName(host)
	if !ok {
		return fmt.Errorf("unknown upload host %q", host)
	}
	for _, f := range h.fields {
		switch f {
		case "key":
			if strings.TrimSpace(cred.Key) == "" {
				return fmt.Errorf("%s: key is required", h.label)
			}
		case "login":
			if strings.TrimSpace(cred.Login) == "" {
				return fmt.Errorf("%s: login is required", h.label)
			}
		case "email":
			if strings.TrimSpace(cred.Email) == "" {
				return fmt.Errorf("%s: email is required", h.label)
			}
		case "token":
			if strings.TrimSpace(cred.Token) == "" {
				return fmt.Errorf("%s: token is required", h.label)
			}
		}
	}
	cred.Key = strings.TrimSpace(cred.Key)
	cred.Login = strings.TrimSpace(cred.Login)
	cred.Email = strings.TrimSpace(cred.Email)

	s, err := Load()
	if err != nil {
		return fmt.Errorf("load central keys: %w", err)
	}
	creds := h.get(s)
	fp := Fingerprint(secretOf(h, cred))
	for _, c := range creds {
		if Fingerprint(secretOf(h, c)) == fp {
			return fmt.Errorf("%s: this credential is already added", h.label)
		}
	}
	h.set(s, append(creds, cred))
	if err := Save(s); err != nil {
		return fmt.Errorf("save central keys: %w", err)
	}
	mu.Lock()
	current = s
	mu.Unlock()
	applyToConfig(s)

	// Probe the new key right away so a typo or dead key is visible in
	// seconds instead of at the next sweep.
	spawnProbe(h, cred)
	return nil
}

// spawnProbe runs an immediate post-add probe in the background. A var so
// tests can stub out the network call.
var spawnProbe = func(h hostSpec, c Cred) {
	go probeOne(h, c)
}

// Remove drops the credential with the given fingerprint from a host's
// central list and applies the remainder.
func Remove(host, fingerprint string) error {
	h, ok := specByName(host)
	if !ok {
		return fmt.Errorf("unknown upload host %q", host)
	}
	s, err := Load()
	if err != nil {
		return fmt.Errorf("load central keys: %w", err)
	}
	creds := h.get(s)
	kept := make([]Cred, 0, len(creds))
	found := false
	for _, c := range creds {
		if Fingerprint(secretOf(h, c)) == fingerprint {
			found = true
			continue
		}
		kept = append(kept, c)
	}
	if !found {
		return fmt.Errorf("%s: no credential matching fingerprint %s", h.label, fingerprint)
	}
	h.set(s, kept)
	if err := Save(s); err != nil {
		return fmt.Errorf("save central keys: %w", err)
	}
	mu.Lock()
	current = s
	delete(statuses, statusKeyOf(h.name, fingerprint))
	mu.Unlock()
	applyToConfig(s)
	persistStatusesAsync()
	return nil
}

// SyncLegacyForm folds the legacy Settings-dialog credential fields into the
// central store, so rotating a key the old way is never silently overwritten
// by the central set. Comma-separated lists become multiple credentials.
// Empty values are ignored (the form always submits every field).
func SyncLegacyForm(voeSXKey, stLogin, stKey, mdEmail, mdToken, vidaraKey, vidMolyKey string) {
	if strings.TrimSpace(voeSXKey+stLogin+stKey+mdEmail+mdToken+vidaraKey+vidMolyKey) == "" {
		return
	}
	s, err := Load()
	if err != nil {
		fmt.Printf("[uploadkeys] sync legacy form: load: %v\n", err)
		return
	}
	replace := func(h hostSpec, creds []Cred) {
		if len(creds) > 0 {
			h.set(s, creds)
		}
	}
	credsFromCSV := func(v string) []Cred {
		var out []Cred
		for _, k := range strings.Split(v, ",") {
			if k = strings.TrimSpace(k); k != "" {
				out = append(out, Cred{Key: k})
			}
		}
		return out
	}
	if h, ok := specByName("VoeSX"); ok {
		replace(h, credsFromCSV(voeSXKey))
	}
	if h, ok := specByName("Vidara"); ok {
		replace(h, credsFromCSV(vidaraKey))
	}
	if h, ok := specByName("VidMoly"); ok {
		replace(h, credsFromCSV(vidMolyKey))
	}
	if h, ok := specByName("Streamtape"); ok {
		logins := strings.Split(stLogin, ",")
		keys := strings.Split(stKey, ",")
		n := len(logins)
		if len(keys) < n {
			n = len(keys)
		}
		var creds []Cred
		for i := 0; i < n; i++ {
			lg := strings.TrimSpace(logins[i])
			ky := strings.TrimSpace(keys[i])
			if lg != "" && ky != "" {
				creds = append(creds, Cred{Login: lg, Key: ky})
			}
		}
		replace(h, creds)
	}
	if h, ok := specByName("Mixdrop"); ok {
		mdEmail = strings.TrimSpace(mdEmail)
		mdToken = strings.TrimSpace(mdToken)
		if mdEmail != "" && mdToken != "" {
			// The legacy dialog only ever displays the FIRST Mixdrop account
			// (the uploader takes a single email/token pair), so an unchanged
			// resubmission would silently truncate the operator's account pool
			// down to one. Only replace when the submitted pair actually
			// differs from the first stored account — i.e. an intentional
			// rotation.
			cur := h.get(s)
			if len(cur) == 0 || cur[0].Email != mdEmail || cur[0].Token != mdToken {
				replace(h, []Cred{{Email: mdEmail, Token: mdToken}})
			}
		}
	}
	if err := Save(s); err != nil {
		fmt.Printf("[uploadkeys] sync legacy form: save: %v\n", err)
		return
	}
	mu.Lock()
	current = s
	mu.Unlock()
	applyToConfig(s)
}
