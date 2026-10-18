package uploadkeys

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	// probeInterval is how old a key's status may get before the sweep
	// re-probes it. VidMoly probes count against its ~50 API calls/day cap, so
	// this must stay comfortably above hourly — and the fleet-wide lease below
	// ensures only ONE node probes per interval, not all 18.
	probeInterval = 2 * time.Hour
	// refreshInterval is how often each node re-reads the central key set and
	// statuses, which is also how fast an edit made on one node's admin page
	// reaches every other node.
	refreshInterval = 20 * time.Minute
	// leaseKey holds the sweep lease so concurrent nodes don't all probe
	// stale keys at once.
	leaseKey = "upload_host_keys_probe_lease"
	// leaseDuration is how long a claimed sweep lease blocks other nodes.
	leaseDuration = 15 * time.Minute
)

type probeLease struct {
	Node  string    `json:"node"`
	Until time.Time `json:"until"`
}

// Start applies the central credential set (seeding it from this node's env
// on first use) and launches the background prober. Idempotent; safe to call
// from main on every node.
func Start() {
	mu.Lock()
	if started {
		mu.Unlock()
		return
	}
	started = true
	mu.Unlock()

	ensureLoaded()
	seedFromConfig()
	uploader.SetKeyDeathHook(onKeyDeath)
	fmt.Println("[uploadkeys] central upload-key store active (prober running, statuses on /admin)")

	go func() {
		// Stagger the first sweep so a fleet-wide restart doesn't probe in one
		// synchronized wave; the staleness check + lease make races harmless.
		time.Sleep(3*time.Minute + time.Duration(rand.Intn(120))*time.Second)
		for {
			refresh()
			probeStale()
			time.Sleep(refreshInterval)
		}
	}()
}

// seedFromConfig writes this node's env-derived credentials into the central
// store when it is completely empty, mirroring the AFFILIATE_WM startup
// seeding: the first node to boot makes Supabase the single source of truth,
// so every existing key becomes visible and manageable on the admin page
// without anyone re-typing 18 .env files.
func seedFromConfig() {
	s, err := Load()
	// A Managed store is the operator's — even when every list in it is empty
	// (they deliberately deleted all keys), env must not resurrect them.
	if err != nil || s == nil || s.Managed || s.nonEmpty() {
		return
	}
	csvCreds := func(v string) []Cred {
		var out []Cred
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, Cred{Key: part})
			}
		}
		return out
	}
	server.ConfigMu.RLock()
	if server.Config == nil {
		server.ConfigMu.RUnlock()
		return
	}
	seed := &Set{
		VoeSX:   csvCreds(server.Config.VoeSXAPIKey),
		Vidara:  csvCreds(server.Config.VidaraKey),
		VidMoly: csvCreds(server.Config.VidMolyKey),
	}
	logins := strings.Split(server.Config.StreamtapeLogin, ",")
	keys := strings.Split(server.Config.StreamtapeKey, ",")
	n := len(logins)
	if len(keys) < n {
		n = len(keys)
	}
	for i := 0; i < n; i++ {
		lg := strings.TrimSpace(logins[i])
		ky := strings.TrimSpace(keys[i])
		if lg != "" && ky != "" {
			seed.Streamtape = append(seed.Streamtape, Cred{Login: lg, Key: ky})
		}
	}
	if strings.TrimSpace(server.Config.MixdropEmail) != "" && strings.TrimSpace(server.Config.MixdropToken) != "" {
		seed.Mixdrop = []Cred{{Email: strings.TrimSpace(server.Config.MixdropEmail), Token: strings.TrimSpace(server.Config.MixdropToken)}}
	}
	server.ConfigMu.RUnlock()

	if !seed.nonEmpty() {
		return
	}
	if err := Save(seed); err != nil {
		fmt.Printf("[uploadkeys] seed central keys: %v\n", err)
		return
	}
	mu.Lock()
	current = seed
	mu.Unlock()
	fmt.Printf("[uploadkeys] seeded central key store from env/config (%d host(s))\n", countHosts(seed))
}

func countHosts(s *Set) int {
	n := 0
	for _, h := range hostSpecs {
		if len(h.get(s)) > 0 {
			n++
		}
	}
	return n
}

// refresh re-reads the central set + statuses and re-applies the set when it
// changed — this is how a key rotated on another node's admin page reaches
// this node's uploads.
func refresh() {
	s, err := Load()
	if err != nil || s == nil {
		return
	}
	mu.Lock()
	changed := !reflect.DeepEqual(current, s)
	current = s
	mu.Unlock()
	if changed {
		applyToConfig(s)
		fmt.Printf("[uploadkeys] central key set changed — applied to config (%d host(s))\n", countHosts(s))
	}
	if rows, err := loadStatuses(); err == nil {
		mergeStatuses(rows)
	}
}

// probeStale probes every configured credential whose status is missing or
// older than probeInterval, behind a fleet-wide lease so VidMoly's daily API
// cap is not burned by every node probing independently.
func probeStale() {
	ensureLoaded()
	mu.RLock()
	s := cloneSet(current)
	st := make(map[string]KeyStatus, len(statuses))
	for k, v := range statuses {
		st[k] = v
	}
	mu.RUnlock()
	if s == nil {
		return
	}

	type job struct {
		h    hostSpec
		cred Cred
	}
	var stale []job
	for _, h := range hostSpecs {
		if !h.probeable {
			continue
		}
		for _, c := range h.get(s) {
			fp := Fingerprint(secretOf(h, c))
			ks, ok := st[statusKeyOf(h.name, fp)]
			if !ok || ks.LastChecked.IsZero() || time.Since(ks.LastChecked) > probeInterval {
				stale = append(stale, job{h: h, cred: c})
			}
		}
	}
	if len(stale) == 0 {
		return
	}
	if !claimProbeLease() {
		return
	}
	for _, j := range stale {
		probeOne(j.h, j.cred)
	}
}

// claimProbeLease tries to take the fleet-wide sweep lease. A fresh lease
// held by ANOTHER node means a peer is already sweeping — skip this round.
// Races on the upsert are possible but harmless (worst case: one extra probe
// per key).
func claimProbeLease() bool {
	var l probeLease
	if blob := loadBlob(leaseKey); len(blob) > 0 {
		_ = json.Unmarshal(blob, &l)
	}
	node := server.NodeID()
	if l.Node != node && time.Now().Before(l.Until) {
		return false
	}
	data, _ := json.Marshal(probeLease{Node: node, Until: time.Now().Add(leaseDuration)})
	_ = saveBlob(leaseKey, data)
	return true
}

// probeFuncs maps each probeable host to its credential check. A var so tests
// can stub the network without an HTTP server per case.
var probeFuncs = map[string]func(Cred) uploader.ProbeResult{
	"VoeSX":      func(c Cred) uploader.ProbeResult { return uploader.ProbeVoeSX(c.Key) },
	"Streamtape": func(c Cred) uploader.ProbeResult { return uploader.ProbeStreamtape(c.Login, c.Key) },
	"Vidara":     func(c Cred) uploader.ProbeResult { return uploader.ProbeVidara(c.Key) },
	"VidMoly":    func(c Cred) uploader.ProbeResult { return uploader.ProbeVidMoly(c.Key) },
}

// probeOne checks a single credential against its host API and records the
// result. Transport-level ambiguity (host down, WAF page, timeout) is recorded
// as "unknown" — a flaky network must never mark a good key dead — and an
// undecided probe never erases a decisive verdict recorded earlier: a network
// blip must not flip a known-dead key back to merely unknown; the next
// decisive probe (healthy or rejected) will overwrite it either way.
func probeOne(h hostSpec, c Cred) KeyStatus {
	ks := KeyStatus{
		Host:        h.name,
		Fingerprint: Fingerprint(secretOf(h, c)),
		LastChecked: time.Now(),
	}
	probe, probeable := probeFuncs[h.name]
	if !probeable {
		// Unprobeable host (Mixdrop): no cheap verify endpoint exists, so its
		// keys are judged only from live upload outcomes (key-death hook).
		// Never overwrite a passive verdict that hook already recorded.
		mu.RLock()
		prev, seen := statuses[statusKeyOf(ks.Host, ks.Fingerprint)]
		mu.RUnlock()
		if seen && prev.Status != StatusUntested {
			return prev
		}
		ks.Status = StatusUntested
		ks.Message = "no check API — judged from live uploads"
		recordStatus(ks.Host, ks.Fingerprint, ks.Status, ks.Message)
		return ks
	}
	res := probe(c)
	switch {
	case res.OK:
		ks.Status = StatusHealthy
	case res.Rejected:
		ks.Status = StatusDead
	default:
		mu.RLock()
		prev, seen := statuses[statusKeyOf(ks.Host, ks.Fingerprint)]
		mu.RUnlock()
		if seen && prev.Status != StatusUntested && prev.Status != StatusUnknown {
			// Keep the decisive verdict AND its original message (the reason it
			// was declared); only refresh LastChecked so the sweep retries the
			// key after the staleness window.
			prev.LastChecked = ks.LastChecked
			recordStatus(prev.Host, prev.Fingerprint, prev.Status, prev.Message)
			return prev
		}
		ks.Status = StatusUnknown
	}
	ks.Message = res.Message
	recordStatus(ks.Host, ks.Fingerprint, ks.Status, ks.Message)
	return ks
}

// ProbeNow probes one credential immediately (admin "Test" button) and
// returns the fresh status.
func ProbeNow(host, fingerprint string) (KeyStatus, error) {
	h, ok := specByName(host)
	if !ok {
		return KeyStatus{}, fmt.Errorf("unknown upload host %q", host)
	}
	ensureLoaded()
	mu.RLock()
	s := cloneSet(current)
	mu.RUnlock()
	if s == nil {
		return KeyStatus{}, fmt.Errorf("central key store not loaded")
	}
	for _, c := range h.get(s) {
		if Fingerprint(secretOf(h, c)) == fingerprint {
			return probeOne(h, c), nil
		}
	}
	return KeyStatus{}, fmt.Errorf("%s: no credential matching fingerprint %s", h.label, fingerprint)
}

var probeAllMu sync.Mutex

// ProbeAllNow probes every configured credential of every probeable host and
// returns the refreshed snapshot. Guarded by a mutex so a double-clicked
// "check all" cannot run two sweeps back to back.
func ProbeAllNow() ([]HostView, error) {
	if !probeAllMu.TryLock() {
		return nil, fmt.Errorf("a check is already running")
	}
	defer probeAllMu.Unlock()
	refresh()
	mu.RLock()
	s := cloneSet(current)
	mu.RUnlock()
	if s == nil {
		return nil, fmt.Errorf("central key store not loaded")
	}
	for _, h := range hostSpecs {
		if !h.probeable {
			// Passive hosts keep whatever the upload hook recorded for them.
			continue
		}
		for _, c := range h.get(s) {
			probeOne(h, c)
		}
	}
	return Snapshot(), nil
}
