package uploadkeys

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

// stubStore replaces the Supabase round-trip with an in-memory blob map and
// resets the package globals, so every test starts from a clean slate and
// cannot leak background writes into a later test.
func stubStore(t *testing.T) map[string]*[]byte {
	t.Helper()
	blobs := map[string]*[]byte{}
	oldLoad, oldSave := loadBlob, saveBlob
	oldDelay := persistDelay
	oldProbes := probeFuncs

	loadBlob = func(key string) []byte {
		if v, ok := blobs[key]; ok {
			return append([]byte(nil), (*v)...)
		}
		return nil
	}
	saveBlob = func(key string, data []byte) error {
		cp := append([]byte(nil), data...)
		blobs[key] = &cp
		return nil
	}
	persistDelay = time.Hour // keep async writes from landing mid-suite
	persistScheduled.Store(false)
	// Every save now verifies its credential in-request; stub the round-trips
	// so the suite never touches a real host (cases that need a specific
	// verdict override the host entry).
	probeFuncs = map[string]func(Cred) uploader.ProbeResult{}
	for host := range oldProbes {
		probeFuncs[host] = func(Cred) uploader.ProbeResult { return uploader.ProbeResult{OK: true} }
	}

	mu.Lock()
	current, statuses, started = nil, nil, false
	lastLoadAttempt = time.Time{}
	mu.Unlock()

	t.Cleanup(func() {
		loadBlob, saveBlob = oldLoad, oldSave
		persistDelay, probeFuncs = oldDelay, oldProbes
		persistScheduled.Store(false)
		mu.Lock()
		current, statuses, started = nil, nil, false
		lastLoadAttempt = time.Time{}
		mu.Unlock()
	})
	return blobs
}

// stubConfig gives the test a fresh config (restored after) and neutralizes
// every env var applyToConfig/seedFromConfig may write.
func stubConfig(t *testing.T) *entity.Config {
	t.Helper()
	old := server.Config
	server.Config = &entity.Config{}
	t.Cleanup(func() { server.Config = old })
	for _, v := range []string{"VOESX_API_KEY", "VIDARA_KEY", "VIDMOLY_KEY", "STREAMTAPE_CREDS"} {
		t.Setenv(v, "")
	}
	return server.Config
}

func storedSet(t *testing.T, blobs map[string]*[]byte) *Set {
	t.Helper()
	v, ok := blobs[keysKey]
	if !ok {
		t.Fatalf("central key store was never saved (keys: %v)", len(blobs))
	}
	var s Set
	if err := json.Unmarshal(*v, &s); err != nil {
		t.Fatalf("decode stored set: %v", err)
	}
	return &s
}

func TestFingerprintAndDisplay(t *testing.T) {
	a, b := Fingerprint("secret-one"), Fingerprint("secret-one")
	if a != b || len(a) != 12 {
		t.Fatalf("fingerprint must be stable 12-char hex, got %q / %q", a, b)
	}
	if a == Fingerprint("secret-two") {
		t.Fatal("different secrets must fingerprint differently")
	}
	if got := display("abcdefghijklmnop"); got != "••••mnop" {
		t.Fatalf("display = %q", got)
	}
	if got := display("ab"); got != "••••" {
		t.Fatalf("short display = %q", got)
	}
}

func TestAddRemoveRoundtrip(t *testing.T) {
	blobs := stubStore(t)
	stubConfig(t)

	if _, err := Add("VoeSX", Cred{Key: "voe-key-1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	s := storedSet(t, blobs)
	if len(s.VoeSX) != 1 || s.VoeSX[0].Key != "voe-key-1" {
		t.Fatalf("stored voesx = %+v", s.VoeSX)
	}

	if _, err := Add("Streamtape", Cred{Login: "user", Key: "st-key"}); err != nil {
		t.Fatalf("Add streamtape: %v", err)
	}
	s = storedSet(t, blobs)
	if len(s.Streamtape) != 1 || s.Streamtape[0].Login != "user" || s.Streamtape[0].Key != "st-key" {
		t.Fatalf("stored streamtape = %+v", s.Streamtape)
	}

	if _, err := Add("Mixdrop", Cred{Email: "a@b.c", Token: "tok"}); err != nil {
		t.Fatalf("Add mixdrop: %v", err)
	}

	fp := Fingerprint("voe-key-1")
	if err := Remove("VoeSX", fp); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	s = storedSet(t, blobs)
	if len(s.VoeSX) != 0 {
		t.Fatalf("voesx still has %d creds after remove", len(s.VoeSX))
	}
	if err := Remove("VoeSX", fp); err == nil {
		t.Fatal("removing a missing fingerprint must error")
	}
}

func TestAddValidatesAndDedupes(t *testing.T) {
	stubStore(t)
	stubConfig(t)

	if _, err := Add("Nope", Cred{Key: "k"}); err == nil {
		t.Fatal("unknown host must be rejected")
	}
	if _, err := Add("VoeSX", Cred{}); err == nil {
		t.Fatal("empty key must be rejected")
	}
	if _, err := Add("Streamtape", Cred{Key: "k"}); err == nil {
		t.Fatal("streamtape without login must be rejected")
	}
	if _, err := Add("VidMoly", Cred{Key: "dup-key"}); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if _, err := Add("VidMoly", Cred{Key: "dup-key"}); err == nil {
		t.Fatal("duplicate credential must be rejected")
	}
}

func TestApplyToConfigOverlaysAllHosts(t *testing.T) {
	stubStore(t)
	cfg := stubConfig(t)
	cfg.VoeSXAPIKey = "old-env-key"
	cfg.StreamtapeLogin = "oldlogin"
	cfg.StreamtapeKey = "oldkey"

	applyToConfig(&Set{
		VoeSX:      []Cred{{Key: "v1"}, {Key: "v2"}},
		Streamtape: []Cred{{Login: "l1", Key: "k1"}, {Login: "l2", Key: "k2"}},
		Mixdrop:    []Cred{{Email: "mix@a.c", Token: "mix-tok"}},
		Vidara:     []Cred{{Key: "vid"}},
		VidMoly:    []Cred{{Key: "vm1"}, {Key: "vm2"}},
	})

	if cfg.VoeSXAPIKey != "v1,v2" {
		t.Errorf("VoeSXAPIKey = %q", cfg.VoeSXAPIKey)
	}
	if got := os.Getenv("VOESX_API_KEY"); got != "v1,v2" {
		t.Errorf("VOESX_API_KEY = %q (must win over env for central keys)", got)
	}
	if cfg.StreamtapeLogin != "l1,l2" || cfg.StreamtapeKey != "k1,k2" {
		t.Errorf("streamtape = %q / %q", cfg.StreamtapeLogin, cfg.StreamtapeKey)
	}
	if got := os.Getenv("STREAMTAPE_CREDS"); got != "l1:k1,l2:k2" {
		t.Errorf("STREAMTAPE_CREDS = %q", got)
	}
	if cfg.MixdropEmail != "mix@a.c" || cfg.MixdropToken != "mix-tok" {
		t.Errorf("mixdrop = %q / %q", cfg.MixdropEmail, cfg.MixdropToken)
	}
	if cfg.VidaraKey != "vid" || cfg.VidMolyKey != "vm1,vm2" {
		t.Errorf("vidara/vitmoly = %q / %q", cfg.VidaraKey, cfg.VidMolyKey)
	}
	if got := os.Getenv("VIDMOLY_KEY"); got != "vm1,vm2" {
		t.Errorf("VIDMOLY_KEY = %q", got)
	}

	// A partial set must not clear hosts it says nothing about.
	applyToConfig(&Set{Vidara: []Cred{{Key: "vid-only"}}})
	if cfg.VoeSXAPIKey != "v1,v2" || cfg.StreamtapeLogin != "l1,l2" {
		t.Errorf("partial apply cleared other hosts: voe=%q st=%q", cfg.VoeSXAPIKey, cfg.StreamtapeLogin)
	}
}

func TestSnapshotMergesStatuses(t *testing.T) {
	blobs := stubStore(t)
	stubConfig(t)

	set := &Set{VoeSX: []Cred{{Key: "good-key"}, {Key: "bad-key"}}}
	data, _ := json.Marshal(set)
	blobs[keysKey] = &data

	goodFP := Fingerprint("good-key")
	badFP := Fingerprint("bad-key")
	now := time.Now().UTC().Truncate(time.Second)
	rows := []KeyStatus{
		{Host: "VoeSX", Fingerprint: goodFP, Status: StatusHealthy, LastChecked: now, Message: ""},
		{Host: "VoeSX", Fingerprint: badFP, Status: StatusDead, LastChecked: now, Message: "authentication rejected"},
	}
	statusData, _ := json.Marshal(rows)
	blobs[statusKey] = &statusData

	views := Snapshot()
	var voe *HostView
	for i := range views {
		if views[i].Host == "VoeSX" {
			voe = &views[i]
		}
	}
	if voe == nil || len(voe.Keys) != 2 {
		t.Fatalf("voesx view = %+v", voe)
	}
	byFP := map[string]KeyView{}
	for _, k := range voe.Keys {
		byFP[k.Fingerprint] = k
	}
	if k := byFP[goodFP]; k.Status != StatusHealthy || k.Display != "••••-key" {
		t.Errorf("good key = %+v", k)
	}
	if k := byFP[badFP]; k.Status != StatusDead || k.Message != "authentication rejected" {
		t.Errorf("bad key = %+v", k)
	}
	// Mixdrop exists as a host even with no keys (its Add form must render).
	foundMixdrop := false
	for _, v := range views {
		if v.Host == "Mixdrop" && len(v.Keys) == 0 && v.Probeable {
			t.Error("mixdrop must not be probeable")
		}
		if v.Host == "Mixdrop" {
			foundMixdrop = true
		}
	}
	if !foundMixdrop {
		t.Fatal("Mixdrop host missing from snapshot")
	}
}

func TestOnKeyDeathClassifies(t *testing.T) {
	stubStore(t)

	onKeyDeath("Vidara", "auth-secret", "authentication rejected")
	if s := statuses[statusKeyOf("Vidara", Fingerprint("auth-secret"))]; s.Status != StatusDead || s.Host != "Vidara" {
		t.Fatalf("auth death = %+v, want dead/Vidara", s)
	}

	onKeyDeath("VoeSX", "quota-secret", "storage quota full")
	if s := statuses[statusKeyOf("VoeSX", Fingerprint("quota-secret"))]; s.Status != StatusQuota {
		t.Fatalf("quota death = %+v, want quota", s)
	}

	onKeyDeath("VidMoly", "limit-secret", "daily api limit")
	if s := statuses[statusKeyOf("VidMoly", Fingerprint("limit-secret"))]; s.Status != StatusQuota {
		t.Fatalf("daily-limit = %+v, want quota", s)
	}
}

// The same key string configured for two different hosts must get independent
// health verdicts — a shared fingerprint must never cross-contaminate.
func TestSameKeyDifferentHostsKeptSeparate(t *testing.T) {
	stubStore(t)

	recordStatus("Vidara", Fingerprint("shared-secret"), StatusDead, "rejected")
	recordStatus("VidMoly", Fingerprint("shared-secret"), StatusHealthy, "")
	if s := statuses[statusKeyOf("Vidara", Fingerprint("shared-secret"))]; s.Status != StatusDead {
		t.Fatalf("vidara verdict clobbered by vidmoly: %+v", s)
	}
	if s := statuses[statusKeyOf("VidMoly", Fingerprint("shared-secret"))]; s.Status != StatusHealthy {
		t.Fatalf("vidmoly verdict clobbered by vidara: %+v", s)
	}
}

func TestSyncLegacyFormReplacesOnlyGivenHosts(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)

	pre := &Set{Vidara: []Cred{{Key: "pre-existing-vidara"}}}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	SyncLegacyForm("n1,n2", "l1,l2", "k1,k2", "mix@a.c", "mix-tok", "vid1", "vm1")

	s := storedSet(t, blobs)
	if len(s.VoeSX) != 2 || s.VoeSX[0].Key != "n1" || s.VoeSX[1].Key != "n2" {
		t.Errorf("voesx = %+v", s.VoeSX)
	}
	if len(s.Streamtape) != 2 || s.Streamtape[1].Login != "l2" || s.Streamtape[1].Key != "k2" {
		t.Errorf("streamtape = %+v", s.Streamtape)
	}
	if len(s.Mixdrop) != 1 || s.Mixdrop[0].Email != "mix@a.c" {
		t.Errorf("mixdrop = %+v", s.Mixdrop)
	}
	if len(s.Vidara) != 1 || s.Vidara[0].Key != "vid1" {
		t.Errorf("vidara = %+v (legacy replace)", s.Vidara)
	}
	if len(s.VidMoly) != 1 || s.VidMoly[0].Key != "vm1" {
		t.Errorf("vidmoly = %+v", s.VidMoly)
	}
	if cfg.VoeSXAPIKey != "n1,n2" {
		t.Errorf("config not applied: %q", cfg.VoeSXAPIKey)
	}

	// All-empty submission is a no-op (the legacy form always posts every field).
	before, _ := json.Marshal(storedSet(t, blobs))
	SyncLegacyForm("", "", "", "", "", "", "")
	after, _ := json.Marshal(storedSet(t, blobs))
	if string(before) != string(after) {
		t.Fatal("all-empty sync must not touch the store")
	}
}

func TestSeedFromConfigOnlyWhenEmpty(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)

	cfg.VoeSXAPIKey = "env1,env2"
	cfg.StreamtapeLogin = "sl1,sl2"
	cfg.StreamtapeKey = "sk1,sk2"
	cfg.MixdropEmail = "mix@a.c"
	cfg.MixdropToken = "mix-tok"
	cfg.VidaraKey = "vid-env"
	cfg.VidMolyKey = "vm-env"

	seedFromConfig()
	s := storedSet(t, blobs)
	if len(s.VoeSX) != 2 || s.VoeSX[1].Key != "env2" {
		t.Errorf("seeded voesx = %+v", s.VoeSX)
	}
	if len(s.Streamtape) != 2 || s.Streamtape[0].Login != "sl1" || s.Streamtape[0].Key != "sk1" {
		t.Errorf("seeded streamtape = %+v", s.Streamtape)
	}
	if len(s.Mixdrop) != 1 || s.Mixdrop[0].Token != "mix-tok" {
		t.Errorf("seeded mixdrop = %+v", s.Mixdrop)
	}
	if len(s.Vidara) != 1 || s.Vidara[0].Key != "vid-env" {
		t.Errorf("seeded vidara = %+v", s.Vidara)
	}
	if len(s.VidMoly) != 1 || s.VidMoly[0].Key != "vm-env" {
		t.Errorf("seeded vidmoly = %+v", s.VidMoly)
	}

	// A non-empty central store must never be re-seeded from env.
	mine := &Set{VoeSX: []Cred{{Key: "central-wins"}}}
	mineData, _ := json.Marshal(mine)
	blobs[keysKey] = &mineData
	mu.Lock()
	current = nil
	mu.Unlock()
	seedFromConfig()
	if got := storedSet(t, blobs); len(got.VoeSX) != 1 || got.VoeSX[0].Key != "central-wins" {
		t.Fatalf("seed overwrote central store: %+v", got.VoeSX)
	}

	// Nothing in config and nothing in the store → no write at all.
	mu.Lock()
	current = nil
	mu.Unlock()
	delete(blobs, keysKey)
	server.Config = &entity.Config{}
	seedFromConfig()
	if _, ok := blobs[keysKey]; ok {
		t.Fatal("empty config must not create a store")
	}
}

func TestClaimProbeLease(t *testing.T) {
	blobs := stubStore(t)

	if !claimProbeLease() {
		t.Fatal("empty lease must be claimable")
	}
	var l probeLease
	if err := json.Unmarshal(*blobs[leaseKey], &l); err != nil || l.Node != server.NodeID() {
		t.Fatalf("lease = %+v (err %v), want our node %q", l, err, server.NodeID())
	}

	// Fresh lease held by someone else blocks us.
	fresh, _ := json.Marshal(probeLease{Node: "other-node", Until: time.Now().Add(10 * time.Minute)})
	blobs[leaseKey] = &fresh
	if claimProbeLease() {
		t.Fatal("fresh foreign lease must block")
	}

	// Expired foreign lease is re-claimable.
	stale, _ := json.Marshal(probeLease{Node: "other-node", Until: time.Now().Add(-time.Minute)})
	blobs[leaseKey] = &stale
	if !claimProbeLease() {
		t.Fatal("expired lease must be claimable")
	}

	// Our own fresh lease does not block us (restart mid-sweep).
	own, _ := json.Marshal(probeLease{Node: server.NodeID(), Until: time.Now().Add(10 * time.Minute)})
	blobs[leaseKey] = &own
	if !claimProbeLease() {
		t.Fatal("own lease must be re-claimable")
	}
}

func TestMergeStatusesNewerWins(t *testing.T) {
	stubStore(t)

	fp := Fingerprint("shared-key")
	k := statusKeyOf("Vidara", fp)
	fresh := time.Now()
	mergeStatuses([]KeyStatus{{Host: "Vidara", Fingerprint: fp, Status: StatusHealthy, LastChecked: fresh}})
	mergeStatuses([]KeyStatus{{Host: "Vidara", Fingerprint: fp, Status: StatusDead, LastChecked: fresh.Add(-time.Hour)}})
	if s := statuses[k]; s.Status != StatusHealthy {
		t.Fatalf("older row clobbered fresh status: %+v", s)
	}

	mergeStatuses([]KeyStatus{{Host: "Vidara", Fingerprint: fp, Status: StatusDead, LastChecked: fresh.Add(time.Hour)}})
	if s := statuses[k]; s.Status != StatusDead {
		t.Fatalf("newer row not applied: %+v", s)
	}
}

// A Managed store with an emptied-out host must actually dark that host on the
// node — otherwise the env-derived fallback (GitHub secrets) resurrects the
// deleted keys. An UNMANAGED set (Supabase unreachable / never seeded) must
// never clear anything.
func TestManagedClearsRemovedHosts(t *testing.T) {
	stubStore(t)
	cfg := stubConfig(t)
	cfg.VoeSXAPIKey = "env-still-there"
	t.Setenv("VOESX_API_KEY", "env-still-there")
	cfg.VidMolyKey = "vm-env"
	cfg.StreamtapeLogin = "sl"
	cfg.StreamtapeKey = "sk"
	cfg.MixdropEmail = "m@a.c"
	cfg.MixdropToken = "m-tok"

	// Unmanaged: empty voesx list must NOT clear the env-derived key.
	applyToConfig(&Set{Vidara: []Cred{{Key: "vid"}}})
	if cfg.VoeSXAPIKey != "env-still-there" {
		t.Fatalf("unmanaged set cleared voesx: %q", cfg.VoeSXAPIKey)
	}

	// Managed: empty lists mean deleted on purpose.
	applyToConfig(&Set{Managed: true, Vidara: []Cred{{Key: "vid"}}})
	if cfg.VoeSXAPIKey != "" || os.Getenv("VOESX_API_KEY") != "" {
		t.Errorf("managed empty voesx not cleared: cfg=%q env=%q", cfg.VoeSXAPIKey, os.Getenv("VOESX_API_KEY"))
	}
	if cfg.VidMolyKey != "" || os.Getenv("VIDMOLY_KEY") != "" {
		t.Errorf("managed empty vidmoly not cleared: cfg=%q env=%q", cfg.VidMolyKey, os.Getenv("VIDMOLY_KEY"))
	}
	if cfg.StreamtapeLogin != "" || cfg.StreamtapeKey != "" || os.Getenv("STREAMTAPE_CREDS") != "" {
		t.Errorf("managed empty streamtape not cleared: %q / %q / %q", cfg.StreamtapeLogin, cfg.StreamtapeKey, os.Getenv("STREAMTAPE_CREDS"))
	}
	if cfg.MixdropEmail != "" || cfg.MixdropToken != "" {
		t.Errorf("managed empty mixdrop not cleared: %q / %q", cfg.MixdropEmail, cfg.MixdropToken)
	}
	if cfg.VidaraKey != "vid" {
		t.Errorf("managed non-empty vidara must still overlay: %q", cfg.VidaraKey)
	}

	// Save stamps Managed so every persisted set is authoritative.
	s := &Set{}
	if err := Save(s); err != nil || !s.Managed {
		t.Fatalf("Save must stamp Managed (err=%v managed=%v)", err, s.Managed)
	}
}

// An unknown (indecisive) probe must never erase a decisive verdict — but a
// decisive probe always overwrites.
func TestProbeUnknownPreservesVerdict(t *testing.T) {
	stubStore(t)

	old := probeFuncs["Vidara"]
	t.Cleanup(func() { probeFuncs["Vidara"] = old })
	probeFuncs["Vidara"] = func(Cred) uploader.ProbeResult { return uploader.ProbeResult{Message: "host down"} }

	voe, _ := specByName("Vidara")
	cred := Cred{Key: "vk-1"}

	// First probe with no verdict → unknown.
	ks := probeOne(voe, cred)
	if ks.Status != StatusUnknown {
		t.Fatalf("first probe = %q, want unknown", ks.Status)
	}

	// A passive dead verdict must survive a later network-glitch probe.
	recordStatus("Vidara", Fingerprint("vk-1"), StatusDead, "rejected by api")
	ks2 := probeOne(voe, cred)
	if ks2.Status != StatusDead || !strings.Contains(ks2.Message, "rejected") {
		t.Fatalf("unknown probe erased dead verdict: %+v", ks2)
	}
	if ks2.LastChecked.Before(ks.LastChecked) {
		t.Error("unknown probe must still refresh LastChecked (retry later)")
	}

	// A decisive probe always overwrites.
	probeFuncs["Vidara"] = func(Cred) uploader.ProbeResult { return uploader.ProbeResult{OK: true} }
	if ks3 := probeOne(voe, cred); ks3.Status != StatusHealthy {
		t.Fatalf("decisive healthy probe must overwrite dead: %+v", ks3)
	}
}

// The legacy settings dialog only displays the FIRST Mixdrop account, so an
// unchanged resubmission must not truncate the operator's account pool.
func TestSyncLegacyFormMixdropPoolPreserved(t *testing.T) {
	blobs := stubStore(t)
	stubConfig(t)

	pre := &Set{Mixdrop: []Cred{{Email: "first@a.c", Token: "tok-1"}, {Email: "second@a.c", Token: "tok-2"}}}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	// Unchanged resubmission of the first account → pool preserved.
	SyncLegacyForm("", "", "", "first@a.c", "tok-1", "", "")
	if got := storedSet(t, blobs); len(got.Mixdrop) != 2 {
		t.Fatalf("unchanged sync truncated mixdrop pool: %+v", got.Mixdrop)
	}

	// Changed token (intentional rotation) → replaced with the single pair.
	SyncLegacyForm("", "", "", "first@a.c", "tok-rotated", "", "")
	if got := storedSet(t, blobs); len(got.Mixdrop) != 1 || got.Mixdrop[0].Token != "tok-rotated" {
		t.Fatalf("rotation not applied: %+v", got.Mixdrop)
	}
}

// A Managed store that the operator emptied must not be re-seeded from env on
// the next boot.
func TestManagedEmptyStoreNotReseeded(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)
	cfg.VoeSXAPIKey = "env-key"

	empty := &Set{Managed: true}
	emptyData, _ := json.Marshal(empty)
	blobs[keysKey] = &emptyData

	seedFromConfig()
	if got := storedSet(t, blobs); len(got.VoeSX) != 0 {
		t.Fatalf("managed empty store re-seeded from env: %+v", got.VoeSX)
	}
}

// A quota verdict from a probe (storage full / daily limit) must land as
// "quota" on the admin page — never "healthy" — because the key cannot accept
// an upload until the cap resets.
func TestProbeOneRecordsQuotaNotHealthy(t *testing.T) {
	stubStore(t)

	old := probeFuncs["Vidara"]
	t.Cleanup(func() { probeFuncs["Vidara"] = old })
	probeFuncs["Vidara"] = func(Cred) uploader.ProbeResult {
		return uploader.ProbeResult{Quota: true, Message: "daily upload limit"}
	}

	h, _ := specByName("Vidara")
	ks := probeOne(h, Cred{Key: "capped-key"})
	if ks.Status != StatusQuota {
		t.Fatalf("probe status = %q, want quota", ks.Status)
	}
	if ks.Message != "daily upload limit" {
		t.Errorf("message = %q", ks.Message)
	}
}

// Editing a credential through the Settings form must drop the replaced key's
// status row (so a new key can never inherit the old key's green badge) and
// verify the new credential inside the save, returning its verdict.
func TestSyncLegacyFormDropsStaleStatusAndVerifiesNewKey(t *testing.T) {
	blobs := stubStore(t)
	stubConfig(t)

	pre := &Set{Vidara: []Cred{{Key: "old-vidara-key"}}}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	oldFP := Fingerprint("old-vidara-key")
	recordStatus("Vidara", oldFP, StatusHealthy, "")
	if statuses[statusKeyOf("Vidara", oldFP)].Status != StatusHealthy {
		t.Fatal("precondition: old key not healthy")
	}

	var probed []string
	probeFuncs["Vidara"] = func(c Cred) uploader.ProbeResult {
		probed = append(probed, c.Key)
		return uploader.ProbeResult{OK: true}
	}

	verdicts := SyncLegacyForm("", "", "", "", "", "new-vidara-key", "")

	if _, ok := statuses[statusKeyOf("Vidara", oldFP)]; ok {
		t.Error("replaced key's status row survived the edit")
	}
	if len(probed) != 1 || probed[0] != "new-vidara-key" {
		t.Fatalf("probed = %+v, want the new Vidara key", probed)
	}
	if len(verdicts) != 1 || verdicts[0].Status != StatusHealthy || verdicts[0].Label != "Vidara" {
		t.Fatalf("verdicts = %+v, want one healthy Vidara verdict", verdicts)
	}
	if got := storedSet(t, blobs); len(got.Vidara) != 1 || got.Vidara[0].Key != "new-vidara-key" {
		t.Fatalf("stored vidara = %+v", got.Vidara)
	}

	// An unchanged resubmission must not re-verify or drop the (now) current key.
	probed = nil
	if verdicts := SyncLegacyForm("", "", "", "", "", "new-vidara-key", ""); len(verdicts) != 0 {
		t.Errorf("unchanged key returned verdicts: %+v", verdicts)
	}
	if len(probed) != 0 {
		t.Errorf("unchanged key re-probed: %+v", probed)
	}
}

// A rotated key the host rejects must surface that error in the save call
// itself — the same request that persisted it — not only after the next sweep.
func TestSyncLegacyFormReportsFailingKeyInSaveResponse(t *testing.T) {
	blobs := stubStore(t)
	stubConfig(t)

	pre := &Set{Vidara: []Cred{{Key: "old-vidara-key"}}}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	probeFuncs["Vidara"] = func(Cred) uploader.ProbeResult {
		return uploader.ProbeResult{Rejected: true, Message: "upload rejected: HTTP 401"}
	}

	verdicts := SyncLegacyForm("", "", "", "", "", "rotated-bad-key", "")
	if len(verdicts) != 1 {
		t.Fatalf("verdicts = %+v, want exactly the rotated key", verdicts)
	}
	v := verdicts[0]
	if v.Status != StatusDead || !strings.Contains(v.Message, "401") {
		t.Fatalf("verdict = %+v, want dead with the host's 401 message", v)
	}
	if v.Label != "Vidara" || v.Display != "••••-key" {
		t.Errorf("verdict identity = %q / %q", v.Label, v.Display)
	}
	if got := storedSet(t, blobs); len(got.Vidara) != 1 || got.Vidara[0].Key != "rotated-bad-key" {
		t.Fatalf("failing key was not saved: %+v", got.Vidara)
	}
}

// Add must return the round-trip verdict it just ran, not leave verification to
// a caller (the router used to re-find the key and probe it a second time).
func TestAddReturnsProbeVerdict(t *testing.T) {
	stubStore(t)
	stubConfig(t)

	probeFuncs["VidMoly"] = func(c Cred) uploader.ProbeResult {
		if c.Key != "vm-new" {
			t.Fatalf("probed key = %q", c.Key)
		}
		return uploader.ProbeResult{Quota: true, Message: "daily upload limit"}
	}

	v, err := Add("VidMoly", Cred{Key: "vm-new"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if v.Host != "VidMoly" || v.Label != "VidMoly" || v.Status != StatusQuota || !strings.Contains(v.Message, "daily") {
		t.Fatalf("verdict = %+v", v)
	}
	if s := statuses[statusKeyOf("VidMoly", Fingerprint("vm-new"))]; s.Status != StatusQuota {
		t.Fatalf("status row = %+v, want quota", s)
	}
}

func TestProbeOneMixdropKeepsPassiveVerdict(t *testing.T) {
	stubStore(t)

	// No check API: first probe reports untested...
	mixdrop, _ := specByName("Mixdrop")
	ks := probeOne(mixdrop, Cred{Email: "a@b.c", Token: "tok"})
	if ks.Status != StatusUntested {
		t.Fatalf("first mixdrop probe = %q, want untested", ks.Status)
	}

	// ...but a later probe must not wipe a passive dead verdict.
	fp := Fingerprint("a@b.c:tok")
	recordStatus("Mixdrop", fp, StatusDead, "upload failed: rejected our credentials")
	ks2 := probeOne(mixdrop, Cred{Email: "a@b.c", Token: "tok"})
	if ks2.Status != StatusDead || !strings.Contains(ks2.Message, "rejected") {
		t.Fatalf("passive verdict wiped: %+v", ks2)
	}
}

// A node that boots AFTER an operator edited the central store must apply the
// loaded set immediately. refresh() only applies when it DETECTS a change, so
// without applyLoaded the booting node's baseline already equals the DB, every
// refresh says "no change", and the whole (6h) session uploads with the stale
// env-derived keys — the admin-page edit would reach only nodes already
// running when it happened.
func TestApplyLoadedAppliesCentralSetAtBoot(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)
	cfg.VoeSXAPIKey = "stale-env-key"
	t.Setenv("VOESX_API_KEY", "stale-env-key")

	central := &Set{Managed: true, VoeSX: []Cred{{Key: "central-new-key"}}}
	data, _ := json.Marshal(central)
	blobs[keysKey] = &data

	// Boot path: load the central set, then apply it — with NO change for a
	// refresh() to detect afterwards.
	ensureLoaded()
	applyLoaded()

	if cfg.VoeSXAPIKey != "central-new-key" {
		t.Errorf("config not applied at boot: %q (stale env key survived)", cfg.VoeSXAPIKey)
	}
	if got := os.Getenv("VOESX_API_KEY"); got != "central-new-key" {
		t.Errorf("env not applied at boot: %q", got)
	}

	// A Managed store the operator emptied must clear the booting node's env
	// too — a boot can never resurrect deliberately deleted keys.
	mu.Lock()
	current = nil
	lastLoadAttempt = time.Time{}
	mu.Unlock()
	empty, _ := json.Marshal(&Set{Managed: true})
	blobs[keysKey] = &empty

	ensureLoaded()
	applyLoaded()
	if cfg.VoeSXAPIKey != "" || os.Getenv("VOESX_API_KEY") != "" {
		t.Errorf("managed empty store not applied at boot: cfg=%q env=%q", cfg.VoeSXAPIKey, os.Getenv("VOESX_API_KEY"))
	}

	// Unmanaged + empty (no store yet) must not touch anything.
	mu.Lock()
	current = nil
	lastLoadAttempt = time.Time{}
	mu.Unlock()
	delete(blobs, keysKey)
	cfg.VoeSXAPIKey = "env-untouched"
	t.Setenv("VOESX_API_KEY", "env-untouched")

	ensureLoaded()
	applyLoaded()
	if cfg.VoeSXAPIKey != "env-untouched" || os.Getenv("VOESX_API_KEY") != "env-untouched" {
		t.Errorf("empty unmanaged load overwrote env: cfg=%q env=%q", cfg.VoeSXAPIKey, os.Getenv("VOESX_API_KEY"))
	}
}

// Adding a key is a rotation: it must REPLACE the host's old keys everywhere
// (config + env + store), drop their status rows, and verify the new one —
// leaving the old dead key in the ring would keep uploads starting on a
// credential the host already refused.
func TestAddReplacesOldKeysAndDropsStatuses(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)

	pre := &Set{VoeSX: []Cred{{Key: "old-dead-key"}, {Key: "old-good-key"}}}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	deadFP := Fingerprint("old-dead-key")
	goodFP := Fingerprint("old-good-key")
	recordStatus("VoeSX", deadFP, StatusDead, "HTTP 401")
	recordStatus("VoeSX", goodFP, StatusHealthy, "")

	var probed []string
	probeFuncs["VoeSX"] = func(c Cred) uploader.ProbeResult {
		probed = append(probed, c.Key)
		return uploader.ProbeResult{OK: true}
	}

	verdict, err := Add("VoeSX", Cred{Key: "brand-new-key"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	s := storedSet(t, blobs)
	if !s.Managed {
		t.Error("replacement set must be stamped Managed")
	}
	if len(s.VoeSX) != 1 || s.VoeSX[0].Key != "brand-new-key" {
		t.Fatalf("stored voesx = %+v, want only the new key", s.VoeSX)
	}
	if _, ok := statuses[statusKeyOf("VoeSX", deadFP)]; ok {
		t.Error("replaced dead key's status row survived")
	}
	if _, ok := statuses[statusKeyOf("VoeSX", goodFP)]; ok {
		t.Error("replaced healthy key's status row survived")
	}
	if len(probed) != 1 || probed[0] != "brand-new-key" {
		t.Fatalf("probed = %+v, want exactly the new key", probed)
	}
	if verdict.Status != StatusHealthy {
		t.Errorf("verdict = %+v, want healthy", verdict)
	}
	if cfg.VoeSXAPIKey != "brand-new-key" || os.Getenv("VOESX_API_KEY") != "brand-new-key" {
		t.Errorf("replacement not applied: cfg=%q env=%q", cfg.VoeSXAPIKey, os.Getenv("VOESX_API_KEY"))
	}
}

// The reaper deletes only keys the host EXPLICITLY rejected (dead): quota,
// unknown, untested and healthy credentials must survive, their verdicts must
// survive, and a host whose last key was dead must go dark (env cleared) —
// while a store without any dead verdict is never re-saved.
func TestReapDeadRemovesOnlyDeadKeys(t *testing.T) {
	blobs := stubStore(t)
	cfg := stubConfig(t)
	t.Setenv("VOESX_API_KEY", "stale-env")
	t.Setenv("VIDARA_KEY", "stale-vidara")

	pre := &Set{
		VoeSX:  []Cred{{Key: "dead-key"}, {Key: "quota-key"}, {Key: "healthy-key"}, {Key: "unknown-key"}},
		Vidara: []Cred{{Key: "only-and-dead"}},
	}
	preData, _ := json.Marshal(pre)
	blobs[keysKey] = &preData

	recordStatus("VoeSX", Fingerprint("dead-key"), StatusDead, "upload rejected: HTTP 401")
	recordStatus("VoeSX", Fingerprint("quota-key"), StatusQuota, "storage quota full")
	recordStatus("VoeSX", Fingerprint("healthy-key"), StatusHealthy, "")
	recordStatus("VoeSX", Fingerprint("unknown-key"), StatusUnknown, "host down")
	recordStatus("Vidara", Fingerprint("only-and-dead"), StatusDead, "authentication rejected")

	// Reaping must not touch the network: no probe may run.
	probes := 0
	origProbes := probeFuncs
	for host, old := range probeFuncs {
		probeFuncs[host] = func(c Cred) uploader.ProbeResult {
			probes++
			return old(c)
		}
	}
	t.Cleanup(func() { probeFuncs = origProbes })

	ensureLoaded()
	if n := reapDead(); n != 2 {
		t.Fatalf("reaped %d keys, want 2 (one per host)", n)
	}
	if probes != 0 {
		t.Fatalf("reap ran %d probe(s), want none", probes)
	}

	s := storedSet(t, blobs)
	if !s.Managed {
		t.Error("reaped set must stay Managed")
	}
	if len(s.VoeSX) != 3 || s.VoeSX[0].Key != "quota-key" || s.VoeSX[1].Key != "healthy-key" || s.VoeSX[2].Key != "unknown-key" {
		t.Fatalf("voesx after reap = %+v, want only quota/healthy/unknown", s.VoeSX)
	}
	if len(s.Vidara) != 0 {
		t.Fatalf("vidara after reap = %+v, want empty (last key was dead)", s.Vidara)
	}
	if _, ok := statuses[statusKeyOf("VoeSX", Fingerprint("dead-key"))]; ok {
		t.Error("reaped key's status row survived")
	}
	if statuses[statusKeyOf("VoeSX", Fingerprint("quota-key"))].Status != StatusQuota {
		t.Error("quota verdict lost by the reaper")
	}
	if statuses[statusKeyOf("VoeSX", Fingerprint("healthy-key"))].Status != StatusHealthy {
		t.Error("healthy verdict lost by the reaper")
	}
	if statuses[statusKeyOf("VoeSX", Fingerprint("unknown-key"))].Status != StatusUnknown {
		t.Error("unknown verdict lost by the reaper")
	}
	if got := os.Getenv("VOESX_API_KEY"); got != "quota-key,healthy-key,unknown-key" {
		t.Errorf("env after reap = %q", got)
	}
	// Last key of the host was dead → Managed apply must dark the host.
	if cfg.VidaraKey != "" || os.Getenv("VIDARA_KEY") != "" {
		t.Errorf("dead-only host not darked: cfg=%q env=%q", cfg.VidaraKey, os.Getenv("VIDARA_KEY"))
	}

	// Idempotent: nothing dead left → no second save, no change.
	before := string(*blobs[keysKey])
	if n := reapDead(); n != 0 {
		t.Errorf("second reap removed %d key(s), want 0", n)
	}
	if string(*blobs[keysKey]) != before {
		t.Error("noop reap must not re-save the store")
	}
}
