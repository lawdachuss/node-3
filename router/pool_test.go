package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// stubManager is a minimal IManager implementation for pool handler tests.
type stubManager struct {
	channels []*entity.ChannelInfo
	created  []*entity.ChannelConfig
	stopped  []string
}

func (s *stubManager) CreateChannel(conf *entity.ChannelConfig, shouldSave bool) error {
	if s.channels != nil {
		for _, ch := range s.channels {
			if strings.EqualFold(ch.Username, conf.Username) {
				return fmt.Errorf("channel %s already exists", conf.Username)
			}
		}
	}
	s.created = append(s.created, conf)
	return nil
}

func (s *stubManager) StopChannel(username string) error {
	s.stopped = append(s.stopped, username)
	return nil
}

func (s *stubManager) ChannelInfo() []*entity.ChannelInfo { return s.channels }
func (s *stubManager) PauseChannel(username string) error { return nil }
func (s *stubManager) ResumeChannel(username string) error {
	return nil
}
func (s *stubManager) Publish(name string, ch *entity.ChannelInfo) {}
func (s *stubManager) PublishLog(username, line string)            {}
func (s *stubManager) PublishUploadState()                         {}
func (s *stubManager) Subscriber(w http.ResponseWriter, r *http.Request) {
}
func (s *stubManager) LoadConfig() error { return nil }
func (s *stubManager) SaveConfig() error { return nil }
func (s *stubManager) WaitForUploads()   {}
func (s *stubManager) StopAllChannels()  {}
func (s *stubManager) WaitForAllChannels() {
}
func (s *stubManager) StopWatcher()                {}
func (s *stubManager) CFBlockedCount() int         { return 0 }
func (s *stubManager) RequestCookieRefresh()       {}
func (s *stubManager) SetCookieRefreshFunc(func()) {}
func (s *stubManager) ChannelMinDurationBeforeUpload(username string) int {
	return 0
}
func (s *stubManager) StartSession(duration time.Duration) {
}
func (s *stubManager) StopSession()  {}
func (s *stubManager) StartWatcher() {}
func (s *stubManager) IsFileUploadInFlight(filePath string) bool {
	return false
}
func (s *stubManager) IsThumbnailAssetUploadInFlight(filePath string) bool {
	return false
}
func (s *stubManager) ActiveRecordingFiles() []string     { return nil }
func (s *stubManager) SessionInfo() (time.Duration, bool) { return 0, false }

func (s *stubManager) IsProcessingSession() bool { return false }
func (s *stubManager) TriggerSessionStop()       {}
func (s *stubManager) UploadEntries() *entity.UploadsResponse {
	return &entity.UploadsResponse{}
}
func (s *stubManager) ReportCFBlock(username string)    {}
func (s *stubManager) ResetCFBlock(username string)     {}
func (s *stubManager) ReportSessionCut(username string) {}

func setStubManager(t *testing.T, m *stubManager) {
	t.Helper()
	server.Manager = m
	t.Cleanup(func() { server.Manager = nil })
}

// testIsolatedMode pins the pool mode every test in this file assumes.
//
// detectPoolMode() treats any GITHUB_REPOSITORY containing "node-" as pooled,
// which is exactly what a GitHub Actions runner provides (lawdachuss/node-1) — so
// without this, these tests pass on a developer machine and fail in CI with a
// pooled-mode 400 ("Supabase not configured") or an empty assignment list.  The
// explicit env var wins over the repository name, and SyncNodeEnvironment()
// recomputes the cached value the handlers read.
func testIsolatedMode(t *testing.T) {
	t.Helper()
	t.Setenv("CHANNEL_POOL_MODE", entity.PoolModeIsolated)
	server.SyncNodeEnvironment()
}

// TestPoolChannelCheckFindsConfigured verifies the realtime checker reports a
// channel that already exists locally as a configured channel.
func TestPoolChannelCheckFindsConfigured(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = nil
	setStubManager(t, &stubManager{channels: []*entity.ChannelInfo{
		{Username: "Alice", Site: "chaturbate", IsOnline: true},
	}})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/pool/check?username=alice&site=chaturbate", nil)

	CheckPoolChannel(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	var resp PoolCheckResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Exists {
		t.Fatalf("expected exists=true for existing channel, got %+v", resp)
	}
	if resp.Source != "configured" {
		t.Fatalf("source = %q, want configured", resp.Source)
	}
}

// TestPoolChannelCheckAvailable verifies an unknown channel is reported as
// available (exists=false) even when the database is not configured.
func TestPoolChannelCheckAvailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = nil
	setStubManager(t, &stubManager{})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/pool/check?username=newgirl&site=chaturbate", nil)

	CheckPoolChannel(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	var resp PoolCheckResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Exists {
		t.Fatalf("expected exists=false for unknown channel, got %+v", resp)
	}
}

// TestPoolChannelCheckRequiresUsername verifies the checker rejects empty input.
func TestPoolChannelCheckRequiresUsername(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = nil
	setStubManager(t, &stubManager{})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/pool/check", nil)

	CheckPoolChannel(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// TestAddToPoolIsolatedCreatesLocalChannel verifies that in isolated mode
// (the default) AddToPool creates a locally configured channel instead of a
// channel_assignments row.
func TestAddToPoolIsolatedCreatesLocalChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	stub := &stubManager{}
	setStubManager(t, stub)

	body, _ := json.Marshal(map[string]interface{}{
		"site": "stripchat", "username": "bob", "resolution": 1080, "framerate": 30,
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/pool/add", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	AddToPool(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	if len(stub.created) != 1 {
		t.Fatalf("created %d channels, want 1", len(stub.created))
	}
	if stub.created[0].Username != "bob" || stub.created[0].Site != "stripchat" {
		t.Fatalf("unexpected created channel: %+v", stub.created[0])
	}
	if stub.created[0].Resolution != 1080 || stub.created[0].Framerate != 30 {
		t.Fatalf("unexpected resolution/framerate: %+v", stub.created[0])
	}
}

// TestAddToPoolIsolatedRejectsDuplicate verifies duplicates are skipped with a
// 409 and a clear message before anything is created.
func TestAddToPoolIsolatedRejectsDuplicate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	stub := &stubManager{channels: []*entity.ChannelInfo{
		{Username: "alice", Site: "chaturbate"},
	}}
	setStubManager(t, stub)

	body, _ := json.Marshal(map[string]interface{}{
		"site": "chaturbate", "username": "alice", "resolution": 1440, "framerate": 60,
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/pool/add", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	AddToPool(c)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", w.Code, w.Body.String())
	}
	if len(stub.created) != 0 {
		t.Fatalf("duplicate channel was created: %+v", stub.created)
	}
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if !strings.Contains(strings.ToLower(resp["error"]), "already exists") {
		t.Fatalf("error message = %q, want it to mention 'already exists'", resp["error"])
	}
}

// TestRemoveFromPoolIsolatedStopsChannel verifies RemoveFromPool stops the
// local channel in isolated mode.
func TestRemoveFromPoolIsolatedStopsChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	stub := &stubManager{}
	setStubManager(t, stub)

	body, _ := json.Marshal(map[string]interface{}{"username": "alice", "site": "chaturbate"})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/pool/remove", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	RemoveFromPool(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	if len(stub.stopped) != 1 || stub.stopped[0] != "alice" {
		t.Fatalf("stopped = %v, want [alice]", stub.stopped)
	}
}

// TestPoolJSONUsesSnakeCaseKeys verifies /api/pool returns snake_case keys
// matching what pool.html's JavaScript reads (a.username, a.site, a.is_live,
// ...). Without this the JS sees undefined for every field and the whole
// table renders with "undefined" usernames.
func TestPoolJSONUsesSnakeCaseKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	setStubManager(t, &stubManager{channels: []*entity.ChannelInfo{
		{Username: "alice", Site: "chaturbate", IsOnline: true},
	}})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/pool", nil)

	GetPoolJSON(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{`"username"`, `"site"`, `"status"`, `"is_live"`, `"source"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("pool JSON missing snake_case key %s; body: %s", want, body)
		}
	}
	if !strings.Contains(body, `"username":"alice"`) {
		t.Fatalf("pool JSON missing username value; body: %s", body)
	}
}

// TestPoolJSONIncludesPausedFlags verifies /api/pool carries the local
// paused/uploading/pending state that the fleet stuck-pause monitor reads from
// each node's pool API to flag paused-but-still-assigned channels.
func TestPoolJSONIncludesPausedFlags(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	setStubManager(t, &stubManager{channels: []*entity.ChannelInfo{
		{Username: "bob", Site: "stripchat", IsPaused: true, PauseReason: "manual"},
		{Username: "alice", Site: "chaturbate", IsOnline: true, UploadStatus: "uploading (1/2 hosts)"},
	}})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/pool", nil)

	GetPoolJSON(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{`"paused"`, `"pause_reason"`, `"uploading"`, `"pending"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("pool JSON missing flag %s; body: %s", want, body)
		}
	}
	// bob is paused and idle (manual reason); alice is recording with an
	// active upload.
	if !strings.Contains(body, `"paused":true`) {
		t.Fatalf("expected paused:true for the paused channel; body: %s", body)
	}
	if !strings.Contains(body, `"paused":false`) {
		t.Fatalf("expected paused:false for the recording channel; body: %s", body)
	}
	if !strings.Contains(body, `"pause_reason":"manual"`) {
		t.Fatalf("expected pause_reason manual for the paused channel; body: %s", body)
	}
	if !strings.Contains(body, `"uploading":true`) {
		t.Fatalf("expected uploading:true for the uploading channel; body: %s", body)
	}
}

// TestPoolPageRendersLocalChannels verifies the pool page renders configured
// channels in isolated mode instead of showing an empty page.
func TestPoolPageRendersLocalChannels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	testIsolatedMode(t)
	server.Config = &entity.Config{}
	setStubManager(t, &stubManager{channels: []*entity.ChannelInfo{
		{Username: "alice", Site: "chaturbate", IsOnline: true},
		{Username: "bob", Site: "stripchat", IsPaused: true},
	}})

	r := SetupRouter()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pool", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"alice", "bob", "Isolated", "Recording", "Paused", "pool-add-btn"} {
		if !strings.Contains(body, want) {
			t.Fatalf("pool page body missing %q", want)
		}
	}
}
