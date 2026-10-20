package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func TestUpdateConfigAcceptsJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server.Config = &entity.Config{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	body, err := json.Marshal(map[string]string{
		"cookies":      "cf_clearance=test_cf; sessionid=test; csrftoken=abc123",
		"user_agent":   "TestAgent/1.0",
		"affiliate_wm": "wm-router",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/update_config", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	UpdateConfig(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if server.Config.Cookies != "cf_clearance=test_cf; sessionid=test; csrftoken=abc123" {
		t.Fatalf("Cookies = %q", server.Config.Cookies)
	}
	if server.Config.UserAgent != "TestAgent/1.0" {
		t.Fatalf("UserAgent = %q", server.Config.UserAgent)
	}
	if server.Config.CfClearance != "test_cf" {
		t.Fatalf("CfClearance = %q", server.Config.CfClearance)
	}
	if server.Config.AffiliateWM != "wm-router" {
		t.Fatalf("AffiliateWM = %q", server.Config.AffiliateWM)
	}
}

// fakeAppSettings is a minimal in-memory app_settings REST endpoint so the
// credential sync inside UpdateConfig runs without Supabase.
type fakeAppSettings struct {
	mu sync.Mutex
	m  map[string]json.RawMessage
}

func (f *fakeAppSettings) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		k := strings.TrimPrefix(r.URL.Query().Get("key"), "eq.")
		if v, ok := f.m[k]; ok {
			fmt.Fprintf(w, `[{"key":%q,"value":%s}]`, k, v)
			return
		}
		io.WriteString(w, `[]`)
	case http.MethodPatch:
		var body struct {
			Value json.RawMessage `json:"value"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		k := strings.TrimPrefix(r.URL.Query().Get("key"), "eq.")
		f.m[k] = body.Value
		fmt.Fprintf(w, `[{"key":%q,"value":%s}]`, k, body.Value)
	case http.MethodPost:
		var body struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.m[body.Key] = body.Value
		fmt.Fprintf(w, `[{"key":%q,"value":%s}]`, body.Key, body.Value)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// The Settings dialog saves with fetch (multipart + Accept: application/json)
// and expects the verdict of every credential the save added or rotated in the
// same response. Mixdrop is the one managed host with no upload check API, so
// this exercises the handler end to end without touching the network.
func TestUpdateConfigReturnsKeyVerdictsToDialogSave(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := httptest.NewServer(&fakeAppSettings{m: map[string]json.RawMessage{}})
	defer db.Close()

	old := server.Config
	server.Config = &entity.Config{SupabaseURL: db.URL, SupabaseAPIKey: "test"}
	t.Cleanup(func() { server.Config = old })
	// applyToConfig mirrors the central set into the environment.
	for _, v := range []string{"VOESX_API_KEY", "VIDARA_KEY", "VIDMOLY_KEY", "STREAMTAPE_CREDS"} {
		t.Setenv(v, "")
	}

	save := func(fields map[string]string, acceptJSON bool) *httptest.ResponseRecorder {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		for k, v := range fields {
			_ = mw.WriteField(k, v)
		}
		_ = mw.Close()

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := httptest.NewRequest(http.MethodPost, "/update_config", &body)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		if acceptJSON {
			req.Header.Set("Accept", "application/json")
		}
		c.Request = req
		UpdateConfig(c)
		// gin records a redirect's 302 in its responseWriter but only flushes
		// it when the engine calls WriteHeaderNow after the handler returns.
		// A direct handler call (this test) must flush itself, or the recorder
		// keeps its 200 default while Location is already set. No-op for the
		// JSON saves above — those already wrote their body.
		c.Writer.WriteHeaderNow()
		return w
	}
	type keyVerdict struct {
		Host    string `json:"host"`
		Label   string `json:"label"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	decode := func(w *httptest.ResponseRecorder) []keyVerdict {
		t.Helper()
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
		}
		var resp struct {
			OK   bool         `json:"ok"`
			Keys []keyVerdict `json:"keys"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode response %q: %v", w.Body.String(), err)
		}
		if !resp.OK {
			t.Fatalf("ok = false in %q", w.Body.String())
		}
		return resp.Keys
	}

	mixdrop := map[string]string{"mixdrop_email": "mix@a.c", "mixdrop_token": "tok"}
	keys := decode(save(mixdrop, true))
	if len(keys) != 1 || keys[0].Host != "Mixdrop" || keys[0].Status != "untested" {
		t.Fatalf("first save verdicts = %+v, want one untested Mixdrop verdict", keys)
	}

	// An unchanged resubmission edits nothing, so it reports no verdicts.
	if keys := decode(save(mixdrop, true)); len(keys) != 0 {
		t.Fatalf("unchanged save returned verdicts: %+v", keys)
	}

	// Plain (non-fetch) form posts keep the original redirect behaviour.
	w := save(mixdrop, false)
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/" {
		t.Fatalf("form post = %d %q, want 302 to /", w.Code, w.Header().Get("Location"))
	}
}
