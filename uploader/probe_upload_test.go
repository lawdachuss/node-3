package uploader

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// readProbeUpload parses a multipart upload and returns the uploaded bytes,
// asserting that the file part carries the recognizable probe clip.
func readProbeUpload(t *testing.T, r *http.Request, field string) []byte {
	t.Helper()
	if err := r.ParseMultipartForm(4 << 20); err != nil {
		t.Errorf("multipart form: %v", err)
		return nil
	}
	f, hdr, err := r.FormFile(field)
	if err != nil {
		t.Errorf("file part %q: %v", field, err)
		return nil
	}
	defer f.Close()
	if !strings.Contains(hdr.Filename, "uploadkey-probe") {
		t.Errorf("uploaded filename = %q, want the recognizable probe name", hdr.Filename)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("read uploaded file: %v", err)
	}
	return data
}

// TestVerifyVoeSXUploadRoundTrip pins the core contract: a healthy verdict is
// only awarded after the real upload endpoint accepted the probe file, and the
// probe file is deleted again afterwards.
func TestVerifyVoeSXUploadRoundTrip(t *testing.T) {
	var uploads, deletes atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/upload/server":
			fmt.Fprintf(w, `{"success":true,"status":200,"result":"%s/up"}`, srv.URL)
		case "/up":
			data := readProbeUpload(t, r, "file")
			uploads.Add(1)
			if r.FormValue("key") != "probe-key" {
				t.Errorf("key field = %q, want probe-key", r.FormValue("key"))
			}
			if len(data) < 1000 {
				t.Errorf("uploaded probe is %d bytes, want the embedded clip", len(data))
			}
			fmt.Fprint(w, `{"success":true,"message":"File has been uploaded successfully.","file":{"file_code":"probeCode1"}}`)
		case "/file/delete":
			deletes.Add(1)
			if r.URL.Query().Get("del_code") != "probeCode1" {
				t.Errorf("del_code = %q, want probeCode1", r.URL.Query().Get("del_code"))
			}
			fmt.Fprint(w, `{"success":true,"status":200}`)
		default:
			t.Errorf("unexpected probe path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	swapBase(t, &voeSXAPIBase, srv.URL)

	res := VerifyVoeSX("probe-key")
	if !res.OK || res.Rejected || res.Quota {
		t.Fatalf("res = %+v, want a healthy upload verdict", res)
	}
	if res.Message == "" {
		t.Error("a healthy verdict should describe the verified upload")
	}
	if uploads.Load() != 1 || deletes.Load() != 1 {
		t.Errorf("uploads=%d deletes=%d, want 1/1", uploads.Load(), deletes.Load())
	}
}

// A key that authenticates but whose upload endpoint refuses it must be dead,
// with the host's own wording surfaced — this is the exact hole that let the
// admin page show healthy keys while every real upload failed.
func TestVerifyVoeSXRejectsWhenUploadRefuses(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/upload/server":
			fmt.Fprintf(w, `{"success":true,"status":200,"result":"%s/up"}`, srv.URL)
		case "/up":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"success":false,"message":"Invalid api key"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	swapBase(t, &voeSXAPIBase, srv.URL)

	res := VerifyVoeSX("bad-key")
	if res.OK || !res.Rejected || res.Quota {
		t.Fatalf("res = %+v, want rejected", res)
	}
	if !strings.Contains(res.Message, "Invalid api key") {
		t.Errorf("message = %q, want the host's rejection wording", res.Message)
	}
}

// A WAF block on the upload POST is not the API rejecting the key: it must stay
// undecided so a Cloudflare episode cannot flip every good key to dead.
func TestVerifyVoeSXUploadBlockPageIsUndecided(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/upload/server":
			fmt.Fprintf(w, `{"success":true,"status":200,"result":"%s/up"}`, srv.URL)
		case "/up":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<html><head><title>Just a moment...</title></head></html>`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	swapBase(t, &voeSXAPIBase, srv.URL)

	res := VerifyVoeSX("good-key")
	if res.OK || res.Rejected || res.Quota {
		t.Fatalf("a block page must stay undecided: %+v", res)
	}
}

// A storage-full account is a quota verdict and must not spend an upload.
func TestVerifyVoeSXStorageFullDoesNotUpload(t *testing.T) {
	var uploads atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/upload/server":
			fmt.Fprint(w, `{"success":false,"status":400,"msg":"Not enough storage space"}`)
		case "/up":
			uploads.Add(1)
			fmt.Fprint(w, `{"success":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	swapBase(t, &voeSXAPIBase, srv.URL)

	res := VerifyVoeSX("full-key")
	if res.OK || res.Rejected || !res.Quota {
		t.Fatalf("res = %+v, want quota", res)
	}
	if uploads.Load() != 0 {
		t.Errorf("uploads = %d, want 0 (a full account must not be uploaded to)", uploads.Load())
	}
}

func TestVerifyVidaraUploadRoundTripAndQuota(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		var uploads, deletes atomic.Int32
		var srv *httptest.Server
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/upload/server":
				fmt.Fprintf(w, `{"msg":"OK","status":200,"result":{"upload_server":"%s/up"}}`, srv.URL)
			case "/up":
				data := readProbeUpload(t, r, "file")
				uploads.Add(1)
				if r.FormValue("api_key") != "probe-key" {
					t.Errorf("api_key field = %q, want probe-key", r.FormValue("api_key"))
				}
				if len(data) < 1000 {
					t.Errorf("uploaded probe is %d bytes", len(data))
				}
				fmt.Fprint(w, `{"filecode":"https://vidara.to/e/CODE123","video_id":1,"title":"probe"}`)
			case "/video/delete":
				deletes.Add(1)
				if r.URL.Query().Get("filecode") != "CODE123" {
					t.Errorf("filecode = %q, want CODE123", r.URL.Query().Get("filecode"))
				}
				fmt.Fprint(w, `{"msg":"OK","status":200,"result":{"filecode":"CODE123","deleted":true}}`)
			default:
				t.Errorf("unexpected probe path %s", r.URL.Path)
			}
		}))
		defer srv.Close()
		swapBase(t, &vidaraAPIBase, srv.URL)

		res := VerifyVidara("probe-key")
		if !res.OK || res.Rejected || res.Quota {
			t.Fatalf("res = %+v, want healthy", res)
		}
		if uploads.Load() != 1 || deletes.Load() != 1 {
			t.Errorf("uploads=%d deletes=%d, want 1/1", uploads.Load(), deletes.Load())
		}
	})

	t.Run("daily limit is quota", func(t *testing.T) {
		var srv *httptest.Server
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/upload/server":
				fmt.Fprintf(w, `{"msg":"OK","status":200,"result":{"upload_server":"%s/up"}}`, srv.URL)
			case "/up":
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"status":429,"msg":"Daily upload limit reached"}`)
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()
		swapBase(t, &vidaraAPIBase, srv.URL)

		res := VerifyVidara("capped-key")
		if res.OK || res.Rejected || !res.Quota {
			t.Fatalf("res = %+v, want quota", res)
		}
	})
}

func TestVerifyVidMolyUploadRoundTrip(t *testing.T) {
	var uploads, deletes atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/upload/server":
			fmt.Fprintf(w, `{"status":200,"msg":"ok","result":"%s/up"}`, srv.URL)
		case "/up":
			data := readProbeUpload(t, r, "file")
			uploads.Add(1)
			if len(data) < 1000 {
				t.Errorf("uploaded probe is %d bytes", len(data))
			}
			fmt.Fprint(w, `<Form name='F1'><textarea name="op">upload_result</textarea><textarea name="fn">CODE123</textarea><textarea name="st">OK</textarea></Form>`)
		case "/api/file/delete":
			deletes.Add(1)
			if r.URL.Query().Get("file_code") != "CODE123" {
				t.Errorf("file_code = %q, want CODE123", r.URL.Query().Get("file_code"))
			}
			fmt.Fprint(w, `{"status":200,"msg":"OK"}`)
		default:
			t.Errorf("unexpected probe path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	swapBase(t, &vidmolyAPIBase, srv.URL)

	res := VerifyVidMoly("probe-key")
	if !res.OK || res.Rejected || res.Quota {
		t.Fatalf("res = %+v, want healthy", res)
	}
	if uploads.Load() != 1 || deletes.Load() != 1 {
		t.Errorf("uploads=%d deletes=%d, want 1/1", uploads.Load(), deletes.Load())
	}
}

func TestVerifyVidMolyDailyLimitIsQuota(t *testing.T) {
	var uploads atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/upload/server":
			fmt.Fprint(w, `{"status":429,"msg":"Daily API limit reached (50)."}`)
		case "/up":
			uploads.Add(1)
			fmt.Fprint(w, `whatever`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	swapBase(t, &vidmolyAPIBase, srv.URL)

	res := VerifyVidMoly("capped-key")
	if res.OK || res.Rejected || !res.Quota {
		t.Fatalf("res = %+v, want quota", res)
	}
	if uploads.Load() != 0 {
		t.Errorf("uploads = %d, want 0 (a capped account must not be uploaded to)", uploads.Load())
	}
}

func TestVerifyStreamtapeUploadRoundTrip(t *testing.T) {
	var uploads, deletes atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/ul":
			if r.URL.Query().Get("login") != "user" || r.URL.Query().Get("key") != "probe-key" {
				t.Errorf("credential query = %s", r.URL.RawQuery)
			}
			fmt.Fprintf(w, `{"status":200,"msg":"OK","result":{"url":"%s/up"}}`, srv.URL)
		case "/up":
			data := readProbeUpload(t, r, "file")
			uploads.Add(1)
			if len(data) < 1000 {
				t.Errorf("uploaded probe is %d bytes", len(data))
			}
			fmt.Fprint(w, `{"status":200,"msg":"Upload finished!","result":{"id":"FILEID","url":"https://streamtape.com/v/FILEID/probe.mp4"}}`)
		case "/file/delete":
			deletes.Add(1)
			if r.URL.Query().Get("file") != "FILEID" {
				t.Errorf("file = %q, want FILEID", r.URL.Query().Get("file"))
			}
			fmt.Fprint(w, `{"status":200,"msg":"OK","result":true}`)
		default:
			t.Errorf("unexpected probe path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	swapBase(t, &streamtapeAPIBase, srv.URL)

	res := VerifyStreamtape("user", "probe-key")
	if !res.OK || res.Rejected || res.Quota {
		t.Fatalf("res = %+v, want healthy", res)
	}
	if uploads.Load() != 1 || deletes.Load() != 1 {
		t.Errorf("uploads=%d deletes=%d, want 1/1", uploads.Load(), deletes.Load())
	}
}

// The upload step — not the cheap account GET — decides rejection: a key whose
// /file/ul lookup succeeds but whose upload POST returns authentication failure
// must be reported dead with the host's wording.
func TestVerifyStreamtapeRejectsWhenUploadAuthFails(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/ul":
			fmt.Fprintf(w, `{"status":200,"msg":"OK","result":{"url":"%s/up"}}`, srv.URL)
		case "/up":
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `Authentication failed`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	swapBase(t, &streamtapeAPIBase, srv.URL)

	res := VerifyStreamtape("user", "bad-key")
	if res.OK || !res.Rejected || res.Quota {
		t.Fatalf("res = %+v, want rejected", res)
	}
	if !strings.Contains(res.Message, "Authentication failed") {
		t.Errorf("message = %q, want the host's rejection wording", res.Message)
	}
}
