package uploader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// swapBase points a probe base var at a fake server and restores it after the
// test.
func swapBase(t *testing.T, target *string, url string) {
	t.Helper()
	old := *target
	*target = url
	t.Cleanup(func() { *target = old })
}

func TestProbeVoeSXClassifiesResults(t *testing.T) {
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		wantOK     bool
		wantReject bool
	}{
		{
			name: "healthy",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"success":true,"status":200,"result":"https://up.example"}`)
			},
			wantOK: true,
		},
		{
			name: "invalid key",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"success":false,"status":403,"msg":"Invalid api key"}`)
			},
			wantReject: true,
		},
		{
			name: "storage full counts as alive",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"success":false,"status":400,"msg":"Not enough storage space"}`)
			},
			wantOK: true,
		},
		{
			name: "host error is undecided",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"error":"maintenance"}`)
			},
			wantOK:     false,
			wantReject: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			swapBase(t, &voeSXAPIBase, srv.URL)

			got := ProbeVoeSX("test-key")
			if got.OK != tc.wantOK {
				t.Errorf("OK = %v, want %v (message: %q)", got.OK, tc.wantOK, got.Message)
			}
			if got.Rejected != tc.wantReject {
				t.Errorf("Rejected = %v, want %v (message: %q)", got.Rejected, tc.wantReject, got.Message)
			}
		})
	}
}

func TestLooksLikeBlockPage(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"plain api rejection", http.StatusForbidden, `{"error":"invalid key"}`, false},
		{"plain 403 word", http.StatusForbidden, `blocked`, false},
		{"cloudflare challenge", http.StatusForbidden, `<html><head><title>Attention Required! | Cloudflare</title></head></html>`, true},
		{"just a moment", http.StatusForbidden, `<!DOCTYPE html><html><title>Just a moment...</title></html>`, true},
		{"html even on 401", http.StatusUnauthorized, `<html>login page</html>`, true},
		{"200 never a block page", http.StatusOK, `<html>weird</html>`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeBlockPage(tc.status, []byte(tc.body)); got != tc.want {
				t.Errorf("looksLikeBlockPage(%d, %q) = %v, want %v", tc.status, tc.body, got, tc.want)
			}
		})
	}
	// authVerdict must downgrade a WAF 403 to undecided but keep a plain 403 as Rejected.
	if r := authVerdict(http.StatusForbidden, []byte(`<html>cf</html>`)); r.OK || r.Rejected {
		t.Errorf("block page must be undecided: %+v", r)
	}
	if r := authVerdict(http.StatusForbidden, []byte(`{"error":"bad key"}`)); !r.Rejected {
		t.Errorf("api 403 must be rejected: %+v", r)
	}
}

func TestProbeVoeSXTransportErrorIsUndecided(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	swapBase(t, &voeSXAPIBase, srv.URL)
	srv.Close() // connection refused

	got := ProbeVoeSX("test-key")
	if got.OK || got.Rejected {
		t.Fatalf("transport error must not decide a key: %+v", got)
	}
	if got.Message == "" {
		t.Fatal("expected an explanatory message")
	}
}

func TestProbeVidaraClassifiesResults(t *testing.T) {
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		wantOK     bool
		wantReject bool
	}{
		{
			name: "healthy",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":200,"result":{"upload_server":"https://up.example"}}`)
			},
			wantOK: true,
		},
		{
			name: "invalid key inside 200 body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":403,"msg":"Invalid api key"}`)
			},
			wantReject: true,
		},
		{
			name: "site error undecided",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":500,"msg":"internal error"}`)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			swapBase(t, &vidaraAPIBase, srv.URL)

			got := ProbeVidara("test-key")
			if got.OK != tc.wantOK || got.Rejected != tc.wantReject {
				t.Errorf("got %+v, want OK=%v Rejected=%v", got, tc.wantOK, tc.wantReject)
			}
		})
	}
}

func TestProbeVidMolyClassifiesResults(t *testing.T) {
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		wantOK     bool
		wantReject bool
	}{
		{
			name: "healthy",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":200,"msg":"ok","result":"https://up.example"}`)
			},
			wantOK: true,
		},
		{
			name: "invalid key",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":403,"msg":"Invalid api key"}`)
			},
			wantReject: true,
		},
		{
			name: "daily limit is a live key",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":429,"msg":"Daily API limit reached (50)."}`)
			},
			wantOK: true,
		},
		{
			name: "server error undecided",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, `oops`)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			swapBase(t, &vidmolyAPIBase, srv.URL)

			got := ProbeVidMoly("test-key")
			if got.OK != tc.wantOK || got.Rejected != tc.wantReject {
				t.Errorf("got %+v, want OK=%v Rejected=%v", got, tc.wantOK, tc.wantReject)
			}
		})
	}
}

func TestProbeStreamtapeClassifiesResults(t *testing.T) {
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		wantOK     bool
		wantReject bool
	}{
		{
			name: "healthy",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("login"); got != "user" {
					t.Errorf("login = %q, want user", got)
				}
				fmt.Fprint(w, `{"status":200,"msg":"ok","result":{"url":"https://ul.example"}}`)
			},
			wantOK: true,
		},
		{
			name: "rejected credentials",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":403,"msg":"Authentication failed"}`)
			},
			wantReject: true,
		},
		{
			name: "http 403 counts as rejected",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `blocked`)
			},
			wantReject: true,
		},
		{
			name: "http 403 WAF block page is undecided",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `<html><head><title>Just a moment...</title></head><body>cf-chl</body></html>`)
			},
		},
		{
			name: "unknown failure undecided",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"status":500,"msg":"temporary"}`)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			swapBase(t, &streamtapeAPIBase, srv.URL)

			got := ProbeStreamtape("user", "key")
			if got.OK != tc.wantOK || got.Rejected != tc.wantReject {
				t.Errorf("got %+v, want OK=%v Rejected=%v", got, tc.wantOK, tc.wantReject)
			}
		})
	}
}
