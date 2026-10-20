package database

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Plan D3: classify the PostgREST schema-cache transient failures in one
// place and make the failure identify its write. See
// docs/superpowers/plans/2026-10-17-fleet-remediation.md Task D3.

func TestIsSchemaCacheStaleClassifiesKnownCodes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "PGRST002 503",
			body: `{"code":"PGRST002","message":"Schema cache is being rebuilt, try again a bit later"}`,
			want: "PGRST002",
		},
		{
			name: "PGRST204 400",
			body: `{"code":"PGRST204","message":"Could not find the 'upload_state' column of 'channel_logs' in the schema cache"}`,
			want: "PGRST204",
		},
		{
			name: "42P10 400",
			body: `{"code":"42P10","message":"there is no unique or exclusion constraint matching the ON CONFLICT specification"}`,
			want: "42P10",
		},
		{
			name: "plain 400 without markers",
			body: `{"message":"invalid input syntax for type integer: \"abc\""}`,
			want: "",
		},
		{
			name: "empty body",
			body: "",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSchemaCacheStale(tc.body); got != tc.want {
				t.Fatalf("IsSchemaCacheStale(%q) = %q, want %q", tc.body, got, tc.want)
			}
		})
	}
}

func TestRestTableNamesTheWrite(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"/channel_logs?select=id&limit=1", "channel_logs"},
		{"/app_settings?apikey=eq.dvr_enabled", "app_settings"},
		{"/rpc/save_recording_with_links", "rpc/save_recording_with_links"},
		{"/recordings", "recordings"},
	}
	for _, tc := range cases {
		if got := RestTable(tc.path); got != tc.want {
			t.Errorf("RestTable(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

// The harness case: a write that keeps hitting PGRST204 until retries are
// exhausted must surface the method and table, not just "HTTP 400".
// maxRetries=1 keeps the test at a single backoff sleep (~2s).
func TestSchemaCacheExhaustedRetriesNameFailingWrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		fmt.Fprint(w, `{"code":"PGRST204","message":"Could not find the 'upload_state' column of 'channel_logs' in the schema cache"}`)
	}))
	defer srv.Close()

	c := &Client{URL: srv.URL, APIKey: "test-key", client: srv.Client()}
	_, err := c.requestWithRetryN("PATCH", "/channel_logs?select=upload_state", nil, 1)
	if err == nil {
		t.Fatal("expected an error after exhausting retries against a permanent PGRST204")
	}
	for _, want := range []string{"PGRST204", "PATCH", "channel_logs"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("exhausted-retries error must carry %q; got: %v", want, err)
		}
	}
}
