package uploader

import "testing"

func TestZipStreamtapeCreds(t *testing.T) {
	cases := []struct {
		login, key string
		want       string
	}{
		{"l1", "k1", "l1:k1"},
		{"l1,l2", "k1,k2", "l1:k1,l2:k2"},
		{"l1, l2", " k1 , k2 ", "l1:k1,l2:k2"},
		{"l1,l2", "k1", "l1:k1"},            // unmatched extra login dropped
		{"l1", "k1,k2", "l1:k1"},            // unmatched extra key dropped
		{"", "", ""},                        // nothing configured
		{"l1", "", ""},                      // half a pair is not a credential
		{" , ", " , ", ""},                  // whitespace-only entries dropped
		{"l1, ,l2", "k1,k2", "l1:k1,l2:k2"}, // empty middle entry dropped
	}
	for _, tc := range cases {
		got := zipStreamtapeCreds(tc.login, tc.key)
		if got != tc.want {
			t.Errorf("zipStreamtapeCreds(%q, %q) = %q, want %q", tc.login, tc.key, got, tc.want)
		}
	}
}

func TestBuildStreamtapeCredListPrefersEnvPool(t *testing.T) {
	t.Setenv("STREAMTAPE_CREDS", "e1:p1,e2:p2")
	if got := buildStreamtapeCredList("l1", "k1"); got != "e1:p1,e2:p2" {
		t.Fatalf("env pool should win, got %q", got)
	}

	t.Setenv("STREAMTAPE_CREDS", "")
	if got := buildStreamtapeCredList("l1,l2", "k1,k2"); got != "l1:k1,l2:k2" {
		t.Fatalf("zipped caller creds, got %q", got)
	}
	if got := buildStreamtapeCredList("l1", "k1"); got != "l1:k1" {
		t.Fatalf("single pair, got %q", got)
	}
}

// TestNewStreamtapeUploaderAcceptsPairedLists verifies the constructor accepts
// comma-separated login/key lists from the central store and zips them into a
// rotating ring of accounts.
func TestNewStreamtapeUploaderAcceptsPairedLists(t *testing.T) {
	t.Setenv("STREAMTAPE_CREDS", "")
	u := NewStreamtapeUploader("l1,l2", "k1,k2")
	if u.creds.count() != 2 {
		t.Fatalf("count = %d, want 2", u.creds.count())
	}
	if l, k, ok := splitStreamtapeCred(u.creds.current()); !ok || l != "l1" || k != "k1" {
		t.Fatalf("current = (%q,%q,%v), want l1/k1/true", l, k, ok)
	}
	u.creds.rotate()
	if l, k, ok := splitStreamtapeCred(u.creds.current()); !ok || l != "l2" || k != "k2" {
		t.Fatalf("after rotate = (%q,%q,%v), want l2/k2/true", l, k, ok)
	}
}
