package uploader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// imgbbRateLimit400 is the exact body ImgBB returns once the free-tier bucket
// is exhausted — HTTP 400 (NOT 429) with "Rate limit reached." (captured live
// across the fleet, 2026-10-17 … 2026-10-20).
const imgbbRateLimit400 = `{"status_code":400,"error":{"message":"Rate limit reached.","code":100}}`

func imgbbTempImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "thumb.jpg")
	if err := os.WriteFile(path, []byte("fake-image-bytes"), 0o644); err != nil {
		t.Fatalf("write temp image: %v", err)
	}
	return path
}

// newImgBBTestUploader builds an uploader against a fake ImgBB with the given
// keys, shrinking every pacing interval so the test stays fast.  The handler
// factory receives the key-seen recorder so tests can inspect which keys were
// contacted.  All overridden package state is restored on cleanup.
func newImgBBTestUploader(t *testing.T, keys []string, handlerFactory func(seen *[]string) http.HandlerFunc) (*ImgBBUploader, *[]string) {
	t.Helper()

	var seen []string
	srv := httptest.NewServer(handlerFactory(&seen))
	t.Cleanup(srv.Close)

	oldURL, oldMin, oldCool, oldDur := imgbbAPIURL, imgbbMinInterval, imgbbCooldownInterval, imgbbKeyCooldownDuration
	imgbbAPIURL = srv.URL
	imgbbMinInterval = time.Millisecond
	imgbbCooldownInterval = 2 * time.Millisecond
	imgbbKeyCooldownDuration = 24 * imgbbCooldownInterval
	imgbbMu.Lock()
	imgbbLastUpload = time.Time{}
	imgbbBackoffUntil = time.Time{}
	imgbbMu.Unlock()
	clearImgBBKeyCooldownForTest()
	t.Cleanup(func() {
		imgbbAPIURL, imgbbMinInterval, imgbbCooldownInterval, imgbbKeyCooldownDuration = oldURL, oldMin, oldCool, oldDur
		imgbbMu.Lock()
		imgbbLastUpload = time.Time{}
		imgbbBackoffUntil = time.Time{}
		imgbbMu.Unlock()
		clearImgBBKeyCooldownForTest()
	})

	u := &ImgBBUploader{
		keys:   &imgbbKeyRing{keys: keys},
		client: srv.Client(),
	}
	return u, &seen
}

// imgbbOKHandler answers 200 with a URL for every request, recording keys.
func imgbbOKHandler(seen *[]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		key := r.FormValue("key")
		*seen = append(*seen, key)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"url":"https://i.imgur.com/ok.jpg"},"status":200}`)
	}
}

// TestImgBBRateLimitedKeyRotatesAndEntersCooldown pins the C1/H4 contract on
// the live-observed shape: key1 answers 400 "Rate limit reached." → the SAME
// Upload attempt immediately retries on key2 (no retry-later needed), and
// key1 enters per-key cooldown so the next call doesn't start on it.
func TestImgBBRateLimitedKeyRotatesAndEntersCooldown(t *testing.T) {
	u, seen := newImgBBTestUploader(t, []string{"key1", "key2"}, func(seen *[]string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			key := r.FormValue("key")
			*seen = append(*seen, key)
			w.Header().Set("Content-Type", "application/json")
			if key == "key1" {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, imgbbRateLimit400)
				return
			}
			fmt.Fprint(w, `{"data":{"url":"https://i.imgur.com/ok.jpg"},"status":200}`)
		}
	})

	url, err := u.Upload(imgbbTempImage(t))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if url != "https://i.imgur.com/ok.jpg" {
		t.Errorf("url = %q, want the key2 success URL", url)
	}
	if got := strings.Join(*seen, ","); got != "key1,key2" {
		t.Errorf("server saw keys [%s], want key1 then key2 within the SAME attempt", got)
	}
	if !imgbbKeyCooled("key1") {
		t.Error("key1 must enter per-key cooldown after a rate-limit 400")
	}
	if imgbbKeyCooled("key2") {
		t.Error("key2 succeeded and must NOT be cooling down")
	}
}

// TestImgBBCooledKeyIsSkippedWithoutBurningAnAttempt: a key already inside its
// cooldown window is never contacted — the attempt goes straight to the usable
// key.
func TestImgBBCooledKeyIsSkippedWithoutBurningAnAttempt(t *testing.T) {
	u, seen := newImgBBTestUploader(t, []string{"key1", "key2"}, imgbbOKHandler)
	coolImgBBKey("key1") // pre-cooled: no request should ever carry key1

	url, err := u.Upload(imgbbTempImage(t))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if url == "" {
		t.Fatal("expected a URL from the usable key")
	}
	if got := strings.Join(*seen, ","); got != "key2" {
		t.Errorf("server saw keys [%s], want only key2 (key1 cooled)", got)
	}
}

// TestImgBBNonRateLimit400FailsFastWithNoCooldown is the Review-Focus guard: a
// 400 that is NOT a rate limit ("invalid key") must fail fast on that key —
// no rotation, no cooldown entry, no silent skip of a bad configuration.
func TestImgBBNonRateLimit400FailsFastWithNoCooldown(t *testing.T) {
	u, seen := newImgBBTestUploader(t, []string{"key1", "key2"}, func(seen *[]string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			*seen = append(*seen, r.FormValue("key"))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"status_code":400,"error":{"message":"invalid key","code":100}}`)
		}
	})

	_, err := u.Upload(imgbbTempImage(t))
	if err == nil {
		t.Fatal("expected an error for a non-rate-limit 400")
	}
	if !strings.Contains(err.Error(), "invalid key") {
		t.Errorf("error should preserve ImgBB's wording, got: %v", err)
	}
	if len(*seen) != 1 || (*seen)[0] != "key1" {
		t.Errorf("server saw %v, want exactly one request on key1 (no rotation on a config error)", *seen)
	}
	if imgbbKeyCooled("key1") || imgbbKeyCooled("key2") {
		t.Error("a non-rate-limit 400 must not create cooldown entries")
	}
}

// TestImgBBAllKeysCoolingDownIsACleanError: every key inside its cooldown
// window → a clear "cooling down" error, no request, no panic.
func TestImgBBAllKeysCoolingDownIsACleanError(t *testing.T) {
	u, seen := newImgBBTestUploader(t, []string{"key1", "key2"}, imgbbOKHandler)
	coolImgBBKey("key1")
	coolImgBBKey("key2")

	_, err := u.Upload(imgbbTempImage(t))
	if err == nil {
		t.Fatal("expected an error when every key is cooling down")
	}
	if !strings.Contains(err.Error(), "cooling down") {
		t.Errorf("error should say the keys are cooling down, got: %v", err)
	}
	if len(*seen) != 0 {
		t.Errorf("server saw %v, want zero requests", *seen)
	}
}

// TestImgBBKeyCooldownExpires recovers on its own: past the window the key
// re-enters rotation without a restart.
func TestImgBBKeyCooldownExpires(t *testing.T) {
	_, _ = newImgBBTestUploader(t, []string{"key1"}, imgbbOKHandler)
	coolImgBBKey("key1")
	if !imgbbKeyCooled("key1") {
		t.Fatal("key1 should be cooling down immediately")
	}
	time.Sleep(imgbbKeyCooldownDuration + 2*time.Millisecond)
	if imgbbKeyCooled("key1") {
		t.Error("key1 must be usable again after its cooldown expires")
	}
}
