package stripchat

import "testing"

// TestParsePKeyFromMasterPrefersKnown verifies that a pkey present in the
// knownPDKeys table is preferred over the first-listed pkey. Stripchat's
// master playlist lists many decoy pkeys; the first line only happens to be
// a known one today because "1" sorts before letters — that must not be
// relied on.
func TestParsePKeyFromMasterPrefersKnown(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "live playlist shape (known pkey first)",
			body: "#EXTM3U\n#EXT-X-MOUFLON:PSCH:v2:1Dzcc6OjP73LKbtI\n#EXT-X-MOUFLON:PSCH:v2:7uUnbD0jMCB9GH32\n#EXT-X-STREAM-INF:BANDWIDTH=1",
			want: "1Dzcc6OjP73LKbtI",
		},
		{
			name: "known pkey buried among decoys",
			body: "#EXTM3U\n#EXT-X-MOUFLON:PSCH:v2:7uUnbD0jMCB9GH32\n#EXT-X-MOUFLON:PSCH:v2:Ook7quaiNgiyuhai\n#EXT-X-MOUFLON:PSCH:v2:qjfc0a2Mi4ngsau6",
			want: "Ook7quaiNgiyuhai",
		},
		{
			name: "no known pkey falls back to first",
			body: "#EXTM3U\n#EXT-X-MOUFLON:PSCH:v2:7uUnbD0jMCB9GH32\n#EXT-X-MOUFLON:PSCH:v2:qjfc0a2Mi4ngsau6",
			want: "7uUnbD0jMCB9GH32",
		},
		{
			name: "no pkey at all",
			body: "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1",
			want: "",
		},
	}
	for _, c := range cases {
		if got := ParsePKeyFromMaster(c.body); got != c.want {
			t.Errorf("%s: ParsePKeyFromMaster() = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestDecryptMouflonURILiveVector pins the MOUFLON v2 algorithm and the
// hardcoded pdkey for pkey 1Dzcc6OjP73LKbtI against a real segment URI
// captured from the live Stripchat CDN on 2026-10-17. If Stripchat changes
// the algorithm or rotates this key, this test fails and flags the need to
// refresh knownPDKeys.
func TestDecryptMouflonURILiveVector(t *testing.T) {
	uri := "https://media-hls.doppiocdn.net/b-hls-28/270762678/270762678_522_wOlYdgG2XMqNJxGZzDL/jf_1792260329_part1.mp4"
	want := "https://media-hls.doppiocdn.net/b-hls-28/270762678/270762678_522_Mq4zph25zA6ER1ad_1792260329_part1.mp4"

	got, err := DecryptMouflonURI(uri, knownPDKeys["1Dzcc6OjP73LKbtI"])
	if err != nil {
		t.Fatalf("DecryptMouflonURI failed: %v", err)
	}
	if got != want {
		t.Fatalf("DecryptMouflonURI() = %q, want %q", got, want)
	}
}

// TestDecryptMouflonURITokenAcrossSlash documents that the captured live
// token contains the base64 '/' character and that the reToken pattern
// consumes everything between the numeric fields.
func TestDecryptMouflonURITokenAcrossSlash(t *testing.T) {
	uri := "https://media-hls.doppiocdn.net/b-hls-28/270762678/270762678_522_wOlYdgG2XMqNJxGZzDL/jf_1792260329_part1.mp4"
	if m := reToken.FindStringSubmatch(uri); m == nil || m[2] != "wOlYdgG2XMqNJxGZzDL/jf" {
		t.Fatalf("reToken did not consume slash-containing token, match=%v", m)
	}
}
