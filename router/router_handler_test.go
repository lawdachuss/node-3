package router

import "testing"

func TestBuildHostPlayersGeneratesEmbeds(t *testing.T) {
	players := buildHostPlayers(map[string]string{
		"GoFile":     "https://gofile.io/d/example",
		"VOE.sx":     "https://voe.sx/abc123",
		"Streamtape": "https://streamtape.com/e/AbCdEf1234/",
	})

	byHost := map[string]hostPlayer{}
	for _, player := range players {
		byHost[player.Host] = player
	}

	if got := byHost["GoFile"].EmbedURL; got != "" {
		t.Fatalf("GoFile embed URL = %q", got)
	}
	if got := byHost["GoFile"].VideoURL; got != "" {
		t.Fatalf("GoFile video URL = %q", got)
	}
	if got := byHost["VOE.sx"].EmbedURL; got != "https://voe.sx/e/abc123" {
		t.Fatalf("VOE embed URL = %q", got)
	}
	// Streamtape no longer embeds (iframe shows "video not available"); it plays
	// through the same-origin relay instead.
	st := byHost["Streamtape"]
	if st.EmbedURL != "" {
		t.Fatalf("Streamtape embed URL = %q, want empty (relay path)", st.EmbedURL)
	}
	if want := "/api/play/streamtape/AbCdEf1234"; st.VideoURL != want {
		t.Fatalf("Streamtape video URL = %q, want %q", st.VideoURL, want)
	}
}
