package router

import (
	"testing"
)

func TestSTPlayAllowedHost(t *testing.T) {
	cases := map[string]bool{
		"tapecontent.net":                  true,
		"861134100.tapecontent.net":        true,
		"foo.tapecontent.net":              true,
		"api.streamtape.com":               false,
		"streamtape.com":                   false,
		"tapecontent.net.evil.com":         false,
		"notapecontent.net":                false,
		"":                                 false,
		"861134100.tapecontent.net:443":    true,
		"861134100.TAPECONTENT.NET":        true,
	}
	for host, want := range cases {
		if got := stPlayAllowedHost(host); got != want {
			t.Errorf("stPlayAllowedHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestStreamtapeVideoURLRelay(t *testing.T) {
	cases := []struct {
		host, link, want string
	}{
		{"Streamtape", "https://streamtape.com/e/AbCdEf1234/", "/api/play/streamtape/AbCdEf1234"},
		{"Streamtape", "https://streamtape.com/v/AbCdEf1234", "/api/play/streamtape/AbCdEf1234"},
		{"streamtape", "https://streamtape.com/e/AbCdEf1234/", "/api/play/streamtape/AbCdEf1234"},
		{"Streamtape", "", ""},
	}
	for _, cse := range cases {
		if got := videoURLForHostLink(cse.host, cse.link); got != cse.want {
			t.Errorf("videoURLForHostLink(%q,%q) = %q, want %q", cse.host, cse.link, got, cse.want)
		}
		if got := embedURLForHostLink(cse.host, cse.link); cse.want != "" && got != "" {
			t.Errorf("embedURLForHostLink(%q,%q) = %q, want empty (relay path)", cse.host, cse.link, got)
		}
	}
}

func TestIsStreamtapeLink(t *testing.T) {
	cases := map[string]bool{
		"https://streamtape.com/e/AbCdEf1234/":             true,
		"https://streamtape.com/v/AbCdEf1234":              true,
		"STREAMTAPE.COM/e/AbCdEf1234":                      true,
		"https://voe.sx/e/abc":                             false,
		"https://mixdrop.ag/e/abc":                         false,
		"https://example.com/abc":                          false,
		"":                      false,
	}
	for u, want := range cases {
		if got := isStreamtapeLink(u); got != want {
			t.Errorf("isStreamtapeLink(%q) = %v, want %v", u, got, want)
		}
	}
}