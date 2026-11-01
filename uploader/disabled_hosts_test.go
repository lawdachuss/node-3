package uploader

import "testing"

// TestSetDisabledHostsExcludesHostsFromAvailable is a regression test for the
// DISABLED_UPLOAD_HOSTS deadlist feature: a host listed as disabled must never
// be registered in the uploader's host set, so it is neither attempted on
// upload nor required by IsAlreadyFullyUploaded/configuredUploadHosts (both of
// which delegate to AvailableHosts).
func TestSetDisabledHostsExcludesHostsFromAvailable(t *testing.T) {
	old := globallyDisabled
	globallyDisabled = nil
	defer func() { globallyDisabled = old }()

	SetDisabledHosts([]string{"AnonMP4", "Vidara", "Streamtape"})
	defer SetDisabledHosts(nil)

	upl := NewMultiHostUploader("voe", "st-user", "st-key", "mix@a.c", "mix-tok", "vid-key", nil)

	hosts := map[string]bool{}
	for _, h := range upl.AvailableHosts() {
		hosts[h] = true
	}

	for _, disabled := range []string{"AnonMP4", "Vidara", "Streamtape"} {
		if hosts[disabled] {
			t.Errorf("disabled host %q still present in AvailableHosts(): %v", disabled, upl.AvailableHosts())
		}
	}
	for _, enabled := range []string{"GoFile", "VOE.sx", "Mixdrop"} {
		if !hosts[enabled] {
			t.Errorf("expected enabled host %q missing from AvailableHosts(): %v", enabled, upl.AvailableHosts())
		}
	}
}
