package channel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/site"
)

// fakeSite lets tests control what the site-API probe (FetchStream) answers
// when a CDN 403/404 must be disambiguated.
type fakeSite struct {
	fetchErr error
}

func (f *fakeSite) FetchStream(ctx context.Context, req *internal.Req, username string) (*site.StreamInfo, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return &site.StreamInfo{RoomStatus: "public"}, nil
}

func (f *fakeSite) GetRoomStatus(ctx context.Context, req *internal.Req, username string) (string, error) {
	return "public", nil
}

func (f *fakeSite) FetchLastBroadcast(ctx context.Context, req *internal.Req, username string) (int64, error) {
	return 0, nil
}

func (f *fakeSite) FetchProfile(ctx context.Context, req *internal.Req, username string) (*database.ChannelProfile, error) {
	return nil, nil
}

func newResolveChannel() *Channel {
	return &Channel{Config: &entity.ChannelConfig{Username: "alice"}}
}

// TestResolveWatchEnd verifies the WatchSegments error classification: soft
// session deaths (stall, CDN 403/404 with the model still live, or a probe
// that fails without confirming offline) must map to ErrStreamStalled so the
// Monitor reconnects fast, while definitive endings (private show / offline)
// keep the original error so the channel benches for the offline interval.
func TestResolveWatchEnd(t *testing.T) {
	// Mirror the production wrap chain: client.Get wraps "get bytes: %w" and
	// the watch loop wraps "get video playlist: %w", so errors.Is must match
	// the internal sentinels through both layers.
	mediaForbidden := fmt.Errorf("get video playlist: get bytes: %w", fmt.Errorf("forbidden: %w", internal.ErrMediaForbidden))
	notFound := fmt.Errorf("get video playlist: get bytes: %w", internal.ErrNotFound)
	other := errors.New("get video playlist: dial tcp: connection refused")

	cases := []struct {
		name      string
		watchErr  error
		fetchErr  error // fakeSite answer; nil = model still live
		wantErr   error
		wantClose string
	}{
		{
			name:      "stall maps to itself with session-expiry reason",
			watchErr:  internal.ErrStreamStalled,
			wantErr:   internal.ErrStreamStalled,
			wantClose: "stream session expired (no new segments)",
		},
		{
			name:     "canceled passes through with no reason (pause/stop set it)",
			watchErr: context.Canceled,
			wantErr:  context.Canceled,
		},
		{
			name:      "unrelated error ends recording with HLS label",
			watchErr:  other,
			wantErr:   other,
			wantClose: "HLS stream ended: " + other.Error(),
		},
		{
			name:      "403 + probe says still live -> fast reconnect",
			watchErr:  mediaForbidden,
			wantErr:   internal.ErrStreamStalled,
			wantClose: "stream session expired (HLS session/token) — reconnecting",
		},
		{
			name:      "404 + probe says still live -> fast reconnect",
			watchErr:  notFound,
			wantErr:   internal.ErrStreamStalled,
			wantClose: "stream session expired (HLS session/token) — reconnecting",
		},
		{
			name:      "403 + probe says private show -> end with private reason",
			watchErr:  mediaForbidden,
			fetchErr:  fmt.Errorf("forbidden: %w", internal.ErrPrivateStream),
			wantErr:   mediaForbidden,
			wantClose: "channel entered a private show",
		},
		{
			name:      "404 + probe says offline -> end with offline reason",
			watchErr:  notFound,
			fetchErr:  internal.ErrChannelOffline,
			wantErr:   notFound,
			wantClose: "channel went offline",
		},
		{
			name:      "404 + probe says deleted -> end with offline reason",
			watchErr:  notFound,
			fetchErr:  internal.ErrNotFound,
			wantErr:   notFound,
			wantClose: "channel went offline",
		},
		{
			name:     "403 + probe canceled -> pass through, no reason",
			watchErr: mediaForbidden,
			fetchErr: context.Canceled,
			wantErr:  mediaForbidden,
		},
		{
			name:      "403 + probe fails transiently -> fast reconnect, not bench",
			watchErr:  mediaForbidden,
			fetchErr:  errors.New("get room status: unexpected status 502"),
			wantErr:   internal.ErrStreamStalled,
			wantClose: "stream session expired (HLS session/token) — reconnecting (site probe failed: get room status: unexpected status 502)",
		},
		{
			name:      "404 + probe CF-blocked -> fast reconnect, not bench",
			watchErr:  notFound,
			fetchErr:  internal.ErrCloudflareBlocked,
			wantErr:   internal.ErrStreamStalled,
			wantClose: "stream session expired (HLS session/token) — reconnecting (site probe failed: blocked by Cloudflare; try with `-cookies` and `-user-agent`)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch := newResolveChannel()
			s := &fakeSite{fetchErr: c.fetchErr}
			got := ch.resolveWatchEnd(context.Background(), s, internal.NewReq(), c.watchErr)

			if !errors.Is(got, c.wantErr) {
				t.Fatalf("resolveWatchEnd error = %v, want %v", got, c.wantErr)
			}
			if c.wantClose == "" && ch.closeReason != "" {
				t.Fatalf("closeReason = %q, want empty", ch.closeReason)
			}
			if c.wantClose != "" && ch.closeReason != c.wantClose {
				t.Fatalf("closeReason = %q, want %q", ch.closeReason, c.wantClose)
			}
		})
	}
}
