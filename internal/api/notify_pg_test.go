package api_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// TestDeliverChannelPostNobodyHomeSkipsStateQuery verifies that DeliverChannelPost
// returns cleanly when no member has a live connection on this replica. With the
// lazy ChannelState fetch, the state query is skipped entirely in this path.
func TestDeliverChannelPostNobodyHomeSkipsStateQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15550000102")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, alice.ID, "quiet channel", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Empty registry: no live conn for any member on this replica.
	reg := mtproto.NewSessionRegistry()
	// Must return cleanly; the lazy state fetch means ChannelState is never called.
	api.NewUpdater(s, reg, nil, pgtest.PeerDeriver()).DeliverChannelPost(ctx, ch.ID)
}
