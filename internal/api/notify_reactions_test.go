package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// A reaction on a copy its owner has soft-deleted must not be pushed to that
// owner: the update names a message gone from every read surface. Deletion is
// per-copy, so the party who kept their copy still gets the push, and the
// reaction row itself is untouched either way.
func TestDeliverReactionsSkipsDeletedCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	p := seedReactionPair(t, s, "+15551352001", "+15551352002", "")

	// B deletes their copy, A keeps theirs.
	if _, err := s.DeleteMessages(ctx, p.b.ID, []int64{p.bLocalID}, false); err != nil {
		t.Fatalf("delete B copy: %v", err)
	}

	// A reacts on the copy they still hold. The write reaches both copies,
	// including B's deleted one — this ticket changes push, not storage.
	targets, err := s.SendReaction(ctx, p.a.ID, p.aLocalID, "❤")
	if err != nil {
		t.Fatalf("react: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("reaction targets = %d, want 2", len(targets))
	}
	stored, err := s.ReactionsByOwnerLocal(ctx, p.b.ID, p.bLocalID)
	if err != nil {
		t.Fatalf("reactions on B copy: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored reactions on deleted copy = %d, want 1", len(stored))
	}

	aConn, aFT := newConnFor(t, reg, p.a.ID)
	_, bFT := newConnFor(t, reg, p.b.ID)

	delivered := make(chan struct{}, len(targets))
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(pushCtx context.Context, ownerID, localID, userID int64) {
			updater.DeliverReactions(pushCtx, ownerID, localID, userID)
			delivered <- struct{}{}
		},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start reaction listener: %v", err)
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("stop reaction listener: %v", err)
		}
	})
	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for reaction listener: %v", err)
	}

	// The reaction is committed; the originating RPC closes before its nudges.
	rpcCtx, cancelRPC := context.WithCancel(ctx)
	cancelRPC()
	for _, target := range targets {
		if err := s.Notify(rpcCtx, store.ChannelReactions, store.ReactionPayload(target.OwnerID, target.LocalID, target.OwnerID)); err != nil {
			t.Fatalf("notify reaction after cancellation: %v", err)
		}
	}
	for range targets {
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			t.Fatal("reaction notification was not delivered")
		}
	}

	if bFT.wasSent() {
		t.Error("B received a reaction push for a message B deleted")
	}
	if !aFT.wasSent() {
		t.Error("A received no reaction push — B's delete must not suppress A's")
	}
	// Reactions stay a zero-pts transient push for the recipient who still gets one.
	if got := aConn.LastPushedPts(); got != 0 {
		t.Errorf("A pts = %d, want 0", got)
	}
}
