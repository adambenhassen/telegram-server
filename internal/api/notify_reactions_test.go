package api_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// A reaction on a copy its owner has soft-deleted must not be pushed to that
// owner: the update names a message gone from every read surface. Deletion is
// per-copy, so the party who kept their copy still gets the push, and the
// reaction row itself is untouched either way.
func TestDeliverReactionsSkipsDeletedCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
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

	// DeliverReactions is synchronous, so both pushes have either happened or
	// not by the time the loop returns.
	for _, target := range targets {
		updater.DeliverReactions(ctx, target.OwnerID, target.LocalID, target.OwnerID)
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
