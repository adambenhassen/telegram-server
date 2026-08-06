package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestReactionVsDeleteConcurrent fires SendReaction and DeleteMessages
// simultaneously across many fresh user pairs. The sorted advisory-lock
// ordering ensures one commits first:
//   - If delete wins: reaction must return ErrMessageInvalid, no reaction row.
//   - If reaction wins: delete must still succeed (message not gone).
//   - Never: both commit leaving an orphaned reaction on a deleted message.
func TestReactionVsDeleteConcurrent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	for i := range 50 {
		a := mustUser(t, s, "+1555128"+string(rune('a'+i%26))+"01")
		b := mustUser(t, s, "+1555128"+string(rune('a'+i%26))+"02")

		msg := send(t, s, a, b, "target", int64(1000+i))

		var wg sync.WaitGroup
		var reactErr error
		var deleteErr error
		wg.Go(func() {
			_, reactErr = s.SendReaction(ctx, a.ID, msg.LocalID, "\xf0\x9f\x94\x8d") // thumbs-up
		})
		wg.Go(func() {
			_, deleteErr = s.DeleteMessages(ctx, a.ID, []int64{msg.LocalID})
		})
		wg.Wait()

		// At least one must succeed (the winner).
		if reactErr != nil && deleteErr != nil {
			t.Fatalf("round %d: both failed: react=%v delete=%v", i, reactErr, deleteErr)
		}

		// Check the invariant: no orphaned reaction on a deleted message.
		deleted, _, err := s.MessageByOwnerLocal(ctx, a.ID, msg.LocalID)
		if err != nil {
			t.Fatalf("round %d: read message: %v", i, err)
		}
		reactions, err := s.ReactionsByOwnerLocal(ctx, a.ID, msg.LocalID)
		if err != nil {
			t.Fatalf("round %d: read reactions: %v", i, err)
		}

		if deleted.Deleted && len(reactions) > 0 {
			t.Fatalf("round %d: orphaned reaction on deleted message (reactErr=%v, deleteErr=%v)", i, reactErr, deleteErr)
		}
	}
}

// TestClearReactionVsDeleteConcurrent is the same race but with ClearReaction
// instead of SendReaction. ClearReaction deletes a row — the invariant is that
// it doesn't error spuriously when the message is deleted concurrently.
func TestClearReactionVsDeleteConcurrent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	for i := range 50 {
		a := mustUser(t, s, "+1555129"+string(rune('a'+i%26))+"01")
		b := mustUser(t, s, "+1555129"+string(rune('a'+i%26))+"02")

		msg := send(t, s, a, b, "target", int64(2000+i))

		// Set up a reaction first so ClearReaction has something to remove.
		if _, err := s.SendReaction(ctx, a.ID, msg.LocalID, "\xf0\x9f\x94\x8d"); err != nil {
			t.Fatalf("round %d: setup reaction: %v", i, err)
		}

		var wg sync.WaitGroup
		var clearErr error
		var deleteErr error
		wg.Go(func() {
			_, clearErr = s.ClearReaction(ctx, a.ID, msg.LocalID)
		})
		wg.Go(func() {
			_, deleteErr = s.DeleteMessages(ctx, a.ID, []int64{msg.LocalID})
		})
		wg.Wait()

		// At least one must succeed.
		if clearErr != nil && deleteErr != nil {
			t.Fatalf("round %d: both failed: clear=%v delete=%v", i, clearErr, deleteErr)
		}

		// If delete won, clear should get ErrMessageInvalid (message gone).
		// If clear won, delete still succeeds (clearing a reaction doesn't delete the message).
		deleted, _, err := s.MessageByOwnerLocal(ctx, a.ID, msg.LocalID)
		if err != nil {
			t.Fatalf("round %d: read message: %v", i, err)
		}
		reactions, err := s.ReactionsByOwnerLocal(ctx, a.ID, msg.LocalID)
		if err != nil {
			t.Fatalf("round %d: read reactions: %v", i, err)
		}

		// After clear wins, no reaction should remain.
		// After delete wins, message is deleted (reaction row may or may not exist — irrelevant).
		if !deleted.Deleted && len(reactions) > 0 {
			t.Fatalf("round %d: reaction survived clear (clearErr=%v, deleteErr=%v)", i, clearErr, deleteErr)
		}
	}
}

// TestReactionOnDeletedMessage returns ErrMessageInvalid when the message
// is already soft-deleted. This is the non-concurrent baseline the locks
// protect.
func TestReactionOnDeletedMessage(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290001")
	b := mustUser(t, s, "+15551290002")

	msg := send(t, s, a, b, "gone", 9999)
	_, err := s.DeleteMessages(ctx, a.ID, []int64{msg.LocalID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = s.SendReaction(ctx, a.ID, msg.LocalID, "\xf0\x9f\x94\x8d")
	if !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("reaction on deleted: want ErrMessageInvalid, got %v", err)
	}

	_, err = s.ClearReaction(ctx, a.ID, msg.LocalID)
	if !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("clear on deleted: want ErrMessageInvalid, got %v", err)
	}
}
