package store_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5"
)

// TestSendReactionBlockedByDeleteLock holds the per-owner advisory locks from
// a raw transaction, soft-deletes the message, then starts SendReaction in a
// goroutine. SendReaction blocks at lockOwners until the holding tx commits,
// then reloads the message and returns ErrMessageInvalid.
func TestSendReactionBlockedByDeleteLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551300001")
	b := mustUser(t, s, "+15551300002")

	msg := send(t, s, a, b, "target", 1)

	// Open a raw connection and hold the advisory locks that SendReaction
	// will try to acquire (lockOwners sorts ascending).
	pool := store.StorePool(s)
	raw, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer raw.Release()
	tx, err := raw.Conn().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck

	ids := []int64{a.ID, b.ID}
	slices.Sort(ids)
	for _, id := range ids {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", id); err != nil {
			t.Fatalf("lock %d: %v", id, err)
		}
	}

	// Soft-delete the message under the lock.
	if _, err := tx.Exec(ctx, "UPDATE messages SET deleted=true WHERE owner_id=$1 AND local_id=$2", a.ID, msg.LocalID); err != nil {
		t.Fatalf("soft-delete: %v", err)
	}

	// Fire SendReaction in a goroutine — it blocks at lockOwners.
	var reactErr error
	done := make(chan struct{})
	go func() {
		_, reactErr = s.SendReaction(ctx, a.ID, msg.LocalID, "\xf0\x9f\x94\x8d")
		close(done)
	}()

	// Wait for the goroutine to reach the advisory lock.
	if err := store.WaitForLockWaiters(ctx, s, 1); err != nil {
		t.Fatalf("goroutine never reached advisory lock: %v", err)
	}

	// Commit the delete — this releases the advisory lock.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit delete: %v", err)
	}

	// SendReaction should now unblock, reload, see deleted=true, and fail.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SendReaction did not return after delete committed")
	}

	if !errors.Is(reactErr, store.ErrMessageInvalid) {
		t.Fatalf("SendReaction: want ErrMessageInvalid, got %v", reactErr)
	}

	// No reaction row should exist.
	reactions, err := s.ReactionsByOwnerLocal(ctx, a.ID, msg.LocalID)
	if err != nil {
		t.Fatalf("read reactions: %v", err)
	}
	if len(reactions) > 0 {
		t.Fatalf("reaction persisted after delete: %+v", reactions)
	}
}

// TestClearReactionBlockedByDeleteLock is the same deterministic pattern for
// ClearReaction: hold the delete lock, soft-delete, then ClearReaction blocks
// and returns ErrMessageInvalid when it unblocks.
func TestClearReactionBlockedByDeleteLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551300011")
	b := mustUser(t, s, "+15551300012")

	msg := send(t, s, a, b, "target", 2)

	// Pre-seed a reaction so ClearReaction has something to clear.
	if _, err := s.SendReaction(ctx, a.ID, msg.LocalID, "\xf0\x9f\x94\x8d"); err != nil {
		t.Fatalf("setup reaction: %v", err)
	}

	// Hold advisory locks and soft-delete.
	pool := store.StorePool(s)
	raw, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer raw.Release()
	tx, err := raw.Conn().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck

	ids := []int64{a.ID, b.ID}
	slices.Sort(ids)
	for _, id := range ids {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", id); err != nil {
			t.Fatalf("lock %d: %v", id, err)
		}
	}

	if _, err := tx.Exec(ctx, "UPDATE messages SET deleted=true WHERE owner_id=$1 AND local_id=$2", a.ID, msg.LocalID); err != nil {
		t.Fatalf("soft-delete: %v", err)
	}

	// Fire ClearReaction — blocks at lockOwners.
	var clearErr error
	done := make(chan struct{})
	go func() {
		_, clearErr = s.ClearReaction(ctx, a.ID, msg.LocalID)
		close(done)
	}()

	// Wait for the goroutine to reach the advisory lock.
	if err := store.WaitForLockWaiters(ctx, s, 1); err != nil {
		t.Fatalf("goroutine never reached advisory lock: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit delete: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ClearReaction did not return after delete committed")
	}

	if !errors.Is(clearErr, store.ErrMessageInvalid) {
		t.Fatalf("ClearReaction: want ErrMessageInvalid, got %v", clearErr)
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
	_, err := s.DeleteMessages(ctx, a.ID, []int64{msg.LocalID}, true)
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
