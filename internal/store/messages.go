package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrMessageInvalid is returned when an edit/delete targets a message the caller
// does not own or that does not exist.
var ErrMessageInvalid = errors.New("message id invalid")

// Message is a persisted message row (one side of a two-sided pair).
type Message struct {
	OwnerID     int64
	LocalID     int64
	PeerID      int64
	FromID      int64
	Date        time.Time
	Text        string
	Out         bool
	EditDate    *time.Time
	Deleted     bool
	RandomID    int64
	PeerLocalID int64
}

func messageFromRow(r db.Message) Message {
	m := Message{
		OwnerID:     r.OwnerID,
		LocalID:     r.LocalID,
		PeerID:      r.PeerID,
		FromID:      r.FromID,
		Date:        r.Date.Time,
		Text:        r.Message,
		Out:         r.Out,
		Deleted:     r.Deleted,
		RandomID:    r.RandomID,
		PeerLocalID: r.PeerLocalID,
	}
	if r.EditDate.Valid {
		t := r.EditDate.Time
		m.EditDate = &t
	}
	return m
}

// MessageByOwnerLocal returns one message by its (owner, local_id); ok=false when absent.
func (s *Store) MessageByOwnerLocal(ctx context.Context, ownerID, localID int64) (Message, bool, error) {
	r, err := s.q.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Message{}, false, nil
	case err != nil:
		return Message{}, false, fmt.Errorf("message by owner local: %w", err)
	}
	return messageFromRow(r), true, nil
}

// SendMessage persists both sides of a 1:1 message in one transaction. Each side
// gets its own local_id and its own pts++. A repeated randomID (per sender) is
// deduped: the original sender message is returned with dup=true and no new rows
// or events. Returns the sender's stored copy plus both owners' resulting pts.
func (s *Store) SendMessage(ctx context.Context, fromID, toID int64, text string, randomID int64) (sender Message, senderPts, recipientPts int, dup bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	if err = qtx.EnsureUpdateState(ctx, fromID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure sender state: %w", err)
	}
	if err = qtx.EnsureUpdateState(ctx, toID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure recipient state: %w", err)
	}
	if err = lockOwners(ctx, tx, fromID, toID); err != nil {
		return Message{}, 0, 0, false, err
	}

	// Idempotency: a resend with the same random_id returns the original.
	if randomID != 0 {
		existing, e := qtx.MessageByRandomID(ctx, db.MessageByRandomIDParams{OwnerID: fromID, RandomID: randomID})
		switch {
		case e == nil:
			sPts, rPts, e2 := currentPts(ctx, qtx, fromID, toID)
			if e2 != nil {
				return Message{}, 0, 0, false, e2
			}
			return messageFromRow(existing), sPts, rPts, true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return Message{}, 0, 0, false, fmt.Errorf("random_id lookup: %w", e)
		}
	}

	sb, err := qtx.BumpState(ctx, fromID)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("bump sender: %w", err)
	}
	rb, err := qtx.BumpState(ctx, toID)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("bump recipient: %w", err)
	}

	// Sender outbox copy (dedup token lives here) + recipient inbox copy.
	if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
		OwnerID: fromID, LocalID: sb.LocalID, PeerID: toID, FromID: fromID,
		Message: text, Out: true, RandomID: randomID, PeerLocalID: rb.LocalID,
	}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("insert sender message: %w", err)
	}
	if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
		OwnerID: toID, LocalID: rb.LocalID, PeerID: fromID, FromID: fromID,
		Message: text, Out: false, RandomID: 0, PeerLocalID: sb.LocalID,
	}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("insert recipient message: %w", err)
	}

	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: fromID, Pts: sb.Pts, Type: int16(EventNewMessage), LocalID: sb.LocalID}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("sender event: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: toID, Pts: rb.Pts, Type: int16(EventNewMessage), LocalID: rb.LocalID}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("recipient event: %w", err)
	}

	// Sender read their own message (unread +0); recipient gains one unread.
	if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: fromID, PeerID: toID, TopMessage: sb.LocalID, UnreadCount: 0}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("sender dialog: %w", err)
	}
	if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: toID, PeerID: fromID, TopMessage: rb.LocalID, UnreadCount: 1}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("recipient dialog: %w", err)
	}

	stored, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: fromID, LocalID: sb.LocalID})
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("reload sender message: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("commit: %w", err)
	}
	return messageFromRow(stored), int(sb.Pts), int(rb.Pts), false, nil
}

// currentPts returns both owners' current pts without advancing them.
func currentPts(ctx context.Context, q *db.Queries, aID, bID int64) (int, int, error) {
	as, err := q.GetState(ctx, aID)
	if err != nil {
		return 0, 0, fmt.Errorf("state a: %w", err)
	}
	bs, err := q.GetState(ctx, bID)
	if err != nil {
		return 0, 0, fmt.Errorf("state b: %w", err)
	}
	return int(as.Pts), int(bs.Pts), nil
}

// lockOwners takes per-owner advisory locks in ascending id order (deadlock-free)
// so concurrent sends touching the same two owners serialize.
func lockOwners(ctx context.Context, tx pgx.Tx, ids ...int64) error {
	seen := make(map[int64]bool, len(ids))
	var uniq []int64
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	for i := 0; i < len(uniq); i++ {
		for j := i + 1; j < len(uniq); j++ {
			if uniq[j] < uniq[i] {
				uniq[i], uniq[j] = uniq[j], uniq[i]
			}
		}
	}
	for _, id := range uniq {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, id); err != nil {
			return fmt.Errorf("advisory lock %d: %w", id, err)
		}
	}
	return nil
}
