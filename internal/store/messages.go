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

	// Take the sorted advisory locks before any per-owner insert. Provisioning
	// state rows before locking could deadlock two opposite-direction first sends
	// on the update_state unique-key insert; locking first serializes them.
	if err = lockOwners(ctx, tx, fromID, toID); err != nil {
		return Message{}, 0, 0, false, err
	}
	if err = qtx.EnsureUpdateState(ctx, fromID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure sender state: %w", err)
	}
	if err = qtx.EnsureUpdateState(ctx, toID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure recipient state: %w", err)
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

// History returns owner's messages with peer, newest-first, excluding deleted.
// offsetID > 0 pages strictly older than that local_id (0 = from newest).
func (s *Store) History(ctx context.Context, ownerID, peerID int64, offsetID, limit int) ([]Message, error) {
	rows, err := s.q.HistoryPage(ctx, db.HistoryPageParams{
		OwnerID:  ownerID,
		PeerID:   peerID,
		OffsetID: int64(offsetID),
		Lim:      int32(limit), //nolint:gosec // limit is a small validated page size

	})
	if err != nil {
		return nil, fmt.Errorf("history page: %w", err)
	}
	msgs := make([]Message, len(rows))
	for i, r := range rows {
		msgs[i] = messageFromRow(r)
	}
	return msgs, nil
}

// EditMessage edits the caller's own outgoing message and its mirror on the
// peer's side, bumping both owners' pts with an edit event. Returns the peer id
// and the editor's new pts. ErrMessageInvalid if the message is absent, deleted,
// or not an outgoing message the caller authored.
func (s *Store) EditMessage(ctx context.Context, ownerID, localID int64, text string) (peerID int64, newPts int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Discover the peer to lock; validated again after the lock (fail closed).
	pre, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, ErrMessageInvalid
	}
	if err != nil {
		return 0, 0, fmt.Errorf("load message: %w", err)
	}
	peerID = pre.PeerID
	if err = lockOwners(ctx, tx, ownerID, peerID); err != nil {
		return 0, 0, err
	}

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, ErrMessageInvalid
	}
	if err != nil {
		return 0, 0, fmt.Errorf("reload message: %w", err)
	}
	if !msg.Out || msg.Deleted {
		return 0, 0, ErrMessageInvalid
	}

	if err = qtx.SetEditedText(ctx, db.SetEditedTextParams{OwnerID: ownerID, LocalID: localID, Message: text}); err != nil {
		return 0, 0, fmt.Errorf("edit owner row: %w", err)
	}
	if err = qtx.SetEditedText(ctx, db.SetEditedTextParams{OwnerID: peerID, LocalID: msg.PeerLocalID, Message: text}); err != nil {
		return 0, 0, fmt.Errorf("edit mirror row: %w", err)
	}

	ownerPts, err := qtx.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump owner: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: ownerID, Pts: ownerPts, Type: int16(EventEdit), LocalID: localID}); err != nil {
		return 0, 0, fmt.Errorf("owner edit event: %w", err)
	}
	peerPts, err := qtx.BumpPtsOnly(ctx, peerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump peer: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: peerID, Pts: peerPts, Type: int16(EventEdit), LocalID: msg.PeerLocalID}); err != nil {
		return 0, 0, fmt.Errorf("peer edit event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	return peerID, int(ownerPts), nil
}

// DeleteMessages marks the given owner-local messages deleted on both sides,
// emitting one delete event per message per affected owner. It fails closed
// (ErrMessageInvalid, no changes) if any id is absent. Returns the resulting
// pts per affected user (owner and peers) for the caller to notify.
func (s *Store) DeleteMessages(ctx context.Context, ownerID int64, localIDs []int64) (map[int64]int, error) {
	if len(localIDs) == 0 {
		return map[int64]int{}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Load every target first; a missing id fails the whole batch (fail closed).
	msgs := make([]db.Message, 0, len(localIDs))
	peers := map[int64]bool{}
	for _, id := range localIDs {
		m, e := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: id})
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, ErrMessageInvalid
		}
		if e != nil {
			return nil, fmt.Errorf("load message %d: %w", id, e)
		}
		msgs = append(msgs, m)
		peers[m.PeerID] = true
	}

	lockIDs := make([]int64, 0, len(peers)+1)
	lockIDs = append(lockIDs, ownerID)
	for p := range peers {
		lockIDs = append(lockIDs, p)
	}
	if err = lockOwners(ctx, tx, lockIDs...); err != nil {
		return nil, err
	}

	perOwner := map[int64]int{}
	for _, m := range msgs {
		if err = qtx.SetDeleted(ctx, db.SetDeletedParams{OwnerID: ownerID, LocalID: m.LocalID}); err != nil {
			return nil, fmt.Errorf("delete owner row: %w", err)
		}
		if err = qtx.SetDeleted(ctx, db.SetDeletedParams{OwnerID: m.PeerID, LocalID: m.PeerLocalID}); err != nil {
			return nil, fmt.Errorf("delete mirror row: %w", err)
		}
		ownerPts, e := qtx.BumpPtsOnly(ctx, ownerID)
		if e != nil {
			return nil, fmt.Errorf("bump owner: %w", e)
		}
		if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: ownerID, Pts: ownerPts, Type: int16(EventDelete), LocalID: m.LocalID}); e != nil {
			return nil, fmt.Errorf("owner delete event: %w", e)
		}
		perOwner[ownerID] = int(ownerPts)
		peerPts, e := qtx.BumpPtsOnly(ctx, m.PeerID)
		if e != nil {
			return nil, fmt.Errorf("bump peer: %w", e)
		}
		if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: m.PeerID, Pts: peerPts, Type: int16(EventDelete), LocalID: m.PeerLocalID}); e != nil {
			return nil, fmt.Errorf("peer delete event: %w", e)
		}
		perOwner[m.PeerID] = int(peerPts)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return perOwner, nil
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
