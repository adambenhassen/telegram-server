package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Dialog is a per-owner conversation summary.
type Dialog struct {
	OwnerID         int64
	PeerID          int64
	TopMessage      int64
	UnreadCount     int
	ReadInboxMaxID  int64
	ReadOutboxMaxID int64
}

// Dialogs lists the owner's conversations, newest activity first.
func (s *Store) Dialogs(ctx context.Context, ownerID int64) ([]Dialog, error) {
	rows, err := s.q.DialogsForOwner(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("dialogs for owner: %w", err)
	}
	out := make([]Dialog, len(rows))
	for i, r := range rows {
		out[i] = Dialog{
			OwnerID:         r.OwnerID,
			PeerID:          r.PeerID,
			TopMessage:      r.TopMessage,
			UnreadCount:     int(r.UnreadCount),
			ReadInboxMaxID:  r.ReadInboxMaxID,
			ReadOutboxMaxID: r.ReadOutboxMaxID,
		}
	}
	return out, nil
}

// ReadHistory marks the reader's inbound history with peer read up to maxID
// (monotonic; never regresses), recomputes the reader's unread count, and
// advances the peer's outbox read marker to the mirror position. It emits a
// read-inbox event for the reader and a read-outbox event for the peer, bumping
// both owners' pts. Returns each owner's new pts. If the reader has no dialog
// with peer there is nothing to read: both current pts are returned unchanged.
func (s *Store) ReadHistory(ctx context.Context, ownerID, peerID, maxID int64) (readerPts, peerPts int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	if err = lockOwners(ctx, tx, ownerID, peerID); err != nil {
		return 0, 0, err
	}

	inbox, err := qtx.AdvanceReadInbox(ctx, db.AdvanceReadInboxParams{MaxID: maxID, OwnerID: ownerID, PeerID: peerID})
	if errors.Is(err, pgx.ErrNoRows) {
		// No dialog to read; leave state untouched.
		rp, pp, e := currentPts(ctx, qtx, ownerID, peerID)
		if e != nil {
			return 0, 0, e
		}
		return rp, pp, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("advance read inbox: %w", err)
	}

	// Translate the reader's maxID into the peer's outbox local-id space via the
	// newest mirror at or below maxID, then advance the peer's outbox marker.
	var peerMax int64
	if err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(peer_local_id), 0) FROM messages
		 WHERE owner_id = $1 AND peer_id = $2 AND local_id <= $3`,
		ownerID, peerID, maxID,
	).Scan(&peerMax); err != nil {
		return 0, 0, fmt.Errorf("peer max local: %w", err)
	}
	if _, err = qtx.AdvanceReadOutbox(ctx, db.AdvanceReadOutboxParams{OwnerID: peerID, PeerID: ownerID, ReadOutboxMaxID: peerMax}); err != nil {
		return 0, 0, fmt.Errorf("advance read outbox: %w", err)
	}

	rPts, err := qtx.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump reader: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: ownerID, Pts: rPts, Type: int16(EventReadIn), LocalID: inbox.ReadInboxMaxID}); err != nil {
		return 0, 0, fmt.Errorf("reader read event: %w", err)
	}
	pPts, err := qtx.BumpPtsOnly(ctx, peerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump peer: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: peerID, Pts: pPts, Type: int16(EventReadOut), LocalID: peerMax}); err != nil {
		return 0, 0, fmt.Errorf("peer read event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	return int(rPts), int(pPts), nil
}
