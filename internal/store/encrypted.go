package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// AcknowledgeEncryptedEvents atomically deletes encrypted events for ownerID
// with qts <= maxQts and returns the random_id values of the deleted rows.
// Single DELETE RETURNING prevents duplicate random_ids on concurrent calls.
func (s *Store) AcknowledgeEncryptedEvents(ctx context.Context, ownerID, maxQts int64) ([]int64, error) {
	ids, err := s.q.DeleteEncryptedEventsByQts(ctx, db.DeleteEncryptedEventsByQtsParams{
		OwnerID: ownerID,
		Qts:     maxQts,
	})
	if err != nil {
		return nil, fmt.Errorf("acknowledge encrypted events: %w", err)
	}
	return ids, nil
}

// GetQts returns the current qts watermark for userID. Returns 0 when the
// update_state row has not been created yet.
func (s *Store) GetQts(ctx context.Context, userID int64) (int64, error) {
	row, err := s.q.GetState(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get qts: %w", err)
	}
	return row.Qts, nil
}
