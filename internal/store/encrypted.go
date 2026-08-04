package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// EncryptedChat is one row from encrypted_chats: two participants sharing an id
// and access_hash.
type EncryptedChat struct {
	ID         int
	AccessHash int64
	User1ID    int64
	User2ID    int64
}

// EncryptedEvent is one row from encrypted_events: an incoming secret-chat
// message waiting for the recipient to acknowledge it via receivedQueue.
type EncryptedEvent struct {
	Qts      int64
	RandomID int64
	Data     []byte
}

// CreateEncryptedChat inserts a secret-chat session. Idempotent on (user1,
// user2) conflict.
func (s *Store) CreateEncryptedChat(ctx context.Context, chatID int, accessHash int64, user1ID, user2ID int64) error {
	if err := s.q.InsertEncryptedChat(ctx, db.InsertEncryptedChatParams{
		ID:         int32(chatID), //nolint:gosec // chat id is positive int space
		AccessHash: accessHash,
		User1ID:    user1ID,
		User2ID:    user2ID,
	}); err != nil {
		return fmt.Errorf("create encrypted chat: %w", err)
	}
	return nil
}

// GetEncryptedChat returns the encrypted-chat row by id.
func (s *Store) GetEncryptedChat(ctx context.Context, chatID int) (EncryptedChat, error) {
	row, err := s.q.GetEncryptedChat(ctx, int32(chatID)) //nolint:gosec // chat id is positive int space
	if errors.Is(err, pgx.ErrNoRows) {
		return EncryptedChat{}, nil
	}
	if err != nil {
		return EncryptedChat{}, fmt.Errorf("get encrypted chat: %w", err)
	}
	return EncryptedChat{
		ID:         int(row.ID),
		AccessHash: row.AccessHash,
		User1ID:    row.User1ID,
		User2ID:    row.User2ID,
	}, nil
}

// InsertEncryptedEvent stores an incoming encrypted message for ownerID at the
// given qts. The caller must have already bumped qts.
func (s *Store) InsertEncryptedEvent(ctx context.Context, ownerID, qts, randomID int64, data []byte) error {
	if err := s.q.InsertEncryptedEvent(ctx, db.InsertEncryptedEventParams{
		OwnerID:  ownerID,
		Qts:      qts,
		RandomID: randomID,
		Data:     data,
	}); err != nil {
		return fmt.Errorf("insert encrypted event: %w", err)
	}
	return nil
}

// EncryptedEventsUpTo returns all encrypted events for ownerID with qts <= maxQts,
// ordered ascending.
func (s *Store) EncryptedEventsUpTo(ctx context.Context, ownerID, maxQts int64) ([]EncryptedEvent, error) {
	rows, err := s.q.EncryptedEventsUpTo(ctx, db.EncryptedEventsUpToParams{
		OwnerID: ownerID,
		Qts:     maxQts,
	})
	if err != nil {
		return nil, fmt.Errorf("encrypted events up to: %w", err)
	}
	events := make([]EncryptedEvent, len(rows))
	for i, r := range rows {
		events[i] = EncryptedEvent{
			Qts:      r.Qts,
			RandomID: r.RandomID,
			Data:     r.Data,
		}
	}
	return events, nil
}

// DeleteEncryptedEventsUpTo removes all encrypted events for ownerID with
// qts <= maxQts. Used by receivedQueue to acknowledge messages.
func (s *Store) DeleteEncryptedEventsUpTo(ctx context.Context, ownerID, maxQts int64) error {
	if err := s.q.DeleteEncryptedEventsUpTo(ctx, db.DeleteEncryptedEventsUpToParams{
		OwnerID: ownerID,
		Qts:     maxQts,
	}); err != nil {
		return fmt.Errorf("delete encrypted events up to: %w", err)
	}
	return nil
}

// GetQts returns the current qts watermark for userID. Returns 0 when the
// update_state row has not been created yet.
func (s *Store) GetQts(ctx context.Context, userID int64) (int64, error) {
	qts, err := s.q.GetQts(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get qts: %w", err)
	}
	return qts, nil
}

// SetQts updates the qts watermark for userID.
func (s *Store) SetQts(ctx context.Context, userID, qts int64) error {
	if err := s.q.SetQts(ctx, db.SetQtsParams{
		UserID: userID,
		Qts:    qts,
	}); err != nil {
		return fmt.Errorf("set qts: %w", err)
	}
	return nil
}

// BumpQts increments qts by one and returns the new value. The caller uses the
// returned value as the qts for the next encrypted event.
func (s *Store) BumpQts(ctx context.Context, userID int64) (int64, error) {
	var qts int64
	if err := s.pool.QueryRow(ctx,
		`UPDATE update_state SET qts = qts + 1, date = now() WHERE user_id = $1 RETURNING qts`,
		userID,
	).Scan(&qts); err != nil {
		return 0, fmt.Errorf("bump qts: %w", err)
	}
	return qts, nil
}
