package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// AuthKey is a persisted MTProto auth key. UserID is 0 while the key is
// unbound; once login binds it to an account UserID holds that user's id.
type AuthKey struct {
	ID     int64
	Value  []byte
	UserID int64
}

// SaveAuthKey stores value under id, idempotently. Re-saving an existing id
// refreshes its value and last-seen time without touching its user binding.
func (s *Store) SaveAuthKey(ctx context.Context, id int64, value []byte) error {
	if err := s.q.SaveAuthKey(ctx, db.SaveAuthKeyParams{ID: id, KeyValue: value}); err != nil {
		return fmt.Errorf("save auth key: %w", err)
	}
	return nil
}

// AuthKeyByID returns the auth key for id, ok=false when absent.
func (s *Store) AuthKeyByID(ctx context.Context, id int64) (AuthKey, bool, error) {
	k, err := s.q.AuthKeyByID(ctx, id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return AuthKey{}, false, nil
	case err != nil:
		return AuthKey{}, false, fmt.Errorf("auth key by id: %w", err)
	}
	return authKeyFromDB(k), true, nil
}

// BindAuthKeyUser links the auth key id to userID. The user must already exist.
func (s *Store) BindAuthKeyUser(ctx context.Context, id, userID int64) error {
	if err := s.q.BindAuthKeyUser(ctx, db.BindAuthKeyUserParams{ID: id, UserID: &userID}); err != nil {
		return fmt.Errorf("bind auth key user: %w", err)
	}
	return nil
}

// DeleteAuthKey removes the auth key id. Deleting a missing id is a no-op.
func (s *Store) DeleteAuthKey(ctx context.Context, id int64) error {
	if err := s.q.DeleteAuthKey(ctx, id); err != nil {
		return fmt.Errorf("delete auth key: %w", err)
	}
	return nil
}

// AuthKeysByUser returns every auth key bound to userID.
func (s *Store) AuthKeysByUser(ctx context.Context, userID int64) ([]AuthKey, error) {
	rows, err := s.q.AuthKeysByUser(ctx, &userID)
	if err != nil {
		return nil, fmt.Errorf("auth keys by user: %w", err)
	}
	keys := make([]AuthKey, len(rows))
	for i, r := range rows {
		keys[i] = authKeyFromDB(r)
	}
	return keys, nil
}

// authKeyFromDB maps a generated row to the domain type, collapsing a NULL
// user_id to UserID 0.
func authKeyFromDB(k db.AuthKey) AuthKey {
	var userID int64
	if k.UserID != nil {
		userID = *k.UserID
	}
	return AuthKey{ID: k.ID, Value: k.KeyValue, UserID: userID}
}
