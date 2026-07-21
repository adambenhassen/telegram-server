package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// AuthKey is a persisted MTProto auth key. UserID is 0 while the key is
// unbound; once login binds it to an account UserID holds that user's id.
// CreatedAt/LastSeenAt back the session dates reported by account.getAuthorizations.
type AuthKey struct {
	ID         int64
	Value      []byte
	UserID     int64
	CreatedAt  time.Time
	LastSeenAt time.Time
}

// SaveAuthKey stores value under id, idempotently. The key value is encrypted at
// rest with the store's master key. Re-saving an existing id refreshes its value
// and last-seen time without touching its user binding.
func (s *Store) SaveAuthKey(ctx context.Context, id int64, value []byte) error {
	enc, err := s.cipher.Seal(value)
	if err != nil {
		return fmt.Errorf("save auth key: %w", err)
	}
	if err := s.q.SaveAuthKey(ctx, db.SaveAuthKeyParams{ID: id, KeyValue: enc}); err != nil {
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
	key, err := s.authKeyFromDB(k)
	if err != nil {
		return AuthKey{}, false, err
	}
	return key, true, nil
}

// BindAuthKeyUser links the auth key id to userID. The user must already exist.
// It returns ErrAuthKeyNotFound when no auth-key row matches id (absent key or a
// concurrent delete), so callers fail closed instead of reporting a false bind.
func (s *Store) BindAuthKeyUser(ctx context.Context, id, userID int64) error {
	rows, err := s.q.BindAuthKeyUser(ctx, db.BindAuthKeyUserParams{ID: id, UserID: &userID})
	if err != nil {
		return fmt.Errorf("bind auth key user: %w", err)
	}
	if rows == 0 {
		return ErrAuthKeyNotFound
	}
	return nil
}

// TouchAuthKey advances last_seen_at to now for id. It backs the DateActive
// field of account.getAuthorizations with real session activity; the mtproto
// loop throttles calls so this is not written on every frame. Touching a missing
// id is a no-op.
func (s *Store) TouchAuthKey(ctx context.Context, id int64) error {
	if err := s.q.TouchAuthKey(ctx, id); err != nil {
		return fmt.Errorf("touch auth key: %w", err)
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
		key, err := s.authKeyFromDB(r)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

// authKeyFromDB maps a generated row to the domain type, decrypting the stored
// key value and collapsing a NULL user_id to UserID 0. A decrypt failure (wrong
// master key or corrupt/tampered row) is returned, never silently swallowed.
func (s *Store) authKeyFromDB(k db.AuthKey) (AuthKey, error) {
	value, err := s.cipher.Open(k.KeyValue)
	if err != nil {
		return AuthKey{}, fmt.Errorf("decrypt auth key %d: %w", k.ID, err)
	}
	var userID int64
	if k.UserID != nil {
		userID = *k.UserID
	}
	return AuthKey{
		ID:         k.ID,
		Value:      value,
		UserID:     userID,
		CreatedAt:  k.CreatedAt.Time,
		LastSeenAt: k.LastSeenAt.Time,
	}, nil
}
