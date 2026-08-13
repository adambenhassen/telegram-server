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
	ID     int64
	Value  []byte
	UserID int64
	// PendingUserID is the half-authorized user staged by signIn for a 2FA
	// account; it is 0 unless a password challenge is outstanding and never
	// grants access on its own.
	PendingUserID int64
	// Provisional is true when the bound user is username-mode and has not
	// yet completed sign-in (no verifier stored). It is derived from the
	// login_mode column and the absence of a user_passwords row, never stored.
	Provisional bool
	CreatedAt   time.Time
	LastSeenAt  time.Time
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
// The Provisional field is derived: true when the bound user has
// login_mode='username' and no user_passwords row.
func (s *Store) AuthKeyByID(ctx context.Context, id int64) (AuthKey, bool, error) {
	row, err := s.q.AuthKeyByID(ctx, id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return AuthKey{}, false, nil
	case err != nil:
		return AuthKey{}, false, fmt.Errorf("auth key by id: %w", err)
	}
	key, err := s.authKeyFromDB(row)
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

// SetPendingUser marks the auth key id as half-authorized for userID: the state
// between auth.signIn and auth.checkPassword when the account has 2FA. It clears
// any existing user_id binding so the key is de-authorized until checkPassword
// promotes it — a re-signIn on an already-bound key must not stay authorized as
// the old user. It never grants access on its own; only PromotePendingUser
// authorizes. Returns ErrAuthKeyNotFound when no auth-key row matches id, so
// callers fail closed.
func (s *Store) SetPendingUser(ctx context.Context, id, userID int64) error {
	rows, err := s.q.SetPendingUser(ctx, db.SetPendingUserParams{ID: id, PendingUserID: &userID})
	if err != nil {
		return fmt.Errorf("set pending user: %w", err)
	}
	if rows == 0 {
		return ErrAuthKeyNotFound
	}
	return nil
}

// PromotePendingUser authorizes the auth key id by moving pending_user_id to
// user_id and clearing pending, but only when the key's current pending matches
// userID. A mismatch or absent pending affects zero rows and returns
// ErrAuthKeyNotFound, so a checkPassword can never authorize a key that signIn
// did not stage for this exact user.
func (s *Store) PromotePendingUser(ctx context.Context, id, userID int64) error {
	rows, err := s.q.PromotePendingUser(ctx, db.PromotePendingUserParams{ID: id, UserID: &userID})
	if err != nil {
		return fmt.Errorf("promote pending user: %w", err)
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

// AuthKeysByUser returns every auth key bound to userID. The Provisional field
// is not populated (the query does not join users/passwords).
func (s *Store) AuthKeysByUser(ctx context.Context, userID int64) ([]AuthKey, error) {
	rows, err := s.q.AuthKeysByUser(ctx, &userID)
	if err != nil {
		return nil, fmt.Errorf("auth keys by user: %w", err)
	}
	keys := make([]AuthKey, len(rows))
	for i, r := range rows {
		key, err := s.authKeyFromDBBasic(r)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

// authKeyFromDBBasic maps the basic db.AuthKey struct (used by AuthKeysByUser)
// to the domain type, decrypting the stored key value and collapsing NULL
// user_id to UserID 0. Provisional is always false — the query does not join
// users/passwords.
func (s *Store) authKeyFromDBBasic(k db.AuthKey) (AuthKey, error) {
	value, err := s.cipher.Open(k.KeyValue)
	if err != nil {
		return AuthKey{}, fmt.Errorf("decrypt auth key %d: %w", k.ID, err)
	}
	var userID int64
	if k.UserID != nil {
		userID = *k.UserID
	}
	var pendingUserID int64
	if k.PendingUserID != nil {
		pendingUserID = *k.PendingUserID
	}
	return AuthKey{
		ID:            k.ID,
		Value:         value,
		UserID:        userID,
		PendingUserID: pendingUserID,
		CreatedAt:     k.CreatedAt.Time,
		LastSeenAt:    k.LastSeenAt.Time,
	}, nil
}

// authKeyFromDB maps an AuthKeyByIDRow to the domain type, decrypting the stored
// key value and collapsing a NULL user_id to UserID 0. A decrypt failure (wrong
// master key or corrupt/tampered row) is returned, never silently swallowed.
// Provisional is derived: true when the bound user has login_mode='username'
// and no user_passwords row (HasPassword is false). Unbound keys (user_id NULL)
// are always non-provisional.
func (s *Store) authKeyFromDB(k db.AuthKeyByIDRow) (AuthKey, error) {
	value, err := s.cipher.Open(k.KeyValue)
	if err != nil {
		return AuthKey{}, fmt.Errorf("decrypt auth key %d: %w", k.ID, err)
	}
	var userID int64
	if k.UserID != nil {
		userID = *k.UserID
	}
	var pendingUserID int64
	if k.PendingUserID != nil {
		pendingUserID = *k.PendingUserID
	}
	provisional := false
	if k.UserID != nil && k.LoginMode != nil && *k.LoginMode == "username" {
		if hasPw, ok := k.HasPassword.(bool); ok && !hasPw {
			provisional = true
		}
	}
	return AuthKey{
		ID:            k.ID,
		Value:         value,
		UserID:        userID,
		PendingUserID: pendingUserID,
		Provisional:   provisional,
		CreatedAt:     k.CreatedAt.Time,
		LastSeenAt:    k.LastSeenAt.Time,
	}, nil
}
