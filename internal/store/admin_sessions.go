package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// AdminSessionRow holds the fields returned by a session lookup.
type AdminSessionRow struct {
	TokenFingerprint []byte
	ExpiresAt        time.Time
	LastActivity     time.Time
}

// CreateAdminSession inserts a new admin session row. Returns error if the
// session hash already exists.
func (s *Store) CreateAdminSession(ctx context.Context, sessionHash []byte, tokenFingerprint []byte, expiresAt, lastActivity time.Time) error {
	return s.q.InsertAdminSession(ctx, db.InsertAdminSessionParams{
		SessionHash:      sessionHash,
		TokenFingerprint: tokenFingerprint,
		ExpiresAt:        pgtype.Timestamptz{Time: expiresAt, Valid: true},
		LastActivity:     pgtype.Timestamptz{Time: lastActivity, Valid: true},
	})
}

// GetAdminSession looks up a session by its SHA-256 hash. Returns the stored
// token fingerprint, expiry, and last-activity timestamp.
func (s *Store) GetAdminSession(ctx context.Context, sessionHash []byte) (AdminSessionRow, error) {
	row, err := s.q.GetAdminSession(ctx, sessionHash)
	if err != nil {
		return AdminSessionRow{}, err
	}
	return AdminSessionRow{
		TokenFingerprint: row.TokenFingerprint,
		ExpiresAt:        row.ExpiresAt.Time,
		LastActivity:     row.LastActivity.Time,
	}, nil
}

// UpdateAdminSessionActivity touches the last_activity timestamp for an
// existing session. Uses GREATEST so concurrent requests never move the
// clock backwards. Returns the number of rows updated (0 means the session
// was deleted between lookup and update).
func (s *Store) UpdateAdminSessionActivity(ctx context.Context, sessionHash []byte, lastActivity time.Time) (int64, error) {
	return s.q.UpdateAdminSessionActivity(ctx, db.UpdateAdminSessionActivityParams{
		SessionHash:  sessionHash,
		LastActivity: pgtype.Timestamptz{Time: lastActivity, Valid: true},
	})
}

// SweepExpiredAdminSessions deletes session rows whose absolute expiry
// deadline has passed. Returns the number of rows deleted.
func (s *Store) SweepExpiredAdminSessions(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredAdminSessions(ctx)
}
