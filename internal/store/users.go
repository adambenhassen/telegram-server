package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrUsernameOccupied is returned when a username is already claimed by another
// account. Distinct from ErrUsernameInvalid (validation failure) and
// ErrUsernameFloodWait (rate limit).
var ErrUsernameOccupied = errors.New("username occupied")

// ErrUsernameFloodWait is returned when a user has exceeded the per-account
// username change rate limit.
var ErrUsernameFloodWait = errors.New("username change flood wait")

// UsernameChangeWindow is the rolling window for the per-account username
// change rate limit.
const UsernameChangeWindow = 24 * time.Hour

// UsernameChangeLimit is the maximum number of username changes a single
// account may make within UsernameChangeWindow.
const UsernameChangeLimit = 2

// User is a persisted account.
type User struct {
	ID         int64
	Phone      string
	FirstName  string
	LastName   string
	IsOnline   bool
	LastSeenAt *time.Time
	// Username is the normalized handle claimed in the usernames table.
	// Nil when the account has no username.
	Username *string
}

// NormalizePhone strips an optional leading '+' so that '+1555...' and
// '1555...' resolve to the same value. Used on both read and write paths.
func NormalizePhone(phone string) string {
	return strings.TrimPrefix(phone, "+")
}

// CreateUser inserts a user for phone, or returns the existing row. It also
// provisions the account's update_state row in the same transaction so the
// update APIs and the two-sided send lock ordering never race a missing row.
func (s *Store) CreateUser(ctx context.Context, phone string) (User, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	u, err := qtx.CreateUser(ctx, NormalizePhone(phone))
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("ensure update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("commit: %w", err)
	}
	return UserFromDB(u), nil
}

// UserByID returns the user for id, ok=false when absent.
func (s *Store) UserByID(ctx context.Context, id int64) (User, bool, error) {
	u, err := s.q.UserByID(ctx, id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by id: %w", err)
	}
	return UserFromDB(u), true, nil
}

// UserByPhone returns the user for phone, ok=false when absent.
func (s *Store) UserByPhone(ctx context.Context, phone string) (User, bool, error) {
	u, err := s.q.UserByPhone(ctx, NormalizePhone(phone))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by phone: %w", err)
	}
	return UserFromDB(u), true, nil
}

// UserFromDB converts a sqlc-generated User model to the store's User type.
func UserFromDB(u db.User) User {
	var lastSeen *time.Time
	if u.LastSeenAt.Valid {
		t := u.LastSeenAt.Time
		lastSeen = &t
	}
	return User{
		ID:         u.ID,
		Phone:      u.Phone,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		IsOnline:   u.IsOnline,
		LastSeenAt: lastSeen,
		Username:   u.Username,
	}
}

// SetUserStatus updates a user's online status and last-seen timestamp in one
// write. When online is true the user is marked online; when false the user is
// marked offline. last_seen_at is set to NOW() in both cases. Returns an error
// when the user does not exist.
func (s *Store) SetUserStatus(ctx context.Context, userID int64, online bool) error {
	n, err := s.q.SetUserStatus(ctx, db.SetUserStatusParams{ID: userID, IsOnline: online})
	if err != nil {
		return fmt.Errorf("set user status: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("set user status: user %d not found", userID)
	}
	return nil
}

// DialogPartners returns the distinct set of user IDs that share a non-deleted
// 1:1 dialog with userID. Used as the fan-out target set for status changes.
func (s *Store) DialogPartners(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT CASE d.owner_id WHEN $1 THEN d.peer_id ELSE d.owner_id END AS partner_id
		FROM dialogs d
		WHERE d.peer_type = 1
		  AND (d.owner_id = $1 OR d.peer_id = $1)
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("dialog partners: %w", err)
	}
	defer rows.Close()

	var partners []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("dialog partners scan: %w", err)
		}
		partners = append(partners, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dialog partners: %w", err)
	}
	return partners, nil
}

// UpdateUsername atomically claims or releases a username for userID. When
// username is non-empty the handle is claimed: a row is inserted into the
// usernames table and the users.username column is updated. When username is
// empty the handle is released: the usernames row is deleted and
// users.username is cleared.
//
// The rate limit is enforced with the same rolling-window pattern as
// CheckAndChargeLookup: an advisory lock serialises concurrent changes for the
// same user, expired rows are pruned, the count is checked, and a change is
// recorded only after a successful claim or release. A clear (empty string)
// counts as a change. Failed claims (USERNAME_OCCUPIED) do not consume a
// rate-limit token.
//
// Returns ErrUsernameOccupied when the handle is already taken by another
// account. Returns ErrUsernameFloodWait when the rate limit is exceeded.
func (s *Store) UpdateUsername(ctx context.Context, userID int64, username string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	// Serialize concurrent changes for the same user so the prune/count/insert
	// sequence cannot race with itself.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", userID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}

	qtx := s.q.WithTx(tx)

	// Prune expired rows before counting.
	cutoff := pgtype.Timestamptz{Time: time.Now().Add(-UsernameChangeWindow), Valid: true}
	if err := qtx.DeleteExpiredUsernameChanges(ctx, db.DeleteExpiredUsernameChangesParams{
		UserID:    userID,
		ChangedAt: cutoff,
	}); err != nil {
		return fmt.Errorf("prune username changes: %w", err)
	}

	// Clear or claim the username first — only record the rate-limit token on
	// success, so a failed claim (USERNAME_OCCUPIED) does not consume quota.
	if username == "" {
		// Clear: release any existing handle for this user. Uses the
		// transaction-bound query set so the DELETE rolls back if a later
		// step fails.
		if _, err := qtx.ReleaseUsernameByOwner(ctx, db.ReleaseUsernameByOwnerParams{
			OwnerType: "user",
			OwnerID:   userID,
		}); err != nil {
			return fmt.Errorf("release username: %w", err)
		}
		// Also clear the denormalized column.
		var nullStr *string
		if _, err := qtx.SetUsername(ctx, db.SetUsernameParams{ID: userID, Username: nullStr}); err != nil {
			return fmt.Errorf("clear username: %w", err)
		}
	} else {
		normalized := strings.ToLower(username)
		// Release the caller's existing handle before claiming the new one.
		// If the INSERT below fails (USERNAME_OCCUPIED), this release rolls
		// back with it, so the old handle is never orphaned.
		if _, err := qtx.ReleaseUsernameByOwner(ctx, db.ReleaseUsernameByOwnerParams{
			OwnerType: "user",
			OwnerID:   userID,
		}); err != nil {
			return fmt.Errorf("release old username: %w", err)
		}
		// Claim: insert into usernames table. PK conflict means occupied.
		_, err := qtx.ClaimUsername(ctx, db.ClaimUsernameParams{
			Handle:    normalized,
			OwnerType: "user",
			OwnerID:   userID,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrUsernameOccupied
			}
			return fmt.Errorf("claim username: %w", err)
		}
		// Update the denormalized column on the users table.
		if _, err := qtx.SetUsername(ctx, db.SetUsernameParams{ID: userID, Username: &normalized}); err != nil {
			return fmt.Errorf("set username: %w", err)
		}
	}

	// Record the successful change and verify the rate limit.
	if err := qtx.InsertUsernameChange(ctx, userID); err != nil {
		return fmt.Errorf("record username change: %w", err)
	}
	count, err := qtx.CountUsernameChanges(ctx, db.CountUsernameChangesParams{
		UserID:    userID,
		ChangedAt: cutoff,
	})
	if err != nil {
		return fmt.Errorf("count username changes: %w", err)
	}
	if count > UsernameChangeLimit {
		return ErrUsernameFloodWait
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ClaimUsername inserts a row into the usernames table. It is used by the
// resolveUsername handler's tests to claim a username for a channel — the
// shipped RPC that does this (channels.setUsername) is a later ticket.
func (s *Store) ClaimUsername(ctx context.Context, handle string, ownerType string, ownerID int64) error {
	_, err := s.q.ClaimUsername(ctx, db.ClaimUsernameParams{
		Handle:    handle,
		OwnerType: ownerType,
		OwnerID:   ownerID,
	})
	return err
}
