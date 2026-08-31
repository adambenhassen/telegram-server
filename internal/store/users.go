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

// ErrUsernameIsLoginCredential is returned when the caller's handle is their
// login credential (login_mode='username'), so it cannot be changed or released.
var ErrUsernameIsLoginCredential = errors.New("username is login credential")

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

	normalized := NormalizePhone(phone)
	u, err := qtx.CreateUser(ctx, &normalized)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("ensure update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("commit: %w", err)
	}
	return UserFromDB(db.UserByIDRow(u)), nil
}

// CreateUsernameUser inserts a username-mode user with no phone. It provisions
// update_state in the same transaction. Returns the new user. Does NOT claim
// the username in the usernames table — the calling handler does that atomically
// with the auth key binding. The handle parameter is accepted for validation
// but not stored; the usernames table claim is the handler's responsibility.
func (s *Store) CreateUsernameUser(ctx context.Context, handle, firstName, lastName string) (User, error) {
	// Validate the handle is present — the handler must not call this without
	// a normalized handle, and the store is the last line of defense before
	// creating an account with no way to be resolved.
	if handle == "" {
		return User{}, errors.New("CreateUsernameUser: handle required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	u, err := qtx.CreateUsernameUser(ctx, db.CreateUsernameUserParams{
		FirstName: firstName,
		LastName:  lastName,
	})
	if err != nil {
		return User{}, fmt.Errorf("create username user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("ensure update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("commit: %w", err)
	}
	return UserFromCreateUsernameUser(u), nil
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

// UsersByID returns every user in ids in one query, keyed by id. Absent ids
// are simply missing from the map. It is the batched counterpart of UserByID
// for callers that hydrate a whole id set at once.
func (s *Store) UsersByID(ctx context.Context, ids []int64) (map[int64]User, error) {
	if len(ids) == 0 {
		return map[int64]User{}, nil
	}
	rows, err := s.q.UsersByID(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("users by id: %w", err)
	}
	out := make(map[int64]User, len(rows))
	for _, r := range rows {
		out[r.ID] = UserFromDB(db.UserByIDRow(r))
	}
	return out, nil
}

// EntitledUserIDs returns the subset of ids the viewer is entitled to see
// live, in one query. An id is entitled iff any of the four live edges holds:
// the id is the viewer, the two share a 1:1 dialog row, both are current
// participants of some chat, or both are current unbanned members of some
// channel. The channel edge requires the viewer's own row to be unbanned as
// well: a banned viewer is not a current member, so the channel admits nothing
// for them. This is the single round-trip predicate loadUsers uses; it
// replaces the per-edge fan-out that would otherwise materialize the viewer's
// entire dialog, chat and channel neighbourhood on every call.
func (s *Store) EntitledUserIDs(ctx context.Context, viewerID int64, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.EntitledUserIDs(ctx, db.EntitledUserIDsParams{
		ViewerID: viewerID,
		Ids:      ids,
	})
	if err != nil {
		return nil, fmt.Errorf("entitled user ids: %w", err)
	}
	out := make([]int64, len(rows))
	for i, r := range rows {
		id, ok := r.(int64)
		if !ok {
			return nil, fmt.Errorf("entitled user ids: unexpected type %T for row %d", r, i)
		}
		out[i] = id
	}
	return out, nil
}

// UserByPhone returns the user for phone, ok=false when absent.
func (s *Store) UserByPhone(ctx context.Context, phone string) (User, bool, error) {
	normalized := NormalizePhone(phone)
	u, err := s.q.UserByPhone(ctx, &normalized)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by phone: %w", err)
	}
	return UserFromDB(db.UserByIDRow(u)), true, nil
}

// UserLoginMode returns the login_mode of the user with id.
// Returns "phone" for phone-mode accounts and "username" for username-mode accounts.
// Returns an error when the user does not exist or the query fails.
func (s *Store) UserLoginMode(ctx context.Context, id int64) (string, error) {
	return s.q.GetUserLoginMode(ctx, id)
}

// UserFromDB converts a user row to the store's User type. Every query that
// loads a user selects the same columns, so sqlc emits one identically shaped
// struct per query and Go converts between them: one converter here is what
// keeps a new query from mapping a user through a second, divergent path.
//
// Username is the handle off the joined usernames row, which is what makes the
// value authoritative. Nil means the account holds no handle, whatever the
// denormalized users.username copy still says.
func UserFromDB(u db.UserByIDRow) User {
	var lastSeen *time.Time
	if u.LastSeenAt.Valid {
		t := u.LastSeenAt.Time
		lastSeen = &t
	}
	var phone string
	if u.Phone != nil {
		phone = *u.Phone
	}
	return User{
		ID:         u.ID,
		Phone:      phone,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		IsOnline:   u.IsOnline,
		LastSeenAt: lastSeen,
		Username:   u.Username,
	}
}

// UserFromCreateUsernameUser converts the CreateUsernameUserRow (which does not
// join usernames) to the store's User type. Phone is empty, Username is nil.
func UserFromCreateUsernameUser(u db.CreateUsernameUserRow) User {
	return User{
		ID:         u.ID,
		Phone:      "",
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		IsOnline:   u.IsOnline,
		LastSeenAt: nil,
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

// DialogPartners returns the distinct set of user IDs that share a 1:1 dialog
// row with userID. Used as the fan-out target set for status changes.
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

	// Block username changes for accounts whose handle is the login credential.
	// This guard lives in the store, not the handler, so M16 and any future
	// caller cannot bypass it.
	loginMode, err := qtx.GetUserLoginMode(ctx, userID)
	if err != nil {
		return fmt.Errorf("get login mode: %w", err)
	}
	if loginMode == "username" {
		return ErrUsernameIsLoginCredential
	}

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

// SearchContacts searches the caller's dialog partners by name. Only users
// with whom the caller has an existing 1:1 dialog are returned. The query is
// matched against the stored name_tsv tsvector (first_name + last_name) using
// plainto_tsquery on the 'simple' dictionary.
func (s *Store) SearchContacts(ctx context.Context, ownerID int64, query string, limit int32) ([]User, error) {
	rows, err := s.q.SearchContactsByName(ctx, db.SearchContactsByNameParams{
		OwnerID: ownerID,
		Query:   query,
		Lim:     limit,
	})
	if err != nil {
		return nil, fmt.Errorf("search contacts: %w", err)
	}
	out := make([]User, len(rows))
	for i, r := range rows {
		out[i] = UserFromDB(db.UserByIDRow(r))
	}
	return out, nil
}

// ClaimUsername claims a username for a user: inserts into the usernames table
// and updates users.username in one transaction. It bypasses the login_mode guard
// that UpdateUsername enforces, so it is used only by tests; account admission
// uses the transaction-bound qtx method directly. PK conflict returns
// ErrUsernameOccupied.
func (s *Store) ClaimUsername(ctx context.Context, userID int64, handle string) error {
	xHandle := strings.ToLower(handle)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	// Release the caller's existing handle first.
	if _, err := qtx.ReleaseUsernameByOwner(ctx, db.ReleaseUsernameByOwnerParams{
		OwnerType: "user",
		OwnerID:   userID,
	}); err != nil {
		return fmt.Errorf("release old username: %w", err)
	}
	_, err = qtx.ClaimUsername(ctx, db.ClaimUsernameParams{
		Handle:    xHandle,
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
	if _, err := qtx.SetUsername(ctx, db.SetUsernameParams{ID: userID, Username: &xHandle}); err != nil {
		return fmt.Errorf("set username: %w", err)
	}
	return tx.Commit(ctx)
}

// ClaimChannelUsername claims a username for a channel atomically: inserts
// into the usernames table AND updates channels.username in one transaction.
// The shipped RPC that does this (channels.setUsername) is a later ticket;
// this method exists so tests can seed valid channel username state without
// violating the non-oracle invariant.
func (s *Store) ClaimChannelUsername(ctx context.Context, channelID int64, handle string) error {
	xHandle := strings.ToLower(handle)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	if _, err := qtx.ClaimUsername(ctx, db.ClaimUsernameParams{
		Handle:    xHandle,
		OwnerType: "channel",
		OwnerID:   channelID,
	}); err != nil {
		return fmt.Errorf("claim username: %w", err)
	}
	if _, err := qtx.SetChannelUsername(ctx, db.SetChannelUsernameParams{
		ID:       channelID,
		Username: &xHandle,
	}); err != nil {
		return fmt.Errorf("set channel username: %w", err)
	}
	return tx.Commit(ctx)
}
