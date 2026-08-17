package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Reconnect pacing, exported so the flapping and idle-recovery cases can be
// asserted on the rule itself instead of on a 30-second wall clock.
func NextBackoff(prev, uptime time.Duration) time.Duration { return nextBackoff(prev, uptime) }

// SetDeniedHook installs a callback that fires in CheckRateLimit after the
// INSERT denial and before the GET. Tests use it to delete the row and
// exercise the ErrNoRows branch. Scoped to the Store so parallel tests
// each own their own hook without racing.
func SetDeniedHook(s *Store, fn func()) { s.deniedHook = fn }

// SetNowFunc pins the clock CheckRateLimit measures the client-visible wait
// against. It exists because the minimum-wait rule only fires on a remainder
// under one second, and a real sub-second window closes under host load before
// the second request lands — the test then sees an allowed request instead of
// the denial it is asserting on. Pinning the clock lets the window stay long
// enough that no scheduler delay can close it while the remainder under test
// stays exact. Scoped to the Store so parallel tests each own their own clock
// without racing.
func SetNowFunc(s *Store, fn func() time.Time) { s.now = fn }

// RateLimitExpiresAt reads a rate-limit row's stored deadline — the value the
// wait is computed from, and the one a test has to know to name a remainder
// relative to it.
func RateLimitExpiresAt(ctx context.Context, s *Store, subjectID int64, surface string) (time.Time, error) {
	var at time.Time
	err := s.pool.QueryRow(ctx,
		"SELECT expires_at FROM rate_limits WHERE subject_id = $1 AND surface = $2",
		subjectID, surface).Scan(&at)
	return at, err
}

// AgeRateLimitWindow rewinds one rate-limit row's window by d, leaving the row
// exactly as it would look had d of wall clock passed since the window opened.
// It is how a test crosses a window boundary: sleeping through a real window
// means the window has to be short, and a short window also closes early under
// host load, so the denial the test asserts on before the boundary stops
// happening. Rewinding lets the window be long enough that only this call can
// close it.
func AgeRateLimitWindow(ctx context.Context, s *Store, subjectID int64, surface string, d time.Duration) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE rate_limits
		    SET window_start = window_start - $3::INTERVAL,
		        expires_at   = expires_at   - $3::INTERVAL
		  WHERE subject_id = $1 AND surface = $2`,
		subjectID, surface, pgtype.Interval{Microseconds: d.Microseconds(), Valid: true})
	if err != nil {
		return err
	}
	// A surface typo would otherwise leave the window untouched and the test
	// asserting nothing.
	if n := tag.RowsAffected(); n != 1 {
		return fmt.Errorf("age rate limit window: %d rows for subject %d surface %q, want 1", n, subjectID, surface)
	}
	return nil
}

// SetSearchPageHook installs a callback that fires in SearchGlobal between the
// key read and the body read. Tests use it to delete the rows the page named and
// exercise the refill branch. Scoped to the Store so parallel tests each own
// their own hook without racing.
func SetSearchPageHook(s *Store, fn func()) { s.searchPageHook = fn }

const (
	ListenerBackoffMin = listenerBackoffMin
	ListenerBackoffMax = listenerBackoffMax
	ListenerStableFor  = listenerStableFor
)

// RemoveChatParticipant drops a participant with no announcement, so the fan-out
// tests can exercise a bare removal racing a chat write. It carries no removal
// logic of its own: the delete and its advisory lock are removeParticipant's, the
// one shipped path that deletes a chat_participants row, which is exactly what
// makes those tests a regression guard for the real invariant rather than for a
// second removal shape that only tests use.
func RemoveChatParticipant(ctx context.Context, s *Store, chatID, userID int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	if _, err := qtx.ChatByIDForUpdate(ctx, chatID); err != nil {
		return err
	}
	if _, err := removeParticipant(ctx, tx, qtx, chatID, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// HoldChatRowLock takes chatID's chats row lock in a transaction of its own and
// holds it until release is called. It is how a test asserts that a path does
// NOT take that lock: with the lock held, a caller that reaches for it blocks
// until its context expires, and one that rejects earlier returns immediately.
// A wall-clock measurement of two concurrent calls cannot tell those apart.
func HoldChatRowLock(ctx context.Context, s *Store, chatID int64) (release func(), err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.q.WithTx(tx).ChatByIDForUpdate(ctx, chatID); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // best effort on the error path
		return nil, err
	}
	return func() { _ = tx.Rollback(ctx) }, nil //nolint:errcheck // nothing to commit
}

// HoldChannelRowLock takes channelID's channels row lock in a transaction of
// its own and holds it until release is called. It mirrors HoldChatRowLock for
// the channel mutation path.
func HoldChannelRowLock(ctx context.Context, s *Store, channelID int64) (release func(), err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.q.WithTx(tx).LockChannel(ctx, channelID); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // best effort on the error path
		return nil, err
	}
	return func() { _ = tx.Rollback(ctx) }, nil //nolint:errcheck // nothing to commit
}

// InsertChatMessageNoFanout writes a chat-peer message row carrying fanout_id = 0
// — the "not a chat message" sentinel a well-formed fan-out never produces. No
// shipped path can create one, and the guards that reject it still have to be
// provable. Returns the row's local_id.
func InsertChatMessageNoFanout(ctx context.Context, s *Store, ownerID, chatID int64, text string) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	b, err := qtx.BumpState(ctx, ownerID)
	if err != nil {
		return 0, err
	}
	if err := qtx.InsertMessage(ctx, db.InsertMessageParams{
		OwnerID: ownerID, LocalID: b.LocalID, PeerType: int16(PeerTypeChat), PeerID: chatID,
		FromID: ownerID, Message: text, Out: true, RandomID: 0, PeerLocalID: 0,
		FanoutID: 0, ActionType: 0, ActionUserID: 0,
		FwdFromID: nil, FwdDate: pgtype.Timestamptz{}, FwdChannelID: nil, FwdChannelPost: nil,
	}); err != nil {
		return 0, err
	}
	if err := qtx.InsertEvent(ctx, db.InsertEventParams{
		OwnerID: ownerID, Pts: b.Pts, Type: int16(EventNewMessage), LocalID: b.LocalID,
	}); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return b.LocalID, nil
}

// SetChannelCaps lowers one Store's channel bounds so the cap branches can be
// exercised at their boundary instead of by writing 10 000 participant rows.
// Scoped to this Store on purpose: a package-level override would leak into
// every other test in a parallel run.
func SetChannelCaps(s *Store, participants, perUser int) {
	s.maxChannelParticipants = participants
	s.maxChannelsPerUser = perUser
}

// ChannelIDMin and ChannelIDMax expose the documented channel-id range, so the
// allocation tests assert against the constants the draw is taken from rather
// than a second transcription of the numbers.
const (
	ChannelIDMin = minChannelID
	ChannelIDMax = maxChannelID
	// ChannelIDAttempts is the redraw bound, exported so the exhaustion test
	// asserts the shipped number instead of a copy of it that can drift.
	ChannelIDAttempts = channelIDAttempts
)

// SetChannelIDSource replaces one Store's channel-id draw. Both branches it
// reaches — the redraw after a collision and the fail-closed path when the draw
// errors — are unreachable through crypto/rand at any test's scale. Scoped to
// this Store on purpose: a package-level override would leak into every other
// test in a parallel run.
func SetChannelIDSource(s *Store, fn func() (int64, error)) { s.newChannelID = fn }

// SetChannelPts forces a channel's pts, so the join path can be asserted
// against a non-zero sequence without a message-send path that does not exist
// until the next ticket.
func SetChannelPts(ctx context.Context, s *Store, channelID, pts int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE channel_state SET pts = $2 WHERE channel_id = $1`, channelID, pts)
	return err
}

// SetChannelBan writes a participant's banned_until directly. Ban mutation is a
// later ticket's RPC; this exists so the read and re-join paths can be tested
// against a banned row today.
func SetChannelBan(ctx context.Context, s *Store, channelID, userID int64, until *time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_participants SET banned_until = $3 WHERE channel_id = $1 AND user_id = $2`,
		channelID, userID, until)
	return err
}

// SetChannelRole writes a participant's role directly. Promotion is a later
// ticket's RPC; this exists so the post-rights check can be tested against an
// admin row today.
func SetChannelRole(ctx context.Context, s *Store, channelID, userID int64, role int16) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_participants SET role = $3 WHERE channel_id = $1 AND user_id = $2`,
		channelID, userID, role)
	return err
}

// SetChannelBanInfinite writes banned_until = 'infinity'. It is separate from
// SetChannelBan because no Go time.Time encodes to infinity, and infinity is the
// value the ban decode has to survive.
func SetChannelBanInfinite(ctx context.Context, s *Store, channelID, userID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_participants SET banned_until = 'infinity' WHERE channel_id = $1 AND user_id = $2`,
		channelID, userID)
	return err
}

// InsertTestEvent bumps the owner's pts and appends one event at the new pts,
// letting update-state tests exercise EventsSince without a full message send.
func InsertTestEvent(ctx context.Context, s *Store, ownerID int64, typ EventType, localID int64) error {
	pts, err := s.q.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return err
	}
	return s.q.InsertEvent(ctx, db.InsertEventParams{
		OwnerID: ownerID,
		Pts:     pts,
		Type:    int16(typ), //nolint:gosec // event types are small constants (1-5)
		LocalID: localID,
	})
}

// SeedChannelWithMember creates a channel owned by creatorID and admits memberID
// to it through the shipped invite path — the only way a participant row is ever
// produced. Returns the channel and the invite hash, so a test can re-join on the
// same hash.
func SeedChannelWithMember(ctx context.Context, s *Store, creatorID, memberID int64) (Channel, string, error) {
	ch, err := s.CreateChannel(ctx, creatorID, "test", "", false)
	if err != nil {
		return Channel{}, "", err
	}
	hash, err := s.CreateChannelInvite(ctx, ch.ID, creatorID)
	if err != nil {
		return Channel{}, "", err
	}
	if _, _, err = s.JoinChannelByInvite(ctx, hash, memberID); err != nil {
		return Channel{}, "", err
	}
	return ch, hash, nil
}

// SeedChannelPost creates a channel, joins memberID to it through the shipped
// invite path, and posts one message carrying fileID. Returns the channel id and
// the post's local_id.
func SeedChannelPost(ctx context.Context, s *Store, creatorID, memberID, fileID int64) (channelID, localID int64, err error) {
	ch, _, err := SeedChannelWithMember(ctx, s, creatorID, memberID)
	if err != nil {
		return 0, 0, err
	}
	post, _, _, err := s.PostChannelMessage(ctx, ch.ID, creatorID, "post", 0, &fileID)
	if err != nil {
		return 0, 0, err
	}
	return ch.ID, post.LocalID, nil
}

// SetChannelPostDeleted soft-deletes a channel post, the state the channel
// branch of the download gate has to treat as revocation.
func SetChannelPostDeleted(ctx context.Context, s *Store, channelID, localID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`,
		channelID, localID)
	return err
}

// SetMessageDeleted soft-deletes a message row by (owner, local_id).
func SetMessageDeleted(ctx context.Context, s *Store, ownerID, localID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE messages SET deleted = true WHERE owner_id = $1 AND local_id = $2`,
		ownerID, localID)
	return err
}

// JoinChannelMember admits userID to an existing channel through the shipped
// invite path. Returns the invite hash used.
func JoinChannelMember(ctx context.Context, s *Store, channelID, userID int64) (string, error) {
	hash, err := s.CreateChannelInvite(ctx, channelID, userID)
	if err != nil {
		return "", err
	}
	if _, _, err = s.JoinChannelByInvite(ctx, hash, userID); err != nil {
		return "", err
	}
	return hash, nil
}

// HoldInviteRowLock takes the row lock on one invite hash in a transaction of
// its own and holds it until release is called. Used by the concurrent
// join/revoke tests to control which transaction commits first.
func HoldInviteRowLock(ctx context.Context, s *Store, hash string) (release func(), err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SELECT 1 FROM channel_invites WHERE hash = $1 FOR UPDATE", hash); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // best effort
		return nil, err
	}
	return func() { _ = tx.Rollback(ctx) }, nil //nolint:errcheck // nothing to commit
}

// StorePool returns the Store's pgxpool.Pool for tests that need a raw
// connection independent of the Store's query layer.
func StorePool(s *Store) *pgxpool.Pool { return s.pool }

// EraseFileRow deletes one files row. Nothing in the shipped server deletes one
// — the eraser is a later stage of M17 — so this is the only way a test can
// reach the state the reference interlock fails closed on.
func EraseFileRow(ctx context.Context, s *Store, fileID int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM files WHERE id = $1`, fileID)
	if err != nil {
		return err
	}
	// A wrong id would otherwise leave the row in place and the test asserting
	// against a file that is still perfectly referenceable.
	if n := tag.RowsAffected(); n != 1 {
		return fmt.Errorf("erase file row: %d rows for id %d, want 1", n, fileID)
	}
	return nil
}

// FileRowHold is an open transaction holding one files row, standing in for the
// eraser that does not exist yet. It is how a test controls the moment a
// reference insert is allowed past the row.
type FileRowHold struct {
	tx pgx.Tx
	id int64
}

// HoldFileRow takes fileID's files row FOR UPDATE in a transaction of its own
// and holds it until the caller releases or erases it.
func HoldFileRow(ctx context.Context, s *Store, fileID int64) (*FileRowHold, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var got int64
	if err := tx.QueryRow(ctx, `SELECT id FROM files WHERE id = $1 FOR UPDATE`, fileID).Scan(&got); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // best effort on the error path
		return nil, err
	}
	return &FileRowHold{tx: tx, id: fileID}, nil
}

// Release drops the lock without touching the row. It takes no context so a
// test can defer it, and it is safe to call twice — a rollback of a finished
// transaction is a no-op — so a test can also release early.
func (h *FileRowHold) Release() {
	_ = h.tx.Rollback(context.Background()) //nolint:errcheck // nothing to commit
}

// EraseAndCommit deletes the held row and commits, which is the sequence the
// eraser will perform and the one a concurrent reference insert has to lose to.
func (h *FileRowHold) EraseAndCommit(ctx context.Context) error {
	if _, err := h.tx.Exec(ctx, `DELETE FROM files WHERE id = $1`, h.id); err != nil {
		return err
	}
	return h.tx.Commit(ctx)
}

// HoldFileRowShared takes fileID's files row FOR SHARE — the same mode a
// reference insert takes — and holds it until release is called. It is how a
// test asserts that two references to one file are concurrent rather than
// serialized.
func HoldFileRowShared(ctx context.Context, s *Store, fileID int64) (release func(), err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var got int64
	if err := tx.QueryRow(ctx, `SELECT id FROM files WHERE id = $1 FOR SHARE`, fileID).Scan(&got); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // best effort on the error path
		return nil, err
	}
	return func() { _ = tx.Rollback(ctx) }, nil //nolint:errcheck // nothing to commit
}

// FilesTableAccesses reports how many scans Postgres has recorded against the
// files table in this test's database. "The text path issues no extra query" is
// a statement about what reached the table, and this is the only place that is
// observable without instrumenting the pool.
//
// Each backend buffers its own statistics and flushes them on a timer that runs
// to ten seconds when idle, so reading the view straight after the work under
// test measures nothing and reports zero — a negative assertion built on that
// would pass on any code at all. Forcing the flush on every pooled connection
// first is what makes this a measurement: the work ran on those connections,
// and by the time each SELECT returns its backend has reported.
//
// Safe to read per test because internal/pgtest clones one database per test,
// and these counters are keyed by database.
func FilesTableAccesses(ctx context.Context, s *Store) (int64, error) {
	for _, c := range s.pool.AcquireAllIdle(ctx) {
		_, err := c.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
		c.Release()
		if err != nil {
			return 0, err
		}
	}
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0)
		   FROM pg_stat_user_tables WHERE relname = 'files'`).Scan(&n)
	// No row means no backend has yet reported a scan of the table, which is
	// zero accesses, not an error.
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// DeleteExpiredUploadPartsPass runs ONE bounded sweep pass. The drain is the
// shipped entry point and is tested through SweepExpiredUploadParts; this is
// how the per-statement bound itself — "a pass takes at most batch rows" — is
// asserted, which a drain hides by design.
func DeleteExpiredUploadPartsPass(ctx context.Context, s *Store, cutoff time.Time, batch int) (int64, error) {
	return s.deleteExpiredUploadParts(ctx, cutoff, batch)
}

// UploadPartDate returns one upload part's stored date — the column the TTL
// sweep compares against. Tests read it back because "the re-save did not move
// the expiry clock" is a statement about this column, and a wall-clock
// measurement in the test process cannot distinguish it from a fast run.
func UploadPartDate(ctx context.Context, s *Store, userID, fileID int64, partIndex int32) (time.Time, error) {
	var at time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT date FROM upload_parts WHERE user_id = $1 AND file_id = $2 AND part_index = $3`,
		userID, fileID, partIndex).Scan(&at)
	return at, err
}

// CountRateLimits returns the number of rate limit rows for a given subject,
// for tests that need to assert the rate_limits table state.
func CountRateLimits(ctx context.Context, s *Store, subjectID int64) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, "SELECT count(*) FROM rate_limits WHERE subject_id = $1", subjectID).Scan(&count)
	return count, err
}

// WaitForLockWaiters blocks until n backends in this test's database are parked
// on a lock. The concurrent join/revoke tests depend on the order goroutines
// enter Postgres's lock queue, and that order is only decided once a statement
// has actually reached the lock — a sleep guesses at when that happened and
// guesses wrong on a loaded machine, admitting the second goroutine first.
// Waiting on the observable state makes the queue order deterministic.
//
// Safe to filter by database because internal/pgtest clones one database per
// test, so the only backends here are this test's.
func WaitForLockWaiters(ctx context.Context, s *Store, n int) error {
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		var got int
		err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&got)
		if err != nil {
			return err
		}
		if got >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waited %s for %d lock waiters, saw %d", timeout, n, got)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
