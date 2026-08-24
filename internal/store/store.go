// Package store persists users and login codes in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/keycrypt"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Store is the Postgres-backed persistence layer.
type Store struct {
	pool   *pgxpool.Pool
	q      *db.Queries
	cipher *keycrypt.Cipher
	// blobs is the backend in-flight upload part bytes go to; the parts rows
	// account for them. Every Store owns one.
	blobs blob.Store

	// log carries the store's own diagnostics: the states a write reaches that
	// no return value reports. Never nil — Open defaults it to slog.Default(),
	// so a caller that passes no logger still gets those records somewhere
	// rather than silence.
	log *slog.Logger

	// Channel bounds, seeded from the defaults in channels.go. They are fields
	// rather than constants only so a test can exercise the cap branches without
	// writing 10 000 rows; a test lowering them on its own Store cannot disturb
	// another test's Store, which package-level vars would not give under
	// t.Parallel().
	maxChannelParticipants int
	maxChannelsPerUser     int

	// newChannelID draws a new channel's id, seeded with randomChannelID. It is
	// a field for the reason the bounds above are: the collision retry and the
	// fail-closed branch are unreachable through crypto/rand at any test's
	// scale, and a test substituting a draw on its own Store leaves every other
	// Store in a parallel run alone.
	newChannelID func() (int64, error)

	// deniedHook is a test-only callback fired in CheckRateLimit after the
	// INSERT denial and before the GET. Scoped to the Store so parallel tests
	// each own their own hook without racing.
	deniedHook func()

	// searchPageHook is a test-only callback fired in SearchGlobal between the
	// key read and the body read, the window a delete or a ban lands in. Scoped
	// to the Store for the reason deniedHook is.
	searchPageHook func()

	// eraseHook is a test-only callback fired in SweepMediaErasure between the
	// scan that names a candidate and the transaction that erases it, carrying
	// the file id. That gap is where every race this pass has to survive lands —
	// a forward, a fresh send, a channel post — and it is not otherwise
	// reachable: a test that raced the sweep from a goroutine would be asserting
	// on whichever side the scheduler happened to run first. Scoped to the Store
	// for the reason deniedHook is.
	eraseHook func(fileID int64)

	// now reads the clock the client-visible rate-limit wait is measured
	// against. Production always holds time.Now; it is a field so a test can
	// pin the remainder of an open window to an exact sub-second value instead
	// of racing a real one, which on a loaded host closes before the assertion
	// runs. Scoped to the Store for the reason deniedHook is.
	now func() time.Time

	// statementTimeout, when non-zero, is applied as a session default on every
	// pooled connection at connect time. It lives on the Store only so Open can
	// read it after the options run; see WithStatementTimeout.
	statementTimeout time.Duration
}

// Sentinel errors returned by the login-code methods.
var (
	ErrCodeInvalid = errors.New("phone code invalid")
	ErrCodeExpired = errors.New("phone code expired")
	// ErrCodeExhausted is returned by VerifyCode when the per-code attempt cap
	// has been reached; the code can no longer be verified.
	ErrCodeExhausted = errors.New("phone code exhausted")
	// ErrResendTooSoon is returned by IssueCode when an active code was issued
	// within the resend cooldown window.
	ErrResendTooSoon = errors.New("phone code resend too soon")
	// ErrAuthKeyNotFound is returned when an auth-key operation matches no row.
	ErrAuthKeyNotFound = errors.New("auth key not found")
	// ErrChatFull is returned when a membership change would take a chat past
	// maxChatParticipants.
	ErrChatFull = errors.New("chat participants limit reached")
	// ErrNotMember is returned when a chat write is attempted by a user who is
	// not a participant of the chat, and by an absent chat id — the two are
	// deliberately indistinguishable.
	ErrNotMember = errors.New("not a chat member")
	// ErrChannelFull is returned when a join would take a channel past
	// maxChannelParticipants.
	ErrChannelFull = errors.New("channel participants limit reached")
	// ErrTooManyChannels is returned when a join would take an account past
	// maxChannelsPerUser.
	ErrTooManyChannels = errors.New("channel limit per account reached")
	// ErrInviteInvalid is returned by JoinChannelByInvite for every rejection
	// that depends on the hash: one that does not exist, and one whose channel is
	// gone. They are one error on purpose — a distinguishable set makes the invite
	// space probeable, and the hash is the whole admission boundary.
	ErrInviteInvalid = errors.New("channel invite invalid")
)

// Option configures a Store at Open time.
type Option func(*Store)

// WithStatementTimeout sets the Postgres statement_timeout every connection in
// the pool runs under: a single SQL statement that runs longer is cancelled by
// the server, and its transaction rolls back, while the session itself
// survives. It is the second half of the bound on how long one RPC can hold a
// transaction open — the per-request deadline in internal/mtproto bounds the
// handler from the client side; this bounds each statement from the database
// side, catching a wedged statement even where no request context flows.
//
// It is a ceiling on one statement, not on an RPC, and it deliberately applies
// to everything that shares the pool, sweeps included — which is why the
// default sits far above any measured legitimate statement, including the
// media-erasure report's worst measured batch (2.2s at ~300k media messages).
// Zero disables it.
func WithStatementTimeout(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.statementTimeout = d
		}
	}
}

// WithBlobStore wires the blob backend the Store uses for in-flight upload
// part bytes.
func WithBlobStore(blobs blob.Store) Option {
	return func(s *Store) {
		s.blobs = blobs
	}
}

// WithLogger routes the store's diagnostics to log instead of slog.Default().
// It is per-Store rather than a package-level setting so that two Stores in one
// process — every parallel test in this package — cannot write into each
// other's handler.
func WithLogger(log *slog.Logger) Option {
	return func(s *Store) {
		if log != nil {
			s.log = log
		}
	}
}

// Open connects to Postgres and verifies the schema is migrated. encKey is the
// 32-byte master key used to encrypt auth keys at rest. The schema is owned by
// the Atlas migrations; Open does not apply them, but fails fast if the target
// database has not been migrated.
func Open(ctx context.Context, dsn string, encKey []byte, opts ...Option) (*Store, error) {
	cipher, err := keycrypt.New(encKey)
	if err != nil {
		return nil, err
	}
	s := &Store{
		cipher:                 cipher,
		maxChannelParticipants: defaultMaxChannelParticipants,
		maxChannelsPerUser:     defaultMaxChannelsPerUser,
		newChannelID:           randomChannelID,
		log:                    slog.Default(),
		now:                    time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	// Session default rather than SET per statement: one line at connect, and
	// every query on the connection carries the ceiling with no per-call cost.
	if s.statementTimeout > 0 {
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(s.statementTimeout.Milliseconds(), 10)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	s.pool = pool
	s.q = db.New(pool)
	// Required rather than defaulted: the part bytes live in the blob backend,
	// so a Store without one cannot serve an upload at all, and there is no
	// directory this package could pick on a caller's behalf that would be
	// right. Left to default it, the miss surfaces as a nil dereference on the
	// first saveFilePart a running server takes.
	if s.blobs == nil {
		pool.Close()
		return nil, errors.New("store: no blob backend configured; pass WithBlobStore")
	}
	// The schema check runs on its own throwaway connection rather than the
	// pool, so it never sits under the session defaults the pool carries — a
	// short WithStatementTimeout is a ceiling on request work, and startup must
	// not fail on a loaded host merely because its own setup query outlived it.
	setupConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("schema check connect: %w", err)
	}
	err = s.checkSchema(ctx, setupConn)
	if cerr := setupConn.Close(ctx); cerr != nil && err == nil {
		err = fmt.Errorf("schema check close: %w", cerr)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// queryRower is the one method of pgxpool.Pool and pgx.Conn checkSchema needs.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// checkSchema fails fast when the database has not been migrated to the current
// version, turning a confusing runtime "column does not exist" into a clear
// startup error.
//
// ponytail: presence-check of the newest migration's artifacts, not a version
// table — pgtest applies raw SQL and has no Atlas revisions table to read. The
// sentinels track the latest migration (messages_file_idx) plus the ones before
// it; update them when a migration adds new schema.
//
// Indexes count as schema here. A database that stops short of one still opens
// clean on every column check and then plans a different query — the media
// reference predicate silently degrades to a per-row Seq Scan of messages —
// so a missing index is exactly the un-migrated state this is here to refuse.
func (s *Store) checkSchema(ctx context.Context, q queryRower) error {
	var hasParticipants, hasFanoutID, hasEvents, hasUserStatus, hasEncryptedEvents, hasFwdFromID, hasReactions, hasPinnedChat, hasPinnedChannel, hasNameTsv, hasRateLimits, hasSendCodeIP, hasSignInFail, hasLoginMode, hasAdminSessions, hasPartSize, hasPartBlobKey, hasPartPayload, hasMessageFileIdx bool
	err := q.QueryRow(ctx, `
		SELECT to_regclass('public.chat_participants') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'messages' AND column_name = 'fanout_id'),
		       to_regclass('public.message_events') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'users' AND column_name = 'is_online'),
		       to_regclass('public.encrypted_events') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'messages' AND column_name = 'fwd_from_id'),
		       to_regclass('public.message_reactions') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'chats' AND column_name = 'pinned_message_id'),
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'channels' AND column_name = 'pinned_message_id'),
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'users' AND column_name = 'name_tsv'),
		       to_regclass('public.rate_limits') IS NOT NULL,
		       to_regclass('public.send_code_ip_calls') IS NOT NULL,
		       to_regclass('public.sign_in_fail_calls') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'users' AND column_name = 'login_mode'),
		       to_regclass('public.admin_sessions') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'upload_parts' AND column_name = 'size'),
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'upload_parts' AND column_name = 'blob_key'),
		       EXISTS(SELECT 1 FROM information_schema.columns
		                   WHERE table_name = 'upload_parts' AND column_name = 'payload'),
		       to_regclass('public.messages_file_idx') IS NOT NULL`,
	).Scan(&hasParticipants, &hasFanoutID, &hasEvents, &hasUserStatus, &hasEncryptedEvents, &hasFwdFromID, &hasReactions, &hasPinnedChat, &hasPinnedChannel, &hasNameTsv, &hasRateLimits, &hasSendCodeIP, &hasSignInFail, &hasLoginMode, &hasAdminSessions, &hasPartSize, &hasPartBlobKey, &hasPartPayload, &hasMessageFileIdx)
	if err != nil {
		return fmt.Errorf("schema check: %w", err)
	}
	if !hasParticipants || !hasFanoutID || !hasEvents || !hasUserStatus || !hasEncryptedEvents || !hasFwdFromID || !hasReactions || !hasPinnedChat || !hasPinnedChannel || !hasNameTsv || !hasRateLimits || !hasSendCodeIP || !hasSignInFail || !hasLoginMode || !hasAdminSessions || !hasPartSize || !hasPartBlobKey || hasPartPayload || !hasMessageFileIdx {
		return errors.New("database schema is not migrated; run: atlas migrate apply --env local")
	}
	return nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
