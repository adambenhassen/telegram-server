// Package store persists users and login codes in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Store is the Postgres-backed persistence layer.
type Store struct {
	pool   *pgxpool.Pool
	q      *db.Queries
	cipher *keycrypt.Cipher

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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	s := &Store{
		pool:                   pool,
		q:                      db.New(pool),
		cipher:                 cipher,
		maxChannelParticipants: defaultMaxChannelParticipants,
		maxChannelsPerUser:     defaultMaxChannelsPerUser,
		newChannelID:           randomChannelID,
		log:                    slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := s.checkSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// checkSchema fails fast when the database has not been migrated to the current
// version, turning a confusing runtime "column does not exist" into a clear
// startup error.
//
// ponytail: presence-check of the newest migration's artifacts, not a version
// table — pgtest applies raw SQL and has no Atlas revisions table to read. The
// sentinels track the latest migration (pinned_message_id on chats/channels) plus
// chat_participants, messages.fanout_id and message_events from the ones
// before it; update them when a migration adds new schema.
func (s *Store) checkSchema(ctx context.Context) error {
	var hasParticipants, hasFanoutID, hasEvents, hasUserStatus, hasEncryptedEvents, hasFwdFromID, hasReactions, hasPinnedChat, hasPinnedChannel, hasNameTsv, hasRateLimits, hasSendCodeIP bool
	err := s.pool.QueryRow(ctx, `
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
		       to_regclass('public.send_code_ip_calls') IS NOT NULL`,
	).Scan(&hasParticipants, &hasFanoutID, &hasEvents, &hasUserStatus, &hasEncryptedEvents, &hasFwdFromID, &hasReactions, &hasPinnedChat, &hasPinnedChannel, &hasNameTsv, &hasRateLimits, &hasSendCodeIP)
	if err != nil {
		return fmt.Errorf("schema check: %w", err)
	}
	if !hasParticipants || !hasFanoutID || !hasEvents || !hasUserStatus || !hasEncryptedEvents || !hasFwdFromID || !hasReactions || !hasPinnedChat || !hasPinnedChannel || !hasNameTsv || !hasRateLimits || !hasSendCodeIP {
		return errors.New("database schema is not migrated; run: atlas migrate apply --env local")
	}
	return nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
