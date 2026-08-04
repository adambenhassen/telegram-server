package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Secret chat lifecycle states, matching the secret_chats.state CHECK constraint.
// 'discarded' is terminal: nothing leaves it, and the id is never reused.
const (
	SecretChatRequested = "requested"
	SecretChatActive    = "active"
	SecretChatDiscarded = "discarded"
)

// MaxOutstandingSecretChats caps how many 'requested' rows one account may hold
// as admin. Each pending row is a durable allocation plus a push to the
// responder, so an uncapped requestEncryption is a spam and storage amplifier on
// an authenticated endpoint. Inbound requests are not counted: the cap must not
// let one account exhaust another's ability to start chats.
const MaxOutstandingSecretChats = 10

// Sentinel errors returned by the secret chat methods.
var (
	// ErrSecretChatsTooMany is returned when a request would take the caller
	// past MaxOutstandingSecretChats.
	ErrSecretChatsTooMany = errors.New("too many outstanding secret chat requests")
	// ErrSecretChatNotFound is returned when no secret_chats row has that id.
	ErrSecretChatNotFound = errors.New("secret chat not found")
	// ErrSecretChatStale is returned when a guarded transition matched no row:
	// the chat is no longer in the state the transition requires, or the caller
	// is not the party allowed to make it.
	ErrSecretChatStale = errors.New("secret chat state changed")
)

// SecretChat is one key-exchange row. GAHash, GA, GAOrB are nil until the step
// that fills them; KeyFingerprint is zero until accept.
type SecretChat struct {
	ID             int32
	AdminID        int64
	ParticipantID  int64
	State          string
	GAHash         []byte
	GA             []byte
	GAOrB          []byte
	KeyFingerprint int64
	RandomID       int64
	Date           time.Time
}

func secretChatFromRow(r db.SecretChat) SecretChat {
	c := SecretChat{
		ID:            r.ID,
		AdminID:       r.AdminID,
		ParticipantID: r.ParticipantID,
		State:         r.State,
		GAHash:        r.GAHash,
		GA:            r.GA,
		GAOrB:         r.GAOrB,
		RandomID:      r.RandomID,
		Date:          r.Date.Time,
	}
	if r.KeyFingerprint != nil {
		c.KeyFingerprint = *r.KeyFingerprint
	}
	return c
}

// Party reports whether userID is one of the chat's two participants. Every
// authorization decision on a secret chat is this membership plus, where the
// step is one-sided, an explicit comparison against AdminID or ParticipantID.
func (c SecretChat) Party(userID int64) bool {
	return userID == c.AdminID || userID == c.ParticipantID
}

// Other returns the id of the party that is not userID. It is what a push is
// addressed to, so it is only meaningful for a userID that is a party.
func (c SecretChat) Other(userID int64) int64 {
	if userID == c.AdminID {
		return c.ParticipantID
	}
	return c.AdminID
}

// CreateSecretChatRequest allocates a chat id and writes the 'requested' row for
// adminID, storing both g_a and its SHA-256. It returns ErrSecretChatsTooMany
// when adminID already holds MaxOutstandingSecretChats outstanding requests.
//
// When randomID is non-zero, a prior row with the same (adminID, randomID) is
// returned instead of creating a new one. This implements client-side dedup: a
// retried requestEncryption with the same random_id reuses the original row,
// fires no second push, and consumes no additional cap.
//
// The count and the insert share a transaction serialised on adminID by an
// advisory lock, so concurrent requests from one account cannot each read the
// same pre-insert count and commit past the cap. The lock is taken on the
// caller's own user id and released at commit; it is a leaf, never held across
// another lock, and never taken by the message fan-out, so it takes no position
// relative to writeMu in internal/mtproto/send.go.
func (s *Store) CreateSecretChatRequest(ctx context.Context, adminID, participantID int64, gA, gAHash []byte, randomID int64) (SecretChat, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SecretChat{}, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", adminID); err != nil {
		return SecretChat{}, false, fmt.Errorf("advisory lock: %w", err)
	}
	qtx := s.q.WithTx(tx)

	// Dedup lookup: if the caller already has a 'requested' row with this
	// random_id, return it. Skips cap check and insert.
	if randomID != 0 {
		existing, err := qtx.GetSecretChatByAdminRandomID(ctx, db.GetSecretChatByAdminRandomIDParams{
			AdminID:  adminID,
			RandomID: randomID,
		})
		if err == nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				return SecretChat{}, false, fmt.Errorf("rollback after dedup: %w", rbErr)
			}
			return secretChatFromRow(existing), true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return SecretChat{}, false, fmt.Errorf("dedup lookup: %w", err)
		}
	}

	outstanding, err := qtx.CountRequestedSecretChats(ctx, adminID)
	if err != nil {
		return SecretChat{}, false, fmt.Errorf("count requested: %w", err)
	}
	if outstanding >= MaxOutstandingSecretChats {
		return SecretChat{}, false, ErrSecretChatsTooMany
	}

	id, err := qtx.NextSecretChatID(ctx)
	if err != nil {
		return SecretChat{}, false, fmt.Errorf("next id: %w", err)
	}
	// Chat ids are int32 on the wire. Exhausting the sequence is not a client
	// error and must not be reported as one, so it fails loudly here rather than
	// as a truncated id.
	if id > math.MaxInt32 {
		return SecretChat{}, false, fmt.Errorf("secret chat id sequence exhausted: %d", id)
	}

	row, err := qtx.InsertSecretChat(ctx, db.InsertSecretChatParams{
		ID:            int32(id), //nolint:gosec // bounded above
		AdminID:       adminID,
		ParticipantID: participantID,
		GAHash:        gAHash,
		GA:            gA,
		RandomID:      randomID,
	})
	if err != nil {
		return SecretChat{}, false, fmt.Errorf("insert secret chat: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SecretChat{}, false, fmt.Errorf("commit: %w", err)
	}
	return secretChatFromRow(row), false, nil
}

// SecretChatByID loads one chat. A missing row is ErrSecretChatNotFound.
func (s *Store) SecretChatByID(ctx context.Context, id int32) (SecretChat, error) {
	row, err := s.q.SecretChatByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecretChat{}, ErrSecretChatNotFound
	}
	if err != nil {
		return SecretChat{}, fmt.Errorf("secret chat by id: %w", err)
	}
	return secretChatFromRow(row), nil
}

// AcceptSecretChat moves a 'requested' chat to 'active' in one guarded
// statement, storing g_b and the acceptor's key fingerprint. participantID must
// be the responder; the initiator matches no row.
//
// ErrSecretChatStale means the guard rejected the transition — the chat was
// already accepted, already discarded, or the caller is not the responder. The
// guard, not a prior read, is what makes a replayed accept safe: a second one
// that succeeded would rewrite the fingerprint under a chat the initiator has
// already keyed.
func (s *Store) AcceptSecretChat(ctx context.Context, id int32, participantID int64, gB []byte, keyFingerprint int64) (SecretChat, error) {
	row, err := s.q.AcceptSecretChat(ctx, db.AcceptSecretChatParams{
		ID:             id,
		ParticipantID:  participantID,
		GAOrB:          gB,
		KeyFingerprint: &keyFingerprint,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return SecretChat{}, ErrSecretChatStale
	}
	if err != nil {
		return SecretChat{}, fmt.Errorf("accept secret chat: %w", err)
	}
	return secretChatFromRow(row), nil
}

// DiscardSecretChat moves a 'requested' or 'active' chat to 'discarded'. An
// already-discarded chat matches no row and reports ErrSecretChatStale, which
// the caller turns into the idempotent success discard owes a client.
//
// It deliberately does not check who the caller is: authorization is one rule
// (either party may discard) and belongs with the handler that already loaded
// the row to decide whom to notify.
func (s *Store) DiscardSecretChat(ctx context.Context, id int32) (SecretChat, error) {
	row, err := s.q.DiscardSecretChat(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecretChat{}, ErrSecretChatStale
	}
	if err != nil {
		return SecretChat{}, fmt.Errorf("discard secret chat: %w", err)
	}
	return secretChatFromRow(row), nil
}

// EncryptedEvent is one per-recipient event row.
type EncryptedEvent struct {
	OwnerID  int64
	Qts      int
	ChatID   int32
	RandomID int64
	Bytes    []byte
	Date     time.Time
}

// EncryptedSend holds the inputs to SendEncryptedMessage.
type EncryptedSend struct {
	RecipientID int64
	ChatID      int32
	RandomID    int64
	Data        []byte
}

// SendEncryptedMessage atomically inserts the encrypted payload for the
// recipient and increments their qts. Returns dup=true when the random_id
// was already stored (idempotent dedup); the returned event carries the
// original date. Returns dup=false on a new insert; the event carries the
// new qts.
//
// An advisory lock on RecipientID serialises concurrent sends so the
// (owner_id, qts) primary key is never contended. The lock is advisory and
// scoped to the transaction; it is never held across another lock, and the
// message fan-out path never holds one on the same id, so it is a leaf.
func (s *Store) SendEncryptedMessage(ctx context.Context, p EncryptedSend) (event EncryptedEvent, dup bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EncryptedEvent{}, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", p.RecipientID); err != nil {
		return EncryptedEvent{}, false, fmt.Errorf("advisory lock: %w", err)
	}
	qtx := s.q.WithTx(tx)

	row, err := qtx.InsertEncryptedEvent(ctx, db.InsertEncryptedEventParams{
		OwnerID:  p.RecipientID,
		ChatID:   p.ChatID,
		RandomID: p.RandomID,
		Bytes:    p.Data,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// Dedup: same random_id already stored; roll back without bumping qts.
		_ = tx.Rollback(ctx) //nolint:errcheck // no-op if already rolled back
		existing, lookupErr := s.q.GetEncryptedEventByRandomID(ctx, db.GetEncryptedEventByRandomIDParams{
			OwnerID:  p.RecipientID,
			RandomID: p.RandomID,
		})
		if lookupErr != nil {
			return EncryptedEvent{}, false, fmt.Errorf("dedup lookup: %w", lookupErr)
		}
		return encryptedEventFromRow(existing), true, nil
	}
	if err != nil {
		return EncryptedEvent{}, false, fmt.Errorf("insert encrypted event: %w", err)
	}

	if _, err := qtx.BumpQts(ctx, p.RecipientID); err != nil {
		return EncryptedEvent{}, false, fmt.Errorf("bump qts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return EncryptedEvent{}, false, fmt.Errorf("commit: %w", err)
	}
	return encryptedEventFromRow(row), false, nil
}

// GetEncryptedEvent loads one event by its (ownerID, qts) primary key.
// Used by the push handler to build updateNewEncryptedMessage from a NOTIFY.
func (s *Store) GetEncryptedEvent(ctx context.Context, ownerID int64, qts int) (EncryptedEvent, error) {
	row, err := s.q.GetEncryptedEvent(ctx, db.GetEncryptedEventParams{
		OwnerID: ownerID,
		Qts:     int64(qts),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return EncryptedEvent{}, fmt.Errorf("encrypted event not found: owner=%d qts=%d", ownerID, qts)
	}
	if err != nil {
		return EncryptedEvent{}, fmt.Errorf("get encrypted event: %w", err)
	}
	return encryptedEventFromRow(row), nil
}

func encryptedEventFromRow(r db.EncryptedEvent) EncryptedEvent {
	return EncryptedEvent{
		OwnerID:  r.OwnerID,
		Qts:      int(r.Qts),
		ChatID:   r.ChatID,
		RandomID: r.RandomID,
		Bytes:    r.Bytes,
		Date:     r.Date.Time,
	}
}
