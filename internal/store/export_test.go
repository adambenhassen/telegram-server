package store

import (
	"context"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Reconnect pacing, exported so the flapping and idle-recovery cases can be
// asserted on the rule itself instead of on a 30-second wall clock.
func NextBackoff(prev, uptime time.Duration) time.Duration { return nextBackoff(prev, uptime) }

const (
	ListenerBackoffMin = listenerBackoffMin
	ListenerBackoffMax = listenerBackoffMax
	ListenerStableFor  = listenerStableFor
)

// RemoveChatParticipant drops a participant under the chats row lock, standing in
// for the membership mutation that owns that operation for real so the fan-out's
// snapshot ordering can be tested against a concurrent removal. Raw SQL rather
// than a generated query on purpose: the shipped delete belongs to the mutation
// ticket, and this must not become the thing it is built on.
func RemoveChatParticipant(ctx context.Context, s *Store, chatID, userID int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if _, err := s.q.WithTx(tx).ChatByIDForUpdate(ctx, chatID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM chat_participants WHERE chat_id = $1 AND user_id = $2`, chatID, userID,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
