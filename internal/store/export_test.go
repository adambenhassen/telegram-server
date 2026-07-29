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

// SeedChannelPost creates a channel with creatorID as its owner, adds memberID
// as a participant, and posts one message carrying fileID. The M7 channel write
// path does not exist yet, so the download gate's channel branch has no shipped
// way to produce its rows; this writes exactly the three the branch reads.
// Returns the channel id and the post's local_id.
func SeedChannelPost(ctx context.Context, s *Store, creatorID, memberID, fileID int64) (channelID, localID int64, err error) {
	if err = s.pool.QueryRow(ctx,
		`INSERT INTO channels (title, creator_id) VALUES ('test', $1) RETURNING id`,
		creatorID).Scan(&channelID); err != nil {
		return 0, 0, err
	}
	if _, err = s.pool.Exec(ctx,
		`INSERT INTO channel_participants (channel_id, user_id) VALUES ($1, $2)`,
		channelID, memberID); err != nil {
		return 0, 0, err
	}
	localID = 1
	if _, err = s.pool.Exec(ctx,
		`INSERT INTO channel_messages (channel_id, local_id, from_id, message, file_id)
		 VALUES ($1, $2, $3, 'post', $4)`,
		channelID, localID, creatorID, fileID); err != nil {
		return 0, 0, err
	}
	return channelID, localID, nil
}

// SetChannelBan sets a participant's banned_until; a nil time clears the ban.
func SetChannelBan(ctx context.Context, s *Store, channelID, userID int64, until *time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_participants SET banned_until = $3 WHERE channel_id = $1 AND user_id = $2`,
		channelID, userID, until)
	return err
}

// SetChannelPostDeleted soft-deletes a channel post, the state the channel
// branch of the download gate has to treat as revocation.
func SetChannelPostDeleted(ctx context.Context, s *Store, channelID, localID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`,
		channelID, localID)
	return err
}
