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
