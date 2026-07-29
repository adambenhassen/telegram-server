package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// A channel_state row lock (SELECT ... FROM channel_state WHERE channel_id = $1
// FOR UPDATE) is the serialisation point for admission to one channel. It is the
// only lock this file introduces: no Go lock, and no advisory lock, so nothing
// here can be part of a lock cycle with the per-owner locks in fanout.go.
//
// CreateChannel does not take it — the channel does not exist yet.
// JoinChannelByInvite takes it and holds it to commit, which is what makes the
// pts it records and the counts the caps are decided on the same snapshot the
// insert lands in.

// defaultMaxChannelParticipants and defaultMaxChannelsPerUser are the bounds
// recorded in the M7 migration. They seed the per-Store fields of the same name
// in Open; nothing but a test overrides them, and a test overriding one Store
// leaves every other Store in a parallel run alone.
//
// Exactness, both on the join path and on the create path: the join path checks
// both under the channel_state row lock, which serialises joins to ONE channel,
// so the per-channel cap is exact and the per-account one is exact per channel —
// two joins to two different channels can both read 499. CreateChannel's check
// has the same property for the same reason, since there is no channel_state row
// to lock before the channel exists. Closing that would take a lock across every
// channel an account touches, which is a far worse trade than an overshoot
// bounded by an account's own concurrency.
const (
	defaultMaxChannelParticipants = 10000 // per channel
	defaultMaxChannelsPerUser     = 500   // per account
)

// inviteHashBytes is the entropy behind one invite. 128 bits is what makes the
// hash space unwalkable, which is the whole of the admission boundary.
const inviteHashBytes = 16

// Channel is a broadcast peer.
type Channel struct {
	ID        int64
	Title     string
	About     string
	CreatorID int64
	Megagroup bool
	Version   int
	Date      time.Time
}

// ChannelMember is one participant row of a channel.
type ChannelMember struct {
	UserID      int64
	Role        int // 0 member, 1 admin, 2 creator
	BannedUntil *time.Time
	JoinPts     int
}

// bannedForever stands in for a banned_until of 'infinity'. pgx decodes that
// value as a zero time.Time carrying an infinity modifier, so dropping the
// modifier on the way into *time.Time would silently turn a permanent ban into
// one that expired in year 1.
var bannedForever = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)

// Banned reports whether the member is under a ban at now. It is the ONE place
// the ban predicate is written: NULL is not banned, and "may act" is
// banned_until IS NULL OR banned_until <= now(). Every read and write path that
// needs the answer calls this rather than re-deriving it, because the inversion
// is what gets written backwards.
func (m ChannelMember) Banned(now time.Time) bool {
	return m.BannedUntil != nil && m.BannedUntil.After(now)
}

// Forever reports whether the member's ban is permanent — banned_until =
// 'infinity'. It exists so bannedForever never has to be recognised outside this
// package: a caller serialising BannedUntil directly would put year 9999 on the
// wire where MTProto wants its own forever ban, and the year is this package's
// stand-in for a value Go's time has no representation for.
func (m ChannelMember) Forever() bool {
	return m.BannedUntil != nil && m.BannedUntil.Equal(bannedForever)
}

func channelFromRow(r db.Channel) Channel {
	return Channel{
		ID:        r.ID,
		Title:     r.Title,
		About:     r.About,
		CreatorID: r.CreatorID,
		Megagroup: r.Megagroup,
		Version:   int(r.Version),
		Date:      r.Date.Time,
	}
}

func channelMemberFromRow(r db.ChannelParticipant) ChannelMember {
	m := ChannelMember{
		UserID:  r.UserID,
		Role:    int(r.Role),
		JoinPts: int(r.JoinPts),
	}
	if r.BannedUntil.Valid {
		t := r.BannedUntil.Time
		if r.BannedUntil.InfinityModifier == pgtype.Infinity {
			t = bannedForever
		}
		m.BannedUntil = &t
	}
	return m
}

// CreateChannel creates a channel owned by creatorID, in one transaction: the
// channels row, its channel_state row, and the creator's participant row at
// role 2 with join_pts 0 — a creator sees the channel's whole history because
// there is none before them. ErrTooManyChannels once the creator already holds
// maxChannelsPerUser participant rows, and then nothing is written: creating is
// the other way an account acquires a row, so leaving the cap to the join path
// would let an account past it by creating instead of joining.
func (s *Store) CreateChannel(ctx context.Context, creatorID int64, title, about string, megagroup bool) (Channel, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Channel{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Same count the join path decides its per-account cap on, and deliberately
	// the same query: two counts of "channels this account is in" would be two
	// definitions of the cap, and they would drift the first time one of them
	// learns to exclude something.
	joined, err := qtx.CountChannelsForUser(ctx, creatorID)
	if err != nil {
		return Channel{}, fmt.Errorf("count channels for user: %w", err)
	}
	if joined >= int64(s.maxChannelsPerUser) {
		return Channel{}, ErrTooManyChannels
	}

	row, err := qtx.InsertChannel(ctx, db.InsertChannelParams{
		Title: title, About: about, CreatorID: creatorID, Megagroup: megagroup,
	})
	if err != nil {
		return Channel{}, fmt.Errorf("insert channel: %w", err)
	}
	if err = qtx.InsertChannelState(ctx, row.ID); err != nil {
		return Channel{}, fmt.Errorf("insert channel state: %w", err)
	}
	if err = qtx.InsertChannelParticipant(ctx, db.InsertChannelParticipantParams{
		ChannelID: row.ID, UserID: creatorID, Role: 2, JoinPts: 0,
	}); err != nil {
		return Channel{}, fmt.Errorf("insert creator participant: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return Channel{}, fmt.Errorf("commit: %w", err)
	}
	return channelFromRow(row), nil
}

// ChannelByID returns one channel; ok=false when absent.
func (s *Store) ChannelByID(ctx context.Context, channelID int64) (Channel, bool, error) {
	r, err := s.q.ChannelByID(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Channel{}, false, nil
	case err != nil:
		return Channel{}, false, fmt.Errorf("channel by id: %w", err)
	}
	return channelFromRow(r), true, nil
}

// ChannelMembers lists the channel's participants ordered by user_id ascending.
func (s *Store) ChannelMembers(ctx context.Context, channelID int64) ([]ChannelMember, error) {
	rows, err := s.q.ChannelParticipants(ctx, channelID)
	if err != nil {
		return nil, fmt.Errorf("channel participants: %w", err)
	}
	out := make([]ChannelMember, len(rows))
	for i, r := range rows {
		out[i] = channelMemberFromRow(r)
	}
	return out, nil
}

// ChannelMemberOf returns userID's participant row of channelID; ok=false when
// there is none. The row is returned as it stands: a ban is data here, not a
// verdict, and the caller decides what it means by calling ChannelMember.Banned.
func (s *Store) ChannelMemberOf(ctx context.Context, channelID, userID int64) (ChannelMember, bool, error) {
	r, err := s.q.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID, UserID: userID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ChannelMember{}, false, nil
	case err != nil:
		return ChannelMember{}, false, fmt.Errorf("channel participant: %w", err)
	}
	return channelMemberFromRow(r), true, nil
}

// CreateChannelInvite issues a bearer invite for the channel: 128 bits from
// crypto/rand, base64url without padding, 22 characters. It fails closed — a
// crypto/rand error is returned, never swallowed and never replaced with a
// fallback draw, because a predictable hash is the admission boundary gone.
//
// It does not check creatorID's rights over the channel; like the rest of this
// package it trusts its caller, and the handler is where that check belongs.
func (s *Store) CreateChannelInvite(ctx context.Context, channelID, creatorID int64) (string, error) {
	var buf [inviteHashBytes]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("invite hash: %w", err)
	}
	hash := base64.RawURLEncoding.EncodeToString(buf[:])

	if err := s.q.InsertChannelInvite(ctx, db.InsertChannelInviteParams{
		Hash: hash, ChannelID: channelID, CreatorID: creatorID,
	}); err != nil {
		return "", fmt.Errorf("insert channel invite: %w", err)
	}
	return hash, nil
}

// JoinChannelByInvite admits userID to the channel the invite hash names. The
// hash is the ONLY input that selects a channel: there is deliberately no
// join-by-channel-id method, because channels.id is dense BIGSERIAL and the peer
// access-hash placeholder is access_hash == id, so an id-keyed join would let any
// account walk 1..N, write its own participant row and read every channel on the
// server. The participant row is an authorization boundary only while the sole
// way to get one is a secret the server issued.
//
// Locking: the channel's channel_state row is taken FOR UPDATE and held to
// commit. That is the only lock taken here, and reading pts under it is what
// stops a post committing between the read and the participant insert — a joiner
// would otherwise be seated at a pts below a message they must not receive, or
// above one they should.
//
// Re-joining is idempotent: an existing row is returned untouched, so join_pts
// never drops and a ban is never cleared by rejoining.
//
// Rejections are deliberately not distinguishable: an unknown hash and a hash
// whose channel is gone both return ErrInviteInvalid, or the invite space becomes
// probeable. ErrChannelFull and ErrTooManyChannels may be distinct because they
// are only reachable with a hash the caller already holds.
func (s *Store) JoinChannelByInvite(ctx context.Context, hash string, userID int64) (Channel, ChannelMember, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	invite, err := qtx.ChannelInviteByHash(ctx, hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Channel{}, ChannelMember{}, ErrInviteInvalid
	case err != nil:
		return Channel{}, ChannelMember{}, fmt.Errorf("channel invite: %w", err)
	}

	state, err := qtx.ChannelStateForUpdate(ctx, invite.ChannelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// An invite outliving its channel is the same rejection as an unknown
		// hash: it says nothing about which ids exist.
		return Channel{}, ChannelMember{}, ErrInviteInvalid
	case err != nil:
		return Channel{}, ChannelMember{}, fmt.Errorf("lock channel state: %w", err)
	}
	channel, err := qtx.ChannelByID(ctx, invite.ChannelID)
	if err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("channel by id: %w", err)
	}

	member, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: invite.ChannelID, UserID: userID,
	})
	switch {
	case err == nil:
		// Already a member. Nothing is written — not the caps' business either,
		// since the count does not move.
		if err = tx.Commit(ctx); err != nil {
			return Channel{}, ChannelMember{}, fmt.Errorf("commit: %w", err)
		}
		return channelFromRow(channel), channelMemberFromRow(member), nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Channel{}, ChannelMember{}, fmt.Errorf("channel participant: %w", err)
	}

	// Both counts are read under the channel_state row lock, so two concurrent
	// joins to this channel cannot both see the last free seat.
	seats, err := qtx.CountChannelParticipants(ctx, invite.ChannelID)
	if err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("count participants: %w", err)
	}
	if seats >= int64(s.maxChannelParticipants) {
		return Channel{}, ChannelMember{}, ErrChannelFull
	}
	joined, err := qtx.CountChannelsForUser(ctx, userID)
	if err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("count channels for user: %w", err)
	}
	if joined >= int64(s.maxChannelsPerUser) {
		return Channel{}, ChannelMember{}, ErrTooManyChannels
	}

	n, err := qtx.InsertChannelParticipantIfAbsent(ctx, db.InsertChannelParticipantIfAbsentParams{
		ChannelID: invite.ChannelID, UserID: userID, Role: 0, JoinPts: state.Pts,
	})
	if err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("insert participant: %w", err)
	}
	if n == 0 {
		// Unreachable while every admission path holds this channel's state row
		// lock. Re-read rather than assume: returning the row that exists is what
		// keeps a re-join from reporting a join_pts that was never written.
		if member, err = qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
			ChannelID: invite.ChannelID, UserID: userID,
		}); err != nil {
			return Channel{}, ChannelMember{}, fmt.Errorf("channel participant: %w", err)
		}
	} else {
		member = db.ChannelParticipant{
			ChannelID: invite.ChannelID, UserID: userID, Role: 0, JoinPts: state.Pts,
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return Channel{}, ChannelMember{}, fmt.Errorf("commit: %w", err)
	}
	return channelFromRow(channel), channelMemberFromRow(member), nil
}

// LeaveChannel deletes userID's participant row; left=false means there was
// none. The creator leaving is allowed here — whether the RPC permits it is the
// handler's call, not this layer's.
func (s *Store) LeaveChannel(ctx context.Context, channelID, userID int64) (bool, error) {
	n, err := s.q.DeleteChannelParticipant(ctx, db.DeleteChannelParticipantParams{
		ChannelID: channelID, UserID: userID,
	})
	if err != nil {
		return false, fmt.Errorf("delete channel participant: %w", err)
	}
	return n > 0, nil
}

// ChannelsForUser returns every channel the user holds a participant row in.
func (s *Store) ChannelsForUser(ctx context.Context, userID int64) ([]Channel, error) {
	rows, err := s.q.ChannelsForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("channels for user: %w", err)
	}
	out := make([]Channel, len(rows))
	for i, r := range rows {
		out[i] = channelFromRow(r)
	}
	return out, nil
}
