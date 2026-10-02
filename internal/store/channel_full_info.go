package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ChannelFullInfoSnapshot is the read set for channels.getFullChannel. The
// channel, membership decision, counts, pts and optional admin invite all come
// from one repeatable-read snapshot.
type ChannelFullInfoSnapshot struct {
	Channel           Channel
	Member            ChannelMember
	HasMember         bool
	ParticipantsCount int64
	AdminsCount       int64
	BannedCount       int64
	ReadInboxMaxID    int64
	UnreadCount       int
	Pts               int
	InviteHash        string
	InviteCreatorID   int64
	InviteDate        time.Time
	HasInvite         bool
}

// SetChannelFullInfoSnapshotHook installs the test-only synchronization seam
// used by API concurrency tests. Production callers leave it nil.
func SetChannelFullInfoSnapshotHook(s *Store, fn func()) {
	s.channelFullInfoSnapshotHook = fn
}

// SetChannelParticipantsSnapshotHook installs the test-only synchronization
// seam used by participant privacy race tests. Production callers leave it nil.
func SetChannelParticipantsSnapshotHook(s *Store, fn func()) {
	s.channelParticipantsSnapshotHook = fn
}

// ChannelFullInfoForViewer selects a channel and its full-info read set in one
// repeatable-read transaction. A concurrent removal or ban may commit while
// this read is being hydrated, but it cannot make the member decision come from
// before the change and the returned counts or invite come from after it.
func (s *Store) ChannelFullInfoForViewer(
	ctx context.Context,
	channelID, viewerID int64,
) (ChannelFullInfoSnapshot, bool, error) {
	var snapshot ChannelFullInfoSnapshot
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("begin channel full-info snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	channel, err := qtx.ChannelByID(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return ChannelFullInfoSnapshot{}, false, fmt.Errorf("commit empty channel full-info snapshot: %w", err)
		}
		return snapshot, false, nil
	case err != nil:
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select channel for full info: %w", err)
	}
	snapshot.Channel = channelFromRow(channel)

	member, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    viewerID,
	})
	switch {
	case err == nil:
		snapshot.Member = channelMemberFromRow(member)
		snapshot.HasMember = true
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select channel viewer membership: %w", err)
	}

	if hook := s.channelFullInfoSnapshotHook; hook != nil {
		hook()
	}
	if (snapshot.HasMember && snapshot.Member.Banned(time.Now())) ||
		(!snapshot.HasMember && snapshot.Channel.Username == nil) {
		if err := tx.Commit(ctx); err != nil {
			return ChannelFullInfoSnapshot{}, false, fmt.Errorf("commit refused channel full-info snapshot: %w", err)
		}
		return snapshot, true, nil
	}

	stats, err := qtx.ChannelFullInfoStats(ctx, channelID)
	if err != nil {
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select channel full-info counts: %w", err)
	}
	snapshot.ParticipantsCount = stats.ParticipantsCount
	snapshot.AdminsCount = stats.AdminsCount
	snapshot.BannedCount = stats.BannedCount
	if snapshot.HasMember {
		readState, err := qtx.ChannelReadStateForViewer(ctx, db.ChannelReadStateForViewerParams{
			ChannelID: channelID,
			UserID:    viewerID,
		})
		if err != nil {
			return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select channel viewer read state: %w", err)
		}
		snapshot.ReadInboxMaxID = readState.ReadMaxID
		snapshot.UnreadCount = int(readState.UnreadCount)
	}

	state, err := qtx.GetChannelState(ctx, channelID)
	if err != nil {
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select channel full-info pts: %w", err)
	}
	snapshot.Pts = int(state.Pts)

	if snapshot.HasMember && snapshot.Member.Role >= channelRoleAdmin && !snapshot.Member.Banned(time.Now()) {
		invite, err := qtx.ChannelActiveInviteByChannel(ctx, channelID)
		switch {
		case err == nil:
			snapshot.InviteHash = invite.Hash
			snapshot.InviteCreatorID = invite.CreatorID
			snapshot.InviteDate = invite.Date.Time
			snapshot.HasInvite = true
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return ChannelFullInfoSnapshot{}, false, fmt.Errorf("select active channel invite: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ChannelFullInfoSnapshot{}, false, fmt.Errorf("commit channel full-info snapshot: %w", err)
	}
	return snapshot, true, nil
}
