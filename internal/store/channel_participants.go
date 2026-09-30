package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ChannelParticipantFilter identifies the supported channel participant views.
// Bots and mentions currently have no rows in the server's data model.
const (
	ChannelParticipantFilterRecent int32 = iota
	ChannelParticipantFilterAdmins
	ChannelParticipantFilterBanned
	ChannelParticipantFilterKicked
	ChannelParticipantFilterSearch
	ChannelParticipantFilterContacts
	ChannelParticipantFilterBots
	ChannelParticipantFilterMentions
)

// ChannelParticipantSnapshot holds the channel and both participant decisions
// read by channels.getParticipant in one repeatable-read transaction.
type ChannelParticipantSnapshot struct {
	Channel        Channel
	Viewer         ChannelMember
	HasViewer      bool
	Participant    ChannelMember
	HasParticipant bool
}

// ChannelParticipantsPageSnapshot holds the channel, viewer authorization row,
// and one bounded participant page from a single database snapshot.
type ChannelParticipantsPageSnapshot struct {
	Channel      Channel
	Viewer       ChannelMember
	HasViewer    bool
	Participants []ChannelMember
	Count        int64
}

// ChannelParticipantForViewer reads the viewer and target rows from one
// repeatable-read snapshot. A missing channel and a missing row are reported
// through found flags so the RPC can collapse unauthorized cases to one error.
func (s *Store) ChannelParticipantForViewer(
	ctx context.Context,
	channelID, viewerID, participantID int64,
) (ChannelParticipantSnapshot, bool, error) {
	var snapshot ChannelParticipantSnapshot
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return snapshot, false, fmt.Errorf("begin channel participant snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	channel, err := qtx.ChannelByID(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return snapshot, false, fmt.Errorf("commit empty channel participant snapshot: %w", err)
		}
		return snapshot, false, nil
	case err != nil:
		return snapshot, false, fmt.Errorf("select channel for participant: %w", err)
	}
	snapshot.Channel = channelFromRow(channel)

	viewer, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    viewerID,
	})
	switch {
	case err == nil:
		snapshot.Viewer = channelMemberFromRow(viewer)
		snapshot.HasViewer = true
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return snapshot, true, fmt.Errorf("commit unauthorized channel participant snapshot: %w", err)
		}
		return snapshot, true, nil
	case err != nil:
		return snapshot, false, fmt.Errorf("select channel participant viewer: %w", err)
	}

	if hook := s.channelParticipantsSnapshotHook; hook != nil {
		hook()
	}
	participant, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    participantID,
	})
	switch {
	case err == nil:
		snapshot.Participant = channelMemberFromRow(participant)
		snapshot.HasParticipant = true
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return snapshot, false, fmt.Errorf("select channel participant target: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return snapshot, false, fmt.Errorf("commit channel participant snapshot: %w", err)
	}
	return snapshot, true, nil
}

// ChannelParticipantsPageForViewer reads membership, count and one bounded
// page under a single repeatable-read transaction. This prevents a removal
// followed by a new admission from mixing the old viewer check with new rows.
func (s *Store) ChannelParticipantsPageForViewer(
	ctx context.Context,
	channelID, viewerID int64,
	filter int32,
	query string,
	offset, limit int32,
) (ChannelParticipantsPageSnapshot, bool, error) {
	var snapshot ChannelParticipantsPageSnapshot
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return snapshot, false, fmt.Errorf("begin channel participants page snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	channel, err := qtx.ChannelByID(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return snapshot, false, fmt.Errorf("commit empty channel participants snapshot: %w", err)
		}
		return snapshot, false, nil
	case err != nil:
		return snapshot, false, fmt.Errorf("select channel for participants page: %w", err)
	}
	snapshot.Channel = channelFromRow(channel)

	viewer, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    viewerID,
	})
	switch {
	case err == nil:
		snapshot.Viewer = channelMemberFromRow(viewer)
		snapshot.HasViewer = true
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return snapshot, true, fmt.Errorf("commit unauthorized channel participants snapshot: %w", err)
		}
		return snapshot, true, nil
	case err != nil:
		return snapshot, false, fmt.Errorf("select channel participants viewer: %w", err)
	}

	if hook := s.channelParticipantsSnapshotHook; hook != nil {
		hook()
	}
	count, err := qtx.ChannelParticipantsPageCount(ctx, db.ChannelParticipantsPageCountParams{
		ChannelID: channelID,
		ViewerID:  viewerID,
		Filter:    filter,
		Query:     query,
	})
	if err != nil {
		return snapshot, false, fmt.Errorf("count channel participants page: %w", err)
	}
	rows, err := qtx.ChannelParticipantsPage(ctx, db.ChannelParticipantsPageParams{
		ChannelID:  channelID,
		ViewerID:   viewerID,
		Filter:     filter,
		Query:      query,
		PageOffset: offset,
		PageLimit:  limit,
	})
	if err != nil {
		return snapshot, false, fmt.Errorf("select channel participants page: %w", err)
	}
	snapshot.Count = count
	snapshot.Participants = make([]ChannelMember, len(rows))
	for i, row := range rows {
		snapshot.Participants[i] = channelMemberFromRow(row)
	}

	if err := tx.Commit(ctx); err != nil {
		return snapshot, false, fmt.Errorf("commit channel participants page snapshot: %w", err)
	}
	return snapshot, true, nil
}
