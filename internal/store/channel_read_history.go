package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ReadChannelHistory advances one current member's channel history marker up
// to maxID, clamped to the committed channel top. It has no effect on pts,
// event logs, or other members' read state.
func (s *Store) ReadChannelHistory(ctx context.Context, channelID, userID, maxID int64) error {
	if channelID <= 0 || userID <= 0 {
		return ErrNotMember
	}
	if maxID < 0 {
		return ErrMessageInvalid
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin channel read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	memberExists, err := qtx.IsChannelMember(ctx, db.IsChannelMemberParams{
		ChannelID: channelID,
		UserID:    userID,
	})
	if err != nil {
		return fmt.Errorf("check channel reader membership: %w", err)
	}
	if !memberExists {
		return ErrNotMember
	}

	if _, err = qtx.LockChannel(ctx, channelID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotMember
	} else if err != nil {
		return fmt.Errorf("lock channel for read: %w", err)
	}
	member, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    userID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotMember
	case err != nil:
		return fmt.Errorf("recheck channel reader membership: %w", err)
	}
	if channelMemberFromRow(member).Banned(time.Now()) {
		return ErrNotMember
	}

	state, err := qtx.LockChannelState(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotMember
	case err != nil:
		return fmt.Errorf("lock channel state for read: %w", err)
	}
	if top := state.NextLocalID - 1; maxID > top {
		maxID = top
	}
	if maxID > 0 {
		if err := qtx.AdvanceChannelReadState(ctx, db.AdvanceChannelReadStateParams{
			ChannelID: channelID,
			UserID:    userID,
			ReadMaxID: maxID,
		}); err != nil {
			return fmt.Errorf("advance channel read state: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit channel read: %w", err)
	}
	return nil
}

func insertInitialChannelReadState(ctx context.Context, qtx *db.Queries, channelID, userID, topID int64) error {
	return qtx.InsertChannelReadStateForNewMember(ctx, db.InsertChannelReadStateForNewMemberParams{
		ChannelID: channelID,
		UserID:    userID,
		TopID:     topID,
	})
}
