package store

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

var chatMediaSubtypeRights = [...]string{
	"send_stickers",
	"send_gifs",
	"send_videos",
	"send_roundvideos",
	"send_audios",
	"send_voices",
}

func hasChatRight(rights []string, right string) bool {
	return slices.Contains(rights, right)
}

func checkChatMessageRestriction(rights []string, creator, media bool, mediaRights []string) error {
	if creator {
		return nil
	}
	if hasChatRight(rights, "send_messages") {
		return ErrChatWriteForbidden
	}
	if media {
		if hasChatRight(rights, "send_media") || hasChatRight(rights, "send_docs") {
			return ErrChatWriteForbidden
		}
		if mediaRights == nil {
			for _, right := range chatMediaSubtypeRights {
				if hasChatRight(rights, right) {
					return ErrChatWriteForbidden
				}
			}
		}
		for _, right := range mediaRights {
			if hasChatRight(rights, right) {
				return ErrChatWriteForbidden
			}
		}
	} else if hasChatRight(rights, "send_plain") {
		return ErrChatWriteForbidden
	}
	return nil
}

// CheckChatWritePermission checks a new basic-chat document send before the
// media handler assembles an upload. The final send repeats the decision in its
// write transaction; this check avoids creating file rows or blobs for a send
// that is already denied. A committed random_id retry bypasses the restriction,
// matching the final send transaction's dedup order.
func (s *Store) CheckChatWritePermission(ctx context.Context, chatID, callerID, randomID int64, mediaRights []string) (duplicate bool, err error) {
	m, err := s.beginChatMutation(ctx, chatID, callerID)
	if err != nil {
		return false, err
	}
	defer func() { _ = m.tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if randomID != 0 {
		_, err = m.qtx.MessageByRandomID(ctx, db.MessageByRandomIDParams{OwnerID: callerID, RandomID: randomID})
		switch {
		case err == nil:
			if err = m.tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("commit permission check: %w", err)
			}
			return true, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return false, fmt.Errorf("random_id lookup: %w", err)
		}
	}
	if err = checkChatMessageRestriction(m.defaultBannedRights, callerID == m.creatorID, true, mediaRights); err != nil {
		return false, err
	}
	if err = m.tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit permission check: %w", err)
	}
	return false, nil
}
