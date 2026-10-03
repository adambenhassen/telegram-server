package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/store/db"
)

// ChatReadHistoryResult is the reader's readHistory response and the owners
// whose committed events need a difference-poll nudge.
type ChatReadHistoryResult struct {
	Pts           int
	PtsCount      int
	NotifyUserIDs []int64
}

// ReadChatHistory marks one current member's chat copies read up to the newest
// non-deleted copy at or below maxID. The bound is captured before owner locks,
// so a send that commits while those locks are acquired cannot widen the
// receipt set. Membership authorizes only after every current participant's
// owner lock has been acquired and the participant rows have been read again.
func (s *Store) ReadChatHistory(ctx context.Context, ownerID, chatID, maxID int64) (ChatReadHistoryResult, error) {
	member, err := s.IsMember(ctx, chatID, ownerID)
	if err != nil {
		return ChatReadHistoryResult{}, err
	}
	if !member {
		return ChatReadHistoryResult{}, ErrNotMember
	}

	bound, err := s.q.MaxChatReadMessageID(ctx, db.MaxChatReadMessageIDParams{
		OwnerID: ownerID, PeerType: int16(PeerTypeChat), PeerID: chatID, LocalID: maxID,
	})
	if err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("bound chat read: %w", err)
	}

	for {
		participants, err := s.q.ChatParticipants(ctx, chatID)
		if err != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("chat participants before read: %w", err)
		}
		if len(participants) > maxChatParticipants {
			return ChatReadHistoryResult{}, fmt.Errorf("chat %d has %d participants, over cap %d", chatID, len(participants), maxChatParticipants)
		}

		memberIDs := make([]int64, len(participants))
		memberSet := make(map[int64]bool, len(participants))
		for i, participant := range participants {
			memberIDs[i] = participant.UserID
			memberSet[participant.UserID] = true
		}
		// This pre-lock snapshot may reject, but never authorizes. Keeping the
		// reader inside the capped member set also bounds the one owner-lock pass.
		if !memberSet[ownerID] {
			return ChatReadHistoryResult{}, ErrNotMember
		}

		result, retry, err := s.readChatHistoryAttempt(ctx, ownerID, chatID, bound, memberIDs)
		if err != nil {
			return ChatReadHistoryResult{}, err
		}
		if !retry {
			return result, nil
		}
	}
}

func (s *Store) readChatHistoryAttempt(ctx context.Context, ownerID, chatID, bound int64, memberIDs []int64) (result ChatReadHistoryResult, retry bool, err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ChatReadHistoryResult{}, false, fmt.Errorf("begin chat read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit or retry
	qtx := s.q.WithTx(tx)

	ownerIDs := append([]int64(nil), memberIDs...)
	if err := lockOwners(ctx, tx, ownerIDs...); err != nil {
		return ChatReadHistoryResult{}, false, err
	}

	participants, err := qtx.ChatParticipants(ctx, chatID)
	if err != nil {
		return ChatReadHistoryResult{}, false, fmt.Errorf("chat participants after owner locks: %w", err)
	}
	if len(participants) > maxChatParticipants {
		return ChatReadHistoryResult{}, false, fmt.Errorf("chat %d has %d participants, over cap %d", chatID, len(participants), maxChatParticipants)
	}
	locked := make(map[int64]bool, len(ownerIDs))
	for _, id := range ownerIDs {
		locked[id] = true
	}
	currentMembers := make([]int64, len(participants))
	readerIsMember := false
	for i, participant := range participants {
		currentMembers[i] = participant.UserID
		if participant.UserID == ownerID {
			readerIsMember = true
		}
		if !locked[participant.UserID] {
			// An add committed after the pre-lock participant read. Restart with
			// its owner in the single sorted lock pass; do not authorize from this
			// transaction's incomplete lock set.
			return ChatReadHistoryResult{}, true, nil
		}
	}
	if !readerIsMember {
		return ChatReadHistoryResult{}, false, ErrNotMember
	}

	result, err = advanceChatReadHistory(ctx, qtx, ownerID, chatID, bound, currentMembers)
	if err != nil {
		return ChatReadHistoryResult{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ChatReadHistoryResult{}, false, fmt.Errorf("commit chat read: %w", err)
	}
	return result, false, nil
}

func advanceChatReadHistory(ctx context.Context, qtx *db.Queries, ownerID, chatID, bound int64, memberIDs []int64) (ChatReadHistoryResult, error) {
	markers, err := qtx.ReadMarkers(ctx, db.ReadMarkersParams{
		OwnerID: ownerID, PeerType: int16(PeerTypeChat), PeerID: chatID,
	})
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && bound <= markers.ReadInboxMaxID) {
		state, stateErr := qtx.GetState(ctx, ownerID)
		if errors.Is(stateErr, pgx.ErrNoRows) {
			return ChatReadHistoryResult{}, nil
		}
		if stateErr != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("read chat reader state: %w", stateErr)
		}
		return ChatReadHistoryResult{Pts: int(state.Pts)}, nil
	}
	if err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("read chat markers: %w", err)
	}

	targets, err := qtx.ChatReadReceiptTargets(ctx, db.ChatReadReceiptTargetsParams{
		OwnerID: ownerID, PeerType: int16(PeerTypeChat), PeerID: chatID,
		AfterID: markers.ReadInboxMaxID, MaxID: bound, MemberIds: memberIDs,
	})
	if err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("find chat read recipients: %w", err)
	}
	inbox, err := qtx.AdvanceChatReadInbox(ctx, db.AdvanceChatReadInboxParams{
		OwnerID: ownerID, PeerType: int16(PeerTypeChat), PeerID: chatID, MaxID: bound,
	})
	if err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("advance chat inbox: %w", err)
	}

	readerPts, err := qtx.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("bump chat reader pts: %w", err)
	}
	if err := qtx.InsertEvent(ctx, db.InsertEventParams{
		OwnerID: ownerID, Pts: readerPts, Type: int16(EventReadIn), LocalID: inbox.ReadInboxMaxID,
	}); err != nil {
		return ChatReadHistoryResult{}, fmt.Errorf("insert chat read-in event: %w", err)
	}

	result := ChatReadHistoryResult{
		Pts: int(readerPts), PtsCount: 1, NotifyUserIDs: []int64{ownerID},
	}
	for _, target := range targets {
		markers, err := qtx.ReadMarkers(ctx, db.ReadMarkersParams{
			OwnerID: target.SenderID, PeerType: int16(PeerTypeChat), PeerID: chatID,
		})
		if err != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("read sender %d chat markers: %w", target.SenderID, err)
		}
		if target.MaxID <= markers.ReadOutboxMaxID {
			continue
		}
		rows, err := qtx.AdvanceReadOutbox(ctx, db.AdvanceReadOutboxParams{
			OwnerID: target.SenderID, PeerType: int16(PeerTypeChat), PeerID: chatID,
			ReadOutboxMaxID: target.MaxID,
		})
		if err != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("advance sender %d chat outbox: %w", target.SenderID, err)
		}
		if rows != 1 {
			return ChatReadHistoryResult{}, fmt.Errorf("advance sender %d chat outbox changed %d rows", target.SenderID, rows)
		}
		senderPts, err := qtx.BumpPtsOnly(ctx, target.SenderID)
		if err != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("bump sender %d chat pts: %w", target.SenderID, err)
		}
		if err := qtx.InsertEvent(ctx, db.InsertEventParams{
			OwnerID: target.SenderID, Pts: senderPts, Type: int16(EventReadOut), LocalID: target.MaxID,
		}); err != nil {
			return ChatReadHistoryResult{}, fmt.Errorf("insert sender %d chat read-out event: %w", target.SenderID, err)
		}
		result.NotifyUserIDs = append(result.NotifyUserIDs, target.SenderID)
	}
	return result, nil
}
