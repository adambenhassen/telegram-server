package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/adambenhassen/telegram-server/internal/store/db"
	"github.com/jackc/pgx/v5"
)

// Reaction is a single reaction on a message copy.
type Reaction struct {
	ReactorID int64
	Reaction  string
}

// ReactionTarget identifies one message copy to push a reaction update to.
type ReactionTarget struct {
	OwnerID int64
	LocalID int64
}

func reactionFromRow(r db.MessageReaction) Reaction {
	return Reaction{
		ReactorID: r.ReactorID,
		Reaction:  r.Reaction,
	}
}

// SendReaction sets or updates the caller's reaction on a message they own.
// The message must exist and the caller must own it (be sender or recipient).
// For 1:1 messages this is straightforward; for group chats every current
// member's copy of the message gets the reaction recorded.
//
// Returns the set of (ownerID, localID) pairs whose copies were affected,
// for the NOTIFY fan-out that pushes updateMessageReactions.
func (s *Store) SendReaction(ctx context.Context, ownerID, localID int64, reaction string) ([]ReactionTarget, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMessageInvalid
		}
		return nil, fmt.Errorf("load message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	peerType := PeerType(msg.PeerType)
	peerID := msg.PeerID

	if peerType == PeerTypeChat {
		// For chat messages, fan out to all current members.
		affected, err := sendChatReaction(ctx, tx, qtx, ownerID, msg, reaction)
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return affected, nil
	}

	// 1:1 message: lock both owners, reload, then write.
	// Advisory locks serialize against concurrent DeleteMessages: a reaction
	// cannot commit after a delete that holds the same owner locks.
	if err = lockOwners(ctx, tx, ownerID, peerID); err != nil {
		return nil, err
	}

	msg, err = qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMessageInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("reload message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}
	peerLocalID := msg.PeerLocalID

	// Upsert on caller's copy.
	if err = qtx.UpsertReaction(ctx, db.UpsertReactionParams{
		OwnerID:   ownerID,
		LocalID:   localID,
		ReactorID: ownerID,
		Reaction:  reaction,
	}); err != nil {
		return nil, fmt.Errorf("upsert owner reaction: %w", err)
	}

	// Upsert on peer's copy.
	if err = qtx.UpsertReaction(ctx, db.UpsertReactionParams{
		OwnerID:   peerID,
		LocalID:   peerLocalID,
		ReactorID: ownerID,
		Reaction:  reaction,
	}); err != nil {
		return nil, fmt.Errorf("upsert peer reaction: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return []ReactionTarget{
		{OwnerID: ownerID, LocalID: localID},
		{OwnerID: peerID, LocalID: peerLocalID},
	}, nil
}

// ClearReaction removes the caller's reaction from a message they own.
// Returns the set of (ownerID, localID) pairs whose copies were affected.
func (s *Store) ClearReaction(ctx context.Context, ownerID, localID int64) ([]ReactionTarget, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMessageInvalid
		}
		return nil, fmt.Errorf("load message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	peerType := PeerType(msg.PeerType)
	peerID := msg.PeerID

	if peerType == PeerTypeChat {
		affected, err := clearChatReaction(ctx, tx, qtx, ownerID, msg)
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return affected, nil
	}

	// 1:1 message: lock both owners, reload, then write.
	if err = lockOwners(ctx, tx, ownerID, peerID); err != nil {
		return nil, err
	}

	msg, err = qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMessageInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("reload message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}
	peerLocalID := msg.PeerLocalID

	if err = qtx.DeleteReaction(ctx, db.DeleteReactionParams{
		OwnerID:   ownerID,
		LocalID:   localID,
		ReactorID: ownerID,
	}); err != nil {
		return nil, fmt.Errorf("delete owner reaction: %w", err)
	}

	if err = qtx.DeleteReaction(ctx, db.DeleteReactionParams{
		OwnerID:   peerID,
		LocalID:   peerLocalID,
		ReactorID: ownerID,
	}); err != nil {
		return nil, fmt.Errorf("delete peer reaction: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return []ReactionTarget{
		{OwnerID: ownerID, LocalID: localID},
		{OwnerID: peerID, LocalID: peerLocalID},
	}, nil
}

// sendChatReaction records the caller's reaction on every current member's
// copy of the chat message. Returns the (ownerID, localID) pairs affected.
//
// Follows the editChatMessage pattern: get copies → lock owners → reload
// message → check membership → write. The per-owner advisory locks make the
// chatMembers read authoritative: a concurrent removal is either visible or
// blocked behind this transaction.
func sendChatReaction(ctx context.Context, tx pgx.Tx, qtx *db.Queries, reactorID int64, pre db.Message, reaction string) ([]ReactionTarget, error) {
	copies, err := chatCopies(ctx, qtx, pre)
	if err != nil {
		return nil, err
	}
	if err = lockOwners(ctx, tx, copyOwners(copies)...); err != nil {
		return nil, err
	}

	// Reload the message under lock.
	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: pre.OwnerID, LocalID: pre.LocalID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMessageInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("reload message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	members, err := chatMembers(ctx, qtx, pre.PeerID)
	if err != nil {
		return nil, err
	}
	if !members[reactorID] {
		return nil, ErrNotMember
	}

	affected := make([]ReactionTarget, 0, len(members))
	for _, c := range copies {
		if !members[c.OwnerID] {
			continue
		}
		if err = qtx.UpsertReaction(ctx, db.UpsertReactionParams{
			OwnerID:   c.OwnerID,
			LocalID:   c.LocalID,
			ReactorID: reactorID,
			Reaction:  reaction,
		}); err != nil {
			return nil, fmt.Errorf("upsert reaction owner %d: %w", c.OwnerID, err)
		}
		affected = append(affected, ReactionTarget{OwnerID: c.OwnerID, LocalID: c.LocalID})
	}
	if len(affected) == 0 {
		return nil, ErrNotMember
	}
	return affected, nil
}

// clearChatReaction removes the caller's reaction from every current member's
// copy of the chat message. Returns the (ownerID, localID) pairs affected.
//
// Same locking pattern as sendChatReaction.
func clearChatReaction(ctx context.Context, tx pgx.Tx, qtx *db.Queries, reactorID int64, pre db.Message) ([]ReactionTarget, error) {
	copies, err := chatCopies(ctx, qtx, pre)
	if err != nil {
		return nil, err
	}
	if err = lockOwners(ctx, tx, copyOwners(copies)...); err != nil {
		return nil, err
	}

	// Reload the message under lock.
	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: pre.OwnerID, LocalID: pre.LocalID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMessageInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("reload message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	members, err := chatMembers(ctx, qtx, pre.PeerID)
	if err != nil {
		return nil, err
	}
	if !members[reactorID] {
		return nil, ErrNotMember
	}

	affected := make([]ReactionTarget, 0, len(members))
	for _, c := range copies {
		if !members[c.OwnerID] {
			continue
		}
		if err = qtx.DeleteReaction(ctx, db.DeleteReactionParams{
			OwnerID:   c.OwnerID,
			LocalID:   c.LocalID,
			ReactorID: reactorID,
		}); err != nil {
			return nil, fmt.Errorf("delete reaction owner %d: %w", c.OwnerID, err)
		}
		affected = append(affected, ReactionTarget{OwnerID: c.OwnerID, LocalID: c.LocalID})
	}
	return affected, nil
}

// ReactionsByOwnerLocal returns all reactions on a specific message copy.
func (s *Store) ReactionsByOwnerLocal(ctx context.Context, ownerID, localID int64) ([]Reaction, error) {
	rows, err := s.q.ReactionsByMessage(ctx, db.ReactionsByMessageParams{OwnerID: ownerID, LocalID: localID})
	if err != nil {
		return nil, fmt.Errorf("reactions by message: %w", err)
	}
	reactions := make([]Reaction, len(rows))
	for i, r := range rows {
		reactions[i] = reactionFromRow(r)
	}
	return reactions, nil
}

// ReactionsByOwnerLocalIDs returns the reactions on each of the owner's message
// copies named by localIDs, keyed by local id. It is the batched form of
// ReactionsByOwnerLocal and orders each message's reactions the same way, so the
// two render identically. Ids with no reaction are absent from the map rather
// than present and empty.
func (s *Store) ReactionsByOwnerLocalIDs(ctx context.Context, ownerID int64, localIDs []int64) (map[int64][]Reaction, error) {
	if len(localIDs) == 0 {
		return map[int64][]Reaction{}, nil
	}
	rows, err := s.q.ReactionsByMessages(ctx, db.ReactionsByMessagesParams{
		OwnerID:  ownerID,
		LocalIds: localIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("reactions by messages: %w", err)
	}
	byMessage := make(map[int64][]Reaction, len(localIDs))
	for _, r := range rows {
		byMessage[r.LocalID] = append(byMessage[r.LocalID], reactionFromRow(r))
	}
	return byMessage, nil
}

// MessagesByOwnerLocalIDs returns messages for the given owner and local ids.
func (s *Store) MessagesByOwnerLocalIDs(ctx context.Context, ownerID int64, localIDs []int64) (map[int64]Message, error) {
	if len(localIDs) == 0 {
		return map[int64]Message{}, nil
	}
	rows, err := s.q.MessagesByOwnerLocalIDs(ctx, db.MessagesByOwnerLocalIDsParams{
		OwnerID:  ownerID,
		LocalIds: localIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("messages by owner local ids: %w", err)
	}
	msgs := make(map[int64]Message, len(rows))
	for _, r := range rows {
		msgs[r.LocalID] = messageFromRow(r)
	}
	return msgs, nil
}
