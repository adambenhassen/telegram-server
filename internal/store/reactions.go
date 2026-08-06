package store

import (
	"context"
	"fmt"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Reaction is a single reaction on a message copy.
type Reaction struct {
	ReactorID int64
	Reaction  string
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
// Returns the set of user ids whose copies were affected (for notification).
// The caller's id is always included.
func (s *Store) SendReaction(ctx context.Context, ownerID, localID int64, reaction string) ([]int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if err != nil {
		return nil, fmt.Errorf("load message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	peerType := PeerType(msg.PeerType)
	peerID := msg.PeerID

	if peerType == PeerTypeChat {
		// For chat messages, fan out to all current members.
		return sendChatReaction(ctx, qtx, ownerID, msg, reaction)
	}

	// 1:1 message: record reaction on the caller's copy AND the peer's copy.
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
	return []int64{ownerID, peerID}, nil
}

// ClearReaction removes the caller's reaction from a message they own.
// Returns the set of user ids whose copies were affected (for notification).
func (s *Store) ClearReaction(ctx context.Context, ownerID, localID int64) ([]int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if err != nil {
		return nil, fmt.Errorf("load message: %w", err)
	}
	if msg.Deleted {
		return nil, ErrMessageInvalid
	}

	peerType := PeerType(msg.PeerType)
	peerID := msg.PeerID

	if peerType == PeerTypeChat {
		return clearChatReaction(ctx, qtx, ownerID, msg)
	}

	// 1:1 message: delete reaction from both copies.
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
	return []int64{ownerID, peerID}, nil
}

// sendChatReaction records the caller's reaction on every current member's
// copy of the chat message. Returns the user ids affected.
func sendChatReaction(ctx context.Context, qtx *db.Queries, reactorID int64, pre db.Message, reaction string) ([]int64, error) {
	copies, err := chatCopies(ctx, qtx, pre)
	if err != nil {
		return nil, err
	}
	members, err := chatMembers(ctx, qtx, pre.PeerID)
	if err != nil {
		return nil, err
	}
	if !members[reactorID] {
		return nil, ErrNotMember
	}

	affected := make([]int64, 0, len(members))
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
		affected = append(affected, c.OwnerID)
	}
	if len(affected) == 0 {
		return nil, ErrNotMember
	}
	return affected, nil
}

// clearChatReaction removes the caller's reaction from every current member's
// copy of the chat message. Returns the user ids affected.
func clearChatReaction(ctx context.Context, qtx *db.Queries, reactorID int64, pre db.Message) ([]int64, error) {
	copies, err := chatCopies(ctx, qtx, pre)
	if err != nil {
		return nil, err
	}
	members, err := chatMembers(ctx, qtx, pre.PeerID)
	if err != nil {
		return nil, err
	}
	if !members[reactorID] {
		return nil, ErrNotMember
	}

	affected := make([]int64, 0, len(members))
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
		affected = append(affected, c.OwnerID)
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

// MessageReactionGroup groups reactions by message for delivery.
type MessageReactionGroup struct {
	LocalID   int64
	Reactions []Reaction
}

// ReactionsForUser returns all reactions on messages owned by userID, grouped
// by message. Used by the reactions delivery path to push updateMessageReactions.
func (s *Store) ReactionsForUser(ctx context.Context, ownerID int64) ([]MessageReactionGroup, error) {
	rows, err := s.q.ReactionsByOwner(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("reactions by owner: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	// Group by local_id.
	byMsg := make(map[int64][]Reaction)
	for _, r := range rows {
		byMsg[r.LocalID] = append(byMsg[r.LocalID], reactionFromRow(r))
	}
	groups := make([]MessageReactionGroup, 0, len(byMsg))
	for localID, reactions := range byMsg {
		groups = append(groups, MessageReactionGroup{
			LocalID:   localID,
			Reactions: reactions,
		})
	}
	return groups, nil
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
