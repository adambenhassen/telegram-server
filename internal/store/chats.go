package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// A chats row lock (SELECT ... FROM chats WHERE id = $1 FOR UPDATE) is the
// serialisation point for everything that touches one chat's member set. It is
// always taken BEFORE any lockOwners advisory lock and never inside one, and a
// transaction locks at most one chats row. lockOwners' argument space is user
// ids only — never pass a chat id, or chat 7 and user 7 falsely serialise.
//
// Nothing in this file takes that lock: the rule is written down here for the
// membership mutations and the fan-out that do.

// maxChatParticipants caps a basic chat at Telegram's own limit. It is what
// bounds the fan-out transaction: one send to a full chat writes 200 message
// rows, 200 events and 200 pts bumps under 200 advisory locks.
const maxChatParticipants = 200

// Chat is a basic group chat.
type Chat struct {
	ID        int64
	Title     string
	CreatorID int64
	Version   int
	Date      time.Time
	// Deactivated has no column yet; chats are never deactivated in M6, so it is
	// always false. It exists for the tg.Chat mapping a later ticket adds.
	Deactivated bool
}

// Participant is one member of a chat.
type Participant struct {
	UserID    int64
	InviterID int64
	Date      time.Time
}

func chatFromRow(r db.Chat) Chat {
	return Chat{
		ID:        r.ID,
		Title:     r.Title,
		CreatorID: r.CreatorID,
		Version:   int(r.Version),
		Date:      r.Date.Time,
	}
}

// CreateChat creates a chat owned by creatorID with the creator and memberIDs as
// participants, all in one transaction. Duplicate ids in memberIDs and creatorID
// appearing in memberIDs are deduped rather than rejected. ErrChatFull if the
// deduped member count exceeds maxChatParticipants; no rows are written then.
func (s *Store) CreateChat(ctx context.Context, creatorID int64, title string, memberIDs []int64) (Chat, error) {
	members := make([]int64, 0, len(memberIDs)+1)
	seen := make(map[int64]bool, len(memberIDs)+1)
	for _, id := range append([]int64{creatorID}, memberIDs...) {
		if !seen[id] {
			seen[id] = true
			members = append(members, id)
		}
	}
	if len(members) > maxChatParticipants {
		return Chat{}, ErrChatFull
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Chat{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// No chats row lock here: the chat does not exist yet, so there is nothing to
	// race against its member set.
	row, err := qtx.InsertChat(ctx, db.InsertChatParams{Title: title, CreatorID: creatorID})
	if err != nil {
		return Chat{}, fmt.Errorf("insert chat: %w", err)
	}
	for _, id := range members {
		if err = qtx.InsertChatParticipant(ctx, db.InsertChatParticipantParams{
			ChatID: row.ID, UserID: id, InviterID: creatorID,
		}); err != nil {
			return Chat{}, fmt.Errorf("insert participant %d: %w", id, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return Chat{}, fmt.Errorf("commit: %w", err)
	}
	return chatFromRow(row), nil
}

// ChatByID returns one chat; ok=false when absent.
func (s *Store) ChatByID(ctx context.Context, chatID int64) (Chat, bool, error) {
	r, err := s.q.ChatByID(ctx, chatID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Chat{}, false, nil
	case err != nil:
		return Chat{}, false, fmt.Errorf("chat by id: %w", err)
	}
	return chatFromRow(r), true, nil
}

// Participants lists the chat's members ordered by user_id ascending.
func (s *Store) Participants(ctx context.Context, chatID int64) ([]Participant, error) {
	rows, err := s.q.ChatParticipants(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("chat participants: %w", err)
	}
	out := make([]Participant, len(rows))
	for i, r := range rows {
		out[i] = Participant{UserID: r.UserID, InviterID: r.InviterID, Date: r.Date.Time}
	}
	return out, nil
}

// IsMember reports whether userID is a participant of chatID. This is the whole
// authorization boundary for the chat RPCs: an unknown chat id and a non-member
// both report false, and nothing but a chat_participants row makes it true.
func (s *Store) IsMember(ctx context.Context, chatID, userID int64) (bool, error) {
	ok, err := s.q.IsChatMember(ctx, db.IsChatMemberParams{ChatID: chatID, UserID: userID})
	if err != nil {
		return false, fmt.Errorf("is chat member: %w", err)
	}
	return ok, nil
}

// ChatsForUser returns every chat the user participates in.
func (s *Store) ChatsForUser(ctx context.Context, userID int64) ([]Chat, error) {
	rows, err := s.q.ChatsForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("chats for user: %w", err)
	}
	out := make([]Chat, len(rows))
	for i, r := range rows {
		out[i] = chatFromRow(r)
	}
	return out, nil
}
