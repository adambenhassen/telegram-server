package store

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/store/db"
)

// ChatHistorySnapshot is the complete read set needed to render one basic-chat
// history response. Membership, messages, participants, chat metadata, and
// entitled profile rows all come from one repeatable-read snapshot.
type ChatHistorySnapshot struct {
	Chat          Chat
	Messages      []Message
	Participants  []Participant
	Users         map[int64]User
	EntitledUsers map[int64]bool
}

// SetChatHistorySnapshotHook installs the test-only pause between membership
// selection and the remaining history snapshot reads.
func SetChatHistorySnapshotHook(s *Store, fn func()) { s.chatHistorySnapshotHook = fn }

// ChatHistoryForMemberSnapshot returns history and its required hydration only
// when viewerID is a current member. An absent chat and a non-member both
// return ErrNotMember.
func (s *Store) ChatHistoryForMemberSnapshot(ctx context.Context, viewerID, chatID int64, offsetID, addOffset, limit int) (ChatHistorySnapshot, error) {
	snapshot := ChatHistorySnapshot{
		Users:         map[int64]User{},
		EntitledUsers: map[int64]bool{},
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("begin chat history snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	chatRows, err := qtx.ChatsByIDsForMember(ctx, db.ChatsByIDsForMemberParams{
		UserID:  viewerID,
		ChatIds: []int64{chatID},
	})
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("select member chat for history: %w", err)
	}
	if len(chatRows) == 0 {
		return ChatHistorySnapshot{}, ErrNotMember
	}
	snapshot.Chat = chatFromRow(chatRows[0])
	if hook := s.chatHistorySnapshotHook; hook != nil {
		hook()
	}

	messageRows, err := historyPage(ctx, qtx, viewerID, PeerTypeChat, chatID, offsetID, addOffset, limit)
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("select chat history page: %w", err)
	}
	snapshot.Messages = messagesFromRows(messageRows)

	participantRows, err := qtx.ChatParticipants(ctx, chatID)
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("select chat history participants: %w", err)
	}
	snapshot.Participants = make([]Participant, len(participantRows))
	for i, row := range participantRows {
		snapshot.Participants[i] = Participant{UserID: row.UserID, InviterID: row.InviterID, Date: row.Date.Time}
	}

	userIDSet := map[int64]struct{}{viewerID: {}}
	includeParticipants := false
	for _, message := range snapshot.Messages {
		if message.FromID != 0 {
			userIDSet[message.FromID] = struct{}{}
		}
		switch message.Action {
		case ChatActionAddUser, ChatActionDeleteUser:
			if message.ActionUserID != 0 {
				userIDSet[message.ActionUserID] = struct{}{}
			}
		case ChatActionCreate:
			includeParticipants = true
		}
	}
	if includeParticipants {
		for _, participant := range snapshot.Participants {
			userIDSet[participant.UserID] = struct{}{}
		}
	}
	userIDs := make([]int64, 0, len(userIDSet))
	for id := range userIDSet {
		userIDs = append(userIDs, id)
	}
	slices.Sort(userIDs)

	userRows, err := qtx.UsersByID(ctx, userIDs)
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("select chat history users: %w", err)
	}
	for _, row := range userRows {
		snapshot.Users[row.ID] = UserFromDB(db.UserByIDRow(row))
	}
	entitledRows, err := qtx.EntitledUserIDs(ctx, db.EntitledUserIDsParams{
		ViewerID: viewerID,
		Ids:      userIDs,
	})
	if err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("select entitled chat history users: %w", err)
	}
	for i, row := range entitledRows {
		id, ok := row.(int64)
		if !ok {
			return ChatHistorySnapshot{}, fmt.Errorf("entitled chat history users: unexpected type %T at row %d", row, i)
		}
		snapshot.EntitledUsers[id] = true
	}

	if err := tx.Commit(ctx); err != nil {
		return ChatHistorySnapshot{}, fmt.Errorf("commit chat history snapshot: %w", err)
	}
	return snapshot, nil
}
