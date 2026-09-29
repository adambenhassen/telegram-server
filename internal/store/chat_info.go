package store

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ChatInfoSnapshot is the complete read set for a member-only basic-chat info
// response. Each map is populated from one repeatable-read snapshot.
type ChatInfoSnapshot struct {
	Chats         map[int64]Chat
	Participants  map[int64][]Participant
	Users         map[int64]User
	EntitledUsers map[int64]bool
}

// SetChatInfoSnapshotHook installs the test-only pause between the membership
// selection and participant/profile reads. Production callers leave it nil.
func SetChatInfoSnapshotHook(s *Store, fn func()) { s.chatInfoSnapshotHook = fn }

// ChatInfoForMemberSnapshot returns only requested chats where viewerID has a
// participant row, along with their participants and entitled profile source
// rows. The membership decision and all hydration share one read-only snapshot.
func (s *Store) ChatInfoForMemberSnapshot(ctx context.Context, viewerID int64, chatIDs []int64) (ChatInfoSnapshot, error) {
	snapshot := ChatInfoSnapshot{
		Chats:         map[int64]Chat{},
		Participants:  map[int64][]Participant{},
		Users:         map[int64]User{},
		EntitledUsers: map[int64]bool{},
	}
	if len(chatIDs) == 0 {
		return snapshot, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ChatInfoSnapshot{}, fmt.Errorf("begin chat info snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	chatRows, err := qtx.ChatsByIDsForMember(ctx, db.ChatsByIDsForMemberParams{
		UserID:  viewerID,
		ChatIds: chatIDs,
	})
	if err != nil {
		return ChatInfoSnapshot{}, fmt.Errorf("select member chats: %w", err)
	}
	selectedIDs := make([]int64, 0, len(chatRows))
	for _, row := range chatRows {
		chat := chatFromRow(row)
		snapshot.Chats[chat.ID] = chat
		selectedIDs = append(selectedIDs, chat.ID)
	}
	if len(selectedIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return ChatInfoSnapshot{}, fmt.Errorf("commit empty chat info snapshot: %w", err)
		}
		return snapshot, nil
	}
	if hook := s.chatInfoSnapshotHook; hook != nil {
		hook()
	}

	participantRows, err := qtx.ChatParticipantsByChatIDs(ctx, selectedIDs)
	if err != nil {
		return ChatInfoSnapshot{}, fmt.Errorf("select chat participants: %w", err)
	}
	userIDSet := make(map[int64]struct{}, len(participantRows)*2)
	for _, row := range participantRows {
		participant := Participant{UserID: row.UserID, InviterID: row.InviterID, Date: row.Date.Time}
		snapshot.Participants[row.ChatID] = append(snapshot.Participants[row.ChatID], participant)
		userIDSet[participant.UserID] = struct{}{}
		userIDSet[participant.InviterID] = struct{}{}
	}
	userIDs := make([]int64, 0, len(userIDSet))
	for id := range userIDSet {
		userIDs = append(userIDs, id)
	}
	slices.Sort(userIDs)
	if len(userIDs) > 0 {
		userRows, err := qtx.UsersByID(ctx, userIDs)
		if err != nil {
			return ChatInfoSnapshot{}, fmt.Errorf("select chat info users: %w", err)
		}
		for _, row := range userRows {
			snapshot.Users[row.ID] = UserFromDB(db.UserByIDRow(row))
		}
		entitledRows, err := qtx.EntitledUserIDs(ctx, db.EntitledUserIDsParams{
			ViewerID: viewerID,
			Ids:      userIDs,
		})
		if err != nil {
			return ChatInfoSnapshot{}, fmt.Errorf("select entitled chat info users: %w", err)
		}
		for i, row := range entitledRows {
			id, ok := row.(int64)
			if !ok {
				return ChatInfoSnapshot{}, fmt.Errorf("entitled chat info users: unexpected type %T at row %d", row, i)
			}
			snapshot.EntitledUsers[id] = true
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ChatInfoSnapshot{}, fmt.Errorf("commit chat info snapshot: %w", err)
	}
	return snapshot, nil
}
