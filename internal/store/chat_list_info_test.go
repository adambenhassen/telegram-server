package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestChatListInfoSnapshotReturnsCountsFor100Chats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	userIDs := make([]int64, 0, 200)
	for i := 1; i <= 200; i++ {
		user, err := s.CreateUser(ctx, fmt.Sprintf("+1555%08d", i))
		if err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		userIDs = append(userIDs, user.ID)
	}

	chatRows, err := store.StorePool(s).Query(ctx, `
INSERT INTO chats (title, creator_id)
SELECT 'group-' || n::text, $1
FROM generate_series(1, 100) AS series(n)
RETURNING id`, userIDs[0])
	if err != nil {
		t.Fatalf("insert chats: %v", err)
	}
	chatIDs := make([]int64, 0, 100)
	for chatRows.Next() {
		var id int64
		if err := chatRows.Scan(&id); err != nil {
			chatRows.Close()
			t.Fatalf("scan chat id: %v", err)
		}
		chatIDs = append(chatIDs, id)
	}
	if err := chatRows.Err(); err != nil {
		chatRows.Close()
		t.Fatalf("read chat ids: %v", err)
	}
	chatRows.Close()
	if len(chatIDs) != 100 {
		t.Fatalf("inserted chats = %d, want 100", len(chatIDs))
	}
	if _, err := store.StorePool(s).Exec(ctx, `
INSERT INTO chat_participants (chat_id, user_id, inviter_id)
SELECT chats.chat_id, users.user_id, $1
FROM unnest($2::bigint[]) AS chats(chat_id)
CROSS JOIN unnest($3::bigint[]) AS users(user_id)`, userIDs[0], chatIDs, userIDs); err != nil {
		t.Fatalf("insert participants: %v", err)
	}

	snapshot, err := s.ChatListInfoForMemberSnapshot(ctx, userIDs[0], chatIDs)
	if err != nil {
		t.Fatalf("chat list snapshot: %v", err)
	}
	if len(snapshot.Chats) != 100 || len(snapshot.ParticipantCounts) != 100 {
		t.Fatalf("snapshot chats/counts = %d/%d, want 100/100", len(snapshot.Chats), len(snapshot.ParticipantCounts))
	}
	for _, id := range chatIDs {
		if _, ok := snapshot.Chats[id]; !ok {
			t.Errorf("chat %d missing from member snapshot", id)
		}
		if got := snapshot.ParticipantCounts[id]; got != 200 {
			t.Errorf("chat %d participant count = %d, want 200", id, got)
		}
	}
}
