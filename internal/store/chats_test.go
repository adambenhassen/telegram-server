package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func participantIDs(t *testing.T, s *store.Store, chatID int64) []int64 {
	t.Helper()
	ps, err := s.Participants(context.Background(), chatID)
	if err != nil {
		t.Fatalf("participants %d: %v", chatID, err)
	}
	ids := make([]int64, len(ps))
	for i, p := range ps {
		ids[i] = p.UserID
	}
	return ids
}

func TestCreateChatDedupesMembers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270001")
	b := mustUser(t, s, "+15551270002")

	// Creator repeated in memberIDs, and one member listed twice.
	c, err := s.CreateChat(ctx, a.ID, "Team", []int64{b.ID, b.ID, a.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if c.ID == 0 || c.Title != "Team" || c.CreatorID != a.ID {
		t.Fatalf("chat = %+v", c)
	}
	if c.Version != 1 {
		t.Fatalf("version = %d, want 1", c.Version)
	}

	// Ascending user_id, deduped: creator and the one member.
	want := []int64{a.ID, b.ID}
	if a.ID > b.ID {
		want = []int64{b.ID, a.ID}
	}
	got := participantIDs(t, s, c.ID)
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("participants = %v, want %v", got, want)
	}

	ps, err := s.Participants(ctx, c.ID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	for _, p := range ps {
		if p.InviterID != a.ID {
			t.Errorf("participant %d inviter = %d, want %d", p.UserID, p.InviterID, a.ID)
		}
	}
}

func TestChatByID(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270011")

	c, err := s.CreateChat(ctx, a.ID, "Solo", nil)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	got, ok, err := s.ChatByID(ctx, c.ID)
	if err != nil || !ok {
		t.Fatalf("chat by id: ok=%v err=%v", ok, err)
	}
	if got.ID != c.ID || got.Title != "Solo" {
		t.Fatalf("chat = %+v, want %+v", got, c)
	}
	if _, ok, err = s.ChatByID(ctx, c.ID+10_000); err != nil || ok {
		t.Fatalf("unknown chat: ok=%v err=%v", ok, err)
	}
}

func TestIsMember(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270021")
	b := mustUser(t, s, "+15551270022")
	outsider := mustUser(t, s, "+15551270023")

	c, err := s.CreateChat(ctx, a.ID, "Members", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	for _, u := range []store.User{a, b} {
		ok, err := s.IsMember(ctx, c.ID, u.ID)
		if err != nil {
			t.Fatalf("is member %d: %v", u.ID, err)
		}
		if !ok {
			t.Errorf("user %d not a member of its own chat", u.ID)
		}
	}
	ok, err := s.IsMember(ctx, c.ID, outsider.ID)
	if err != nil {
		t.Fatalf("is member outsider: %v", err)
	}
	if ok {
		t.Error("non-member reported as member")
	}
	ok, err = s.IsMember(ctx, c.ID+10_000, a.ID)
	if err != nil {
		t.Fatalf("is member unknown chat: %v", err)
	}
	if ok {
		t.Error("unknown chat id reported as member")
	}
}

func TestParticipantsAscending(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270031")
	members := make([]int64, 0, 4)
	for i := range 4 {
		members = append(members, mustUser(t, s, fmt.Sprintf("+1555127004%d", i)).ID)
	}
	// Hand them over in descending order; storage order must not depend on it.
	reversed := make([]int64, len(members))
	for i, id := range members {
		reversed[len(members)-1-i] = id
	}

	c, err := s.CreateChat(ctx, a.ID, "Ordered", reversed)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	got := participantIDs(t, s, c.ID)
	if len(got) != 5 {
		t.Fatalf("participants = %v, want 5", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("participants not ascending: %v", got)
		}
	}
}

func TestChatsForUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270051")
	b := mustUser(t, s, "+15551270052")
	lonely := mustUser(t, s, "+15551270053")

	both, err := s.CreateChat(ctx, a.ID, "Both", []int64{b.ID})
	if err != nil {
		t.Fatalf("create both: %v", err)
	}
	solo, err := s.CreateChat(ctx, a.ID, "SoloA", nil)
	if err != nil {
		t.Fatalf("create solo: %v", err)
	}

	as, err := s.ChatsForUser(ctx, a.ID)
	if err != nil {
		t.Fatalf("chats for a: %v", err)
	}
	if len(as) != 2 {
		t.Fatalf("a chats = %+v, want 2", as)
	}
	bs, err := s.ChatsForUser(ctx, b.ID)
	if err != nil {
		t.Fatalf("chats for b: %v", err)
	}
	if len(bs) != 1 || bs[0].ID != both.ID {
		t.Fatalf("b chats = %+v, want only %d", bs, both.ID)
	}
	if bs[0].ID == solo.ID {
		t.Error("b sees a chat it is not in")
	}
	none, err := s.ChatsForUser(ctx, lonely.ID)
	if err != nil {
		t.Fatalf("chats for lonely: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("lonely chats = %+v, want none", none)
	}
}

func TestCreateChatRejectsOversizeAndWritesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270061")

	members := make([]int64, 0, 201)
	for i := range 201 {
		members = append(members, mustUser(t, s, fmt.Sprintf("+1555128%05d", i)).ID)
	}

	if _, err := s.CreateChat(ctx, a.ID, "Too big", members); !errors.Is(err, store.ErrChatFull) {
		t.Fatalf("create oversize chat: want ErrChatFull, got %v", err)
	}
	// No chat row, so no participant row either.
	for _, id := range append([]int64{a.ID}, members...) {
		cs, err := s.ChatsForUser(ctx, id)
		if err != nil {
			t.Fatalf("chats for %d: %v", id, err)
		}
		if len(cs) != 0 {
			t.Fatalf("user %d gained chats %+v after a rejected create", id, cs)
		}
	}
	// ChatsForUser joins chat_participants, so it cannot see an orphan chats row.
	// This database is cloned per test and starts empty, so no id may resolve.
	for id := int64(1); id <= 10; id++ {
		c, ok, err := s.ChatByID(ctx, id)
		if err != nil {
			t.Fatalf("chat by id %d: %v", id, err)
		}
		if ok {
			t.Fatalf("rejected create left chat row %+v", c)
		}
	}

	// The cap itself is inclusive: creator + 199 members is exactly 200 and must
	// succeed. Runs after the emptiness assertions so it cannot mask them.
	c, err := s.CreateChat(ctx, a.ID, "Exactly full", members[:199])
	if err != nil {
		t.Fatalf("create chat at the 200 limit: %v", err)
	}
	if got := participantIDs(t, s, c.ID); len(got) != 200 {
		t.Fatalf("participants at the limit = %d, want 200", len(got))
	}
}
