package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// rpcMessage extracts the RPC error message, failing the test when err is not one.
func rpcMessage(t *testing.T, err error) string {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("expected rpc error, got %v", err)
	}
	return rpc.Message
}

func inputUsers(ids ...int64) []tg.InputUserClass {
	out := make([]tg.InputUserClass, len(ids))
	for i, id := range ids {
		out[i] = &tg.InputUser{UserID: id, AccessHash: id}
	}
	return out
}

func TestChatTitleValidation(t *testing.T) {
	t.Parallel()

	for name, in := range map[string]string{
		"empty":      "",
		"whitespace": "   \t\n ",
		"too long":   strings.Repeat("a", 256),
	} {
		if _, err := api.ChatTitle(in); err == nil {
			t.Errorf("%s: expected CHAT_TITLE_EMPTY, got nil", name)
		} else if msg := rpcMessage(t, err); msg != "CHAT_TITLE_EMPTY" {
			t.Errorf("%s: got %s, want CHAT_TITLE_EMPTY", name, msg)
		}
	}

	got, err := api.ChatTitle("  Team  ")
	if err != nil || got != "Team" {
		t.Fatalf("valid title: got %q err=%v, want \"Team\"", got, err)
	}
	if _, err := api.ChatTitle(strings.Repeat("a", 255)); err != nil {
		t.Fatalf("255 chars rejected: %v", err)
	}
}

func TestHandleCreateChatRejectsBadTitle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551292001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}

	for name, title := range map[string]string{
		"empty":      "",
		"whitespace": "   ",
		"too long":   strings.Repeat("a", 256),
	} {
		_, err := api.CreateChatForTest(s, a.ID, &tg.MessagesCreateChatRequest{Title: title})
		if msg := rpcMessage(t, err); msg != "CHAT_TITLE_EMPTY" {
			t.Errorf("%s: got %s, want CHAT_TITLE_EMPTY", name, msg)
		}
		chats, err := s.ChatsForUser(ctx, a.ID)
		if err != nil {
			t.Fatalf("chats for user: %v", err)
		}
		if len(chats) != 0 {
			t.Fatalf("%s: created %d chats, want 0", name, len(chats))
		}
	}
}

func TestHandleCreateChatUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.CreateChatForTest(s, 0, &tg.MessagesCreateChatRequest{Title: "Team"})
	if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unbound create: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
}

func TestHandleCreateChatFansOutToEveryMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users := make([]store.User, 0, 4)
	for _, phone := range []string{"+15551292101", "+15551292102", "+15551292103", "+15551292104"} {
		u, err := s.CreateUser(ctx, phone)
		if err != nil {
			t.Fatalf("user %s: %v", phone, err)
		}
		users = append(users, u)
	}
	creator, invited := users[0], users[1:]

	enc, err := api.CreateChatForTest(s, creator.ID, &tg.MessagesCreateChatRequest{
		Users: inputUsers(invited[0].ID, invited[1].ID, invited[2].ID),
		Title: "Team",
	})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesInvitedUsers", enc)
	}
	if len(res.MissingInvitees) != 0 {
		t.Fatalf("missing invitees = %v, want none", res.MissingInvitees)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("updates type = %T, want *tg.Updates", res.Updates)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(ups.Chats))
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T, want *tg.Chat", ups.Chats[0])
	}
	if chat.Title != "Team" || chat.ParticipantsCount != 4 || !chat.Creator {
		t.Fatalf("chat = %+v, want title Team, 4 participants, creator", chat)
	}
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups.Updates))
	}
	newMsg, ok := ups.Updates[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("update type = %T, want *tg.UpdateNewMessage", ups.Updates[0])
	}
	if newMsg.PtsCount != 1 {
		t.Fatalf("pts count = %d, want 1", newMsg.PtsCount)
	}
	svc, ok := newMsg.Message.(*tg.MessageService)
	if !ok {
		t.Fatalf("message type = %T, want *tg.MessageService", newMsg.Message)
	}
	action, ok := svc.Action.(*tg.MessageActionChatCreate)
	if !ok {
		t.Fatalf("action type = %T, want *tg.MessageActionChatCreate", svc.Action)
	}
	if action.Title != "Team" || len(action.Users) != 4 {
		t.Fatalf("action = %+v, want title Team and 4 users", action)
	}

	parts, err := s.Participants(ctx, chat.ID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if len(parts) != 4 {
		t.Fatalf("participants = %d, want 4", len(parts))
	}

	// One service-message row per member, all sharing one fanout id, each with
	// its owner's pts advanced by the create.
	var fanoutID int64
	for _, u := range users {
		m, ok, err := s.MessageByOwnerLocal(ctx, u.ID, 1)
		if err != nil || !ok {
			t.Fatalf("user %d copy: ok=%v err=%v", u.ID, ok, err)
		}
		if m.Action != store.ChatActionCreate || m.PeerType != store.PeerTypeChat || m.PeerID != chat.ID {
			t.Fatalf("user %d copy = %+v, want chat create row", u.ID, m)
		}
		if fanoutID == 0 {
			fanoutID = m.FanoutID
		}
		if m.FanoutID == 0 || m.FanoutID != fanoutID {
			t.Fatalf("user %d fanout id = %d, want %d", u.ID, m.FanoutID, fanoutID)
		}
		st, err := s.State(ctx, u.ID)
		if err != nil {
			t.Fatalf("state %d: %v", u.ID, err)
		}
		if st.Pts != 1 {
			t.Fatalf("user %d pts = %d, want 1", u.ID, st.Pts)
		}
	}
}

func TestHandleCreateChatReportsMissingInvitee(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292201")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551292202")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	absent := other.ID + 100000

	enc, err := api.CreateChatForTest(s, creator.ID, &tg.MessagesCreateChatRequest{
		Users: inputUsers(other.ID, absent),
		Title: "Team",
	})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesInvitedUsers", enc)
	}
	if len(res.MissingInvitees) != 1 || res.MissingInvitees[0].UserID != absent {
		t.Fatalf("missing invitees = %+v, want just %d", res.MissingInvitees, absent)
	}
	chats, err := s.ChatsForUser(ctx, creator.ID)
	if err != nil {
		t.Fatalf("chats for user: %v", err)
	}
	if len(chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(chats))
	}
	parts, err := s.Participants(ctx, chats[0].ID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("participants = %d, want 2 (absent user excluded)", len(parts))
	}
}

func TestHandleCreateChatRejectsMalformedInputUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292301")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}

	_, err = api.CreateChatForTest(s, creator.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{&tg.InputUser{UserID: 7, AccessHash: 8}},
		Title: "Team",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
	chats, err := s.ChatsForUser(ctx, creator.ID)
	if err != nil {
		t.Fatalf("chats for user: %v", err)
	}
	if len(chats) != 0 {
		t.Fatalf("chats = %d, want 0", len(chats))
	}
}

// createChatForTest builds a chat through the handler and returns it with its
// members, so the editChatTitle cases start from the real RPC's output.
func createChatForTest(t *testing.T, s *store.Store, creator int64, title string, members ...int64) int64 {
	t.Helper()
	enc, err := api.CreateChatForTest(s, creator, &tg.MessagesCreateChatRequest{
		Users: inputUsers(members...), Title: title,
	})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesInvitedUsers", enc)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok || len(ups.Chats) != 1 {
		t.Fatalf("updates = %T with %d chats, want *tg.Updates with 1", res.Updates, len(ups.Chats))
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T, want *tg.Chat", ups.Chats[0])
	}
	return chat.ID
}

func TestHandleEditChatTitleRenamesAndAnnounces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292401")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551292402")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	chatID := createChatForTest(t, s, creator.ID, "Team", member.ID)

	before, _, err := s.ChatByID(ctx, chatID)
	if err != nil {
		t.Fatalf("chat before: %v", err)
	}

	enc, err := api.EditChatTitleForTest(s, member.ID, &tg.MessagesEditChatTitleRequest{
		ChatID: chatID, Title: "Team 2",
	})
	if err != nil {
		t.Fatalf("edit title: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(ups.Chats))
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok || chat.Title != "Team 2" {
		t.Fatalf("chat = %+v, want title Team 2", ups.Chats[0])
	}
	if chat.Version <= before.Version {
		t.Fatalf("version = %d, want > %d", chat.Version, before.Version)
	}

	after, _, err := s.ChatByID(ctx, chatID)
	if err != nil {
		t.Fatalf("chat after: %v", err)
	}
	if after.Title != "Team 2" {
		t.Fatalf("stored title = %q, want Team 2", after.Title)
	}

	// Both members hold an edit-title service message, and neither reply leaks
	// the other member's pts.
	for _, uid := range []int64{creator.ID, member.ID} {
		m, ok, err := s.MessageByOwnerLocal(ctx, uid, 2)
		if err != nil || !ok {
			t.Fatalf("user %d rename copy: ok=%v err=%v", uid, ok, err)
		}
		if m.Action != store.ChatActionEditTitle || m.Text != "Team 2" {
			t.Fatalf("user %d rename copy = %+v", uid, m)
		}
	}
}

func TestHandleEditChatTitleRejectsBadTitle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292501")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	chatID := createChatForTest(t, s, creator.ID, "Team")

	for name, title := range map[string]string{
		"empty":      "",
		"whitespace": "  ",
		"too long":   strings.Repeat("a", 256),
	} {
		_, err := api.EditChatTitleForTest(s, creator.ID, &tg.MessagesEditChatTitleRequest{
			ChatID: chatID, Title: title,
		})
		if msg := rpcMessage(t, err); msg != "CHAT_TITLE_EMPTY" {
			t.Errorf("%s: got %s, want CHAT_TITLE_EMPTY", name, msg)
		}
	}
	c, _, err := s.ChatByID(ctx, chatID)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if c.Title != "Team" {
		t.Fatalf("title = %q, want Team", c.Title)
	}
}

// TestHandleEditChatTitleNonMember covers the F4 boundary from the wire side:
// the handler surfaces store.ErrNotMember as PEER_ID_INVALID, which is what
// makes the store's in-transaction membership check reach the caller correctly.
// The removal-mid-call race itself is not reachable at this level.
func TestHandleEditChatTitleNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292601")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551292602")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	chatID := createChatForTest(t, s, creator.ID, "Team")

	_, err = api.EditChatTitleForTest(s, outsider.ID, &tg.MessagesEditChatTitleRequest{
		ChatID: chatID, Title: "Hijacked",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("non-member: got %s, want PEER_ID_INVALID", msg)
	}
	c, _, err := s.ChatByID(ctx, chatID)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if c.Title != "Team" {
		t.Fatalf("title = %q, want Team", c.Title)
	}
}

// TestHandleEditChatTitleAbsentChat pins that an absent chat is indistinguishable
// from a chat the caller is not in: a distinguishable answer over a dense
// BIGSERIAL id space would let a caller enumerate every chat on the server.
func TestHandleEditChatTitleAbsentChat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "+15551292701")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}
	chatID := createChatForTest(t, s, caller.ID, "Team")

	_, err = api.EditChatTitleForTest(s, caller.ID, &tg.MessagesEditChatTitleRequest{
		ChatID: chatID + 100000, Title: "Ghost",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("absent chat: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestHandleEditChatTitleUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.EditChatTitleForTest(s, 0, &tg.MessagesEditChatTitleRequest{ChatID: 1, Title: "Team"})
	if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unbound edit: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
}
