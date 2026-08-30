package api_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// chatUser creates a user with a phone unique to this file's block.
func chatUser(t *testing.T, s *store.Store, n int) store.User {
	t.Helper()
	u, err := s.CreateUser(context.Background(), fmt.Sprintf("+1555131%04d", n))
	if err != nil {
		t.Fatalf("user %d: %v", n, err)
	}
	return u
}

func apiPts(t *testing.T, s *store.Store, userID int64) int {
	t.Helper()
	st, err := s.State(context.Background(), userID)
	if err != nil {
		t.Fatalf("state %d: %v", userID, err)
	}
	return st.Pts
}

func apiParticipants(t *testing.T, s *store.Store, chatID int64) []int64 {
	t.Helper()
	ps, err := s.Participants(context.Background(), chatID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	ids := make([]int64, len(ps))
	for i, p := range ps {
		ids[i] = p.UserID
	}
	return ids
}

// serviceCopy returns one owner's copy of local message localID, failing when it
// is absent.
func serviceCopy(t *testing.T, s *store.Store, ownerID, localID int64) store.Message {
	t.Helper()
	m, ok, err := s.MessageByOwnerLocal(context.Background(), ownerID, localID)
	if err != nil || !ok {
		t.Fatalf("owner %d copy %d: ok=%v err=%v", ownerID, localID, ok, err)
	}
	return m
}

func wantRPC(t *testing.T, err error, msg string) {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != msg {
		t.Fatalf("got %v, want %s", err, msg)
	}
}

func TestAddChatUserByNonMemberIsPeerIDInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 1)
	b := chatUser(t, s, 2)
	outsider := chatUser(t, s, 3)
	target := chatUser(t, s, 4)
	chat, err := s.CreateChat(ctx, a.ID, "Members", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	_, err = api.AddChatUserForTest(s, outsider.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(outsider.ID, target.ID),
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want unchanged 2", got)
	}

	// A non-member removal is the same boundary and the same error.
	_, err = api.DeleteChatUserForTest(s, outsider.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(outsider.ID, b.ID),
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants after remove attempt = %v, want unchanged 2", got)
	}
}

// An unknown chat id must be indistinguishable from a chat the caller is not in,
// or the dense id space becomes a census of every chat on the server.
func TestChatUserUnknownChatIsPeerIDInvalid(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a := chatUser(t, s, 11)
	target := chatUser(t, s, 12)

	_, err := api.AddChatUserForTest(s, a.ID, &tg.MessagesAddChatUserRequest{
		ChatID: 9_999_999,
		UserID: api.InputUser(a.ID, target.ID),
	})
	wantRPC(t, err, "PEER_ID_INVALID")

	_, err = api.DeleteChatUserForTest(s, a.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: 9_999_999,
		UserID: api.InputUser(a.ID, target.ID),
	})
	wantRPC(t, err, "PEER_ID_INVALID")
}

// A target id no users row backs must be rejected before the store sees it:
// without the check the add fails the chat_participants FK as an INTERNAL and the
// removal quietly reports success.
func TestChatUserUnknownTargetIsPeerIDInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 81)
	b := chatUser(t, s, 82)
	chat, err := s.CreateChat(ctx, a.ID, "Ghost target", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	const unused = 8_888_888

	_, err = api.AddChatUserForTest(s, a.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID,
		UserID: &tg.InputUser{UserID: unused, AccessHash: unused},
	})
	wantRPC(t, err, "PEER_ID_INVALID")

	_, err = api.DeleteChatUserForTest(s, a.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: &tg.InputUser{UserID: unused, AccessHash: unused},
	})
	wantRPC(t, err, "PEER_ID_INVALID")

	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want unchanged 2", got)
	}
	if got := apiPts(t, s, a.ID); got != 0 {
		t.Errorf("caller pts = %d, want 0", got)
	}
}

func TestDeleteChatUserNonCreatorIsPeerIDInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 85)
	member := chatUser(t, s, 86)
	target := chatUser(t, s, 87)
	chat, err := s.CreateChat(ctx, creator.ID, "Members", []int64{member.ID, target.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	_, err = api.DeleteChatUserForTest(s, member.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(member.ID, target.ID),
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	if got := apiParticipants(t, s, chat.ID); len(got) != 3 {
		t.Fatalf("participants = %v, want unchanged 3", got)
	}
	for _, u := range []store.User{creator, member, target} {
		if got := apiPts(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
	}
}

func TestDeleteChatUserCannotRemoveCreator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 88)
	member := chatUser(t, s, 89)
	chat, err := s.CreateChat(ctx, creator.ID, "Members", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	for _, caller := range []store.User{creator, member} {
		_, err = api.DeleteChatUserForTest(s, caller.ID, &tg.MessagesDeleteChatUserRequest{
			ChatID: chat.ID,
			UserID: api.InputUser(caller.ID, creator.ID),
		})
		wantRPC(t, err, "PEER_ID_INVALID")
	}
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Fatalf("participants = %v, want unchanged 2", got)
	}
}

// A full chat reports USERS_TOO_MUCH, not the generic INTERNAL a dropped
// ErrChatFull mapping would produce.
func TestAddChatUserAtCapIsUsersTooMuch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 91)
	members := make([]int64, 0, 199)
	for i := range 199 {
		u, err := s.CreateUser(ctx, fmt.Sprintf("+1555133%04d", i))
		if err != nil {
			t.Fatalf("member %d: %v", i, err)
		}
		members = append(members, u.ID)
	}
	chat, err := s.CreateChat(ctx, a.ID, "Full", members)
	if err != nil {
		t.Fatalf("create full chat: %v", err)
	}
	outsider := chatUser(t, s, 92)

	_, err = api.AddChatUserForTest(s, a.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(a.ID, outsider.ID),
	})
	wantRPC(t, err, "USERS_TOO_MUCH")
	if got := apiParticipants(t, s, chat.ID); len(got) != 200 {
		t.Errorf("participants = %d, want unchanged 200", len(got))
	}
}

func TestAddChatUserAlreadyMemberReturnsEmptyUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 21)
	b := chatUser(t, s, 22)
	chat, err := s.CreateChat(ctx, a.ID, "Dup", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	enc, err := api.AddChatUserForTest(s, a.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(a.ID, b.ID),
	})
	if err != nil {
		t.Fatalf("re-add: %v", err)
	}
	invited, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesInvitedUsers", enc)
	}
	ups, ok := invited.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("updates = %T, want *tg.Updates", invited.Updates)
	}
	if len(ups.Updates) != 0 {
		t.Errorf("updates = %+v, want empty", ups.Updates)
	}
	for _, u := range []store.User{a, b} {
		if got := apiPts(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if _, present, err := s.MessageByOwnerLocal(ctx, u.ID, 1); err != nil || present {
			t.Errorf("owner %d got a message row: present=%v err=%v", u.ID, present, err)
		}
	}
	if v, _, err := s.ChatByID(ctx, chat.ID); err != nil || v.Version != chat.Version {
		t.Errorf("version = %d, want unchanged %d (err=%v)", v.Version, chat.Version, err)
	}
}

func TestAddChatUserAnnouncesToEveryMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 31)
	b := chatUser(t, s, 32)
	c := chatUser(t, s, 33)
	chat, err := s.CreateChat(ctx, a.ID, "Growing", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Adding remains open to every member, not just the creator.
	enc, err := api.AddChatUserForTest(s, b.ID, &tg.MessagesAddChatUserRequest{
		ChatID:   chat.ID,
		UserID:   api.InputUser(b.ID, c.ID),
		FwdLimit: 100,
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	invited, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesInvitedUsers", enc)
	}
	ups, ok := invited.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("updates = %T, want *tg.Updates", invited.Updates)
	}
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %+v, want one updateNewMessage", ups.Updates)
	}
	nm, ok := ups.Updates[0].(*tg.UpdateNewMessage)
	if !ok || nm.Pts != 1 {
		t.Fatalf("update = %+v, want updateNewMessage at pts 1", ups.Updates[0])
	}
	svc, ok := nm.Message.(*tg.MessageService)
	if !ok {
		t.Fatalf("message = %T, want *tg.MessageService", nm.Message)
	}
	act, ok := svc.Action.(*tg.MessageActionChatAddUser)
	if !ok || len(act.Users) != 1 || act.Users[0] != c.ID {
		t.Fatalf("action = %+v, want ChatAddUser on %d", svc.Action, c.ID)
	}
	// The reply must carry the chat at its bumped version.
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %+v, want one", ups.Chats)
	}
	tlChat, ok := ups.Chats[0].(*tg.Chat)
	if !ok || tlChat.Version != chat.Version+1 || tlChat.ParticipantsCount != 3 {
		t.Errorf("chat = %+v, want version %d and 3 participants", ups.Chats[0], chat.Version+1)
	}

	if got := apiParticipants(t, s, chat.ID); len(got) != 3 {
		t.Fatalf("participants = %v, want 3", got)
	}
	fanout := serviceCopy(t, s, a.ID, 1).FanoutID
	for _, u := range []store.User{a, b, c} {
		if got := apiPts(t, s, u.ID); got != 1 {
			t.Errorf("owner %d pts = %d, want 1", u.ID, got)
		}
		m := serviceCopy(t, s, u.ID, 1)
		if m.Action != store.ChatActionAddUser || m.ActionUserID != c.ID {
			t.Errorf("owner %d copy = action %d subject %d, want AddUser on %d", u.ID, m.Action, m.ActionUserID, c.ID)
		}
		if m.FanoutID != fanout {
			t.Errorf("owner %d fanout_id = %d, want shared %d", u.ID, m.FanoutID, fanout)
		}
	}
}

func TestDeleteChatUserAnnouncesToRemainingAndRemoved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 41)
	b := chatUser(t, s, 42)
	c := chatUser(t, s, 43)
	chat, err := s.CreateChat(ctx, a.ID, "Shrinking", []int64{b.ID, c.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	// c speaks before being removed: their own copy must survive the removal.
	if _, _, _, err = s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: c.ID, Text: "still mine", RandomID: 7001,
	}); err != nil {
		t.Fatalf("chat send: %v", err)
	}

	enc, err := api.DeleteChatUserForTest(s, a.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID:        chat.ID,
		UserID:        api.InputUser(a.ID, c.ID),
		RevokeHistory: true,
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result = %T, want *tg.Updates", enc)
	}
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %+v, want one updateNewMessage", ups.Updates)
	}

	if got := apiParticipants(t, s, chat.ID); len(got) != 2 || got[0] == c.ID || got[1] == c.ID {
		t.Fatalf("participants = %v, want a and b only", got)
	}
	fanout := serviceCopy(t, s, a.ID, 2).FanoutID
	// The removed user gets the announcement too, at their own local id.
	for _, u := range []store.User{a, b, c} {
		m := serviceCopy(t, s, u.ID, 2)
		if m.Action != store.ChatActionDeleteUser || m.ActionUserID != c.ID {
			t.Errorf("owner %d copy = action %d subject %d, want DeleteUser on %d", u.ID, m.Action, m.ActionUserID, c.ID)
		}
		if m.FanoutID != fanout {
			t.Errorf("owner %d fanout_id = %d, want shared %d", u.ID, m.FanoutID, fanout)
		}
	}
	// RevokeHistory is ignored: the removed member keeps their own copies.
	old := serviceCopy(t, s, c.ID, 1)
	if old.Text != "still mine" || old.Deleted {
		t.Errorf("removed user's own message = %+v, want kept and undeleted", old)
	}

	// A later chat send must not reach the removed user.
	if _, perOwner, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "after", RandomID: 7002,
	}); err != nil {
		t.Fatalf("later send: %v", err)
	} else if _, reached := perOwner[c.ID]; reached {
		t.Errorf("removed user reached by later send: %+v", perOwner)
	}
	if _, present, err := s.MessageByOwnerLocal(ctx, c.ID, 3); err != nil || present {
		t.Errorf("removed user got a third row: present=%v err=%v", present, err)
	}
}

func TestDeleteChatUserNonMemberChangesNothing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 51)
	b := chatUser(t, s, 52)
	outsider := chatUser(t, s, 53)
	chat, err := s.CreateChat(ctx, a.ID, "Stable", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	enc, err := api.DeleteChatUserForTest(s, a.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(a.ID, outsider.ID),
	})
	if err != nil {
		t.Fatalf("remove non-member: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result = %T, want *tg.Updates", enc)
	}
	if len(ups.Updates) != 0 {
		t.Errorf("updates = %+v, want empty", ups.Updates)
	}
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want unchanged 2", got)
	}
	if got := apiPts(t, s, a.ID); got != 0 {
		t.Errorf("caller pts = %d, want 0", got)
	}
	if v, _, err := s.ChatByID(ctx, chat.ID); err != nil || v.Version != chat.Version {
		t.Errorf("version = %d, want unchanged %d (err=%v)", v.Version, chat.Version, err)
	}
}

// Self-removal is how leaving a chat works, so it is allowed even though the
// announcement's sender is no longer a member by the time it is written.
func TestDeleteChatUserSelfLeaves(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 61)
	b := chatUser(t, s, 62)
	chat, err := s.CreateChat(ctx, a.ID, "Leaving", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	enc, err := api.DeleteChatUserForTest(s, b.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: &tg.InputUserSelf{},
	})
	if err != nil {
		t.Fatalf("self leave: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result = %T, want *tg.Updates", enc)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %+v, want one", ups.Chats)
	}
	if _, ok := ups.Chats[0].(*tg.ChatForbidden); !ok {
		t.Fatalf("chats[0] = %T, want *tg.ChatForbidden", ups.Chats[0])
	}

	if got := apiParticipants(t, s, chat.ID); len(got) != 1 || got[0] != a.ID {
		t.Fatalf("participants = %v, want just %d", got, a.ID)
	}
	// The leaver is announced to as well, so their client learns it is out.
	m := serviceCopy(t, s, b.ID, 1)
	if m.Action != store.ChatActionDeleteUser || m.ActionUserID != b.ID {
		t.Errorf("leaver copy = %+v, want DeleteUser on self", m)
	}
}

func TestAddChatUserUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.AddChatUserForTest(s, 0, &tg.MessagesAddChatUserRequest{
		ChatID: 1,
		UserID: &tg.InputUser{UserID: 1, AccessHash: 1},
	})
	wantRPC(t, err, "AUTH_KEY_UNREGISTERED")
}

func TestDeleteChatUserUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.DeleteChatUserForTest(s, 0, &tg.MessagesDeleteChatUserRequest{
		ChatID: 1,
		UserID: &tg.InputUser{UserID: 1, AccessHash: 1},
	})
	wantRPC(t, err, "AUTH_KEY_UNREGISTERED")
}

// A removed member still holds their dialog row, so getDialogs keeps listing the
// chat — and must describe it as tg.ChatForbidden rather than serving live
// metadata (F6).
func TestRemovedUserGetDialogsSeesChatForbidden(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openStore(t)
	a := chatUser(t, s, 71)
	b := chatUser(t, s, 72)
	chat, err := s.CreateChat(ctx, a.ID, "Forbidden", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if _, _, _, err = s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "hi", RandomID: 7101,
	}); err != nil {
		t.Fatalf("chat send: %v", err)
	}
	if _, err = api.DeleteChatUserForTest(s, a.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID,
		UserID: api.InputUser(a.ID, b.ID),
	}); err != nil {
		t.Fatalf("remove: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, b.ID)
	if err != nil {
		t.Fatalf("get dialogs: %v", err)
	}
	dlgs, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesDialogs", enc)
	}
	if len(dlgs.Dialogs) == 0 {
		t.Fatalf("removed user's dialogs = %+v, want the chat still listed", dlgs.Dialogs)
	}
	var sawForbidden bool
	for _, c := range dlgs.Chats {
		if f, ok := c.(*tg.ChatForbidden); ok && f.ID == chat.ID {
			sawForbidden = true
		}
	}
	if !sawForbidden {
		t.Errorf("chats = %+v, want a *tg.ChatForbidden for %d", dlgs.Chats, chat.ID)
	}
}
