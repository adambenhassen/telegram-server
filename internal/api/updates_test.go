package api_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMain(m *testing.M) {
	if err := pgtest.Prewarm(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest prewarm: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestBuildUpdatesNewMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	a, err := s.CreateUser(ctx, "+15551290001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551290002")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "hi", 7); err != nil {
		t.Fatalf("send: %v", err)
	}

	ups, users, state, err := api.BuildUpdatesForTest(s, b.ID, 0)
	if err != nil {
		t.Fatalf("build updates: %v", err)
	}
	if state.Pts != 1 {
		t.Fatalf("state pts = %d, want 1", state.Pts)
	}
	if len(ups) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups))
	}
	nm, ok := ups[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("update type = %T, want *tg.UpdateNewMessage", ups[0])
	}
	msg, ok := nm.Message.(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T", nm.Message)
	}
	if msg.Message != "hi" || msg.Out {
		t.Fatalf("message = %q out=%v, want \"hi\" out=false", msg.Message, msg.Out)
	}
	peer, ok := msg.PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != a.ID {
		t.Fatalf("peer = %+v, want PeerUser %d", msg.PeerID, a.ID)
	}
	if len(users) == 0 {
		t.Fatal("no users hydrated")
	}
	// The recipient's copy has from == peer == a, so only the sender is hydrated
	// here: a stranger's phone number must not come with them.
	for _, uc := range users {
		u, ok := uc.(*tg.User)
		if !ok {
			t.Fatalf("user type = %T, want *tg.User", uc)
		}
		if u.Self || u.ID != a.ID {
			t.Fatalf("hydrated user = id %d self=%v, want id %d self=false", u.ID, u.Self, a.ID)
		}
		if u.Phone != "" {
			t.Fatalf("peer phone = %q, want empty", u.Phone)
		}
	}

	// The sender's own copy hydrates both sides: a keeps its own phone as self,
	// b is a peer there and must not disclose one.
	_, senderUsers, _, err := api.BuildUpdatesForTest(s, a.ID, 0)
	if err != nil {
		t.Fatalf("build updates sender: %v", err)
	}
	var sawSelf, sawPeer bool
	for _, uc := range senderUsers {
		u, ok := uc.(*tg.User)
		if !ok {
			t.Fatalf("sender user type = %T, want *tg.User", uc)
		}
		if u.Self {
			sawSelf = true
			if u.ID != a.ID || u.Phone != a.Phone {
				t.Fatalf("self user = id %d phone %q, want id %d phone %q", u.ID, u.Phone, a.ID, a.Phone)
			}
			continue
		}
		sawPeer = true
		if u.ID != b.ID || u.Phone != "" {
			t.Fatalf("peer user = id %d phone %q, want id %d phone \"\"", u.ID, u.Phone, b.ID)
		}
	}
	if !sawSelf || !sawPeer {
		t.Fatalf("sender side hydrated self=%v peer=%v, want both", sawSelf, sawPeer)
	}

	enc, err := api.GetStateForTest(s, b.ID)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	st, ok := enc.(*tg.UpdatesState)
	if !ok || st.Pts != 1 {
		t.Fatalf("getState = %#v, want pts 1", enc)
	}
}

func TestInputPeer(t *testing.T) {
	t.Parallel()
	pt, id, err := api.InputPeer(&tg.InputPeerChat{ChatID: 3})
	if err != nil || pt != store.PeerTypeChat || id != 3 {
		t.Fatalf("inputPeer(chat 3) = (%d, %d, %v), want (chat, 3, nil)", pt, id, err)
	}
	pt, id, err = api.InputPeer(&tg.InputPeerUser{UserID: 5, AccessHash: 5})
	if err != nil || pt != store.PeerTypeUser || id != 5 {
		t.Fatalf("inputPeer(user 5) = (%d, %d, %v), want (user, 5, nil)", pt, id, err)
	}
	for _, p := range []tg.InputPeerClass{
		&tg.InputPeerUser{UserID: 5, AccessHash: 4},
		&tg.InputPeerChat{ChatID: 0},
		&tg.InputPeerEmpty{},
		&tg.InputPeerSelf{},
	} {
		if _, _, err := api.InputPeer(p); err == nil {
			t.Fatalf("inputPeer(%T %+v) = nil error, want PEER_ID_INVALID", p, p)
		}
	}
}

func TestInputUserID(t *testing.T) {
	t.Parallel()
	if id, err := api.InputUserID(&tg.InputUserSelf{}, 5); err != nil || id != 5 {
		t.Fatalf("inputUserID(self, 5) = (%d, %v), want (5, nil)", id, err)
	}
	if id, err := api.InputUserID(&tg.InputUser{UserID: 9, AccessHash: 9}, 5); err != nil || id != 9 {
		t.Fatalf("inputUserID(user 9) = (%d, %v), want (9, nil)", id, err)
	}
	for _, u := range []tg.InputUserClass{
		&tg.InputUser{UserID: 9, AccessHash: 8},
		&tg.InputUserEmpty{},
	} {
		if _, err := api.InputUserID(u, 5); err == nil {
			t.Fatalf("inputUserID(%T %+v) = nil error, want PEER_ID_INVALID", u, u)
		}
	}
}

func TestMessageToTL(t *testing.T) {
	t.Parallel()

	chatMsg := api.MessageToTL(store.Message{
		LocalID: 4, PeerType: store.PeerTypeChat, PeerID: 3, FromID: 5, Text: "hi",
	}, nil)
	m, ok := chatMsg.(*tg.Message)
	if !ok {
		t.Fatalf("chat message type = %T, want *tg.Message", chatMsg)
	}
	peer, ok := m.PeerID.(*tg.PeerChat)
	if !ok || peer.ChatID != 3 {
		t.Fatalf("chat message peer = %+v, want PeerChat 3", m.PeerID)
	}
	if from, ok := m.FromID.(*tg.PeerUser); !ok || from.UserID != 5 {
		t.Fatalf("chat message from = %+v, want PeerUser 5", m.FromID)
	}

	svc := api.MessageToTL(store.Message{
		LocalID: 6, PeerType: store.PeerTypeChat, PeerID: 3, FromID: 5,
		Action: store.ChatActionDeleteUser, ActionUserID: 12,
	}, nil)
	ms, ok := svc.(*tg.MessageService)
	if !ok {
		t.Fatalf("service message type = %T, want *tg.MessageService", svc)
	}
	del, ok := ms.Action.(*tg.MessageActionChatDeleteUser)
	if !ok || del.UserID != 12 {
		t.Fatalf("action = %+v, want MessageActionChatDeleteUser 12", ms.Action)
	}

	add := api.MessageToTL(store.Message{
		PeerType: store.PeerTypeChat, PeerID: 3, Action: store.ChatActionAddUser, ActionUserID: 9,
	}, nil)
	ams, ok := add.(*tg.MessageService)
	if !ok {
		t.Fatalf("add message type = %T, want *tg.MessageService", add)
	}
	au, ok := ams.Action.(*tg.MessageActionChatAddUser)
	if !ok || len(au.Users) != 1 || au.Users[0] != 9 {
		t.Fatalf("action = %+v, want MessageActionChatAddUser [9]", ams.Action)
	}

	create := api.MessageToTL(store.Message{
		PeerType: store.PeerTypeChat, PeerID: 3, Text: "team", Action: store.ChatActionCreate,
	}, []int64{5, 9, 12})
	cms, ok := create.(*tg.MessageService)
	if !ok {
		t.Fatalf("create message type = %T, want *tg.MessageService", create)
	}
	cr, ok := cms.Action.(*tg.MessageActionChatCreate)
	if !ok || cr.Title != "team" || len(cr.Users) != 3 {
		t.Fatalf("action = %+v, want MessageActionChatCreate team [5 9 12]", cms.Action)
	}

	// A 1:1 row is unchanged from before chats existed.
	plain := api.MessageToTL(store.Message{
		LocalID: 1, PeerType: store.PeerTypeUser, PeerID: 5, FromID: 5, Text: "hi",
	}, nil)
	pm, ok := plain.(*tg.Message)
	if !ok {
		t.Fatalf("plain message type = %T, want *tg.Message", plain)
	}
	if pu, ok := pm.PeerID.(*tg.PeerUser); !ok || pu.UserID != 5 {
		t.Fatalf("plain peer = %+v, want PeerUser 5", pm.PeerID)
	}
}

func TestChatToTLCreator(t *testing.T) {
	t.Parallel()
	c := store.Chat{ID: 3, Title: "team", CreatorID: 5, Version: 2, Date: time.Unix(1000, 0)}
	if got := api.ChatToTL(c, 3, 5); !got.Creator || got.ParticipantsCount != 3 || got.Version != 2 || got.Title != "team" {
		t.Fatalf("chatToTL for creator = %+v", got)
	}
	if got := api.ChatToTL(c, 3, 9); got.Creator {
		t.Fatalf("chatToTL for non-creator has Creator set: %+v", got)
	}
}

// chatFixture creates a chat owned by the first of three fresh users and returns
// the store, the users and the chat.
func chatFixture(t *testing.T, phonePrefix string) (*store.Store, []store.User, store.Chat) {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	users := make([]store.User, 3)
	for i := range users {
		u, err := s.CreateUser(ctx, fmt.Sprintf("%s%03d", phonePrefix, i))
		if err != nil {
			t.Fatalf("user %d: %v", i, err)
		}
		users[i] = u
	}
	chat, err := s.CreateChat(ctx, users[0].ID, "team", []int64{users[1].ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	return s, users, chat
}

func TestBuildUpdatesChatMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, users, chat := chatFixture(t, "+1555131")

	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Text: "hi", RandomID: 11,
	}); err != nil {
		t.Fatalf("send chat message: %v", err)
	}

	ups, _, chats, err := api.BuildUpdatesChatsForTest(s, users[1].ID, 0)
	if err != nil {
		t.Fatalf("build updates: %v", err)
	}
	if len(ups) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups))
	}
	nm, ok := ups[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("update type = %T, want *tg.UpdateNewMessage", ups[0])
	}
	msg, ok := nm.Message.(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T, want *tg.Message", nm.Message)
	}
	peer, ok := msg.PeerID.(*tg.PeerChat)
	if !ok || peer.ChatID != chat.ID {
		t.Fatalf("peer = %+v, want PeerChat %d", msg.PeerID, chat.ID)
	}
	// The service message announcing a third member's arrival hydrates as a
	// MessageService, not a Message.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Action: store.ChatActionAddUser,
		ActionUserID: users[2].ID, Extra: []int64{users[2].ID},
	}); err != nil {
		t.Fatalf("send add-user service message: %v", err)
	}
	svcUps, svcUsers, _, err := api.BuildUpdatesChatsForTest(s, users[1].ID, 1)
	if err != nil {
		t.Fatalf("build updates service: %v", err)
	}
	if len(svcUps) != 1 {
		t.Fatalf("service updates = %d, want 1", len(svcUps))
	}
	svcNM, ok := svcUps[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("service update type = %T, want *tg.UpdateNewMessage", svcUps[0])
	}
	ms, ok := svcNM.Message.(*tg.MessageService)
	if !ok {
		t.Fatalf("service message type = %T, want *tg.MessageService", svcNM.Message)
	}
	au, ok := ms.Action.(*tg.MessageActionChatAddUser)
	if !ok || len(au.Users) != 1 || au.Users[0] != users[2].ID {
		t.Fatalf("action = %+v, want MessageActionChatAddUser [%d]", ms.Action, users[2].ID)
	}
	// The added user is named by the action, so the batch must carry them: a
	// client with only the sender renders the add as an unknown user.
	if !hasUser(svcUsers, users[2].ID) {
		t.Fatalf("batch users = %v, want the added user %d", userIDs(svcUsers), users[2].ID)
	}

	if len(chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(chats))
	}
	c, ok := chats[0].(*tg.Chat)
	if !ok || c.ID != chat.ID || c.Title != "team" || c.ParticipantsCount != 2 {
		t.Fatalf("chat = %+v, want id %d title team participants 2", chats[0], chat.ID)
	}
}

// A chat the viewer is not a member of must come back as chatForbidden: their
// retained message copies outlive membership, so loadChats is the gate that
// stops title/version/participant count leaking after removal.
func TestLoadChatsNonMemberForbidden(t *testing.T) {
	t.Parallel()
	s, users, chat := chatFixture(t, "+1555132")

	member, err := api.LoadChatsForTest(s, []int64{chat.ID}, users[1].ID)
	if err != nil {
		t.Fatalf("load chats member: %v", err)
	}
	if len(member) != 1 {
		t.Fatalf("member chats = %d, want 1", len(member))
	}
	if _, ok := member[0].(*tg.Chat); !ok {
		t.Fatalf("member chat type = %T, want *tg.Chat", member[0])
	}

	outsider, err := api.LoadChatsForTest(s, []int64{chat.ID}, users[2].ID)
	if err != nil {
		t.Fatalf("load chats outsider: %v", err)
	}
	if len(outsider) != 1 {
		t.Fatalf("outsider chats = %d, want 1", len(outsider))
	}
	f, ok := outsider[0].(*tg.ChatForbidden)
	if !ok {
		t.Fatalf("outsider chat type = %T, want *tg.ChatForbidden", outsider[0])
	}
	if f.ID != chat.ID || f.Title != "team" {
		t.Fatalf("forbidden chat = %+v, want id %d title team", f, chat.ID)
	}
}

// A create service message replayed by someone who is not in the chat must not
// name the chat's current members: MessageActionChatCreate.Users is the same
// disclosure loadChats gates, reached through the message instead of the chat.
func TestBuildUpdatesCreateActionHidesMembersFromNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, users, chat := chatFixture(t, "+1555133")

	// Extra delivers a copy to a user who is not a participant, which is how a
	// removed member keeps their retained copy of an event.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Text: "team",
		Action: store.ChatActionCreate, Extra: []int64{users[2].ID},
	}); err != nil {
		t.Fatalf("send create service message: %v", err)
	}

	createUsers := func(viewerID int64) (action []int64, batch []int64) {
		t.Helper()
		ups, batchUsers, _, err := api.BuildUpdatesChatsForTest(s, viewerID, 0)
		if err != nil {
			t.Fatalf("build updates for %d: %v", viewerID, err)
		}
		if len(ups) != 1 {
			t.Fatalf("updates for %d = %d, want 1", viewerID, len(ups))
		}
		nm, ok := ups[0].(*tg.UpdateNewMessage)
		if !ok {
			t.Fatalf("update type = %T, want *tg.UpdateNewMessage", ups[0])
		}
		ms, ok := nm.Message.(*tg.MessageService)
		if !ok {
			t.Fatalf("message type = %T, want *tg.MessageService", nm.Message)
		}
		cr, ok := ms.Action.(*tg.MessageActionChatCreate)
		if !ok {
			t.Fatalf("action = %T, want *tg.MessageActionChatCreate", ms.Action)
		}
		return cr.Users, userIDs(batchUsers)
	}

	got, batch := createUsers(users[1].ID)
	if len(got) != 2 {
		t.Fatalf("member sees users %v, want both participants", got)
	}
	// The action names them, so the batch must resolve them.
	for _, id := range got {
		if !slices.Contains(batch, id) {
			t.Fatalf("batch users %v miss participant %d named by the action", batch, id)
		}
	}
	got, batch = createUsers(users[2].ID)
	if len(got) != 0 {
		t.Fatalf("non-member sees users %v, want none", got)
	}
	// Nor may a participant reach them through the batch's user list. The sender
	// is named by from_id and is not what the gate withholds; the other member is.
	if slices.Contains(batch, users[1].ID) {
		t.Fatalf("non-member batch users %v disclose participant %d", batch, users[1].ID)
	}
}

// userIDs lists the ids of a batch's hydrated users.
func userIDs(us []tg.UserClass) []int64 {
	out := make([]int64, 0, len(us))
	for _, uc := range us {
		if u, ok := uc.(*tg.User); ok {
			out = append(out, u.ID)
		}
	}
	return out
}

func hasUser(us []tg.UserClass, id int64) bool {
	return slices.Contains(userIDs(us), id)
}
