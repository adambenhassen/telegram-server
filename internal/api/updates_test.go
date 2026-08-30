package api_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
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
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
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
	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "hi", 7, 0, 0); err != nil {
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
		if u.AccessHash != api.DeriveUserHash(b.ID, u.ID) {
			t.Fatalf("access_hash = %d, want derived hash for viewer %d, peer %d", u.AccessHash, b.ID, u.ID)
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
			if u.AccessHash != api.DeriveUserHash(a.ID, u.ID) {
				t.Fatalf("self access_hash = %d, want derived hash for viewer %d, peer %d", u.AccessHash, a.ID, u.ID)
			}
			continue
		}
		sawPeer = true
		if u.ID != b.ID || u.Phone != "" {
			t.Fatalf("peer user = id %d phone %q, want id %d phone \"\"", u.ID, u.Phone, b.ID)
		}
		if u.AccessHash != api.DeriveUserHash(a.ID, u.ID) {
			t.Fatalf("peer access_hash = %d, want derived hash for viewer %d, peer %d", u.AccessHash, a.ID, u.ID)
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
	pt, id, err := api.InputPeer(&tg.InputPeerChat{ChatID: 3}, 0)
	if err != nil || pt != store.PeerTypeChat || id != 3 {
		t.Fatalf("inputPeer(chat 3) = (%d, %d, %v), want (chat, 3, nil)", pt, id, err)
	}
	pt, id, err = api.InputPeer(&tg.InputPeerUser{UserID: 5, AccessHash: api.DeriveUserHash(5, 5)}, 5)
	if err != nil || pt != store.PeerTypeUser || id != 5 {
		t.Fatalf("inputPeer(user 5) = (%d, %d, %v), want (user, 5, nil)", pt, id, err)
	}
	for _, p := range []tg.InputPeerClass{
		&tg.InputPeerUser{UserID: 5, AccessHash: 4},
		&tg.InputPeerChat{ChatID: 0},
		&tg.InputPeerEmpty{},
		&tg.InputPeerSelf{},
	} {
		if _, _, err := api.InputPeer(p, 0); err == nil {
			t.Fatalf("inputPeer(%T %+v) = nil error, want PEER_ID_INVALID", p, p)
		}
	}
}

func TestInputUserID(t *testing.T) {
	t.Parallel()
	if id, err := api.InputUserID(&tg.InputUserSelf{}, 5); err != nil || id != 5 {
		t.Fatalf("inputUserID(self, 5) = (%d, %v), want (5, nil)", id, err)
	}
	if id, err := api.InputUserID(api.InputUser(5, 9), 5); err != nil || id != 9 {
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

func TestChannelToTL(t *testing.T) {
	t.Parallel()
	c := store.Channel{ID: 4, Title: "news", CreatorID: 5, Date: time.Unix(1000, 0)}
	viewerID := int64(7)

	forbidden, ok := api.ChannelToTL(c, store.ChannelMember{}, false, viewerID).(*tg.ChannelForbidden)
	if !ok {
		t.Fatalf("channelToTL for non-member = %T, want *tg.ChannelForbidden", forbidden)
	}
	if forbidden.Title != "" || forbidden.ID != 4 {
		t.Fatalf("channelToTL for non-member = %+v, want id 4, empty title", forbidden)
	}
	if forbidden.AccessHash != api.DeriveChannelHash(viewerID, 4) {
		t.Fatalf("channelToTL for non-member access_hash = %d, want derived hash for viewer %d, channel %d", forbidden.AccessHash, viewerID, 4)
	}

	got, ok := api.ChannelToTL(c, store.ChannelMember{UserID: 5, Role: 2}, true, viewerID).(*tg.Channel)
	if !ok {
		t.Fatalf("channelToTL for member = %T, want *tg.Channel", got)
	}
	if !got.Broadcast || got.Megagroup || !got.Creator || got.Left {
		t.Fatalf("channelToTL for creator of a broadcast = %+v", got)
	}
	if got.Title != "news" || got.Date != 1000 {
		t.Fatalf("channelToTL for member = %+v, want news/date 1000", got)
	}
	if got.AccessHash != api.DeriveChannelHash(viewerID, 4) {
		t.Fatalf("channelToTL for member access_hash = %d, want derived hash for viewer %d, channel %d", got.AccessHash, viewerID, 4)
	}

	// Cross-viewer: hash for viewer 7 must differ from hash for viewer 9.
	got2, ok := api.ChannelToTL(c, store.ChannelMember{UserID: 5, Role: 2}, true, 9).(*tg.Channel)
	if !ok {
		t.Fatalf("channelToTL for viewer 9 = %T, want *tg.Channel", got2)
	}
	if got.AccessHash == got2.AccessHash {
		t.Error("different viewers returned same channel access_hash")
	}

	c.Megagroup = true
	got, ok = api.ChannelToTL(c, store.ChannelMember{UserID: 9, Role: 0}, true, viewerID).(*tg.Channel)
	if !ok {
		t.Fatalf("channelToTL for megagroup member = %T, want *tg.Channel", got)
	}
	if got.Broadcast || !got.Megagroup || got.Creator {
		t.Fatalf("channelToTL for plain member of a megagroup = %+v", got)
	}
}

func TestLoadChannelsNonMemberForbidden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551950001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551950002")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "news", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// A missing id is skipped, exactly as loadChats skips a missing chat.
	got, err := api.LoadChannelsForTest(s, []int64{ch.ID, ch.ID + 100000}, creator.ID)
	if err != nil {
		t.Fatalf("load channels: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("load channels for creator = %d entries, want 1", len(got))
	}
	if c, ok := got[0].(*tg.Channel); !ok || c.Title != "news" {
		t.Fatalf("load channels for creator = %#v, want *tg.Channel news", got[0])
	}

	got, err = api.LoadChannelsForTest(s, []int64{ch.ID}, outsider.ID)
	if err != nil {
		t.Fatalf("load channels outsider: %v", err)
	}
	if c, ok := got[0].(*tg.ChannelForbidden); !ok || c.Title != "" {
		t.Fatalf("load channels for outsider = %#v, want *tg.ChannelForbidden with empty title", got[0])
	}
}

// chatFixture creates a chat with members participants, owned by the first of
// members+1 fresh users: the trailing user is a non-participant. It returns the
// store, the users and the chat.
func chatFixture(t *testing.T, phonePrefix string, members int) (*store.Store, []store.User, store.Chat) {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	users := make([]store.User, members+1)
	for i := range users {
		u, err := s.CreateUser(ctx, fmt.Sprintf("%s%03d", phonePrefix, i))
		if err != nil {
			t.Fatalf("user %d: %v", i, err)
		}
		users[i] = u
	}
	invited := make([]int64, members-1)
	for i := range invited {
		invited[i] = users[i+1].ID
	}
	chat, err := s.CreateChat(ctx, users[0].ID, "team", invited)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	return s, users, chat
}

func TestBuildUpdatesChatMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, users, chat := chatFixture(t, "+1555131", 2)

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
	// client with only the sender renders the add as an unknown user. The viewer
	// shares no live edge with the just-added user, so the gate degrades them to
	// userEmpty — the id is still present, which is what the client needs.
	if !hasUserOrEmpty(svcUsers, users[2].ID) {
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
	s, users, chat := chatFixture(t, "+1555132", 2)

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
	if f.ID != chat.ID || f.Title != "" {
		t.Fatalf("forbidden chat = %+v, want id %d empty title", f, chat.ID)
	}

	// A rename by the creator must not reach the outsider: the live title
	// is a writable channel into an account that is no longer in the chat.
	if _, _, _, err := s.SetChatTitle(context.Background(), chat.ID, users[0].ID, "renamed"); err != nil {
		t.Fatalf("set chat title: %v", err)
	}
	after, err := api.LoadChatsForTest(s, []int64{chat.ID}, users[2].ID)
	if err != nil {
		t.Fatalf("load chats outsider after rename: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("outsider chats after rename = %d, want 1", len(after))
	}
	f2, ok := after[0].(*tg.ChatForbidden)
	if !ok {
		t.Fatalf("outsider chat type after rename = %T, want *tg.ChatForbidden", after[0])
	}
	if f2.ID != chat.ID || f2.Title != "" {
		t.Fatalf("forbidden chat after rename = %+v, want id %d empty title", f2, chat.ID)
	}
}

// A create service message replayed by someone who is not in the chat must not
// name the chat's current members: MessageActionChatCreate.Users is the same
// disclosure loadChats gates, reached through the message instead of the chat.
func TestBuildUpdatesCreateActionHidesMembersFromNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, users, chat := chatFixture(t, "+1555133", 3)

	// Extra delivers a copy to a user who is not a participant, which is how a
	// removed member keeps their retained copy of an event.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Text: "team",
		Action: store.ChatActionCreate, Extra: []int64{users[3].ID},
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
	if len(got) != 3 {
		t.Fatalf("member sees users %v, want all three participants", got)
	}
	// The action names them, so the batch must resolve them. users[2] is neither
	// the sender nor the viewer, so it reaches the batch only through the action's
	// user list: it is what makes this assertion discriminate.
	for _, id := range got {
		if !slices.Contains(batch, id) {
			t.Fatalf("batch users %v miss participant %d named by the action", batch, id)
		}
	}
	got, batch = createUsers(users[3].ID)
	if len(got) != 0 {
		t.Fatalf("non-member sees users %v, want none", got)
	}
	// Nor may a participant reach them through the batch's user list. The sender
	// is named by from_id and is not what the gate withholds; the others are.
	for _, id := range []int64{users[1].ID, users[2].ID} {
		if slices.Contains(batch, id) {
			t.Fatalf("non-member batch users %v disclose participant %d", batch, id)
		}
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

// hasUserOrEmpty reports whether us carries id as either a full user or a
// degraded userEmpty, which the loadUsers gate emits for an id the viewer is
// not entitled to see live.
func hasUserOrEmpty(us []tg.UserClass, id int64) bool {
	for _, uc := range us {
		switch u := uc.(type) {
		case *tg.User:
			if u.ID == id {
				return true
			}
		case *tg.UserEmpty:
			if u.ID == id {
				return true
			}
		}
	}
	return false
}

func TestGetDifferenceRendersSameMediaAsHistory(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	u1, u2 := mediaUsers(t, s, "+15551295001", "+15551295002")
	mediaMessage(t, s, u1, u2, "here", true)

	enc, err := api.GetDifferenceForTest(s, u2.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0})
	if err != nil {
		t.Fatalf("get difference: %v", err)
	}
	diff, ok := enc.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("difference type = %T, want *tg.UpdatesDifference", enc)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("new messages = %d, want 1", len(diff.NewMessages))
	}
	pulled, ok := diff.NewMessages[0].(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T, want *tg.Message", diff.NewMessages[0])
	}

	msgs := historyMessages(t, s, u2, u1)
	if len(msgs) != 1 {
		t.Fatalf("history = %d messages, want 1", len(msgs))
	}
	read, ok := msgs[0].(*tg.Message)
	if !ok {
		t.Fatalf("history message type = %T, want *tg.Message", msgs[0])
	}

	// The pull path and the read path must agree on the media they serve.
	if diffDoc, readDoc := mediaDocument(t, pulled), mediaDocument(t, read); !reflect.DeepEqual(diffDoc, readDoc) {
		t.Fatalf("getDifference document = %+v, getHistory document = %+v", diffDoc, readDoc)
	}
}

func TestUserStatusToTL(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Online user → UserStatusOnline.
	got := api.UserStatusToTL(store.User{IsOnline: true}, false)
	if _, ok := got.(*tg.UserStatusOnline); !ok {
		t.Fatalf("IsOnline=true → %T, want *tg.UserStatusOnline", got)
	}

	// Offline with last-seen → UserStatusOffline with correct timestamp.
	got = api.UserStatusToTL(store.User{IsOnline: false, LastSeenAt: &now}, false)
	off, ok := got.(*tg.UserStatusOffline)
	if !ok {
		t.Fatalf("IsOnline=false LastSeenAt=<time> → %T, want *tg.UserStatusOffline", got)
	}
	if off.WasOnline != int(now.Unix()) {
		t.Fatalf("WasOnline = %d, want %d", off.WasOnline, int(now.Unix()))
	}

	// Offline with nil last-seen → UserStatusEmpty.
	got = api.UserStatusToTL(store.User{IsOnline: false, LastSeenAt: nil}, false)
	if _, ok := got.(*tg.UserStatusEmpty); !ok {
		t.Fatalf("IsOnline=false LastSeenAt=nil → %T, want *tg.UserStatusEmpty", got)
	}

	// Self → UserStatusRecently regardless of online state.
	got = api.UserStatusToTL(store.User{IsOnline: true, LastSeenAt: &now}, true)
	if _, ok := got.(*tg.UserStatusRecently); !ok {
		t.Fatalf("self=true → %T, want *tg.UserStatusRecently", got)
	}
	got = api.UserStatusToTL(store.User{IsOnline: false, LastSeenAt: nil}, true)
	if _, ok := got.(*tg.UserStatusRecently); !ok {
		t.Fatalf("self=true IsOnline=false → %T, want *tg.UserStatusRecently", got)
	}
}

func TestUserToTLStatusField(t *testing.T) {
	t.Parallel()
	now := time.Now()

	online := api.UserToTL(store.User{ID: 1, IsOnline: true}, 2, false)
	if _, ok := online.Status.(*tg.UserStatusOnline); !ok {
		t.Fatalf("online user status = %T, want *tg.UserStatusOnline", online.Status)
	}

	offline := api.UserToTL(store.User{ID: 1, IsOnline: false, LastSeenAt: &now}, 2, false)
	if _, ok := offline.Status.(*tg.UserStatusOffline); !ok {
		t.Fatalf("offline user status = %T, want *tg.UserStatusOffline", offline.Status)
	}

	never := api.UserToTL(store.User{ID: 1, IsOnline: false, LastSeenAt: nil}, 2, false)
	if _, ok := never.Status.(*tg.UserStatusEmpty); !ok {
		t.Fatalf("never-seen user status = %T, want *tg.UserStatusEmpty", never.Status)
	}

	self := api.UserToTL(store.User{ID: 2, IsOnline: true, LastSeenAt: &now}, 2, true)
	if _, ok := self.Status.(*tg.UserStatusRecently); !ok {
		t.Fatalf("self user status = %T, want *tg.UserStatusRecently", self.Status)
	}
}

func TestDocumentToTLFileReferenceIsTheFileID(t *testing.T) {
	t.Parallel()

	d := api.DocumentToTL(2, store.File{ID: 0x0102030405060708, AccessHash: 7, MimeType: "text/plain", Size: 11})
	if len(d.FileReference) != 8 {
		t.Fatalf("file reference = %d bytes, want 8", len(d.FileReference))
	}
	if got := int64(binary.BigEndian.Uint64(d.FileReference)); got != d.ID { //nolint:gosec // G115: opaque 64-bit id, sign irrelevant
		t.Fatalf("file reference decodes to %d, want %d", got, d.ID)
	}
	if d.DCID != 2 {
		t.Fatalf("dc id = %d, want 2", d.DCID)
	}
	// A file with no name carries no attribute rather than an empty one.
	if len(d.Attributes) != 0 {
		t.Fatalf("attributes = %d, want 0 for an unnamed file", len(d.Attributes))
	}
}

// A send and an edit of the same row put two events in one batch naming one
// local id. batchMessages loads that row once and both updates must still carry
// the media, so the dedup cannot drop the second event's row.
func TestGetDifferenceRendersMediaOnEditOfTheSameRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u1, u2 := mediaUsers(t, s, "+15551295003", "+15551295004")
	sent, f := mediaMessage(t, s, u1, u2, "here", true)

	if _, _, err := s.EditMessage(ctx, u1.ID, sent.LocalID, "there"); err != nil {
		t.Fatalf("edit: %v", err)
	}

	enc, err := api.GetDifferenceForTest(s, u1.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0})
	if err != nil {
		t.Fatalf("get difference: %v", err)
	}
	diff, ok := enc.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("difference type = %T, want *tg.UpdatesDifference", enc)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("new messages = %d, want 1", len(diff.NewMessages))
	}
	newMsg, ok := diff.NewMessages[0].(*tg.Message)
	if !ok {
		t.Fatalf("new message type = %T, want *tg.Message", diff.NewMessages[0])
	}
	if doc := mediaDocument(t, newMsg); doc.ID != f.ID {
		t.Fatalf("new message document = %d, want %d", doc.ID, f.ID)
	}

	var edits int
	for _, u := range diff.OtherUpdates {
		up, ok := u.(*tg.UpdateEditMessage)
		if !ok {
			continue
		}
		edits++
		edited, ok := up.Message.(*tg.Message)
		if !ok {
			t.Fatalf("edited message type = %T, want *tg.Message", up.Message)
		}
		if edited.Message != "there" {
			t.Fatalf("edited text = %q, want %q", edited.Message, "there")
		}
		if doc := mediaDocument(t, edited); doc.ID != f.ID {
			t.Fatalf("edited message document = %d, want %d", doc.ID, f.ID)
		}
	}
	if edits != 1 {
		t.Fatalf("edit updates = %d, want 1", edits)
	}
}

// TestGetDifferenceQtsGapFilling checks that missed encrypted messages are
// returned in NewEncryptedMessages when the client's qts lags behind.
func TestGetDifferenceQtsGapFilling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551299001")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15551299002")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, alice.ID); err != nil {
		t.Fatalf("ensure alice state: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure bob state: %v", err)
	}

	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte("ga"), []byte("hash"), 1)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if _, err := s.AcceptSecretChat(ctx, chat.ID, bob.ID, []byte("gb"), 0); err != nil {
		t.Fatalf("accept chat: %v", err)
	}

	// Pre-send qts for bob is 0; alice sends one message.
	preSendQts := 0
	_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
		RecipientID: bob.ID,
		ChatID:      chat.ID,
		RandomID:    42,
		Data:        []byte("secret"),
	})
	if err != nil {
		t.Fatalf("send encrypted: %v", err)
	}

	// AC1: bob calls getDifference with pre-send qts; expects message in NewEncryptedMessages.
	enc, err := api.GetDifferenceForTest(s, bob.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0, Qts: preSendQts})
	if err != nil {
		t.Fatalf("get difference: %v", err)
	}
	diff, ok := enc.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesDifference", enc)
	}
	if len(diff.NewEncryptedMessages) != 1 {
		t.Fatalf("new encrypted messages = %d, want 1", len(diff.NewEncryptedMessages))
	}
	msg, ok := diff.NewEncryptedMessages[0].(*tg.EncryptedMessage)
	if !ok {
		t.Fatalf("encrypted message type = %T, want *tg.EncryptedMessage", diff.NewEncryptedMessages[0])
	}
	if msg.RandomID != 42 {
		t.Fatalf("random_id = %d, want 42", msg.RandomID)
	}
	if diff.State.Qts != 1 {
		t.Fatalf("state.qts = %d, want 1", diff.State.Qts)
	}
	// AC5: no secret chat messages in new_messages.
	if len(diff.NewMessages) != 0 {
		t.Fatalf("new_messages = %d, want 0", len(diff.NewMessages))
	}

	// AC2: bob already caught up (req.Qts == state.Qts) — no encrypted messages.
	enc, err = api.GetDifferenceForTest(s, bob.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0, Qts: 1})
	if err != nil {
		t.Fatalf("get difference caught up: %v", err)
	}
	switch v := enc.(type) {
	case *tg.UpdatesDifferenceEmpty:
		// expected — no pts events either
	case *tg.UpdatesDifference:
		if len(v.NewEncryptedMessages) != 0 {
			t.Fatalf("caught-up: NewEncryptedMessages = %d, want 0", len(v.NewEncryptedMessages))
		}
	default:
		t.Fatalf("caught-up type = %T", enc)
	}

	// AC3: bob ahead (req.Qts > state.Qts) — no encrypted messages; use a
	// future date so secret_chats query returns nothing, giving differenceEmpty.
	futureDate := int(time.Now().Add(time.Hour).Unix())
	enc, err = api.GetDifferenceForTest(s, bob.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0, Qts: 999, Date: futureDate})
	if err != nil {
		t.Fatalf("get difference ahead: %v", err)
	}
	if _, ok := enc.(*tg.UpdatesDifferenceEmpty); !ok {
		t.Fatalf("ahead type = %T, want *tg.UpdatesDifferenceEmpty", enc)
	}

	// AC4: differenceSlice when the gap exceeds maxDiffEvents (500).
	// Use fresh users so their qts starts at 0.
	carol, err := s.CreateUser(ctx, "+15551299003")
	if err != nil {
		t.Fatalf("create carol: %v", err)
	}
	dave, err := s.CreateUser(ctx, "+15551299004")
	if err != nil {
		t.Fatalf("create dave: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, carol.ID); err != nil {
		t.Fatalf("ensure carol state: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, dave.ID); err != nil {
		t.Fatalf("ensure dave state: %v", err)
	}
	chat2, _, err := s.CreateSecretChatRequest(ctx, carol.ID, dave.ID, []byte("ga2"), []byte("hash2"), 2)
	if err != nil {
		t.Fatalf("create chat2: %v", err)
	}
	if _, err := s.AcceptSecretChat(ctx, chat2.ID, dave.ID, []byte("gb2"), 0); err != nil {
		t.Fatalf("accept chat2: %v", err)
	}

	// Seed 501 events so the window is truncated at 500.
	const overCap = 501
	for i := range overCap {
		_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
			RecipientID: dave.ID,
			ChatID:      chat2.ID,
			RandomID:    int64(1000 + i),
			Data:        []byte("x"),
		})
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	// dave's state.Qts is now 501; call with Qts=0 to span the full gap.
	enc, err = api.GetDifferenceForTest(s, dave.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0, Qts: 0, Date: futureDate})
	if err != nil {
		t.Fatalf("get difference slice: %v", err)
	}
	slice, ok := enc.(*tg.UpdatesDifferenceSlice)
	if !ok {
		t.Fatalf("AC4 type = %T, want *tg.UpdatesDifferenceSlice", enc)
	}
	if len(slice.NewEncryptedMessages) != 500 {
		t.Fatalf("AC4 encrypted messages = %d, want 500", len(slice.NewEncryptedMessages))
	}
	// IntermediateState.Qts must be the 500th event's qts (500), not state.Qts (501).
	if slice.IntermediateState.Qts != 500 {
		t.Fatalf("AC4 IntermediateState.Qts = %d, want 500", slice.IntermediateState.Qts)
	}
}
