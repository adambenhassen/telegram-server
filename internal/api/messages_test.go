package api_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestPeerUserIDValidatesAccessHash(t *testing.T) {
	t.Parallel()

	id, err := api.PeerUserID(&tg.InputPeerUser{UserID: 5, AccessHash: 5})
	if err != nil || id != 5 {
		t.Fatalf("valid peer: id=%d err=%v", id, err)
	}

	for name, peer := range map[string]tg.InputPeerClass{
		"wrong hash": &tg.InputPeerUser{UserID: 5, AccessHash: 6},
		"zero id":    &tg.InputPeerUser{UserID: 0, AccessHash: 0},
		"self":       &tg.InputPeerSelf{},
		"chat":       &tg.InputPeerChat{ChatID: 1},
	} {
		if _, err := api.PeerUserID(peer); err == nil {
			t.Errorf("%s: expected PEER_ID_INVALID, got nil", name)
		}
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, _ := openStoreDSN(t)
	return s
}

// openStoreDSN opens a store and hands back the DSN it runs on, for a test that
// also needs a raw connection to the same database. Each pgtest.DSN call clones
// a fresh database, so the DSN has to come from the same call as the store.
func openStoreDSN(t *testing.T) (*store.Store, string) {
	t.Helper()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s, dsn
}

func TestHandleSendMessagePersistsAndReturnsUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551291001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551291002")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	enc, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerUser{UserID: b.ID, AccessHash: b.ID},
		Message:  "hello",
		RandomID: 4242,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	var sawMsgID, sawNewMsg bool
	for _, u := range ups.Updates {
		switch up := u.(type) {
		case *tg.UpdateMessageID:
			if up.RandomID == 4242 {
				sawMsgID = true
			}
		case *tg.UpdateNewMessage:
			if m, ok := up.Message.(*tg.Message); ok && m.Message == "hello" && m.Out {
				sawNewMsg = true
			}
		}
	}
	if !sawMsgID || !sawNewMsg {
		t.Fatalf("updates missing pieces: msgID=%v newMsg=%v", sawMsgID, sawNewMsg)
	}

	// Recipient got the inbox copy.
	recv, ok, err := s.MessageByOwnerLocal(ctx, b.ID, 1)
	if err != nil || !ok {
		t.Fatalf("recipient copy: ok=%v err=%v", ok, err)
	}
	if recv.Text != "hello" || recv.Out {
		t.Fatalf("recipient copy wrong: %+v", recv)
	}
}

func TestHandleSendMessageUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.SendMessageForTest(s, 0, &tg.MessagesSendMessageRequest{
		Peer:    &tg.InputPeerUser{UserID: 1, AccessHash: 1},
		Message: "x",
	})
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unbound send: got %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

// rpcError asserts err is an RPC error carrying msg.
func rpcError(t *testing.T, err error, msg string) {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != msg {
		t.Fatalf("got %v, want %s", err, msg)
	}
}

// chatWith creates one user per phone and a chat owned by the first of them,
// with every other user as a member.
func chatWith(t *testing.T, s *store.Store, phones ...string) ([]store.User, store.Chat) {
	t.Helper()
	ctx := context.Background()
	users := make([]store.User, len(phones))
	for i, p := range phones {
		u, err := s.CreateUser(ctx, p)
		if err != nil {
			t.Fatalf("user %s: %v", p, err)
		}
		users[i] = u
	}
	members := make([]int64, 0, len(users)-1)
	for _, u := range users[1:] {
		members = append(members, u.ID)
	}
	chat, err := s.CreateChat(ctx, users[0].ID, "Crew", members)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	return users, chat
}

func TestHandleSendMessageToChatFansOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292001", "+15551292002", "+15551292003")

	// Pull one member's pts off the sender's before the fan-out, so an envelope
	// that echoed another owner's entry from perOwner cannot coincide with the
	// sender's own.
	if _, err := api.SendMessageForTest(s, users[1].ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerUser{UserID: users[2].ID, AccessHash: users[2].ID}, Message: "dm", RandomID: 1,
	}); err != nil {
		t.Fatalf("pre-advance: %v", err)
	}

	enc, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerChat{ChatID: chat.ID},
		Message:  "hi",
		RandomID: 42,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	// The envelope must name the sender's own copy and the sender's own new pts.
	sent, herr := s.History(ctx, users[0].ID, store.PeerTypeChat, chat.ID, 0, 1)
	if herr != nil || len(sent) != 1 {
		t.Fatalf("sender copy: %+v err=%v", sent, herr)
	}
	senderState, serr := s.State(ctx, users[0].ID)
	if serr != nil {
		t.Fatalf("sender state: %v", serr)
	}

	var sawMsgID, sawNewMsg bool
	for _, u := range ups.Updates {
		switch up := u.(type) {
		case *tg.UpdateMessageID:
			if up.RandomID != 42 {
				t.Errorf("updateMessageID random_id = %d, want 42", up.RandomID)
			}
			if int64(up.ID) != sent[0].LocalID {
				t.Errorf("updateMessageID id = %d, want %d", up.ID, sent[0].LocalID)
			}
			sawMsgID = true
		case *tg.UpdateNewMessage:
			m, isMsg := up.Message.(*tg.Message)
			if !isMsg {
				continue
			}
			peer, isChat := m.PeerID.(*tg.PeerChat)
			from, isUser := m.FromID.(*tg.PeerUser)
			sawNewMsg = isChat && peer.ChatID == chat.ID &&
				isUser && from.UserID == users[0].ID && m.Out && m.Message == "hi"
			if int64(m.ID) != sent[0].LocalID {
				t.Errorf("updateNewMessage id = %d, want %d", m.ID, sent[0].LocalID)
			}
			if up.Pts != senderState.Pts {
				t.Errorf("updateNewMessage pts = %d, want the sender's %d", up.Pts, senderState.Pts)
			}
			if up.PtsCount != 1 {
				t.Errorf("updateNewMessage pts_count = %d, want 1", up.PtsCount)
			}
		}
	}
	if !sawMsgID || !sawNewMsg {
		t.Fatalf("updates missing pieces: msgID=%v newMsg=%v", sawMsgID, sawNewMsg)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(ups.Chats))
	}
	if c, isChat := ups.Chats[0].(*tg.Chat); !isChat || c.ID != chat.ID {
		t.Fatalf("chat entry = %#v, want live chat %d", ups.Chats[0], chat.ID)
	}

	// One row per member, and every member's pts advanced by the send.
	for _, u := range users {
		msgs, herr := s.History(ctx, u.ID, store.PeerTypeChat, chat.ID, 0, 100)
		if herr != nil {
			t.Fatalf("history %d: %v", u.ID, herr)
		}
		if len(msgs) != 1 || msgs[0].Text != "hi" || msgs[0].FromID != users[0].ID {
			t.Fatalf("user %d copies = %+v", u.ID, msgs)
		}
		if msgs[0].Out != (u.ID == users[0].ID) {
			t.Errorf("user %d out = %v", u.ID, msgs[0].Out)
		}
	}
}

func TestHandleSendMessageToChatIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551292011", "+15551292012", "+15551292013")

	// A resend must cost nothing a client can observe, and a lost !dup guard shows
	// up as a second round of update nudges long before it shows up in the rows.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err = conn.Exec(ctx, "LISTEN "+store.ChannelUpdates); err != nil {
		t.Fatalf("listen: %v", err)
	}

	peer := &tg.InputPeerChat{ChatID: chat.ID}
	first, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "hi", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// One nudge per member for the send that did write.
	notified := make(map[string]bool, len(users))
	for range users {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, werr := conn.WaitForNotification(waitCtx)
		cancel()
		if werr != nil {
			t.Fatalf("wait notification: %v", werr)
		}
		notified[n.Payload] = true
	}
	for _, u := range users {
		if !notified[strconv.FormatInt(u.ID, 10)] {
			t.Errorf("no update nudge for member %d", u.ID)
		}
	}
	before := make(map[int64]int, len(users))
	for _, u := range users {
		st, serr := s.State(ctx, u.ID)
		if serr != nil {
			t.Fatalf("state %d: %v", u.ID, serr)
		}
		before[u.ID] = st.Pts
	}

	second, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "hi", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	if msgID(t, first) != msgID(t, second) {
		t.Errorf("resend id = %d, want %d", msgID(t, second), msgID(t, first))
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if n, werr := conn.WaitForNotification(waitCtx); werr == nil {
		t.Errorf("resend emitted a nudge for %q", n.Payload)
	} else if !errors.Is(werr, context.DeadlineExceeded) {
		t.Fatalf("wait after resend: %v", werr)
	}
	for _, u := range users {
		st, serr := s.State(ctx, u.ID)
		if serr != nil {
			t.Fatalf("state %d: %v", u.ID, serr)
		}
		if st.Pts != before[u.ID] {
			t.Errorf("user %d pts = %d, want %d", u.ID, st.Pts, before[u.ID])
		}
		msgs, herr := s.History(ctx, u.ID, store.PeerTypeChat, chat.ID, 0, 100)
		if herr != nil {
			t.Fatalf("history %d: %v", u.ID, herr)
		}
		if len(msgs) != 1 {
			t.Errorf("user %d rows = %d, want 1", u.ID, len(msgs))
		}
	}
}

// msgID pulls the sender's local id out of an updateMessageID envelope.
func msgID(t *testing.T, enc bin.Encoder) int {
	t.Helper()
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	for _, u := range ups.Updates {
		if m, isID := u.(*tg.UpdateMessageID); isID {
			return m.ID
		}
	}
	t.Fatal("no updateMessageID")
	return 0
}

func TestHandleChatRPCsRejectNonMembers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292021", "+15551292022")
	outsider, err := s.CreateUser(ctx, "+15551292023")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}

	// A chat the caller is not in and a chat that does not exist are the same error.
	for _, chatID := range []int64{chat.ID, chat.ID + 10_000} {
		_, serr := api.SendMessageForTest(s, outsider.ID, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: "probe", RandomID: 7,
		})
		rpcError(t, serr, "PEER_ID_INVALID")
		_, herr := api.GetHistoryForTest(s, outsider.ID, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID},
		})
		rpcError(t, herr, "PEER_ID_INVALID")
	}

	// Nothing was written for anyone.
	for _, u := range append(users, outsider) {
		msgs, herr := s.History(ctx, u.ID, store.PeerTypeChat, chat.ID, 0, 100)
		if herr != nil {
			t.Fatalf("history %d: %v", u.ID, herr)
		}
		if len(msgs) != 0 {
			t.Errorf("user %d rows = %d, want 0", u.ID, len(msgs))
		}
	}
}

func TestHandleGetHistoryOnChatListsEveryAuthor(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292031", "+15551292032", "+15551292033")
	peer := &tg.InputPeerChat{ChatID: chat.ID}

	for i, u := range users {
		if _, err := api.SendMessageForTest(s, u.ID, &tg.MessagesSendMessageRequest{
			Peer: peer, Message: "m", RandomID: int64(100 + i),
		}); err != nil {
			t.Fatalf("send %d: %v", u.ID, err)
		}
	}

	enc, err := api.GetHistoryForTest(s, users[0].ID, &tg.MessagesGetHistoryRequest{Peer: peer})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	res, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesMessages", enc)
	}
	if len(res.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(res.Messages))
	}
	// Newest-first.
	prev := 1 << 30
	for _, m := range res.Messages {
		msg, isMsg := m.(*tg.Message)
		if !isMsg {
			t.Fatalf("message type = %T", m)
		}
		if msg.ID >= prev {
			t.Fatalf("ids not descending: %d after %d", msg.ID, prev)
		}
		prev = msg.ID
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	for _, u := range users {
		if !got[u.ID] {
			t.Errorf("user list missing author %d", u.ID)
		}
	}
	if len(res.Chats) != 1 {
		t.Errorf("chats = %d, want 1", len(res.Chats))
	}
}

func TestHandleGetDialogsMixesUsersAndChats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292041", "+15551292042")
	other, err := s.CreateUser(ctx, "+15551292043")
	if err != nil {
		t.Fatalf("other: %v", err)
	}

	if _, err = api.SendMessageForTest(s, users[1].ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerUser{UserID: other.ID, AccessHash: other.ID}, Message: "1:1", RandomID: 1,
	}); err != nil {
		t.Fatalf("dm: %v", err)
	}
	// The group's last message is from the other member, so the caller can only
	// render it if Users carries the top message's author.
	if _, err = api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Message: "group", RandomID: 2,
	}); err != nil {
		t.Fatalf("chat send: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	if len(res.Dialogs) != 2 {
		t.Fatalf("dialogs = %d, want 2", len(res.Dialogs))
	}
	var sawUser, sawChat bool
	for _, d := range res.Dialogs {
		switch p := d.(*tg.Dialog).Peer.(type) {
		case *tg.PeerUser:
			sawUser = p.UserID == other.ID
		case *tg.PeerChat:
			sawChat = p.ChatID == chat.ID
		}
	}
	if !sawUser || !sawChat {
		t.Fatalf("peers: user=%v chat=%v", sawUser, sawChat)
	}
	if len(res.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(res.Chats))
	}
	c, isChat := res.Chats[0].(*tg.Chat)
	if !isChat || c.Title != "Crew" {
		t.Fatalf("chat entry = %#v, want live chat titled Crew", res.Chats[0])
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	for _, id := range []int64{users[0].ID, users[1].ID, other.ID} {
		if !got[id] {
			t.Errorf("user list missing %d", id)
		}
	}
}

// A service row names user ids in its action, and a client renders them as
// unknown users unless the enclosing Users covers them.
func TestHandleGetHistoryOnChatListsServiceActionUsers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292071", "+15551292072")
	joiner, err := s.CreateUser(ctx, "+15551292073")
	if err != nil {
		t.Fatalf("joiner: %v", err)
	}
	if _, _, _, err = s.AddChatUser(ctx, chat.ID, joiner.ID, users[0].ID); err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, _, _, err = s.RemoveChatUser(ctx, chat.ID, joiner.ID, users[0].ID); err != nil {
		t.Fatalf("remove: %v", err)
	}

	enc, err := api.GetHistoryForTest(s, users[1].ID, &tg.MessagesGetHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID},
	})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	res, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesMessages", enc)
	}
	var sawAdd, sawDelete bool
	for _, m := range res.Messages {
		svc, isSvc := m.(*tg.MessageService)
		if !isSvc {
			continue
		}
		switch a := svc.Action.(type) {
		case *tg.MessageActionChatAddUser:
			sawAdd = len(a.Users) == 1 && a.Users[0] == joiner.ID
		case *tg.MessageActionChatDeleteUser:
			sawDelete = a.UserID == joiner.ID
		}
	}
	if !sawAdd || !sawDelete {
		t.Fatalf("service rows: add=%v delete=%v", sawAdd, sawDelete)
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	if !got[joiner.ID] {
		t.Errorf("user list missing action subject %d", joiner.ID)
	}
}

func TestHandleGetDialogsHidesChatAfterRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292051", "+15551292052")

	if _, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Message: "before", RandomID: 5,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, users[1].ID, users[0].ID); err != nil {
		t.Fatalf("remove: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	if len(res.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(res.Chats))
	}
	forbidden, ok := res.Chats[0].(*tg.ChatForbidden)
	if !ok {
		t.Fatalf("chat entry = %#v, want *tg.ChatForbidden", res.Chats[0])
	}
	if forbidden.ID != chat.ID {
		t.Errorf("forbidden id = %d, want %d", forbidden.ID, chat.ID)
	}
	// The dialog row survives removal by design; only the metadata is withheld.
	if len(res.Dialogs) != 1 {
		t.Errorf("dialogs = %d, want 1", len(res.Dialogs))
	}
}

// When the top message of a chat dialog is an add-user service row, the user
// named by action_user_id must appear in the Users list so the client can
// render the participant instead of an unknown placeholder. Without the fix,
// Users only contains the author and the viewer.
func TestHandleGetDialogsListsAddUserActionSubject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292081", "+15551292082")
	joiner, err := s.CreateUser(ctx, "+15551292083")
	if err != nil {
		t.Fatalf("joiner: %v", err)
	}
	if _, _, _, err = s.AddChatUser(ctx, chat.ID, joiner.ID, users[0].ID); err != nil {
		t.Fatalf("add: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	for _, id := range []int64{users[0].ID, users[1].ID, joiner.ID} {
		if !got[id] {
			t.Errorf("user list missing %d", id)
		}
	}
}

// When the top message of a chat dialog is a delete-user service row, the
// removed user must appear in the Users list. Without the fix, the removed
// user is absent and the client renders an unknown placeholder.
func TestHandleGetDialogsListsDeleteUserActionSubject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292091", "+15551292092", "+15551292093")
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, users[2].ID, users[0].ID); err != nil {
		t.Fatalf("remove: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	for _, id := range []int64{users[0].ID, users[1].ID, users[2].ID} {
		if !got[id] {
			t.Errorf("user list missing %d", id)
		}
	}
}

// When the only message in a chat is the create row, the top message on the
// dialog must carry the participant ids, and every participant must appear in
// Users. Without the fix, the action renders with an empty user list.
func TestHandleGetDialogsOnCreateRowListsParticipants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292101", "+15551292102", "+15551292103")
	// CreateChat writes no message; handleCreateChat announces the create row,
	// and that fan-out is what gives each member a dialog.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Text: "Crew", Action: store.ChatActionCreate,
	}); err != nil {
		t.Fatalf("create row: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	for _, u := range users {
		if !got[u.ID] {
			t.Errorf("user list missing participant %d", u.ID)
		}
	}
	// Top message should be the create action with users populated.
	if len(res.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(res.Messages))
	}
	svc, ok := res.Messages[0].(*tg.MessageService)
	if !ok {
		t.Fatalf("top message type = %T, want *tg.MessageService", res.Messages[0])
	}
	createAction, ok := svc.Action.(*tg.MessageActionChatCreate)
	if !ok {
		t.Fatalf("action type = %T, want *tg.MessageActionChatCreate", svc.Action)
	}
	if len(createAction.Users) != 3 {
		t.Errorf("create action users = %d, want 3", len(createAction.Users))
	}
}

// A viewer removed from a chat still gets their dialog row, and RemoveChatUser
// fans the announcement to the removed user, so their top message is the
// delete-user row naming them. Without the fix ActionUserID never enters the
// user list. The chat itself stays degraded to tg.ChatForbidden.
func TestHandleGetDialogsRemovedViewerSeesDeleteRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292111", "+15551292112")
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, users[1].ID, users[0].ID); err != nil {
		t.Fatalf("remove: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	// Chat should be forbidden.
	if len(res.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(res.Chats))
	}
	forbidden, ok := res.Chats[0].(*tg.ChatForbidden)
	if !ok {
		t.Fatalf("chat entry = %#v, want *tg.ChatForbidden", res.Chats[0])
	}
	if forbidden.ID != chat.ID {
		t.Errorf("forbidden id = %d, want %d", forbidden.ID, chat.ID)
	}
	// The dialog row survives removal.
	if len(res.Dialogs) != 1 {
		t.Errorf("dialogs = %d, want 1", len(res.Dialogs))
	}
	// Top message is the delete-user announcement naming the removed viewer.
	if len(res.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(res.Messages))
	}
	svc, ok := res.Messages[0].(*tg.MessageService)
	if !ok {
		t.Fatalf("top message type = %T, want *tg.MessageService", res.Messages[0])
	}
	del, ok := svc.Action.(*tg.MessageActionChatDeleteUser)
	if !ok {
		t.Fatalf("action type = %T, want *tg.MessageActionChatDeleteUser", svc.Action)
	}
	if del.UserID != users[1].ID {
		t.Errorf("action user = %d, want %d", del.UserID, users[1].ID)
	}
	got := make(map[int64]bool, len(res.Users))
	for _, u := range res.Users {
		got[u.GetID()] = true
	}
	if !got[users[1].ID] {
		t.Errorf("user list missing removed viewer %d", users[1].ID)
	}
}

// Criterion 4, the authorization gate itself: a non-member whose dialog top
// message is still the create row must not get the chat's live member list.
// No RPC produces that state — RemoveChatUser fans a delete row that becomes
// the removed account's top message — so the membership row is dropped
// directly, leaving the retained dialog pointing at the create row.
func TestHandleGetDialogsNonMemberOnCreateRowGetsNoParticipants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551292121", "+15551292122", "+15551292123")
	// Fan the create row while the viewer is still a member: SendChatMessage
	// writes dialog rows only for current members and rejects a non-member
	// sender, so this cannot move below the DELETE.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: users[0].ID, Text: "Crew", Action: store.ChatActionCreate,
	}); err != nil {
		t.Fatalf("create row: %v", err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err = conn.Exec(ctx,
		"DELETE FROM chat_participants WHERE chat_id = $1 AND user_id = $2", chat.ID, users[1].ID,
	); err != nil {
		t.Fatalf("drop membership: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, users[1].ID)
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	// The dialog row survives, degraded to a forbidden chat.
	if len(res.Dialogs) != 1 {
		t.Errorf("dialogs = %d, want 1", len(res.Dialogs))
	}
	if len(res.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(res.Chats))
	}
	forbidden, ok := res.Chats[0].(*tg.ChatForbidden)
	if !ok {
		t.Fatalf("chat entry = %#v, want *tg.ChatForbidden", res.Chats[0])
	}
	if forbidden.ID != chat.ID {
		t.Errorf("forbidden id = %d, want %d", forbidden.ID, chat.ID)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(res.Messages))
	}
	svc, ok := res.Messages[0].(*tg.MessageService)
	if !ok {
		t.Fatalf("top message type = %T, want *tg.MessageService", res.Messages[0])
	}
	createAction, ok := svc.Action.(*tg.MessageActionChatCreate)
	if !ok {
		t.Fatalf("action type = %T, want *tg.MessageActionChatCreate", svc.Action)
	}
	// The gate: no Participants call, so no member ids reach a non-member.
	if len(createAction.Users) != 0 {
		t.Errorf("create action users = %d, want 0 for a non-member", len(createAction.Users))
	}
	for _, u := range res.Users {
		if u.GetID() == users[2].ID {
			t.Errorf("user list leaks member %d to a non-member", users[2].ID)
		}
	}
}

// F7: readHistory and setTyping stay 1:1-only. Typing in particular resolves its
// peer id as a user id on delivery, so accepting a chat peer would push
// updateUserTyping to whichever account shares the chat's id.
func TestReadHistoryAndSetTypingRejectChatPeers(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551292061", "+15551292062")

	_, err := api.ReadHistoryForTest(s, users[0].ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 1,
	})
	rpcError(t, err, "PEER_ID_INVALID")

	_, err = api.SetTypingForTest(s, users[0].ID, &tg.MessagesSetTypingRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Action: &tg.SendMessageTypingAction{},
	})
	rpcError(t, err, "PEER_ID_INVALID")
}

// TestSendAndEditMessageRejectUnstorableText pins the API boundary against text
// Postgres cannot store. A NUL byte or an invalid UTF-8 sequence reaches the
// driver intact and fails the INSERT, so without this guard a client bug is a
// 500 and an error log line an unprivileged caller can repeat at will.
func TestSendAndEditMessageRejectUnstorableText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551293001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551293002")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	peer := &tg.InputPeerUser{UserID: b.ID, AccessHash: b.ID}

	for name, text := range map[string]string{
		"nul byte":     "a\x00b",
		"invalid utf8": "\xff",
	} {
		_, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
			Peer: peer, Message: text, RandomID: 1,
		})
		rpcError(t, err, "MESSAGE_EMPTY")
		msgs, err := s.History(ctx, a.ID, store.PeerTypeUser, b.ID, 0, 100)
		if err != nil {
			t.Fatalf("%s: history: %v", name, err)
		}
		if len(msgs) != 0 {
			t.Fatalf("%s: stored %d messages, want 0", name, len(msgs))
		}
	}

	// A stored message must survive an edit carrying the same text.
	enc, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "hello", RandomID: 2,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	assertEncodes(t, enc)
	sent, ok, err := s.MessageByOwnerLocal(ctx, a.ID, 1)
	if err != nil || !ok {
		t.Fatalf("sent message: ok=%v err=%v", ok, err)
	}

	_, err = api.EditMessageForTest(s, a.ID, &tg.MessagesEditMessageRequest{
		Peer: peer, ID: int(sent.LocalID), Message: "\xff",
	})
	rpcError(t, err, "MESSAGE_EMPTY")
	after, ok, err := s.MessageByOwnerLocal(ctx, a.ID, sent.LocalID)
	if err != nil || !ok {
		t.Fatalf("message after edit: ok=%v err=%v", ok, err)
	}
	if after.Text != "hello" {
		t.Fatalf("text = %q, want hello", after.Text)
	}
}

// TestSendAndEditMessageKeepMultiByteText pins that the guard rejects only what
// the database cannot store: valid multi-byte UTF-8 round-trips unchanged.
func TestSendAndEditMessageKeepMultiByteText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551293011")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551293012")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	peer := &tg.InputPeerUser{UserID: b.ID, AccessHash: b.ID}

	const text = "Привет 👋"
	enc, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: 7,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	assertEncodes(t, enc)
	sent, ok, err := s.MessageByOwnerLocal(ctx, a.ID, 1)
	if err != nil || !ok {
		t.Fatalf("sent message: ok=%v err=%v", ok, err)
	}
	if sent.Text != text {
		t.Fatalf("stored text = %q, want %q", sent.Text, text)
	}

	const edited = "Пока 👋"
	if _, err := api.EditMessageForTest(s, a.ID, &tg.MessagesEditMessageRequest{
		Peer: peer, ID: int(sent.LocalID), Message: edited,
	}); err != nil {
		t.Fatalf("edit: %v", err)
	}
	after, ok, err := s.MessageByOwnerLocal(ctx, a.ID, sent.LocalID)
	if err != nil || !ok {
		t.Fatalf("message after edit: ok=%v err=%v", ok, err)
	}
	if after.Text != edited {
		t.Fatalf("edited text = %q, want %q", after.Text, edited)
	}
}

// dialogsFor gives owner n DM dialogs, top messages 1..n, newest last.
func dialogsFor(t *testing.T, s *store.Store, phonePrefix string, n int) store.User {
	t.Helper()
	ctx := context.Background()
	owner, err := s.CreateUser(ctx, phonePrefix+"000")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	for i := range n {
		peer, perr := s.CreateUser(ctx, phonePrefix+strconv.Itoa(101+i))
		if perr != nil {
			t.Fatalf("peer %d: %v", i, perr)
		}
		// The peer sends, so the owner gets an inbox copy and a dialog.
		if _, perr = api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerUser{UserID: owner.ID, AccessHash: owner.ID}, Message: "hi", RandomID: int64(i + 1),
		}); perr != nil {
			t.Fatalf("dm %d: %v", i, perr)
		}
	}
	return owner
}

// topMessage reads a reply dialog's top message.
func topMessage(t *testing.T, d tg.DialogClass) int {
	t.Helper()
	dlg, ok := d.(*tg.Dialog)
	if !ok {
		t.Fatalf("dialog type = %T, want *tg.Dialog", d)
	}
	return dlg.TopMessage
}

// A list longer than the page must come back as a slice carrying the whole-list
// total, so the client knows there is more behind the page it got.
func TestHandleGetDialogsTruncatesToSlice(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	const total = 21 // one over the default limit
	owner := dialogsFor(t, s, "+1555129321", total)

	// Limit 0 clamps to defaultDialogsLimit.
	enc, err := api.GetDialogsPageForTest(s, owner.ID, &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}})
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogsSlice)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogsSlice", enc)
	}
	if len(res.Dialogs) != 20 {
		t.Fatalf("dialogs = %d, want 20", len(res.Dialogs))
	}
	if res.Count != total {
		t.Fatalf("count = %d, want %d", res.Count, total)
	}
	// Newest first, so the page starts at the last dialog written.
	if top := topMessage(t, res.Dialogs[0]); top != total {
		t.Fatalf("first dialog top_message = %d, want %d", top, total)
	}

	// Paging past the first page reaches the end and drops back to the plain reply.
	last := topMessage(t, res.Dialogs[len(res.Dialogs)-1])
	enc, err = api.GetDialogsPageForTest(s, owner.ID, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{}, OffsetID: last, Limit: 20,
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	rest, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("second page type = %T, want *tg.MessagesDialogs", enc)
	}
	if len(rest.Dialogs) != total-20 {
		t.Fatalf("second page = %d dialogs, want %d", len(rest.Dialogs), total-20)
	}
	if got := topMessage(t, rest.Dialogs[0]); got != last-1 {
		t.Fatalf("second page starts at %d, want %d — offset is not strictly older", got, last-1)
	}
}

// An over-large limit clamps rather than serving the whole list unbounded.
func TestHandleGetDialogsClampsLimit(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	owner := dialogsFor(t, s, "+1555129331", 3)

	enc, err := api.GetDialogsPageForTest(s, owner.ID, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{}, Limit: 500,
	})
	if err != nil {
		t.Fatalf("dialogs: %v", err)
	}
	// Three dialogs is short of the clamped 100, so the reply stays plain.
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDialogs", enc)
	}
	if len(res.Dialogs) != 3 {
		t.Fatalf("dialogs = %d, want 3", len(res.Dialogs))
	}

	// A limit the list exactly fills is a full page, so it must advertise the total.
	enc, err = api.GetDialogsPageForTest(s, owner.ID, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{}, Limit: 3,
	})
	if err != nil {
		t.Fatalf("exact page: %v", err)
	}
	slice, ok := enc.(*tg.MessagesDialogsSlice)
	if !ok {
		t.Fatalf("exact page type = %T, want *tg.MessagesDialogsSlice", enc)
	}
	if slice.Count != 3 || len(slice.Dialogs) != 3 {
		t.Fatalf("exact page = %d dialogs, count %d, want 3 and 3", len(slice.Dialogs), slice.Count)
	}
}

// mediaMessage sends a message from one user to another carrying a stored file,
// and returns the sender's row and the file it attached.
func mediaMessage(t *testing.T, s *store.Store, from, to store.User, text string, stored bool) (store.Message, store.File) {
	t.Helper()
	ctx := context.Background()
	f, err := s.AllocateFile(ctx, from.ID, 11, "text/plain", "hello.txt", 1<<31)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	if stored {
		if err := s.MarkFileStored(ctx, f.ID); err != nil {
			t.Fatalf("mark stored: %v", err)
		}
	}
	sender, _, _, _, err := s.SendMessage(ctx, from.ID, to.ID, text, 909, f.ID) //nolint:dogsled // only the sender row is needed here
	if err != nil {
		t.Fatalf("send media message: %v", err)
	}
	return sender, f
}

// mediaUsers creates a pair of accounts for a media test.
func mediaUsers(t *testing.T, s *store.Store, a, b string) (store.User, store.User) {
	t.Helper()
	ctx := context.Background()
	u1, err := s.CreateUser(ctx, a)
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	u2, err := s.CreateUser(ctx, b)
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	return u1, u2
}

// historyMessages runs getHistory for caller against peer and returns the page.
func historyMessages(t *testing.T, s *store.Store, caller, peer store.User) []tg.MessageClass {
	t.Helper()
	enc, err := api.GetHistoryForTest(s, caller.ID, &tg.MessagesGetHistoryRequest{
		Peer: &tg.InputPeerUser{UserID: peer.ID, AccessHash: peer.ID},
	})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	res, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("history type = %T, want *tg.MessagesMessages", enc)
	}
	return res.Messages
}

func TestHandleGetHistoryRendersDocumentMedia(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	u1, u2 := mediaUsers(t, s, "+15551294001", "+15551294002")
	_, f := mediaMessage(t, s, u1, u2, "here", true)

	msgs := historyMessages(t, s, u2, u1)
	if len(msgs) != 1 {
		t.Fatalf("history = %d messages, want 1", len(msgs))
	}
	m, ok := msgs[0].(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T, want *tg.Message", msgs[0])
	}
	if m.Message != "here" {
		t.Fatalf("text = %q, want %q", m.Message, "here")
	}
	doc := mediaDocument(t, m)
	if doc.ID != f.ID || doc.AccessHash != f.AccessHash {
		t.Fatalf("document id/hash = %d/%d, want %d/%d", doc.ID, doc.AccessHash, f.ID, f.AccessHash)
	}
	if doc.MimeType != "text/plain" || doc.Size != 11 {
		t.Fatalf("document mime/size = %q/%d, want text/plain/11", doc.MimeType, doc.Size)
	}
	if len(doc.Attributes) != 1 {
		t.Fatalf("attributes = %d, want 1", len(doc.Attributes))
	}
	name, ok := doc.Attributes[0].(*tg.DocumentAttributeFilename)
	if !ok || name.FileName != "hello.txt" {
		t.Fatalf("attribute = %+v, want DocumentAttributeFilename hello.txt", doc.Attributes[0])
	}
}

func TestHandleGetHistoryUnstoredFileRendersPlainMessage(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	u1, u2 := mediaUsers(t, s, "+15551294003", "+15551294004")
	mediaMessage(t, s, u1, u2, "here", false)

	msgs := historyMessages(t, s, u2, u1)
	if len(msgs) != 1 {
		t.Fatalf("history = %d messages, want 1", len(msgs))
	}
	m, ok := msgs[0].(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T, want *tg.Message", msgs[0])
	}
	if m.Media != nil {
		t.Fatalf("media = %+v, want none for an unstored file", m.Media)
	}
	if m.Message != "here" {
		t.Fatalf("text = %q, want %q", m.Message, "here")
	}
}

// mediaDocument asserts a wire message carries document media and returns it.
func mediaDocument(t *testing.T, m *tg.Message) *tg.Document {
	t.Helper()
	media, ok := m.Media.(*tg.MessageMediaDocument)
	if !ok {
		t.Fatalf("media type = %T, want *tg.MessageMediaDocument", m.Media)
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		t.Fatalf("document type = %T, want *tg.Document", media.Document)
	}
	return doc
}
