package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// --- helpers ---

func createClient(addrPort int, key *rsa.PrivateKey, dcID int, collector *updateCollector, sess telegram.SessionStorage) *telegram.Client {
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addrPort}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		UpdateHandler:  collector,
		SessionStorage: sess,
	})
}

func flowFor(phone string, codes *multiCodeSink) auth.Flow {
	return auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx, phone)
			})),
		auth.SendCodeOptions{},
	)
}

func execChat(t *testing.T, ctx context.Context, cmds chan command, fn func(ctx context.Context, c *tg.Client) error) {
	t.Helper()
	d := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: d}:
	case <-ctx.Done():
		t.Fatalf("command enqueue timeout: %v", ctx.Err())
	}
	if err := <-d; err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// waitService waits for a service message with the given action type and
// returns the envelope it arrived in.
func (u *updateCollector) waitService(ctx context.Context, wantAction tg.MessageActionClass) (serviceMsgEnvelope, error) {
	for {
		select {
		case env := <-u.serviceMsg:
			if actionType(env.svc.Action) == actionType(wantAction) {
				return env, nil
			}
		case <-ctx.Done():
			return serviceMsgEnvelope{}, ctx.Err()
		}
	}
}

// waitNoService asserts that no service message with the given action type
// arrives within the context deadline.
func (u *updateCollector) waitNoService(ctx context.Context, wantAction tg.MessageActionClass) error {
	for {
		select {
		case env := <-u.serviceMsg:
			if actionType(env.svc.Action) == actionType(wantAction) {
				return fmt.Errorf("unexpected service message %s", actionType(env.svc.Action))
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func actionType(v tg.MessageActionClass) string { return fmt.Sprintf("%T", v) }

// hasChat reports whether chats carries a *tg.Chat with the given id.
func hasChat(chats []tg.ChatClass, id int64) bool {
	for _, c := range chats {
		if ch, ok := c.(*tg.Chat); ok && ch.ID == id {
			return true
		}
	}
	return false
}

func checkFullChat(t *testing.T, res *tg.MessagesChatFull, chatID, creatorID, viewerID, memberBID, memberCID int64) error {
	t.Helper()
	full, ok := res.FullChat.(*tg.ChatFull)
	if !ok {
		return fmt.Errorf("full chat type = %T, want *tg.ChatFull", res.FullChat)
	}
	if full.ID != chatID || full.About != "" {
		return fmt.Errorf("full chat id/about = %d/%q, want %d/empty", full.ID, full.About, chatID)
	}
	if _, ok := full.ChatPhoto.(*tg.PhotoEmpty); !ok {
		return fmt.Errorf("chat photo = %T, want *tg.PhotoEmpty", full.ChatPhoto)
	}
	if _, ok := full.GetExportedInvite(); ok {
		return errors.New("full chat unexpectedly includes exported invite")
	}
	participants, ok := full.Participants.(*tg.ChatParticipants)
	if !ok {
		return fmt.Errorf("participants type = %T, want *tg.ChatParticipants", full.Participants)
	}
	if participants.ChatID != chatID || len(participants.Participants) != 3 {
		return fmt.Errorf("participants chat/count = %d/%d, want %d/3", participants.ChatID, len(participants.Participants), chatID)
	}
	roles := make(map[int64]string, len(participants.Participants))
	for _, participant := range participants.Participants {
		switch p := participant.(type) {
		case *tg.ChatParticipantCreator:
			roles[p.UserID] = "creator"
		case *tg.ChatParticipant:
			if p.InviterID != creatorID || p.Date <= 0 {
				return fmt.Errorf("member %d inviter/date = %d/%d, want %d and a join date", p.UserID, p.InviterID, p.Date, creatorID)
			}
			roles[p.UserID] = "member"
		default:
			return fmt.Errorf("participant type = %T, want creator or member", participant)
		}
	}
	wantIDs := map[int64]bool{creatorID: true}
	for _, id := range []int64{memberBID, memberCID} {
		wantIDs[id] = true
	}
	if len(roles) != len(wantIDs) || roles[creatorID] != "creator" {
		return fmt.Errorf("participant roles = %v, want creator %d and members %v", roles, creatorID, wantIDs)
	}
	for id := range wantIDs {
		wantRole := "member"
		if id == creatorID {
			wantRole = "creator"
		}
		if roles[id] != wantRole {
			return fmt.Errorf("participant %d role = %q, want %q; roles=%v", id, roles[id], wantRole, roles)
		}
	}
	if len(res.Chats) != 1 {
		return fmt.Errorf("chats = %d, want 1", len(res.Chats))
	}
	chat, ok := res.Chats[0].(*tg.Chat)
	if !ok || chat.ID != chatID {
		return fmt.Errorf("chat entry = %T/%v, want basic chat %d", res.Chats[0], res.Chats[0], chatID)
	}
	if chat.Version != participants.Version {
		return fmt.Errorf("chat/version = %d/%d, want the same snapshot version", chat.Version, participants.Version)
	}
	if len(res.Users) != 3 {
		return fmt.Errorf("users = %d, want 3", len(res.Users))
	}
	users := make(map[int64]*tg.User, len(res.Users))
	for _, user := range res.Users {
		profile, ok := user.(*tg.User)
		if !ok {
			return fmt.Errorf("user entry = %T, want entitled *tg.User", user)
		}
		if profile.ID == viewerID {
			if !profile.Self || profile.Phone == "" {
				return fmt.Errorf("self profile self/phone = %t/%q, want true and a phone", profile.Self, profile.Phone)
			}
		} else if profile.Self || profile.Phone != "" {
			return fmt.Errorf("non-self profile %d self/phone = %t/%q, want false/empty", profile.ID, profile.Self, profile.Phone)
		}
		users[profile.ID] = profile
	}
	for id := range roles {
		if users[id] == nil {
			return fmt.Errorf("participant %d missing from users", id)
		}
	}
	return nil
}

// waitNoNewMsg asserts that no regular message arrives within the context
// deadline.
func (u *updateCollector) waitNoNewMsg(ctx context.Context) error {
	for {
		select {
		case <-u.newMsg:
			return errors.New("unexpected new message")
		case <-ctx.Done():
			return nil
		}
	}
}

// --- tests ---

// TestChatsRealtime exercises the full chat lifecycle against a real gotd
// client: create, send, edit title, add member, remove member, and prove the
// removed member no longer receives messages.
func TestChatsRealtime(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB, phoneC, phoneD = "+15551290001", "+15551290002", "+15551290003", "+15551290004"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB, phoneC, phoneD)

	collA, collB, collC, collD := newUpdateCollector(), newUpdateCollector(), newUpdateCollector(), newUpdateCollector()
	clientA, clientB, clientC, clientD :=
		createClient(addr.Port, key, dcID, collA, nil),
		createClient(addr.Port, key, dcID, collB, nil),
		createClient(addr.Port, key, dcID, collC, nil),
		createClient(addr.Port, key, dcID, collD, nil)

	aCmds, bCmds, cCmds, dCmds := make(chan command), make(chan command), make(chan command), make(chan command)
	aID, bID, cID, dID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC, errD := make(chan error, 1), make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA, codes), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB, codes), bID, bCmds) }()
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC, codes), cID, cCmds) }()
	go func() { errD <- runInteractive(ctx, clientD, flowFor(phoneD, codes), dID, dCmds) }()

	logins := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID, bUserID, cUserID, dUserID := logins(aID, "A"), logins(bID, "B"), logins(cID, "C"), logins(dID, "D")

	// 1. A creates chat with B and C, title "Team".
	var chatID int64
	var chatVersion int
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		inv, err := c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Team",
			Users: []tg.InputUserClass{
				inputUser(aUserID, bUserID),
				inputUser(aUserID, cUserID),
			},
		})
		if err != nil {
			return err
		}
		ups, ok := inv.Updates.(*tg.Updates)
		if !ok {
			return errors.New("createChat: unexpected updates type")
		}
		if len(ups.Chats) != 1 {
			return errors.New("createChat: no chat in response")
		}
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return errors.New("createChat: chat is not *tg.Chat")
		}
		if chat.ParticipantsCount != 3 {
			return errors.New("createChat: participants_count != 3")
		}
		chatID = chat.ID
		chatVersion = chat.Version
		return nil
	})

	// A saves a poll restriction. The response is the versioned rights update,
	// and a repeated save is reported as unchanged instead of advancing the chat.
	var savedRightsVersion int
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		result, err := c.MessagesEditChatDefaultBannedRights(ctx, &tg.MessagesEditChatDefaultBannedRightsRequest{
			Peer:         &tg.InputPeerChat{ChatID: chatID},
			BannedRights: tg.ChatBannedRights{SendPolls: true},
		})
		if err != nil {
			return fmt.Errorf("edit default rights: %w", err)
		}
		updates, ok := result.(*tg.Updates)
		if !ok || len(updates.Updates) != 1 {
			return fmt.Errorf("edit default rights result = %T, want Updates with one entry", result)
		}
		updated, ok := updates.Updates[0].(*tg.UpdateChatDefaultBannedRights)
		if !ok {
			return fmt.Errorf("default rights update = %T, want *tg.UpdateChatDefaultBannedRights", updates.Updates[0])
		}
		peer, ok := updated.Peer.(*tg.PeerChat)
		if !ok || peer.ChatID != chatID {
			return fmt.Errorf("default rights peer = %T/%v, want chat %d", updated.Peer, updated.Peer, chatID)
		}
		if !updated.DefaultBannedRights.SendPolls || updated.DefaultBannedRights.SendMessages || updated.Version != chatVersion+1 {
			return fmt.Errorf("default rights update = %+v, want SendPolls only at version %d", updated, chatVersion+1)
		}
		savedRightsVersion = updated.Version
		return nil
	})
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditChatDefaultBannedRights(ctx, &tg.MessagesEditChatDefaultBannedRightsRequest{
			Peer:         &tg.InputPeerChat{ChatID: chatID},
			BannedRights: tg.ChatBannedRights{SendPolls: true},
		})
		var rpc *tgerr.Error
		if !errors.As(err, &rpc) || rpc.Message != "CHAT_NOT_MODIFIED" {
			return fmt.Errorf("unchanged default rights error = %w, want CHAT_NOT_MODIFIED", err)
		}
		return nil
	})

	// Every current member sees the same three participants, with profile
	// fields gated for the current viewer. Each read reopens the saved rights.
	for _, member := range []struct {
		cmds   chan command
		userID int64
		who    string
	}{{aCmds, aUserID, "A"}, {bCmds, bUserID, "B"}, {cCmds, cUserID, "C"}} {
		execChat(t, ctx, member.cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesGetFullChat(ctx, chatID)
			if err != nil {
				return fmt.Errorf("%s getFullChat: %w", member.who, err)
			}
			if err := checkFullChat(t, res, chatID, aUserID, member.userID, bUserID, cUserID); err != nil {
				return err
			}
			chat, ok := res.Chats[0].(*tg.Chat)
			if !ok {
				return fmt.Errorf("%s getFullChat chat = %T, want *tg.Chat", member.who, res.Chats[0])
			}
			rights, ok := chat.GetDefaultBannedRights()
			if !ok || !rights.SendPolls || rights.SendMessages || chat.Version != savedRightsVersion {
				return fmt.Errorf("%s getFullChat rights/version = %+v/%d, want SendPolls only/version %d", member.who, rights, chat.Version, savedRightsVersion)
			}
			return nil
		})
	}

	// Create a distinct inaccessible basic chat and a channel so getChats can
	// prove it reads only the basic-chat namespace. The other group has no
	// dialog rows; membership is the only authority this read needs.
	otherChat, err := st.CreateChat(ctx, cUserID, "Other", []int64{bUserID})
	if err != nil {
		t.Fatalf("create inaccessible chat: %v", err)
	}
	if otherChat.ID != bUserID {
		t.Fatalf("namespace collision fixture chat/user ids = %d/%d, want a real collision", otherChat.ID, bUserID)
	}
	channel, err := st.CreateChannel(ctx, dUserID, "Channel", "", true)
	if err != nil {
		t.Fatalf("create channel for namespace check: %v", err)
	}
	const absentChatID = int64(999999)

	// getChats deduplicates ids, drops non-positive ids before lookup, and emits
	// empty ChatForbidden entries for existing non-members and absent chats.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		got, err := c.MessagesGetChats(ctx, []int64{chatID, chatID, otherChat.ID, absentChatID, dUserID, channel.ID, 0, -1})
		if err != nil {
			return fmt.Errorf("getChats: %w", err)
		}
		res, ok := got.(*tg.MessagesChats)
		if !ok {
			return fmt.Errorf("getChats result = %T, want *tg.MessagesChats", got)
		}
		if len(res.Chats) != 5 {
			return fmt.Errorf("getChats entries = %d, want 5 positive unique ids", len(res.Chats))
		}
		memberChat, ok := res.Chats[0].(*tg.Chat)
		if !ok {
			return fmt.Errorf("member chat result = %T, want *tg.Chat", res.Chats[0])
		}
		if memberChat.ParticipantsCount != 3 {
			return fmt.Errorf("member chat participant count = %d, want 3 to match getFullChat", memberChat.ParticipantsCount)
		}
		defaultRights, ok := memberChat.GetDefaultBannedRights()
		if !ok || !defaultRights.SendPolls || memberChat.Version != savedRightsVersion {
			return fmt.Errorf("member getChats default rights/version = %+v/%d, want SendPolls/version %d", defaultRights, memberChat.Version, savedRightsVersion)
		}
		for i, id := range []int64{otherChat.ID, absentChatID, dUserID, channel.ID} {
			forbidden, ok := res.Chats[i+1].(*tg.ChatForbidden)
			if !ok || forbidden.ID != id || forbidden.Title != "" {
				return fmt.Errorf("getChats[%d] = %T/%v, want empty ChatForbidden %d", i+1, res.Chats[i+1], res.Chats[i+1], id)
			}
		}
		return nil
	})

	// A non-member cannot distinguish an existing chat from a missing one by
	// either RPC, while getChats uses the same empty forbidden constructor.
	var deniedFull, missingFull error
	execChat(t, ctx, dCmds, func(ctx context.Context, c *tg.Client) error {
		_, deniedFull = c.MessagesGetFullChat(ctx, chatID)
		_, missingFull = c.MessagesGetFullChat(ctx, absentChatID)
		return nil
	})
	var deniedRPC, missingRPC *tgerr.Error
	if !errors.As(deniedFull, &deniedRPC) || !errors.As(missingFull, &missingRPC) || deniedRPC.Code != 400 || deniedRPC.Message != "PEER_ID_INVALID" || missingRPC.Code != deniedRPC.Code || missingRPC.Message != deniedRPC.Message {
		t.Fatalf("getFullChat denial/missing = %v/%v, want identical PEER_ID_INVALID", deniedFull, missingFull)
	}
	execChat(t, ctx, dCmds, func(ctx context.Context, c *tg.Client) error {
		got, err := c.MessagesGetChats(ctx, []int64{chatID, otherChat.ID, absentChatID, 0, -1})
		if err != nil {
			return fmt.Errorf("non-member getChats: %w", err)
		}
		res, ok := got.(*tg.MessagesChats)
		if !ok || len(res.Chats) != 3 {
			return fmt.Errorf("non-member getChats result = %T/%v, want three positive ids", got, got)
		}
		for i, id := range []int64{chatID, otherChat.ID, absentChatID} {
			forbidden, ok := res.Chats[i].(*tg.ChatForbidden)
			if !ok || forbidden.ID != id || forbidden.Title != "" {
				return fmt.Errorf("non-member getChats[%d] = %T/%v, want empty ChatForbidden %d", i, res.Chats[i], res.Chats[i], id)
			}
		}
		return nil
	})
	var overCapReachedStore bool
	store.SetChatListInfoSnapshotHook(st, func([]int64) { overCapReachedStore = true })
	assertChannelRPCError(t, ctx, aCmds, "LIMIT_INVALID", func(ctx context.Context, c *tg.Client) error {
		tooMany := make([]int64, 101)
		for i := range tooMany {
			tooMany[i] = chatID
		}
		_, err := c.MessagesGetChats(ctx, tooMany)
		return err
	})
	store.SetChatListInfoSnapshotHook(st, nil)
	if overCapReachedStore {
		t.Fatal("oversized getChats request reached the store")
	}
	for _, id := range []int64{0, -1} {
		assertChannelRPCError(t, ctx, aCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesGetFullChat(ctx, id)
			return err
		})
	}

	// 2. B and C receive create service message.
	waitSvc := func(coll *updateCollector, who string) {
		t.Helper()
		env, err := coll.waitService(ctx, &tg.MessageActionChatCreate{})
		if err != nil {
			t.Fatalf("%s wait create service: %v", who, err)
		}
		cr, ok := env.svc.Action.(*tg.MessageActionChatCreate)
		if !ok {
			t.Fatalf("%s action = %T", who, env.svc.Action)
		}
		if cr.Title != "Team" {
			t.Fatalf("%s create title = %q", who, cr.Title)
		}
		// The enclosing envelope carries the chat the service message is about.
		if !hasChat(env.chats, chatID) {
			t.Fatalf("%s create envelope chats = %v, want chat %d", who, env.chats, chatID)
		}
	}
	waitSvc(collB, "B")
	waitSvc(collC, "C")

	// 3. B sends message to chat; A and C receive it.
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "hello from B",
			RandomID: 100001,
		})
		return err
	})
	recvMsg := func(coll *updateCollector, who string, wantText string, wantFrom int64) *tg.Message {
		t.Helper()
		select {
		case m := <-coll.newMsg:
			if m.Message != wantText {
				t.Fatalf("%s msg = %q, want %q", who, m.Message, wantText)
			}
			peer, ok := m.PeerID.(*tg.PeerChat)
			if !ok {
				t.Fatalf("%s peer = %T", who, m.PeerID)
			}
			if peer.ChatID != chatID {
				t.Fatalf("%s peer chatID = %d", who, peer.ChatID)
			}
			from, ok := m.FromID.(*tg.PeerUser)
			if !ok {
				t.Fatalf("%s from = %T", who, m.FromID)
			}
			if from.UserID != wantFrom {
				t.Fatalf("%s fromID = %d, want %d", who, from.UserID, wantFrom)
			}
			return m
		case <-ctx.Done():
			t.Fatalf("%s timed out waiting for message: %v", who, ctx.Err())
			return nil
		}
	}
	recvMsg(collA, "A", "hello from B", bUserID)
	recvMsg(collC, "C", "hello from B", bUserID)
	// Drain B's own copy of the sent message.
	recvMsg(collB, "B", "hello from B", bUserID)

	// Add two more group messages so every member has the same three content
	// rows to page around. Their owner-local ids are already above 1 because the
	// chat-create service row occupies the earlier position.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: "middle from A", RandomID: 100003,
		})
		return err
	})
	recvMsg(collA, "A", "middle from A", aUserID)
	recvMsg(collB, "B", "middle from A", aUserID)
	recvMsg(collC, "C", "middle from A", aUserID)
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: "newest from C", RandomID: 100004,
		})
		return err
	})
	recvMsg(collA, "A", "newest from C", cUserID)
	recvMsg(collB, "B", "newest from C", cUserID)
	recvMsg(collC, "C", "newest from C", cUserID)

	getHistory := func(cmds chan command, peer tg.InputPeerClass, offsetID, addOffset, limit int, who string) *tg.MessagesMessages {
		t.Helper()
		var history *tg.MessagesMessages
		execChat(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer: peer, OffsetID: offsetID, AddOffset: addOffset, Limit: limit,
			})
			if err != nil {
				return fmt.Errorf("%s getHistory: %w", who, err)
			}
			var ok bool
			history, ok = res.(*tg.MessagesMessages)
			if !ok {
				return fmt.Errorf("%s getHistory result = %T, want *tg.MessagesMessages", who, res)
			}
			return nil
		})
		return history
	}
	messageTexts := func(history *tg.MessagesMessages) []string {
		var texts []string
		for _, msg := range history.Messages {
			if m, ok := msg.(*tg.Message); ok && m.Message != "" {
				texts = append(texts, m.Message)
			}
		}
		return texts
	}
	assertTexts := func(who string, got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s history texts = %v, want %v", who, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s history texts = %v, want %v", who, got, want)
			}
		}
	}
	assertLocalIDsAboveOne := func(who string, history *tg.MessagesMessages) {
		t.Helper()
		for _, msg := range history.Messages {
			if m, ok := msg.(*tg.Message); ok && m.Message != "" && m.ID <= 1 {
				t.Errorf("%s history message %q has local id %d, want greater than 1", who, m.Message, m.ID)
			}
		}
	}
	wantGroupTexts := []string{"newest from C", "middle from A", "hello from B"}
	for _, member := range []struct {
		cmds chan command
		who  string
	}{{aCmds, "A"}, {bCmds, "B"}, {cCmds, "C"}} {
		history := getHistory(member.cmds, &tg.InputPeerChat{ChatID: chatID}, 1, -25, 50, member.who)
		assertTexts(member.who+" around-unread chat", messageTexts(history), wantGroupTexts)
		assertLocalIDsAboveOne(member.who+" around-unread chat", history)
	}

	// Seed a separate user-peer history after each owner's group service rows so
	// all three copies have owner-local ids above the around-unread boundary.
	peerAB, peerBA := peerUser(aUserID, bUserID), peerUser(bUserID, aUserID)
	for _, message := range []struct {
		cmds     chan command
		peer     tg.InputPeerClass
		text     string
		randomID int64
		to       *updateCollector
	}{
		{aCmds, peerAB, "user oldest", 100101, collB},
		{bCmds, peerBA, "user middle", 100102, collA},
		{aCmds, peerAB, "user newest", 100103, collB},
	} {
		execChat(t, ctx, message.cmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: message.peer, Message: message.text, RandomID: message.randomID,
			})
			return err
		})
		got := recvOrCtx(t, ctx, message.to.newMsg, message.text+" user-peer update")
		if got.Message != message.text {
			t.Fatalf("user-peer update = %q, want %q", got.Message, message.text)
		}
	}
	wantUserTexts := []string{"user newest", "user middle", "user oldest"}
	for _, userPeer := range []struct {
		cmds chan command
		peer tg.InputPeerClass
		who  string
	}{{aCmds, peerAB, "A"}, {bCmds, peerBA, "B"}} {
		around := getHistory(userPeer.cmds, userPeer.peer, 1, -25, 50, userPeer.who)
		assertTexts(userPeer.who+" around-unread user peer", messageTexts(around), wantUserTexts)
		assertLocalIDsAboveOne(userPeer.who+" around-unread user peer", around)

		newest := getHistory(userPeer.cmds, userPeer.peer, 0, 0, 50, userPeer.who+" newest")
		assertTexts(userPeer.who+" offset zero", messageTexts(newest), wantUserTexts)
		latestID := 0
		for _, msg := range newest.Messages {
			if m, ok := msg.(*tg.Message); ok && m.Message == "user newest" {
				latestID = m.ID
			}
		}
		if latestID <= 1 {
			t.Fatalf("%s newest local id = %d, want greater than 1", userPeer.who, latestID)
		}
		older := getHistory(userPeer.cmds, userPeer.peer, latestID, 0, 50, userPeer.who+" older")
		assertTexts(userPeer.who+" older than newest", messageTexts(older), []string{"user middle", "user oldest"})
		positive := getHistory(userPeer.cmds, userPeer.peer, 0, 1, 50, userPeer.who+" positive")
		assertTexts(userPeer.who+" positive add_offset", messageTexts(positive), []string{"user middle", "user oldest"})
		limited := getHistory(userPeer.cmds, userPeer.peer, 0, 0, 1, userPeer.who+" limited")
		assertTexts(userPeer.who+" requested limit", messageTexts(limited), []string{"user newest"})
	}

	// A self-delete hides only A's retained copy; B still reads the same fan-out
	// message from B's own row.
	var aMiddleID int
	for _, msg := range getHistory(aCmds, &tg.InputPeerChat{ChatID: chatID}, 1, -25, 50, "A before self-delete").Messages {
		if m, ok := msg.(*tg.Message); ok && m.Message == "middle from A" {
			aMiddleID = m.ID
		}
	}
	if aMiddleID <= 1 {
		t.Fatalf("A middle message local id = %d, want greater than 1", aMiddleID)
	}
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{ID: []int{aMiddleID}})
		return err
	})
	assertTexts("A self-deleted chat copy", messageTexts(getHistory(aCmds, &tg.InputPeerChat{ChatID: chatID}, 1, -25, 50, "A deleted")), []string{"newest from C", "hello from B"})
	assertTexts("B retained chat copy", messageTexts(getHistory(bCmds, &tg.InputPeerChat{ChatID: chatID}, 1, -25, 50, "B retained")), wantGroupTexts)

	// 4. A edits title to "Team 2"; B and C receive editTitle.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditChatTitle(ctx, &tg.MessagesEditChatTitleRequest{
			ChatID: chatID, Title: "Team 2",
		})
		return err
	})
	waitEditTitle := func(coll *updateCollector, who string) {
		t.Helper()
		env, err := coll.waitService(ctx, &tg.MessageActionChatEditTitle{})
		if err != nil {
			t.Fatalf("%s wait edit title service: %v", who, err)
		}
		et, ok := env.svc.Action.(*tg.MessageActionChatEditTitle)
		if !ok {
			t.Fatalf("%s action = %T", who, env.svc.Action)
		}
		if et.Title != "Team 2" {
			t.Fatalf("%s edit title = %q", who, et.Title)
		}
	}
	waitEditTitle(collB, "B")
	waitEditTitle(collC, "C")

	// 5. B adds D; D receives add service message, retaining B as inviter.
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesAddChatUser(ctx, &tg.MessagesAddChatUserRequest{
			ChatID:   chatID,
			UserID:   inputUser(bUserID, dUserID),
			FwdLimit: 0,
		})
		return err
	})
	waitAddUser := func(coll *updateCollector, who string) {
		t.Helper()
		env, err := coll.waitService(ctx, &tg.MessageActionChatAddUser{})
		if err != nil {
			t.Fatalf("%s wait add user service: %v", who, err)
		}
		au, ok := env.svc.Action.(*tg.MessageActionChatAddUser)
		if !ok {
			t.Fatalf("%s action = %T", who, env.svc.Action)
		}
		if len(au.Users) != 1 || au.Users[0] != dUserID {
			t.Fatalf("%s addUser users = %v", who, au.Users)
		}
	}
	waitAddUser(collD, "D")

	// D sees chat in getDialogs.
	execChat(t, ctx, dCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetDate: 0, OffsetID: 0, OffsetPeer: &tg.InputPeerEmpty{}, Limit: 100,
		})
		if err != nil {
			return err
		}
		diags, ok := res.(*tg.MessagesDialogs)
		if !ok {
			return errors.New("getDialogs: unexpected type")
		}
		found := false
		for _, d := range diags.Dialogs {
			if dial, ok := d.(*tg.Dialog); ok {
				if pc, ok := dial.Peer.(*tg.PeerChat); ok && pc.ChatID == chatID {
					found = true
					break
				}
			}
		}
		if !found {
			return errors.New("D getDialogs: chat not found")
		}
		return nil
	})

	// 6. A removes C; C receives delete service message and loses full-info access.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteChatUser(ctx, &tg.MessagesDeleteChatUserRequest{
			ChatID: chatID, UserID: inputUser(aUserID, cUserID),
		})
		return err
	})
	waitDeleteUser := func(coll *updateCollector, who string) {
		t.Helper()
		env, err := coll.waitService(ctx, &tg.MessageActionChatDeleteUser{})
		if err != nil {
			t.Fatalf("%s wait delete user service: %v", who, err)
		}
		dl, ok := env.svc.Action.(*tg.MessageActionChatDeleteUser)
		if !ok {
			t.Fatalf("%s action = %T", who, env.svc.Action)
		}
		if dl.UserID != cUserID {
			t.Fatalf("%s deleteUser userID = %d", who, dl.UserID)
		}
	}
	waitDeleteUser(collC, "C")
	assertChannelRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetFullChat(ctx, chatID)
		return err
	})

	// 7. A sends one more message; B and D receive it, C does not.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "after C left",
			RandomID: 100002,
		})
		return err
	})
	recvMsg(collB, "B", "after C left", aUserID)
	recvMsg(collD, "D", "after C left", aUserID)
	recvMsg(collA, "A", "after C left", aUserID)
	// The window is the assertion, so it hangs off Background: derived from ctx
	// an already-exhausted parent would return immediately and pass vacuously.
	noCtx, noCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := collC.waitNoNewMsg(noCtx); err != nil {
		t.Errorf("C should not receive message after removal: %v", err)
	}
	noCancel()
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: "later from B", RandomID: 100005,
		})
		return err
	})
	recvMsg(collA, "A", "later from B", bUserID)
	recvMsg(collB, "B", "later from B", bUserID)
	recvMsg(collD, "D", "later from B", bUserID)

	// Pause full-info hydration after its membership read, then remove the
	// viewer and add C. The response must stay on its original repeatable-read
	// snapshot and omit C from both participant and user vectors.
	var snapshotMutationErr error
	var snapshotMutationRan bool
	store.SetChatInfoSnapshotHook(st, func() {
		store.SetChatInfoSnapshotHook(st, nil)
		_, _, _, snapshotMutationErr = st.RemoveChatUser(ctx, chatID, bUserID, bUserID)
		if snapshotMutationErr != nil {
			return
		}
		_, _, _, snapshotMutationErr = st.AddChatUser(ctx, chatID, cUserID, aUserID)
		snapshotMutationRan = true
	})
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetFullChat(ctx, chatID)
		if err != nil {
			return fmt.Errorf("concurrent getFullChat: %w", err)
		}
		full, ok := res.FullChat.(*tg.ChatFull)
		if !ok {
			return fmt.Errorf("concurrent full chat = %T, want *tg.ChatFull", res.FullChat)
		}
		participants, ok := full.Participants.(*tg.ChatParticipants)
		if !ok {
			return fmt.Errorf("concurrent participants = %T, want *tg.ChatParticipants", full.Participants)
		}
		if len(res.Chats) != 1 {
			return fmt.Errorf("concurrent chats = %d, want 1", len(res.Chats))
		}
		chat, ok := res.Chats[0].(*tg.Chat)
		if !ok || chat.ID != chatID || chat.Version != participants.Version || chat.ParticipantsCount != len(participants.Participants) {
			return fmt.Errorf("concurrent chat metadata = %T/%v with participant version %d", res.Chats[0], res.Chats[0], participants.Version)
		}
		participantIDs := map[int64]bool{}
		for _, participant := range participants.Participants {
			switch p := participant.(type) {
			case *tg.ChatParticipantCreator:
				participantIDs[p.UserID] = true
			case *tg.ChatParticipant:
				participantIDs[p.UserID] = true
			}
		}
		if !snapshotMutationRan || !participantIDs[bUserID] || participantIDs[cUserID] {
			return fmt.Errorf("concurrent participant ids = %v, mutation=%t; want pre-change B and no new C", participantIDs, snapshotMutationRan)
		}
		for _, user := range res.Users {
			switch u := user.(type) {
			case *tg.User:
				if u.ID == cUserID {
					return errors.New("new member C leaked into concurrent users snapshot")
				}
			case *tg.UserEmpty:
				if u.ID == cUserID {
					return errors.New("new member C leaked as UserEmpty into concurrent users snapshot")
				}
			}
		}
		return nil
	})
	if snapshotMutationErr != nil {
		t.Fatalf("concurrent membership mutation: %v", snapshotMutationErr)
	}

	// B invited D before leaving. D retains the raw inviter id, but B's profile
	// is now outside D's entitlement set.
	execChat(t, ctx, dCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetFullChat(ctx, chatID)
		if err != nil {
			return fmt.Errorf("D getFullChat after B left: %w", err)
		}
		full, ok := res.FullChat.(*tg.ChatFull)
		if !ok {
			return fmt.Errorf("D full chat = %T, want *tg.ChatFull", res.FullChat)
		}
		participants, ok := full.Participants.(*tg.ChatParticipants)
		if !ok {
			return fmt.Errorf("D participants = %T, want *tg.ChatParticipants", full.Participants)
		}
		inviterFound := false
		for _, participant := range participants.Participants {
			if member, ok := participant.(*tg.ChatParticipant); ok && member.UserID == dUserID {
				inviterFound = member.InviterID == bUserID && member.Date > 0
			}
		}
		if !inviterFound {
			return errors.New("D participant missing B inviter id and join date")
		}
		bEmpty := false
		for _, user := range res.Users {
			if userEmpty, ok := user.(*tg.UserEmpty); ok && userEmpty.ID == bUserID {
				bEmpty = true
			}
			if profile, ok := user.(*tg.User); ok && profile.ID != dUserID && profile.Phone != "" {
				return fmt.Errorf("non-self profile %d exposes phone %q", profile.ID, profile.Phone)
			}
		}
		if !bEmpty {
			return errors.New("departed inviter B is not represented by UserEmpty")
		}
		return nil
	})
	// D received B's message before B left. The retained message stays readable,
	// while B's profile is now UserEmpty because D has no other live edge to B.
	execChat(t, ctx, dCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Limit: 100,
		})
		if err != nil {
			return fmt.Errorf("D getHistory after B left: %w", err)
		}
		history, ok := res.(*tg.MessagesMessages)
		if !ok {
			return fmt.Errorf("D history = %T, want *tg.MessagesMessages", res)
		}
		var sawDepartedMessage, sawEmptyProfile bool
		for _, message := range history.Messages {
			if msg, ok := message.(*tg.Message); ok && msg.Message == "later from B" {
				if from, ok := msg.FromID.(*tg.PeerUser); ok && from.UserID == bUserID {
					sawDepartedMessage = true
				}
			}
		}
		for _, user := range history.Users {
			switch u := user.(type) {
			case *tg.UserEmpty:
				if u.ID == bUserID {
					sawEmptyProfile = true
				}
			case *tg.User:
				if u.ID == bUserID {
					return errors.New("departed author B profile is exposed in history")
				}
			}
		}
		if !sawDepartedMessage || !sawEmptyProfile {
			return fmt.Errorf("departed history message=%t UserEmpty=%t, want both", sawDepartedMessage, sawEmptyProfile)
		}
		return nil
	})
	assertChannelRPCError(t, ctx, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetFullChat(ctx, chatID)
		return err
	})

	close(aCmds)
	close(bCmds)
	close(cCmds)
	close(dCmds)
	for _, ch := range []chan error{errA, errB, errC, errD} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestChatsRemovedMemberIsInert proves the three controls on removed-member
// state: F1 edit/delete rejected, F6 chatForbidden in dialogs, F3 send/history
// rejected.
func TestChatsRemovedMemberIsInert(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneC = "+15551291001", "+15551291002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneC)

	collA, collC := newUpdateCollector(), newUpdateCollector()
	clientA, clientC :=
		createClient(addr.Port, key, dcID, collA, nil),
		createClient(addr.Port, key, dcID, collC, nil)

	aCmds, cCmds := make(chan command), make(chan command)
	aID, cID := make(chan int64, 1), make(chan int64, 1)
	errA, errC := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA, codes), aID, aCmds) }()
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC, codes), cID, cCmds) }()

	logins := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID, cUserID := logins(aID, "A"), logins(cID, "C")

	// A and C create a chat (A invites C).
	var chatID int64
	var cMsgID int
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		inv, err := c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Two",
			Users: []tg.InputUserClass{
				inputUser(aUserID, cUserID),
			},
		})
		if err != nil {
			return err
		}
		ups, ok := inv.Updates.(*tg.Updates)
		if !ok {
			return errors.New("unexpected updates type")
		}
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return errors.New("no chat")
		}
		chatID = chat.ID
		return nil
	})

	// C sends a message to the chat.
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "C message",
			RandomID: 200001,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("unexpected send result")
		}
		for _, u := range ups.Updates {
			if nm, ok := u.(*tg.UpdateNewMessage); ok {
				if m, ok := nm.Message.(*tg.Message); ok {
					cMsgID = m.ID
				}
			}
		}
		return nil
	})

	// A removes C.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteChatUser(ctx, &tg.MessagesDeleteChatUserRequest{
			ChatID: chatID, UserID: inputUser(aUserID, cUserID),
		})
		return err
	})
	// Wait for C to receive the removal.
	if _, err = collC.waitService(ctx, &tg.MessageActionChatDeleteUser{}); err != nil {
		t.Fatalf("C wait delete: %v", err)
	}
	retained, err := st.History(ctx, cUserID, store.PeerTypeChat, chatID, 0, 10)
	if err != nil {
		t.Fatalf("C retained history rows: %v", err)
	}
	retainedMessage := false
	for _, message := range retained {
		if message.Text == "C message" && !message.Deleted {
			retainedMessage = true
		}
	}
	if !retainedMessage {
		t.Fatalf("C retained rows = %+v, want the undeleted owner copy", retained)
	}
	assertChannelRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Limit: 10,
		})
		return err
	})

	// F1: C tries editMessage → MESSAGE_ID_INVALID; A does not receive edit.
	var editErr error
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, ID: cMsgID, Message: "C edited",
		})
		editErr = err
		return nil
	})
	if editErr == nil {
		t.Fatal("C editMessage should fail")
	}
	var tgErr *tgerr.Error
	if errors.As(editErr, &tgErr) {
		if tgErr.Message != "MESSAGE_ID_INVALID" {
			t.Fatalf("edit error = %s, want MESSAGE_ID_INVALID", tgErr.Message)
		}
	} else {
		t.Fatalf("edit error type = %T, want *tgerr.Error", editErr)
	}
	// A should not receive edit update. The window is the assertion, so it hangs
	// off Background: derived from ctx an already-exhausted parent would return
	// immediately and pass vacuously.
	noCtx, noCancel := context.WithTimeout(context.Background(), 2*time.Second)
	select {
	case <-collA.editMsg:
		t.Error("A should not receive edit from removed member")
	case <-noCtx.Done():
	}
	noCancel()

	// F1: C's revoke delete still fails — a removed member may not take the
	// message out of the current members' history.
	var delErr error
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{Revoke: true, ID: []int{cMsgID}})
		delErr = err
		return nil
	})
	if delErr == nil {
		t.Fatal("C revoke deleteMessages should fail")
	}
	if errors.As(delErr, &tgErr) {
		if tgErr.Message != "MESSAGE_ID_INVALID" {
			t.Fatalf("delete error = %s, want MESSAGE_ID_INVALID", tgErr.Message)
		}
	} else {
		t.Fatalf("delete error type = %T, want *tgerr.Error", delErr)
	}
	// A should not receive a delete update either.
	noDelCtx, noDelCancel := context.WithTimeout(context.Background(), 2*time.Second)
	select {
	case <-collA.delMsg:
		t.Error("A should not receive delete from removed member")
	case <-noDelCtx.Done():
	}
	noDelCancel()

	// Self-only delete: C may still clear the retained copy from its own view.
	// It succeeds, and A's copy — and A's update stream — are untouched.
	var selfDelErr error
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{ID: []int{cMsgID}})
		selfDelErr = err
		return nil
	})
	if selfDelErr != nil {
		t.Fatalf("C self-only deleteMessages: %v", selfDelErr)
	}
	noSelfDelCtx, noSelfDelCancel := context.WithTimeout(context.Background(), 2*time.Second)
	select {
	case <-collA.delMsg:
		t.Error("A should not receive a delete for C's self-only delete")
	case <-noSelfDelCtx.Done():
	}
	noSelfDelCancel()

	// F6: C's getDialogs lists chat as chatForbidden.
	checkForbidden := func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetDate: 0, OffsetID: 0, OffsetPeer: &tg.InputPeerEmpty{}, Limit: 100,
		})
		if err != nil {
			return err
		}
		diags, ok := res.(*tg.MessagesDialogs)
		if !ok {
			return errors.New("unexpected dialogs type")
		}
		for _, d := range diags.Dialogs {
			if dial, ok := d.(*tg.Dialog); ok {
				if pc, ok := dial.Peer.(*tg.PeerChat); ok && pc.ChatID == chatID {
					for _, ch := range diags.Chats {
						if cf, ok := ch.(*tg.ChatForbidden); ok && cf.ID == chatID {
							if cf.Title != "" {
								return errors.New("chatForbidden carries a title")
							}
							return nil
						}
					}
					return errors.New("chat is not chatForbidden")
				}
			}
		}
		return errors.New("chat not in dialogs")
	}
	execChat(t, ctx, cCmds, checkForbidden)

	// F6: A renames the chat after the removal. C must not track it — no
	// chatEditTitle service message reaches C, and the dialog stays forbidden.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditChatTitle(ctx, &tg.MessagesEditChatTitleRequest{
			ChatID: chatID, Title: "Two renamed",
		})
		return err
	})
	noTitleCtx, noTitleCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := collC.waitNoService(noTitleCtx, &tg.MessageActionChatEditTitle{}); err != nil {
		t.Errorf("C should not receive title change after removal: %v", err)
	}
	noTitleCancel()
	execChat(t, ctx, cCmds, checkForbidden)

	// F3: C's sendMessage → PEER_ID_INVALID.
	var sendErr error
	execChat(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "C after removal",
			RandomID: 200002,
		})
		sendErr = err
		return nil
	})
	if sendErr == nil {
		t.Fatal("C sendMessage should fail")
	}
	if errors.As(sendErr, &tgErr) {
		if tgErr.Message != "PEER_ID_INVALID" {
			t.Fatalf("send error = %s, want PEER_ID_INVALID", tgErr.Message)
		}
	} else {
		t.Fatalf("send error type = %T, want *tgerr.Error", sendErr)
	}

	// F7: the removed member's loadUsers gate. A and C share no 1:1 dialog and
	// no live chat or channel, so C's getDialogs must degrade A to userEmpty —
	// and must not leak A's new handle after A changes it.
	var newHandle string
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.AccountUpdateUsername(ctx, "removed_a_newhandle")
		if err != nil {
			return err
		}
		u, ok := res.(*tg.User)
		if !ok || u.Username == "" {
			return errors.New("updateUsername: no username in reply")
		}
		newHandle = u.Username
		return nil
	})
	if newHandle != "removed_a_newhandle" {
		t.Fatalf("A new handle = %q, want removed_a_newhandle", newHandle)
	}

	checkUserEmpty := func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetDate: 0, OffsetID: 0, OffsetPeer: &tg.InputPeerEmpty{}, Limit: 100,
		})
		if err != nil {
			return err
		}
		diags, ok := res.(*tg.MessagesDialogs)
		if !ok {
			return errors.New("unexpected dialogs type")
		}
		var sawA bool
		for _, uc := range diags.Users {
			switch u := uc.(type) {
			case *tg.User:
				if u.ID == aUserID {
					return fmt.Errorf("removed member A served as live user, username %q", u.Username)
				}
			case *tg.UserEmpty:
				if u.ID == aUserID {
					sawA = true
				} else {
					return fmt.Errorf("userEmpty id = %d, want A's %d", u.ID, aUserID)
				}
			}
		}
		if !sawA {
			return errors.New("A not present in C's dialogs users at all")
		}
		return nil
	}
	execChat(t, ctx, cCmds, checkUserEmpty)

	close(aCmds)
	close(cCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errC; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client C run: %v", err)
	}
}

// TestChatsOfflineBackfill proves the getDifference backfill for chats: three
// messages and a title change sent while B is offline are returned when B
// reconnects.
func TestChatsOfflineBackfill(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15551292001", "+15551292002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

	sessA, sessB := &session.StorageMemory{}, &session.StorageMemory{}

	// B logs in, captures id, then disconnects.
	var bUserID int64
	bClient := createClient(addr.Port, key, dcID, newUpdateCollector(), sessB)
	if err := bClient.Run(ctx, func(ctx context.Context) error {
		if err := bClient.Auth().IfNecessary(ctx, flowFor(phoneB, codes)); err != nil {
			return err
		}
		self, err := bClient.Self(ctx)
		if err != nil {
			return err
		}
		bUserID = self.ID
		return nil
	}); err != nil {
		t.Fatalf("B login: %v", err)
	}

	// A logs in, creates chat with B, sends 3 messages, changes title.
	var chatID int64
	var aUserID int64
	aClient := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		if err := aClient.Auth().IfNecessary(ctx, flowFor(phoneA, codes)); err != nil {
			return err
		}
		self, err := aClient.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		api := aClient.API()

		inv, err := api.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Backfill",
			Users: []tg.InputUserClass{
				inputUser(aUserID, bUserID),
			},
		})
		if err != nil {
			return err
		}
		ups, ok := inv.Updates.(*tg.Updates)
		if !ok {
			return errors.New("unexpected updates type")
		}
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return errors.New("no chat in response")
		}
		chatID = chat.ID

		for i := 1; i <= 3; i++ {
			_, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer:     &tg.InputPeerChat{ChatID: chatID},
				Message:  "msg " + string(rune('0'+i)),
				RandomID: 300000 + int64(i),
			})
			if err != nil {
				return err
			}
		}

		_, err = api.MessagesEditChatTitle(ctx, &tg.MessagesEditChatTitleRequest{
			ChatID: chatID, Title: "Backfilled",
		})
		return err
	}); err != nil {
		t.Fatalf("A login+chat+send: %v", err)
	}

	// B reconnects and calls getDifference from pts 0.
	var diff tg.UpdatesDifferenceClass
	var state *tg.UpdatesState
	bClient2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessB)
	if err := bClient2.Run(ctx, func(ctx context.Context) error {
		d, err := bClient2.API().UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		diff = d
		st, err := bClient2.API().UpdatesGetState(ctx)
		if err != nil {
			return err
		}
		state = st
		return nil
	}); err != nil {
		t.Fatalf("B getDifference: %v", err)
	}

	full, ok := diff.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("difference type = %T, want *tg.UpdatesDifference", diff)
	}

	// Expect exactly: chatCreate, "msg 1".."msg 3", chatEditTitle — in that
	// order. Ids are allocated per owner in pts order, so ascending ids are the
	// client-visible proof the batch is replayed in pts order.
	wantOrder := []string{
		actionType(&tg.MessageActionChatCreate{}),
		"msg 1", "msg 2", "msg 3",
		actionType(&tg.MessageActionChatEditTitle{}),
	}
	if len(full.NewMessages) != len(wantOrder) {
		t.Fatalf("backfill messages = %d, want %d", len(full.NewMessages), len(wantOrder))
	}
	prevID := 0
	for i, m := range full.NewMessages {
		var got string
		var id int
		switch msg := m.(type) {
		case *tg.Message:
			got, id = msg.Message, msg.ID
		case *tg.MessageService:
			got, id = actionType(msg.Action), msg.ID
		default:
			t.Fatalf("backfill message %d type = %T", i, m)
		}
		if got != wantOrder[i] {
			t.Fatalf("backfill message %d = %q, want %q", i, got, wantOrder[i])
		}
		if id <= prevID {
			t.Fatalf("backfill message %d id %d not ascending (previous %d)", i, id, prevID)
		}
		prevID = id
	}

	// The chat itself is in the difference.
	if !hasChat(full.Chats, chatID) {
		t.Fatalf("backfill Chats = %v, want chat %d", full.Chats, chatID)
	}

	// B's pts matches getState.
	if full.State.Pts != state.Pts {
		t.Fatalf("diff pts %d != getState pts %d", full.State.Pts, state.Pts)
	}
}

// TestChatsCrossReplica proves fan-out delivery crosses replicas: two servers
// share one database, A connects to server 1 and B to server 2, both in one
// chat. A sends; B receives over LISTEN/NOTIFY.
func TestChatsCrossReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln1 := mustListen(t, ctx, "127.0.0.1:0")
	ln2 := mustListen(t, ctx, "127.0.0.1:0")
	port1 := tcpPort(t, ln1)
	port2 := tcpPort(t, ln2)
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln1))
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln2))

	const phoneA, phoneB = "+15551293001", "+15551293002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

	// B connects to server 2 and collects pushes.
	collB := newUpdateCollector()
	bCmds := make(chan command)
	bID := make(chan int64, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, createClient(port2, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	var bUserID int64
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("client B login timeout: %v", ctx.Err())
	}

	// A connects to server 1, logs in, creates chat with B.
	var chatID int64
	var aUserID int64
	aClient := createClient(port1, key, dcID, newUpdateCollector(), nil)
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		if err := aClient.Auth().IfNecessary(ctx, flowFor(phoneA, codes)); err != nil {
			return err
		}
		self, err := aClient.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		api := aClient.API()

		inv, err := api.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Cross",
			Users: []tg.InputUserClass{
				inputUser(aUserID, bUserID),
			},
		})
		if err != nil {
			return err
		}
		ups, ok := inv.Updates.(*tg.Updates)
		if !ok {
			return errors.New("unexpected updates type")
		}
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return errors.New("no chat")
		}
		chatID = chat.ID

		// A sends a message to the chat.
		_, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "cross replica msg",
			RandomID: 400001,
		})
		return err
	}); err != nil {
		t.Fatalf("A login+chat+send: %v", err)
	}

	// B (on server 2) receives the message.
	select {
	case m := <-collB.newMsg:
		if m.Message != "cross replica msg" {
			t.Fatalf("B received %q, want %q", m.Message, "cross replica msg")
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for cross-replica message: %v", ctx.Err())
	}

	close(bCmds)
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}
