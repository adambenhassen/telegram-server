package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// globalHits pulls the plain messages out of a searchGlobal reply, asserting the
// slice shape a cross-peer page has to use, and returns them with the next_rate
// a client pages on.
func globalHits(t *testing.T, res tg.MessagesMessagesClass) (*tg.MessagesMessagesSlice, []*tg.Message) {
	t.Helper()
	slice, ok := res.(*tg.MessagesMessagesSlice)
	if !ok {
		t.Fatalf("searchGlobal reply = %T, want *tg.MessagesMessagesSlice", res)
	}
	msgs := make([]*tg.Message, 0, len(slice.Messages))
	for _, m := range slice.Messages {
		msg, isMsg := m.(*tg.Message)
		if !isMsg {
			t.Fatalf("hit = %T, want *tg.Message", m)
		}
		msgs = append(msgs, msg)
	}
	return slice, msgs
}

// TestSearchGlobalAcrossDialogs drives messages.searchGlobal through real gotd
// clients: one query reaches a 1:1, a chat and a channel at once, a caller who
// is not in the channel never sees its post, and paging under the total serves
// every match exactly once.
func TestSearchGlobalAcrossDialogs(t *testing.T) {
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

	const phoneA, phoneB = "+15551322001", "+15551322002"
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	collB := newUpdateCollector()
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
	}()

	login := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")

	// A 1:1 with B, a chat with B, and a channel B never joins — one "deadline"
	// message in each, plus one message in each that matches nothing.
	send := func(cmds chan command, peer tg.InputPeerClass, text string, randomID int64) {
		t.Helper()
		execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peer, Message: text, RandomID: randomID,
			})
			return err
		})
	}
	send(aCmds, peerUser(aUserID, bUserID), "the deadline is monday", 7001)
	send(aCmds, peerUser(aUserID, bUserID), "unrelated dm", 7002)

	var chatID int64
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		inv, err := c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Crew",
			Users: []tg.InputUserClass{inputUser(aUserID, bUserID)},
		})
		if err != nil {
			return err
		}
		ups, isUps := inv.Updates.(*tg.Updates)
		if !isUps {
			return errors.New("createChat: unexpected updates type")
		}
		chat, isChat := ups.Chats[0].(*tg.Chat)
		if !isChat {
			return errors.New("createChat: chat is not *tg.Chat")
		}
		chatID = chat.ID
		return nil
	})
	if _, err = collB.waitService(ctx, &tg.MessageActionChatCreate{}); err != nil {
		t.Fatalf("B wait create service: %v", err)
	}
	send(aCmds, &tg.InputPeerChat{ChatID: chatID}, "chat deadline moved", 7003)

	chID := createBroadcastChannel(t, ctx, aCmds, "Ops")
	send(aCmds, peerChannel(aUserID, chID), "channel deadline notice", 7004)
	send(aCmds, peerChannel(aUserID, chID), "unrelated chatter", 7005)

	searchGlobal := func(cmds chan command, req *tg.MessagesSearchGlobalRequest) (*tg.MessagesMessagesSlice, []*tg.Message) {
		t.Helper()
		var res tg.MessagesMessagesClass
		execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
			out, err := c.MessagesSearchGlobal(ctx, req)
			if err != nil {
				return err
			}
			res = out
			return nil
		})
		return globalHits(t, res)
	}
	query := func(limit int) *tg.MessagesSearchGlobalRequest {
		return &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      limit,
		}
	}

	// 1. A gets all three hits at once, with the peers named and hydrated.
	slice, msgs := searchGlobal(aCmds, query(10))
	if len(msgs) != 3 {
		t.Fatalf("A searchGlobal: got %d hits, want 3", len(msgs))
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		switch p := m.PeerID.(type) {
		case *tg.PeerUser:
			seen["user"] = p.UserID == bUserID
		case *tg.PeerChat:
			seen["chat"] = p.ChatID == chatID
		case *tg.PeerChannel:
			seen["channel"] = p.ChannelID == chID
		}
	}
	if !seen["user"] || !seen["chat"] || !seen["channel"] {
		t.Fatalf("A searchGlobal peers = %v, want one hit per peer kind", seen)
	}
	if !hasChannel(slice.Chats, chID) {
		t.Errorf("A searchGlobal: channel %d missing from Chats", chID)
	}
	var sawChat bool
	for _, c := range slice.Chats {
		if chat, isChat := c.(*tg.Chat); isChat && chat.ID == chatID {
			sawChat = true
		}
	}
	if !sawChat {
		t.Errorf("A searchGlobal: chat %d missing from Chats", chatID)
	}
	var sawB bool
	for _, u := range slice.Users {
		if u.GetID() == bUserID {
			sawB = true
		}
	}
	if !sawB {
		t.Errorf("A searchGlobal: user %d missing from Users", bUserID)
	}

	// 2. B shares the 1:1 and the chat but never joined the channel, so the
	// channel post is not in B's result and the channel is not in B's Chats.
	bSlice, bMsgs := searchGlobal(bCmds, query(10))
	if len(bMsgs) != 2 {
		t.Fatalf("B searchGlobal: got %d hits, want 2", len(bMsgs))
	}
	for _, m := range bMsgs {
		if p, isChannel := m.PeerID.(*tg.PeerChannel); isChannel {
			t.Fatalf("B was served a post from channel %d", p.ChannelID)
		}
	}
	if hasChannel(bSlice.Chats, chID) {
		t.Errorf("B searchGlobal: channel %d leaked through Chats", chID)
	}

	// 3. Paging one hit at a time serves every match exactly once, in the order
	// a single page gives them.
	inputPeerOf := func(p tg.PeerClass) tg.InputPeerClass {
		switch v := p.(type) {
		case *tg.PeerUser:
			return peerUser(aUserID, v.UserID)
		case *tg.PeerChat:
			return &tg.InputPeerChat{ChatID: v.ChatID}
		case *tg.PeerChannel:
			return peerChannel(aUserID, v.ChannelID)
		default:
			t.Fatalf("peer %T has no input form", p)
			return nil
		}
	}
	req := query(1)
	var paged []string
	for range len(msgs) + 1 {
		page, pageMsgs := searchGlobal(aCmds, req)
		if len(pageMsgs) == 0 {
			break
		}
		if len(pageMsgs) != 1 {
			t.Fatalf("limit=1 page returned %d hits", len(pageMsgs))
		}
		paged = append(paged, pageMsgs[0].Message)
		rate, hasRate := page.GetNextRate()
		if !hasRate {
			t.Fatal("non-empty page carries no next_rate")
		}
		req = &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetRate: rate,
			OffsetPeer: inputPeerOf(pageMsgs[0].PeerID),
			OffsetID:   pageMsgs[0].ID,
			Limit:      1,
		}
	}
	if len(paged) != len(msgs) {
		t.Fatalf("paged %v, want the same %d hits as one page", paged, len(msgs))
	}
	for i, m := range msgs {
		if paged[i] != m.Message {
			t.Fatalf("page sequence diverges at %d: got %q, want %q", i, paged[i], m.Message)
		}
	}

	close(aCmds)
	close(bCmds)
	for _, e := range []struct {
		who string
		ch  chan error
	}{{"A", errA}, {"B", errB}} {
		if err := <-e.ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client %s run: %v", e.who, err)
		}
	}
}
