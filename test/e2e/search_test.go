package e2e_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSearchMessages(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB := newClient(collA), newClient(collB)
	const phoneA, phoneB = "+15551284001", "+15551284002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("client B login timeout")
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)
	peerA := peerUser(bUserID, aUserID)

	send := func(cmds chan command, peer tg.InputPeerClass, text string, randomID int64) {
		if err := exec(cmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peer, Message: text, RandomID: randomID,
			})
			return err
		}); err != nil {
			t.Fatalf("send %q: %v", text, err)
		}
	}

	// A sends 5 messages to B: 2 with "hello", 1 with "foo", 2 unrelated.
	send(aCmds, peerB, "hello world", 100)
	send(aCmds, peerB, "foo bar", 200)
	send(aCmds, peerB, "just some random text", 300)
	send(aCmds, peerB, "another hello here", 400)
	send(aCmds, peerB, "totally unrelated msg", 500)

	// B sends 2 messages (should never appear in A's search or vice versa).
	send(bCmds, peerA, "hello from B", 600)
	send(bCmds, peerA, "foo from B", 700)

	// Small delay to ensure all messages are committed.
	time.Sleep(100 * time.Millisecond)

	search := func(cmds chan command, peer tg.InputPeerClass, q string, offsetID, limit int) ([]*tg.Message, error) {
		var msgs []*tg.Message
		err := exec(cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer:     peer,
				Q:        q,
				Filter:   &tg.InputMessagesFilterEmpty{},
				OffsetID: offsetID,
				Limit:    limit,
			})
			if err != nil {
				return err
			}
			m, ok := res.(*tg.MessagesMessages)
			if !ok {
				t.Errorf("search result type = %T, want *tg.MessagesMessages", res)
				return nil
			}
			msgs = make([]*tg.Message, 0, len(m.Messages))
			for _, m := range m.Messages {
				if msg, ok := m.(*tg.Message); ok {
					msgs = append(msgs, msg)
				}
			}
			return nil
		})
		return msgs, err
	}

	// 1. A searches for "hello" — should return 3: A's two sent messages and B's
	// "hello from B" (inbound matching, both directions).
	msgs, err := search(aCmds, peerB, "hello", 0, 10)
	if err != nil {
		t.Fatalf("A search hello: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("A search hello: got %d messages, want 3", len(msgs))
	}
	// Newest-first: "another hello here" (randomID 400), "hello from B" (randomID 600),
	// "hello world" (randomID 100). Ordered by local_id DESC.
	if msgs[0].Message != "hello from B" {
		t.Errorf("A search hello[0] = %q, want %q", msgs[0].Message, "hello from B")
	}
	if msgs[1].Message != "another hello here" {
		t.Errorf("A search hello[1] = %q, want %q", msgs[1].Message, "another hello here")
	}
	if msgs[2].Message != "hello world" {
		t.Errorf("A search hello[2] = %q, want %q", msgs[2].Message, "hello world")
	}

	// 2. A searches for "foo" — should return 2: A's "foo bar" and B's "foo from B".
	msgs, err = search(aCmds, peerB, "foo", 0, 10)
	if err != nil {
		t.Fatalf("A search foo: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("A search foo: got %d messages, want 2", len(msgs))
	}
	// Newest-first: "foo from B" (randomID 700) then "foo bar" (randomID 200).
	if msgs[0].Message != "foo from B" {
		t.Errorf("A search foo[0] = %q, want %q", msgs[0].Message, "foo from B")
	}
	if msgs[1].Message != "foo bar" {
		t.Errorf("A search foo[1] = %q, want %q", msgs[1].Message, "foo bar")
	}

	// 3. B searches for "hello" — should return 3: B's own sent message ("hello from B")
	// AND A's inbound messages ("hello world", "another hello here"): all three rows
	// belong to B's owner_id, both directions match.
	msgs, err = search(bCmds, peerA, "hello", 0, 10)
	if err != nil {
		t.Fatalf("B search hello: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("B search hello: got %d messages, want 3", len(msgs))
	}
	// Newest-first: "hello from B" (randomID 600, sent after A's messages),
	// then "another hello here", then "hello world".
	if msgs[0].Message != "hello from B" {
		t.Errorf("B search hello[0] = %q, want %q", msgs[0].Message, "hello from B")
	}
	if msgs[1].Message != "another hello here" {
		t.Errorf("B search hello[1] = %q, want %q", msgs[1].Message, "another hello here")
	}
	if msgs[2].Message != "hello world" {
		t.Errorf("B search hello[2] = %q, want %q", msgs[2].Message, "hello world")
	}

	// 4. Pagination: search with limit=1, then use offset_id.
	msgs, err = search(aCmds, peerB, "hello", 0, 1)
	if err != nil {
		t.Fatalf("A search hello (limit=1): %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("A search hello (limit=1): got %d messages, want 1", len(msgs))
	}
	firstID := msgs[0].ID
	if msgs[0].Message != "hello from B" {
		t.Errorf("A search hello (limit=1)[0] = %q, want %q", msgs[0].Message, "hello from B")
	}

	// Follow up with offset_id to get the next result.
	msgs, err = search(aCmds, peerB, "hello", firstID, 1)
	if err != nil {
		t.Fatalf("A search hello (offset=%d): %v", firstID, err)
	}
	if len(msgs) != 1 {
		t.Fatalf("A search hello (offset=%d): got %d messages, want 1", firstID, len(msgs))
	}
	secondID := msgs[0].ID
	if msgs[0].Message != "another hello here" {
		t.Errorf("A search hello (offset=%d)[0] = %q, want %q", firstID, msgs[0].Message, "another hello here")
	}

	// Third page.
	msgs, err = search(aCmds, peerB, "hello", secondID, 1)
	if err != nil {
		t.Fatalf("A search hello (offset=%d): %v", secondID, err)
	}
	if len(msgs) != 1 {
		t.Fatalf("A search hello (offset=%d): got %d messages, want 1", secondID, len(msgs))
	}
	if msgs[0].Message != "hello world" {
		t.Errorf("A search hello (offset=%d)[0] = %q, want %q", secondID, msgs[0].Message, "hello world")
	}

	// 5. Search with empty query returns SEARCH_QUERY_EMPTY.
	err = exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:   peerB,
			Q:      "",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	})
	if err == nil {
		t.Fatal("A search empty query: expected error, got nil")
	}
	var rpcErr *tgerr.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("A search empty query: expected RPC error, got %T: %v", err, err)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "SEARCH_QUERY_EMPTY" {
		t.Fatalf("A search empty query: got %d %s, want 400 SEARCH_QUERY_EMPTY", rpcErr.Code, rpcErr.Message)
	}

	// 6. Search for non-existent term returns empty results.
	msgs, err = search(aCmds, peerB, "zzzznotfound", 0, 10)
	if err != nil {
		t.Fatalf("A search not found: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("A search not found: got %d messages, want 0", len(msgs))
	}

	// 7. Search with unsupported filter returns INPUT_FILTER_INVALID.
	err = exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:   peerB,
			Q:      "hello",
			Filter: &tg.InputMessagesFilterPhotos{},
		})
		return err
	})
	if err == nil {
		t.Fatal("A search photos filter: expected error, got nil")
	}
	if !errors.As(err, &rpcErr) {
		t.Fatalf("A search photos filter: expected RPC error, got %T: %v", err, err)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "INPUT_FILTER_INVALID" {
		t.Fatalf("A search photos filter: got %d %s, want 400 INPUT_FILTER_INVALID", rpcErr.Code, rpcErr.Message)
	}

	// 8. Search with oversized query returns MESSAGE_TOO_LONG.
	err = exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		longQuery := strings.Repeat("a", 501)
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:   peerB,
			Q:      longQuery,
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	})
	if err == nil {
		t.Fatal("A search oversized query: expected error, got nil")
	}
	if !errors.As(err, &rpcErr) {
		t.Fatalf("A search oversized query: expected RPC error, got %T: %v", err, err)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "MESSAGE_TOO_LONG" {
		t.Fatalf("A search oversized query: got %d %s, want 400 MESSAGE_TOO_LONG", rpcErr.Code, rpcErr.Message)
	}

	// 9. Search with a query at the cap (500 chars) succeeds without error.
	msgs, err = search(aCmds, peerB, strings.Repeat("a", 500), 0, 10)
	if err != nil {
		t.Fatalf("A search at cap (500): %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("A search at cap (500): got %d messages, want 0", len(msgs))
	}

	// 10. Search with a forged AccessHash returns PEER_ID_INVALID.
	err = exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer: &tg.InputPeerUser{
				UserID:     bUserID,
				AccessHash: 999999999, // wrong hash
			},
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	})
	if err == nil {
		t.Fatal("A search forged hash: expected error, got nil")
	}
	if !errors.As(err, &rpcErr) {
		t.Fatalf("A search forged hash: expected RPC error, got %T: %v", err, err)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "PEER_ID_INVALID" {
		t.Fatalf("A search forged hash: got %d %s, want 400 PEER_ID_INVALID", rpcErr.Code, rpcErr.Message)
	}

	// 11. Search a dialog with a user the caller has never exchanged messages with
	// returns empty results, not an error.
	collC := newUpdateCollector()
	clientC := newClient(collC)
	const phoneC = "+15551284003"
	cCmds := make(chan command)
	cID := make(chan int64, 1)
	errC := make(chan error, 1)
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC), cID, cCmds) }()
	var cUserID int64
	select {
	case cUserID = <-cID:
	case <-time.After(30 * time.Second):
		t.Fatal("client C login timeout")
	}
	peerC := peerUser(aUserID, cUserID)
	msgs, err = search(aCmds, peerC, "hello", 0, 10)
	if err != nil {
		t.Fatalf("A search unknown peer: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("A search unknown peer: got %d messages, want 0", len(msgs))
	}
	close(cCmds)
	if err := <-errC; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client C run: %v", err)
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

// TestSearchChatPeer exercises messages.search with InputPeerChat: it returns
// matching messages from the caller's own history rows for that chat, both
// directions, newest-first. Channel peers still return PEER_ID_INVALID.
func TestSearchChatPeer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	collA, collB, collC := newUpdateCollector(), newUpdateCollector(), newUpdateCollector()
	clientA, clientB, clientC := newClient(collA), newClient(collB), newClient(collC)
	const phoneA, phoneB, phoneC = "+15551285001", "+15551285002", "+15551285003"

	aCmds, bCmds, cCmds := make(chan command), make(chan command), make(chan command)
	aID, bID, cID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC), cID, cCmds) }()

	logins := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-time.After(30 * time.Second):
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID, bUserID, cUserID := logins(aID, "A"), logins(bID, "B"), logins(cID, "C")

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	// A creates chat with B and C, title "Team".
	var chatID int64
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
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
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return errors.New("createChat: chat is not *tg.Chat")
		}
		chatID = chat.ID
		return nil
	}); err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Wait for B and C to receive the create service message.
	waitSvc := func(coll *updateCollector, who string) {
		t.Helper()
		sCtx, sCancel := context.WithTimeout(ctx, 10*time.Second)
		defer sCancel()
		_, err := coll.waitService(sCtx, &tg.MessageActionChatCreate{})
		if err != nil {
			t.Fatalf("%s wait create service: %v", who, err)
		}
	}
	waitSvc(collB, "B")
	waitSvc(collC, "C")

	chatPeer := func() *tg.InputPeerChat { return &tg.InputPeerChat{ChatID: chatID} }

	sendToChat := func(cmds chan command, text string, randomID int64) {
		if err := exec(cmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: chatPeer(), Message: text, RandomID: randomID,
			})
			return err
		}); err != nil {
			t.Fatalf("send %q: %v", text, err)
		}
	}

	// A sends "quarterly report ready" to the chat.
	sendToChat(aCmds, "quarterly report ready", 1000)
	// B sends "review the budget" to the chat.
	sendToChat(bCmds, "review the budget", 2000)
	// A sends "budget approved" to the chat.
	sendToChat(aCmds, "budget approved", 3000)
	// C sends "quarterly update" to the chat.
	sendToChat(cCmds, "quarterly update", 4000)

	// Drain received messages to unblock clients.
	drainMessages := func(coll *updateCollector, count int) {
		n := 1
		for range count {
			select {
			case <-coll.newMsg:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for message %d", n)
			}
			n++
		}
	}
	// Each member receives all 4 chat messages sent above; the create service
	// message arrives on the serviceMsg channel, not newMsg.
	drainMessages(collA, 4)
	drainMessages(collB, 4)
	drainMessages(collC, 4)

	// Small delay to ensure all messages are committed.
	time.Sleep(100 * time.Millisecond)

	searchChat := func(cmds chan command, chatPeer tg.InputPeerClass, q string) ([]*tg.Message, error) {
		var msgs []*tg.Message
		err := exec(cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer:     chatPeer,
				Q:        q,
				Filter:   &tg.InputMessagesFilterEmpty{},
				OffsetID: 0,
				Limit:    10,
			})
			if err != nil {
				return err
			}
			m, ok := res.(*tg.MessagesMessages)
			if !ok {
				t.Errorf("search result type = %T, want *tg.MessagesMessages", res)
				return nil
			}
			msgs = make([]*tg.Message, 0, len(m.Messages))
			for _, m := range m.Messages {
				if msg, ok := m.(*tg.Message); ok {
					msgs = append(msgs, msg)
				}
			}
			return nil
		})
		return msgs, err
	}

	// 1. B searches chat for "quarterly" — should return both "quarterly report ready"
	// (A's message, inbound for B) and "quarterly update" (C's message, inbound for B).
	msgs, err := searchChat(bCmds, chatPeer(), "quarterly")
	if err != nil {
		t.Fatalf("B search chat quarterly: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("B search chat quarterly: got %d messages, want 2", len(msgs))
	}
	// Newest-first.
	if msgs[0].Message != "quarterly update" {
		t.Errorf("B search[0] = %q, want %q", msgs[0].Message, "quarterly update")
	}
	if msgs[1].Message != "quarterly report ready" {
		t.Errorf("B search[1] = %q, want %q", msgs[1].Message, "quarterly report ready")
	}

	// 2. A searches chat for "budget" — should return both "review the budget"
	// (B's message, inbound for A) and "budget approved" (A's own message).
	msgs, err = searchChat(aCmds, chatPeer(), "budget")
	if err != nil {
		t.Fatalf("A search chat budget: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("A search chat budget: got %d messages, want 2", len(msgs))
	}
	if msgs[0].Message != "budget approved" {
		t.Errorf("A search[0] = %q, want %q", msgs[0].Message, "budget approved")
	}
	if msgs[1].Message != "review the budget" {
		t.Errorf("A search[1] = %q, want %q", msgs[1].Message, "review the budget")
	}

	// 3. Non-member (outsider) cannot search the chat.
	collD := newUpdateCollector()
	clientD := newClient(collD)
	const phoneD = "+15551285004"
	dCmds := make(chan command)
	dID := make(chan int64, 1)
	errD := make(chan error, 1)
	go func() { errD <- runInteractive(ctx, clientD, flowFor(phoneD), dID, dCmds) }()
	var _ int64
	select {
	case <-dID:
	case <-time.After(30 * time.Second):
		t.Fatal("client D login timeout")
	}
	var searchErr error
	if err := exec(dCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:   chatPeer(),
			Q:      "quarterly",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		searchErr = err
		return nil
	}); err != nil {
		t.Fatalf("D search chat: %v", err)
	}
	if searchErr == nil {
		t.Fatal("D (non-member) search chat: expected PEER_ID_INVALID, got nil")
	}
	var rpcErr *tgerr.Error
	if !errors.As(searchErr, &rpcErr) {
		t.Fatalf("D search chat: expected RPC error, got %T: %v", searchErr, searchErr)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "PEER_ID_INVALID" {
		t.Fatalf("D search chat: got %d %s, want 400 PEER_ID_INVALID", rpcErr.Code, rpcErr.Message)
	}
	close(dCmds)
	if err := <-errD; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client D run: %v", err)
	}

	// 4. Channel peer returns PEER_ID_INVALID.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  1,
				AccessHash: 0,
			},
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	}); err == nil {
		t.Fatal("A search channel peer: expected error, got nil")
	} else if !errors.As(err, &rpcErr) {
		t.Fatalf("A search channel peer: expected RPC error, got %T: %v", err, err)
	} else if rpcErr.Code != 400 || rpcErr.Message != "PEER_ID_INVALID" {
		t.Fatalf("A search channel peer: got %d %s, want 400 PEER_ID_INVALID", rpcErr.Code, rpcErr.Message)
	}

	close(aCmds)
	close(bCmds)
	close(cCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
	if err := <-errC; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client C run: %v", err)
	}
}
