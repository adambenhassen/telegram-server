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

	// 1. A searches for "hello" — should return exactly 2 A-owned messages.
	msgs, err := search(aCmds, peerB, "hello", 0, 10)
	if err != nil {
		t.Fatalf("A search hello: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("A search hello: got %d messages, want 2", len(msgs))
	}
	// Results should be newest-first: "another hello here" then "hello world".
	if msgs[0].Message != "another hello here" {
		t.Errorf("A search hello[0] = %q, want %q", msgs[0].Message, "another hello here")
	}
	if msgs[1].Message != "hello world" {
		t.Errorf("A search hello[1] = %q, want %q", msgs[1].Message, "hello world")
	}

	// 2. A searches for "foo" — should return exactly 1 result.
	msgs, err = search(aCmds, peerB, "foo", 0, 10)
	if err != nil {
		t.Fatalf("A search foo: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("A search foo: got %d messages, want 1", len(msgs))
	}
	if msgs[0].Message != "foo bar" {
		t.Errorf("A search foo[0] = %q, want %q", msgs[0].Message, "foo bar")
	}

	// 3. B searches for "hello" — should return B's own sent message ("hello from B"),
	// not A's messages stored on B's side as inbound. The out=true predicate in the
	// search query ensures only the caller's own sent messages are returned.
	msgs, err = search(bCmds, peerA, "hello", 0, 10)
	if err != nil {
		t.Fatalf("B search hello: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("B search hello: got %d messages, want 1", len(msgs))
	}
	if msgs[0].Message != "hello from B" {
		t.Errorf("B search hello[0] = %q, want %q", msgs[0].Message, "hello from B")
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
	if msgs[0].Message != "another hello here" {
		t.Errorf("A search hello (limit=1)[0] = %q, want %q", msgs[0].Message, "another hello here")
	}

	// Follow up with offset_id to get the next result.
	msgs, err = search(aCmds, peerB, "hello", firstID, 1)
	if err != nil {
		t.Fatalf("A search hello (offset=%d): %v", firstID, err)
	}
	if len(msgs) != 1 {
		t.Fatalf("A search hello (offset=%d): got %d messages, want 1", firstID, len(msgs))
	}
	if msgs[0].Message != "hello world" {
		t.Errorf("A search hello (offset=%d)[0] = %q, want %q", firstID, msgs[0].Message, "hello world")
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
	msgs, err = func() ([]*tg.Message, error) {
		var out []*tg.Message
		err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
			capQuery := strings.Repeat("a", 500)
			res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer:     peerB,
				Q:        capQuery,
				Filter:   &tg.InputMessagesFilterEmpty{},
				OffsetID: 0,
				Limit:    10,
			})
			if err != nil {
				return err
			}
			m, ok := res.(*tg.MessagesMessages)
			if !ok {
				return nil
			}
			out = make([]*tg.Message, 0, len(m.Messages))
			for _, msg := range m.Messages {
				if msg, ok := msg.(*tg.Message); ok {
					out = append(out, msg)
				}
			}
			return nil
		})
		return out, err
	}()
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
