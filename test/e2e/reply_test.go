package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestReplyPersisted(t *testing.T) {
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
	const phoneA, phoneB = "+15551290001", "+15551290002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-ctx.Done():
		t.Fatalf("client A login timeout: %v", ctx.Err())
	}
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("client B login timeout: %v", ctx.Err())
	}

	peerB := peerUser(aUserID, bUserID)

	// 1. A sends a normal message (no reply) — get its local_id.
	var firstMsgID int
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "first message",
			RandomID: 90001,
		})
		if err != nil {
			return err
		}
		switch u := res.(type) {
		case *tg.Updates:
			for _, x := range u.Updates {
				if nm, ok := x.(*tg.UpdateNewMessage); ok {
					if m, ok := nm.Message.(*tg.Message); ok {
						firstMsgID = m.ID
					}
				}
			}
		case *tg.UpdateShort:
			if m, ok := u.Update.(*tg.UpdateNewMessage); ok {
				if msg, ok := m.Message.(*tg.Message); ok {
					firstMsgID = msg.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A send first message: %v", err)
	}
	if firstMsgID == 0 {
		t.Fatal("first message ID is zero")
	}

	// Drain both sides' push for the first message.
	firstFromB := recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage for first message")
	if firstFromB.Message != "first message" {
		t.Fatalf("B first message = %q, want %q", firstFromB.Message, "first message")
	}
	firstFromA := recvOrCtx(t, ctx, collA.newMsg, "A updateNewMessage for first message")
	if firstFromA.Message != "first message" {
		t.Fatalf("A first message = %q, want %q", firstFromA.Message, "first message")
	}

	// 2. A sends a reply to the first message.
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		req := &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "reply to first",
			RandomID: 90002,
		}
		req.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: firstMsgID})
		_, err := c.MessagesSendMessage(ctx, req)
		return err
	}); err != nil {
		t.Fatalf("A send reply: %v", err)
	}

	// 3. B receives the reply via updateNewMessage and checks ReplyTo field.
	replyMsg := recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage for reply")
	if replyMsg.Message != "reply to first" {
		t.Fatalf("B received %q, want %q", replyMsg.Message, "reply to first")
	}
	if replyTo, ok := replyMsg.GetReplyTo(); !ok {
		t.Fatal("reply message missing ReplyTo field")
	} else if hdr, ok := replyTo.(*tg.MessageReplyHeader); !ok {
		t.Fatalf("ReplyTo type = %T, want *tg.MessageReplyHeader", replyTo)
	} else if id, ok := hdr.GetReplyToMsgID(); !ok {
		t.Fatal("ReplyTo header missing ReplyToMsgID")
	} else if id != firstMsgID {
		t.Fatalf("ReplyToMsgID = %d, want %d", id, firstMsgID)
	}

	// 4. A's own view of the reply also has ReplyTo set.
	aReplyMsg := recvOrCtx(t, ctx, collA.newMsg, "A updateNewMessage for reply")
	if replyTo, ok := aReplyMsg.GetReplyTo(); !ok {
		t.Fatal("A's own reply message missing ReplyTo field")
	} else if hdr, ok := replyTo.(*tg.MessageReplyHeader); !ok {
		t.Fatalf("A ReplyTo type = %T, want *tg.MessageReplyHeader", replyTo)
	} else if id, ok := hdr.GetReplyToMsgID(); !ok {
		t.Fatal("A ReplyTo header missing ReplyToMsgID")
	} else if id != firstMsgID {
		t.Fatalf("A ReplyToMsgID = %d, want %d", id, firstMsgID)
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

func TestReplyInHistory(t *testing.T) {
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
	const phoneA, phoneB = "+15551291001", "+15551291002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-ctx.Done():
		t.Fatalf("client A login timeout: %v", ctx.Err())
	}
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("client B login timeout: %v", ctx.Err())
	}

	peerB := peerUser(aUserID, bUserID)
	peerA := peerUser(bUserID, aUserID)

	// 1. A sends a normal message.
	var firstMsgID int
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "original",
			RandomID: 91001,
		})
		if err != nil {
			return err
		}
		switch u := res.(type) {
		case *tg.Updates:
			for _, x := range u.Updates {
				if nm, ok := x.(*tg.UpdateNewMessage); ok {
					if m, ok := nm.Message.(*tg.Message); ok {
						firstMsgID = m.ID
					}
				}
			}
		case *tg.UpdateShort:
			if m, ok := u.Update.(*tg.UpdateNewMessage); ok {
				if msg, ok := m.Message.(*tg.Message); ok {
					firstMsgID = msg.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A send first message: %v", err)
	}

	// Drain both sides' push for the first message.
	recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage for original")
	recvOrCtx(t, ctx, collA.newMsg, "A updateNewMessage for original")

	// 2. A sends a reply.
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		req := &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "reply",
			RandomID: 91002,
		}
		req.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: firstMsgID})
		_, err := c.MessagesSendMessage(ctx, req)
		return err
	}); err != nil {
		t.Fatalf("A send reply: %v", err)
	}

	// Drain updates so history is clean.
	time.Sleep(200 * time.Millisecond)

	// 3. B calls getHistory and verifies the reply message has ReplyTo set.
	if err := exec(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerA, Limit: 10})
		if err != nil {
			return err
		}
		m, ok := res.(*tg.MessagesMessages)
		if !ok {
			t.Errorf("history type = %T", res)
			return nil
		}
		if len(m.Messages) < 2 {
			t.Errorf("history len = %d, want >= 2", len(m.Messages))
			return nil
		}
		// The reply message should be the most recent (first in list).
		reply, ok := m.Messages[0].(*tg.Message)
		if !ok {
			t.Errorf("history[0] type = %T, want *tg.Message", m.Messages[0])
			return nil
		}
		if reply.Message != "reply" {
			t.Errorf("most recent message = %q, want %q", reply.Message, "reply")
			return nil
		}
		if replyTo, ok := reply.GetReplyTo(); !ok {
			t.Error("history reply message missing ReplyTo field")
		} else if hdr, ok := replyTo.(*tg.MessageReplyHeader); !ok {
			t.Errorf("ReplyTo type = %T, want *tg.MessageReplyHeader", replyTo)
		} else if id, ok := hdr.GetReplyToMsgID(); !ok {
			t.Error("history ReplyTo header missing ReplyToMsgID")
		} else if id != firstMsgID {
			t.Errorf("history ReplyToMsgID = %d, want %d", id, firstMsgID)
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory: %v", err)
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

func TestNoReplyToWhenZero(t *testing.T) {
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
	const phoneA, phoneB = "+15551292001", "+15551292002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-ctx.Done():
		t.Fatalf("client A login timeout: %v", ctx.Err())
	}
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("client B login timeout: %v", ctx.Err())
	}

	peerB := peerUser(aUserID, bUserID)
	peerA := peerUser(bUserID, aUserID)

	// 1. A sends a message with no reply_to — neither side should see ReplyTo.
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "no reply here",
			RandomID: 92001,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}

	// B receives via updateNewMessage.
	got := recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage")
	if replyTo, ok := got.GetReplyTo(); ok {
		t.Fatalf("non-reply message has ReplyTo = %+v, want none", replyTo)
	}

	// A also receives their own copy.
	aGot := recvOrCtx(t, ctx, collA.newMsg, "A updateNewMessage")
	if replyTo, ok := aGot.GetReplyTo(); ok {
		t.Fatalf("A's own non-reply message has ReplyTo = %+v, want none", replyTo)
	}

	// 2. Verify via getHistory as well.
	if err := exec(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerB, Limit: 10})
		if err != nil {
			return err
		}
		m, ok := res.(*tg.MessagesMessages)
		if !ok {
			t.Errorf("history type = %T", res)
			return nil
		}
		if len(m.Messages) != 1 {
			t.Errorf("history len = %d, want 1", len(m.Messages))
			return nil
		}
		msg, ok := m.Messages[0].(*tg.Message)
		if !ok {
			t.Errorf("history[0] type = %T, want *tg.Message", m.Messages[0])
			return nil
		}
		if replyTo, ok := msg.GetReplyTo(); ok {
			t.Errorf("history message has ReplyTo = %+v, want none", replyTo)
		}
		return nil
	}); err != nil {
		t.Fatalf("A getHistory: %v", err)
	}

	if err := exec(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerA, Limit: 10})
		if err != nil {
			return err
		}
		m, ok := res.(*tg.MessagesMessages)
		if !ok {
			t.Errorf("history type = %T", res)
			return nil
		}
		if len(m.Messages) != 1 {
			t.Errorf("history len = %d, want 1", len(m.Messages))
			return nil
		}
		msg, msgOk := m.Messages[0].(*tg.Message)
		if !msgOk {
			t.Errorf("history[0] type = %T, want *tg.Message", m.Messages[0])
			return nil
		}
		if replyTo, ok := msg.GetReplyTo(); ok {
			t.Errorf("history message has ReplyTo = %+v, want none", replyTo)
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory: %v", err)
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

// exec sends a command to the interactive client and waits for the result.
// The server/bootstrap helpers and type helpers live in messaging_test.go,
// as does the bootServerWithRegistry entrypoint for these e2e tests.
func exec(t *testing.T, ctx context.Context, cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
	t.Helper()
	d := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: d}:
	case <-ctx.Done():
		t.Fatalf("command enqueue timeout: %v", ctx.Err())
		return ctx.Err()
	}
	return <-d
}
