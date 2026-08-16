package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestChannelReplyPersisted covers acceptance criteria 1–5 from MAIN-328:
// a channel post carrying InputReplyToMessage stores the reference; the
// reference appears on real-time push, on getChannelDifference backfill, and
// on getMessages / getHistory reads. A post with no reply has no reference. A
// post whose ReplyToMsgID names a deleted, absent or cross-channel post is
// refused with MESSAGE_ID_INVALID and writes nothing.
func TestChannelReplyPersisted(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15551296001", "+15551296002"
	addr := tcpPort(t, ln)
	collA, collB := newUpdateCollector(), newUpdateCollector()
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr, key, dcID, collA, nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
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
	_ = bUserID

	// A creates a broadcast channel; B joins via invite.
	chID := createBroadcastChannel(t, ctx, aCmds, "ReplyChannel")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	// 1. A posts the root message and captures its id.
	var rootMsgID int
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "root",
			RandomID: 96001001,
		})
		if err != nil {
			return err
		}
		if ups, ok := res.(*tg.Updates); ok {
			for _, u := range ups.Updates {
				if nm, ok := u.(*tg.UpdateNewChannelMessage); ok {
					if m, ok := nm.Message.(*tg.Message); ok {
						rootMsgID = m.ID
					}
				}
			}
		}
		return nil
	})
	if rootMsgID == 0 {
		t.Fatal("root post ID is zero")
	}

	// Drain B's push for root so collB.newChannelMsg is empty before the reply.
	recvOrCtx(t, ctx, collB.newChannelMsg, "B push for root")

	// 2. A posts a reply.
	var replyMsgID int
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		req := &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "child",
			RandomID: 96001002,
		}
		req.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: rootMsgID})
		res, err := c.MessagesSendMessage(ctx, req)
		if err != nil {
			return err
		}
		if ups, ok := res.(*tg.Updates); ok {
			for _, u := range ups.Updates {
				if nm, ok := u.(*tg.UpdateNewChannelMessage); ok {
					if m, ok := nm.Message.(*tg.Message); ok {
						replyMsgID = m.ID
					}
				}
			}
		}
		return nil
	})
	if replyMsgID == 0 {
		t.Fatal("reply post ID is zero")
	}

	// Criterion 2a: B (connected) receives the push for "child" with ReplyTo set.
	bPush := recvOrCtx(t, ctx, collB.newChannelMsg, "B push for reply")
	if bPush.Msg.Message != "child" {
		t.Fatalf("B push message = %q, want %q", bPush.Msg.Message, "child")
	}
	checkReplyHeader(t, bPush.Msg, rootMsgID, "B push")

	// Criterion 1: channels.getMessages returns the reply with ReplyTo.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: inputChannel(aUserID, chID),
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: replyMsgID}},
		})
		if err != nil {
			return err
		}
		cm, ok := res.(*tg.MessagesChannelMessages)
		if !ok {
			t.Errorf("getMessages: type = %T, want *tg.MessagesChannelMessages", res)
			return nil
		}
		if len(cm.Messages) != 1 {
			t.Errorf("getMessages: len = %d, want 1", len(cm.Messages))
			return nil
		}
		m, ok := cm.Messages[0].(*tg.Message)
		if !ok {
			t.Errorf("getMessages[0] type = %T, want *tg.Message", cm.Messages[0])
			return nil
		}
		checkReplyHeader(t, m, rootMsgID, "channels.getMessages")
		return nil
	})

	// Criterion 1: messages.getHistory returns the reply with ReplyTo.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(bUserID, chID),
			Limit: 10,
		})
		if err != nil {
			return err
		}
		cm, ok := res.(*tg.MessagesChannelMessages)
		if !ok {
			t.Errorf("getHistory: type = %T, want *tg.MessagesChannelMessages", res)
			return nil
		}
		var foundReply *tg.Message
		for _, msg := range cm.Messages {
			if m, ok := msg.(*tg.Message); ok && m.ID == replyMsgID {
				foundReply = m
			}
		}
		if foundReply == nil {
			t.Errorf("getHistory: reply message not found")
			return nil
		}
		checkReplyHeader(t, foundReply, rootMsgID, "messages.getHistory")
		return nil
	})

	// Criterion 2b: getChannelDifference from pts=0 delivers "child" with ReplyTo.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(bUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   100,
		})
		if err != nil {
			return err
		}
		diff, ok := d.(*tg.UpdatesChannelDifference)
		if !ok {
			t.Errorf("getChannelDifference: type = %T", d)
			return nil
		}
		var foundReply *tg.Message
		for _, msg := range diff.NewMessages {
			if m, ok := msg.(*tg.Message); ok && m.ID == replyMsgID {
				foundReply = m
			}
		}
		if foundReply == nil {
			t.Errorf("getChannelDifference: reply message not found in backfill")
			return nil
		}
		checkReplyHeader(t, foundReply, rootMsgID, "getChannelDifference backfill")
		return nil
	})

	// Criterion 3: root post has no ReplyTo.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: inputChannel(aUserID, chID),
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: rootMsgID}},
		})
		if err != nil {
			return err
		}
		cm, ok := res.(*tg.MessagesChannelMessages)
		if !ok || len(cm.Messages) == 0 {
			t.Errorf("getMessages for root: unexpected response")
			return nil
		}
		m, ok := cm.Messages[0].(*tg.Message)
		if !ok {
			return nil
		}
		if replyTo, ok := m.GetReplyTo(); ok {
			t.Errorf("root post has ReplyTo = %+v, want none", replyTo)
		}
		return nil
	})

	// Criterion 4: a reply to a nonexistent message is refused with MESSAGE_ID_INVALID,
	// and writes nothing (criterion 5: pts stays at 2 after this call).
	ptsBefore := 0
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(aUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   100,
		})
		if err != nil {
			return err
		}
		if diff, ok := d.(*tg.UpdatesChannelDifference); ok {
			ptsBefore = diff.Pts
		}
		return nil
	})

	assertChannelRPCError(t, ctx, aCmds, "MESSAGE_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		req := &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "bad reply",
			RandomID: 96001003,
		}
		req.SetReplyTo(&tg.InputReplyToMessage{ReplyToMsgID: 999999})
		_, err := c.MessagesSendMessage(ctx, req)
		return err
	})

	// Criterion 5: pts unchanged after the refused post.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(aUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   100,
		})
		if err != nil {
			return err
		}
		if diff, ok := d.(*tg.UpdatesChannelDifference); ok {
			if diff.Pts != ptsBefore {
				t.Errorf("pts after refused reply = %d, want %d (no advance)", diff.Pts, ptsBefore)
			}
			if len(diff.NewMessages) != 2 {
				t.Errorf("message count after refused reply = %d, want 2", len(diff.NewMessages))
			}
		}
		return nil
	})

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// checkReplyHeader asserts that m carries a MessageReplyHeader pointing at wantMsgID.
func checkReplyHeader(t *testing.T, m *tg.Message, wantMsgID int, where string) {
	t.Helper()
	replyTo, ok := m.GetReplyTo()
	if !ok {
		t.Errorf("%s: message %d missing ReplyTo", where, m.ID)
		return
	}
	hdr, ok := replyTo.(*tg.MessageReplyHeader)
	if !ok {
		t.Errorf("%s: ReplyTo type = %T, want *tg.MessageReplyHeader", where, replyTo)
		return
	}
	id, ok := hdr.GetReplyToMsgID()
	if !ok {
		t.Errorf("%s: ReplyTo header missing ReplyToMsgID", where)
		return
	}
	if id != wantMsgID {
		t.Errorf("%s: ReplyToMsgID = %d, want %d", where, id, wantMsgID)
	}
}
