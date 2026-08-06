package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestReactionsRealtime proves the full reaction lifecycle: A reacts on a
// shared 1:1 message, B receives updateMessageReactions live; A clears the
// reaction, B receives push reflecting cleared state; getHistory returns
// current reactions on the message.
func TestReactionsRealtime(t *testing.T) {
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

	const phoneA, phoneB = "+15551285001", "+15551285002"

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB :=
		createClient(addr.Port, key, dcID, collA, nil),
		createClient(addr.Port, key, dcID, collB, nil)

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA, codes), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB, codes), bID, bCmds) }()

	logins := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-time.After(30 * time.Second):
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID, bUserID := logins(aID, "A"), logins(bID, "B")

	peerB := peerUser(aUserID, bUserID)
	peerA := peerUser(bUserID, aUserID)

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	// 1. A sends a message to B.
	var msgID int
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "react to this",
			RandomID: 500001,
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
					msgID = m.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	// Drain B's new message.
	recvOr(t, collB.newMsg, "B updateNewMessage")

	// 2. A reacts with a heart emoji.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendReaction(ctx, &tg.MessagesSendReactionRequest{
			Peer:  peerB,
			MsgID: msgID,
			Reaction: []tg.ReactionClass{
				&tg.ReactionEmoji{Emoticon: "\u2764"}, // heart
			},
		})
		return err
	}); err != nil {
		t.Fatalf("A sendReaction: %v", err)
	}

	// 2b. B receives updateMessageReactions in real-time.
	reactPush := recvOr(t, collB.msgReactions, "B updateMessageReactions")
	if reactPush.MsgID != msgID {
		t.Fatalf("reaction push msgID = %d, want %d", reactPush.MsgID, msgID)
	}
	if len(reactPush.Reactions.Results) != 1 {
		t.Fatalf("reaction push results = %d, want 1", len(reactPush.Reactions.Results))
	}
	emoji, ok := reactPush.Reactions.Results[0].Reaction.(*tg.ReactionEmoji)
	if !ok || emoji.Emoticon != "\u2764" {
		t.Fatalf("reaction push emoji = %v, want heart", emoji)
	}

	// 3. B's getHistory shows the reaction on the message.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerA,
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history type")
		}
		if len(msgs.Messages) == 0 {
			return errors.New("no messages in history")
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return errors.New("first message is not *tg.Message")
		}
		if m.Message != "react to this" {
			return errors.New("wrong message in history")
		}
		reactions, ok := m.GetReactions()
		if !ok {
			return errors.New("no reactions on message")
		}
		if len(reactions.Results) != 1 {
			return errors.New("expected 1 reaction")
		}
		emoji, ok := reactions.Results[0].Reaction.(*tg.ReactionEmoji)
		if !ok {
			return errors.New("reaction is not emoji")
		}
		if emoji.Emoticon != "\u2764" {
			return errors.New("wrong reaction emoji")
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory with reactions: %v", err)
	}

	// 4. A clears the reaction (empty reaction list).
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendReaction(ctx, &tg.MessagesSendReactionRequest{
			Peer:     peerB,
			MsgID:    msgID,
			Reaction: nil,
		})
		return err
	}); err != nil {
		t.Fatalf("A clearReaction: %v", err)
	}

	// 4b. B receives updateMessageReactions reflecting cleared state.
	clearPush := recvOr(t, collB.msgReactions, "B updateMessageReactions (clear)")
	if clearPush.MsgID != msgID {
		t.Fatalf("clear push msgID = %d, want %d", clearPush.MsgID, msgID)
	}
	if len(clearPush.Reactions.Results) != 0 {
		t.Fatalf("clear push results = %d, want 0", len(clearPush.Reactions.Results))
	}

	// 5. B's getHistory shows no reactions.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerA,
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history type")
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return errors.New("first message is not *tg.Message")
		}
		_, hasReactions := m.GetReactions()
		if hasReactions {
			return errors.New("reactions should be cleared")
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory after clear: %v", err)
	}

	// 6. A reacts again with a different emoji.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendReaction(ctx, &tg.MessagesSendReactionRequest{
			Peer:  peerB,
			MsgID: msgID,
			Reaction: []tg.ReactionClass{
				&tg.ReactionEmoji{Emoticon: "\U0001f44d"}, // thumbs up
			},
		})
		return err
	}); err != nil {
		t.Fatalf("A sendReaction (thumbs up): %v", err)
	}

	// 7. B's getHistory shows the new reaction.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerA,
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history type")
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return errors.New("first message is not *tg.Message")
		}
		reactions, ok := m.GetReactions()
		if !ok {
			return errors.New("no reactions on message")
		}
		emoji, ok := reactions.Results[0].Reaction.(*tg.ReactionEmoji)
		if !ok {
			return errors.New("reaction is not emoji")
		}
		if emoji.Emoticon != "\U0001f44d" {
			return errors.New("wrong reaction emoji after update")
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory with new reaction: %v", err)
	}

	// 8. C (not a party to the conversation) cannot react.
	var cUserID int64
	const phoneC = "+15551285003"
	collC := newUpdateCollector()
	clientC := createClient(addr.Port, key, dcID, collC, nil)
	cCmds := make(chan command)
	cID := make(chan int64, 1)
	errC := make(chan error, 1)
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC, codes), cID, cCmds) }()
	select {
	case cUserID = <-cID:
	case <-time.After(30 * time.Second):
		t.Fatal("client C login timeout")
	}

	var reactErr error
	execChat(t, cCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendReaction(ctx, &tg.MessagesSendReactionRequest{
			Peer:  peerUser(cUserID, aUserID),
			MsgID: msgID,
			Reaction: []tg.ReactionClass{
				&tg.ReactionEmoji{Emoticon: "\u2764"},
			},
		})
		reactErr = err
		return nil
	})
	if reactErr == nil {
		t.Fatal("C should not be able to react to A's message")
	}
	var tgErr *tgerr.Error
	if !errors.As(reactErr, &tgErr) {
		t.Fatalf("C reaction error type = %T, want *tgerr.Error", reactErr)
	}
	if tgErr.Message != "MESSAGE_ID_INVALID" {
		t.Fatalf("C reaction error = %s, want MESSAGE_ID_INVALID", tgErr.Message)
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

// TestReactionsChatFanout proves the chat reaction fan-out: A, B, C in a chat;
// A reacts on a message; B and C each receive updateMessageReactions in real-time;
// getHistory for each shows the reaction.
func TestReactionsChatFanout(t *testing.T) {
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

	const phoneA, phoneB, phoneC = "+15551286001", "+15551286002", "+15551286003"

	collA, collB, collC := newUpdateCollector(), newUpdateCollector(), newUpdateCollector()
	clientA, clientB, clientC :=
		createClient(addr.Port, key, dcID, collA, nil),
		createClient(addr.Port, key, dcID, collB, nil),
		createClient(addr.Port, key, dcID, collC, nil)

	aCmds, bCmds, cCmds := make(chan command), make(chan command), make(chan command)
	aID, bID, cID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA, codes), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB, codes), bID, bCmds) }()
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC, codes), cID, cCmds) }()

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

	// 1. A creates a chat with B and C.
	var chatID int64
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		inv, err := c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Reactions",
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
			return errors.New("createChat: not a *tg.Chat")
		}
		chatID = chat.ID
		return nil
	}); err != nil {
		t.Fatalf("createChat: %v", err)
	}
	// Drain B and C service messages for create.
	recvOr(t, collB.serviceMsg, "B create service")
	recvOr(t, collC.serviceMsg, "C create service")

	// 2. A sends a message to the chat.
	var msgID int
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChat{ChatID: chatID},
			Message:  "react to this in chat",
			RandomID: 600001,
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
					msgID = m.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A send to chat: %v", err)
	}
	// Drain B and C new messages.
	recvOr(t, collB.newMsg, "B updateNewMessage")
	recvOr(t, collC.newMsg, "C updateNewMessage")

	// 3. A reacts with a heart emoji.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendReaction(ctx, &tg.MessagesSendReactionRequest{
			Peer:  &tg.InputPeerChat{ChatID: chatID},
			MsgID: msgID,
			Reaction: []tg.ReactionClass{
				&tg.ReactionEmoji{Emoticon: "\u2764"},
			},
		})
		return err
	}); err != nil {
		t.Fatalf("A sendReaction in chat: %v", err)
	}

	// 3b. B receives updateMessageReactions.
	reactB := recvOr(t, collB.msgReactions, "B updateMessageReactions (chat)")
	if reactB.MsgID != msgID {
		t.Fatalf("B reaction push msgID = %d, want %d", reactB.MsgID, msgID)
	}
	if len(reactB.Reactions.Results) != 1 {
		t.Fatalf("B reaction push results = %d, want 1", len(reactB.Reactions.Results))
	}
	emojiB, ok := reactB.Reactions.Results[0].Reaction.(*tg.ReactionEmoji)
	if !ok || emojiB.Emoticon != "\u2764" {
		t.Fatalf("B reaction push emoji = %v, want heart", emojiB)
	}

	// 3c. C receives updateMessageReactions.
	reactC := recvOr(t, collC.msgReactions, "C updateMessageReactions (chat)")
	if reactC.MsgID != msgID {
		t.Fatalf("C reaction push msgID = %d, want %d", reactC.MsgID, msgID)
	}
	if len(reactC.Reactions.Results) != 1 {
		t.Fatalf("C reaction push results = %d, want 1", len(reactC.Reactions.Results))
	}
	emojiC, ok := reactC.Reactions.Results[0].Reaction.(*tg.ReactionEmoji)
	if !ok || emojiC.Emoticon != "\u2764" {
		t.Fatalf("C reaction push emoji = %v, want heart", emojiC)
	}

	// 4. B's getHistory shows the reaction.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  &tg.InputPeerChat{ChatID: chatID},
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history type")
		}
		if len(msgs.Messages) == 0 {
			return errors.New("no messages in history")
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return errors.New("first message is not *tg.Message")
		}
		reactions, ok := m.GetReactions()
		if !ok {
			return errors.New("no reactions on message")
		}
		if len(reactions.Results) != 1 {
			return errors.New("expected 1 reaction")
		}
		return nil
	}); err != nil {
		t.Fatalf("B getHistory with reactions: %v", err)
	}

	// 5. C's getHistory also shows the reaction.
	if err := exec(cCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  &tg.InputPeerChat{ChatID: chatID},
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history type")
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return errors.New("first message is not *tg.Message")
		}
		reactions, ok := m.GetReactions()
		if !ok {
			return errors.New("no reactions on message")
		}
		if len(reactions.Results) != 1 {
			return errors.New("expected 1 reaction")
		}
		return nil
	}); err != nil {
		t.Fatalf("C getHistory with reactions: %v", err)
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
