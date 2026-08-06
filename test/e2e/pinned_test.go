package e2e_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestPinnedChat proves the chat pin lifecycle: A (creator) pins a message,
// B and C receive updatePinnedMessages in real-time; A unpins, members receive
// the push; pinning the same message again is idempotent.
func TestPinnedChat(t *testing.T) {
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

	const phoneA, phoneB, phoneC = "+15551297001", "+15551297002", "+15551297003"

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
			Title: "Pin Test",
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
			Message:  "pin me",
			RandomID: 700001,
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

	// 3. A pins the message.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID},
			ID:   msgID,
		})
		return err
	}); err != nil {
		t.Fatalf("A pin: %v", err)
	}

	// 3b. B receives updatePinnedMessages.
	pinB := recvOr(t, collB.pinnedMsg, "B updatePinnedMessages")
	if !pinB.Pinned {
		t.Fatal("B pin push: Pinned = false, want true")
	}
	peerChat, ok := pinB.Peer.(*tg.PeerChat)
	if !ok || peerChat.ChatID != chatID {
		t.Fatalf("B pin push peer = %T, want *tg.PeerChat with chatID %d", pinB.Peer, chatID)
	}

	// 3c. C receives updatePinnedMessages.
	pinC := recvOr(t, collC.pinnedMsg, "C updatePinnedMessages")
	if !pinC.Pinned {
		t.Fatal("C pin push: Pinned = false, want true")
	}

	// 4. Pinning the same message again is idempotent (no extra push).
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID},
			ID:   msgID,
		})
		return err
	}); err != nil {
		t.Fatalf("A pin (idempotent): %v", err)
	}

	// 5. A unpins.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer:  &tg.InputPeerChat{ChatID: chatID},
			ID:    msgID,
			Unpin: true,
		})
		return err
	}); err != nil {
		t.Fatalf("A unpin: %v", err)
	}

	// 5b. B receives updatePinnedMessages with Pinned=false.
	unpinB := recvOr(t, collB.pinnedMsg, "B updatePinnedMessages (unpin)")
	if unpinB.Pinned {
		t.Fatal("B unpin push: Pinned = true, want false")
	}

	// 5c. C receives updatePinnedMessages with Pinned=false.
	unpinC := recvOr(t, collC.pinnedMsg, "C updatePinnedMessages (unpin)")
	if unpinC.Pinned {
		t.Fatal("C unpin push: Pinned = true, want false")
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

// TestPinnedChannel proves the channel pin lifecycle: A (creator/admin) pins
// a channel post; members receive push; non-admin cannot pin; admin unpins.
func TestPinnedChannel(t *testing.T) {
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

	const phoneA, phoneB = "+15551298001", "+15551298002"

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

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	// 1. A creates a megagroup channel.
	var channelID int64
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "Pin Channel",
			About:     "Test channel",
			Megagroup: true,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("unexpected create channel result")
		}
		ch, ok := ups.Chats[0].(*tg.Channel)
		if !ok {
			return errors.New("not a *tg.Channel")
		}
		channelID = ch.ID
		return nil
	}); err != nil {
		t.Fatalf("createChannel: %v", err)
	}

	// 2. A exports an invite and B joins.
	var inviteHash string
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		inv, err := c.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: peerChannel(aUserID, channelID),
		})
		if err != nil {
			return err
		}
		exp, ok := inv.(*tg.ChatInviteExported)
		if !ok {
			return errors.New("exportChatInvite: unexpected response type")
		}
		inviteHash = strings.TrimPrefix(exp.Link, "https://t.me/+")
		return nil
	}); err != nil {
		t.Fatalf("export invite: %v", err)
	}

	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesImportChatInvite(ctx, inviteHash)
		return err
	}); err != nil {
		t.Fatalf("B join channel: %v", err)
	}

	// 3. A posts a message to the channel.
	var postID int
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, channelID),
			Message:  "pin this post",
			RandomID: 800001,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("unexpected send result")
		}
		for _, u := range ups.Updates {
			if nm, ok := u.(*tg.UpdateNewChannelMessage); ok {
				if m, ok := nm.Message.(*tg.Message); ok {
					postID = m.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A post to channel: %v", err)
	}
	// Drain B's channel message.
	recvOr(t, collB.newChannelMsg, "B newChannelMsg")

	// 4. A pins the post.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: peerChannel(aUserID, channelID),
			ID:   postID,
		})
		return err
	}); err != nil {
		t.Fatalf("A pin channel post: %v", err)
	}

	// 4b. B receives updatePinnedMessages.
	pinB := recvOr(t, collB.pinnedMsg, "B updatePinnedMessages (channel)")
	if !pinB.Pinned {
		t.Fatal("B channel pin push: Pinned = false, want true")
	}
	peerCh, ok := pinB.Peer.(*tg.PeerChannel)
	if !ok || peerCh.ChannelID != channelID {
		t.Fatalf("B channel pin push peer = %T, want *tg.PeerChannel with channelID %d", pinB.Peer, channelID)
	}

	// 5. B (non-admin) tries to pin — should fail with CHAT_ADMIN_REQUIRED.
	var pinErr error
	if execErr := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: peerChannel(bUserID, channelID),
			ID:   postID,
		})
		pinErr = err
		return nil
	}); execErr != nil {
		t.Fatalf("B exec: %v", execErr)
	}
	if pinErr == nil {
		t.Fatal("B should not be able to pin in channel")
	}
	var tgErr *tgerr.Error
	if !errors.As(pinErr, &tgErr) {
		t.Fatalf("B pin error type = %T, want *tgerr.Error", pinErr)
	}
	if tgErr.Message != "CHAT_ADMIN_REQUIRED" {
		t.Fatalf("B pin error = %s, want CHAT_ADMIN_REQUIRED", tgErr.Message)
	}

	// 6. A unpins.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer:  peerChannel(aUserID, channelID),
			ID:    postID,
			Unpin: true,
		})
		return err
	}); err != nil {
		t.Fatalf("A unpin channel post: %v", err)
	}

	// 6b. B receives updatePinnedMessages with Pinned=false.
	unpinB := recvOr(t, collB.pinnedMsg, "B updatePinnedMessages (channel unpin)")
	if unpinB.Pinned {
		t.Fatal("B channel unpin push: Pinned = true, want false")
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
