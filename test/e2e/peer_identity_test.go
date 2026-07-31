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

// TestPeerIdentityStrangerStart proves the stranger-start flow: two clients
// with no prior contact. A resolves B via contacts.resolvePhone using B's
// phone number (known out of band), sends B a message using only the peer
// returned by the server, and B receives it live. No hand-built access hash
// appears in the test.
func TestPeerIdentityStrangerStart(t *testing.T) {
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

	const phoneA, phoneB = "+15551270001", "+15551270002"

	collB := newUpdateCollector()
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
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
		case <-time.After(30 * time.Second):
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	login(aID, "A")
	login(bID, "B")

	// A resolves B's phone → gets B's user with server-issued access_hash.
	var bUser *tg.User
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneB)
		if err != nil {
			return err
		}
		if len(rp.Users) != 1 {
			return errors.New("resolvePhone: no users in response")
		}
		bUser, ok = rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: user not *tg.User")
		}
		return nil
	})

	// A sends message to B using only the peer from resolvePhone (no derived
	// hash — the server's access_hash on bUser is used directly).
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerUser{UserID: bUser.ID, AccessHash: bUser.AccessHash},
			Message:  "stranger hello",
			RandomID: 900001,
		})
		return err
	})

	// B receives the message live.
	select {
	case m := <-collB.newMsg:
		if m.Message != "stranger hello" {
			t.Fatalf("B received %q, want %q", m.Message, "stranger hello")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("B timed out waiting for stranger message")
	}

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestPeerIdentityPlaceholderRefused proves that a client constructing a peer
// with access_hash equal to the id (M1 placeholder) is refused — for both user
// and channel peers.
func TestPeerIdentityPlaceholderRefused(t *testing.T) {
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
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15551271001", "+15551271002"

	collB := newUpdateCollector()
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
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
		case <-time.After(30 * time.Second):
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	login(bID, "B")

	// A resolves B so A has a valid peer for B too (proves placeholder is the
	// only thing being tested, not "unknown peer").
	var bUser *tg.User
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneB)
		if err != nil {
			return err
		}
		bUser, ok = rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: not *tg.User")
		}
		return nil
	})

	// A sends with placeholder hash (access_hash == user_id) → PEER_ID_INVALID.
	assertPeerRPCError(t, aCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerUser{UserID: bUser.ID, AccessHash: bUser.ID},
			Message:  "placeholder",
			RandomID: 900101,
		})
		return err
	})

	// A creates channel, exports invite, B joins.
	chID := createBroadcastChannel(t, aCmds, "Placeholder")
	hash := exportChannelInvite(t, aUserID, aCmds, chID)
	importChannelInvite(t, bCmds, hash)

	// B sends with placeholder hash (access_hash == channel_id) → PEER_ID_INVALID.
	assertPeerRPCError(t, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerChannel{ChannelID: chID, AccessHash: chID},
			Message:  "placeholder channel",
			RandomID: 900102,
		})
		return err
	})

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestPeerIdentityReplayRefused proves that a hash the server issued to A for
// peer B is refused when a third client C submits it.
func TestPeerIdentityReplayRefused(t *testing.T) {
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
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB, phoneC = "+15551272001", "+15551272002", "+15551272003"

	aCmds, bCmds, cCmds := make(chan command), make(chan command), make(chan command)
	aID, bID, cID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	go func() {
		errC <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneC, codes), cID, cCmds)
	}()

	login := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-time.After(30 * time.Second):
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	login(bID, "B")
	_ = login(cID, "C")

	// A resolves B → gets B's user with A's access_hash for B.
	var bUserForA *tg.User
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneB)
		if err != nil {
			return err
		}
		bUserForA, ok = rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: not *tg.User")
		}
		return nil
	})

	// C tries to send to B using A's access_hash for B → PEER_ID_INVALID.
	assertPeerRPCError(t, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerUser{UserID: bUserForA.ID, AccessHash: bUserForA.AccessHash},
			Message:  "replay",
			RandomID: 900201,
		})
		return err
	})

	// Channel replay: A creates channel, exports invite for B; B joins.
	// Then C tries to use B's access_hash for the channel (issued to B) → refused.
	chID := createBroadcastChannel(t, aCmds, "ReplayChannel")

	// A exports invite for B.
	hashB := exportChannelInvite(t, aUserID, aCmds, chID)

	// B joins via invite; the response contains the channel with B's access_hash.
	var chForB *tg.Channel
	execChannel(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesImportChatInvite(ctx, hashB)
		if err != nil {
			return err
		}
		ok, joined := res.(*tg.MessagesChatInviteJoinResultOk)
		if !joined {
			return errors.New("importChatInvite: unexpected response type")
		}
		ups, isUps := ok.Updates.(*tg.Updates)
		if !isUps {
			return errors.New("importChatInvite: unexpected Updates type")
		}
		for _, ch := range ups.Chats {
			if channel, ok := ch.(*tg.Channel); ok && channel.ID == chID {
				chForB = channel
				return nil
			}
		}
		return errors.New("channel not in import response")
	})

	// C tries to use B's channel access_hash → PEER_ID_INVALID.
	assertPeerRPCError(t, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  &tg.InputPeerChannel{ChannelID: chID, AccessHash: chForB.AccessHash},
			Limit: 10,
		})
		return err
	})

	close(aCmds)
	close(bCmds)
	close(cCmds)
	for _, ch := range []chan error{errA, errB, errC} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// assertPeerRPCError calls fn on cmds and asserts the result is a tgerr with the
// given message. Unlike execChat, it does not fail on error — the error is
// expected and inspected.
func assertPeerRPCError(t *testing.T, cmds chan command, want string, fn func(ctx context.Context, c *tg.Client) error) {
	t.Helper()
	done := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: done}:
	case <-time.After(10 * time.Second):
		t.Fatal("command enqueue timeout")
	}
	err := <-done
	if err == nil {
		t.Fatalf("expected %s, got nil", want)
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		t.Fatalf("error type = %T, want *tgerr.Error (%s)", err, want)
	}
	if tgErr.Message != want {
		t.Fatalf("error = %s, want %s", tgErr.Message, want)
	}
}
