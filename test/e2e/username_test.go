package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// assertUsernameRPCError calls fn on cmds and asserts the result is a tgerr
// with the given message. It does not call t.Fatal on the fn error so the
// expected rejection can be inspected.
func assertUsernameRPCError(t *testing.T, cmds chan command, want string, fn func(ctx context.Context, c *tg.Client) error) {
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

// --- tests ---

// TestUsernameSetAndResolveUser proves the full username lifecycle for a user:
// set, resolve (case-insensitive), clear, then resolve returns USERNAME_NOT_OCCUPIED.
func TestUsernameSetAndResolveUser(t *testing.T) {
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

	const phoneA, phoneB = "+15551270001", "+15551270002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
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
	_ = login(bID, "B")

	// A sets username "alice".
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "alice")
		return err
	})

	// B resolves "alice" → receives A's user peer with a valid access_hash.
	execChat(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "alice"})
		if err != nil {
			return err
		}
		if len(res.Users) != 1 {
			return errors.New("resolveUsername: no users in response")
		}
		u, ok := res.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolveUsername: users[0] is not *tg.User")
		}
		if u.ID != aUserID {
			return errors.New("resolveUsername: user id mismatch")
		}
		if u.AccessHash == 0 {
			return errors.New("resolveUsername: access_hash is zero")
		}
		return nil
	})

	// B resolves "Alice" → same result (case-insensitive).
	execChat(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "Alice"})
		if err != nil {
			return err
		}
		if len(res.Users) != 1 {
			return errors.New("resolveUsername: no users in response")
		}
		u, ok := res.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolveUsername: users[0] is not *tg.User")
		}
		if u.ID != aUserID {
			return errors.New("resolveUsername: user id mismatch (case-insensitive)")
		}
		return nil
	})

	// A clears the username.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "")
		return err
	})

	// B resolves "alice" → returns USERNAME_NOT_OCCUPIED.
	assertUsernameRPCError(t, bCmds, "USERNAME_NOT_OCCUPIED", func(ctx context.Context, c *tg.Client) error {
		_, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "alice"})
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

// TestUsernameUniqueness proves username claim exclusivity: B cannot claim a
// username A already holds (case-insensitive), but can after A clears it.
func TestUsernameUniqueness(t *testing.T) {
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

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
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
	_ = login(aID, "A")
	_ = login(bID, "B")

	// A sets username "taken".
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "taken")
		return err
	})

	// B attempts "Taken" → USERNAME_OCCUPIED (case-insensitive).
	assertUsernameRPCError(t, bCmds, "USERNAME_OCCUPIED", func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "Taken")
		return err
	})

	// A clears the username.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "")
		return err
	})

	// B sets "taken" → succeeds.
	execChat(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateUsername(ctx, "taken")
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

// TestPublicChannelJoinByUsername proves the full public channel lifecycle:
// create, set username, resolve, join, receive posts, and getDialogs.
func TestPublicChannelJoinByUsername(t *testing.T) {
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

	const phoneA, phoneB = "+15551272001", "+15551272002"

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
	bUserID := login(bID, "B")

	// A creates a broadcast channel.
	chID := createBroadcastChannel(t, aCmds, "Public Channel")

	// A sets the channel username via channels.updateUsername.
	execChannel(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsUpdateUsername(ctx, &tg.ChannelsUpdateUsernameRequest{
			Channel:  inputChannel(aUserID, chID),
			Username: "publicchan",
		})
		return err
	})

	// B resolves "publicchan" → receives the channel peer.
	var resolvedChID int64
	execChannel(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "publicchan"})
		if err != nil {
			return err
		}
		if len(res.Chats) != 1 {
			return errors.New("resolveUsername: no chats in response")
		}
		ch, ok := res.Chats[0].(*tg.Channel)
		if !ok {
			return errors.New("resolveUsername: chats[0] is not *tg.Channel")
		}
		resolvedChID = ch.ID
		if ch.ID != chID {
			return errors.New("resolveUsername: channel id mismatch")
		}
		return nil
	})

	// B calls channels.joinChannel → succeeds and becomes a member.
	execChannel(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsJoinChannel(ctx, inputChannel(bUserID, resolvedChID))
		return err
	})

	// A posts a message to the channel.
	execChannel(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "hello public channel",
			RandomID: 6000001,
		})
		return err
	})

	// B receives UpdateNewChannelMessage with the correct text.
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "hello public channel" {
			t.Fatalf("B channel msg = %q, want %q", upd.Msg.Message, "hello public channel")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("B timed out waiting for channel message")
	}

	// B calls getChannelDifference from join_pts → receives the new post.
	execChannel(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(bUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   10,
		})
		if err != nil {
			return err
		}
		diff, ok := d.(*tg.UpdatesChannelDifference)
		if !ok {
			return errors.New("getChannelDifference: unexpected type")
		}
		if len(diff.NewMessages) != 1 {
			return errors.New("getChannelDifference: expected 1 message")
		}
		msg, ok := diff.NewMessages[0].(*tg.Message)
		if !ok {
			return errors.New("getChannelDifference: message is not *tg.Message")
		}
		if msg.Message != "hello public channel" {
			return errors.New("getChannelDifference: message text mismatch")
		}
		return nil
	})

	// B's getDialogs includes the channel.
	execChannel(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      20,
		})
		if err != nil {
			return err
		}
		var dialogs []tg.DialogClass
		switch d := res.(type) {
		case *tg.MessagesDialogs:
			dialogs = d.Dialogs
		case *tg.MessagesDialogsSlice:
			dialogs = d.Dialogs
		default:
			return errors.New("getDialogs: unexpected type")
		}
		for _, d := range dialogs {
			dlg, ok := d.(*tg.Dialog)
			if !ok {
				continue
			}
			peerCh, ok := dlg.Peer.(*tg.PeerChannel)
			if !ok || peerCh.ChannelID != chID {
				continue
			}
			return nil // found
		}
		return errors.New("getDialogs: channel not found")
	})

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestPrivateChannelRefusesDirectJoin proves that a channel without a username
// cannot be joined via channels.joinChannel — it returns PEER_ID_INVALID.
func TestPrivateChannelRefusesDirectJoin(t *testing.T) {
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

	const phoneA, phoneB = "+15551273001", "+15551273002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
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
	_ = login(aID, "A")
	bUserID := login(bID, "B")

	// A creates a broadcast channel with no username set.
	chID := createBroadcastChannel(t, aCmds, "Private Channel")

	// B attempts channels.joinChannel → PEER_ID_INVALID (no username = private).
	assertChannelRPCError(t, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsJoinChannel(ctx, inputChannel(bUserID, chID))
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

// TestResolveUsernameRateLimit proves that 20 distinct username lookups succeed
// but the 21st returns FLOOD_WAIT.
func TestResolveUsernameRateLimit(t *testing.T) {
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

	const phoneA = "+15551274001"

	aCmds := make(chan command)
	aID := make(chan int64, 1)
	errA := make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
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
	_ = login(aID, "A")

	// 20 distinct username lookups — all USERNAME_NOT_OCCUPIED but succeed.
	for i := range 20 {
		username := fmt.Sprintf("nobody_%d", i)
		assertUsernameRPCError(t, aCmds, "USERNAME_NOT_OCCUPIED", func(ctx context.Context, c *tg.Client) error {
			_, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: username})
			return err
		})
	}

	// 21st distinct lookup → rate-limit error.
	assertUsernameRPCError(t, aCmds, "FLOOD_WAIT", func(ctx context.Context, c *tg.Client) error {
		_, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "nobody_21"})
		return err
	})

	close(aCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
}
