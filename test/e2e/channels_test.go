package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// inviteHash strips the link prefix and returns the bare hash.
func inviteHash(link string) string {
	return strings.TrimPrefix(link, "https://t.me/+")
}

// hasChannel reports whether chats contains a *tg.Channel with the given id.
func hasChannel(chats []tg.ChatClass, id int64) bool {
	for _, c := range chats {
		if ch, ok := c.(*tg.Channel); ok && ch.ID == id {
			return true
		}
	}
	return false
}

// execChannel runs fn on cmds, failing the test on error. It is execChat
// renamed for clarity in tests where a chat-specific name would be misleading,
// but the mechanics are identical — execChat is defined in chats_test.go.
func execChannel(t *testing.T, ctx context.Context, cmds chan command, fn func(ctx context.Context, c *tg.Client) error) {
	t.Helper()
	execChat(t, ctx, cmds, fn)
}

// createBroadcastChannel creates a broadcast channel as the caller and
// returns its id.
func createBroadcastChannel(t *testing.T, ctx context.Context, cmds chan command, title string) int64 {
	t.Helper()
	return doCreateChannel(t, ctx, cmds, title, false)
}

// createMegagroup creates a megagroup channel as the caller and returns its id.
func createMegagroup(t *testing.T, ctx context.Context, cmds chan command, title string) int64 {
	t.Helper()
	return doCreateChannel(t, ctx, cmds, title, true)
}

func doCreateChannel(t *testing.T, ctx context.Context, cmds chan command, title string, megagroup bool) int64 {
	t.Helper()
	var chID int64
	execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     title,
			About:     "",
			Broadcast: !megagroup,
			Megagroup: megagroup,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("createChannel: unexpected updates type")
		}
		for _, ch := range ups.Chats {
			if channel, ok := ch.(*tg.Channel); ok {
				chID = channel.ID
				return nil
			}
		}
		return errors.New("createChannel: no channel in response")
	})
	return chID
}

// exportChannelInvite exports an invite for chID and returns the bare hash.
func exportChannelInvite(t *testing.T, ctx context.Context, viewerID int64, cmds chan command, chID int64) string {
	t.Helper()
	var hash string
	execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: peerChannel(viewerID, chID),
		})
		if err != nil {
			return err
		}
		inv, ok := res.(*tg.ChatInviteExported)
		if !ok {
			return errors.New("exportChatInvite: unexpected response type")
		}
		hash = inviteHash(inv.Link)
		return nil
	})
	return hash
}

// importChannelInvite joins via hash and returns the joined channel id.
func importChannelInvite(t *testing.T, ctx context.Context, cmds chan command, hash string) int64 {
	t.Helper()
	var chID int64
	execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesImportChatInvite(ctx, hash)
		if err != nil {
			return err
		}
		// Server returns Updates wrapped in MessagesChatInviteJoinResultOk.
		ok, joined := res.(*tg.MessagesChatInviteJoinResultOk)
		if !joined {
			return errors.New("importChatInvite: unexpected response type")
		}
		ups, isUps := ok.Updates.(*tg.Updates)
		if !isUps {
			return errors.New("importChatInvite: unexpected Updates type")
		}
		for _, u := range ups.Updates {
			if uc, ok2 := u.(*tg.UpdateChannel); ok2 {
				chID = uc.ChannelID
				return nil
			}
		}
		return errors.New("importChatInvite: no UpdateChannel in response")
	})
	return chID
}

// assertChannelRPCError calls fn on cmds and asserts the result is a tgerr
// with the given message. It does not call t.Fatal on the fn error so the
// expected rejection can be inspected.
func assertChannelRPCError(t *testing.T, ctx context.Context, cmds chan command, want string, fn func(ctx context.Context, c *tg.Client) error) {
	t.Helper()
	done := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: done}:
	case <-ctx.Done():
		t.Fatalf("command enqueue timeout: %v", ctx.Err())
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

// TestChannelsLifecycle proves gate 1: A creates a broadcast channel, exports
// an invite, B imports it and is a member, A posts and B receives
// UpdateNewChannelMessage live with the correct text and Pts=1.
func TestChannelsLifecycle(t *testing.T) {
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

	const phoneA, phoneB = "+15551295001", "+15551295002"

	collA, collB := newUpdateCollector(), newUpdateCollector()
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, collA, nil), flowFor(phoneA, codes), aID, aCmds)
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

	// A creates a broadcast channel.
	chID := createBroadcastChannel(t, ctx, aCmds, "Lifecycle")

	// A exports an invite; B imports it.
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	// B is a member: channels.getChannels returns a *tg.Channel for chID.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsGetChannels(ctx, []tg.InputChannelClass{
			inputChannel(bUserID, chID),
		})
		if err != nil {
			return err
		}
		chats, ok := res.(*tg.MessagesChats)
		if !ok {
			return errors.New("getChannels: unexpected response type")
		}
		if !hasChannel(chats.Chats, chID) {
			return errors.New("getChannels: channel not in response")
		}
		return nil
	})

	// A posts a message.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "hello channel",
			RandomID: 5000001,
		})
		return err
	})

	// B receives UpdateNewChannelMessage with the correct text and Pts=1.
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "hello channel" {
			t.Fatalf("B channel msg = %q, want %q", upd.Msg.Message, "hello channel")
		}
		peer, ok := upd.Msg.PeerID.(*tg.PeerChannel)
		if !ok {
			t.Fatalf("B peer = %T, want *tg.PeerChannel", upd.Msg.PeerID)
		}
		if peer.ChannelID != chID {
			t.Fatalf("B peer channelID = %d, want %d", peer.ChannelID, chID)
		}
		if upd.Pts != 1 {
			t.Fatalf("B UpdateNewChannelMessage Pts = %d, want 1", upd.Pts)
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for channel message: %v", ctx.Err())
	}

	// The sender receives UpdateNewChannelMessage in the RPC reply. Verify Pts=1.
	// Drain A's own copy from newChannelMsg (sent when A's command loop processed
	// the RPC response that arrived as an *tg.Updates).
	//
	// The sender-side UpdateNewChannelMessage is in the RPC reply, not pushed
	// asynchronously, so it may already be in the buffer by now. A separate
	// execChannel command reads back the channel pts to assert.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(aUserID, chID),
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
		if diff.Pts != 1 {
			return errors.New("channel pts after one post != 1")
		}
		if len(diff.NewMessages) != 1 {
			return errors.New("channel difference: expected 1 message")
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

// TestChannelsBroadcastWriteBoundary proves gate 2: B (role 0) sending to a
// broadcast channel is refused with PEER_ID_INVALID; after A promotes B via
// editAdmin B's send succeeds.
func TestChannelsBroadcastWriteBoundary(t *testing.T) {
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

	const phoneA, phoneB = "+15551295011", "+15551295012"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	collA, collB := newUpdateCollector(), newUpdateCollector()
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, collA, nil), flowFor(phoneA, codes), aID, aCmds)
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

	// A creates broadcast channel, B joins.
	chID := createBroadcastChannel(t, ctx, aCmds, "Broadcast")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	// B (role 0) sends → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(bUserID, chID),
			Message:  "B send before promotion",
			RandomID: 5001001,
		})
		return err
	})

	// A promotes B to admin.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsEditAdmin(ctx, &tg.ChannelsEditAdminRequest{
			Channel: inputChannel(aUserID, chID),
			UserID:  inputUser(aUserID, bUserID),
			AdminRights: tg.ChatAdminRights{
				PostMessages: true,
			},
			Rank: "",
		})
		return err
	})

	// B sends after promotion → succeeds.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(bUserID, chID),
			Message:  "B send after promotion",
			RandomID: 5001002,
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

// TestChannelsMegagroup proves gate 3: in a megagroup a plain member (role 0)
// may send without any admin promotion.
func TestChannelsMegagroup(t *testing.T) {
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

	const phoneA, phoneB = "+15551295021", "+15551295022"

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
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")

	// A creates megagroup, B joins.
	chID := createMegagroup(t, ctx, aCmds, "Megagroup")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	// B (role 0) sends to megagroup → succeeds.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(bUserID, chID),
			Message:  "megagroup post by plain member",
			RandomID: 5002001,
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

// TestChannelsAdmissionIsInvite proves gate 4: C, holding only the channel's
// (id, access_hash) pair and no invite, is refused on getHistory, getMessages
// and getChannelDifference — all three with PEER_ID_INVALID.
func TestChannelsAdmissionIsInvite(t *testing.T) {
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

	const phoneA, phoneC = "+15551295031", "+15551295032"

	aCmds, cCmds := make(chan command), make(chan command)
	aID, cID := make(chan int64, 1), make(chan int64, 1)
	errA, errC := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errC <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneC, codes), cID, cCmds)
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
	login(aID, "A")
	cUserID := login(cID, "C")

	// A creates channel. C learns the id through the test variable — no invite.
	chID := createBroadcastChannel(t, ctx, aCmds, "Private")

	// C: getHistory → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(cUserID, chID),
			Limit: 10,
		})
		return err
	})

	// C: channels.getMessages → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: inputChannel(cUserID, chID),
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: 1}},
		})
		return err
	})

	// C: getChannelDifference → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(cUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   10,
		})
		return err
	})

	close(aCmds)
	close(cCmds)
	for _, ch := range []chan error{errA, errC} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestChannelsOfflineBackfill proves gate 5:
//   - B disconnects, A posts twice; B reconnects and getChannelDifference
//     returns both posts with Final=true.
//   - D joins fresh; its first getChannelDifference is empty (join_pts floor)
//     while getHistory returns both posts (the deliberate asymmetry).
func TestChannelsOfflineBackfill(t *testing.T) {
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

	const phoneA, phoneB, phoneD = "+15551295041", "+15551295042", "+15551295043"

	sessA := &session.StorageMemory{}
	sessB := &session.StorageMemory{}

	// A creates channel, exports invite.
	var (
		aUserID int64
		bUserID int64
		chID    int64
		hashB   string
	)
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

		res, err := api.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "Backfill",
			About:     "",
			Broadcast: true,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("createChannel: unexpected updates type")
		}
		for _, ch := range ups.Chats {
			if channel, ok := ch.(*tg.Channel); ok {
				chID = channel.ID
				break
			}
		}
		if chID == 0 {
			return errors.New("createChannel: no channel id")
		}

		inv, err := api.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: peerChannel(aUserID, chID),
		})
		if err != nil {
			return err
		}
		exported, ok := inv.(*tg.ChatInviteExported)
		if !ok {
			return errors.New("exportChatInvite: unexpected type")
		}
		hashB = inviteHash(exported.Link)
		return nil
	}); err != nil {
		t.Fatalf("A setup: %v", err)
	}

	// B joins via invite, then disconnects.
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
		_, err = bClient.API().MessagesImportChatInvite(ctx, hashB)
		return err
	}); err != nil {
		t.Fatalf("B join: %v", err)
	}

	// A reconnects, posts twice.
	aClient2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	if err := aClient2.Run(ctx, func(ctx context.Context) error {
		api := aClient2.API()
		for i, text := range []string{"post 1", "post 2"} {
			if _, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer:     peerChannel(aUserID, chID),
				Message:  text,
				RandomID: 5003000 + int64(i),
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A post: %v", err)
	}

	// B reconnects and calls getChannelDifference from pts 0.
	var bDiff tg.UpdatesChannelDifferenceClass
	bClient2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessB)
	if err := bClient2.Run(ctx, func(ctx context.Context) error {
		d, err := bClient2.API().UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(bUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   100,
		})
		if err != nil {
			return err
		}
		bDiff = d
		return nil
	}); err != nil {
		t.Fatalf("B getDifference: %v", err)
	}

	full, ok := bDiff.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("B difference type = %T, want *tg.UpdatesChannelDifference", bDiff)
	}
	if !full.Final {
		t.Fatal("B difference Final = false, want true")
	}
	if len(full.NewMessages) != 2 {
		t.Fatalf("B backfill messages = %d, want 2", len(full.NewMessages))
	}
	wantTexts := []string{"post 1", "post 2"}
	for i, m := range full.NewMessages {
		msg, ok := m.(*tg.Message)
		if !ok {
			t.Fatalf("B backfill message %d type = %T", i, m)
		}
		if msg.Message != wantTexts[i] {
			t.Fatalf("B backfill message %d = %q, want %q", i, msg.Message, wantTexts[i])
		}
	}

	// A exports a second invite for D.
	var hashD string
	aClient3 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	if err := aClient3.Run(ctx, func(ctx context.Context) error {
		inv, err := aClient3.API().MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: peerChannel(aUserID, chID),
		})
		if err != nil {
			return err
		}
		exported, ok := inv.(*tg.ChatInviteExported)
		if !ok {
			return errors.New("exportChatInvite: unexpected type")
		}
		hashD = inviteHash(exported.Link)
		return nil
	}); err != nil {
		t.Fatalf("A export for D: %v", err)
	}

	// D joins fresh and calls getDifference then getHistory in one session.
	var (
		dDiffEmpty   bool
		dHistoryMsgs []tg.MessageClass
		dUserID      int64
	)
	dClient := createClient(addr.Port, key, dcID, newUpdateCollector(), nil)
	if err := dClient.Run(ctx, func(ctx context.Context) error {
		if err := dClient.Auth().IfNecessary(ctx, flowFor(phoneD, codes)); err != nil {
			return err
		}
		self, err := dClient.Self(ctx)
		if err != nil {
			return err
		}
		dUserID = self.ID
		api := dClient.API()

		// D imports invite (join_pts is set to current channel pts = 2).
		if _, err := api.MessagesImportChatInvite(ctx, hashD); err != nil {
			return err
		}

		// D calls getDifference from pts 0; join_pts floor clamps it to 2 = currentPts.
		d, err := api.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(dUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   100,
		})
		if err != nil {
			return err
		}
		switch dd := d.(type) {
		case *tg.UpdatesChannelDifferenceEmpty:
			dDiffEmpty = dd.Final
		case *tg.UpdatesChannelDifference:
			dDiffEmpty = len(dd.NewMessages) == 0 && dd.Final
		}

		// D calls getHistory — full history is available to members regardless of join_pts.
		hist, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(dUserID, chID),
			Limit: 100,
		})
		if err != nil {
			return err
		}
		switch h := hist.(type) {
		case *tg.MessagesChannelMessages:
			dHistoryMsgs = h.Messages
		default:
			return errors.New("getHistory: unexpected type")
		}
		return nil
	}); err != nil {
		t.Fatalf("D session: %v", err)
	}

	if !dDiffEmpty {
		t.Fatal("D getDifference should be empty (join_pts floor), but got messages")
	}
	if len(dHistoryMsgs) != 2 {
		t.Fatalf("D getHistory = %d messages, want 2", len(dHistoryMsgs))
	}
}

// TestChannelsBan proves gate 6: A bans B; B's getHistory and
// getChannelDifference both fail with PEER_ID_INVALID, B receives no further
// live posts; unban restores getHistory and getChannelDifference.
//
// The media assertion from gate 6 (LOCATION_INVALID on a previously accessible
// file after ban) is omitted: messages.sendMedia returns PEER_ID_INVALID for
// channel peers (internal/api/media.go:141), so no channel media send path
// exists in M7 and the gate cannot be written.
func TestChannelsBan(t *testing.T) {
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

	const phoneA, phoneB = "+15551295051", "+15551295052"

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
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")

	// A creates broadcast channel. B joins via invite.
	chID := createBroadcastChannel(t, ctx, aCmds, "BanTest")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	// A posts "live 1"; B receives it to confirm live delivery works pre-ban.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "live 1",
			RandomID: 5004001,
		})
		return err
	})
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "live 1" {
			t.Fatalf("B pre-ban msg = %q, want %q", upd.Msg.Message, "live 1")
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for live 1: %v", ctx.Err())
	}

	// A bans B permanently.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsEditBanned(ctx, &tg.ChannelsEditBannedRequest{
			Channel:     inputChannel(aUserID, chID),
			Participant: peerUser(aUserID, bUserID),
			BannedRights: tg.ChatBannedRights{
				ViewMessages: true,
				UntilDate:    0,
			},
		})
		return err
	})

	// B: getHistory → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(bUserID, chID),
			Limit: 10,
		})
		return err
	})

	// B: getChannelDifference → PEER_ID_INVALID.
	assertChannelRPCError(t, ctx, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel(bUserID, chID),
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     0,
			Limit:   10,
		})
		return err
	})

	// A posts "live 2"; B should NOT receive it (banned).
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "live 2",
			RandomID: 5004002,
		})
		return err
	})
	noCtx, noCancel := context.WithTimeout(ctx, 3*time.Second)
	select {
	case <-collB.newChannelMsg:
		t.Error("B should not receive channel message after ban")
	case <-noCtx.Done():
	}
	noCancel()

	// A unbans B (zero BannedRights).
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsEditBanned(ctx, &tg.ChannelsEditBannedRequest{
			Channel:      inputChannel(aUserID, chID),
			Participant:  peerUser(aUserID, bUserID),
			BannedRights: tg.ChatBannedRights{},
		})
		return err
	})

	// B: getHistory → succeeds.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(bUserID, chID),
			Limit: 10,
		})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesChannelMessages)
		if !ok {
			return errors.New("getHistory after unban: unexpected type")
		}
		if len(msgs.Messages) == 0 {
			return errors.New("getHistory after unban: no messages")
		}
		return nil
	})

	// B: getChannelDifference → UpdatesChannelDifference with Final=true
	// and the messages posted before/during the ban (access restored after unban).
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
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
			return fmt.Errorf("getChannelDifference after unban: got %T, want *tg.UpdatesChannelDifference", d)
		}
		if !diff.Final {
			return errors.New("getChannelDifference after unban: Final != true")
		}
		return nil
	})

	// A posts "live 3"; B receives it (unban restored delivery).
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "live 3",
			RandomID: 5004003,
		})
		return err
	})
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "live 3" {
			t.Fatalf("B post-unban msg = %q, want %q", upd.Msg.Message, "live 3")
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for live 3 after unban: %v", ctx.Err())
	}

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// TestChannelsCrossReplica proves gate 7: two servers share one database, A
// connects to server 1 and B to server 2. A posts to a channel; B receives the
// UpdateNewChannelMessage over LISTEN/NOTIFY.
func TestChannelsCrossReplica(t *testing.T) {
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
	ln1 := mustListen(t, ctx, "127.0.0.1:0")
	ln2 := mustListen(t, ctx, "127.0.0.1:0")
	port1 := tcpPort(t, ln1)
	port2 := tcpPort(t, ln2)
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln1))
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln2))

	const phoneA, phoneB = "+15551295061", "+15551295062"

	sessA := &session.StorageMemory{}

	// B connects to server 2 and collects channel pushes.
	collB := newUpdateCollector()
	bCmds := make(chan command)
	bID := make(chan int64, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, createClient(port2, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	select {
	case <-bID:
	case <-ctx.Done():
		t.Fatalf("B login timeout: %v", ctx.Err())
	}

	// A connects to server 1, creates channel, exports invite; B imports on server 2.
	var (
		aUserID int64
		chID    int64
		hashB   string
	)
	aClient := createClient(port1, key, dcID, newUpdateCollector(), sessA)
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

		res, err := api.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "CrossReplica",
			About:     "",
			Broadcast: true,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("createChannel: unexpected updates type")
		}
		for _, ch := range ups.Chats {
			if channel, ok := ch.(*tg.Channel); ok {
				chID = channel.ID
				break
			}
		}
		if chID == 0 {
			return errors.New("createChannel: no channel id")
		}

		inv, err := api.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: peerChannel(aUserID, chID),
		})
		if err != nil {
			return err
		}
		exported, ok := inv.(*tg.ChatInviteExported)
		if !ok {
			return errors.New("exportChatInvite: unexpected type")
		}
		hashB = inviteHash(exported.Link)
		return nil
	}); err != nil {
		t.Fatalf("A setup: %v", err)
	}

	// B imports invite on server 2.
	importChannelInvite(t, ctx, bCmds, hashB)

	// A reconnects to server 1, posts a message.
	aClient2 := createClient(port1, key, dcID, newUpdateCollector(), sessA)
	if err := aClient2.Run(ctx, func(ctx context.Context) error {
		_, err := aClient2.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "cross replica channel",
			RandomID: 5005001,
		})
		return err
	}); err != nil {
		t.Fatalf("A post: %v", err)
	}

	// B (on server 2) receives the channel message over LISTEN/NOTIFY.
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "cross replica channel" {
			t.Fatalf("B received %q, want %q", upd.Msg.Message, "cross replica channel")
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for cross-replica channel message: %v", ctx.Err())
	}

	close(bCmds)
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}
