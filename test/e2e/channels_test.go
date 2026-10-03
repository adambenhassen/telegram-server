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

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/rsakey"
	"github.com/teagramhq/teagram-server/internal/store"
)

const testPublicLinkPrefix = "https://test.example/"

// inviteHash strips the link prefix and returns the bare hash.
func inviteHash(link string) string {
	return strings.TrimPrefix(link, testPublicLinkPrefix+"+")
}

func validInviteHash(hash string) bool {
	if len(hash) != 22 {
		return false
	}
	for i := range len(hash) {
		c := hash[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
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
		var hasPrefix bool
		hash, hasPrefix = strings.CutPrefix(inv.Link, testPublicLinkPrefix+"+")
		if !hasPrefix || !validInviteHash(hash) {
			return fmt.Errorf("exportChatInvite: link %q has no configured prefix or valid hash", inv.Link)
		}
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

func assertChannelRPCErrorPrefix(t *testing.T, ctx context.Context, cmds chan command, prefix string, fn func(ctx context.Context, c *tg.Client) error) {
	t.Helper()
	done := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: done}:
	case <-ctx.Done():
		t.Fatalf("command enqueue timeout: %v", ctx.Err())
	}
	err := <-done
	if err == nil {
		t.Fatalf("expected RPC error with prefix %q, got nil", prefix)
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		t.Fatalf("error type = %T, want *tgerr.Error (%q)", err, prefix)
	}
	if !strings.HasPrefix(tgErr.Message, prefix) {
		t.Fatalf("error = %s, want prefix %q", tgErr.Message, prefix)
	}
}

// testSmokeMegagroupSlowMode exercises slow-mode writes through a real client
// alongside the committed-post cooldown and retry contract.
func testSmokeMegagroupSlowMode(t *testing.T) {
	t.Helper()
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

	const phoneA, phoneB = "+15551295061", "+15551295062"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)
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
	aUserID, bUserID := login(aID, "A"), login(bID, "B")
	channelID := createMegagroup(t, ctx, aCmds, "Smoke slow mode")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, channelID)
	importChannelInvite(t, ctx, bCmds, hash)
	toggleSlowMode := func(ctx context.Context, c *tg.Client, userID int64, seconds int) error {
		result, err := c.ChannelsToggleSlowMode(ctx, &tg.ChannelsToggleSlowModeRequest{
			Channel: inputChannel(userID, channelID),
			Seconds: seconds,
		})
		if err != nil {
			return err
		}
		updates, ok := result.(*tg.Updates)
		if !ok {
			return fmt.Errorf("toggleSlowMode result = %T, want *tg.Updates", result)
		}
		for _, update := range updates.Updates {
			if channel, ok := update.(*tg.UpdateChannel); ok && channel.ChannelID == channelID {
				return nil
			}
		}
		return fmt.Errorf("toggleSlowMode updates omit channel %d", channelID)
	}
	assertSlowModeReadback := func(ctx context.Context, c *tg.Client, userID int64, want int) error {
		result, err := c.ChannelsGetFullChannel(ctx, inputChannel(userID, channelID))
		if err != nil {
			return err
		}
		full, ok := result.FullChat.(*tg.ChannelFull)
		if !ok {
			return fmt.Errorf("getFullChannel response = %T, want *tg.ChannelFull", result.FullChat)
		}
		seconds, ok := full.GetSlowmodeSeconds()
		if !ok || seconds != want {
			return fmt.Errorf("slowmode_seconds = %d present=%v, want %d", seconds, ok, want)
		}
		return nil
	}
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		return toggleSlowMode(ctx, c, aUserID, 10)
	})
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		return assertSlowModeReadback(ctx, c, bUserID, 10)
	})
	assertChannelRPCErrorPrefix(t, ctx, aCmds, "CHAT_NOT_MODIFIED", func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsToggleSlowMode(ctx, &tg.ChannelsToggleSlowModeRequest{
			Channel: inputChannel(aUserID, channelID),
			Seconds: 10,
		})
		return err
	})

	const firstRandomID = 5006101
	var firstID, firstPts int
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerChannel(bUserID, channelID), Message: "first slow-mode post", RandomID: firstRandomID,
		})
		if err != nil {
			return err
		}
		updates, ok := res.(*tg.Updates)
		if !ok {
			return fmt.Errorf("first send result = %T, want *tg.Updates", res)
		}
		for _, update := range updates.Updates {
			post, ok := update.(*tg.UpdateNewChannelMessage)
			if !ok {
				continue
			}
			message, ok := post.Message.(*tg.Message)
			if !ok {
				return fmt.Errorf("first post message = %T, want *tg.Message", post.Message)
			}
			if message.Message != "first slow-mode post" || post.Pts != 1 {
				return fmt.Errorf("first post text/pts = %q/%d, want %q/1", message.Message, post.Pts, "first slow-mode post")
			}
			firstID, firstPts = message.ID, post.Pts
			return nil
		}
		return errors.New("first send result has no UpdateNewChannelMessage")
	})
	if firstID == 0 || firstPts != 1 {
		t.Fatalf("first post identity/pts = %d/%d, want nonzero/1", firstID, firstPts)
	}

	assertChannelRPCErrorPrefix(t, ctx, bCmds, "SLOWMODE_WAIT_", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerChannel(bUserID, channelID), Message: "distinct slow-mode post", RandomID: 5006102,
		})
		return err
	})

	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerChannel(bUserID, channelID), Message: "first slow-mode post", RandomID: firstRandomID,
		})
		if err != nil {
			return fmt.Errorf("retry committed random ID: %w", err)
		}
		updates, ok := res.(*tg.Updates)
		if !ok {
			return fmt.Errorf("retry result = %T, want *tg.Updates", res)
		}
		for _, update := range updates.Updates {
			post, ok := update.(*tg.UpdateNewChannelMessage)
			if !ok {
				continue
			}
			message, ok := post.Message.(*tg.Message)
			if !ok || message.ID != firstID || message.Message != "first slow-mode post" || post.Pts != firstPts {
				return fmt.Errorf("retry post = %T/%v pts=%d, want original id/text/pts %d/%q/%d", post.Message, post.Message, post.Pts, firstID, "first slow-mode post", firstPts)
			}
			return nil
		}
		return errors.New("retry result has no UpdateNewChannelMessage")
	})

	pts, err := st.ChannelState(ctx, channelID)
	if err != nil || pts != 1 {
		t.Fatalf("channel pts after retry = %d err=%v, want 1", pts, err)
	}
	events, err := st.ChannelEventsWindow(ctx, channelID, 0, pts, 10)
	if err != nil || len(events) != 1 || events[0].Pts != firstPts {
		t.Fatalf("channel events after retry = %+v err=%v, want only the first event", events, err)
	}
	history, err := st.ChannelHistory(ctx, channelID, 0, 10)
	if err != nil || len(history) != 1 || history[0].Message != "first slow-mode post" {
		t.Fatalf("channel history after retry = %+v err=%v, want only the original post", history, err)
	}
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		return toggleSlowMode(ctx, c, aUserID, 0)
	})
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		return assertSlowModeReadback(ctx, c, bUserID, 0)
	})

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
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

	const phoneA, phoneB = "+15551295001", "+15551295002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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

	// A creates a broadcast channel and immediately fetches its full info, as the
	// client does while advancing through channel setup.
	var chID int64
	var chAccessHash int64
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		created, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "Lifecycle",
			About:     "Setup details",
			Broadcast: true,
		})
		if err != nil {
			return err
		}
		updates, ok := created.(*tg.Updates)
		if !ok {
			return errors.New("createChannel: unexpected updates type")
		}
		for _, chat := range updates.Chats {
			if channel, ok := chat.(*tg.Channel); ok {
				chID = channel.ID
				chAccessHash = channel.AccessHash
				break
			}
		}
		if chID == 0 {
			return errors.New("createChannel: no channel in response")
		}

		full, err := c.ChannelsGetFullChannel(ctx, &tg.InputChannel{
			ChannelID:  chID,
			AccessHash: chAccessHash,
		})
		if err != nil {
			return err
		}
		channelFull, ok := full.FullChat.(*tg.ChannelFull)
		if !ok {
			return fmt.Errorf("getFullChannel: full info = %T, want *tg.ChannelFull", full.FullChat)
		}
		if channelFull.ID != chID || channelFull.About != "Setup details" {
			return fmt.Errorf("getFullChannel: id/about = %d/%q, want %d/%q", channelFull.ID, channelFull.About, chID, "Setup details")
		}
		if count, ok := channelFull.GetParticipantsCount(); !ok || count != 1 {
			return fmt.Errorf("getFullChannel: participant count = %d present=%v, want 1", count, ok)
		}
		if channelFull.Pts != 0 {
			return fmt.Errorf("getFullChannel: pts = %d, want 0", channelFull.Pts)
		}
		return nil
	})

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

	const phoneA, phoneB = "+15551295011", "+15551295012"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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

// TestChannelsMegagroup proves gate 3: a plain member may send with unrestricted
// defaults, but stored send_plain restrictions block that member while admins
// and the creator remain able to post.
func TestChannelsMegagroup(t *testing.T) {
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

	const phoneA, phoneB, phoneC = "+15551295021", "+15551295022", "+15551295023"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB, phoneC)

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
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")
	login(cID, "C")

	// A creates megagroup, B joins.
	chID := createMegagroup(t, ctx, aCmds, "Megagroup")
	hash := exportChannelInvite(t, ctx, aUserID, aCmds, chID)
	importChannelInvite(t, ctx, bCmds, hash)

	postAndCheckReply := func(userID int64, cmds chan command, text string, randomID int64, wantPts int) {
		t.Helper()
		execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peerChannel(userID, chID), Message: text, RandomID: randomID,
			})
			if err != nil {
				return err
			}
			updates, ok := res.(*tg.Updates)
			if !ok {
				return fmt.Errorf("sendMessage result = %T, want *tg.Updates", res)
			}
			for _, update := range updates.Updates {
				post, ok := update.(*tg.UpdateNewChannelMessage)
				if !ok {
					continue
				}
				message, ok := post.Message.(*tg.Message)
				if !ok {
					return fmt.Errorf("post reply message = %T, want *tg.Message", post.Message)
				}
				if message.Message != text || post.Pts != wantPts {
					return fmt.Errorf("post reply text=%q pts=%d, want %q/%d", message.Message, post.Pts, text, wantPts)
				}
				return nil
			}
			return errors.New("sendMessage result has no UpdateNewChannelMessage")
		})
	}

	// B (role 0) posts with the unrestricted default and gets the post in the RPC reply.
	postAndCheckReply(bUserID, bCmds, "megagroup post by plain member", 5002001, 1)

	// B is promoted and saves SendPolls through the same method the Permissions
	// screen uses. The reply names the channel whose default rights committed.
	if err = st.SetChannelRole(ctx, chID, aUserID, bUserID, 1); err != nil {
		t.Fatalf("promote B: %v", err)
	}
	channelBefore, ok, err := st.ChannelByID(ctx, chID)
	if err != nil || !ok {
		t.Fatalf("read channel before rights save: ok=%v err=%v", ok, err)
	}
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		result, err := c.MessagesEditChatDefaultBannedRights(ctx, &tg.MessagesEditChatDefaultBannedRightsRequest{
			Peer:         peerChannel(bUserID, chID),
			BannedRights: tg.ChatBannedRights{SendPolls: true},
		})
		if err != nil {
			return fmt.Errorf("admin save default rights: %w", err)
		}
		updates, ok := result.(*tg.Updates)
		if !ok || len(updates.Updates) != 1 {
			return fmt.Errorf("default rights result = %T, want Updates with one entry", result)
		}
		updated, ok := updates.Updates[0].(*tg.UpdateChatDefaultBannedRights)
		if !ok {
			return fmt.Errorf("default rights update = %T, want *tg.UpdateChatDefaultBannedRights", updates.Updates[0])
		}
		peer, ok := updated.Peer.(*tg.PeerChannel)
		if !ok || peer.ChannelID != chID {
			return fmt.Errorf("updated peer = %T/%v, want channel %d", updated.Peer, updated.Peer, chID)
		}
		if !updated.DefaultBannedRights.SendPolls || updated.DefaultBannedRights.SendMessages || updated.Version != channelBefore.Version+1 {
			return fmt.Errorf("updated rights/version = %+v/%d, want SendPolls only/version %d", updated.DefaultBannedRights, updated.Version, channelBefore.Version+1)
		}
		return nil
	})

	// A fresh member-only full-channel read is the Permissions reopen path.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		full, err := c.ChannelsGetFullChannel(ctx, inputChannel(bUserID, chID))
		if err != nil {
			return fmt.Errorf("member getFullChannel: %w", err)
		}
		if len(full.Chats) != 1 {
			return fmt.Errorf("member getFullChannel chats = %d, want 1", len(full.Chats))
		}
		channel, ok := full.Chats[0].(*tg.Channel)
		if !ok {
			return fmt.Errorf("member full-channel peer = %T, want *tg.Channel", full.Chats[0])
		}
		rights, ok := channel.GetDefaultBannedRights()
		if !ok || !rights.SendPolls || rights.SendMessages {
			return fmt.Errorf("member getFullChannel default rights = %+v present=%v, want SendPolls only", rights, ok)
		}
		return nil
	})

	// A public username preview is visible to C but carries no private defaults.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsUpdateUsername(ctx, &tg.ChannelsUpdateUsernameRequest{
			Channel: inputChannel(aUserID, chID), Username: "megagroup_rights_preview",
		})
		return err
	})
	execChannel(t, ctx, cCmds, func(ctx context.Context, c *tg.Client) error {
		preview, err := c.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: "megagroup_rights_preview"})
		if err != nil {
			return fmt.Errorf("outsider resolveUsername: %w", err)
		}
		if len(preview.Chats) != 1 {
			return fmt.Errorf("outsider preview chats = %d, want 1", len(preview.Chats))
		}
		channel, ok := preview.Chats[0].(*tg.Channel)
		if !ok || channel.ID != chID {
			return fmt.Errorf("outsider preview peer = %T/%v, want channel %d", preview.Chats[0], preview.Chats[0], chID)
		}
		if rights, ok := channel.GetDefaultBannedRights(); ok {
			return fmt.Errorf("outsider public preview disclosed default rights: %+v", rights)
		}
		return nil
	})

	// Demotion does not erase the committed rights, and a plain member still
	// reads them on the next member-only channel response.
	if err = st.SetChannelRole(ctx, chID, aUserID, bUserID, 0); err != nil {
		t.Fatalf("demote B: %v", err)
	}
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		channelsResult, err := c.ChannelsGetChannels(ctx, []tg.InputChannelClass{inputChannel(bUserID, chID)})
		if err != nil {
			return fmt.Errorf("member getChannels: %w", err)
		}
		channels, ok := channelsResult.(*tg.MessagesChats)
		if !ok {
			return fmt.Errorf("member getChannels result = %T, want *tg.MessagesChats", channelsResult)
		}
		if len(channels.Chats) != 1 {
			return fmt.Errorf("member getChannels chats = %d, want 1", len(channels.Chats))
		}
		channel, ok := channels.Chats[0].(*tg.Channel)
		if !ok {
			return fmt.Errorf("member channel peer = %T, want *tg.Channel", channels.Chats[0])
		}
		rights, ok := channel.GetDefaultBannedRights()
		if !ok || !rights.SendPolls || channel.ID != chID {
			return fmt.Errorf("demoted member rights/id = %+v/%d present=%v, want SendPolls/channel %d", rights, channel.ID, ok, chID)
		}
		return nil
	})

	// Add a text restriction through Save, then prove a denied post changes
	// neither channel state nor membership.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditChatDefaultBannedRights(ctx, &tg.MessagesEditChatDefaultBannedRightsRequest{
			Peer:         peerChannel(aUserID, chID),
			BannedRights: tg.ChatBannedRights{SendPolls: true, SendPlain: true},
		})
		return err
	})
	beforePts, err := st.ChannelState(ctx, chID)
	if err != nil {
		t.Fatalf("channel state before denied post: %v", err)
	}
	beforeEvents, err := st.ChannelEventsWindow(ctx, chID, 0, beforePts, 10)
	if err != nil {
		t.Fatalf("channel events before denied post: %v", err)
	}
	assertChannelRPCError(t, ctx, bCmds, "CHAT_WRITE_FORBIDDEN", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerChannel(bUserID, chID), Message: "blocked member post", RandomID: 5002002,
		})
		return err
	})
	afterPts, err := st.ChannelState(ctx, chID)
	if err != nil || afterPts != beforePts {
		t.Fatalf("channel pts after denied post = %d, err %v; want %d", afterPts, err, beforePts)
	}
	afterEvents, err := st.ChannelEventsWindow(ctx, chID, 0, afterPts, 10)
	if err != nil || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("channel events after denied post = %d, err %v; want %d", len(afterEvents), err, len(beforeEvents))
	}
	member, found, err := st.ChannelMemberOf(ctx, chID, bUserID)
	if err != nil || !found || member.Role != 0 || member.BannedUntil != nil {
		t.Fatalf("member after denied post = %+v, found=%v err=%v; want unchanged role 0", member, found, err)
	}

	if err = st.SetChannelRole(ctx, chID, aUserID, bUserID, 1); err != nil {
		t.Fatalf("promote B: %v", err)
	}
	postAndCheckReply(bUserID, bCmds, "megagroup post by admin", 5002003, beforePts+1)
	postAndCheckReply(aUserID, aCmds, "megagroup post by creator", 5002004, beforePts+2)

	close(aCmds)
	close(bCmds)
	close(cCmds)
	for _, ch := range []chan error{errA, errB, errC} {
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

	const phoneA, phoneC = "+15551295031", "+15551295032"
	seedPhoneUsers(t, ctx, st, phoneA, phoneC)

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

	const phoneA, phoneB, phoneD = "+15551295041", "+15551295042", "+15551295043"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB, phoneD)

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

	const phoneA, phoneB = "+15551295051", "+15551295052"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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
	// The window is the assertion, so it hangs off Background: derived from ctx
	// an already-exhausted parent would return immediately and pass vacuously.
	noCtx, noCancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	ln1 := mustListen(t, ctx, "127.0.0.1:0")
	ln2 := mustListen(t, ctx, "127.0.0.1:0")
	port1 := tcpPort(t, ln1)
	port2 := tcpPort(t, ln2)
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln1))
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln2))

	const phoneA, phoneB = "+15551295061", "+15551295062"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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

func TestChannelsInviteToChannelPushesViewerChannelAndLivePosts(t *testing.T) {
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

	const phoneA, phoneB = "+15551299981", "+15551299982"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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
	defer func() {
		close(aCmds)
		close(bCmds)
		if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client A run: %v", err)
		}
		if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client B run: %v", err)
		}
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
	chID := createBroadcastChannel(t, ctx, aCmds, "Direct invite")

	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsInviteToChannel(ctx, &tg.ChannelsInviteToChannelRequest{
			Channel: inputChannel(aUserID, chID),
			Users:   []tg.InputUserClass{inputUser(aUserID, bUserID)},
		})
		if err != nil {
			return err
		}
		if res == nil || res.Updates == nil {
			return errors.New("inviteToChannel response omitted updates")
		}
		return nil
	})

	var bChannel *tg.Channel
	select {
	case update := <-collB.channelUpdate:
		if update.Update.ChannelID != chID {
			t.Fatalf("B channel update id = %d, want %d", update.Update.ChannelID, chID)
		}
		for _, chat := range update.Chats {
			if ch, ok := chat.(*tg.Channel); ok && ch.ID == chID {
				bChannel = ch
			}
		}
		if bChannel == nil {
			t.Fatalf("B channel update omitted channel %d in its viewer context", chID)
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for its invited channel update: %v", ctx.Err())
	}

	// The hash carried by B's update must be B's own hash: it works for B's
	// getChannels request while A's per-viewer hash does not.
	if bChannel.AccessHash == peerChannel(aUserID, chID).AccessHash {
		t.Fatal("B received A's channel access hash")
	}
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{
			ChannelID: chID, AccessHash: bChannel.AccessHash,
		}})
		if err != nil {
			return err
		}
		chats, ok := res.(*tg.MessagesChats)
		if !ok || !hasChannel(chats.Chats, chID) {
			return fmt.Errorf("B getChannels response = %T, channel %d missing", res, chID)
		}
		return nil
	})

	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "direct invite live post",
			RandomID: 9910001,
		})
		return err
	})
	select {
	case update := <-collB.newChannelMsg:
		if update.Msg.Message != "direct invite live post" || update.Pts != 1 {
			t.Fatalf("B channel post = %q at pts %d, want expected message at pts 1", update.Msg.Message, update.Pts)
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for the post after direct invite: %v", ctx.Err())
	}

	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsLeaveChannel(ctx, inputChannel(bUserID, chID))
		return err
	})
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(aUserID, chID),
			Message:  "post after removal",
			RandomID: 9910002,
		})
		return err
	})
	noCtx, noCancel := context.WithTimeout(context.Background(), 3*time.Second)
	select {
	case <-collB.newChannelMsg:
		t.Error("B should not receive a channel post after leaving")
	case <-noCtx.Done():
	}
	noCancel()
}
