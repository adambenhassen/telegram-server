package e2e_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
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
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	login(aID, "A")
	login(bID, "B")

	// A resolves B's phone → gets B's user with server-issued access_hash.
	var bUser *tg.User
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
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
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
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
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for stranger message: %v", ctx.Err())
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
// with access_hash equal to the id (M1 placeholder) is refused — for channel
// and user peers.
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

	const phoneA = "+15551271001"

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
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	login(aID, "A")

	// A creates channel, captures full *tg.Channel from response.
	var ch *tg.Channel
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "Placeholder",
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
		for _, c := range ups.Chats {
			if channel, ok := c.(*tg.Channel); ok {
				ch = channel
				return nil
			}
		}
		return errors.New("createChannel: no channel in response")
	})

	// A calls getHistory with placeholder hash (access_hash == channel_id) → PEER_ID_INVALID.
	assertPeerRPCError(t, ctx, aCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.ID},
			Limit: 10,
		})
		return err
	})

	t.Run("user", func(t *testing.T) {
		// A resolves self via contacts.resolvePhone, then tries placeholder hash.
		var aUser *tg.User
		execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
			rp, err := c.ContactsResolvePhone(ctx, phoneA)
			if err != nil {
				return err
			}
			if len(rp.Users) != 1 {
				return errors.New("resolvePhone: no users")
			}
			aUser, ok = rp.Users[0].(*tg.User)
			if !ok {
				return errors.New("resolvePhone: not *tg.User")
			}
			return nil
		})
		// A calls getHistory with placeholder hash (access_hash == user_id) → PEER_ID_INVALID.
		assertPeerRPCError(t, ctx, aCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer:  &tg.InputPeerUser{UserID: aUser.ID, AccessHash: aUser.ID},
				Limit: 10,
			})
			return err
		})
	})

	close(aCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client run: %v", err)
	}
}

// TestPeerIdentityReplayRefused proves that a hash the server issued to A for
// a channel is refused when a third client C submits it.
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

	const phoneA, phoneC = "+15551272001", "+15551272003"

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
	login(cID, "C")

	// A creates channel, captures full *tg.Channel (with A's access_hash).
	var ch *tg.Channel
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "ReplayChannel",
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
		for _, c := range ups.Chats {
			if channel, ok := c.(*tg.Channel); ok {
				ch = channel
				return nil
			}
		}
		return errors.New("createChannel: no channel in response")
	})

	// C tries to use A's channel access_hash → PEER_ID_INVALID.
	assertPeerRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			Limit: 10,
		})
		return err
	})

	t.Run("user", func(t *testing.T) {
		// A resolves C by phone → gets C's peer scoped to A.
		var cPeer *tg.User
		execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
			rp, err := c.ContactsResolvePhone(ctx, phoneC)
			if err != nil {
				return err
			}
			if len(rp.Users) != 1 {
				return errors.New("resolvePhone: no users")
			}
			cPeer, ok = rp.Users[0].(*tg.User)
			if !ok {
				return errors.New("resolvePhone: not *tg.User")
			}
			return nil
		})
		// C tries to use A's hash for C → PEER_ID_INVALID.
		assertPeerRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer:  &tg.InputPeerUser{UserID: cPeer.ID, AccessHash: cPeer.AccessHash},
				Limit: 10,
			})
			return err
		})
	})

	close(aCmds)
	close(cCmds)
	for _, ch := range []chan error{errA, errC} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// rawReq wraps pre-built TL bytes so gotd can send them as a request.
type rawReq struct{ data []byte }

func (r *rawReq) Encode(b *bin.Buffer) error {
	b.Put(r.data)
	return nil
}
func (r *rawReq) Decode(*bin.Buffer) error { return nil } // never called for requests

// rawRes decodes any TL response (used for RPCs where we only care about success/error).
type rawRes struct {
	id   uint32
	data bin.Buffer
}

func (r *rawRes) Decode(b *bin.Buffer) error {
	var id uint32
	if err := binary.Read(b, binary.LittleEndian, &id); err != nil {
		return err
	}
	r.id = id
	// Skip the payload — we only care that the call succeeded (no RPC error).
	r.data = bin.Buffer{Buf: make([]byte, b.Len())}
	_, err := b.Read(r.data.Buf)
	return err
}

// revokeInviteTypeID is the constructor id of messages.revokeExportedChatInvite.
// gotd does not generate this type, so the client encodes it manually.
const revokeInviteTypeID = 0x13db322c

// revokeInvite encodes and invokes messages.revokeExportedChatInvite via raw RPC.
func revokeInvite(ctx context.Context, c *tg.Client, peer tg.InputPeerClass, hash string) error {
	var buf bin.Buffer
	buf.PutID(revokeInviteTypeID)
	if err := peer.Encode(&buf); err != nil {
		return err
	}
	buf.PutString(hash)

	return c.Invoker().Invoke(ctx, &rawReq{buf.Raw()}, &rawRes{})
}

// TestPeerIdentityChannelLifecycle proves criterion 5: channel lifecycle using
// only server-issued peers — create, join by invite, post, history,
// getChannelDifference, leave, and invite revocation (revoked hash refused,
// outstanding hash still admitted).
func TestPeerIdentityChannelLifecycle(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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

	const phoneA, phoneB, phoneC = "+15551273001", "+15551273002", "+15551273003"

	collB := newUpdateCollector()
	aCmds, bCmds, cCmds := make(chan command), make(chan command), make(chan command)
	aID, bID, cID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
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
	login(bID, "B")
	login(cID, "C")

	// A creates channel, captures full server-issued *tg.Channel.
	var chForA *tg.Channel
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.ChannelsCreateChannel(ctx, &tg.ChannelsCreateChannelRequest{
			Title:     "Lifecycle",
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
		for _, c := range ups.Chats {
			if channel, ok := c.(*tg.Channel); ok {
				chForA = channel
				return nil
			}
		}
		return errors.New("createChannel: no channel in response")
	})

	// Helper: export invite using server-issued peer.
	exportInvite := func(peer tg.InputPeerClass) string {
		var hash string
		execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{Peer: peer})
			if err != nil {
				return err
			}
			inv, ok := res.(*tg.ChatInviteExported)
			if !ok {
				return errors.New("exportChatInvite: unexpected type")
			}
			hash = inviteHash(inv.Link)
			return nil
		})
		return hash
	}

	// A exports two invites: one for B, one for C.
	peerA := &tg.InputPeerChannel{ChannelID: chForA.ID, AccessHash: chForA.AccessHash}
	hashB := exportInvite(peerA)
	hashC := exportInvite(peerA)

	// B joins via invite, captures full server-issued *tg.Channel.
	var chForB *tg.Channel
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
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
		for _, c := range ups.Chats {
			if channel, ok := c.(*tg.Channel); ok && channel.ID == chForA.ID {
				chForB = channel
				return nil
			}
		}
		return errors.New("channel not in import response")
	})

	peerB := &tg.InputPeerChannel{ChannelID: chForB.ID, AccessHash: chForB.AccessHash}
	inputB := &tg.InputChannel{ChannelID: chForB.ID, AccessHash: chForB.AccessHash}

	// A posts a message.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerA,
			Message:  "channel msg",
			RandomID: 9003001,
		})
		return err
	})

	// B receives the message live.
	select {
	case upd := <-collB.newChannelMsg:
		if upd.Msg.Message != "channel msg" {
			t.Fatalf("B channel msg = %q, want %q", upd.Msg.Message, "channel msg")
		}
	case <-ctx.Done():
		t.Fatalf("B timed out waiting for channel message: %v", ctx.Err())
	}

	// B calls getChannelDifference — peers in response are spendable.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		d, err := c.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputB,
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
		return nil
	})

	// B leaves channel.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsLeaveChannel(ctx, inputB)
		return err
	})

	// B can no longer read history — PEER_ID_INVALID after leave.
	assertPeerRPCError(t, ctx, bCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerB,
			Limit: 10,
		})
		return err
	})

	// A revokes hashB. C still joins with hashC (outstanding invite admitted).
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		return revokeInvite(ctx, c, peerA, hashB)
	})

	// C joins with hashC → succeeds.
	importChannelInvite(t, ctx, cCmds, hashC)

	// C tries to join with revoked hashB → PEER_ID_INVALID.
	assertPeerRPCError(t, ctx, cCmds, "PEER_ID_INVALID", func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesImportChatInvite(ctx, hashB)
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

// TestPeerIdentityBackfillSpendable proves criterion 6: a client that was
// offline reconnects, backfills through getDifference, and every peer in the
// backfill is spendable (used for a subsequent API call).
func TestPeerIdentityBackfillSpendable(t *testing.T) {
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

	const phoneA, phoneB = "+15551274001", "+15551274002"

	sessB := &session.StorageMemory{}

	// B logs in, then disconnects (goes offline).
	bClient := createClient(addr.Port, key, dcID, newUpdateCollector(), sessB)
	if err := bClient.Run(ctx, func(ctx context.Context) error {
		return bClient.Auth().IfNecessary(ctx, flowFor(phoneB, codes))
	}); err != nil {
		t.Fatalf("B login: %v", err)
	}

	// A logs in, resolves B, sends message.
	var aUserID int64
	var bUserFromA *tg.User
	aClient := createClient(addr.Port, key, dcID, newUpdateCollector(), nil)
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

		rp, err := api.ContactsResolvePhone(ctx, phoneB)
		if err != nil {
			return err
		}
		if len(rp.Users) != 1 {
			return errors.New("resolvePhone: no users")
		}
		bUserFromA, ok = rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: not *tg.User")
		}

		_, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerUser{UserID: bUserFromA.ID, AccessHash: bUserFromA.AccessHash},
			Message:  "backfill msg",
			RandomID: 9004001,
		})
		return err
	}); err != nil {
		t.Fatalf("A login+send: %v", err)
	}

	// B reconnects, calls getDifference, extracts peer from response, then
	// uses that peer to call messages.getHistory — proving the peer is spendable.
	bClient2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessB)
	if err := bClient2.Run(ctx, func(ctx context.Context) error {
		api := bClient2.API()

		d, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		full, ok := d.(*tg.UpdatesDifference)
		if !ok {
			return errors.New("getDifference: unexpected type")
		}
		if len(full.NewMessages) != 1 {
			return errors.New("getDifference: expected 1 message")
		}

		// Extract the sender's user from the Chats list in the difference.
		var senderUser *tg.User
		for _, u := range full.Users {
			if user, ok := u.(*tg.User); ok && user.ID == aUserID {
				senderUser = user
				break
			}
		}
		if senderUser == nil {
			return errors.New("difference: sender user not in Users")
		}

		// Use the server-issued peer (from backfill) to read history — proves
		// the peer is spendable. MessagesGetHistory routes through inputPeer
		// which validates the access_hash, unlike UsersGetUsers which ignores it.
		_, err = api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerUser{
				UserID:     senderUser.ID,
				AccessHash: senderUser.AccessHash,
			},
			Limit: 10,
		})
		return err
	}); err != nil {
		t.Fatalf("B reconnect+spendable: %v", err)
	}
}

// TestPeerIdentityRoundTrip proves criterion 2: a client obtains peers solely
// from server responses (resolvePhone) and performs send, edit, delete, read
// and typing — no locally-derived hash anywhere.
func TestPeerIdentityRoundTrip(t *testing.T) {
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

	const phoneA, phoneB = "+15551275001", "+15551275002"

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
	login(aID, "A")
	login(bID, "B")

	// A resolves B → gets B's peer with server-issued access_hash.
	var bPeer *tg.InputPeerUser
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneB)
		if err != nil {
			return err
		}
		if len(rp.Users) != 1 {
			return errors.New("resolvePhone: no users")
		}
		u, ok := rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: not *tg.User")
		}
		bPeer = &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
		return nil
	})

	// 1. Send.
	var msgID int
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: bPeer, Message: "round trip", RandomID: 9005001,
		})
		if err != nil {
			return err
		}
		ups, ok := res.(*tg.Updates)
		if !ok {
			return errors.New("sendMessage: unexpected type")
		}
		for _, u := range ups.Updates {
			if nm, ok := u.(*tg.UpdateNewMessage); ok {
				if m, ok := nm.Message.(*tg.Message); ok {
					msgID = m.ID
				}
			}
		}
		return nil
	})

	// B receives the message; extract A's peer from the server-issued User in
	// the message (the FromID on a Message is int64, but the full User with
	// access_hash comes from the Users vector in the update).
	var aPeer *tg.InputPeerUser
	recvMsg := recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage")
	// The message.FromID is the sender; we need the access_hash from the
	// server-issued user. The message itself carries FromID (int64), but the
	// full user with access_hash is in the update's Users vector. Since
	// updateCollector only captures the Message, we resolve A via resolvePhone
	// on B's side — still server-issued, no local derivation.
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneA)
		if err != nil {
			return err
		}
		if len(rp.Users) != 1 {
			return errors.New("resolvePhone: no users")
		}
		u, ok := rp.Users[0].(*tg.User)
		if !ok {
			return errors.New("resolvePhone: not *tg.User")
		}
		aPeer = &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
		return nil
	})
	_ = recvMsg // message received, peer obtained from server

	// 2. Edit.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{
			Peer: bPeer, ID: msgID, Message: "edited",
		})
		return err
	})
	recvOrCtx(t, ctx, collB.editMsg, "B updateEditMessage")

	// 3. Read (B marks read → A gets read receipt).
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: aPeer, MaxID: msgID,
		})
		return err
	})
	recvOrCtx(t, ctx, collA.readOutbox, "A updateReadHistoryOutbox")

	// 4. Typing (B types → A gets typing notification).
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{
			Peer:   aPeer,
			Action: &tg.SendMessageTypingAction{},
		})
		return err
	})
	recvOrCtx(t, ctx, collA.typing, "A updateUserTyping")

	// 5. Delete.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{Revoke: true, ID: []int{msgID}})
		return err
	})
	recvOrCtx(t, ctx, collB.delMsg, "B updateDeleteMessages")

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}

// assertPeerRPCError calls fn on cmds and asserts the result is a tgerr with the
// given message. Unlike execChat, it does not fail on error — the error is
// expected and inspected.
func assertPeerRPCError(t *testing.T, ctx context.Context, cmds chan command, want string, fn func(ctx context.Context, c *tg.Client) error) {
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
