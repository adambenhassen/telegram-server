package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/telegram/updates/hook"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSmoke(t *testing.T) {
	t.Run("one-to-one", func(t *testing.T) {
		t.Parallel()
		testSmokeOneToOne(t)
	})
	t.Run("saved-messages", func(t *testing.T) {
		t.Parallel()
		testSmokeSavedMessages(t)
	})
}

func testSmokeOneToOne(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneA, phoneB = "+15551046001", "+15551046002"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB)

	a1 := newSmokeClient(t, f, phoneA)
	a2 := newSmokeClient(t, f, phoneA)
	b1 := newSmokeClient(t, f, phoneB)
	b2 := newSmokeClient(t, f, phoneB)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, a1.id, 2, "A")
	waitForDistinctAuthKeys(t, f.ctx, f.registry, b1.id, 2, "B")

	// Seed A's local ID space through a real Saved Messages send, so the two
	// accounts' IDs differ when they receive the same 1:1 message.
	var seedResult tg.UpdatesClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		seedResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "smoke-id-seed", RandomID: 1046001,
		})
		return err
	}); err != nil {
		t.Fatalf("send Saved Messages ID seed: %v", err)
	}
	seed := assertSmokeSendResult(t, seedResult, "smoke-id-seed", 1, 1)
	assertObservedMessage(t, f.ctx, a1.seen, seed.Message, seed.ID, true, a1.id, 1, "A1 ID seed", true)
	assertObservedMessage(t, f.ctx, a2.seen, seed.Message, seed.ID, true, a1.id, 1, "A2 ID seed", true)
	assertObservedMessage(t, f.ctx, a2.push, seed.Message, seed.ID, true, a1.id, 1, "A2 ID seed push")

	var aToBResult tg.UpdatesClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		aToBResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a1.id, b1.id), Message: "a-to-b-smoke", RandomID: 1046002,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	aToB := assertSmokeSendResult(t, aToBResult, "a-to-b-smoke", 2, 2)
	assertObservedMessage(t, f.ctx, a1.seen, aToB.Message, aToB.ID, true, b1.id, 2, "A1 sender echo", true)
	assertObservedMessage(t, f.ctx, a2.seen, aToB.Message, aToB.ID, true, b1.id, 2, "A2 sender echo", true)
	assertObservedMessage(t, f.ctx, b1.seen, "a-to-b-smoke", 1, false, a1.id, 1, "B1 incoming", true)
	assertObservedMessage(t, f.ctx, b2.seen, "a-to-b-smoke", 1, false, a1.id, 1, "B2 incoming", true)
	assertObservedMessage(t, f.ctx, a2.push, aToB.Message, aToB.ID, true, b1.id, 2, "A2 sender echo push")
	assertObservedMessage(t, f.ctx, b1.push, "a-to-b-smoke", 1, false, a1.id, 1, "B1 incoming push")
	assertObservedMessage(t, f.ctx, b2.push, "a-to-b-smoke", 1, false, a1.id, 1, "B2 incoming push")

	var bToAResult tg.UpdatesClass
	if err := b1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		bToAResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(b1.id, a1.id), Message: "b-to-a-smoke", RandomID: 1046003,
		})
		return err
	}); err != nil {
		t.Fatalf("B send: %v", err)
	}
	bToA := assertSmokeSendResult(t, bToAResult, "b-to-a-smoke", 2, 2)
	assertObservedMessage(t, f.ctx, b1.seen, bToA.Message, bToA.ID, true, a1.id, 2, "B1 sender echo", true)
	assertObservedMessage(t, f.ctx, b2.seen, bToA.Message, bToA.ID, true, a1.id, 2, "B2 sender echo", true)
	assertObservedMessage(t, f.ctx, a1.seen, "b-to-a-smoke", 3, false, b1.id, 3, "A1 incoming", true)
	assertObservedMessage(t, f.ctx, a2.seen, "b-to-a-smoke", 3, false, b1.id, 3, "A2 incoming", true)
	assertObservedMessage(t, f.ctx, b2.push, bToA.Message, bToA.ID, true, a1.id, 2, "B2 sender echo push")
	assertObservedMessage(t, f.ctx, a1.push, "b-to-a-smoke", 3, false, b1.id, 3, "A1 incoming push")
	assertObservedMessage(t, f.ctx, a2.push, "b-to-a-smoke", 3, false, b1.id, 3, "A2 incoming push")

	if err := b1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		_, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: peerUser(b1.id, a1.id), MaxID: 1,
		})
		return err
	}); err != nil {
		t.Fatalf("B read A's received message: %v", err)
	}
	assertObservedReadMarker(t, f.ctx, a1.seen, aToB.ID, 4, "A1 read receipt")
	assertObservedReadMarker(t, f.ctx, a2.seen, aToB.ID, 4, "A2 read receipt")
	assertObservedReadMarker(t, f.ctx, a1.push, aToB.ID, 4, "A1 read receipt push")
	assertObservedReadMarker(t, f.ctx, a2.push, aToB.ID, 4, "A2 read receipt push")

	wantA := map[string]smokeHistoryMessage{
		"a-to-b-smoke": {id: 2, out: true},
		"b-to-a-smoke": {id: 3, out: false},
	}
	wantB := map[string]smokeHistoryMessage{
		"a-to-b-smoke": {id: 1, out: false},
		"b-to-a-smoke": {id: 2, out: true},
	}
	for _, check := range []struct {
		client *smokeClient
		peer   int64
		want   map[string]smokeHistoryMessage
	}{
		{client: a1, peer: b1.id, want: wantA},
		{client: a2, peer: b1.id, want: wantA},
		{client: b1, peer: a1.id, want: wantB},
		{client: b2, peer: a1.id, want: wantB},
	} {
		if err := check.client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			return verifySmokeHistory(ctx, api, peerUser(check.client.id, check.peer), check.peer, check.want)
		}); err != nil {
			t.Fatalf("getHistory for %d with peer %d: %v", check.client.id, check.peer, err)
		}
	}

	// Every managed stream and live push stream must contain each message once.
	for _, collector := range []*updateCollector{a1.seen, a2.seen, b1.seen, b2.seen, a1.push, a2.push, b1.push, b2.push} {
		assertNoMessageFor(t, f.ctx, collector.newMsg, "smoke session")
	}

	a1.stopClient(t)
	a2.stopClient(t)
	b1.stopClient(t)
	b2.stopClient(t)
	f.restart(t)

	assertSmokeReconnect(t, f, a1.session, a1.id, a1.id, b1.id, wantA)
	assertSmokeReconnect(t, f, b1.session, b1.id, b1.id, a1.id, wantB)
}

func testSmokeSavedMessages(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phone = "+15551046003"
	seedPhoneUsers(t, f.ctx, f.store, phone)
	client := newSmokeClient(t, f, phone)

	var result tg.UpdatesClass
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		result, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "saved-smoke", RandomID: 1046004,
		})
		return err
	}); err != nil {
		t.Fatalf("send Saved Messages: %v", err)
	}
	saved := assertSmokeSendResult(t, result, "saved-smoke", 1, 1)
	assertObservedMessage(t, f.ctx, client.seen, saved.Message, saved.ID, true, client.id, 1, "Saved Messages sender echo", true)
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		return verifySmokeHistory(ctx, api, &tg.InputPeerSelf{}, client.id, map[string]smokeHistoryMessage{
			"saved-smoke": {id: saved.ID, out: true},
		})
	}); err != nil {
		t.Fatalf("getHistory for Saved Messages: %v", err)
	}
	assertNoMessageFor(t, f.ctx, client.seen.newMsg, "Saved Messages session")
}

type smokeFixture struct {
	ctx      context.Context
	key      *rsa.PrivateKey
	dsn      string
	store    *store.Store
	codes    *multiCodeSink
	dcID     int
	port     int
	registry *mtproto.SessionRegistry
	stop     func()
}

func newSmokeFixture(t *testing.T) *smokeFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	key, err := rsakey.LoadOrGenerate(filepath.Join(t.TempDir(), "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	f := &smokeFixture{ctx: ctx, key: key, dsn: dsn, store: st, codes: newMultiCodeSink(), dcID: 2}
	f.start(t, "127.0.0.1:0")
	return f
}

func (f *smokeFixture) start(t *testing.T, address string) {
	t.Helper()
	ln := mustListen(t, f.ctx, address)
	if _, ok := ln.Addr().(*net.TCPAddr); !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	f.port = tcpPort(t, ln)
	f.registry, f.stop = bootServerWithRegistry(t, f.ctx, f.key, f.dcID, f.store, f.dsn, f.codes.Logger(), ln)
	stop := f.stop
	t.Cleanup(stop)
}

func (f *smokeFixture) restart(t *testing.T) {
	t.Helper()
	f.stop()
	f.start(t, fmt.Sprintf("127.0.0.1:%d", f.port))
}

func (f *smokeFixture) managedClient(sess *session.StorageMemory, seen, push *updateCollector, manager *updates.Manager) *telegram.Client {
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             f.dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: f.dcID, IPAddress: "127.0.0.1", Port: f.port}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &f.key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		SessionStorage: sess,
		UpdateHandler:  observedManagerHandler{observer: push, manager: manager},
		Middlewares: []telegram.Middleware{
			hook.UpdateHook(manager.Handle),
			hook.AffectedHook(manager),
		},
	})
}

func (f *smokeFixture) savedSessionClient(sess *session.StorageMemory) *telegram.Client {
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             f.dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: f.dcID, IPAddress: "127.0.0.1", Port: f.port}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &f.key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		SessionStorage: sess,
	})
}

type smokeClient struct {
	client  *telegram.Client
	session *session.StorageMemory
	manager *updates.Manager
	seen    *updateCollector
	push    *updateCollector
	cmds    chan command
	err     chan error
	id      int64
	stop    sync.Once
}

func newSmokeClient(t *testing.T, f *smokeFixture, phone string) *smokeClient {
	t.Helper()
	sess := &session.StorageMemory{}
	seen, push := newUpdateCollector(), newUpdateCollector()
	manager := updates.New(updates.Config{Handler: seen})
	client := &smokeClient{
		client:  f.managedClient(sess, seen, push, manager),
		session: sess,
		manager: manager,
		seen:    seen,
		push:    push,
		cmds:    make(chan command),
		err:     make(chan error, 1),
	}
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
			return f.codes.wait(ctx, phone)
		})),
		auth.SendCodeOptions{},
	)
	ids, ready := make(chan int64, 1), make(chan struct{}, 1)
	go func() {
		client.err <- runManagedInteractive(f.ctx, client.client, flow, ids, ready, client.cmds, manager, true)
	}()
	t.Cleanup(func() { client.stopClient(t) })
	select {
	case client.id = <-ids:
	case <-f.ctx.Done():
		t.Fatalf("smoke client login timed out: %v", f.ctx.Err())
	}
	select {
	case <-ready:
	case <-f.ctx.Done():
		t.Fatalf("smoke update manager startup timed out: %v", f.ctx.Err())
	}
	return client
}

func (c *smokeClient) call(ctx context.Context, fn func(context.Context, *tg.Client) error) error {
	done := make(chan error, 1)
	select {
	case c.cmds <- command{fn: fn, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *smokeClient) stopClient(t *testing.T) {
	t.Helper()
	c.stop.Do(func() {
		close(c.cmds)
		if err := <-c.err; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("smoke client run: %v", err)
		}
		c.manager.Reset()
	})
}

type smokeSend struct {
	Message string
	ID      int
}

func assertSmokeSendResult(t *testing.T, result tg.UpdatesClass, text string, wantID, wantPts int) smokeSend {
	t.Helper()
	message, pts, ok := outgoingMessage(t, result, text)
	if !ok || countOutgoingMessages(result, text) != 1 {
		t.Fatalf("sendMessage result omitted exactly one outgoing %q", text)
	}
	if message.ID != wantID {
		t.Fatalf("outgoing %q id = %d, want %d", text, message.ID, wantID)
	}
	if pts != wantPts {
		t.Fatalf("outgoing %q pts = %d, want %d", text, pts, wantPts)
	}
	return smokeSend{Message: message.Message, ID: message.ID}
}

type smokeHistoryMessage struct {
	id  int
	out bool
}

func verifySmokeHistory(
	ctx context.Context,
	api *tg.Client,
	peer tg.InputPeerClass,
	peerID int64,
	want map[string]smokeHistoryMessage,
) error {
	result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: 10})
	if err != nil {
		return err
	}
	history, ok := result.(*tg.MessagesMessages)
	if !ok {
		return fmt.Errorf("history response = %T, want *tg.MessagesMessages", result)
	}
	if len(history.Messages) != len(want) {
		return fmt.Errorf("history count = %d, want %d", len(history.Messages), len(want))
	}
	seen := make(map[string]bool, len(want))
	for _, class := range history.Messages {
		message, ok := class.(*tg.Message)
		if !ok {
			return fmt.Errorf("history message = %T, want *tg.Message", class)
		}
		expect, ok := want[message.Message]
		if !ok {
			return fmt.Errorf("history contains unexpected text %q", message.Message)
		}
		if seen[message.Message] {
			return fmt.Errorf("history contains duplicate text %q", message.Message)
		}
		seen[message.Message] = true
		if message.ID != expect.id || message.Out != expect.out {
			return fmt.Errorf("history %q = {id:%d out:%v}, want {id:%d out:%v}", message.Message, message.ID, message.Out, expect.id, expect.out)
		}
		peer, ok := message.PeerID.(*tg.PeerUser)
		if !ok || peer.UserID != peerID {
			return fmt.Errorf("history %q peer = %+v, want user %d", message.Message, message.PeerID, peerID)
		}
	}
	for text := range want {
		if !seen[text] {
			return fmt.Errorf("history is missing %q", text)
		}
	}
	return nil
}

func assertSmokeReconnect(
	t *testing.T,
	f *smokeFixture,
	sess *session.StorageMemory,
	wantUserID, viewerID, peerID int64,
	want map[string]smokeHistoryMessage,
) {
	t.Helper()
	client := f.savedSessionClient(sess)
	if err := client.Run(f.ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized || status.User == nil || status.User.ID != wantUserID {
			return fmt.Errorf("reconnected authorization = %+v, want authorized user %d", status, wantUserID)
		}
		return verifySmokeHistory(ctx, client.API(), peerUser(viewerID, peerID), peerID, want)
	}); err != nil {
		t.Fatalf("reconnect saved session for %d: %v", wantUserID, err)
	}
}
