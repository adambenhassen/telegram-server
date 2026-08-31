package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// --- per-phone code sink (two clients log in at once) ---

type multiCodeSink struct {
	mu    sync.Mutex
	chans map[string]chan string
}

func newMultiCodeSink() *multiCodeSink { return &multiCodeSink{chans: map[string]chan string{}} }

func (m *multiCodeSink) chFor(phone string) chan string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.chans[phone]
	if !ok {
		ch = make(chan string, 1)
		m.chans[phone] = ch
	}
	return ch
}

func (m *multiCodeSink) wait(ctx context.Context, phone string) (string, error) {
	select {
	case code := <-m.chFor(phone):
		return code, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (m *multiCodeSink) Logger() *slog.Logger                     { return slog.New(m) }
func (m *multiCodeSink) Enabled(context.Context, slog.Level) bool { return true }
func (m *multiCodeSink) WithAttrs([]slog.Attr) slog.Handler       { return m }
func (m *multiCodeSink) WithGroup(string) slog.Handler            { return m }

func (m *multiCodeSink) Handle(_ context.Context, r slog.Record) error {
	var phone, code string
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "phone":
			phone = a.Value.String()
		case "code":
			code = a.Value.String()
		}
		return true
	})
	if phone != "" && code != "" {
		select {
		case m.chFor(phone) <- code:
		default:
		}
	}
	return nil
}

// --- client-side update collector ---

// serviceMsgEnvelope carries a service message received via updateNewMessage,
// together with the Chats list of the envelope it arrived in.
type serviceMsgEnvelope struct {
	svc   *tg.MessageService
	chats []tg.ChatClass
}

type chanMsgUpdate struct {
	Msg *tg.Message
	Pts int
}

type updateCollector struct {
	newMsg        chan *tg.Message
	editMsg       chan *tg.Message
	delMsg        chan []int
	readOutbox    chan int
	typing        chan int64
	serviceMsg    chan serviceMsgEnvelope
	newChannelMsg chan chanMsgUpdate
	userStatus    chan *tg.UpdateUserStatus
	msgReactions  chan *tg.UpdateMessageReactions
	pinnedMsg     chan *tg.UpdatePinnedMessages
	points        chan int
}

func newUpdateCollector() *updateCollector {
	return &updateCollector{
		newMsg:        make(chan *tg.Message, 4),
		editMsg:       make(chan *tg.Message, 4),
		delMsg:        make(chan []int, 4),
		readOutbox:    make(chan int, 4),
		typing:        make(chan int64, 4),
		serviceMsg:    make(chan serviceMsgEnvelope, 4),
		newChannelMsg: make(chan chanMsgUpdate, 4),
		userStatus:    make(chan *tg.UpdateUserStatus, 8),
		msgReactions:  make(chan *tg.UpdateMessageReactions, 8),
		pinnedMsg:     make(chan *tg.UpdatePinnedMessages, 8),
		points:        make(chan int, 8),
	}
}

func (u *updateCollector) Handle(_ context.Context, upd tg.UpdatesClass) error {
	switch t := upd.(type) {
	case *tg.Updates:
		for _, x := range t.Updates {
			u.dispatch(x, t.Chats)
		}
	case *tg.UpdateShort:
		u.dispatch(t.Update, nil)
	}
	return nil
}

func (u *updateCollector) dispatch(x tg.UpdateClass, chats []tg.ChatClass) {
	switch up := x.(type) {
	case *tg.UpdateNewMessage:
		switch m := up.Message.(type) {
		case *tg.Message:
			send(u.newMsg, m)
			send(u.points, up.Pts)
		case *tg.MessageService:
			send(u.serviceMsg, serviceMsgEnvelope{svc: m, chats: chats})
		}
	case *tg.UpdateEditMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			send(u.editMsg, m)
		}
	case *tg.UpdateDeleteMessages:
		send(u.delMsg, up.Messages)
	case *tg.UpdateReadHistoryOutbox:
		send(u.readOutbox, up.MaxID)
	case *tg.UpdateUserTyping:
		send(u.typing, up.UserID)
	case *tg.UpdateNewChannelMessage:
		if m, ok := up.Message.(*tg.Message); ok {
			send(u.newChannelMsg, chanMsgUpdate{Msg: m, Pts: up.Pts})
		}
	case *tg.UpdateUserStatus:
		send(u.userStatus, up)
	case *tg.UpdateMessageReactions:
		send(u.msgReactions, up)
	case *tg.UpdatePinnedMessages:
		send(u.pinnedMsg, up)
	}
}

func tcpPort(t *testing.T, ln net.Listener) int {
	t.Helper()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	return addr.Port
}

func send[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

// recvOrCtx receives one value from ch, bounded only by the test's own context,
// and fails naming what it was waiting for. The bound is deliberately the whole
// test budget and never a sub-deadline: under full parallelism a wait can sit
// runnable but unscheduled for tens of seconds (Postgres template lock, CPU
// contention from other packages), and a fixed constant turns that into a
// failure of whichever wait the scheduler starved rather than of the code.
func recvOrCtx[T any](t *testing.T, ctx context.Context, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		var zero T
		return zero
	}
}

// bootServerWithDelivery boots a server plus its LISTEN/NOTIFY delivery listener,
// wired to the server's session registry, and returns a stop function.
func bootServerWithDelivery(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, dsn string, log *slog.Logger, ln net.Listener) func() {
	t.Helper()
	_, stop := bootServerWithRegistry(t, ctx, key, dcID, st, dsn, log, ln)
	return stop
}

// bootServerWithRegistry is bootServerWithDelivery plus the booted server's
// session registry, for tests that assert which sockets a replica still holds.
func bootServerWithRegistry(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, dsn string, log *slog.Logger, ln net.Listener) (*mtproto.SessionRegistry, func()) {
	t.Helper()
	return bootServerWithLimits(t, ctx, key, dcID, st, dsn, log, ln, config.RateLimitsConfig{})
}

// bootServerWithLimits is bootServerWithRegistry against a chosen rate-limit
// configuration, for the tests whose subject is what the shipped numbers do to
// an ordinary client.
func bootServerWithLimits(
	t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store,
	dsn string, log *slog.Logger, ln net.Listener, rateLimits config.RateLimitsConfig,
) (*mtproto.SessionRegistry, func()) {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	// Sign-in here reads the code off the log, so the gated line must be on.
	blobs := testBlobs(t)
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), rateLimits, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	updater := api.NewUpdater(st, server.Registry(), log, pgtest.PeerDeriver())
	_, stopListener, err := store.StartListener(ctx, dsn, updater.Deliver, updater.DeliverTyping, updater.Evict, updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus, updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned, log)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()

	var once bool
	return server.Registry(), func() {
		if once {
			return
		}
		once = true
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
		if lerr := stopListener(); lerr != nil {
			t.Errorf("listener stop: %v", lerr)
		}
	}
}

// interactiveClient logs in, publishes its self id, then serves commands from
// cmds until the channel closes or ctx ends. Pushed updates reach collector on
// the client's own reader goroutine, independent of the command loop.
type command struct {
	fn   func(ctx context.Context, api *tg.Client) error
	done chan error
}

func runInteractive(ctx context.Context, client *telegram.Client, flow auth.Flow, selfOut chan<- int64, cmds <-chan command) error {
	return client.Run(ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return err
		}
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		selfOut <- self.ID
		for {
			select {
			case <-ctx.Done():
				return nil
			case c, ok := <-cmds:
				if !ok {
					return nil
				}
				c.done <- c.fn(ctx, client.API())
			}
		}
	})
}

func TestMessagingRealtime(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	const phoneA, phoneB = "+15551280001", "+15551280002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

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

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-ctx.Done():
			t.Fatalf("command enqueue timeout: %v", ctx.Err())
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)
	peerA := peerUser(bUserID, aUserID)

	// 1. Real-time proof: A sends, B receives updateNewMessage live.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerB, Message: "hello realtime", RandomID: 12345,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	got := recvOrCtx(t, ctx, collB.newMsg, "B updateNewMessage")
	if got.Message != "hello realtime" {
		t.Fatalf("B received %q, want %q", got.Message, "hello realtime")
	}

	// 2. History on both sides shows the message.
	assertHistory := func(cmds chan command, peer tg.InputPeerClass, who string) {
		if err := exec(cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: 10})
			if err != nil {
				return err
			}
			m, ok := res.(*tg.MessagesMessages)
			if !ok {
				t.Errorf("%s history type = %T", who, res)
				return nil
			}
			if len(m.Messages) != 1 {
				t.Errorf("%s history len = %d, want 1", who, len(m.Messages))
			}
			return nil
		}); err != nil {
			t.Fatalf("%s getHistory: %v", who, err)
		}
	}
	assertHistory(aCmds, peerB, "A")
	assertHistory(bCmds, peerA, "B")

	// 3. B reads → A receives updateReadHistoryOutbox.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{Peer: peerA, MaxID: got.ID})
		return err
	}); err != nil {
		t.Fatalf("B readHistory: %v", err)
	}
	recvOrCtx(t, ctx, collA.readOutbox, "A updateReadHistoryOutbox")

	// 4. A edits → B receives updateEditMessage.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesEditMessage(ctx, &tg.MessagesEditMessageRequest{Peer: peerB, ID: 1, Message: "edited live"})
		return err
	}); err != nil {
		t.Fatalf("A edit: %v", err)
	}
	edited := recvOrCtx(t, ctx, collB.editMsg, "B updateEditMessage")
	if edited.Message != "edited live" {
		t.Fatalf("B edit text = %q", edited.Message)
	}

	// 5. A typing → B receives updateUserTyping.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{Peer: peerB, Action: &tg.SendMessageTypingAction{}})
		return err
	}); err != nil {
		t.Fatalf("A setTyping: %v", err)
	}
	if from := recvOrCtx(t, ctx, collB.typing, "B updateUserTyping"); from != aUserID {
		t.Fatalf("typing from = %d, want %d", from, aUserID)
	}

	// 6. A deletes → B receives updateDeleteMessages.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{Revoke: true, ID: []int{1}})
		return err
	}); err != nil {
		t.Fatalf("A delete: %v", err)
	}
	recvOrCtx(t, ctx, collB.delMsg, "B updateDeleteMessages")

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

// TestMessagingOfflineBackfill proves the getDifference backstop: a message sent
// while the recipient is disconnected is returned by updates.getDifference when
// the recipient reconnects.
func TestMessagingOfflineBackfill(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	sessA, sessB := &session.StorageMemory{}, &session.StorageMemory{}
	newClient := func(sess *session.StorageMemory) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
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
	const phoneA, phoneB = "+15551282001", "+15551282002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

	// Log in B, capture its id, then disconnect (goes offline).
	var bUserID int64
	bClient := newClient(sessB)
	if err := bClient.Run(ctx, func(ctx context.Context) error {
		if err := bClient.Auth().IfNecessary(ctx, flowFor(phoneB)); err != nil {
			return err
		}
		self, err := bClient.Self(ctx)
		if err != nil {
			return err
		}
		bUserID = self.ID
		return nil
	}); err != nil {
		t.Fatalf("B login: %v", err)
	}

	// A logs in and sends to the now-offline B.
	var aUserID int64
	aClient := newClient(sessA)
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		if err := aClient.Auth().IfNecessary(ctx, flowFor(phoneA)); err != nil {
			return err
		}
		self, err := aClient.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		_, err = aClient.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "sent while offline",
			RandomID: 55555,
		})
		return err
	}); err != nil {
		t.Fatalf("A login+send: %v", err)
	}

	// B reconnects and backfills via getDifference from pts 0.
	var diff tg.UpdatesDifferenceClass
	bClient2 := newClient(sessB)
	if err := bClient2.Run(ctx, func(ctx context.Context) error {
		d, err := bClient2.API().UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		diff = d
		return nil
	}); err != nil {
		t.Fatalf("B getDifference: %v", err)
	}

	full, ok := diff.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("difference type = %T, want *tg.UpdatesDifference", diff)
	}
	if len(full.NewMessages) != 1 {
		t.Fatalf("backfill new messages = %d, want 1", len(full.NewMessages))
	}
	m, ok := full.NewMessages[0].(*tg.Message)
	if !ok || m.Message != "sent while offline" {
		t.Fatalf("backfilled message = %+v, want text %q", full.NewMessages[0], "sent while offline")
	}
}

// TestMessagingCrossReplica proves the LISTEN/NOTIFY path crosses processes: two
// servers share one database, A connects to server 1 and B to server 2, and A's
// send reaches B live only because the NOTIFY emitted by server 1 wakes server
// 2's listener, which pushes to B's connection in that process.
func TestMessagingCrossReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	newClient := func(port int, collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
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
	const phoneA, phoneB = "+15551283001", "+15551283002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

	// B stays connected to server 2, collecting pushes.
	collB := newUpdateCollector()
	bCmds := make(chan command)
	bID := make(chan int64, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, newClient(port2, collB), flowFor(phoneB), bID, bCmds)
	}()
	var bUserID int64
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("client B login timeout: %v", ctx.Err())
	}

	// A connects to server 1 and sends to B.
	var aUserID int64
	aClient := newClient(port1, newUpdateCollector())
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		if err := aClient.Auth().IfNecessary(ctx, flowFor(phoneA)); err != nil {
			return err
		}
		self, err := aClient.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		_, err = aClient.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "across replicas",
			RandomID: 77777,
		})
		return err
	}); err != nil {
		t.Fatalf("A login+send: %v", err)
	}

	got := recvOrCtx(t, ctx, collB.newMsg, "B cross-replica updateNewMessage")
	if got.Message != "across replicas" {
		t.Fatalf("B received %q, want %q", got.Message, "across replicas")
	}

	close(bCmds)
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}
