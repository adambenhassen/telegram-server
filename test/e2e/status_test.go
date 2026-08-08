package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// bootServerWithStatus mirrors bootServerWithDelivery but also wires
// server.OnStatusChange so connection-lifecycle events call SetUserStatus and
// emit a NOTIFY, enabling the full online/offline push path.
func bootServerWithStatus(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, dsn string, log *slog.Logger, ln net.Listener) func() {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{})
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	server.OnStatusChange(func(ctx context.Context, userID int64, online bool) {
		if err := st.SetUserStatus(ctx, userID, online); err != nil {
			log.Error("set user status", "user_id", userID, "online", online, "err", err)
			return
		}
		if err := st.Notify(ctx, store.ChannelStatus, store.StatusPayload(userID, online)); err != nil {
			log.Error("notify status", "user_id", userID, "err", err)
		}
	})

	updater := api.NewUpdater(st, server.Registry(), log, pgtest.PeerDeriver())
	_, stopListener, err := store.StartListener(ctx, dsn, updater.Deliver, updater.DeliverTyping, updater.Evict, updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus, updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned, log)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, transport.Listen(ln)) }()

	var once bool
	return func() {
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

// recvStatus waits for an updateUserStatus on coll's channel with a 10 s timeout.
func recvStatus(t *testing.T, coll *updateCollector, what string) *tg.UpdateUserStatus {
	t.Helper()
	select {
	case upd := <-coll.userStatus:
		return upd
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// drainStatus empties the userStatus channel without blocking.
func drainStatus(coll *updateCollector) {
	for {
		select {
		case <-coll.userStatus:
		default:
			return
		}
	}
}

// TestStatusOnlineRoundTrip covers AC 1 (connect → online push) and AC 2
// (disconnect → offline push with nonzero WasOnline timestamp).
func TestStatusOnlineRoundTrip(t *testing.T) {
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
	stop := bootServerWithStatus(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15559001001", "+15559001002"
	sessA := &session.StorageMemory{}

	// B stays connected and collects status pushes.
	collB := newUpdateCollector()
	bCmds := make(chan command)
	bID := make(chan int64, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	var bUserID int64
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("B login timeout")
	}

	// Phase 1: A logs in with sessA, establishes a dialog with B, then disconnects.
	var aUserID int64
	clientA1 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	if err := clientA1.Run(ctx, func(ctx context.Context) error {
		if err := clientA1.Auth().IfNecessary(ctx, flowFor(phoneA, codes)); err != nil {
			return err
		}
		self, err := clientA1.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		_, err = clientA1.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "hello status",
			RandomID: 55900001,
		})
		return err
	}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("A first run: %v", err)
	}

	// B received A's message — drain it.
	select {
	case <-collB.newMsg:
	case <-time.After(10 * time.Second):
		t.Fatal("B did not receive A's message")
	}

	// AC 2: A just disconnected — B must receive updateUserStatus{Offline} with nonzero WasOnline.
	// Skip any Online push from A's login that may have arrived before the message was drained.
	var off *tg.UserStatusOffline
	for {
		upd := recvStatus(t, collB, "B updateUserStatus offline")
		if upd.UserID != aUserID {
			continue
		}
		if o, ok := upd.Status.(*tg.UserStatusOffline); ok {
			off = o
			break
		}
	}
	if off.WasOnline == 0 {
		t.Fatal("WasOnline is zero — last_seen_at was not set on disconnect")
	}

	// Phase 2: A reconnects using sessA (no re-auth needed).
	aCtx, aCancel := context.WithCancel(ctx)
	defer aCancel()
	aReady := make(chan struct{})
	errA2 := make(chan error, 1)
	clientA2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	go func() {
		errA2 <- clientA2.Run(aCtx, func(ctx context.Context) error {
			// Session is already valid; binding already fired on the server side.
			close(aReady)
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case <-aReady:
	case <-time.After(20 * time.Second):
		t.Fatal("A reconnect timeout")
	}

	// AC 1: B must receive updateUserStatus{Online} for A.
	onUpd := recvStatus(t, collB, "B updateUserStatus online")
	if onUpd.UserID != aUserID {
		t.Fatalf("online UserID = %d, want %d", onUpd.UserID, aUserID)
	}
	if _, ok := onUpd.Status.(*tg.UserStatusOnline); !ok {
		t.Fatalf("online status type = %T, want *tg.UserStatusOnline", onUpd.Status)
	}

	aCancel()
	if err := <-errA2; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("A reconnect run: %v", err)
	}
	close(bCmds)
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("B run: %v", err)
	}
}

// TestStatusExplicitUpdateStatus covers AC 3: account.updateStatus(offline=true)
// while still connected pushes updateUserStatus{Offline} to dialog partners.
func TestStatusExplicitUpdateStatus(t *testing.T) {
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
	stop := bootServerWithStatus(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15559002001", "+15559002002"

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

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("B login timeout")
	}

	// Establish dialog A→B.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "setup",
			RandomID: 55900101,
		})
		return err
	})
	recvOr(t, collB.newMsg, "B newMsg setup")

	// Drain any status updates from the connection/setup phase.
	drainStatus(collA)
	drainStatus(collB)

	// A calls account.updateStatus(offline=true) while still connected.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.AccountUpdateStatus(ctx, true)
		return err
	})

	// B must receive updateUserStatus{Offline} for A.
	upd := recvStatus(t, collB, "B updateUserStatus offline from updateStatus RPC")
	if upd.UserID != aUserID {
		t.Fatalf("status UserID = %d, want %d", upd.UserID, aUserID)
	}
	if _, ok := upd.Status.(*tg.UserStatusOffline); !ok {
		t.Fatalf("status type = %T, want *tg.UserStatusOffline", upd.Status)
	}

	// A is still connected — prove it by making a subsequent RPC.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.UpdatesGetState(ctx)
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

// TestStatusGetDialogsCarriesOnline covers AC 4: getDialogs returns UserStatusOnline
// for A in the Users vector while A is connected.
func TestStatusGetDialogsCarriesOnline(t *testing.T) {
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
	stop := bootServerWithStatus(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15559003001", "+15559003002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
	}()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("B login timeout")
	}
	_ = bUserID

	// A sends B a message so B has A in their dialog list.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "hello",
			RandomID: 55900201,
		})
		return err
	})

	// B calls getDialogs; A must appear with UserStatusOnline.
	execChat(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      20,
		})
		if err != nil {
			return err
		}
		var users []tg.UserClass
		switch d := res.(type) {
		case *tg.MessagesDialogs:
			users = d.Users
		case *tg.MessagesDialogsSlice:
			users = d.Users
		default:
			t.Errorf("getDialogs type = %T", res)
			return nil
		}
		for _, u := range users {
			user, ok := u.(*tg.User)
			if !ok || user.ID != aUserID {
				continue
			}
			if _, ok := user.Status.(*tg.UserStatusOnline); !ok {
				t.Errorf("A status in getDialogs = %T, want *tg.UserStatusOnline", user.Status)
			}
			return nil
		}
		t.Errorf("A (id=%d) not found in getDialogs Users", aUserID)
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

// TestStatusNeverConnected covers AC 5: a user account created without the
// status lifecycle wired (is_online=false, last_seen_at=NULL) appears as
// UserStatusEmpty to other users — not UserStatusOffline{WasOnline:0}.
func TestStatusNeverConnected(t *testing.T) {
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
	// Boot WITHOUT OnStatusChange so SetUserStatus is never called;
	// C's account row stays at is_online=false, last_seen_at=NULL.
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneB, phoneC = "+15559004001", "+15559004002"

	bCmds := make(chan command)
	bID := make(chan int64, 1)
	errB := make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	select {
	case <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("B login timeout")
	}

	// C logs in (creates the user row via auth.signIn) then immediately disconnects.
	// OnStatusChange is not wired, so SetUserStatus is never called.
	clientC := createClient(addr.Port, key, dcID, newUpdateCollector(), nil)
	if err := clientC.Run(ctx, func(ctx context.Context) error {
		return clientC.Auth().IfNecessary(ctx, flowFor(phoneC, codes))
	}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("C login: %v", err)
	}

	// B resolves C; C's status must be UserStatusEmpty — not UserStatusOffline{WasOnline:0}.
	execChat(t, bCmds, func(ctx context.Context, c *tg.Client) error {
		rp, err := c.ContactsResolvePhone(ctx, phoneC)
		if err != nil {
			return err
		}
		if len(rp.Users) == 0 {
			t.Error("resolvePhone: no users for C")
			return nil
		}
		cUser, ok := rp.Users[0].(*tg.User)
		if !ok {
			t.Errorf("resolvePhone: user type = %T", rp.Users[0])
			return nil
		}
		switch s := cUser.Status.(type) {
		case *tg.UserStatusEmpty:
			// Correct: C has null last_seen_at and is_online=false.
		case *tg.UserStatusRecently:
			// Also acceptable per acceptance criteria.
		case *tg.UserStatusOffline:
			if s.WasOnline == 0 {
				t.Errorf("C status = UserStatusOffline{WasOnline:0} — zero-timestamp bug")
			}
		default:
			t.Errorf("C status = %T, want UserStatusEmpty or UserStatusRecently", cUser.Status)
		}
		return nil
	})

	close(bCmds)
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("B run: %v", err)
	}
}

// TestStatusSelfRecently covers AC 6: users.getUsers for self returns
// UserStatusRecently (Telegram's canonical sentinel — own last-seen not disclosed).
func TestStatusSelfRecently(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	stop := bootServerWithStatus(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA = "+15559005001"
	aCmds := make(chan command)
	aID := make(chan int64, 1)
	errA := make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	var aUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("A login timeout")
	}

	// A calls users.getUsers for itself; status must be UserStatusRecently.
	execChat(t, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
		if err != nil {
			return err
		}
		if len(res) == 0 {
			t.Error("getUsers: empty response")
			return nil
		}
		self, ok := res[0].(*tg.User)
		if !ok {
			t.Errorf("getUsers: type = %T", res[0])
			return nil
		}
		if self.ID != aUserID {
			t.Errorf("getUsers: self ID = %d, want %d", self.ID, aUserID)
		}
		if _, ok := self.Status.(*tg.UserStatusRecently); !ok {
			t.Errorf("self Status = %T, want *tg.UserStatusRecently", self.Status)
		}
		return nil
	})

	close(aCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("A run: %v", err)
	}
}

// TestStatusNoCrossContamination covers AC 7: D, who has no dialog with A,
// must not receive a status push when A goes online.
func TestStatusNoCrossContamination(t *testing.T) {
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
	stop := bootServerWithStatus(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB, phoneD = "+15559007001", "+15559007002", "+15559007004"

	// B has a dialog with A (should receive pushes); D shares no dialog with A.
	collB, collD := newUpdateCollector(), newUpdateCollector()
	bCmds, dCmds := make(chan command), make(chan command)
	bID, dID := make(chan int64, 1), make(chan int64, 1)
	errB, errD := make(chan error, 1), make(chan error, 1)
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, collB, nil), flowFor(phoneB, codes), bID, bCmds)
	}()
	go func() {
		errD <- runInteractive(ctx, createClient(addr.Port, key, dcID, collD, nil), flowFor(phoneD, codes), dID, dCmds)
	}()

	var bUserID int64
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("B login timeout")
	}
	select {
	case <-dID:
	case <-time.After(30 * time.Second):
		t.Fatal("D login timeout")
	}

	// Phase 1: A logs in with sessA, sends B a message (A↔B dialog only), disconnects.
	sessA := &session.StorageMemory{}
	var aUserID int64
	clientA1 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	if err := clientA1.Run(ctx, func(ctx context.Context) error {
		if err := clientA1.Auth().IfNecessary(ctx, flowFor(phoneA, codes)); err != nil {
			return err
		}
		self, err := clientA1.Self(ctx)
		if err != nil {
			return err
		}
		aUserID = self.ID
		_, err = clientA1.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "dialog setup",
			RandomID: 55900701,
		})
		return err
	}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("A first run: %v", err)
	}

	// Drain B's message.
	select {
	case <-collB.newMsg:
	case <-time.After(10 * time.Second):
		t.Fatal("B did not receive setup message")
	}
	// Drain any Online push from A's login, then wait for the Offline push
	// (from A's disconnect). The ordering of Online vs the setup message is
	// not guaranteed, so skip Online updates until Offline arrives.
	for {
		upd := recvStatus(t, collB, "B updateUserStatus offline (setup)")
		if _, ok := upd.Status.(*tg.UserStatusOffline); ok {
			break
		}
	}
	drainStatus(collD)

	// Phase 2: A reconnects — B must get Online; D must not receive anything for A.
	aCtx, aCancel := context.WithCancel(ctx)
	defer aCancel()
	aReady := make(chan struct{})
	errA2 := make(chan error, 1)
	clientA2 := createClient(addr.Port, key, dcID, newUpdateCollector(), sessA)
	go func() {
		errA2 <- clientA2.Run(aCtx, func(ctx context.Context) error {
			close(aReady)
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case <-aReady:
	case <-time.After(20 * time.Second):
		t.Fatal("A reconnect timeout")
	}

	// B must receive Online push for A — proves the push system fired.
	onUpd := recvStatus(t, collB, "B updateUserStatus online for A")
	if _, ok := onUpd.Status.(*tg.UserStatusOnline); !ok {
		t.Errorf("B status = %T, want *tg.UserStatusOnline", onUpd.Status)
	}
	if onUpd.UserID != aUserID {
		t.Errorf("B status UserID = %d, want %d", onUpd.UserID, aUserID)
	}

	// D must not receive any status push for A (2 s grace window).
	select {
	case upd := <-collD.userStatus:
		if upd.UserID == aUserID {
			t.Errorf("D received status push for A (cross-contamination): %T", upd.Status)
		}
	case <-time.After(2 * time.Second):
		// Correct: D received nothing for A.
	}

	aCancel()
	if err := <-errA2; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("A reconnect run: %v", err)
	}
	close(bCmds)
	close(dCmds)
	for _, ch := range []chan error{errB, errD} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}
