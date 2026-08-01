package api_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// fakeTransport is a transport.Conn that records whether Send was called. It
// sits behind a real *mtproto.Conn registered in the SessionRegistry so
// DeliverEncryption exercises the full path: registry lookup → store reload →
// render → PushTo → wire.
type fakeTransport struct {
	mu      sync.Mutex
	sent    bool
	sendErr error
}

func (f *fakeTransport) Send(_ context.Context, _ *bin.Buffer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = true
	return f.sendErr
}

func (f *fakeTransport) Recv(_ context.Context, _ *bin.Buffer) error { return errors.New("unused") }
func (f *fakeTransport) Close() error                                { return nil }

func (f *fakeTransport) wasSent() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
}

func testKey() crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i)
	}
	return raw.WithID()
}

// newConnFor builds a real *mtproto.Conn backed by fakeTransport, owned by
// userID, registered in reg, and returns the conn plus transport for assertions.
func newConnFor(t *testing.T, reg *mtproto.SessionRegistry, userID int64) (*mtproto.Conn, *fakeTransport) {
	t.Helper()
	ft := &fakeTransport{}
	c := mtproto.NewTestConn(ft, testKey())
	c.SetOwner(userID)
	if !reg.Add(userID, c) {
		t.Fatalf("registry rejected conn for user %d", userID)
	}
	t.Cleanup(func() { reg.Remove(userID, c) })
	return c, ft
}

// TestDeliverEncryptionRequested drives DeliverEncryption for the requested
// state and verifies the full push path: row reload, g_a in payload, zero pts.
func TestDeliverEncryptionRequested(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555143901")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+1555143902")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	chat := createRequestedChat(t, s, alice.ID, bob.ID)

	// Register bob's conn so DeliverEncryption has a target.
	conn, ft := newConnFor(t, reg, bob.ID)

	// Start listener to exercise NOTIFY→dispatch→DeliverEncryption chain.
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(_ context.Context, userID, chatID int64) {
			updater.DeliverEncryption(ctx, userID, chatID)
		},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	// Emit encryption NOTIFY.
	if err := s.Notify(ctx, store.ChannelEncryption, store.EncryptionPayload(bob.ID, int64(chat.ID))); err != nil {
		t.Fatalf("notify: %v", err)
	}

	// Give listener time to dispatch.
	time.Sleep(100 * time.Millisecond)

	// Push must have reached the transport.
	if !ft.wasSent() {
		t.Fatal("no push delivered — row reload or render failed")
	}

	// pts watermark must be unchanged (zero-pts push).
	if got := conn.LastPushedPts(); got != 0 {
		t.Errorf("pts = %d, want 0 (zero-pts push must not advance watermark)", got)
	}

	// Verify g_a is present by reloading the row and rendering independently.
	// DeliverEncryption did the reload (proven by push succeeding); this
	// proves the row carries g_a and the renderer includes it.
	chat2, err := s.SecretChatByID(ctx, chat.ID)
	if err != nil {
		t.Fatalf("reload chat: %v", err)
	}
	rendered := api.EncryptedChatFor(chat2, bob.ID)
	req, ok := rendered.(*tg.EncryptedChatRequested)
	if !ok {
		t.Fatalf("rendered type = %T, want *tg.EncryptedChatRequested", rendered)
	}
	if len(req.GA) == 0 {
		t.Error("g_a missing in payload")
	}
}

// TestDeliverEncryptionActive drives DeliverEncryption for the active state.
func TestDeliverEncryptionActive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555143911")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+1555143912")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	chat := createActiveChat(t, s, alice.ID, bob.ID)

	conn, ft := newConnFor(t, reg, alice.ID)

	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(_ context.Context, userID, chatID int64) {
			updater.DeliverEncryption(ctx, userID, chatID)
		},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	if err := s.Notify(ctx, store.ChannelEncryption, store.EncryptionPayload(alice.ID, int64(chat.ID))); err != nil {
		t.Fatalf("notify: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !ft.wasSent() {
		t.Fatal("no push for active state")
	}
	if got := conn.LastPushedPts(); got != 0 {
		t.Errorf("pts = %d, want 0", got)
	}

	// Active → EncryptedChat with key material.
	rendered := api.EncryptedChatFor(chat, alice.ID)
	ec, ok := rendered.(*tg.EncryptedChat)
	if !ok {
		t.Fatalf("rendered type = %T, want *tg.EncryptedChat", rendered)
	}
	if len(ec.GAOrB) == 0 {
		t.Error("g_a_or_b missing in active state")
	}
}

// TestDeliverEncryptionDiscarded drives DeliverEncryption for the discarded state.
func TestDeliverEncryptionDiscarded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555143921")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+1555143922")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	chat := createDiscardedChat(t, s, alice.ID, bob.ID)

	conn, ft := newConnFor(t, reg, bob.ID)

	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(_ context.Context, userID, chatID int64) {
			updater.DeliverEncryption(ctx, userID, chatID)
		},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	if err := s.Notify(ctx, store.ChannelEncryption, store.EncryptionPayload(bob.ID, int64(chat.ID))); err != nil {
		t.Fatalf("notify: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !ft.wasSent() {
		t.Fatal("no push for discarded state")
	}
	if got := conn.LastPushedPts(); got != 0 {
		t.Errorf("pts = %d, want 0", got)
	}

	rendered := api.EncryptedChatFor(chat, bob.ID)
	if _, ok := rendered.(*tg.EncryptedChatDiscarded); !ok {
		t.Fatalf("rendered type = %T, want *tg.EncryptedChatDiscarded", rendered)
	}
}

// TestDeliverEncryptionWrongChannelNeverDispatches proves the NOTIFY channel
// name matters: a notification on the wrong channel is never routed to the
// encryption callback. Uses the same DSN so NOTIFY reaches the listener.
func TestDeliverEncryptionWrongChannelNeverDispatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	encrypted := make(chan struct{}, 1)
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) { encrypted <- struct{}{} },
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	// Emit on the updates channel instead of encryption, on the SAME database.
	if err := s.Notify(ctx, store.ChannelUpdates, "1|1"); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case <-encrypted:
		t.Fatal("encryption callback fired for a non-encryption channel")
	case <-time.After(500 * time.Millisecond):
		// Correct: no dispatch.
	}
}

// createRequestedChat creates a secret chat in the 'requested' state.
func createRequestedChat(t *testing.T, s *store.Store, adminID, participantID int64) store.SecretChat {
	t.Helper()
	ga := make([]byte, 256)
	for i := range ga {
		ga[i] = byte(i % 256)
	}
	gaHash := make([]byte, 32)
	for i := range gaHash {
		gaHash[i] = byte(i % 256)
	}
	chat, _, err := s.CreateSecretChatRequest(context.Background(), adminID, participantID, ga, gaHash, 0)
	if err != nil {
		t.Fatalf("create secret chat: %v", err)
	}
	return chat
}

// createActiveChat creates a secret chat in the 'active' state.
func createActiveChat(t *testing.T, s *store.Store, adminID, participantID int64) store.SecretChat {
	t.Helper()
	chat := createRequestedChat(t, s, adminID, participantID)
	gb := make([]byte, 256)
	for i := range gb {
		gb[i] = byte((i + 1) % 256)
	}
	accepted, err := s.AcceptSecretChat(context.Background(), chat.ID, participantID, gb, 42)
	if err != nil {
		t.Fatalf("accept secret chat: %v", err)
	}
	return accepted
}

// createDiscardedChat creates a secret chat in the 'discarded' state.
func createDiscardedChat(t *testing.T, s *store.Store, adminID, participantID int64) store.SecretChat {
	t.Helper()
	chat := createRequestedChat(t, s, adminID, participantID)
	discarded, err := s.DiscardSecretChat(context.Background(), chat.ID)
	if err != nil {
		t.Fatalf("discard secret chat: %v", err)
	}
	return discarded
}
