package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// sendStatus creates a 1:1 dialog between from and to by sending a message.
func sendStatus(t *testing.T, s *store.Store, from, to store.User) {
	t.Helper()
	_, _, _, _, err := s.SendMessage(context.Background(), from.ID, to.ID, "ping", 1, 0, 0) //nolint:dogsled // dialog creation only
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
}

func TestDeliverStatusPushesToPartnersOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555300001")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+1555300002")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	sendStatus(t, s, alice, bob)

	// Set alice offline so LastSeenAt is populated.
	if err := s.SetUserStatus(ctx, alice.ID, true); err != nil {
		t.Fatalf("set alice online: %v", err)
	}
	if err := s.SetUserStatus(ctx, alice.ID, false); err != nil {
		t.Fatalf("set alice offline: %v", err)
	}

	// Register connections for both users.
	aliceFT := &fakeTransport{}
	bobFT := &fakeTransport{}
	aliceConn := mtproto.NewTestConn(aliceFT, testKey())
	aliceConn.SetOwner(alice.ID)
	reg.Add(alice.ID, aliceConn)
	t.Cleanup(func() { reg.Remove(alice.ID, aliceConn) })

	bobConn := mtproto.NewTestConn(bobFT, testKey())
	bobConn.SetOwner(bob.ID)
	reg.Add(bob.ID, bobConn)
	t.Cleanup(func() { reg.Remove(bob.ID, bobConn) })

	// Alice goes offline → bob should receive updateUserStatus, alice should not.
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(_ context.Context, userID int64, online bool) {
			updater.DeliverStatus(ctx, userID, online)
		},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, bool) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(alice.ID, false)); err != nil {
		t.Fatalf("notify: %v", err)
	}

	if !waitSent(bobFT) {
		t.Fatal("bob received no push")
	}
	// Alice must NOT have received a push (no self-push).
	if aliceFT.wasSent() {
		t.Fatal("alice received a push — self-push must not happen")
	}

	// Bob's pts watermark must be unchanged (zero-pts transient push).
	if got := bobConn.LastPushedPts(); got != 0 {
		t.Errorf("bob pts = %d, want 0", got)
	}
}

func TestDeliverStatusOnline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555300011")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+1555300012")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	sendStatus(t, s, alice, bob)

	bobFT := &fakeTransport{}
	bobConn := mtproto.NewTestConn(bobFT, testKey())
	bobConn.SetOwner(bob.ID)
	reg.Add(bob.ID, bobConn)
	t.Cleanup(func() { reg.Remove(bob.ID, bobConn) })

	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(_ context.Context, userID int64, online bool) {
			updater.DeliverStatus(ctx, userID, online)
		},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, bool) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(alice.ID, true)); err != nil {
		t.Fatalf("notify: %v", err)
	}

	if !waitSent(bobFT) {
		t.Fatal("bob received no push for online status")
	}
}

func TestDeliverStatusNoPartners(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver())

	alice, err := s.CreateUser(ctx, "+1555300021")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}

	// Alice has no dialogs.
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(_ context.Context, userID int64, online bool) {
			updater.DeliverStatus(ctx, userID, online)
		},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, bool) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	// Must not panic or error.
	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(alice.ID, true)); err != nil {
		t.Fatalf("notify: %v", err)
	}
}

func TestDeliverStatusMalformedPayload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Listener with a status callback that would panic if called with bad data.
	panicOnCall := make(chan struct{})
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) { close(panicOnCall) },
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, bool) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck // teardown

	// Malformed payloads must be dropped.
	for _, payload := range []string{"no-pipe", "abc|def", "|1", "1|"} {
		if err := s.Notify(ctx, store.ChannelStatus, payload); err != nil {
			t.Fatalf("notify %q: %v", payload, err)
		}
	}

	select {
	case <-panicOnCall:
		t.Fatal("status callback fired for malformed payload")
	case <-time.After(200 * time.Millisecond):
		// Correct: no dispatch.
	}
}
