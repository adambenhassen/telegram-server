package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func openDSN(t *testing.T, dsn string) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func TestNotifyReachesRawListener(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, "LISTEN "+store.ChannelUpdates); err != nil {
		t.Fatalf("listen: %v", err)
	}

	if err := s.Notify(ctx, store.ChannelUpdates, "42"); err != nil {
		t.Fatalf("notify: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait notification: %v", err)
	}
	if n.Payload != "42" {
		t.Fatalf("payload = %q, want 42", n.Payload)
	}
}

func TestStartListenerDispatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	delivered := make(chan int64, 1)
	typed := make(chan [2]int64, 1)
	evicted := make(chan [2]int64, 1)
	_, stop, err := store.StartListener(ctx, dsn,
		func(_ context.Context, userID int64) { delivered <- userID },
		func(_ context.Context, peerID, fromID int64) { typed <- [2]int64{peerID, fromID} },
		func(_ context.Context, userID, authKeyID int64) { evicted <- [2]int64{userID, authKeyID} },
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	if err := s.Notify(ctx, store.ChannelUpdates, "7"); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	select {
	case got := <-delivered:
		if got != 7 {
			t.Fatalf("delivered userID = %d, want 7", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver callback not invoked")
	}

	if err := s.Notify(ctx, store.ChannelTyping, store.TypingPayload(3, 9)); err != nil {
		t.Fatalf("notify typing: %v", err)
	}
	select {
	case got := <-typed:
		if got != [2]int64{3, 9} {
			t.Fatalf("typing = %v, want [3 9]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("typing callback not invoked")
	}

	// A malformed evict payload must be dropped, not widened into a callback with
	// a partially parsed id: the callback closes live sockets.
	if err := s.Notify(ctx, store.ChannelEvict, "not-a-pair"); err != nil {
		t.Fatalf("notify malformed evict: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelEvict, store.EvictPayload(11, -4242)); err != nil {
		t.Fatalf("notify evict: %v", err)
	}
	select {
	case got := <-evicted:
		if got != [2]int64{11, -4242} {
			t.Fatalf("evict = %v, want [11 -4242]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("evict callback not invoked")
	}
}
