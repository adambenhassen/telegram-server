package store_test

import (
	"context"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestListenerRoutesOnlyOwnerFromDialogFilterNotification(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner, err := s.CreateUser(ctx, "+15551092001")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	got := make(chan int64, 1)
	noInt := func(context.Context, int64) {}
	listener, stop, err := store.StartListenerWithDialogFilters(
		loopCtx,
		dsn,
		noInt,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		noInt,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		func(_ context.Context, id int64) { got <- id },
		func(context.Context, int64) {},
		func() {},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatalf("start dialog filter listener: %v", err)
	}
	_ = listener
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("stop listener: %v", err)
		}
	})

	if err := s.Notify(ctx, store.ChannelDialogFilters, "not-an-owner"); err != nil {
		t.Fatalf("send malformed dialog filter notification: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelDialogFilters, strconv.FormatInt(owner.ID, 10)); err != nil {
		t.Fatalf("send dialog filter notification: %v", err)
	}
	select {
	case gotOwner := <-got:
		if gotOwner != owner.ID {
			t.Fatalf("dialog filter notification routed owner %d, want %d", gotOwner, owner.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dialog filter owner notification was not delivered")
	}
	select {
	case duplicate := <-got:
		t.Fatalf("unexpected extra dialog filter invalidation for owner %d", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
}
