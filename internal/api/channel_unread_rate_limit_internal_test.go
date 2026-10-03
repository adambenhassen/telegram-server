package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestChannelUnreadAggregationLimitIsSharedAcrossAccountSessions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	firstStore, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() {
		if err := firstStore.Close(); err != nil {
			t.Errorf("close first store: %v", err)
		}
	})
	secondStore, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() {
		if err := secondStore.Close(); err != nil {
			t.Errorf("close second store: %v", err)
		}
	})

	owner, err := firstStore.CreateUser(ctx, "+15551234001")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	other, err := secondStore.CreateUser(ctx, "+15551234002")
	if err != nil {
		t.Fatalf("create other owner: %v", err)
	}
	limit := store.RateLimitConfig{Limit: 1, Window: time.Minute}
	firstSession := &handlers{store: firstStore, log: slog.New(slog.DiscardHandler), rateLimitChannelUnreadCounts: limit}
	secondSession := &handlers{store: secondStore, log: slog.New(slog.DiscardHandler), rateLimitChannelUnreadCounts: limit}
	ownerRequest := &mtproto.Request{Ctx: ctx, UserID: owner.ID}
	if err := firstSession.checkChannelUnreadCountRateLimit(ownerRequest); err != nil {
		t.Fatalf("first session aggregation: %v", err)
	}
	if err := secondSession.checkChannelUnreadCountRateLimit(ownerRequest); !isChannelUnreadFloodWait(err) {
		t.Fatalf("second session aggregation = %v, want FLOOD_WAIT", err)
	}
	if err := secondSession.checkChannelUnreadCountRateLimit(&mtproto.Request{Ctx: ctx, UserID: other.ID}); err != nil {
		t.Fatalf("separate account aggregation: %v", err)
	}
}

func isChannelUnreadFloodWait(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 420
}
