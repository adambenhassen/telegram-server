package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"

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

func TestGetDifferenceServesEventsAfterDialogUnreadBudgetIsExhausted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	sender, err := s.CreateUser(ctx, "+15551234003")
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := s.CreateUser(ctx, "+15551234004")
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	if _, err := s.CreateChannel(ctx, recipient.ID, "Unread aggregation unavailable", "", false); err != nil {
		t.Fatalf("create recipient channel: %v", err)
	}
	dbConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to mark channel summary unavailable: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close(context.Background()) }) //nolint:errcheck // teardown
	if _, err := dbConn.Exec(ctx, `UPDATE channel_post_summary_state SET ready = false WHERE channel_id = (SELECT id FROM channels WHERE creator_id = $1)`, recipient.ID); err != nil {
		t.Fatalf("mark channel summary unavailable: %v", err)
	}
	if _, err := s.State(ctx, recipient.ID); err == nil {
		t.Fatal("getState with an unavailable channel summary succeeded, want an error")
	}
	if _, _, _, _, err := s.SendMessage(ctx, sender.ID, recipient.ID, "missed event", 34003, 0, 0); err != nil {
		t.Fatalf("send message: %v", err)
	}

	h := &handlers{
		store:                        s,
		log:                          slog.New(slog.DiscardHandler),
		peers:                        pgtest.PeerDeriver(),
		now:                          time.Now,
		rateLimitChannelUnreadCounts: store.RateLimitConfig{Limit: 1, Window: time.Minute},
	}
	request := &mtproto.Request{Ctx: ctx, UserID: recipient.ID}
	if err := h.checkChannelUnreadCountRateLimit(request); err != nil {
		t.Fatalf("first dialog aggregation: %v", err)
	}
	if err := h.checkChannelUnreadCountRateLimit(request); !isChannelUnreadFloodWait(err) {
		t.Fatalf("second dialog aggregation = %v, want FLOOD_WAIT", err)
	}

	var buf bin.Buffer
	if err := (&tg.UpdatesGetDifferenceRequest{Pts: 0}).Encode(&buf); err != nil {
		t.Fatalf("encode getDifference request: %v", err)
	}
	result, err := h.handleGetDifference(&mtproto.Request{Ctx: ctx, UserID: recipient.ID, Buf: &buf})
	if err != nil {
		t.Fatalf("getDifference with exhausted dialog budget: %v", err)
	}
	difference, ok := result.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("getDifference result = %T, want *tg.UpdatesDifference", result)
	}
	if len(difference.NewMessages) != 1 {
		t.Fatalf("getDifference messages = %d, want 1: %+v", len(difference.NewMessages), difference.NewMessages)
	}
	message, ok := difference.NewMessages[0].(*tg.Message)
	if !ok || message.Message != "missed event" {
		t.Fatalf("getDifference message = %#v, want missed event", difference.NewMessages[0])
	}
}

func isChannelUnreadFloodWait(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 420
}
