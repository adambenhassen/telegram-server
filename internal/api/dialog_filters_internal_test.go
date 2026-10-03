package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestDialogFilterEntityRangesUseUTF16Boundaries(t *testing.T) {
	t.Parallel()
	title := tg.TextWithEntities{
		Text:     "A😀B",
		Entities: []tg.MessageEntityClass{&tg.MessageEntityCustomEmoji{Offset: 1, Length: 2, DocumentID: 42}},
	}
	if _, err := validateDialogFilterEntities(title); err != nil {
		t.Fatalf("valid astral UTF-16 entity: %v", err)
	}
	title.Entities = []tg.MessageEntityClass{&tg.MessageEntityCustomEmoji{Offset: 2, Length: 1, DocumentID: 42}}
	if _, err := validateDialogFilterEntities(title); !errors.Is(err, errEntityBoundsInvalid) {
		t.Fatalf("surrogate-splitting entity error = %v, want ENTITY_BOUNDS_INVALID", err)
	}
}

func TestDialogFilterMarkerGuardUsesEarlierDateAndIncludesBoundary(t *testing.T) {
	t.Parallel()
	now := time.Unix(10_000, 0)
	if !dialogFilterMarkerWithinGuard(now.Add(-60*time.Second), true, 10_000, now) {
		t.Fatal("marker exactly at the 60-second cutoff was excluded")
	}
	if dialogFilterMarkerWithinGuard(now.Add(-61*time.Second), true, 10_000, now) {
		t.Fatal("marker older than the 60-second cutoff was included")
	}
	if !dialogFilterMarkerWithinGuard(now.Add(-60*time.Second), true, 10_100, now) {
		t.Fatal("future client date was not clamped to server time")
	}
	if !dialogFilterMarkerWithinGuard(now.Add(5*time.Second), true, 10_000, now) {
		t.Fatal("marker within the accepted 5-second skew was excluded")
	}
	if dialogFilterMarkerWithinGuard(now, false, 10_000, now) {
		t.Fatal("missing marker triggered a refresh")
	}
}

func TestDialogFilterRateLimitUsesFixedSixtySecondFloodWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	user, err := s.CreateUser(ctx, "+15551090031")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	h := &handlers{store: s, log: slog.New(slog.DiscardHandler)}
	req := &mtproto.Request{Ctx: ctx, UserID: user.ID}
	for attempt := range 60 {
		if err := h.checkDialogFilterRateLimit(req); err != nil {
			t.Fatalf("mutation attempt %d was denied: %v", attempt+1, err)
		}
	}
	err = h.checkDialogFilterRateLimit(req)
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != 420 || rpc.Message != "FLOOD_WAIT_60" {
		t.Fatalf("61st mutation error = %v, want FLOOD_WAIT_60", err)
	}
}
