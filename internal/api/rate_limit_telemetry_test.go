package api_test

import (
	"context"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestRateLimitDenialRecorderFailureIsContained(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	var calls atomic.Int32
	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		if calls.Add(1) > 1 {
			panic("telemetry clock unavailable")
		}
		return start
	})
	api.RecordRateLimitDenialForTest(metrics, "message_send")
}

func TestRateLimitDenialTelemetryFollowsRealHandlerOutcomes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15551297001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	var authKeyID [8]byte
	authKeyID[7] = 1
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
	if err := s.SetPendingUser(ctx, keyID, user.ID); err != nil {
		t.Fatalf("set pending user: %v", err)
	}

	metrics := store.NewNotificationMetrics()
	limit := store.RateLimitConfig{Limit: 1, Window: time.Minute}
	checkPassword := func(addr netip.Addr) error {
		_, err := api.CheckPasswordForTestWithLimitsAndMetrics(s, metrics, authKeyID, addr, limit, limit, &tg.AuthCheckPasswordRequest{
			Password: &tg.InputCheckPasswordSRP{SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256)},
		})
		return err
	}

	// An invalid address is a real FLOOD_WAIT from reserveRateLimitIP. The
	// account reservation is refunded before returning, and only the IP
	// surface is recorded.
	if err := checkPassword(netip.Addr{}); !isFloodWait(err) {
		t.Fatalf("invalid-address checkPassword: expected FLOOD_WAIT, got %v", err)
	}
	want := store.RateLimitDenialSurfaceCounts{CheckPasswordIP: 1}
	got := metrics.Snapshot().RateLimitDenials
	if got.Count != 1 || got.BySurface != want || got.Dropped != 0 {
		t.Fatalf("after returned FLOOD_WAIT: denial snapshot = %+v, want count 1, surfaces %+v, dropped 0", got, want)
	}

	// The refunded account reservation admits the next request. Its invalid SRP
	// challenge is not FLOOD_WAIT, so it must not change the recorder.
	if err := checkPassword(netip.MustParseAddr("192.0.2.101")); err == nil || isFloodWait(err) {
		t.Fatalf("admitted checkPassword: expected non-FLOOD_WAIT proof error, got %v", err)
	}
	got = metrics.Snapshot().RateLimitDenials
	if got.Count != 1 || got.BySurface != want || got.Dropped != 0 {
		t.Fatalf("after admitted request: denial snapshot = %+v, want count 1, surfaces %+v, dropped 0", got, want)
	}

	// A cancelled storage operation reaches the real updateProfile rate-limit
	// check and returns an internal error. Storage failure is not a denial.
	failedCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = api.UpdateProfileForTestWithLimitsAndMetricsContext(failedCtx, s, metrics, user.ID, limit, &tg.AccountUpdateProfileRequest{FirstName: "Updated"})
	if err == nil || isFloodWait(err) {
		t.Fatalf("storage-failed updateProfile: expected non-FLOOD_WAIT error, got %v", err)
	}
	got = metrics.Snapshot().RateLimitDenials
	if got.Count != 1 || got.BySurface != want || got.Dropped != 0 {
		t.Fatalf("after storage failure: denial snapshot = %+v, want count 1, surfaces %+v, dropped 0", got, want)
	}
}
