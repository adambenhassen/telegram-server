package api_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestChannelUpdatesProductionDeliverRecordsOnePushSample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	metrics := store.NewNotificationMetrics()
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver(), metrics)

	alice, err := s.CreateUser(ctx, "+15554127301")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15554127302")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, alice.ID, bob.ID, "hello", 1, 0, 0); err != nil {
		t.Fatalf("persist message: %v", err)
	}

	transport := &fakeTransport{}
	conn := mtproto.NewTestConn(transport, testKey())
	conn.SetOwner(bob.ID)
	if !reg.Add(bob.ID, conn) {
		t.Fatal("registry rejected bob connection")
	}
	t.Cleanup(func() { reg.Remove(bob.ID, conn) })

	_, stop, err := store.StartListener(ctx, dsn,
		updater.Deliver,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
		metrics,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown

	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for listener: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(bob.ID, 10)); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	if !waitSent(transport) {
		t.Fatal("production Deliver did not write the pending update")
	}

	deadline := time.Now().Add(5 * time.Second)
	var snapshot store.NotificationMetricsSnapshot
	for {
		snapshot = metrics.Snapshot()
		if snapshot.Push.SampleCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("push snapshot = %+v, want one sample", snapshot.Push)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot.Channels.Updates != 1 {
		t.Fatalf("valid tg_updates count = %d, want one", snapshot.Channels.Updates)
	}
	if snapshot.Push.Outcomes != (store.PushOutcomeCounts{Success: 1}) {
		t.Fatalf("push outcomes = %+v, want exactly one success", snapshot.Push.Outcomes)
	}
	var bucketSamples int64
	for _, count := range snapshot.Push.LatencyBucketCounts {
		bucketSamples += count
	}
	if bucketSamples != 1 {
		t.Fatalf("push latency buckets = %v, want exactly one sample", snapshot.Push.LatencyBucketCounts)
	}
}
