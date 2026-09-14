package store_test

import (
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestNotificationMetricsRollingWindowAndRestart(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })

	initial := metrics.Snapshot()
	if initial.WindowSeconds != 0 || initial.NotifyCount != 0 || initial.Invalid != 0 {
		t.Fatalf("initial snapshot = %+v, want an empty zero-second window", initial)
	}

	if err := metrics.RecordValidNotification(store.ChannelUpdates); err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordValidNotification(store.ChannelTyping); err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordInvalidNotification(); err != nil {
		t.Fatal(err)
	}

	now = start.Add(10 * time.Second)
	got := metrics.Snapshot()
	if got.WindowSeconds != 10 {
		t.Errorf("window seconds = %v, want 10", got.WindowSeconds)
	}
	if got.NotifyCount != 2 {
		t.Errorf("notify count = %d, want 2", got.NotifyCount)
	}
	if got.RatePerSecond != 0.2 {
		t.Errorf("rate per second = %v, want 0.2", got.RatePerSecond)
	}
	if got.Invalid != 1 {
		t.Errorf("invalid count = %d, want 1", got.Invalid)
	}
	if got.Channels.Updates != 1 || got.Channels.Typing != 1 {
		t.Errorf("channel counts = %+v, want updates=1 and typing=1", got.Channels)
	}

	now = start.Add(3_600 * time.Second)
	if got := metrics.Snapshot(); got.NotifyCount != 0 || got.Invalid != 0 {
		t.Fatalf("events at the one-hour boundary = %+v, want expired", got)
	}

	now = start.Add(3_601 * time.Second)
	got = metrics.Snapshot()
	if got.NotifyCount != 0 || got.Invalid != 0 {
		t.Errorf("expired events = %+v, want zero", got)
	}
	if got.WindowSeconds != 3_600 {
		t.Errorf("capped window seconds = %v, want 3600", got.WindowSeconds)
	}

	restarted := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	if got := restarted.Snapshot(); got.WindowSeconds != 0 || got.NotifyCount != 0 || got.Invalid != 0 {
		t.Errorf("restarted snapshot = %+v, want empty state", got)
	}
}

func TestNotificationMetricsConcurrentExactCounting(t *testing.T) {
	t.Parallel()

	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	const perChannel = 2_000
	channels := []string{
		store.ChannelUpdates,
		store.ChannelTyping,
		store.ChannelEvict,
		store.ChannelPost,
		store.ChannelEncryption,
		store.ChannelStatus,
		store.ChannelEncryptedMsg,
		store.ChannelReactions,
		store.ChannelPinned,
	}

	var wg sync.WaitGroup
	for _, channel := range channels {
		wg.Go(func() {
			for range perChannel {
				if err := metrics.RecordValidNotification(channel); err != nil {
					t.Errorf("record %s: %v", channel, err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range perChannel {
			if err := metrics.RecordInvalidNotification(); err != nil {
				t.Errorf("record invalid: %v", err)
				return
			}
		}
	})
	wg.Wait()

	got := metrics.Snapshot()
	if got.NotifyCount != int64(len(channels)*perChannel) {
		t.Errorf("notify count = %d, want %d", got.NotifyCount, len(channels)*perChannel)
	}
	if got.Invalid != perChannel {
		t.Errorf("invalid count = %d, want %d", got.Invalid, perChannel)
	}
	wantChannels := store.NotificationChannelCounts{
		Updates:      perChannel,
		Typing:       perChannel,
		Evict:        perChannel,
		ChannelPost:  perChannel,
		Encryption:   perChannel,
		Status:       perChannel,
		EncryptedMsg: perChannel,
		Reactions:    perChannel,
		Pinned:       perChannel,
	}
	if got.Channels != wantChannels {
		t.Errorf("fixed channel counts = %+v, want each channel %d", got.Channels, perChannel)
	}
}

func TestNotificationMetricsUnknownChannelIsOnlyInvalid(t *testing.T) {
	t.Parallel()

	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	if err := metrics.RecordValidNotification("arbitrary-attacker-channel"); err == nil {
		t.Fatal("unknown channel was accepted")
	}

	got := metrics.Snapshot()
	if got.NotifyCount != 0 {
		t.Errorf("notify count = %d, want 0", got.NotifyCount)
	}
	if got.Invalid != 1 {
		t.Errorf("invalid count = %d, want 1", got.Invalid)
	}
	if got.Channels != (store.NotificationChannelCounts{}) {
		t.Errorf("unknown channel created a series: %+v", got.Channels)
	}
}
