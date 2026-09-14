package store_test

import (
	"sync"
	"sync/atomic"
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

func TestNotificationMetricsRateLimitDenialsUseFixedSurfaces(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	surfaces := []string{
		"message_send",
		"create_chat",
		"add_chat_user",
		"create_channel",
		"messages_search",
		"contacts_search",
		"messages_search_global",
		"save_file_part",
		"upload_get_file",
		"send_code_ip_calls",
		"send_code_ip_distinct_numbers",
		"sign_in_fail_ip",
		"check_password",
		"check_password_ip",
		"get_password_ip",
		"sign_up_ip",
		"password_proof",
		"get_password",
		"update_profile",
	}

	initial := metrics.Snapshot().RateLimitDenials
	if initial.Count != 0 || initial.RatePerSecond != 0 || initial.Dropped != 0 {
		t.Fatalf("initial rate-limit denial snapshot = %+v, want empty zero-rate state", initial)
	}
	for _, surface := range surfaces {
		metrics.RecordRateLimitDenial(surface)
	}
	metrics.RecordRateLimitDenial("attacker-controlled-surface")

	got := metrics.Snapshot().RateLimitDenials
	if got.Count != int64(len(surfaces)) {
		t.Errorf("denial count = %d, want %d fixed-surface denials", got.Count, len(surfaces))
	}
	if got.Dropped != 1 {
		t.Errorf("dropped denials = %d, want 1", got.Dropped)
	}
	if got.RatePerSecond != 0 {
		t.Errorf("zero-second denial rate = %v, want 0", got.RatePerSecond)
	}
	want := store.RateLimitDenialSurfaceCounts{
		MessageSend:               1,
		CreateChat:                1,
		AddChatUser:               1,
		CreateChannel:             1,
		MessagesSearch:            1,
		ContactsSearch:            1,
		MessagesSearchGlobal:      1,
		SaveFilePart:              1,
		UploadGetFile:             1,
		SendCodeIPCalls:           1,
		SendCodeIPDistinctNumbers: 1,
		SignInFailIP:              1,
		CheckPassword:             1,
		CheckPasswordIP:           1,
		GetPasswordIP:             1,
		SignUpIP:                  1,
		PasswordProof:             1,
		GetPassword:               1,
		UpdateProfile:             1,
	}
	if got.BySurface != want {
		t.Errorf("fixed denial surfaces = %+v, want %+v", got.BySurface, want)
	}

	now = start.Add(3_600 * time.Second)
	got = metrics.Snapshot().RateLimitDenials
	if got.Count != 0 || got.Dropped != 0 || got.BySurface != (store.RateLimitDenialSurfaceCounts{}) {
		t.Errorf("expired rate-limit denials = %+v, want zero", got)
	}
	if got.WindowSeconds != 3_600 || got.RatePerSecond != 0 {
		t.Errorf("expired denial window/rate = %v/%v, want 3600/0", got.WindowSeconds, got.RatePerSecond)
	}
}

func TestNotificationMetricsConcurrentRateLimitDenials(t *testing.T) {
	t.Parallel()

	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	const workers = 8
	const attempts = 1_000
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for attempt := range attempts {
				surface := "message_send"
				if (worker+attempt)%2 == 0 {
					surface = "password_proof"
				}
				metrics.RecordRateLimitDenial(surface)
			}
		})
	}
	wg.Wait()

	got := metrics.Snapshot().RateLimitDenials
	if got.Count != workers*attempts {
		t.Fatalf("concurrent denial count = %d, want %d", got.Count, workers*attempts)
	}
	if got.BySurface.MessageSend+got.BySurface.PasswordProof != got.Count {
		t.Errorf("concurrent denial breakdown = %+v, want aggregate %d", got.BySurface, got.Count)
	}
}

func TestNotificationMetricsPushOutcomesAndPercentiles(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })

	acceptedAt := now
	now = start.Add(24 * time.Millisecond)
	metrics.RecordPushOutcome(store.PushOutcomeSuccess, acceptedAt)

	acceptedAt = now
	now = start.Add(60 * time.Millisecond)
	metrics.RecordPushOutcome(store.PushOutcomeSuccess, acceptedAt)
	metrics.RecordPushOutcome(store.PushOutcomeOwnerMismatch, now)
	metrics.RecordPushOutcome(store.PushOutcomeEncodeFailure, now)
	metrics.RecordPushOutcome(store.PushOutcomeWriteFailure, now)

	got := metrics.Snapshot()
	if got.Push.SampleCount != 2 {
		t.Errorf("push sample count = %d, want 2", got.Push.SampleCount)
	}
	if got.Push.P50Milliseconds != 50 || got.Push.P95Milliseconds != 50 || got.Push.P50Overflow || got.Push.P95Overflow {
		t.Errorf("push percentiles = p50=%v/%v p95=%v/%v, want 50ms without overflow", got.Push.P50Milliseconds, got.Push.P50Overflow, got.Push.P95Milliseconds, got.Push.P95Overflow)
	}
	if got.Push.Outcomes != (store.PushOutcomeCounts{
		Success:       2,
		OwnerMismatch: 1,
		EncodeFailure: 1,
		WriteFailure:  1,
	}) {
		t.Errorf("push outcomes = %+v, want one fixed count per attempt", got.Push.Outcomes)
	}
	if got.Push.LatencyBucketCounts[5] != 2 {
		t.Errorf("latency buckets = %v, want both samples in the >20ms and <=50ms bucket", got.Push.LatencyBucketCounts)
	}
	if got.Push.WindowSeconds != 0.06 {
		t.Errorf("push window = %v, want 0.06", got.Push.WindowSeconds)
	}

	now = start.Add(3_600 * time.Second)
	got = metrics.Snapshot()
	if got.Push.SampleCount != 0 || got.Push.P50Milliseconds != 0 || got.Push.P95Milliseconds != 0 {
		t.Errorf("expired push telemetry = %+v, want zero samples and percentiles", got.Push)
	}
	if got.Push.Outcomes != (store.PushOutcomeCounts{}) {
		t.Errorf("expired push outcomes = %+v, want zero", got.Push.Outcomes)
	}
}

func TestNotificationMetricsPushLatencyEmptyAndZeroDuration(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })

	got := metrics.Snapshot().Push
	if got.SampleCount != 0 || got.P50Milliseconds != 0 || got.P95Milliseconds != 0 || got.P50Overflow || got.P95Overflow {
		t.Fatalf("empty push snapshot = %+v, want zero percentiles without overflow", got)
	}

	metrics.RecordPushOutcome(store.PushOutcomeSuccess, now)
	got = metrics.Snapshot().Push
	if got.SampleCount != 1 || got.LatencyBucketCounts[0] != 1 {
		t.Errorf("zero-duration push = %+v, want one sample in the first bucket", got)
	}
	if got.P50Milliseconds != 1 || got.P95Milliseconds != 1 || got.P50Overflow || got.P95Overflow {
		t.Errorf("zero-duration percentiles = p50=%v/%v p95=%v/%v, want 1ms without overflow", got.P50Milliseconds, got.P50Overflow, got.P95Milliseconds, got.P95Overflow)
	}
}

func TestNotificationMetricsPushLatencyBoundariesAndOverflow(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	finiteBounds := []time.Duration{
		time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		time.Minute,
	}
	for _, latency := range finiteBounds {
		now = start.Add(latency)
		metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)
	}
	now = start.Add(time.Minute + time.Millisecond)
	metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)

	got := metrics.Snapshot().Push
	if got.SampleCount != 16 {
		t.Fatalf("push sample count = %d, want 16", got.SampleCount)
	}
	wantBounds := [15]float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000}
	if got.LatencyBucketUpperBoundsMilliseconds != wantBounds {
		t.Errorf("bucket bounds = %v, want %v", got.LatencyBucketUpperBoundsMilliseconds, wantBounds)
	}
	for i, count := range got.LatencyBucketCounts {
		if count != 1 {
			t.Errorf("bucket %d count = %d, want 1", i, count)
		}
	}
	if got.P50Milliseconds != 200 || got.P50Overflow {
		t.Errorf("p50 = %v/%v, want 200ms without overflow", got.P50Milliseconds, got.P50Overflow)
	}
	if got.P95Milliseconds != 60000 || !got.P95Overflow {
		t.Errorf("p95 = %v/%v, want 60000ms with overflow", got.P95Milliseconds, got.P95Overflow)
	}
}

func TestNotificationMetricsConcurrentPushRecordAndSnapshot(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return start })

	const workers = 8
	const attempts = 1_000
	var inconsistentSnapshots atomic.Int64
	var snapshots sync.WaitGroup
	snapshots.Go(func() {
		for range attempts {
			got := metrics.Snapshot()
			var bucketSamples int64
			for _, count := range got.Push.LatencyBucketCounts {
				bucketSamples += count
			}
			if got.Push.SampleCount != bucketSamples || got.Push.SampleCount != got.Push.Outcomes.Success {
				inconsistentSnapshots.Add(1)
			}
		}
	})

	var records sync.WaitGroup
	for worker := range workers {
		records.Go(func() {
			for attempt := range attempts {
				outcome := store.PushOutcomeSuccess
				if (worker+attempt)%2 == 0 {
					outcome = store.PushOutcomeWriteFailure
				}
				metrics.RecordPushOutcome(outcome, start)
			}
		})
	}
	records.Wait()
	snapshots.Wait()
	if got := inconsistentSnapshots.Load(); got != 0 {
		t.Errorf("concurrent snapshots observed %d inconsistent push samples", got)
	}

	got := metrics.Snapshot()
	if got.Push.SampleCount+got.Push.Outcomes.WriteFailure != workers*attempts {
		t.Errorf("concurrent push counts = %+v, want %d attempts", got.Push.Outcomes, workers*attempts)
	}
}
