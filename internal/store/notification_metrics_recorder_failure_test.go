package store_test

import (
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestNotificationMetricsRecorderFailuresAreFixedAndRolling(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	for range 10_000 {
		metrics.RecordRecorderFailure(store.RecorderFailureRateLimitDenial)
		metrics.RecordRecorderFailure(store.RecorderFailurePushOutcome)
		metrics.RecordRecorderFailure(store.RecorderFailureNotification)
	}

	got := metrics.Snapshot().RecorderFailures
	want := store.RecorderFailureCounts{
		RateLimitDenial: 10_000,
		PushOutcome:     10_000,
		Notification:    10_000,
	}
	if got != want {
		t.Fatalf("recorder failures = %+v, want %+v", got, want)
	}

	now = start.Add(time.Hour + time.Second)
	if got := metrics.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{}) {
		t.Fatalf("expired recorder failures = %+v, want zero", got)
	}

	restarted := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	if got := restarted.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{}) {
		t.Fatalf("restarted recorder failures = %+v, want zero", got)
	}
}
