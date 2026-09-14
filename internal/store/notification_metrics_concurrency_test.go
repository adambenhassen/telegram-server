package store_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestNotificationMetricsConcurrentPushPublication(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return start })
	var hookCalls atomic.Int32
	firstOutcomePublished := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondLatencyReached := make(chan struct{})
	store.SetNotificationMetricsPushHooks(metrics, func() {
		switch hookCalls.Add(1) {
		case 1:
			close(firstOutcomePublished)
			<-releaseFirst
		case 2:
			close(secondLatencyReached)
		}
	})

	firstDone := make(chan struct{})
	go func() {
		metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)
		close(firstDone)
	}()
	select {
	case <-firstOutcomePublished:
	case <-time.After(time.Second):
		t.Fatal("first recorder did not pause after publishing its outcome")
	}

	secondDone := make(chan struct{})
	go func() {
		metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)
		close(secondDone)
	}()
	select {
	case <-secondLatencyReached:
	case <-time.After(time.Second):
		t.Fatal("second recorder did not overlap the paused publication")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("overlapping recorder did not complete while the first was paused")
	}

	outcomes, latencies, ok := store.NotificationMetricsPushBucketSnapshot(metrics, start.Unix())
	if !ok {
		t.Fatal("snapshot rejected an independently completed push")
	}
	if outcomes.Success != 1 || latencies[0] != 1 {
		t.Fatalf("snapshot during paused publication = outcomes=%v latencies=%v, want one complete sample", outcomes, latencies)
	}

	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first recorder did not finish")
	}

	outcomes, latencies, ok = store.NotificationMetricsPushBucketSnapshot(metrics, start.Unix())
	if !ok {
		t.Fatal("completed push publication was not snapshot-visible")
	}
	if outcomes.Success != 2 || latencies[0] != 2 {
		t.Fatalf("completed push publication = outcomes=%v latencies=%v, want two matching samples", outcomes, latencies)
	}
}
