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
	var publicationCalls atomic.Int32
	firstPublicationStarted := make(chan struct{})
	secondPublicationStarted := make(chan struct{})
	beforePublication := func() {
		switch publicationCalls.Add(1) {
		case 1:
			close(firstPublicationStarted)
		case 2:
			close(secondPublicationStarted)
		}
	}
	var hookCalls atomic.Int32
	firstOutcomePublished := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondLatencyReached := make(chan struct{})
	beforeLatency := func() {
		switch hookCalls.Add(1) {
		case 1:
			close(firstOutcomePublished)
			<-releaseFirst
		case 2:
			close(secondLatencyReached)
		}
	}
	store.SetNotificationMetricsPushHooks(metrics, beforePublication, beforeLatency)

	firstDone := make(chan struct{})
	go func() {
		metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)
		close(firstDone)
	}()
	select {
	case <-firstPublicationStarted:
	case <-time.After(time.Second):
		t.Fatal("first recorder did not start publication")
	}
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
	case <-secondPublicationStarted:
	case <-time.After(time.Second):
		t.Fatal("second recorder did not overlap the paused publication")
	}
	select {
	case <-secondLatencyReached:
		t.Fatal("overlapping recorder published before the first pair completed")
	case <-time.After(100 * time.Millisecond):
	}

	outcomes, latencies, ok := store.NotificationMetricsPushBucketSnapshot(metrics, start.Unix())
	if ok {
		t.Fatalf("snapshot accepted an in-flight push: outcomes=%v latencies=%v", outcomes, latencies)
	}

	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first recorder did not finish")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second recorder did not finish")
	}

	outcomes, latencies, ok = store.NotificationMetricsPushBucketSnapshot(metrics, start.Unix())
	if !ok {
		t.Fatal("completed push publication was not snapshot-visible")
	}
	if outcomes.Success != 2 || latencies[0] != 2 {
		t.Fatalf("completed push publication = outcomes=%v latencies=%v, want two matching samples", outcomes, latencies)
	}
}
