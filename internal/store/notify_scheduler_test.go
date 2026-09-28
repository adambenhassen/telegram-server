package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestNotificationSchedulerIsolatesBlockedKeys(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scheduler := store.NewNotificationSchedulerForTest(ctx)
	defer scheduler.Stop()

	aStarted := make(chan struct{})
	releaseA := make(chan struct{})
	aSecond := make(chan struct{})
	bDone := make(chan struct{})
	if !scheduler.Submit("a", ctx, false, func(context.Context) {
		close(aStarted)
		<-releaseA
	}) {
		t.Fatal("submit first A task")
	}
	select {
	case <-aStarted:
	case <-time.After(time.Second):
		t.Fatal("first A task did not start")
	}
	if !scheduler.Submit("a", ctx, false, func(context.Context) {
		close(aSecond)
	}) {
		t.Fatal("submit second A task")
	}
	if !scheduler.Submit("b", ctx, false, func(context.Context) {
		close(bDone)
	}) {
		t.Fatal("submit B task")
	}
	select {
	case <-bDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("B task waited behind blocked A task")
	}
	select {
	case <-aSecond:
		t.Fatal("same-key A task ran out of order")
	default:
	}
	for range store.NotificationQueueLimitForTest() * 2 {
		scheduler.Submit("a", ctx, false, func(context.Context) {})
	}
	cDone := make(chan struct{})
	if !scheduler.Submit("c", ctx, false, func(context.Context) {
		close(cDone)
	}) {
		t.Fatal("a full A lane rejected an unrelated C task")
	}
	select {
	case <-cDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("C task waited behind the full A lane")
	}
	close(releaseA)
	select {
	case <-aSecond:
	case <-time.After(time.Second):
		t.Fatal("second A task did not run after the first completed")
	}
}

func TestNotificationSchedulerCoalescesAndBoundsPendingWork(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scheduler := store.NewNotificationSchedulerForTest(ctx)
	defer scheduler.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	done := make(chan struct{})
	if !scheduler.Submit("user", ctx, true, func(context.Context) {
		runs.Add(1)
		close(started)
		<-release
	}) {
		t.Fatal("submit initial task")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial task did not start")
	}
	for range store.NotificationQueueLimitForTest() * 4 {
		if !scheduler.Submit("user", ctx, true, func(context.Context) {
			runs.Add(1)
			close(done)
		}) {
			t.Fatal("coalescible task was rejected")
		}
	}
	pending := scheduler.Pending()
	if pending != 1 {
		t.Fatalf("pending coalescible tasks = %d, want one", pending)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coalesced task did not run")
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("scheduler runs = %d, want initial plus one coalesced task", got)
	}
}

func TestNotificationSchedulerCapsOneOwnersPendingWork(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scheduler := store.NewNotificationSchedulerForTest(ctx)

	aStarted := make(chan struct{})
	releaseA := make(chan struct{})
	bStarted := make(chan struct{})
	releaseB := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseA)
			close(releaseB)
		})
	}
	defer func() {
		release()
		scheduler.Stop()
	}()
	if !scheduler.Submit("a", ctx, false, func(context.Context) {
		close(aStarted)
		<-releaseA
	}) {
		t.Fatal("submit first A task")
	}
	select {
	case <-aStarted:
	case <-time.After(time.Second):
		t.Fatal("first A task did not start")
	}
	if !scheduler.Submit("b", ctx, false, func(context.Context) {
		close(bStarted)
		<-releaseB
	}) {
		t.Fatal("submit first B task")
	}
	select {
	case <-bStarted:
	case <-time.After(time.Second):
		t.Fatal("first B task did not start")
	}

	acceptedA := 0
	for range store.NotificationQueueLimitForTest() * 2 {
		if scheduler.Submit("a", ctx, false, func(context.Context) {}) {
			acceptedA++
		}
	}
	if want := store.NotificationLanePendingLimitForTest(); acceptedA != want {
		t.Fatalf("accepted A pending tasks = %d, want per-owner cap %d", acceptedA, want)
	}
	if got := scheduler.Pending(); got != acceptedA {
		t.Fatalf("pending callbacks = %d, want %d", got, acceptedA)
	}

	bNudge := make(chan struct{})
	if !scheduler.Submit("b", ctx, false, func(context.Context) {
		close(bNudge)
	}) {
		t.Fatal("B nudge was rejected by A's pending work")
	}
	release()
	select {
	case <-bNudge:
	case <-time.After(time.Second):
		t.Fatal("B nudge did not run")
	}
}
