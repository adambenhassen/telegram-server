package mtproto_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestSessionRegistryAddRemove(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	c1, c2 := &mtproto.Conn{}, &mtproto.Conn{}

	r.Add(7, c1)
	r.Add(7, c2)
	if got := r.Conns(7); len(got) != 2 {
		t.Fatalf("Conns(7) = %d, want 2", len(got))
	}
	if got := r.Conns(9); len(got) != 0 {
		t.Fatalf("Conns(9) = %d, want 0", len(got))
	}

	r.Remove(7, c1)
	got := r.Conns(7)
	if len(got) != 1 || got[0] != c2 {
		t.Fatalf("after remove, Conns(7) = %+v, want [c2]", got)
	}

	r.Remove(7, c2)
	if got := r.Conns(7); len(got) != 0 {
		t.Fatalf("after remove all, Conns(7) = %d, want 0", len(got))
	}
}

// TestSessionRegistryCap covers the per-user connection cap: past it a new
// connection is refused and no live one is evicted, and a slot freed by a
// departing connection is reusable.
func TestSessionRegistryCap(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	conns := make([]*mtproto.Conn, mtproto.MaxUserConns)
	for i := range conns {
		conns[i] = &mtproto.Conn{}
		if !r.Add(3, conns[i]) {
			t.Fatalf("Add %d of %d refused under the cap", i+1, mtproto.MaxUserConns)
		}
	}

	extra := &mtproto.Conn{}
	if r.Add(3, extra) {
		t.Fatal("Add past the cap must be refused")
	}
	got := r.Conns(3)
	if len(got) != mtproto.MaxUserConns {
		t.Fatalf("Conns(3) = %d, want %d", len(got), mtproto.MaxUserConns)
	}
	if got[0] != conns[0] {
		t.Fatal("the oldest connection was evicted; the cap must refuse, never evict")
	}
	// A different user is unaffected by another's cap.
	if !r.Add(4, extra) {
		t.Fatal("another user's Add refused by user 3's cap")
	}

	r.Remove(3, conns[0])
	if !r.Add(3, extra) {
		t.Fatal("Add refused after a connection freed a slot")
	}
}

func TestSessionRegistryConcurrent(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	var wg sync.WaitGroup
	for range 50 {
		c := &mtproto.Conn{}
		wg.Go(func() {
			r.Add(1, c)
			_ = r.Conns(1)
			r.Remove(1, c)
		})
	}
	wg.Wait()
	if got := r.Conns(1); len(got) != 0 {
		t.Fatalf("residual conns after concurrent churn: %d", len(got))
	}
}

func TestSessionRegistry_TotalConns(t *testing.T) {
	r := mtproto.NewSessionRegistry()

	if got := r.TotalConns(); got != 0 {
		t.Errorf("empty registry: expected 0, got %d", got)
	}

	c1 := &mtproto.Conn{}
	c2 := &mtproto.Conn{}
	c3 := &mtproto.Conn{}

	// One user with one conn.
	r.Add(1, c1)
	if got := r.TotalConns(); got != 1 {
		t.Errorf("one conn: expected 1, got %d", got)
	}

	// Two users with one conn each.
	r.Add(2, c2)
	if got := r.TotalConns(); got != 2 {
		t.Errorf("two conns: expected 2, got %d", got)
	}

	// One user with two conns.
	r.Add(1, c3)
	if got := r.TotalConns(); got != 3 {
		t.Errorf("three conns: expected 3, got %d", got)
	}

	// Remove one conn.
	r.Remove(1, c1)
	if got := r.TotalConns(); got != 2 {
		t.Errorf("after remove: expected 2, got %d", got)
	}

	// Remove all conns for user 1.
	r.Remove(1, c3)
	if got := r.TotalConns(); got != 1 {
		t.Errorf("after second remove: expected 1, got %d", got)
	}

	// Remove last conn.
	r.Remove(2, c2)
	if got := r.TotalConns(); got != 0 {
		t.Errorf("empty: expected 0, got %d", got)
	}
}

func TestSessionRegistry_TotalSessions(t *testing.T) {
	r := mtproto.NewSessionRegistry()

	if got := r.TotalSessions(); got != 0 {
		t.Errorf("empty registry: expected 0, got %d", got)
	}

	c1 := &mtproto.Conn{}
	c2 := &mtproto.Conn{}
	c3 := &mtproto.Conn{}

	// One user with one conn = one session.
	r.Add(1, c1)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("one session: expected 1, got %d", got)
	}

	// Same user with two conns = still one session.
	r.Add(1, c2)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("same user, two conns: expected 1, got %d", got)
	}

	// Two users = two sessions.
	r.Add(2, c3)
	if got := r.TotalSessions(); got != 2 {
		t.Errorf("two sessions: expected 2, got %d", got)
	}

	// Remove one user's conns.
	r.Remove(1, c1)
	r.Remove(1, c2)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("after removing user 1: expected 1, got %d", got)
	}

	// Remove last user.
	r.Remove(2, c3)
	if got := r.TotalSessions(); got != 0 {
		t.Errorf("empty: expected 0, got %d", got)
	}
}

func TestSessionRegistrySampleDeliveryLagDeduplicatesHeads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r := mtproto.NewSessionRegistry()
	a := mtproto.NewTestConn(&fakeConn{}, testKey(t))
	b := mtproto.NewTestConn(&fakeConn{}, testKey(t))
	c := mtproto.NewTestConn(&fakeConn{}, testKey(t))
	a.SetOwner(7)
	b.SetOwner(7)
	c.SetOwner(9)
	for _, item := range []struct {
		conn *mtproto.Conn
		pts  int
	}{
		{a, 8}, {b, 10}, {c, 10},
	} {
		if _, err := item.conn.PushTo(ctx, map[*mtproto.Conn]int64{a: 7, b: 7, c: 9}[item.conn], &mt.Pong{PingID: 1}, item.pts); err != nil {
			t.Fatalf("set watermark %d: %v", item.pts, err)
		}
	}
	if !r.Add(7, a) || !r.Add(7, b) || !r.Add(9, c) {
		t.Fatal("register connections")
	}

	var calls = map[int64]int{}
	sample := r.SampleDeliveryLag(ctx, func(_ context.Context, userID int64) (int64, error) {
		calls[userID]++
		if userID == 7 {
			return 11, nil
		}
		return 4, nil
	})
	if sample.EligibleConnections != 3 {
		t.Errorf("eligible connections = %d, want 3", sample.EligibleConnections)
	}
	if sample.SampledConnections != 3 {
		t.Errorf("sampled connections = %d, want 3", sample.SampledConnections)
	}
	if sample.WorstPts != 3 {
		t.Errorf("worst pts = %d, want 3", sample.WorstPts)
	}
	if calls[7] != 1 || calls[9] != 1 {
		t.Errorf("account-head calls = %v, want one call per account", calls)
	}

	partial := r.SampleDeliveryLag(ctx, func(_ context.Context, userID int64) (int64, error) {
		if userID == 7 {
			return 0, errors.New("head unavailable")
		}
		return 4, nil
	})
	if partial.EligibleConnections != 3 || partial.SampledConnections != 1 || partial.WorstPts != 0 {
		t.Errorf("partial sample = %+v, want eligible=3 sampled=1 worst=0", partial)
	}
}

func TestSessionRegistrySampleDeliveryLagZeroWatermark(t *testing.T) {
	t.Parallel()

	r := mtproto.NewSessionRegistry()
	conn := mtproto.NewTestConn(&fakeConn{}, testKey(t))
	if got := conn.LastPushedPts(); got != 0 {
		t.Fatalf("new connection watermark = %d, want 0", got)
	}
	if !r.Add(12, conn) {
		t.Fatal("register connection")
	}

	sample := r.SampleDeliveryLag(context.Background(), func(context.Context, int64) (int64, error) {
		return 17, nil
	})
	if sample.EligibleConnections != 1 || sample.SampledConnections != 1 || sample.WorstPts != 17 {
		t.Fatalf("sample = %+v, want eligible=1 sampled=1 worst=17", sample)
	}
}

func TestSessionRegistrySampleDeliveryLagCapsWork(t *testing.T) {
	t.Parallel()

	const eligible = 1025
	r := mtproto.NewSessionRegistry()
	for userID := int64(1); userID <= eligible; userID++ {
		if !r.Add(userID, &mtproto.Conn{}) {
			t.Fatalf("register connection for user %d", userID)
		}
	}

	var calls atomic.Int64
	sample := r.SampleDeliveryLag(context.Background(), func(context.Context, int64) (int64, error) {
		calls.Add(1)
		return 1, nil
	})
	if sample.EligibleConnections != eligible {
		t.Fatalf("eligible connections = %d, want %d", sample.EligibleConnections, eligible)
	}
	if sample.SampledConnections != 1024 {
		t.Fatalf("sampled connections = %d, want 1024", sample.SampledConnections)
	}
	if calls.Load() != 1024 {
		t.Fatalf("account-head calls = %d, want 1024", calls.Load())
	}
	if !sample.Complete {
		t.Fatal("bounded sample did not finish its selected work")
	}
}

func TestSessionRegistrySampleDeliveryLagStopsOnCancellation(t *testing.T) {
	t.Parallel()

	r := mtproto.NewSessionRegistry()
	for userID := int64(1); userID <= 3; userID++ {
		if !r.Add(userID, &mtproto.Conn{}) {
			t.Fatalf("register connection for user %d", userID)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int64
	sample := r.SampleDeliveryLag(ctx, func(context.Context, int64) (int64, error) {
		calls.Add(1)
		cancel()
		return 1, nil
	})
	if calls.Load() != 1 {
		t.Fatalf("account-head calls = %d, want 1 after cancellation", calls.Load())
	}
	if sample.SampledConnections != 0 {
		t.Fatalf("sampled connections = %d, want 0 after cancellation", sample.SampledConnections)
	}
	if sample.Complete {
		t.Fatal("cancelled sample reported complete")
	}
}

func TestSessionRegistrySampleDeliveryLagStopsAtDeadline(t *testing.T) {
	t.Parallel()

	r := mtproto.NewSessionRegistry()
	for userID := int64(1); userID <= 3; userID++ {
		if !r.Add(userID, &mtproto.Conn{}) {
			t.Fatalf("register connection for user %d", userID)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var calls atomic.Int64
	sample := r.SampleDeliveryLag(ctx, func(ctx context.Context, _ int64) (int64, error) {
		calls.Add(1)
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if calls.Load() != 1 {
		t.Fatalf("account-head calls = %d, want 1 at deadline", calls.Load())
	}
	if sample.Complete || sample.SampledConnections != 0 {
		t.Fatalf("deadline sample = %+v, want incomplete with no sampled connections", sample)
	}
}

func TestSessionRegistryDeliveryLagSnapshotCountUnderChurn(t *testing.T) {
	t.Parallel()

	r := mtproto.NewSessionRegistry()
	if !r.Add(1, &mtproto.Conn{}) {
		t.Fatal("register initial connection")
	}

	headStarted := make(chan struct{})
	releaseHead := make(chan struct{})
	result := make(chan mtproto.DeliveryLagSample, 1)
	go func() {
		result <- r.SampleDeliveryLag(context.Background(), func(context.Context, int64) (int64, error) {
			close(headStarted)
			<-releaseHead
			return 1, nil
		})
	}()
	<-headStarted

	for userID := int64(2); userID <= 4; userID++ {
		if !r.Add(userID, &mtproto.Conn{}) {
			t.Fatalf("register churn connection for user %d", userID)
		}
	}
	close(releaseHead)
	sample := <-result
	if sample.EligibleConnections != 1 {
		t.Fatalf("snapshot eligible connections = %d, want 1", sample.EligibleConnections)
	}
	if sample.SampledConnections != 1 {
		t.Fatalf("snapshot sampled connections = %d, want 1", sample.SampledConnections)
	}
	if got := r.TotalConns(); got != 4 {
		t.Fatalf("live connection count after concurrent adds = %d, want 4", got)
	}
}

func TestSessionRegistrySamplingDoesNotBlockPush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	transport := &fakeConn{}
	conn := mtproto.NewTestConn(transport, testKey(t))
	conn.SetOwner(7)
	r := mtproto.NewSessionRegistry()
	if !r.Add(7, conn) {
		t.Fatal("register connection")
	}

	headStarted := make(chan struct{})
	releaseHead := make(chan struct{})
	sampleDone := make(chan mtproto.DeliveryLagSample, 1)
	go func() {
		sampleDone <- r.SampleDeliveryLag(ctx, func(context.Context, int64) (int64, error) {
			close(headStarted)
			<-releaseHead
			return 11, nil
		})
	}()
	<-headStarted

	released := false
	release := func() {
		if !released {
			close(releaseHead)
			released = true
		}
	}
	defer release()

	pushDone := make(chan struct{})
	var pushed bool
	var pushErr error
	go func() {
		pushed, pushErr = conn.PushTo(ctx, 7, &mt.Pong{PingID: 1}, 5)
		close(pushDone)
	}()
	select {
	case <-pushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("PushTo blocked while the sampler waited on account head")
	}
	if pushErr != nil {
		t.Fatalf("PushTo: %v", pushErr)
	}
	if !pushed {
		t.Fatal("PushTo refused a push for the current owner")
	}
	if got := conn.LastPushedPts(); got != 5 {
		t.Fatalf("watermark = %d, want 5 before releasing the sampler", got)
	}

	release()
	if sample := <-sampleDone; sample.SampledConnections != 1 || sample.WorstPts != 6 {
		t.Fatalf("sample after independent push = %+v, want one sample with lag 6", sample)
	}
}

func TestSessionRegistryConcurrentDeliveryLagSampling(t *testing.T) {
	t.Parallel()

	r := mtproto.NewSessionRegistry()
	var wg sync.WaitGroup
	for range 50 {
		conn := &mtproto.Conn{}
		wg.Go(func() {
			for range 10 {
				r.Add(1, conn)
				_ = r.SampleDeliveryLag(context.Background(), func(context.Context, int64) (int64, error) {
					return 1, nil
				})
				r.Remove(1, conn)
			}
		})
	}
	wg.Wait()
}
