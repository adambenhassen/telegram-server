package mtproto

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionRegistrySampleDeliveryLagLockContention(t *testing.T) {
	r := NewSessionRegistry()
	if !r.Add(1, &Conn{}) {
		t.Fatal("register connection")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var calls atomic.Int64
	r.mu.Lock()
	result := make(chan DeliveryLagSample, 1)
	go func() {
		result <- r.SampleDeliveryLag(ctx, func(context.Context, int64) (int64, error) {
			calls.Add(1)
			return 1, nil
		})
	}()
	sample := <-result
	r.mu.Unlock()

	if sample.EligibleConnections != 1 || sample.SampledConnections != 0 || sample.Complete {
		t.Fatalf("lock-contention sample = %+v, want eligible=1, sampled=0, incomplete", sample)
	}
	if calls.Load() != 0 {
		t.Fatalf("account-head calls = %d, want 0 under lock contention", calls.Load())
	}
}
