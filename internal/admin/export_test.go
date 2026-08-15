package admin

import (
	"context"
	"io"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// SSEDefaultEvent returns the event name the dashboard subscribes to.
func SSEDefaultEvent() string {
	return sseDefaultEvent
}

// SubscribeForTest registers a stream without an HTTP request, so a test can
// hold a subscription it deliberately never drains. It returns the payload
// channel, the snapshot handed to a new subscriber, and an unsubscribe func.
func (b *Broadcaster) SubscribeForTest() (<-chan []byte, []byte, func(), error) {
	sub, last, err := b.subscribe()
	if err != nil {
		return nil, nil, nil, err
	}
	return sub.ch, last, func() { b.unsubscribe(sub) }, nil
}

// CollectMetricsStrict collects a snapshot the way the SSE sampler does: any
// failed query fails the whole snapshot.
func CollectMetricsStrict(ctx context.Context, reg *mtproto.SessionRegistry, st *store.Store) (MetricsResponse, error) {
	return collectMetrics(ctx, reg, st, requireAllMetrics)
}

// CollectMetricsTolerant collects a snapshot the way GET /admin/metrics does: a
// failed pts-gap query degrades to zero.
func CollectMetricsTolerant(ctx context.Context, reg *mtproto.SessionRegistry, st *store.Store) (MetricsResponse, error) {
	return collectMetrics(ctx, reg, st, tolerateGapFailure)
}

// EncodeFragment exposes the SSE wire encoding so the framing rules can be
// asserted without standing up a stream.
func EncodeFragment(f Fragment) []byte {
	return encodeFragment(f)
}

// SSEMaxStreamDuration returns the default bound on one SSE stream's lifetime.
func SSEMaxStreamDuration() time.Duration {
	return sseMaxStreamDuration
}

// IdleTimeout returns the admin session idle timeout.
func IdleTimeout() time.Duration {
	return idleTimeout
}

// RenderDashboard renders the full dashboard page into w.
// Exposed for package-level integration tests.
func RenderDashboard(w io.Writer, data DashboardData) error {
	return dashboardPage(data).Render(context.Background(), w)
}

// RenderFragment renders only the SSE-swappable metrics fragment into w.
// Exposed for testing DashboardFragmentRenderer without going through HTTP.
func RenderFragment(w io.Writer, data DashboardData) error {
	return metricsFragment(data).Render(context.Background(), w)
}
