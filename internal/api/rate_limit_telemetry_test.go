package api_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestRateLimitDenialRecorderFailureIsContained(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	var calls atomic.Int32
	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		if calls.Add(1) > 1 {
			panic("telemetry clock unavailable")
		}
		return start
	})
	api.RecordRateLimitDenialForTest(metrics, "message_send")
}
