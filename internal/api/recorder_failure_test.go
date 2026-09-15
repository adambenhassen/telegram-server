//nolint:testpackage // These tests exercise the unexported delivery and rate-limit boundaries.
package api

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

type recorderFailureLogHandler struct {
	records []slog.Record
}

func (h *recorderFailureLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recorderFailureLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record)
	return nil
}

func (h *recorderFailureLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recorderFailureLogHandler) WithGroup(string) slog.Handler { return h }

func assertRecorderFailureLog(t *testing.T, handler *recorderFailureLogHandler, category, raw string) {
	t.Helper()
	if len(handler.records) != 1 {
		t.Fatalf("captured %d recorder failure logs, want one", len(handler.records))
	}
	record := handler.records[0]
	if record.Message != "telemetry recorder failure" {
		t.Errorf("message = %q, want telemetry recorder failure", record.Message)
	}
	attrs := map[string]string{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		if strings.Contains(attr.Value.String(), raw) {
			t.Errorf("attribute %q contains raw recorder error %q", attr.Key, raw)
		}
		return true
	})
	if attrs["category"] != category {
		t.Errorf("category = %q, want %q", attrs["category"], category)
	}
	if len(attrs) != 1 {
		t.Errorf("log attributes = %v, want only the fixed category", attrs)
	}
	if strings.Contains(record.Message, raw) {
		t.Errorf("message contains raw recorder error %q", raw)
	}
}

func TestRateLimitDenialRecorderFailuresAreContainedAndObservable(t *testing.T) {
	for _, test := range []struct {
		name  string
		panic bool
	}{
		{name: "error"},
		{name: "panic", panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := "raw rate-limit recorder failure"
			metrics := store.NewNotificationMetricsWithClock(func() time.Time { return time.Unix(1_700_000_000, 0) })
			logs := &recorderFailureLogHandler{}
			h := &handlers{
				log:              slog.New(logs),
				rateLimitMetrics: metrics,
				rateLimitRecorder: func(string) error {
					if test.panic {
						panic(raw)
					}
					return errors.New(raw)
				},
			}

			h.recordRateLimitDenial("message_send")

			if got := metrics.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{RateLimitDenial: 1}) {
				t.Fatalf("recorder failure signal = %+v, want one rate-limit failure", got)
			}
			assertRecorderFailureLog(t, logs, "rate_limit_denial", raw)
		})
	}
}

func TestPushOutcomeRecorderFailuresPreservePush(t *testing.T) {
	for _, test := range []struct {
		name  string
		panic bool
	}{
		{name: "error"},
		{name: "panic", panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := "raw push recorder failure"
			metrics := store.NewNotificationMetricsWithClock(func() time.Time { return time.Unix(1_700_000_000, 0) })
			logs := &recorderFailureLogHandler{}
			conn := &outcomePushConn{pushed: true}
			u := &Updater{
				log:         slog.New(logs),
				pushMetrics: metrics,
				pushRecorder: func(store.PushOutcome, time.Time) error {
					if test.panic {
						panic(raw)
					}
					return errors.New(raw)
				},
			}

			u.deliverAt(context.Background(), 7, []pushConn{conn}, func(int) (updateBatch, error) {
				return batch(0, 1, 1), nil
			}, time.Unix(1_700_000_000, 0))

			if conn.attempts != 1 || conn.pts != 1 {
				t.Fatalf("push after recorder failure = attempts %d pts %d, want one successful push", conn.attempts, conn.pts)
			}
			if got := metrics.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{PushOutcome: 1}) {
				t.Fatalf("recorder failure signal = %+v, want one push failure", got)
			}
			assertRecorderFailureLog(t, logs, "push_outcome", raw)
		})
	}
}
