package store_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type listenerRecorderFailureLogHandler struct {
	records []slog.Record
}

func (h *listenerRecorderFailureLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *listenerRecorderFailureLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record)
	return nil
}

func (h *listenerRecorderFailureLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *listenerRecorderFailureLogHandler) WithGroup(string) slog.Handler { return h }

func TestStartListenerRecorderFailuresAreContainedAndObservable(t *testing.T) {
	for _, test := range []struct {
		name  string
		panic bool
	}{
		{name: "error"},
		{name: "panic", panic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dsn := pgtest.DSN(t)
			s := openDSN(t, dsn)
			raw := "raw notification recorder failure"
			metrics := store.NewNotificationMetricsWithClock(func() time.Time { return time.Unix(1_700_000_000, 0) })
			logs := &listenerRecorderFailureLogHandler{}
			delivered := make(chan int64, 1)
			listener, stop, err := store.StartListener(ctx, dsn,
				func(_ context.Context, userID int64) { delivered <- userID },
				func(context.Context, int64, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64, bool) {},
				func(context.Context, int64, int) {},
				func(context.Context, int64, int64, int64) {},
				func(context.Context, store.PeerType, int64, int32) {},
				slog.New(logs),
				metrics,
			)
			if err != nil {
				t.Fatalf("start listener: %v", err)
			}
			store.SetListenerRecorderHooks(listener, func(string) error {
				if test.panic {
					panic(raw)
				}
				return errors.New(raw)
			}, nil)
			defer func() {
				if err := stop(); err != nil {
					t.Errorf("stop: %v", err)
				}
			}()

			if err := s.Notify(ctx, store.ChannelUpdates, "17"); err != nil {
				t.Fatalf("notify: %v", err)
			}
			select {
			case got := <-delivered:
				if got != 17 {
					t.Fatalf("delivered userID = %d, want 17", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("deliver callback not invoked after recorder failure")
			}

			if got := metrics.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{Notification: 1}) {
				t.Fatalf("recorder failure signal = %+v, want one notification failure", got)
			}
			if len(logs.records) != 1 {
				t.Fatalf("captured %d recorder failure logs, want one", len(logs.records))
			}
			record := logs.records[0]
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
			if attrs["category"] != "notification" {
				t.Errorf("category = %q, want notification", attrs["category"])
			}
			if len(attrs) != 1 {
				t.Errorf("log attributes = %v, want only the fixed category", attrs)
			}
			if strings.Contains(record.Message, raw) {
				t.Errorf("message contains raw recorder error %q", raw)
			}
		})
	}
}
