package store_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type listenerRecorderFailureLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *listenerRecorderFailureLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *listenerRecorderFailureLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record)
	return nil
}

func (h *listenerRecorderFailureLogHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
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

func TestStartListenerInvalidRecorderFailureIsContainedAndObservable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)
	raw := "raw invalid notification recorder failure"
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
	store.SetListenerRecorderHooks(listener, nil, func() error { return errors.New(raw) })
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	if err := s.Notify(ctx, store.ChannelUpdates, "not-a-user-payload"); err != nil {
		t.Fatalf("notify: %v", err)
	}
	var records []slog.Record
	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for len(records) == 0 {
		select {
		case <-deadline.C:
			t.Fatal("recorder failure log was not emitted")
		case <-ticker.C:
			records = logs.snapshot()
		}
	}
	select {
	case got := <-delivered:
		t.Fatalf("invalid notification delivered userID %d", got)
	default:
	}

	if got := metrics.Snapshot().RecorderFailures; got != (store.RecorderFailureCounts{Notification: 1}) {
		t.Fatalf("recorder failure signal = %+v, want one notification failure", got)
	}
	var found bool
	for _, record := range records {
		if record.Message != "telemetry recorder failure" {
			continue
		}
		found = true
		attrs := map[string]string{}
		record.Attrs(func(attr slog.Attr) bool {
			attrs[attr.Key] = attr.Value.String()
			if strings.Contains(attr.Value.String(), raw) || strings.Contains(attr.Value.String(), "not-a-user-payload") {
				t.Errorf("attribute %q contains recorder or notification content", attr.Key)
			}
			return true
		})
		if attrs["category"] != "notification" {
			t.Errorf("category = %q, want notification", attrs["category"])
		}
		if len(attrs) != 1 {
			t.Errorf("log attributes = %v, want only the fixed category", attrs)
		}
	}
	if !found {
		t.Fatalf("captured logs = %+v, want telemetry recorder failure", logs.records)
	}
}
