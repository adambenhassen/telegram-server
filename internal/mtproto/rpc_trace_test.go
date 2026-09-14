package mtproto_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestRPCTraceRecordsAllResultClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    error
		result mtproto.RPCResultClass
	}{
		{name: "success", result: mtproto.RPCResultSuccess},
		{name: "invalid request", err: tgerr.New(400, "INPUT_REQUEST_INVALID"), result: mtproto.RPCResultInvalidRequest},
		{name: "unauthenticated", err: tgerr.New(401, "AUTH_KEY_UNREGISTERED"), result: mtproto.RPCResultUnauthenticated},
		{name: "unauthorized", err: tgerr.New(403, "CHAT_FORBIDDEN"), result: mtproto.RPCResultUnauthorized},
		{name: "rate limited", err: tgerr.New(420, "FLOOD_WAIT_7"), result: mtproto.RPCResultRateLimited},
		{name: "deadline", err: context.DeadlineExceeded, result: mtproto.RPCResultDeadline},
		{name: "internal", err: tgerr.New(500, "INTERNAL"), result: mtproto.RPCResultInternal},
		{name: "transport failure", err: io.ErrClosedPipe, result: mtproto.RPCResultTransportFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := time.Unix(1_700_000_000, 0)
			now := start
			exporter := mtproto.NewMemoryRPCSpanExporter(2)
			tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{
				Exporter: exporter,
				Now:      func() time.Time { return now },
			})
			defer func() {
				if err := tracer.Close(); err != nil {
					t.Fatalf("close tracer: %v", err)
				}
			}()

			d := mtproto.NewDispatcher()
			d.HandleFunc(tg.MessagesSendMessageRequestTypeID, func(_ *mtproto.Conn, _ *mtproto.Request) error {
				now = now.Add(5 * time.Millisecond)
				return test.err
			})
			req := traceRequest(tg.MessagesSendMessageRequestTypeID)
			if err := tracer.Wrap(d).OnMessage(nil, req); !errors.Is(err, test.err) {
				t.Fatalf("OnMessage error = %v, want %v", err, test.err)
			}

			if err := tracer.Close(); err != nil {
				t.Fatalf("flush tracer: %v", err)
			}
			spans := exporter.Spans()
			if len(spans) != 1 {
				t.Fatalf("spans = %d, want 1", len(spans))
			}
			span := spans[0]
			if span.Name != mtproto.RPCSpanName || span.Method != "messages.sendMessage" {
				t.Fatalf("span identity = %+v", span)
			}
			if span.Result != test.result {
				t.Errorf("result = %q, want %q", span.Result, test.result)
			}
			if span.Duration != 5*time.Millisecond {
				t.Errorf("duration = %s, want 5ms", span.Duration)
			}
		})
	}
}

func TestRPCTraceUnknownMethodsCollapse(t *testing.T) {
	t.Parallel()

	exporter := mtproto.NewMemoryRPCSpanExporter(32)
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{Exporter: exporter, QueueCapacity: 32})
	d := mtproto.NewDispatcher()
	d.Fallback(mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error { return nil }))
	h := tracer.Wrap(d)
	for i := range 20 {
		if err := h.OnMessage(nil, traceRequest(uint32(0xf0000000+i))); err != nil {
			t.Fatalf("unknown request %d: %v", i, err)
		}
	}
	if err := tracer.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}
	spans := exporter.Spans()
	if len(spans) != 20 {
		t.Fatalf("spans = %d, want 20", len(spans))
	}
	for i, span := range spans {
		if span.Method != mtproto.UnknownRPCMethod {
			t.Errorf("span %d method = %q, want %q", i, span.Method, mtproto.UnknownRPCMethod)
		}
	}
}

func TestRPCTraceDoesNotRetainRequestData(t *testing.T) {
	t.Parallel()

	const (
		sentinelBody   = "sentinel-message-body"
		sentinelDevice = "sentinel-device-model"
		sentinelUserID = int64(991337)
		sentinelAddr   = "198.51.100.77"
	)
	exporter := mtproto.NewMemoryRPCSpanExporter(2)
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{Exporter: exporter})
	d := mtproto.NewDispatcher()
	d.HandleFunc(tg.HelpGetConfigRequestTypeID, func(_ *mtproto.Conn, req *mtproto.Request) error {
		if req.Buf == nil {
			return errors.New(sentinelBody)
		}
		return nil
	})
	wrapped := &tg.InvokeWithLayerRequest{
		Layer: 100,
		Query: &tg.InitConnectionRequest{
			APIID:          1,
			DeviceModel:    sentinelDevice,
			SystemVersion:  "test-system",
			AppVersion:     "test-app",
			SystemLangCode: "en",
			LangPack:       "",
			LangCode:       "en",
			Query:          &tg.HelpGetConfigRequest{},
		},
	}
	var b bin.Buffer
	if err := wrapped.Encode(&b); err != nil {
		t.Fatal(err)
	}
	b.Buf = append(b.Buf, []byte(sentinelBody)...)
	req := &mtproto.Request{
		UserID:     sentinelUserID,
		ClientAddr: netip.MustParseAddr(sentinelAddr),
		Buf:        &b,
		Ctx:        context.Background(),
	}
	if err := tracer.Wrap(mtproto.UnpackInvoke(d)).OnMessage(nil, req); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}
	got := fmt.Sprint(exporter.Spans())
	if got == "" {
		t.Fatal("span snapshot is empty")
	}
	for _, forbidden := range []string{sentinelBody, sentinelDevice, sentinelAddr, strconv.FormatInt(sentinelUserID, 10)} {
		if contains(got, forbidden) {
			t.Fatalf("span snapshot retained forbidden request data: %q", got)
		}
	}
}

func TestRPCTraceDisabledDoesNoWork(t *testing.T) {
	t.Parallel()

	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{})
	d := mtproto.NewDispatcher()
	d.HandleFunc(tg.MessagesSendMessageRequestTypeID, func(_ *mtproto.Conn, _ *mtproto.Request) error { return nil })
	if got := tracer.Wrap(d); got != d {
		t.Fatal("disabled tracer wrapped the handler")
	}
	if err := tracer.Wrap(d).OnMessage(nil, traceRequest(tg.MessagesSendMessageRequestTypeID)); err != nil {
		t.Fatal(err)
	}
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	if got := tracer.Snapshot(); got != (mtproto.RPCTracerSnapshot{}) {
		t.Fatalf("disabled tracer snapshot = %+v, want zero", got)
	}
}

func TestRPCTraceDropsWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	exporter := newBlockingRPCExporter()
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{
		Exporter:        exporter,
		QueueCapacity:   1,
		ShutdownTimeout: time.Second,
	})
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	select {
	case <-exporter.started:
	case <-time.After(time.Second):
		t.Fatal("exporter did not receive first span")
	}
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	if got := tracer.Snapshot().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	close(exporter.release)
	if err := tracer.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}
}

func TestRPCTraceContainsExporterFailures(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	exporter := mtproto.RPCSpanExporterFunc(func(context.Context, mtproto.RPCSpan) error {
		switch calls.Add(1) {
		case 1:
			return errors.New("forbidden exporter detail")
		default:
			panic("forbidden exporter panic")
		}
	})
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{Exporter: exporter, QueueCapacity: 2})
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	if err := tracer.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}
	got := tracer.Snapshot()
	if got.ExporterErrors != 1 || got.ExporterPanics != 1 {
		t.Fatalf("failure snapshot = %+v, want one error and one panic", got)
	}
}

func TestRPCTraceShutdownIsBounded(t *testing.T) {
	t.Parallel()

	exporter := newBlockingRPCExporter()
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{
		Exporter:        exporter,
		ShutdownTimeout: 20 * time.Millisecond,
	})
	tracer.Record(mtproto.RPCSpan{Method: "messages.sendMessage", Result: mtproto.RPCResultSuccess})
	select {
	case <-exporter.started:
	case <-time.After(time.Second):
		t.Fatal("exporter did not receive span")
	}
	if err := tracer.Close(); !errors.Is(err, mtproto.ErrRPCTracerShutdownTimeout) {
		t.Fatalf("close error = %v, want bounded timeout", err)
	}
	close(exporter.release)
	if err := tracer.Close(); err != nil {
		t.Fatalf("close after release: %v", err)
	}
}

func traceRequest(id uint32) *mtproto.Request {
	var b bin.Buffer
	b.PutID(id)
	return &mtproto.Request{Buf: &b, Ctx: context.Background()}
}

func contains(s, want string) bool {
	for i := 0; i+len(want) <= len(s); i++ {
		if s[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

type blockingRPCExporter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingRPCExporter() *blockingRPCExporter {
	return &blockingRPCExporter{started: make(chan struct{}), release: make(chan struct{})}
}

func (e *blockingRPCExporter) Export(context.Context, mtproto.RPCSpan) error {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return nil
}
