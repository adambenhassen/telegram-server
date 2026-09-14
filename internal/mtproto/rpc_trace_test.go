package mtproto_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
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

func TestRPCTraceIncludesFallbackReplyFailure(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	start := time.Unix(1_700_000_000, 0)
	now := start
	exporter := mtproto.NewMemoryRPCSpanExporter(1)
	tracer := mtproto.NewRPCTracer(mtproto.RPCTracerConfig{
		Exporter: exporter,
		Now:      func() time.Time { return now },
	})
	d := mtproto.NewDispatcher()
	d.Fallback(mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error {
		now = now.Add(5 * time.Millisecond)
		return tgerr.New(420, "FLOOD_WAIT_7")
	}))
	srv := mtproto.New(exchange.PrivateKey{}, 2, &statusKeyStore{key: key, users: []int64{0}}, d, nil)
	srv.SetRPCTracer(tracer)

	var sends int
	conn := &scriptedConn{
		frames: [][]byte{statusClientFrame(t, key, 42, 1<<32, &tg.UsersGetUsersRequest{})},
		sendFn: func() error {
			sends++
			if sends == 1 {
				return nil
			}
			now = now.Add(7 * time.Millisecond)
			return io.ErrClosedPipe
		},
	}
	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("ServeConn = %v, want closed-pipe reply failure", err)
	}
	if sends != 2 {
		t.Fatalf("transport writes = %d, want session-created and fallback replies", sends)
	}
	if err := tracer.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}

	spans := exporter.Spans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Method != mtproto.UnknownRPCMethod {
		t.Fatalf("method = %q, want %q", spans[0].Method, mtproto.UnknownRPCMethod)
	}
	if spans[0].Result != mtproto.RPCResultTransportFailure {
		t.Fatalf("result = %q, want %q", spans[0].Result, mtproto.RPCResultTransportFailure)
	}
	if spans[0].Duration != 12*time.Millisecond {
		t.Fatalf("duration = %s, want 12ms", spans[0].Duration)
	}
}

func TestRPCTraceRealRateLimitReplyPath(t *testing.T) {
	t.Parallel()

	const (
		sentinelBody   = "sentinel-rpc-body-user-991337"
		sentinelDevice = "sentinel-rpc-device-991337"
		sentinelAddr   = "198.51.100.88"
		sentinelUserID = int64(991337)
		sentinelPeerID = int64(991338)
	)
	for _, test := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			blobs, err := blob.NewLocal(t.TempDir())
			if err != nil {
				t.Fatalf("blob store: %v", err)
			}
			s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(blobs))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			})

			limit := store.RateLimitConfig{Limit: 1, Window: time.Minute}
			if _, err := s.CheckRateLimit(ctx, sentinelUserID, "message_send", limit); err != nil {
				t.Fatalf("seed rate limit: %v", err)
			}
			handler := api.New(
				s,
				2,
				api.DefaultConfig(2, "127.0.0.1", 0),
				slog.New(slog.DiscardHandler),
				false,
				100<<20,
				blobs,
				2<<30,
				pgtest.PeerDeriver(),
				config.RateLimitsConfig{MessageSend: limit},
				config.RegistrationClosed,
			)

			var exporter *mtproto.MemoryRPCSpanExporter
			var tracer *mtproto.RPCTracer
			if test.enabled {
				exporter = mtproto.NewMemoryRPCSpanExporter(1)
				tracer = mtproto.NewRPCTracer(mtproto.RPCTracerConfig{Exporter: exporter})
			} else {
				tracer = mtproto.NewRPCTracer(mtproto.RPCTracerConfig{})
			}
			t.Cleanup(func() {
				if err := tracer.Close(); err != nil {
					t.Errorf("close tracer: %v", err)
				}
			})

			var body bin.Buffer
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
					Query: &tg.MessagesSendMessageRequest{
						Peer: &tg.InputPeerUser{
							UserID:     sentinelPeerID,
							AccessHash: pgtest.PeerDeriver().Derive(sentinelUserID, peerhash.KindUser, sentinelPeerID),
						},
						Message:  sentinelBody,
						RandomID: 0,
					},
				},
			}
			if err := wrapped.Encode(&body); err != nil {
				t.Fatalf("encode request: %v", err)
			}
			req := &mtproto.Request{
				UserID:     sentinelUserID,
				ClientAddr: netip.MustParseAddr(sentinelAddr),
				MsgID:      1 << 32,
				Buf:        &body,
				Ctx:        ctx,
			}
			transport := &fakeConn{sendErr: io.ErrClosedPipe}
			conn := mtproto.NewTestConn(transport, rebindTestKey())
			if err := tracer.Wrap(handler).OnMessage(conn, req); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("OnMessage = %v, want closed-pipe reply failure", err)
			}
			if got := transport.writes(); got != 1 {
				t.Fatalf("transport writes = %d, want one SendErr reply", got)
			}

			if err := tracer.Close(); err != nil {
				t.Fatalf("flush tracer: %v", err)
			}
			if test.enabled {
				spans := exporter.Spans()
				if len(spans) != 1 {
					t.Fatalf("spans = %d, want 1", len(spans))
				}
				span := spans[0]
				if span.Method != "messages.sendMessage" {
					t.Fatalf("method = %q, want messages.sendMessage", span.Method)
				}
				if span.Result != mtproto.RPCResultTransportFailure {
					t.Fatalf("result = %q, want %q", span.Result, mtproto.RPCResultTransportFailure)
				}
				if span.Duration < 0 {
					t.Fatalf("duration = %s, want non-negative", span.Duration)
				}
				got := fmt.Sprint(spans)
				for _, forbidden := range []string{sentinelBody, sentinelDevice, sentinelAddr, strconv.FormatInt(sentinelUserID, 10)} {
					if contains(got, forbidden) {
						t.Fatalf("span snapshot retained forbidden request data: %q", got)
					}
				}
			} else if got := tracer.Snapshot(); got != (mtproto.RPCTracerSnapshot{}) {
				t.Fatalf("disabled tracer snapshot = %+v, want zero", got)
			}
		})
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
