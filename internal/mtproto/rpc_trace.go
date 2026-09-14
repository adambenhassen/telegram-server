package mtproto

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tgerr"
)

const (
	// RPCSpanName is fixed so the span name cannot become a request-controlled
	// series or export field.
	RPCSpanName = "telegram.rpc.server"
	// UnknownRPCMethod is the one value used for malformed and unregistered
	// constructors.
	UnknownRPCMethod = "unknown"
	unknownRPCMethod = UnknownRPCMethod

	defaultRPCTraceQueueCapacity = 64
	defaultRPCTraceShutdown      = time.Second
	maxRPCTraceMethodLength      = 64
)

// RPCResultClass is the closed set of outcomes recorded for one RPC.
type RPCResultClass string

const (
	RPCResultSuccess          RPCResultClass = "success"
	RPCResultInvalidRequest   RPCResultClass = "invalid_request"
	RPCResultUnauthenticated  RPCResultClass = "unauthenticated"
	RPCResultUnauthorized     RPCResultClass = "unauthorized"
	RPCResultRateLimited      RPCResultClass = "rate_limited"
	RPCResultDeadline         RPCResultClass = "deadline"
	RPCResultInternal         RPCResultClass = "internal"
	RPCResultTransportFailure RPCResultClass = "transport_failure"
)

// RPCSpan is the complete server-side trace record. Its fields are fixed and
// deliberately contain no request, response, identity, address, payload, or
// error data.
type RPCSpan struct {
	Name     string
	Method   string
	Duration time.Duration
	Result   RPCResultClass
}

// RPCSpanExporter receives already-redacted spans. This package provides no
// network exporter or endpoint configuration; callers should use an in-memory
// exporter for tests and local diagnostics.
type RPCSpanExporter interface {
	Export(context.Context, RPCSpan) error
}

// RPCSpanExporterFunc adapts a function to RPCSpanExporter.
type RPCSpanExporterFunc func(context.Context, RPCSpan) error

// Export implements RPCSpanExporter.
func (f RPCSpanExporterFunc) Export(ctx context.Context, span RPCSpan) error {
	return f(ctx, span)
}

// RPCTracerConfig configures the bounded in-process trace recorder.
//
// A nil Exporter is the disabled state. In that state NewRPCTracer starts no
// worker and TraceRPCs adds no request-path work.
type RPCTracerConfig struct {
	Exporter        RPCSpanExporter
	QueueCapacity   int
	ShutdownTimeout time.Duration
	Now             func() time.Time
}

// RPCTracerSnapshot contains fixed, unlabeled recorder counters.
type RPCTracerSnapshot struct {
	Exported       int64
	Dropped        int64
	ExporterErrors int64
	ExporterPanics int64
}

// ErrRPCTracerShutdownTimeout reports that a supplied exporter did not finish
// its bounded best-effort shutdown window.
var ErrRPCTracerShutdownTimeout = errors.New("rpc tracer shutdown timeout")

// ErrRPCSpanExporterFull is returned by the bounded in-memory exporter when
// its fixed retention capacity is full.
var ErrRPCSpanExporterFull = errors.New("rpc span exporter full")

// RPCTracer records a fixed-size asynchronous stream of redacted RPC spans.
// The queue is never closed, which keeps concurrent recorders safe while
// shutdown drains or drops the remaining bounded contents.
type RPCTracer struct {
	exporter        RPCSpanExporter
	queue           chan RPCSpan
	stop            chan struct{}
	done            chan struct{}
	cancel          context.CancelFunc
	shutdownTimeout time.Duration
	now             func() time.Time
	closeOnce       sync.Once
	active          atomic.Bool

	exported       atomic.Int64
	dropped        atomic.Int64
	exporterErrors atomic.Int64
	exporterPanics atomic.Int64
}

// NewRPCTracer creates a bounded tracer. A nil exporter creates a disabled
// tracer and no goroutine. Zero queue and shutdown settings use safe bounded
// defaults; an exporter is never invoked synchronously by an RPC.
func NewRPCTracer(cfg RPCTracerConfig) *RPCTracer {
	if cfg.Exporter == nil {
		return &RPCTracer{}
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = defaultRPCTraceQueueCapacity
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultRPCTraceShutdown
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	t := &RPCTracer{
		exporter:        cfg.Exporter,
		queue:           make(chan RPCSpan, cfg.QueueCapacity),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		cancel:          cancel,
		shutdownTimeout: cfg.ShutdownTimeout,
		now:             cfg.Now,
	}
	t.active.Store(true)
	go t.run(workerCtx)
	return t
}

// NewRPCTracerWithExporter creates an enabled tracer with default bounds.
func NewRPCTracerWithExporter(exporter RPCSpanExporter) *RPCTracer {
	return NewRPCTracer(RPCTracerConfig{Exporter: exporter})
}

// Enabled reports whether this tracer has an exporter and has not begun
// shutdown.
func (t *RPCTracer) Enabled() bool {
	return t != nil && t.active.Load()
}

// Snapshot returns the fixed recorder counters.
func (t *RPCTracer) Snapshot() RPCTracerSnapshot {
	if t == nil {
		return RPCTracerSnapshot{}
	}
	return RPCTracerSnapshot{
		Exported:       t.exported.Load(),
		Dropped:        t.dropped.Load(),
		ExporterErrors: t.exporterErrors.Load(),
		ExporterPanics: t.exporterPanics.Load(),
	}
}

// Start begins a span without allocating any span state when tracing is
// disabled.
func (t *RPCTracer) Start() *rpcSpanHandle {
	if !t.Enabled() {
		return nil
	}
	return &rpcSpanHandle{tracer: t, started: t.now()}
}

// Finish completes a span started by Start. Method and result are reduced to
// the fixed vocabulary before they enter the bounded queue.
func (t *RPCTracer) Finish(handle *rpcSpanHandle, method string, result RPCResultClass) {
	if handle == nil || handle.tracer != t || !handle.finished.CompareAndSwap(false, true) {
		return
	}
	duration := max(t.now().Sub(handle.started), 0)
	t.Record(RPCSpan{
		Name:     RPCSpanName,
		Method:   method,
		Duration: duration,
		Result:   result,
	})
}

// Record enqueues an already bounded span. It is nonblocking and drops when
// the fixed queue is full.
func (t *RPCTracer) Record(span RPCSpan) {
	if !t.Enabled() {
		return
	}
	span = sanitizeRPCSpan(span)
	select {
	case t.queue <- span:
	default:
		t.dropped.Add(1)
	}
}

// Wrap applies the tracing boundary to a handler.
func (t *RPCTracer) Wrap(next Handler) Handler {
	return TraceRPCs(next, t)
}

// Close stops recording and waits for the configured bounded best-effort
// drain. Exporter errors, panics, and a timeout never escape as request errors.
func (t *RPCTracer) Close() error {
	if t == nil || t.done == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.active.Store(false)
		close(t.stop)
	})
	timer := time.NewTimer(t.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-t.done:
		return nil
	case <-timer.C:
		t.cancel()
		return ErrRPCTracerShutdownTimeout
	}
}

// Shutdown is the context-bounded form of Close.
func (t *RPCTracer) Shutdown(ctx context.Context) error {
	if t == nil || t.done == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.active.Store(false)
		close(t.stop)
	})
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		t.cancel()
		return ctx.Err()
	}
}

type rpcSpanHandle struct {
	tracer   *RPCTracer
	started  time.Time
	finished atomic.Bool
}

func (t *RPCTracer) run(workerCtx context.Context) {
	defer close(t.done)
	for {
		select {
		case span := <-t.queue:
			if !t.active.Load() {
				// A span that was still queued must use a fresh bounded shutdown
				// context instead of the worker context used during normal serving.
				t.exportShutdownSpan(span)
				t.drain()
				return
			}
			t.export(workerCtx, span)
		case <-t.stop:
			t.drain()
			return
		}
	}
}

func (t *RPCTracer) exportShutdownSpan(span RPCSpan) {
	ctx, cancel := context.WithTimeout(context.Background(), t.shutdownTimeout)
	defer cancel()
	t.export(ctx, span)
}

func (t *RPCTracer) drain() {
	ctx, cancel := context.WithTimeout(context.Background(), t.shutdownTimeout)
	defer cancel()
	for {
		select {
		case span := <-t.queue:
			if ctx.Err() != nil {
				return
			}
			t.export(ctx, span)
		default:
			return
		}
	}
}

func (t *RPCTracer) export(ctx context.Context, span RPCSpan) {
	defer func() {
		if recover() != nil {
			t.exporterPanics.Add(1)
		}
	}()
	if err := t.exporter.Export(ctx, span); err != nil {
		t.exporterErrors.Add(1)
		return
	}
	t.exported.Add(1)
}

func sanitizeRPCSpan(span RPCSpan) RPCSpan {
	span.Name = RPCSpanName
	span.Method = sanitizeRPCMethod(span.Method)
	span.Result = sanitizeRPCResult(span.Result)
	if span.Duration < 0 {
		span.Duration = 0
	}
	return span
}

func sanitizeRPCMethod(method string) string {
	if method == "" || method == UnknownRPCMethod || len(method) > maxRPCTraceMethodLength || !strings.Contains(method, ".") {
		return UnknownRPCMethod
	}
	if _, ok := compiledMethodVocabulary[method]; !ok {
		return UnknownRPCMethod
	}
	for i := range method {
		if method[i] < '!' || method[i] > '~' {
			return UnknownRPCMethod
		}
	}
	return method
}

func sanitizeRPCResult(result RPCResultClass) RPCResultClass {
	switch result {
	case RPCResultSuccess, RPCResultInvalidRequest, RPCResultUnauthenticated,
		RPCResultUnauthorized, RPCResultRateLimited, RPCResultDeadline,
		RPCResultInternal, RPCResultTransportFailure:
		return result
	default:
		return RPCResultInternal
	}
}

// ClassifyRPCError maps server-visible RPC errors to the fixed result classes.
// It never returns error text or codes to the trace record.
func ClassifyRPCError(err error) RPCResultClass {
	if err == nil {
		return RPCResultSuccess
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RPCResultDeadline
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return RPCResultTransportFailure
	}
	rpcErr, ok := errors.AsType[*tgerr.Error](err)
	if ok {
		switch {
		case rpcErr.Code == 420 || rpcErr.Type == "FLOOD_WAIT":
			return RPCResultRateLimited
		case rpcErr.Code == 401:
			return RPCResultUnauthenticated
		case rpcErr.Code == 403:
			return RPCResultUnauthorized
		case rpcErr.Code == 400:
			return RPCResultInvalidRequest
		case rpcErr.Code >= 500:
			return RPCResultInternal
		default:
			return RPCResultInternal
		}
	}
	return RPCResultInternal
}

func requestRPCResult(req *Request, err error) RPCResultClass {
	if req != nil && req.rpcResult != "" {
		result := req.rpcResult
		if result == RPCResultInternal && req.Ctx != nil && errors.Is(req.Ctx.Err(), context.DeadlineExceeded) {
			return RPCResultDeadline
		}
		if err != nil && result == RPCResultSuccess {
			return ClassifyRPCError(err)
		}
		return sanitizeRPCResult(result)
	}
	if req != nil && req.Ctx != nil && errors.Is(req.Ctx.Err(), context.DeadlineExceeded) {
		return RPCResultDeadline
	}
	return ClassifyRPCError(err)
}

func requestRPCMethod(req *Request) string {
	if req != nil && req.rpcMethod != "" {
		return req.rpcMethod
	}
	return UnknownRPCMethod
}

// TraceRPCs records one span around each request handed to next. The request's
// dispatcher-selected method is used after wrappers have been peeled, so
// invokeWithLayer and similar wrappers cannot create extra spans or labels.
func TraceRPCs(next Handler, tracer *RPCTracer) Handler {
	if existing, ok := next.(*rpcTraceHandler); ok {
		next = existing.next
	}
	if next == nil || !tracer.Enabled() {
		return next
	}
	return &rpcTraceHandler{next: next, tracer: tracer}
}

type rpcTraceHandler struct {
	next   Handler
	tracer *RPCTracer
}

func (h *rpcTraceHandler) OnMessage(c *Conn, req *Request) (err error) {
	handle := h.tracer.Start()
	defer func() {
		h.tracer.Finish(handle, requestRPCMethod(req), requestRPCResult(req, err))
	}()
	return h.next.OnMessage(c, req)
}

// MemoryRPCSpanExporter retains a fixed number of spans for tests and local
// diagnostics. It has no network or disk behavior.
type MemoryRPCSpanExporter struct {
	mu       sync.Mutex
	capacity int
	spans    []RPCSpan
}

// NewMemoryRPCSpanExporter creates a fixed-capacity in-memory exporter.
func NewMemoryRPCSpanExporter(capacity int) *MemoryRPCSpanExporter {
	if capacity <= 0 {
		capacity = defaultRPCTraceQueueCapacity
	}
	return &MemoryRPCSpanExporter{capacity: capacity, spans: make([]RPCSpan, 0, capacity)}
}

// NewInMemoryRPCSpanExporter is an explicit alias for test call sites.
func NewInMemoryRPCSpanExporter(capacity int) *MemoryRPCSpanExporter {
	return NewMemoryRPCSpanExporter(capacity)
}

// Export implements RPCSpanExporter.
func (e *MemoryRPCSpanExporter) Export(ctx context.Context, span RPCSpan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.spans) >= e.capacity {
		return ErrRPCSpanExporterFull
	}
	e.spans = append(e.spans, sanitizeRPCSpan(span))
	return nil
}

// Spans returns a snapshot of the retained spans.
func (e *MemoryRPCSpanExporter) Spans() []RPCSpan {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]RPCSpan(nil), e.spans...)
}

// Capacity reports the fixed retention capacity.
func (e *MemoryRPCSpanExporter) Capacity() int {
	if e == nil {
		return 0
	}
	return e.capacity
}
