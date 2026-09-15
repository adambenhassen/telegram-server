package e2e_test

import (
	"bufio"
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const observabilitySentinel = "MAIN744_SENTINEL_PAYLOAD_998877665544332211"

type observabilityLogState struct {
	mu    sync.Mutex
	lines []string
}

type observabilityLogHandler struct {
	state  *observabilityLogState
	code   *multiCodeSink
	attrs  []slog.Attr
	groups []string
}

func newObservabilityLogHandler(code *multiCodeSink) *observabilityLogHandler {
	return &observabilityLogHandler{state: &observabilityLogState{}, code: code}
}

func (h *observabilityLogHandler) Logger() *slog.Logger { return slog.New(h) }

func (h *observabilityLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *observabilityLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &observabilityLogHandler{
		state:  h.state,
		code:   h.code,
		attrs:  append(append([]slog.Attr(nil), h.attrs...), attrs...),
		groups: append([]string(nil), h.groups...),
	}
}

func (h *observabilityLogHandler) WithGroup(group string) slog.Handler {
	return &observabilityLogHandler{
		state:  h.state,
		code:   h.code,
		attrs:  append([]slog.Attr(nil), h.attrs...),
		groups: append(append([]string(nil), h.groups...), group),
	}
}

func (h *observabilityLogHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})

	var phone, code string
	var line strings.Builder
	line.WriteString(record.Message)
	for _, attr := range attrs {
		attr.Value = attr.Value.Resolve()
		if attr.Key == "" {
			continue
		}
		value := attr.Value.String()
		if attr.Key == "phone" {
			phone = value
		}
		if attr.Key == "code" {
			code = value
		}
		line.WriteByte(' ')
		line.WriteString(attr.Key)
		line.WriteByte('=')
		line.WriteString(value)
	}
	if phone != "" && code != "" && h.code != nil {
		select {
		case h.code.chFor(phone) <- code:
		default:
		}
	}

	h.state.mu.Lock()
	h.state.lines = append(h.state.lines, line.String())
	h.state.mu.Unlock()
	return nil
}

func (h *observabilityLogHandler) String() string {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	return strings.Join(h.state.lines, "\n")
}

type observabilityCollector struct {
	messages chan *tg.Message
	points   chan int
}

func newObservabilityCollector() *observabilityCollector {
	return &observabilityCollector{
		messages: make(chan *tg.Message, 64),
		points:   make(chan int, 64),
	}
}

func (c *observabilityCollector) Handle(_ context.Context, updates tg.UpdatesClass) error {
	switch updates := updates.(type) {
	case *tg.Updates:
		for _, update := range updates.Updates {
			c.dispatch(update)
		}
	case *tg.UpdateShort:
		c.dispatch(updates.Update)
	}
	return nil
}

func (c *observabilityCollector) dispatch(update tg.UpdateClass) {
	newMessage, ok := update.(*tg.UpdateNewMessage)
	if !ok {
		return
	}
	message, ok := newMessage.Message.(*tg.Message)
	if !ok {
		return
	}
	send(c.messages, message)
	send(c.points, newMessage.Pts)
}

type observabilityReplica struct {
	st           *store.Store
	dsn          string
	registry     *mtproto.SessionRegistry
	log          *slog.Logger
	serverCancel context.CancelFunc
	serveErr     chan error
	listenerStop func() error
}

func startObservabilityReplica(
	t *testing.T,
	ctx context.Context,
	key *rsa.PrivateKey,
	dcID int,
	st *store.Store,
	dsn string,
	log *slog.Logger,
	ln net.Listener,
	metrics *store.NotificationMetrics,
) *observabilityReplica {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, testBlobs(t), 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed, metrics)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	updater := api.NewUpdater(st, server.Registry(), log, pgtest.PeerDeriver(), metrics)
	_, stopListener, err := store.StartListener(ctx, dsn, updater.Deliver, updater.DeliverTyping, updater.Evict, updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus, updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned, log, metrics)
	if err != nil {
		t.Fatalf("start observability listener: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(serverCtx, ln)
	}()

	return &observabilityReplica{
		st:           st,
		dsn:          dsn,
		registry:     server.Registry(),
		log:          log,
		serverCancel: serverCancel,
		serveErr:     serveErr,
		listenerStop: stopListener,
	}
}

func (r *observabilityReplica) stopListener(t *testing.T) {
	t.Helper()
	if r.listenerStop == nil {
		return
	}
	stop := r.listenerStop
	r.listenerStop = nil
	if err := stop(); err != nil {
		t.Errorf("listener stop: %v", err)
	}
}

func (r *observabilityReplica) stop(t *testing.T) {
	t.Helper()
	r.stopListener(t)
	if r.serverCancel == nil {
		return
	}
	cancel := r.serverCancel
	r.serverCancel = nil
	cancel()
	select {
	case err := <-r.serveErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			t.Errorf("server serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("server did not stop")
	}
}

func waitObservabilityConn(
	t *testing.T,
	ctx context.Context,
	registry *mtproto.SessionRegistry,
	userID int64,
	within time.Duration,
) *mtproto.Conn {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if conns := registry.Conns(userID); len(conns) > 0 {
			return conns[0]
		}
		select {
		case <-poll.C:
		case <-ctx.Done():
			t.Fatalf("waiting for user %d connection: %v", userID, ctx.Err())
			return nil
		case <-deadline.C:
			t.Fatalf("user %d connection not registered within %s", userID, within)
			return nil
		}
	}
}

type observabilityPushTelemetry struct {
	PushLatencySampleCount int64              `json:"push_latency_sample_count"`
	PushOutcomes           admin.PushOutcomes `json:"push_outcomes"`
}

func observabilityAdminHandler(
	st *store.Store,
	registry *mtproto.SessionRegistry,
	identity admin.ProcessIdentity,
	lagSampler *admin.DeliveryLagSampler,
	metrics *store.NotificationMetrics,
	tokenHash string,
	log *slog.Logger,
) http.Handler {
	cache := admin.NewMetricsSnapshotCache(registry, st, identity, lagSampler, metrics)
	return admin.AdminRouter(admin.LoginHandlerConfig{
		Store:         st,
		TokenHash:     tokenHash,
		Logger:        log,
		Metrics:       cache,
		NotifyMetrics: metrics,
		DeliveryLag:   lagSampler,
	}, registry)
}

func requestObservabilityMetrics(
	t *testing.T,
	ctx context.Context,
	httpHandler http.Handler,
	sessionID string,
) (string, admin.MetricsResponse, http.Header) {
	t.Helper()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	if sessionID != "" {
		request.AddCookie(admin.SessionCookie(sessionID))
	}
	recorder := httptest.NewRecorder()
	httpHandler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /admin/metrics status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
	var response admin.MetricsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode /admin/metrics: %v", err)
	}
	return recorder.Body.String(), response, recorder.Header().Clone()
}

func readObservabilitySSEUntil(t *testing.T, ctx context.Context, reader io.Reader, marker string, timeout time.Duration) string {
	t.Helper()
	type result struct {
		body string
		err  error
	}
	results := make(chan result, 1)
	go func() {
		var body strings.Builder
		stream := bufio.NewReader(reader)
		for {
			line, err := stream.ReadString('\n')
			body.WriteString(line)
			if strings.Contains(body.String(), marker) {
				results <- result{body: body.String()}
				return
			}
			if err != nil {
				results <- result{body: body.String(), err: err}
				return
			}
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("read admin SSE: %v", result.err)
		}
		return result.body
	case <-ctx.Done():
		t.Fatalf("admin SSE context ended: %v", ctx.Err())
	case <-timer.C:
		t.Fatalf("timed out waiting for admin SSE marker %q", marker)
	}
	return ""
}

func observabilityScriptJSON(t *testing.T, body, scriptID string) []byte {
	t.Helper()
	startTag := `<script id="` + scriptID + `" type="application/json">`
	start := strings.Index(body, startTag)
	if start < 0 {
		t.Fatalf("SSE body has no %s script: %q", scriptID, body)
	}
	start += len(startTag)
	end := strings.Index(body[start:], "</script>")
	if end < 0 {
		t.Fatalf("SSE %s script is unterminated", scriptID)
	}
	return []byte(body[start : start+end])
}

func assertObservabilityNoSentinel(t *testing.T, name, body, sentinel string) {
	t.Helper()
	if strings.Contains(body, sentinel) {
		t.Fatalf("%s contains message sentinel", name)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &fields); err == nil {
		for field := range fields {
			if strings.Contains(field, sentinel) {
				t.Fatalf("%s contains sentinel in metric field name %q", name, field)
			}
		}
	}
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func TestCrossReplicaObservabilityMetrics(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	stA, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stA.Close(); err != nil {
			t.Errorf("store A close: %v", err)
		}
	})
	stB, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stB.Close(); err != nil {
			t.Errorf("store B close: %v", err)
		}
	})

	const dcID = 2
	const phoneA = "+15551284401"
	const phoneB = "+15551284402"
	codes := newMultiCodeSink()
	logAHandler := newObservabilityLogHandler(codes)
	logBHandler := newObservabilityLogHandler(codes)
	logA, logB := logAHandler.Logger(), logBHandler.Logger()

	userA, err := stA.CreateUser(ctx, phoneA)
	if err != nil {
		t.Fatalf("create user A: %v", err)
	}
	userB, err := stA.CreateUser(ctx, phoneB)
	if err != nil {
		t.Fatalf("create user B: %v", err)
	}

	// SendMessage commits the event and account head independently of Notify.
	// These warm-up events establish a non-zero live-delivery baseline while the
	// receiving replica is already connected, without making the final sample
	// depend on callback timing.
	for i := 1; i <= 41; i++ {
		_, senderPts, recipientPts, duplicate, err := stA.SendMessage(ctx, userA.ID, userB.ID, fmt.Sprintf("warmup-%02d", i), int64(i), 0, 0)
		if err != nil {
			t.Fatalf("warm-up message %d: %v", i, err)
		}
		if duplicate {
			t.Fatalf("warm-up message %d was unexpectedly duplicate", i)
		}
		if senderPts != i || recipientPts != i {
			t.Fatalf("warm-up message %d points = sender %d recipient %d, want %d", i, senderPts, recipientPts, i)
		}
	}

	lnA := mustListen(t, ctx, "127.0.0.1:0")
	lnB := mustListen(t, ctx, "127.0.0.1:0")
	metricsA := store.NewNotificationMetrics()
	replicaA := startObservabilityReplica(t, ctx, key, dcID, stA, dsn, logA, lnA, metricsA)
	t.Cleanup(func() { replicaA.stop(t) })
	replicaB := startObservabilityReplica(t, ctx, key, dcID, stB, dsn, logB, lnB, nil)
	t.Cleanup(func() { replicaB.stop(t) })

	if err := store.WaitForNotificationListener(ctx, stB, 2); err != nil {
		t.Fatalf("wait for both listeners: %v", err)
	}

	collector := newObservabilityCollector()
	clientB := telegram.NewClient(1, "hash", telegram.Options{
		DC:            dcID,
		DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: tcpPort(t, lnB)}}},
		PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
		Resolver:      dcs.Plain(dcs.PlainOptions{}),
		UpdateHandler: collector,
	})
	bCmds := make(chan command)
	bID := make(chan int64, 1)
	bErr := make(chan error, 1)
	go func() {
		bErr <- runInteractive(ctx, clientB, auth.NewFlow(
			auth.Constant(phoneB, "", auth.CodeAuthenticatorFunc(func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx, phoneB)
			})),
			auth.SendCodeOptions{},
		), bID, bCmds)
	}()
	t.Cleanup(func() {
		close(bCmds)
		select {
		case err := <-bErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("client B: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("client B did not stop")
		}
	})
	if got := recvOrCtx(t, ctx, bID, "client B login"); got != userB.ID {
		t.Fatalf("client B id = %d, want %d", got, userB.ID)
	}
	bConn := waitObservabilityConn(t, ctx, replicaB.registry, userB.ID, 5*time.Second)
	if got := len(replicaB.registry.Conns(userB.ID)); got != 1 {
		t.Fatalf("replica B live connections = %d, want 1", got)
	}

	// Delivering the committed warm-up events proves callback ordering and
	// gives the receiving connection an authoritative watermark of 41.
	if err := stA.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(userB.ID, 10)); err != nil {
		t.Fatalf("notify warm-up events: %v", err)
	}
	for i := 1; i <= 41; i++ {
		message := recvOrCtx(t, ctx, collector.messages, fmt.Sprintf("warm-up message %d", i))
		if message.Message != fmt.Sprintf("warmup-%02d", i) {
			t.Fatalf("warm-up message %d = %q", i, message.Message)
		}
		if got := recvOrCtx(t, ctx, collector.points, fmt.Sprintf("warm-up point %d", i)); got != i {
			t.Fatalf("warm-up point %d = %d", i, got)
		}
	}
	if got := bConn.LastPushedPts(); got != 41 {
		t.Fatalf("warm-up watermark = %d, want 41", got)
	}

	// Restart only the receiving listener with metrics enabled. This keeps the
	// final counters specific to the one notification under test.
	replicaB.stopListener(t)
	metricsB := store.NewNotificationMetrics()
	updaterB := api.NewUpdater(stB, replicaB.registry, logB, pgtest.PeerDeriver(), metricsB)
	_, stopListenerB, err := store.StartListener(ctx, dsn, updaterB.Deliver, updaterB.DeliverTyping, updaterB.Evict, updaterB.DeliverChannelPost, updaterB.DeliverEncryption, updaterB.DeliverStatus, updaterB.DeliverEncryptedMsg, updaterB.DeliverReactions, updaterB.DeliverPinned, logB, metricsB)
	if err != nil {
		t.Fatalf("start measured listener: %v", err)
	}
	replicaB.listenerStop = stopListenerB
	if err := store.WaitForNotificationListener(ctx, stB, 2); err != nil {
		t.Fatalf("wait for measured listeners: %v", err)
	}

	const rawAdminToken = "main-744-admin-token" //nolint:gosec // G101: test credential
	const adminSessionID = "main-744-session"
	tokenHash := sha256Hex(rawAdminToken)
	tokenFingerprint, err := admin.TokenFingerprint(tokenHash)
	if err != nil {
		t.Fatalf("token fingerprint: %v", err)
	}
	now := time.Now().UTC()
	if err := stB.CreateAdminSession(ctx, admin.HashSessionID(adminSessionID), tokenFingerprint, now.Add(time.Hour), now); err != nil {
		t.Fatalf("create admin session: %v", err)
	}
	identity, err := admin.NewProcessIdentity("main-744-replica-b")
	if err != nil {
		t.Fatalf("process identity: %v", err)
	}
	lagSampler := admin.NewDeliveryLagSampler()

	unauthenticatedHandler := observabilityAdminHandler(stB, replicaB.registry, identity, lagSampler, metricsB, tokenHash, logB)
	unauthenticatedRequest := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	unauthenticatedRecorder := httptest.NewRecorder()
	unauthenticatedHandler.ServeHTTP(unauthenticatedRecorder, unauthenticatedRequest)
	if unauthenticatedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics status = %d, want %d", unauthenticatedRecorder.Code, http.StatusUnauthorized)
	}
	if !strings.Contains(unauthenticatedRecorder.Header().Get("Cache-Control"), "no-store") {
		t.Fatalf("unauthenticated metrics Cache-Control = %q, want no-store", unauthenticatedRecorder.Header().Get("Cache-Control"))
	}

	metricsHandler := func() http.Handler {
		return observabilityAdminHandler(stB, replicaB.registry, identity, lagSampler, metricsB, tokenHash, logB)
	}
	baselineBody, baseline, baselineHeaders := requestObservabilityMetrics(t, ctx, metricsHandler(), adminSessionID)
	if baselineHeaders.Get("Cache-Control") != "no-store" {
		t.Fatalf("authenticated metrics Cache-Control = %q, want no-store", baselineHeaders.Get("Cache-Control"))
	}
	if baseline.NotifyCount != 0 || baseline.NotifyChannels != (admin.NotifyChannels{}) {
		t.Fatalf("baseline notification metrics = %+v, want all zero", baseline)
	}
	if baseline.PushLatencySampleCount != 0 {
		t.Fatalf("baseline push samples = %d, want 0", baseline.PushLatencySampleCount)
	}
	if baseline.DeliveryLag.State != admin.DeliveryLagAvailable || baseline.DeliveryLag.Coverage != admin.DeliveryLagCoverageFull || baseline.DeliveryLag.WorstPts == nil || *baseline.DeliveryLag.WorstPts != 0 {
		t.Fatalf("baseline delivery lag = %+v, want available/full/0", baseline.DeliveryLag)
	}
	assertObservabilityNoSentinel(t, "baseline metrics", baselineBody, observabilitySentinel)

	_, senderPts, recipientPts, duplicate, err := stA.SendMessage(ctx, userA.ID, userB.ID, observabilitySentinel, 42, 0, 0)
	if err != nil {
		t.Fatalf("commit sentinel message: %v", err)
	}
	if duplicate || senderPts != 42 || recipientPts != 42 {
		t.Fatalf("sentinel message result = duplicate %v sender pts %d recipient pts %d, want false/42/42", duplicate, senderPts, recipientPts)
	}
	stateBefore, err := stB.State(ctx, userB.ID)
	if err != nil {
		t.Fatalf("read state before notify: %v", err)
	}
	if stateBefore.Pts != 42 {
		t.Fatalf("account head before notify = %d, want 42", stateBefore.Pts)
	}
	laggedBody, lagged, _ := requestObservabilityMetrics(t, ctx, metricsHandler(), adminSessionID)
	if lagged.NotifyCount != 0 || lagged.PushLatencySampleCount != 0 {
		t.Fatalf("lagged notification metrics = %+v, want all zero", lagged)
	}
	if lagged.DeliveryLag.State != admin.DeliveryLagAvailable || lagged.DeliveryLag.Coverage != admin.DeliveryLagCoverageFull || lagged.DeliveryLag.WorstPts == nil || *lagged.DeliveryLag.WorstPts != 1 || lagged.DeliveryLag.EligibleConnections != 1 || lagged.DeliveryLag.SampledConnections != 1 {
		t.Fatalf("lagged delivery lag = %+v, want available/full/1 with one connection", lagged.DeliveryLag)
	}
	assertObservabilityNoSentinel(t, "lagged metrics", laggedBody, observabilitySentinel)

	if err := stA.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(userB.ID, 10)); err != nil {
		t.Fatalf("notify sentinel message: %v", err)
	}
	message := recvOrCtx(t, ctx, collector.messages, "sentinel update")
	if message.Message != observabilitySentinel {
		t.Fatalf("delivered message = %q, want sentinel", message.Message)
	}
	if got := recvOrCtx(t, ctx, collector.points, "sentinel pts"); got != 42 {
		t.Fatalf("delivered pts = %d, want 42", got)
	}
	if got := bConn.LastPushedPts(); got != 42 {
		t.Fatalf("final watermark = %d, want 42", got)
	}

	duplicateTimer := time.NewTimer(500 * time.Millisecond)
	select {
	case extra := <-collector.messages:
		duplicateTimer.Stop()
		t.Fatalf("unexpected duplicate client update %q", extra.Message)
	case <-duplicateTimer.C:
	}

	finalBody, final, finalHeaders := requestObservabilityMetrics(t, ctx, metricsHandler(), adminSessionID)
	if finalHeaders.Get("Cache-Control") != "no-store" {
		t.Fatalf("final metrics Cache-Control = %q, want no-store", finalHeaders.Get("Cache-Control"))
	}
	if final.NotifyCount != 1 || final.NotifyInvalid != 0 || final.NotifyChannels != (admin.NotifyChannels{Updates: 1}) {
		t.Fatalf("final notification metrics = %+v, want one tg_updates notification", final)
	}
	if final.PushLatencySampleCount != 1 || final.PushOutcomes != (admin.PushOutcomes{Success: 1}) {
		t.Fatalf("final push metrics = samples %d outcomes %+v, want one success", final.PushLatencySampleCount, final.PushOutcomes)
	}
	if final.PushLatencyP50 <= 0 || final.PushLatencyP95 <= 0 {
		t.Fatalf("final push percentiles = p50 %.2f p95 %.2f, want positive samples", final.PushLatencyP50, final.PushLatencyP95)
	}
	var pushSamples int64
	for _, count := range final.PushLatencyBucketCounts {
		pushSamples += count
	}
	if pushSamples != 1 {
		t.Fatalf("final push bucket samples = %d, want 1", pushSamples)
	}
	if final.DeliveryLag.State != admin.DeliveryLagAvailable || final.DeliveryLag.Coverage != admin.DeliveryLagCoverageFull || final.DeliveryLag.WorstPts == nil || *final.DeliveryLag.WorstPts != 0 || final.DeliveryLag.SampledAt == nil {
		t.Fatalf("final delivery lag = %+v, want available/full/0", final.DeliveryLag)
	}
	if len(final.Uninstrumented) != 0 {
		t.Fatalf("final uninstrumented metrics = %v, want none", final.Uninstrumented)
	}
	assertObservabilityNoSentinel(t, "final metrics", finalBody, observabilitySentinel)

	stateAfter, err := stB.State(ctx, userB.ID)
	if err != nil {
		t.Fatalf("read state after notify: %v", err)
	}
	if stateAfter.Pts != 42 {
		t.Fatalf("account head after notify = %d, want 42", stateAfter.Pts)
	}
	history, err := stB.History(ctx, userB.ID, store.PeerTypeUser, userA.ID, 0, 100)
	if err != nil {
		t.Fatalf("read message history: %v", err)
	}
	if len(history) != 42 {
		t.Fatalf("message history len = %d, want 42", len(history))
	}
	sentinelCount := 0
	for _, item := range history {
		if item.Text == observabilitySentinel {
			sentinelCount++
		}
	}
	if sentinelCount != 1 {
		t.Fatalf("sentinel history count = %d, want 1", sentinelCount)
	}

	sseCache := admin.NewMetricsSnapshotCache(replicaB.registry, stB, identity, lagSampler, metricsB)
	broadcaster := admin.NewBroadcaster(admin.BroadcasterConfig{
		Sample:            sseCache.Snapshot,
		Render:            admin.DefaultFragmentRenderer,
		Interval:          time.Hour,
		Heartbeat:         time.Hour,
		MaxStreamDuration: 5 * time.Second,
		Logger:            logB,
	})
	sseCtx, sseCancel := context.WithCancel(ctx)
	sseDone := make(chan struct{})
	go func() {
		broadcaster.Run(sseCtx)
		close(sseDone)
	}()
	sseHandler := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:         stB,
		TokenHash:     tokenHash,
		Logger:        logB,
		Events:        broadcaster,
		Metrics:       sseCache,
		NotifyMetrics: metricsB,
		DeliveryLag:   lagSampler,
	}, replicaB.registry)
	sseServer := httptest.NewServer(sseHandler)
	t.Cleanup(func() {
		sseCancel()
		sseServer.Close()
		select {
		case <-sseDone:
		case <-time.After(5 * time.Second):
			t.Errorf("SSE broadcaster did not stop")
		}
	})
	sseRequest, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseServer.URL+"/admin/events", nil)
	if err != nil {
		t.Fatalf("create SSE request: %v", err)
	}
	sseRequest.AddCookie(admin.SessionCookie(adminSessionID))
	sseResponse, err := http.DefaultClient.Do(sseRequest)
	if err != nil {
		t.Fatalf("GET /admin/events: %v", err)
	}
	defer sseResponse.Body.Close() //nolint:errcheck // test cleanup also closes the server
	if sseResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/events status = %d", sseResponse.StatusCode)
	}
	if !strings.HasPrefix(sseResponse.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("SSE Content-Type = %q", sseResponse.Header.Get("Content-Type"))
	}
	if sseResponse.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("SSE Cache-Control = %q, want no-store", sseResponse.Header.Get("Cache-Control"))
	}
	sseBody := readObservabilitySSEUntil(t, sseCtx, sseResponse.Body, `<script id="delivery-lag-telemetry" type="application/json">`, 5*time.Second)
	assertObservabilityNoSentinel(t, "SSE metrics", sseBody, observabilitySentinel)
	var sseLag admin.DeliveryLag
	if err := json.Unmarshal(observabilityScriptJSON(t, sseBody, "delivery-lag-telemetry"), &sseLag); err != nil {
		t.Fatalf("decode SSE delivery lag: %v", err)
	}
	if sseLag.State != admin.DeliveryLagAvailable || sseLag.Coverage != admin.DeliveryLagCoverageFull || sseLag.WorstPts == nil || *sseLag.WorstPts != 0 {
		t.Fatalf("SSE delivery lag = %+v, want available/full/0", sseLag)
	}
	var ssePush observabilityPushTelemetry
	if err := json.Unmarshal(observabilityScriptJSON(t, sseBody, "push-telemetry"), &ssePush); err != nil {
		t.Fatalf("decode SSE push telemetry: %v", err)
	}
	if ssePush.PushLatencySampleCount != 1 || ssePush.PushOutcomes != (admin.PushOutcomes{Success: 1}) {
		t.Fatalf("SSE push telemetry = %+v, want one success", ssePush)
	}

	if logs := logAHandler.String() + "\n" + logBHandler.String(); strings.Contains(logs, observabilitySentinel) {
		t.Fatalf("replica logs contain message sentinel")
	}
}
