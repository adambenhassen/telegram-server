package admin_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// sseTestBroadcaster builds a Broadcaster over a canned sampler and renderer,
// runs it for the lifetime of the test, and returns it.
func sseTestBroadcaster(t *testing.T, cfg admin.BroadcasterConfig) *admin.Broadcaster {
	t.Helper()

	if cfg.Sample == nil {
		cfg.Sample = func(context.Context) (admin.MetricsResponse, error) {
			return admin.MetricsResponse{Connections: 7}, nil
		}
	}
	if cfg.Render == nil {
		cfg.Render = func(m admin.MetricsResponse) ([]admin.Fragment, error) {
			// Event left empty on purpose: the stream tests then assert the
			// real default event name rather than a name local to the test.
			return []admin.Fragment{{
				HTML: "<span id=\"v-connections\">" + admin.FmtInt(int64(m.Connections)) + "</span>",
			}}, nil
		}
	}
	if cfg.Interval == 0 {
		cfg.Interval = 20 * time.Millisecond
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 20 * time.Millisecond
	}

	b := admin.NewBroadcaster(cfg)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Broadcaster.Run did not return after context cancel")
		}
	})

	return b
}

// readSSEUntil reads from r until want appears or the deadline passes.
func readSSEUntil(t *testing.T, r io.Reader, want string, timeout time.Duration) string {
	t.Helper()

	type result struct {
		text string
		err  error
	}
	ch := make(chan result, 1)

	go func() {
		var sb strings.Builder
		br := bufio.NewReader(r)
		for {
			line, err := br.ReadString('\n')
			sb.WriteString(line)
			if strings.Contains(sb.String(), want) {
				ch <- result{sb.String(), nil}
				return
			}
			if err != nil {
				ch <- result{sb.String(), err}
				return
			}
		}
	}()

	select {
	case res := <-ch:
		if !strings.Contains(res.text, want) {
			t.Fatalf("stream did not contain %q (err=%v); got:\n%s", want, res.err, res.text)
		}
		return res.text
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %q in stream", want)
		return ""
	}
}

// waitForClients polls until the broadcaster reports n subscribers.
func waitForClients(t *testing.T, b *admin.Broadcaster, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if b.Clients() == n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected %d SSE clients, got %d", n, b.Clients())
}

// TestSSE_streams_events verifies the SSE mechanics: the correct content type,
// no caching, and a named event carrying the rendered fragment.
func TestSSE_streams_events(t *testing.T) {
	t.Parallel()

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	if ab := resp.Header.Get("X-Accel-Buffering"); ab != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", ab)
	}

	got := readSSEUntil(t, resp.Body, "v-connections", 5*time.Second)
	if !strings.Contains(got, "event: "+admin.SSEDefaultEvent()) {
		t.Errorf("stream missing event name; got:\n%s", got)
	}
	if !strings.Contains(got, "retry: ") {
		t.Errorf("stream missing reconnect hint; got:\n%s", got)
	}
	if !strings.Contains(got, "data: fragments <span id=\"v-connections\">7</span>") {
		t.Errorf("stream missing rendered fragment; got:\n%s", got)
	}
}

// TestSSE_multiline_fragment_is_framed_per_line verifies that a fragment
// spanning several lines is emitted as one data: line per source line, which is
// what the EventSource spec requires for the client to reassemble it.
func TestSSE_multiline_fragment_is_framed_per_line(t *testing.T) {
	t.Parallel()

	got := string(admin.EncodeFragment(admin.Fragment{
		Event: "custom-event",
		HTML:  "<div>\r\n  <span>1</span>\n</div>",
	}))

	want := "event: custom-event\n" +
		"data: selector #metrics-stream\n" +
		"data: mergeMode morph\n" +
		"data: fragments <div>\n" +
		"data: fragments   <span>1</span>\n" +
		"data: fragments </div>\n\n"
	if got != want {
		t.Errorf("EncodeFragment =\n%q\nwant\n%q", got, want)
	}
}

// TestSSE_heartbeat_keeps_idle_stream_open verifies that a stream with no
// metric changes still emits keepalive comments, so an idle proxy does not cut
// the connection.
func TestSSE_heartbeat_keeps_idle_stream_open(t *testing.T) {
	t.Parallel()

	// A sampler that always fails means no events are ever emitted; only the
	// heartbeat can keep the stream alive.
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			return admin.MetricsResponse{}, errors.New("db down")
		},
	})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 despite sampler failure, got %d", resp.StatusCode)
	}

	got := readSSEUntil(t, resp.Body, ": keepalive", 5*time.Second)
	if strings.Contains(got, "event: ") {
		t.Errorf("sampler failed but an event was emitted:\n%s", got)
	}
}

// TestSSE_disconnect_releases_subscriber verifies that a client going away
// tears the subscription down instead of leaking a goroutine per dead
// connection.
func TestSSE_disconnect_releases_subscriber(t *testing.T) {
	t.Parallel()

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}

	readSSEUntil(t, resp.Body, "v-connections", 5*time.Second)
	waitForClients(t, b, 1, 5*time.Second)

	cancel()
	_ = resp.Body.Close() //nolint:errcheck // best-effort close

	waitForClients(t, b, 0, 5*time.Second)
}

// TestSSE_shutdown_closes_streams verifies that cancelling the broadcaster
// context ends every open stream rather than leaving connections hanging.
func TestSSE_shutdown_closes_streams(t *testing.T) {
	t.Parallel()

	b := admin.NewBroadcaster(admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			return admin.MetricsResponse{Connections: 1}, nil
		},
		Render: func(admin.MetricsResponse) ([]admin.Fragment, error) {
			return []admin.Fragment{{HTML: "<b>hi</b>"}}, nil
		},
		Interval:  20 * time.Millisecond,
		Heartbeat: time.Hour,
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	go b.Run(runCtx)

	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		cancelRun()
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancelRun()
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

	readSSEUntil(t, resp.Body, "<b>hi</b>", 5*time.Second)
	waitForClients(t, b, 1, 5*time.Second)

	cancelRun()

	// The response body must reach EOF: the handler returned and the stream
	// was closed cleanly.
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read after shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream stayed open after broadcaster shutdown")
	}

	waitForClients(t, b, 0, 5*time.Second)
}

// TestSSE_caps_concurrent_streams verifies the hard cap on simultaneous
// streams: over the cap the endpoint refuses with 503 and a Retry-After rather
// than accumulating connections.
func TestSSE_caps_concurrent_streams(t *testing.T) {
	t.Parallel()

	const maxStreams = 2
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{MaxClients: maxStreams})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	ctx := t.Context()

	for i := range maxStreams {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("new request %d: %v", i, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stream %d: expected 200, got %d", i, resp.StatusCode)
		}
		readSSEUntil(t, resp.Body, "v-connections", 5*time.Second)
	}
	waitForClients(t, b, maxStreams, 5*time.Second)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect over cap: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("over cap: expected 503, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("over cap: expected a Retry-After header")
	}
	if b.Clients() != maxStreams {
		t.Errorf("refused stream still counted: %d clients, want %d", b.Clients(), maxStreams)
	}
}

// TestSSE_one_sampler_for_all_clients verifies the fan-out property: N
// connected clients cost one sample per interval, not N.
func TestSSE_one_sampler_for_all_clients(t *testing.T) {
	t.Parallel()

	var samples atomic.Int64
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			samples.Add(1)
			return admin.MetricsResponse{Connections: 7}, nil
		},
		Interval: 50 * time.Millisecond,
	})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	ctx := t.Context()

	const clients = 4
	for i := range clients {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("new request %d: %v", i, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close
		readSSEUntil(t, resp.Body, "v-connections", 5*time.Second)
	}
	waitForClients(t, b, clients, 5*time.Second)

	before := samples.Load()
	time.Sleep(250 * time.Millisecond)
	elapsedSamples := samples.Load() - before

	// Five intervals elapsed, so a shared sampler takes roughly five samples.
	// A per-connection loop would take four times that. The bound is loose
	// enough to survive a slow CI box but still fails the per-connection shape.
	if elapsedSamples > 12 {
		t.Errorf("took %d samples for %d clients over ~5 intervals; sampler is not shared", elapsedSamples, clients)
	}
}

// TestSSE_idle_broadcaster_does_not_query verifies that with nobody connected
// the shared sampler stays off the database entirely.
func TestSSE_idle_broadcaster_does_not_query(t *testing.T) {
	t.Parallel()

	var samples atomic.Int64
	sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			samples.Add(1)
			return admin.MetricsResponse{}, nil
		},
		Interval: 10 * time.Millisecond,
	})

	time.Sleep(150 * time.Millisecond)

	if n := samples.Load(); n != 0 {
		t.Errorf("idle broadcaster sampled %d times, want 0", n)
	}
}

// TestSSE_rejects_non_get verifies the endpoint only answers GET.
func TestSSE_rejects_non_get(t *testing.T) {
	t.Parallel()

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/events", nil)
	rec := httptest.NewRecorder()
	admin.EventsHandler(b).ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: expected 405, got %d", rec.Code)
	}
}

// TestSSE_disabled_without_broadcaster verifies the route reports itself
// unavailable rather than panicking when no broadcaster is wired up.
func TestSSE_disabled_without_broadcaster(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/events", nil)
	rec := httptest.NewRecorder()
	admin.EventsHandler(nil).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("nil broadcaster: expected 503, got %d", rec.Code)
	}
}

// TestSSE_stream_lifetime_is_bounded verifies that a stream is recycled before
// the admin session idle timeout can expire underneath it. The reconnect the
// client then makes runs through RequireAdmin again, which is what refreshes
// the session's last-activity timestamp.
func TestSSE_stream_lifetime_is_bounded(t *testing.T) {
	t.Parallel()

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		MaxStreamDuration: 100 * time.Millisecond,
		Heartbeat:         time.Hour,
	})
	srv := httptest.NewServer(admin.EventsHandler(b))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // best-effort close

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream outlived its maximum duration")
	}

	waitForClients(t, b, 0, 5*time.Second)

	if admin.SSEMaxStreamDuration() >= admin.IdleTimeout() {
		t.Errorf("default stream lifetime %v must stay under the session idle timeout %v",
			admin.SSEMaxStreamDuration(), admin.IdleTimeout())
	}
}

// TestSSE_route_behind_admin_gate verifies the stream composes with the rest of
// the admin surface: RequireAdmin gates it, and the MAIN-261 security headers
// still apply without breaking the streaming response.
func TestSSE_route_behind_admin_gate(t *testing.T) {
	t.Parallel()

	st := newAuthTestStore(t)
	rawToken := "gate-test-token"
	tokenHash := sha256hex([]byte(rawToken))

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Interval:          30 * time.Millisecond,
		Heartbeat:         30 * time.Millisecond,
		MaxStreamDuration: 400 * time.Millisecond,
	})
	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
		Logger:    slog.Default(),
		Events:    b,
	}, mtproto.NewSessionRegistry())

	// Without a session the gate rejects the stream before the handler runs.
	unauth := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/events", nil)
	unauthRec := httptest.NewRecorder()
	h.ServeHTTP(unauthRec, unauth)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated stream: expected 401, got %d", unauthRec.Code)
	}

	sessionID := loginAndGetSession(t, h, rawToken)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/events", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated stream: expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if !rec.Flushed {
		t.Error("stream was never flushed; events would sit in the response buffer")
	}
	for header, want := range map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"Content-Security-Policy": "frame-ancestors 'none'",
		"Cache-Control":           "no-store",
	} {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	body := rec.Body.String()
	// Past first paint: the stream must keep producing through the middleware
	// chain, not just deliver the snapshot it opened with.
	if n := strings.Count(body, "event: "+admin.SSEDefaultEvent()); n < 2 {
		t.Errorf("only %d events reached the client through the router; the stream stopped after first paint:\n%s", n, body)
	}
	if !strings.Contains(body, ": keepalive") {
		t.Errorf("no keepalive reached the client through the router:\n%s", body)
	}
}

// TestSSE_default_contract_is_the_dashboard_contract pins the event name and
// patch target agreed with MAIN-302. Datastar answers an unknown event name or
// a selector that hits nothing with a 200 and no patch — a page that never
// updates — so a rename on either side has to break a test here.
func TestSSE_default_contract_is_the_dashboard_contract(t *testing.T) {
	t.Parallel()

	if got := admin.SSEDefaultEvent(); got != "datastar-merge-fragments" {
		t.Errorf("default event name = %q, want datastar-merge-fragments", got)
	}

	fragments, err := admin.DefaultFragmentRenderer(admin.MetricsResponse{Connections: 3})
	if err != nil {
		t.Fatalf("DefaultFragmentRenderer: %v", err)
	}
	if len(fragments) != 1 {
		t.Fatalf("DefaultFragmentRenderer returned %d fragments, want 1", len(fragments))
	}
	if fragments[0].Event != "" && fragments[0].Event != admin.SSEDefaultEvent() {
		t.Errorf("fragment event = %q, want %q", fragments[0].Event, admin.SSEDefaultEvent())
	}
	// The selector must address the element the fragment replaces: a selector
	// drift between the two sides freezes the dashboard silently.
	if fragments[0].Selector != "" && fragments[0].Selector != "#metrics-stream" {
		t.Errorf("fragment selector = %q, want #metrics-stream", fragments[0].Selector)
	}
	if !strings.Contains(fragments[0].HTML, `id="metrics-stream"`) {
		t.Errorf("fragment does not carry the agreed patch target id:\n%s", fragments[0].HTML)
	}

	// DashboardFragmentRenderer, not DefaultFragmentRenderer, is what the
	// server wires in production, so it is the one that decides whether the
	// live dashboard updates. It has to answer to the same contract.
	prod, err := admin.DashboardFragmentRenderer(admin.MetricsResponse{Connections: 3})
	if err != nil {
		t.Fatalf("DashboardFragmentRenderer: %v", err)
	}
	if len(prod) != 1 {
		t.Fatalf("DashboardFragmentRenderer returned %d fragments, want 1", len(prod))
	}
	if prod[0].Event != "" && prod[0].Event != admin.SSEDefaultEvent() {
		t.Errorf("production fragment event = %q, want %q", prod[0].Event, admin.SSEDefaultEvent())
	}
	if prod[0].Selector != "" && prod[0].Selector != "#metrics-stream" {
		t.Errorf("production fragment selector = %q, want #metrics-stream", prod[0].Selector)
	}
	assertSingleRootWithTargetID(t, "DashboardFragmentRenderer", prod[0].HTML)
	if !strings.Contains(prod[0].HTML, `id="v-connections"`) {
		t.Errorf("production fragment does not carry the first-paint metric ids:\n%s", prod[0].HTML)
	}
	assertSingleRootWithTargetID(t, "DefaultFragmentRenderer", fragments[0].HTML)

	// An empty Event on the wire must resolve to the Datastar event name and
	// the dashboard's target selector.
	wire := string(admin.EncodeFragment(admin.Fragment{HTML: "<i>x</i>"}))
	if !strings.HasPrefix(wire, "event: datastar-merge-fragments\n") {
		t.Errorf("unnamed fragment encoded as %q", wire)
	}
	if !strings.Contains(wire, "data: selector #metrics-stream\n") {
		t.Errorf("unnamed fragment did not pin the dashboard selector:\n%s", wire)
	}
}

// assertSingleRootWithTargetID fails unless html is exactly one element and
// that element carries the id the selector addresses.
//
// The bundle merges every top-level node of a fragment into the same selector
// in turn, so a fragment of sibling sections is applied section by section and
// only the last one survives: the dashboard loses most of its cards on the
// first tick while the stream, the event name and the selector all still look
// correct. Nothing but a structural check catches that.
func assertSingleRootWithTargetID(t *testing.T, name, fragment string) {
	t.Helper()

	nodes, err := html.ParseFragment(strings.NewReader(fragment), &html.Node{
		Type:     html.ElementNode,
		Data:     "body",
		DataAtom: atom.Body,
	})
	if err != nil {
		t.Fatalf("%s: parsing fragment: %v", name, err)
	}

	var roots []*html.Node
	for _, n := range nodes {
		if n.Type == html.ElementNode {
			roots = append(roots, n)
		}
	}
	if len(roots) != 1 {
		names := make([]string, 0, len(roots))
		for _, n := range roots {
			names = append(names, n.Data)
		}
		t.Fatalf("%s: fragment has %d top-level elements (%s), want exactly 1 — every one after the first overwrites it",
			name, len(roots), strings.Join(names, ", "))
	}

	var id string
	for _, a := range roots[0].Attr {
		if a.Key == "id" {
			id = a.Val
		}
	}
	if id != "metrics-stream" {
		t.Errorf("%s: fragment root id = %q, want metrics-stream", name, id)
	}
}

// TestSSE_slow_reader_gets_the_latest_snapshot verifies the fan-out contract for
// a client that stops draining: the broadcaster replaces the queued payload
// instead of blocking on it or growing a backlog, so the reader resumes on
// current values rather than replaying history.
func TestSSE_slow_reader_gets_the_latest_snapshot(t *testing.T) {
	t.Parallel()

	var sampleNo atomic.Int64
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			return admin.MetricsResponse{Connections: int(sampleNo.Add(1))}, nil
		},
		Interval: 10 * time.Millisecond,
	})

	ch, _, unsubscribe, err := b.SubscribeForTest()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Never read while the sampler runs, so every fan-out finds the buffer full.
	deadline := time.Now().Add(2 * time.Second)
	for sampleNo.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	produced := sampleNo.Load()
	if produced < 5 {
		t.Fatalf("sampler only produced %d snapshots", produced)
	}

	// Unsubscribing stops further fan-out to this channel under the same lock
	// fanout takes, so what remains in it is exactly what was queued.
	unsubscribe()

	var queued [][]byte
	for {
		select {
		case payload := <-ch:
			queued = append(queued, payload)
			continue
		default:
		}
		break
	}

	if len(queued) != 1 {
		t.Fatalf("%d payloads queued for a reader that never drained, want 1: a backlog accumulated", len(queued))
	}

	// The one queued payload is a recent snapshot, not the first that arrived
	// and then sat there while five more were produced.
	got := string(queued[0])
	if !strings.Contains(got, ">"+admin.FmtInt(produced)+"<") &&
		!strings.Contains(got, ">"+admin.FmtInt(produced-1)+"<") {
		t.Errorf("slow reader was served a stale snapshot after %d samples:\n%s", produced, got)
	}
}

// TestSSE_wakes_sampler_when_first_client_returns verifies that the sampler is
// woken on the 0-to-1 subscriber transition, not only on the very first
// connection. Without it, a client reconnecting after an idle period would be
// served whatever stale snapshot the previous client left behind and would wait
// a full interval for real data.
func TestSSE_wakes_sampler_when_first_client_returns(t *testing.T) {
	t.Parallel()

	var samples atomic.Int64
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			samples.Add(1)
			return admin.MetricsResponse{Connections: int(samples.Load())}, nil
		},
		// Long enough that a wake, not the ticker, has to be what samples.
		Interval: time.Hour,
	})

	_, _, unsubscribe, err := b.SubscribeForTest()
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	waitForSamples(t, &samples, 1, 2*time.Second)

	// Everyone leaves, then someone comes back.
	unsubscribe()
	waitForClients(t, b, 0, 2*time.Second)

	_, _, unsubscribe2, err := b.SubscribeForTest()
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}
	defer unsubscribe2()

	waitForSamples(t, &samples, 2, 2*time.Second)
}

// TestSSE_extra_clients_do_not_wake_the_sampler verifies the other half of that
// rule: connections 2..N reuse the last snapshot, so a crowd of tabs — or a
// reconnect loop — cannot multiply the query load.
func TestSSE_extra_clients_do_not_wake_the_sampler(t *testing.T) {
	t.Parallel()

	var samples atomic.Int64
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample: func(context.Context) (admin.MetricsResponse, error) {
			samples.Add(1)
			return admin.MetricsResponse{Connections: 1}, nil
		},
		Interval: time.Hour,
	})

	_, _, unsubscribe, err := b.SubscribeForTest()
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	defer unsubscribe()
	waitForSamples(t, &samples, 1, 2*time.Second)

	for i := range 5 {
		_, last, drop, err := b.SubscribeForTest()
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		defer drop()
		if last == nil {
			t.Errorf("client %d was not served the last snapshot", i)
		}
	}

	time.Sleep(100 * time.Millisecond)
	if n := samples.Load(); n != 1 {
		t.Errorf("5 extra clients drove %d samples, want 1", n)
	}
}

// waitForSamples polls until the sampler has run at least n times.
func waitForSamples(t *testing.T, samples *atomic.Int64, n int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if samples.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected at least %d samples, got %d", n, samples.Load())
}

// TestSSE_sampler_fails_snapshot_on_partial_query verifies the difference
// between the two metric surfaces when the pts-gap query fails.
//
// The JSON endpoint degrades that field to zero, which is safe for a poller:
// the next poll 10s later corrects it. The stream cannot do that. It only emits
// on a successful sample and its contract is that a failed sample leaves the
// last values in place, so a zeroed MaxPtsGap riding a fresh timestamp would
// push "every client is caught up" and nothing would ever contradict it.
func TestSSE_sampler_fails_snapshot_on_partial_query(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() }) //nolint:errcheck // best-effort close

	registry := mtproto.NewSessionRegistry()

	// Both surfaces agree while every query works.
	if _, err := admin.CollectMetricsStrict(ctx, registry, st); err != nil {
		t.Fatalf("healthy strict collect: %v", err)
	}

	// Break only what MaxPtsGap reads; the rest of the snapshot still resolves.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, `ALTER TABLE update_state RENAME TO update_state_hidden`); err != nil {
		t.Fatalf("break pts gap query: %v", err)
	}

	if _, err := admin.CollectMetricsStrict(ctx, registry, st); err == nil {
		t.Error("SSE sampler returned a snapshot with a failed pts-gap query; it would push a false zero")
	}

	tolerant, err := admin.CollectMetricsTolerant(ctx, registry, st)
	if err != nil {
		t.Fatalf("GET /admin/metrics behaviour changed: %v", err)
	}
	if tolerant.MaxPtsGap != 0 {
		t.Errorf("tolerant MaxPtsGap = %d, want 0", tolerant.MaxPtsGap)
	}
	if tolerant.TotalUsers < 0 {
		t.Errorf("tolerant snapshot is not otherwise populated: %+v", tolerant)
	}
}
