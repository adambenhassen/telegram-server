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

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
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
			return []admin.Fragment{{
				Event: "metrics",
				HTML:  "<span id=\"v-connections\">" + admin.FmtInt(int64(m.Connections)) + "</span>",
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
	if !strings.Contains(got, "event: metrics") {
		t.Errorf("stream missing event name; got:\n%s", got)
	}
	if !strings.Contains(got, "retry: ") {
		t.Errorf("stream missing reconnect hint; got:\n%s", got)
	}
	if !strings.Contains(got, "data: <span id=\"v-connections\">7</span>") {
		t.Errorf("stream missing rendered fragment; got:\n%s", got)
	}
}

// TestSSE_multiline_fragment_is_framed_per_line verifies that a fragment
// spanning several lines is emitted as one data: line per source line, which is
// what the EventSource spec requires for the client to reassemble it.
func TestSSE_multiline_fragment_is_framed_per_line(t *testing.T) {
	t.Parallel()

	got := string(admin.EncodeFragment(admin.Fragment{
		Event: "metrics",
		HTML:  "<div>\r\n  <span>1</span>\n</div>",
	}))

	want := "event: metrics\ndata: <div>\ndata:   <span>1</span>\ndata: </div>\n\n"
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
	if strings.Contains(got, "event: metrics") {
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
			return []admin.Fragment{{Event: "metrics", HTML: "<b>hi</b>"}}, nil
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
		MaxStreamDuration: 200 * time.Millisecond,
		Heartbeat:         time.Hour,
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
	if !strings.Contains(rec.Body.String(), "event: metrics") {
		t.Errorf("no event reached the client through the router:\n%s", rec.Body.String())
	}
}
