package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// Server-sent-events defaults for GET /admin/events.
const (
	// sseDefaultEvent is the event name used when a Fragment leaves it empty.
	// It is the name Datastar's MergeFragments plugin registers on: any
	// datastar-prefixed event the bundle does not know is dispatched but
	// handled by nothing.
	//
	// The event name and the fragment's selector are the contract with
	// MAIN-302, and the dashboard page is the side that must be matched: an
	// unknown event name or a selector that hits nothing is answered with a
	// 200 and no patch, so a mismatch reads as a frozen dashboard rather than
	// an error anyone would notice.
	sseDefaultEvent = "datastar-merge-fragments"

	// sseDefaultSelector is the element a fragment without its own selector
	// patches. It is the same target the dashboard declares for first paint
	// and must stay in lockstep with it; TestSSE_default_contract_is_the_
	// dashboard_contract asserts both sides.
	sseDefaultSelector = "#metrics-stream"

	// sseDefaultMode is the merge mode applied to the selector, carried on
	// the mergeMode data line. inner keeps the element the first paint
	// created (listeners and attributes intact) and replaces only its
	// children, which is what the dashboard wants.
	sseDefaultMode = "inner"

	// sseInterval is the shared sampler's cadence. One sample per interval
	// serves every connected client.
	sseInterval = 10 * time.Second

	// sseHeartbeat is how often an idle stream emits a comment line. Proxies
	// commonly cut a connection that has been silent for 30-60 s.
	sseHeartbeat = 15 * time.Second

	// sseRetryHint is the reconnect delay advertised to the browser.
	sseRetryHint = 5 * time.Second

	// sseCapRetryAfter is the backoff sent with the 503 that a refused stream
	// gets. It is deliberately much longer than sseRetryHint: hitting the cap
	// is not a transient condition, and a client retrying every 5 s would spin
	// against a full server.
	sseCapRetryAfter = 60 * time.Second

	// sseMaxStreamDuration bounds a single stream's lifetime. It must stay
	// below idleTimeout: RequireAdmin refreshes a session's last-activity
	// timestamp per request, and a stream that outlived the idle timeout
	// would reconnect into a session that had expired underneath it. Recycling
	// the connection first turns each reconnect into that refreshing request.
	sseMaxStreamDuration = 25 * time.Minute

	// sseMaxClients caps concurrent streams across the whole admin server.
	// This is an internal tool with a handful of operators; the cap exists so
	// a runaway client cannot pin an unbounded number of connections.
	sseMaxClients = 32
)

// Fragment is one server-rendered HTML update addressed to a named SSE event.
// Datastar's MergeFragments plugin merges the HTML into the element the
// selector names.
type Fragment struct {
	// Event is the SSE event name. Empty means sseDefaultEvent.
	Event string
	// Selector targets the element to merge into. Empty means
	// sseDefaultSelector.
	Selector string
	// MergeMode is the merge mode Datastar applies (morph, inner, outer,
	// ...). Empty means sseDefaultMode.
	MergeMode string
	// HTML is the rendered fragment. It is written verbatim, so the renderer
	// owns escaping.
	HTML string
}

// FragmentRenderer turns a metrics snapshot into the fragments to push. It is
// the seam between this endpoint and the dashboard's markup: the endpoint owns
// SSE mechanics and the sampling cadence, the renderer owns what the fragments
// contain and which events carry them.
type FragmentRenderer func(MetricsResponse) ([]Fragment, error)

// Sampler collects one metrics snapshot.
type Sampler func(context.Context) (MetricsResponse, error)

// NewMetricsSampler returns a Sampler reading the same snapshot that
// GET /admin/metrics serves.
func NewMetricsSampler(registry *mtproto.SessionRegistry, st *store.Store) Sampler {
	return func(ctx context.Context) (MetricsResponse, error) {
		// requireAllMetrics: the stream stays silent on a partial snapshot
		// rather than pushing a metric that reads as good news.
		return collectMetrics(ctx, registry, st, requireAllMetrics)
	}
}

// BroadcasterConfig configures a Broadcaster. Every duration and bound has a
// default; the zero value of the struct is usable apart from Sample and Render.
type BroadcasterConfig struct {
	// Sample collects the snapshot pushed to clients.
	Sample Sampler
	// Render turns a snapshot into fragments. Defaults to DefaultFragmentRenderer.
	Render FragmentRenderer
	// Interval is the sampling cadence. Defaults to sseInterval.
	Interval time.Duration
	// Heartbeat is the idle keepalive cadence. Defaults to sseHeartbeat.
	Heartbeat time.Duration
	// MaxStreamDuration bounds one stream's lifetime. Defaults to
	// sseMaxStreamDuration.
	MaxStreamDuration time.Duration
	// MaxClients caps concurrent streams. Defaults to sseMaxClients.
	MaxClients int
	// Logger is used for sampling and rendering failures. Defaults to
	// slog.Default().
	Logger *slog.Logger
}

// subscriber is one connected SSE stream. The channel carries pre-encoded SSE
// payloads; it is buffered by one so a slow reader delays only itself, and the
// broadcaster replaces a queued payload rather than blocking on it — every
// payload is a full snapshot, so the latest one supersedes the last.
type subscriber struct {
	ch chan []byte
}

// Broadcaster samples metrics on a single shared cadence and fans the rendered
// fragments out to every connected stream. It exists so N open dashboards cost
// one query set per interval rather than N.
//
// Run owns the sampling goroutine; the HTTP handler only reads from its
// subscription. Nothing samples while no client is connected.
type Broadcaster struct {
	sample    Sampler
	render    FragmentRenderer
	interval  time.Duration
	heartbeat time.Duration
	maxStream time.Duration
	maxClient int
	logger    *slog.Logger

	// wake asks the sampler for an off-cadence sample. It is buffered by one,
	// so concurrent first connections collapse into a single extra sample.
	wake chan struct{}

	mu sync.Mutex
	// subs holds the live subscriptions. The mutex also guards sends on each
	// subscriber channel, so a channel is never closed while a send is in
	// flight: only closeAll closes them, and only for subs still in this map.
	subs   map[*subscriber]struct{}
	last   []byte
	closed bool
}

// NewBroadcaster builds a Broadcaster. Call Run to start sampling.
func NewBroadcaster(cfg BroadcasterConfig) *Broadcaster {
	b := &Broadcaster{
		sample:    cfg.Sample,
		render:    cfg.Render,
		interval:  cfg.Interval,
		heartbeat: cfg.Heartbeat,
		maxStream: cfg.MaxStreamDuration,
		maxClient: cfg.MaxClients,
		logger:    cfg.Logger,
		wake:      make(chan struct{}, 1),
		subs:      make(map[*subscriber]struct{}),
	}
	if b.render == nil {
		b.render = DefaultFragmentRenderer
	}
	if b.interval <= 0 {
		b.interval = sseInterval
	}
	if b.heartbeat <= 0 {
		b.heartbeat = sseHeartbeat
	}
	if b.maxStream <= 0 {
		b.maxStream = sseMaxStreamDuration
	}
	if b.maxClient <= 0 {
		b.maxClient = sseMaxClients
	}
	if b.logger == nil {
		b.logger = slog.Default()
	}
	return b
}

// Run samples and fans out until ctx is cancelled, then closes every open
// stream. It blocks, so callers run it in its own goroutine tied to the admin
// server's lifetime.
func (b *Broadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	defer b.closeAll()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.wake:
			b.tick(ctx)
		case <-ticker.C:
			b.tick(ctx)
		}
	}
}

// Clients reports the number of open streams.
func (b *Broadcaster) Clients() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// tick collects one snapshot and pushes it. A sampling or rendering failure
// leaves the last good payload in place: clients keep their current values and
// the dashboard's freshness threshold is what surfaces the staleness.
func (b *Broadcaster) tick(ctx context.Context) {
	if b.Clients() == 0 {
		return
	}
	if b.sample == nil {
		return
	}

	m, err := b.sample(ctx)
	if err != nil {
		b.logger.Error("admin sse sample", "err", err)
		return
	}

	fragments, err := b.render(m)
	if err != nil {
		b.logger.Error("admin sse render", "err", err)
		return
	}

	var payload []byte
	for _, f := range fragments {
		payload = append(payload, encodeFragment(f)...)
	}
	if len(payload) == 0 {
		return
	}

	b.fanout(payload)
}

// fanout records the payload as the latest snapshot and delivers it to every
// subscriber, replacing any payload still queued for a slow reader.
func (b *Broadcaster) fanout(payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.last = payload
	for sub := range b.subs {
		select {
		case sub.ch <- payload:
		default:
			// Reader is behind. Drop the superseded payload and queue this one.
			select {
			case <-sub.ch:
			default:
			}
			select {
			case sub.ch <- payload:
			default:
			}
		}
	}
}

// errTooManyStreams is returned by subscribe when the concurrent stream cap is
// reached.
var errTooManyStreams = errors.New("admin sse: stream cap reached")

// errBroadcasterClosed is returned by subscribe after Run has stopped.
var errBroadcasterClosed = errors.New("admin sse: broadcaster closed")

// subscribe registers a stream and returns it along with the latest payload, so
// a client that connects mid-cadence renders immediately instead of waiting a
// full interval.
func (b *Broadcaster) subscribe() (*subscriber, []byte, error) {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return nil, nil, errBroadcasterClosed
	}
	if len(b.subs) >= b.maxClient {
		b.mu.Unlock()
		return nil, nil, errTooManyStreams
	}

	sub := &subscriber{ch: make(chan []byte, 1)}
	b.subs[sub] = struct{}{}
	last := b.last
	firstClient := len(b.subs) == 1
	b.mu.Unlock()

	// Waking the sampler on the 0-to-1 transition covers both the very first
	// connection and a reconnect after the broadcaster has been idle, where
	// last is however old the last client left it. Connections 2..N reuse last
	// untouched, and the buffered wake collapses a burst into one sample, so
	// neither reconnect churn nor a crowd of tabs multiplies the query load.
	//
	// last is still served immediately even when stale: it paints the page now
	// and the fresh sample this wake triggers replaces it a moment later. The
	// fragment carries the server timestamp, so a snapshot that never refreshes
	// surfaces on the dashboard's freshness threshold rather than passing for
	// live data.
	if firstClient {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}

	return sub, last, nil
}

// unsubscribe removes a stream. Calling it after closeAll is a no-op.
func (b *Broadcaster) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, sub)
}

// closeAll ends every open stream and refuses further subscriptions.
func (b *Broadcaster) closeAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	for sub := range b.subs {
		close(sub.ch)
		delete(b.subs, sub)
	}
}

// encodeFragment serialises one fragment as a Datastar SSE event. The
// MergeFragments plugin parses the data lines into keys: selector, mergeMode
// and fragments. Multi-line HTML needs one data: line per source line and
// Datastar rejoins the lines of the same key with newlines.
func encodeFragment(f Fragment) []byte {
	event := f.Event
	if event == "" {
		event = sseDefaultEvent
	}
	selector := f.Selector
	if selector == "" {
		selector = sseDefaultSelector
	}
	mergeMode := f.MergeMode
	if mergeMode == "" {
		mergeMode = sseDefaultMode
	}

	html := strings.ReplaceAll(f.HTML, "\r\n", "\n")
	html = strings.ReplaceAll(html, "\r", "\n")

	var buf bytes.Buffer
	buf.WriteString("event: ")
	buf.WriteString(event)
	buf.WriteByte('\n')
	buf.WriteString("data: selector ")
	buf.WriteString(selector)
	buf.WriteByte('\n')
	buf.WriteString("data: mergeMode ")
	buf.WriteString(mergeMode)
	buf.WriteByte('\n')
	for line := range strings.SplitSeq(html, "\n") {
		buf.WriteString("data: fragments ")
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	return buf.Bytes()
}

// EventsHandler returns the handler for GET /admin/events. It is registered
// behind RequireAdmin like every other admin route, so an unauthenticated
// request never reaches the stream.
//
// A nil Broadcaster means live updates are not wired up; the route reports
// itself unavailable rather than 404ing, so the dashboard degrades to its
// server-rendered first paint.
func EventsHandler(b *Broadcaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if b == nil {
			http.Error(w, "events unavailable", http.StatusServiceUnavailable)
			return
		}

		// Without a Flusher every event would sit in the response buffer until
		// the handler returned, which for a stream is never.
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		sub, last, err := b.subscribe()
		if err != nil {
			w.Header().Set("Retry-After", strconv.Itoa(int(sseCapRetryAfter.Seconds())))
			http.Error(w, "too many streams", http.StatusServiceUnavailable)
			return
		}
		defer b.unsubscribe(sub)

		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-store")
		// Tell nginx and friends not to buffer the response; without it a
		// reverse proxy can hold events back until its own buffer fills.
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		if _, err := fmt.Fprintf(w, "retry: %d\n\n", sseRetryHint.Milliseconds()); err != nil {
			return
		}
		flusher.Flush()

		if last != nil {
			if _, err := w.Write(last); err != nil {
				return
			}
			flusher.Flush()
		}

		heartbeat := time.NewTicker(b.heartbeat)
		defer heartbeat.Stop()
		lifetime := time.NewTimer(b.maxStream)
		defer lifetime.Stop()

		for {
			select {
			case <-r.Context().Done():
				// Client went away, or the server is shutting the request down.
				return
			case <-lifetime.C:
				// Stream recycled; the client reconnects after sseRetryHint.
				return
			case payload, open := <-sub.ch:
				if !open {
					// Broadcaster shut down.
					return
				}
				if _, err := w.Write(payload); err != nil {
					return
				}
				flusher.Flush()
			case <-heartbeat.C:
				if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// DefaultFragmentRenderer renders the dashboard's metric values as a single
// datastar-merge-fragments event. It keeps the endpoint useful and testable on
// its own; the dashboard supplies its own renderer for its own markup.
func DefaultFragmentRenderer(m MetricsResponse) ([]Fragment, error) {
	// The logout form's CSRF token is session-bound and deliberately absent
	// from a fragment that is rendered once and fanned out to every client.
	data := BuildDashboardData(m, "")

	var buf bytes.Buffer
	if err := metricsFragmentTmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("render metrics fragment: %w", err)
	}
	return []Fragment{{Event: sseDefaultEvent, HTML: buf.String()}}, nil
}

var metricsFragmentTmpl = template.Must(template.New("metrics-fragment").Parse(metricsFragmentHTML))

// metricsFragmentHTML mirrors the data-metric attributes the dashboard uses, so
// a patch target can address the same values it server-rendered on first paint.
// The wrapper id is the patch target agreed on MAIN-302 and is asserted by
// TestSSE_default_contract_is_the_dashboard_contract.
const metricsFragmentHTML = `<div id="metrics-stream" data-timestamp="{{.ServerTimestamp}}">` +
	`<span data-metric="connections">{{.Connections}}</span>` +
	`<span data-metric="sessions">{{.Sessions}}</span>` +
	`<span data-metric="messages_1h">{{.Messages1H}}</span>` +
	`<span data-metric="max_pts_gap">{{.MaxPtsGap}}</span>` +
	`<span data-metric="total_users">{{.TotalUsers}}</span>` +
	`<span data-metric="active_users_1h">{{.ActiveUsers1H}}</span>` +
	`<span data-metric="active_users_24h">{{.ActiveUsers24H}}</span>` +
	`<span data-metric="total_channels">{{.TotalChannels}}</span>` +
	`<span data-metric="total_chats">{{.TotalChats}}</span>` +
	`<span data-metric="messages_24h">{{.Messages24H}}</span>` +
	`<span data-metric="rate_limit_active">{{.RateLimitActive}}</span>` +
	`</div>`
