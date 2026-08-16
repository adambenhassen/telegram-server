package admin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/admin/assets"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestDashboardHandler_requires_auth(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/dashboard", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without session, got %d", rec.Code)
	}
}

func TestDashboardHandler_returns_html(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	rawToken := "dashboard-test-token"
	tokenHash := sha256hex([]byte(rawToken))
	registry := mtproto.NewSessionRegistry()

	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
	}, registry)

	sessionID := loginAndGetSession(t, h, rawToken)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid session, got %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}

	body := rec.Body.String()

	// Server-rendered metric values are present.
	if !strings.Contains(body, `data-metric="connections"`) {
		t.Error("body missing connections metric element")
	}
	if !strings.Contains(body, `data-metric="total_users"`) {
		t.Error("body missing total_users metric element")
	}

	// CSRF token is embedded for the logout form.
	if !strings.Contains(body, `name="csrf_token"`) {
		t.Error("body missing csrf_token in logout form")
	}

	// The logout token is derived from the session: the dashboard must not
	// issue any CSRF cookie (re-issuing it invalidated other tabs' forms).
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			t.Error("dashboard response set a CSRF cookie; token must be session-derived")
		}
	}

	// Logout form is a POST, not an anchor.
	if !strings.Contains(body, `method="post" action="/admin/logout"`) {
		t.Error("body missing POST logout form")
	}

	// Uninstrumented card is present.
	if !strings.Contains(body, "Not yet instrumented") {
		t.Error("body missing uninstrumented card")
	}

	// No raw -1 values — unknown estimates must be rendered as em-dash.
	if strings.Contains(body, ">-1<") {
		t.Error("body contains raw -1 value; should render as —")
	}

	// SSE live-update endpoint is present.
	if !strings.Contains(body, "/admin/events") {
		t.Error("body missing SSE connect target")
	}
}

func TestDashboardHandler_post_rejected(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	rawToken := "dashboard-post-test-token"
	tokenHash := sha256hex([]byte(rawToken))
	registry := mtproto.NewSessionRegistry()

	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
	}, registry)

	sessionID := loginAndGetSession(t, h, rawToken)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatal("POST /admin/dashboard should not return 200")
	}
}

func TestDashboardData_empty_deployment_alert(t *testing.T) {
	t.Parallel()

	// An empty deployment (all zeros) should show the empty alert.
	m := admin.MetricsResponse{
		Uninstrumented: []string{"notify_count", "push_latency_p50_ms", "push_latency_p95_ms"},
		StorageRows:    admin.StorageRows{Users: -1, Messages: -1},
	}
	data := admin.BuildDashboardData(m, "csrf")
	if !data.ShowEmptyAlert {
		t.Error("ShowEmptyAlert should be true when all three key fields are zero")
	}
}

func TestDashboardData_no_empty_alert_when_active(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{
		TotalUsers:  5,
		Connections: 0,
		Messages24H: 0,
	}
	data := admin.BuildDashboardData(m, "csrf")
	if data.ShowEmptyAlert {
		t.Error("ShowEmptyAlert should be false when total_users > 0")
	}
}

func TestDashboardData_storage_unknown(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{
		StorageRows: admin.StorageRows{Messages: -1, Files: -1},
	}
	data := admin.BuildDashboardData(m, "csrf")

	for _, row := range data.StorageRows {
		if row.Table == "messages" || row.Table == "files" {
			if row.Display != "—" {
				t.Errorf("row %s: Display = %q, want —", row.Table, row.Display)
			}
			if row.Source != "Unknown" {
				t.Errorf("row %s: Source = %q, want Unknown", row.Table, row.Source)
			}
		}
	}
}

func TestDashboardData_storage_estimated(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{
		StorageRows: admin.StorageRows{Messages: 1204880},
	}
	data := admin.BuildDashboardData(m, "csrf")

	for _, row := range data.StorageRows {
		if row.Table == "messages" {
			if !strings.HasPrefix(row.Display, "~") {
				t.Errorf("estimated row should have ~ prefix, got %q", row.Display)
			}
			if row.Source != "Estimated" {
				t.Errorf("Source = %q, want Estimated", row.Source)
			}
		}
	}
}

func TestDashboardData_uninstrumented_driven_by_payload(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{
		Uninstrumented: []string{"notify_count"},
	}
	data := admin.BuildDashboardData(m, "csrf")

	if len(data.UninstrumentedNames) != 1 {
		t.Fatalf("expected 1 uninstrumented entry, got %d", len(data.UninstrumentedNames))
	}
	if data.UninstrumentedNames[0].Field != "notify_count" {
		t.Errorf("Field = %q, want notify_count", data.UninstrumentedNames[0].Field)
	}
}

// TestDashboardHandler_503_on_db_error verifies that a closed store causes
// DashboardHandler to return 503 rather than rendering zeros as real metrics.
// DashboardHandler is called directly (no auth middleware) since we are
// testing handler behavior, not the gate.
func TestDashboardHandler_503_on_db_error(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	registry := mtproto.NewSessionRegistry()
	h := admin.DashboardHandler(registry, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/dashboard", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on DB error, got %d", rec.Code)
	}
}

// TestDashboardHTML_uninstr_scaffold_present_when_empty verifies that the
// uninstrumented card DOM scaffold is always emitted even when the initial
// uninstrumented list is empty, so patchUninstrumented can show it from a
// later poll response without re-inserting DOM nodes.
func TestDashboardHTML_uninstr_scaffold_present_when_empty(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{Uninstrumented: []string{}}
	data := admin.BuildDashboardData(m, "csrf-test")

	var buf strings.Builder
	if err := admin.RenderDashboard(&buf, data); err != nil {
		t.Fatal(err)
	}
	body := buf.String()

	if !strings.Contains(body, `id="uninstr-card"`) {
		t.Error("uninstr-card scaffold absent from HTML when uninstrumented is empty")
	}
	// Card must carry the hidden class so it is not visible on first paint.
	if !strings.Contains(body, `uninstr-card hidden`) && !strings.Contains(body, `uninstr-card" hidden`) {
		t.Error("uninstr-card should carry hidden class when uninstrumented is empty")
	}
}

// TestDashboardHTML_uninstr_scaffold_visible_when_populated verifies that the
// uninstrumented card is visible (no hidden class on the card element) when
// the uninstrumented list is non-empty.
func TestDashboardHTML_uninstr_scaffold_visible_when_populated(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{Uninstrumented: []string{"notify_count"}}
	data := admin.BuildDashboardData(m, "csrf-test")

	var buf strings.Builder
	if err := admin.RenderDashboard(&buf, data); err != nil {
		t.Fatal(err)
	}
	body := buf.String()

	if !strings.Contains(body, `id="uninstr-card"`) {
		t.Error("uninstr-card missing from HTML")
	}
	// The card must not carry the hidden class when fields are listed.
	if strings.Contains(body, `uninstr-card hidden`) || strings.Contains(body, `uninstr-card" hidden`) {
		t.Error("uninstr-card should not be hidden when uninstrumented is non-empty")
	}
	if !strings.Contains(body, "NOTIFY events per hour") {
		t.Error("uninstr-card missing label for notify_count")
	}
}

// TestDashboardHandler_security_headers verifies the required security headers
// are present on every dashboard response.
func TestDashboardHandler_security_headers(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	rawToken := "dashboard-headers-test-token"
	tokenHash := sha256hex([]byte(rawToken))
	registry := mtproto.NewSessionRegistry()

	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
	}, registry)
	sessionID := loginAndGetSession(t, h, rawToken)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := rec.Header().Get("Content-Security-Policy"); !strings.Contains(got, "frame-ancestors") {
		t.Errorf("Content-Security-Policy = %q, missing frame-ancestors", got)
	}
}

// TestRenderFragment_all_zeros verifies that the SSE metrics fragment renders
// without error when all metrics are zero (empty deployment baseline).
func TestRenderFragment_all_zeros(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{}
	data := admin.BuildDashboardData(m, "")

	var buf strings.Builder
	if err := admin.RenderFragment(&buf, data); err != nil {
		t.Fatal(err)
	}
	body := buf.String()

	// The empty-deployment banner must be visible.
	if !strings.Contains(body, `id="banner-empty"`) {
		t.Error("banner-empty element missing from fragment")
	}
	// CSRF token must never appear in the fragment (fanned out to all clients).
	if strings.Contains(body, "csrf_token") {
		t.Error("csrf_token must not appear in SSE fragment")
	}
	// SSE swap target element IDs must be present.
	if !strings.Contains(body, `id="v-connections"`) {
		t.Error("fragment missing v-connections metric element")
	}
}

// TestRenderFragment_non_empty verifies that the fragment renders live metrics
// correctly when the snapshot contains non-zero data.
func TestRenderFragment_non_empty(t *testing.T) {
	t.Parallel()

	m := admin.MetricsResponse{
		Connections:   5,
		Sessions:      3,
		TotalUsers:    100,
		ActiveUsers1H: 12,
	}
	data := admin.BuildDashboardData(m, "")

	var buf strings.Builder
	if err := admin.RenderFragment(&buf, data); err != nil {
		t.Fatal(err)
	}
	body := buf.String()

	// The banner must carry the hidden class when metrics are non-zero.
	bannerIdx := strings.Index(body, `id="banner-empty"`)
	if bannerIdx < 0 {
		t.Fatal("banner-empty element missing from fragment")
	}
	// Find the end of the opening tag to bound the search.
	tagEnd := strings.Index(body[bannerIdx:], ">")
	if tagEnd < 0 {
		tagEnd = 800
	}
	bannerCtx := body[bannerIdx:min(bannerIdx+tagEnd+1, len(body))]
	if !strings.Contains(bannerCtx, "hidden") {
		t.Error("banner-empty should carry hidden class when metrics are non-zero")
	}
	// Connection count must appear.
	if !strings.Contains(body, "5") {
		t.Error("fragment missing connection count")
	}
	// CSRF token must never appear.
	if strings.Contains(body, "csrf_token") {
		t.Error("csrf_token must not appear in SSE fragment")
	}
}

// TestDashboardCSS_defines_the_utilities_the_page_relies_on guards against the
// embedded stylesheet going stale against the templates.
//
// dashboard.css is generated by `make css` and committed. Nothing regenerates
// it automatically, so a template can start using a utility the stylesheet was
// never built with — and the page still renders, just without that rule. That
// is silent for a visual class, but not for `hidden`: the disconnected banner,
// the empty-deployment banner and the uninstrumented card are all shown or
// hidden by toggling it, so a missing rule leaves them permanently visible.
func TestDashboardCSS_defines_the_utilities_the_page_relies_on(t *testing.T) {
	t.Parallel()

	css, err := assets.FS.ReadFile("dashboard.css")
	if err != nil {
		t.Fatalf("reading embedded dashboard.css: %v", err)
	}
	sheet := string(css)

	// hidden is load-bearing: it is the only thing keeping the banners off the
	// page, and it must actually resolve to display:none.
	hidden := regexp.MustCompile(`(^|[,}])\.hidden\{[^}]*display:none`)
	if !hidden.MatchString(sheet) {
		t.Error("dashboard.css has no `.hidden{display:none}` rule — every element the templates hide by toggling that class is permanently visible; regenerate with `make css`")
	}

	// A representative sample of the layout utilities the dashboard renders
	// with. These disappear together when the stylesheet is built without the
	// templates in scope, which is how `.hidden` went missing. `block` is
	// checked as `sm:block`: the dashboard only ever uses the responsive form
	// (the tooltip reveal), and with source(none) scoping an unscoped utility
	// that no template emits is not generated, so the bare name is absent
	// from a correct build. The emitted selector escapes the colon
	// (`.sm\:block`), so the pattern is built by hand for that one. The
	// prefix also admits `}`: the rule sits inside a media query, and any
	// `sm:` utility Tailwind sorts ahead of `display` puts it off the
	// opening-brace boundary.
	for _, class := range []string{"flex", "grid", "border", "absolute", "relative"} {
		re := regexp.MustCompile(`(^|[,}])\.` + regexp.QuoteMeta(class) + `\{`)
		if !re.MatchString(sheet) {
			t.Errorf("dashboard.css defines no .%s rule; the stylesheet is stale against the templates — regenerate with `make css`", class)
		}
	}
	smBlock := regexp.MustCompile(`(^|[,}{])\.sm\\:block\{`)
	if !smBlock.MatchString(sheet) {
		t.Error("dashboard.css defines no .sm\\:block rule; the stylesheet is stale against the templates — regenerate with `make css`")
	}
}

func TestFmtInt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{-1, "—"},
	}
	for _, c := range cases {
		got := admin.FmtInt(c.n)
		if got != c.want {
			t.Errorf("FmtInt(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}
