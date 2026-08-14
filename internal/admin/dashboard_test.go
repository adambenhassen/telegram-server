package admin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
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
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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

	// Auto-refresh JS is present.
	if !strings.Contains(body, "/admin/metrics") {
		t.Error("body missing auto-refresh fetch target")
	}
}

func TestDashboardHandler_post_rejected(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
