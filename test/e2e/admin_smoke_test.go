package e2e_test

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/config"
)

func testSmokeAdminProxyLogin(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const rawToken = "smoke-admin-token"
	tokenSum := sha256.Sum256([]byte(rawToken))
	tokenHash := hex.EncodeToString(tokenSum[:])

	for _, tc := range []struct {
		name            string
		configured      string
		wantAdminOrigin string
		wrongOrigin     string
	}{
		{
			name:            "configured HTTPS proxy origin",
			configured:      "https://telegram-server.tailaa4918.ts.net",
			wantAdminOrigin: "https://telegram-server.tailaa4918.ts.net",
			wrongOrigin:     "http://localhost:2445",
		},
		{
			name:            "derived listener origin",
			configured:      "",
			wantAdminOrigin: "http://127.0.0.1:2445",
			wrongOrigin:     "https://telegram-server.tailaa4918.ts.net",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", f.dsn)
			t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("0", 64))
			t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
			t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2445")
			t.Setenv("TG_ADMIN_TOKEN_HASH", tokenHash)
			t.Setenv("TG_ADMIN_ORIGIN", tc.configured)

			cfg, err := config.Load(slog.Default())
			if err != nil {
				t.Fatalf("load admin config: %v", err)
			}
			if cfg.AdminOrigin == "" {
				t.Fatal("loaded config supplied an empty origin to the admin router")
			}
			if cfg.AdminOrigin != tc.wantAdminOrigin {
				t.Fatalf("admin origin = %q, want %q", cfg.AdminOrigin, tc.wantAdminOrigin)
			}

			router := admin.AdminRouter(admin.LoginHandlerConfig{
				Store:       f.store,
				TokenHash:   cfg.AdminTokenHash,
				Logger:      slog.Default(),
				AdminOrigin: cfg.AdminOrigin,
			}, f.registry)

			get := httptest.NewRequestWithContext(f.ctx, http.MethodGet, "/admin/login", nil)
			getRec := httptest.NewRecorder()
			router.ServeHTTP(getRec, get)
			if getRec.Code != http.StatusOK {
				t.Fatalf("GET /admin/login returned %d, want 200", getRec.Code)
			}
			csrfToken := smokeFormCSRFToken(t, getRec.Body.String())
			csrfCookie := ""
			for _, cookie := range getRec.Result().Cookies() {
				if cookie.Name == "__Host-csrf-token" {
					csrfCookie = cookie.Value
				}
			}
			if csrfCookie == "" {
				t.Fatal("login page did not set its CSRF cookie")
			}

			wrongForm := strings.NewReader("csrf_token=" + csrfToken + "&token=" + rawToken)
			wrongReq := httptest.NewRequestWithContext(f.ctx, http.MethodPost, "/admin/login", wrongForm)
			wrongReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			wrongReq.Header.Set("Origin", tc.wrongOrigin)
			wrongReq.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
			wrongRec := httptest.NewRecorder()
			router.ServeHTTP(wrongRec, wrongReq)
			if wrongRec.Code != http.StatusUnauthorized {
				t.Fatalf("login with untrusted Origin %q returned %d, want 401", tc.wrongOrigin, wrongRec.Code)
			}

			form := strings.NewReader("csrf_token=" + csrfToken + "&token=" + rawToken)
			req := httptest.NewRequestWithContext(f.ctx, http.MethodPost, "/admin/login", form)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Origin", cfg.AdminOrigin)
			req.Header.Set("Sec-Fetch-Site", "same-origin")
			req.Header.Set("Forwarded", "host=attacker.example;proto=http")
			req.Header.Set("X-Forwarded-Host", "attacker.example")
			req.Header.Set("X-Forwarded-Proto", "http")
			req.Host = "attacker.example"
			req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusFound {
				t.Fatalf("login with trusted Origin returned %d, want 302", rec.Code)
			}

			sessionCookie := ""
			for _, cookie := range rec.Result().Cookies() {
				if cookie.Name == "__Host-admin-session" {
					sessionCookie = cookie.Value
				}
			}
			if sessionCookie == "" {
				t.Fatal("successful login did not set an admin session")
			}
			dashboardReq := httptest.NewRequestWithContext(f.ctx, http.MethodGet, "/admin/dashboard", nil)
			dashboardReq.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
			dashboardRec := httptest.NewRecorder()
			router.ServeHTTP(dashboardRec, dashboardReq)
			if dashboardRec.Code != http.StatusOK {
				t.Fatalf("GET /admin/dashboard after login returned %d, want 200", dashboardRec.Code)
			}
		})
	}
}

func smokeFormCSRFToken(t *testing.T, body string) string {
	t.Helper()
	marker := `name="csrf_token" value="`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatal("page omitted csrf_token form field")
	}
	start += len(marker)
	end := strings.Index(body[start:], `"`)
	if end < 0 {
		t.Fatal("csrf_token form field has no closing quote")
	}
	return body[start : start+end]
}
