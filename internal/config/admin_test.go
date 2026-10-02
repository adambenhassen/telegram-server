package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

func TestAdminDisabled(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "")
	t.Setenv("TG_ADMIN_TOKEN_HASH", "")
	t.Setenv("TG_ADMIN_ORIGIN", "")
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminListenAddr != "" {
		t.Errorf("AdminListenAddr = %q, want empty", cfg.AdminListenAddr)
	}
	if cfg.AdminTokenHash != "" {
		t.Errorf("AdminTokenHash = %q, want empty", cfg.AdminTokenHash)
	}
}

func TestAdminBothSet(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))
	t.Setenv("TG_ADMIN_ORIGIN", "")
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminListenAddr != "127.0.0.1:2444" {
		t.Errorf("AdminListenAddr = %q", cfg.AdminListenAddr)
	}
	if cfg.AdminTokenHash != strings.Repeat("a", 64) {
		t.Errorf("AdminTokenHash = %q", cfg.AdminTokenHash)
	}
	if cfg.AdminOrigin != "http://127.0.0.1:2444" {
		t.Errorf("AdminOrigin = %q, want derived origin http://127.0.0.1:2444", cfg.AdminOrigin)
	}
}

func TestAdminListenOnly(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_TOKEN_HASH", "")
	t.Setenv("TG_ADMIN_ORIGIN", "")
	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error when TG_ADMIN_LISTEN_ADDR is set without TG_ADMIN_TOKEN_HASH")
	}
	if !strings.Contains(err.Error(), "TG_ADMIN_LISTEN_ADDR") {
		t.Errorf("error %q does not name TG_ADMIN_LISTEN_ADDR", err)
	}
	if !strings.Contains(err.Error(), "TG_ADMIN_TOKEN_HASH") {
		t.Errorf("error %q does not name TG_ADMIN_TOKEN_HASH", err)
	}
}

func TestAdminTokenOnly(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))
	t.Setenv("TG_ADMIN_ORIGIN", "")
	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error when TG_ADMIN_TOKEN_HASH is set without TG_ADMIN_LISTEN_ADDR")
	}
	if !strings.Contains(err.Error(), "TG_ADMIN_TOKEN_HASH") {
		t.Errorf("error %q does not name TG_ADMIN_TOKEN_HASH", err)
	}
	if !strings.Contains(err.Error(), "TG_ADMIN_LISTEN_ADDR") {
		t.Errorf("error %q does not name TG_ADMIN_LISTEN_ADDR", err)
	}
}

func TestAdminTokenHashInvalid(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_ORIGIN", "")

	for name, token := range map[string]string{
		"short":         strings.Repeat("a", 63),
		"long":          strings.Repeat("a", 65),
		"uppercase":     strings.Repeat("A", 64),
		"invalid chars": strings.Repeat("g", 64),
		"not hex":       "zzzz" + strings.Repeat("a", 60),
		"empty":         "",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_ADMIN_TOKEN_HASH", token)
			_, err := config.Load(discardLog())
			if err == nil {
				t.Fatalf("expected error for token hash %q", token)
			}
			if !strings.Contains(err.Error(), "TG_ADMIN_TOKEN_HASH") {
				t.Errorf("error %q does not name TG_ADMIN_TOKEN_HASH", err)
			}
		})
	}
}

func TestAdminOriginRejectsInvalidValues(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))

	for name, origin := range map[string]string{
		"empty host":             "https:///",
		"userinfo":               "https://user@example.com",
		"path":                   "https://example.com/path",
		"root path":              "https://example.com/",
		"query":                  "https://example.com?next=/",
		"fragment":               "https://example.com#admin",
		"wildcard":               "https://*.example.com",
		"null origin":            "null",
		"uppercase scheme":       "HTTPS://example.com",
		"uppercase host":         "https://Example.com",
		"non-ascii host":         "https://exämple.com",
		"https default port":     "https://example.com:443",
		"http default port":      "http://localhost:80",
		"http non-loopback host": "http://example.com",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_ADMIN_ORIGIN", origin)
			_, err := config.Load(discardLog())
			if err == nil {
				t.Fatalf("Load accepted invalid TG_ADMIN_ORIGIN %q", origin)
			}
			if !strings.Contains(err.Error(), "TG_ADMIN_ORIGIN") {
				t.Errorf("error %q does not name TG_ADMIN_ORIGIN", err)
			}
		})
	}
}

func TestAdminOriginRequiresListener(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "")
	t.Setenv("TG_ADMIN_TOKEN_HASH", "")
	t.Setenv("TG_ADMIN_ORIGIN", "https://admin.example.com")

	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("Load accepted TG_ADMIN_ORIGIN without TG_ADMIN_LISTEN_ADDR")
	}
	if !strings.Contains(err.Error(), "TG_ADMIN_ORIGIN") || !strings.Contains(err.Error(), "TG_ADMIN_LISTEN_ADDR") {
		t.Errorf("error %q does not name both TG_ADMIN_ORIGIN and TG_ADMIN_LISTEN_ADDR", err)
	}
}

func TestAdminOriginAcceptsCanonicalValues(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))

	for _, origin := range []string{
		"https://telegram-server.tailaa4918.ts.net",
		"https://admin.example.com:8443",
		"http://localhost:2445",
		"http://127.2.3.4:2445",
		"http://[::1]:2445",
	} {
		t.Run(origin, func(t *testing.T) {
			t.Setenv("TG_ADMIN_ORIGIN", origin)
			cfg, err := config.Load(discardLog())
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.AdminOrigin != origin {
				t.Errorf("AdminOrigin = %q, want configured origin %q", cfg.AdminOrigin, origin)
			}
		})
	}
}

func TestAdminOriginWhitespaceUsesDerivedValue(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", ":2445")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))

	for _, origin := range []string{"", " \t\n "} {
		t.Run(origin, func(t *testing.T) {
			t.Setenv("TG_ADMIN_ORIGIN", origin)
			cfg, err := config.Load(discardLog())
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.AdminOrigin != "http://localhost:2445" {
				t.Errorf("AdminOrigin = %q, want derived origin http://localhost:2445", cfg.AdminOrigin)
			}
		})
	}
}

func TestAdminOriginUnsetUsesDerivedValue(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", ":2445")
	t.Setenv("TG_ADMIN_TOKEN_HASH", strings.Repeat("a", 64))
	t.Setenv("TG_ADMIN_ORIGIN", "temporary")
	if err := os.Unsetenv("TG_ADMIN_ORIGIN"); err != nil {
		t.Fatalf("unset TG_ADMIN_ORIGIN: %v", err)
	}

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AdminOrigin != "http://localhost:2445" {
		t.Errorf("AdminOrigin = %q, want derived origin http://localhost:2445", cfg.AdminOrigin)
	}
}
