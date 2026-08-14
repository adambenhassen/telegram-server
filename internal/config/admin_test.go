package config_test

import (
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

func TestAdminDisabled(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "")
	t.Setenv("TG_ADMIN_TOKEN_HASH", "")
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
}

func TestAdminListenOnly(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_ADMIN_LISTEN_ADDR", "127.0.0.1:2444")
	t.Setenv("TG_ADMIN_TOKEN_HASH", "")
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
