package config_test

import (
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":2443" {
		t.Errorf("ListenAddr = %q, want :2443", cfg.ListenAddr)
	}
	if cfg.DCID != 2 {
		t.Errorf("DCID = %d, want 2", cfg.DCID)
	}
	if cfg.PostgresDSN != "postgres://localhost/tg" {
		t.Errorf("PostgresDSN = %q", cfg.PostgresDSN)
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error when DSN missing")
	}
}
