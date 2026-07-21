package config_test

import (
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

// validEncKey is 64 hex chars = 32 bytes, the required master-key length.
const validEncKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
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
	if len(cfg.AuthKeyEncKey) != 32 {
		t.Errorf("AuthKeyEncKey len = %d, want 32", len(cfg.AuthKeyEncKey))
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error when DSN missing")
	}
}

func TestLoadEncKey(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	tests := map[string]string{
		"missing":      "",
		"not hex":      "zzzz",
		"wrong length": strings.Repeat("00", 16), // 16 bytes, want 32
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_AUTHKEY_ENC_KEY", key)
			if _, err := config.Load(); err == nil {
				t.Fatalf("expected error for %s enc key", name)
			}
		})
	}
}
