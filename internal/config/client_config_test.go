package config_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestLoadClientConfigDoesNotRequirePostgresOrAuthKey(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_LISTEN_ADDR", "0.0.0.0:2443")
	t.Setenv("TG_ADVERTISE_ADDR", "mtproto.example.com:443")
	t.Setenv("TG_DC_ID", "7")

	cfg, err := config.LoadClientConfig()
	if err != nil {
		t.Fatalf("LoadClientConfig: %v", err)
	}
	if cfg.AdvertiseHost != "mtproto.example.com" || cfg.AdvertisePort != 443 || cfg.DCID != 7 {
		t.Fatalf("client config = %+v", cfg)
	}
}

func TestLoadClientConfigRejectsInvalidDC(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	for _, raw := range []string{"0", "-1", "2147483648", "not-an-int"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("TG_DC_ID", raw)
			if _, err := config.LoadClientConfig(); err == nil || !strings.Contains(err.Error(), "TG_DC_ID") {
				t.Fatalf("LoadClientConfig(%q) error = %v", raw, err)
			}
		})
	}
}

func TestLoadDiscoveryLimits(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("00", 32))
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")

	cfg, err := config.Load(slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Load defaults: %v", err)
	}
	want := mtproto.DefaultDiscoveryLimits()
	if cfg.DiscoveryLimits != want {
		t.Fatalf("default discovery limits = %+v, want %+v", cfg.DiscoveryLimits, want)
	}

	t.Setenv("TG_RATE_LIMIT_DISCOVERY", "3")
	t.Setenv("TG_RATE_LIMIT_DISCOVERY_WINDOW", "15s")
	t.Setenv("TG_RATE_LIMIT_DISCOVERY_IP", "2")
	t.Setenv("TG_RATE_LIMIT_DISCOVERY_IP_WINDOW", "20s")
	cfg, err = config.Load(slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Load overrides: %v", err)
	}
	if got := cfg.DiscoveryLimits; got.MaxRequests != 3 || got.Window != 15*time.Second || got.MaxRequestsPerNet != 2 || got.PerNetWindow != 20*time.Second {
		t.Fatalf("discovery limits = %+v", got)
	}

	t.Setenv("TG_RATE_LIMIT_DISCOVERY_WINDOW", "0s")
	if _, err := config.Load(slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), "TG_RATE_LIMIT_DISCOVERY_WINDOW") {
		t.Fatalf("zero global window error = %v", err)
	}
}
