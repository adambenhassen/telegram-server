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

func TestLoadDiscoveryLimitsValidation(t *testing.T) {
	defaults := mtproto.DefaultDiscoveryLimits()
	disabledGlobal := defaults
	disabledGlobal.MaxRequests = 0
	disabledGlobal.Window = 0
	disabledPerNet := defaults
	disabledPerNet.MaxRequestsPerNet = 0
	disabledPerNet.PerNetWindow = 0

	const (
		globalLimit       = "TG_RATE_LIMIT_DISCOVERY"
		globalLimitAlias  = "TG_DISCOVERY_RATE_LIMIT"
		globalWindow      = "TG_RATE_LIMIT_DISCOVERY_WINDOW"
		globalWindowAlias = "TG_DISCOVERY_RATE_LIMIT_WINDOW"
		perNetLimit       = "TG_RATE_LIMIT_DISCOVERY_IP"
		perNetLimitAlias  = "TG_DISCOVERY_RATE_LIMIT_PER_IP"
		perNetWindow      = "TG_RATE_LIMIT_DISCOVERY_IP_WINDOW"
		perNetWindowAlias = "TG_DISCOVERY_RATE_LIMIT_PER_IP_WINDOW"
	)

	tests := []struct {
		name       string
		env        map[string]string
		wantErr    []string
		wantLimits mtproto.DiscoveryLimits
	}{
		{name: "global count malformed", env: map[string]string{globalLimit: "many"}, wantErr: []string{globalLimit}},
		{name: "global count negative", env: map[string]string{globalLimit: "-1"}, wantErr: []string{globalLimit}},
		{name: "global window malformed", env: map[string]string{globalWindow: "soon"}, wantErr: []string{globalWindow}},
		{name: "global window negative", env: map[string]string{globalWindow: "-1s"}, wantErr: []string{globalWindow}},
		{name: "per-network count malformed", env: map[string]string{perNetLimit: "many"}, wantErr: []string{perNetLimit}},
		{name: "per-network count negative", env: map[string]string{perNetLimit: "-1"}, wantErr: []string{perNetLimit}},
		{name: "per-network window malformed", env: map[string]string{perNetWindow: "soon"}, wantErr: []string{perNetWindow}},
		{name: "per-network window negative", env: map[string]string{perNetWindow: "-1s"}, wantErr: []string{perNetWindow}},
		{
			name:    "global count aliases conflict",
			env:     map[string]string{globalLimit: "3", globalLimitAlias: "4"},
			wantErr: []string{globalLimit, globalLimitAlias},
		},
		{
			name:    "global window aliases conflict",
			env:     map[string]string{globalWindow: "1s", globalWindowAlias: "2s"},
			wantErr: []string{globalWindow, globalWindowAlias},
		},
		{
			name:    "per-network count aliases conflict",
			env:     map[string]string{perNetLimit: "3", perNetLimitAlias: "4"},
			wantErr: []string{perNetLimit, perNetLimitAlias},
		},
		{
			name:    "per-network window aliases conflict",
			env:     map[string]string{perNetWindow: "1s", perNetWindowAlias: "2s"},
			wantErr: []string{perNetWindow, perNetWindowAlias},
		},
		{
			name:       "global disabled with zero window",
			env:        map[string]string{globalLimit: "0", globalWindow: "0s"},
			wantLimits: disabledGlobal,
		},
		{
			name:       "per-network disabled with zero window",
			env:        map[string]string{perNetLimit: "0", perNetWindow: "0s"},
			wantLimits: disabledPerNet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("00", 32))
			t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
			for _, name := range []string{
				globalLimit, globalLimitAlias,
				globalWindow, globalWindowAlias,
				perNetLimit, perNetLimitAlias,
				perNetWindow, perNetWindowAlias,
			} {
				t.Setenv(name, "")
			}
			for name, value := range tt.env {
				t.Setenv(name, value)
			}

			cfg, err := config.Load(slog.New(slog.DiscardHandler))
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("expected error, got discovery limits %+v", cfg.DiscoveryLimits)
				}
				for _, name := range tt.wantErr {
					if !strings.Contains(err.Error(), name) {
						t.Errorf("error %q does not name %s", err, name)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.DiscoveryLimits != tt.wantLimits {
				t.Errorf("discovery limits = %+v, want %+v", cfg.DiscoveryLimits, tt.wantLimits)
			}
		})
	}
}
