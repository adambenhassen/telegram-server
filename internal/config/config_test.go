package config_test

import (
	"strings"
	"testing"
	"time"

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
	if cfg.MaxFileBytes != 104857600 {
		t.Errorf("MaxFileBytes = %d, want 104857600", cfg.MaxFileBytes)
	}
	if cfg.UploadPartTTL != 6*time.Hour {
		t.Errorf("UploadPartTTL = %v, want 6h", cfg.UploadPartTTL)
	}
}

func TestLoadMaxFileBytes(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    int64
		wantErr bool
	}{
		"unset":       {raw: "", want: 104857600},
		"override":    {raw: "2097152", want: 2097152},
		"not integer": {raw: "10MB", wantErr: true},
		"zero":        {raw: "0", wantErr: true},
		"negative":    {raw: "-1", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_MAX_FILE_BYTES", tc.raw)
			cfg, err := config.Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got MaxFileBytes = %d", name, cfg.MaxFileBytes)
				}
				if !strings.Contains(err.Error(), "TG_MAX_FILE_BYTES") {
					t.Errorf("error %q does not name TG_MAX_FILE_BYTES", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MaxFileBytes != tc.want {
				t.Errorf("MaxFileBytes = %d, want %d", cfg.MaxFileBytes, tc.want)
			}
		})
	}
}

func TestLoadUploadPartTTL(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		"unset":        {raw: "", want: 6 * time.Hour},
		"override":     {raw: "30m", want: 30 * time.Minute},
		"not duration": {raw: "soon", wantErr: true},
		"zero":         {raw: "0s", wantErr: true},
		"negative":     {raw: "-1h", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_UPLOAD_PART_TTL", tc.raw)
			cfg, err := config.Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got UploadPartTTL = %v", name, cfg.UploadPartTTL)
				}
				if !strings.Contains(err.Error(), "TG_UPLOAD_PART_TTL") {
					t.Errorf("error %q does not name TG_UPLOAD_PART_TTL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.UploadPartTTL != tc.want {
				t.Errorf("UploadPartTTL = %v, want %v", cfg.UploadPartTTL, tc.want)
			}
		})
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error when DSN missing")
	}
}

func TestLoadLogLoginCodes(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    bool
		wantErr bool
	}{
		"unset":       {raw: "", want: false},
		"true":        {raw: "true", want: true},
		"false":       {raw: "false", want: false},
		"not boolean": {raw: "maybe", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_LOG_LOGIN_CODES", tc.raw)
			cfg, err := config.Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got LogLoginCodes = %v", name, cfg.LogLoginCodes)
				}
				// The error must name the variable, so a typo is diagnosable
				// from the startup log alone.
				if !strings.Contains(err.Error(), "TG_LOG_LOGIN_CODES") {
					t.Errorf("error %q does not name TG_LOG_LOGIN_CODES", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.LogLoginCodes != tc.want {
				t.Errorf("LogLoginCodes = %v, want %v", cfg.LogLoginCodes, tc.want)
			}
		})
	}
}

func TestLoadAdvertiseAddr(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		listen    string
		advertise string
		wantHost  string
		wantPort  int
		wantErr   bool
	}{
		"derived from default listen addr": {wantHost: "127.0.0.1", wantPort: 2443},
		"derived from wildcard":            {listen: "0.0.0.0:2443", wantHost: "127.0.0.1", wantPort: 2443},
		"derived from ipv6 wildcard":       {listen: "[::]:2443", wantHost: "127.0.0.1", wantPort: 2443},
		"derived from explicit host":       {listen: "192.168.1.5:2443", wantHost: "192.168.1.5", wantPort: 2443},
		"set with derivable listen addr":   {listen: ":2443", advertise: "tg.example.com:2443", wantHost: "tg.example.com", wantPort: 2443},
		"set overrides listen addr":        {listen: "0.0.0.0:2443", advertise: "10.0.0.7:9999", wantHost: "10.0.0.7", wantPort: 9999},
		// Rule 3: an explicit value is used verbatim, wildcard included.
		"set to wildcard is verbatim": {listen: ":2443", advertise: "0.0.0.0:2443", wantHost: "0.0.0.0", wantPort: 2443},
		"not host port":               {advertise: "nope", wantErr: true},
		"port not an integer":         {advertise: "host:abc", wantErr: true},
		"empty host":                  {advertise: ":2443", wantErr: true},
		"port zero":                   {advertise: "host:0", wantErr: true},
		"port negative":               {advertise: "host:-1", wantErr: true},
		"port above range":            {advertise: "host:99999", wantErr: true},
		"highest valid port":          {advertise: "host:65535", wantHost: "host", wantPort: 65535},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_LISTEN_ADDR", tc.listen)
			t.Setenv("TG_ADVERTISE_ADDR", tc.advertise)
			cfg, err := config.Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got %s:%d", name, cfg.AdvertiseHost, cfg.AdvertisePort)
				}
				// The error must name the variable, so a typo is diagnosable
				// from the startup log alone.
				if !strings.Contains(err.Error(), "TG_ADVERTISE_ADDR") {
					t.Errorf("error %q does not name TG_ADVERTISE_ADDR", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.AdvertiseHost != tc.wantHost || cfg.AdvertisePort != tc.wantPort {
				t.Errorf("advertise = %s:%d, want %s:%d", cfg.AdvertiseHost, cfg.AdvertisePort, tc.wantHost, tc.wantPort)
			}
		})
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
