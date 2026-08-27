package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// TestLoadRPCDeadlineDefaults pins the shipped per-RPC bounds and their
// overrides: the deadline a dispatched RPC runs under and the statement
// timeout every pooled connection carries. Both accept zero as an explicit
// off, refuse negative, and default to the derived numbers.
func TestLoadRPCDeadlineDefaults(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RPCDeadline != mtproto.DefaultRPCDeadline {
		t.Errorf("RPCDeadline = %v, want shipped default %v", cfg.RPCDeadline, mtproto.DefaultRPCDeadline)
	}
	if cfg.StatementTimeout != config.DefaultStatementTimeout {
		t.Errorf("StatementTimeout = %v, want shipped default %v", cfg.StatementTimeout, config.DefaultStatementTimeout)
	}
}

func TestLoadRPCTimeoutOverrides(t *testing.T) {
	tests := []struct {
		name      string
		env       string
		value     string
		want      time.Duration
		wantErr   string
		checkStmt bool
	}{
		{name: "rpc override", env: "TG_RPC_DEADLINE", value: "90s", want: 90 * time.Second},
		{name: "rpc off", env: "TG_RPC_DEADLINE", value: "0s", want: 0},
		{name: "rpc negative", env: "TG_RPC_DEADLINE", value: "-5s", wantErr: "TG_RPC_DEADLINE"},
		{name: "rpc garbage", env: "TG_RPC_DEADLINE", value: "soon", wantErr: "TG_RPC_DEADLINE"},
		{name: "statement override", env: "TG_STATEMENT_TIMEOUT", value: "45s", want: 45 * time.Second, checkStmt: true},
		{name: "statement off", env: "TG_STATEMENT_TIMEOUT", value: "0s", want: 0, checkStmt: true},
		{name: "statement negative", env: "TG_STATEMENT_TIMEOUT", value: "-1m", wantErr: "TG_STATEMENT_TIMEOUT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			t.Setenv(tc.env, tc.value)

			cfg, err := config.Load(discardLog())
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error for %s=%s", tc.env, tc.value)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error %q does not name %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			got := cfg.RPCDeadline
			if tc.checkStmt {
				got = cfg.StatementTimeout
			}
			if got != tc.want {
				t.Errorf("%s = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}
