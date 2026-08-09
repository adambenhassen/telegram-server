package config_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestLoadClientAddrTrustDefault pins the default source. Socket is the only
// address the server can know is real, and the per-IP defaults below are keyed
// on it, so both have to arrive together.
func TestLoadClientAddrTrustDefault(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClientAddrTrust != config.ClientAddrSocket {
		t.Errorf("ClientAddrTrust = %q, want %q", cfg.ClientAddrTrust, config.ClientAddrSocket)
	}
	if got := cfg.RateLimits.SendCodeIP.Calls; got.Limit != 10 || got.Window != time.Hour {
		t.Errorf("SendCodeIP.Calls = %+v, want 10 per hour", got)
	}
	if got := cfg.RateLimits.SendCodeIP.Phones; got.Limit != 20 || got.Window != 24*time.Hour {
		t.Errorf("SendCodeIP.Phones = %+v, want 20 per 24h", got)
	}
}

// TestLoadClientAddrTrustRejectsUnsupported is why the knob exists at all: an
// operator naming a mode this build does not implement must be told, not quietly
// served socket addresses. Behind a load balancer that fallback would key every
// client on earth to the balancer's address, turning the per-IP cap into a
// global one, so it has to fail the start.
func TestLoadClientAddrTrustRejectsUnsupported(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", "proxy-protocol")

	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("an unsupported trust mode started the server")
	}
	if !strings.Contains(err.Error(), "TG_CLIENT_ADDR_TRUST") {
		t.Errorf("error %q does not name the variable", err)
	}
	if !strings.Contains(err.Error(), string(config.ClientAddrSocket)) {
		t.Errorf("error %q does not name the supported value %q", err, config.ClientAddrSocket)
	}
}

// TestLoadClientAddrTrustAcceptsSocket covers the one value that is supported,
// spelled out.
func TestLoadClientAddrTrustAcceptsSocket(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrSocket))

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClientAddrTrust != config.ClientAddrSocket {
		t.Errorf("ClientAddrTrust = %q, want %q", cfg.ClientAddrTrust, config.ClientAddrSocket)
	}
}

// TestLoadClientAddrTrustEnvOverridesLimits proves each per-IP knob reaches its
// own counter. Distinct values on all four, so a name typo or a value landing on
// the wrong counter fails here rather than shipping.
func TestLoadClientAddrTrustEnvOverridesLimits(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP", "3")
	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP_WINDOW", "15m")
	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP_PHONES", "7")
	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP_PHONES_WINDOW", "6h")

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.RateLimits.SendCodeIP.Calls; got.Limit != 3 || got.Window != 15*time.Minute {
		t.Errorf("SendCodeIP.Calls = %+v, want 3 per 15m", got)
	}
	if got := cfg.RateLimits.SendCodeIP.Phones; got.Limit != 7 || got.Window != 6*time.Hour {
		t.Errorf("SendCodeIP.Phones = %+v, want 7 per 6h", got)
	}

	// Zero disables one counter without touching the other.
	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP", "0")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.SendCodeIP.Calls.Limit != 0 {
		t.Errorf("SendCodeIP.Calls limit = %d, want 0 (disabled)", cfg.RateLimits.SendCodeIP.Calls.Limit)
	}
	if !cfg.RateLimits.SendCodeIP.Enabled() {
		t.Error("disabling the call counter also disabled the distinct-number one")
	}

	t.Setenv("TG_RATE_LIMIT_SEND_CODE_IP", "nope")
	if _, err := config.Load(discardLog()); err == nil || !strings.Contains(err.Error(), "TG_RATE_LIMIT_SEND_CODE_IP") {
		t.Errorf("a non-integer limit gave %v, want an error naming the variable", err)
	}
}

// TestWarnClientAddrTrust proves the operational warning is emitted exactly once
// per start, and only where it is true.
//
// Socket mode assumes one peer address is one client, and both ways that fails
// have to be named: a proxy or L4 load balancer in front of the server, and a
// carrier NAT in front of the clients. This line is the whole of the mitigation
// until an address source that sees past them lands, so an omission is a real
// gap and not a cosmetic one — an operator who is not told reads the resulting
// flood waits as a bug in the limiter.
func TestWarnClientAddrTrust(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cfg.WarnClientAddrTrust(log)
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("emitted %d warn lines, want exactly 1:\n%s", n, buf.String())
	}
	for _, want := range []string{"load balancer", "carrier NAT", "per-IP"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("warning does not mention %q:\n%s", want, buf.String())
		}
	}

	// Nothing is keyed on an address when both counters are off, so there is no
	// assumption left to warn about.
	buf.Reset()
	cfg.RateLimits.SendCodeIP = store.SendCodeIPLimits{}
	cfg.WarnClientAddrTrust(log)
	if buf.Len() != 0 {
		t.Errorf("warned with the per-IP limits disabled:\n%s", buf.String())
	}
}
