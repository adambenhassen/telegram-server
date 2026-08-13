package config_test

import (
	"bytes"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
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
	for _, want := range []config.ClientAddrTrust{config.ClientAddrSocket, config.ClientAddrProxyV2} {
		if !strings.Contains(err.Error(), string(want)) {
			t.Errorf("error %q does not name the supported value %q", err, want)
		}
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
	for _, want := range []string{"load balancer", "carrier NAT", "per-IP", "handshakes"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("warning does not mention %q:\n%s", want, buf.String())
		}
	}

	// The pre-auth connection cap is keyed on an address too, and it is the
	// harsher one to collapse: sendCode limits sharing a bucket refuse code
	// requests, a pre-auth cap sharing one refuses handshakes server-wide. So
	// turning the sendCode counters off leaves the assumption in place, and the
	// warning with it.
	buf.Reset()
	cfg.RateLimits.SendCodeIP = store.SendCodeIPLimits{}
	cfg.WarnClientAddrTrust(log)
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("emitted %d warn lines with only the pre-auth per-IP cap on, want exactly 1:\n%s", n, buf.String())
	}

	// SignInFailIP alone also warns — it is a per-IP limit too.
	buf.Reset()
	cfg.PreAuth.MaxConnsPerNet = 0
	cfg.WarnClientAddrTrust(log)
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("emitted %d warn lines with only SignInFailIP on, want exactly 1:\n%s", n, buf.String())
	}

	// Nothing is keyed on an address once every per-IP limit is off, so there is
	// no assumption left to warn about.
	buf.Reset()
	cfg.RateLimits.SignInFailIP = store.RateLimitConfig{}
	cfg.WarnClientAddrTrust(log)
	if buf.Len() != 0 {
		t.Errorf("warned with every per-IP limit disabled:\n%s", buf.String())
	}
}

// TestWarnPreAuthLifetime covers the one pre-auth value that is accepted and
// still worth saying something about. A ceiling under the handshake's own read
// timeout expires while a client is legitimately waiting inside key exchange, so
// slow clients fail intermittently and it reads as a network fault rather than
// as the number the operator just set. Zero is the ceiling turned off, which is
// supported, and must not be warned about as if it were too small.
func TestWarnPreAuthLifetime(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	for name, tc := range map[string]struct {
		lifetime time.Duration
		wantWarn bool
	}{
		"default":             {lifetime: mtproto.DefaultPreAuthLimits().Lifetime},
		"at the read timeout": {lifetime: time.Minute},
		"under it":            {lifetime: 30 * time.Second, wantWarn: true},
		"off":                 {lifetime: 0},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := config.Load(discardLog())
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			cfg.PreAuth.Lifetime = tc.lifetime

			var buf bytes.Buffer
			cfg.WarnPreAuthLifetime(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			if warned := strings.Contains(buf.String(), "level=WARN"); warned != tc.wantWarn {
				t.Errorf("lifetime %v warned = %v, want %v:\n%s", tc.lifetime, warned, tc.wantWarn, buf.String())
			}
		})
	}
}

// TestLoadClientAddrProxyV2 covers the mode that reads the address out of a
// PROXY protocol v2 header. The allowlist is the whole of the trust decision, so
// it is parsed here and asserted entry by entry: a CIDR that silently widened or
// dropped would decide which senders may name any address they like.
func TestLoadClientAddrProxyV2(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
	// Mixed forms on purpose: an operator with one balancer writes a bare
	// address, one with a subnet writes a CIDR, and both have to work.
	t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", "10.0.0.0/8, 192.0.2.7 ,2001:db8::/32")

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClientAddrTrust != config.ClientAddrProxyV2 {
		t.Errorf("ClientAddrTrust = %q, want %q", cfg.ClientAddrTrust, config.ClientAddrProxyV2)
	}
	want := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("192.0.2.7/32"),
		netip.MustParsePrefix("2001:db8::/32"),
	}
	if !slices.Equal(cfg.ClientAddrProxies, want) {
		t.Errorf("ClientAddrProxies = %v, want %v", cfg.ClientAddrProxies, want)
	}
}

// TestLoadClientAddrProxyV2RequiresAllowlist is the fail-closed startup rule. An
// empty allowlist in this mode is not "trust nobody" by accident and must never
// be read as "trust everybody": either reading is a guess, so the server refuses
// to start and names the variable that is missing.
func TestLoadClientAddrProxyV2RequiresAllowlist(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cidrs string
	}{
		{name: "unset", cidrs: ""},
		{name: "blank", cidrs: "  "},
		{name: "separators only", cidrs: ", ,"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
			t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", tt.cidrs)

			_, err := config.Load(discardLog())
			if err == nil {
				t.Fatal("started with no balancer allowlist: every sender could name any client address")
			}
			if !strings.Contains(err.Error(), "TG_CLIENT_ADDR_PROXY_CIDRS") {
				t.Errorf("error %q does not name the variable", err)
			}
		})
	}
}

// TestLoadClientAddrProxyV2RejectsBadCIDR fails the start on an allowlist entry
// that does not parse, rather than enforcing the entries that did. A dropped
// entry is a balancer whose connections are all refused, which looks like an
// outage rather than a typo.
func TestLoadClientAddrProxyV2RejectsBadCIDR(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
	t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", "10.0.0.0/8,not-an-address")

	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("an unparsable allowlist entry started the server")
	}
	if !strings.Contains(err.Error(), "TG_CLIENT_ADDR_PROXY_CIDRS") {
		t.Errorf("error %q does not name the variable", err)
	}
	if !strings.Contains(err.Error(), "not-an-address") {
		t.Errorf("error %q does not name the entry that failed", err)
	}
}

// TestLoadClientAddrProxiesRejectedInSocketMode catches the misconfiguration
// this ticket exists to prevent: an operator who lists their balancers but does
// not switch the mode is behind a load balancer with socket keying, which is the
// collapsed global bucket. Their allowlist is proof of intent, so the mismatch
// fails the start instead of being ignored.
func TestLoadClientAddrProxiesRejectedInSocketMode(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", "10.0.0.0/8")

	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("a balancer allowlist was accepted in socket mode, where nothing reads it")
	}
	for _, want := range []string{"TG_CLIENT_ADDR_PROXY_CIDRS", "TG_CLIENT_ADDR_TRUST"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// TestWarnClientAddrTrustSilentInProxyV2 pins the other half of the warning's
// contract. The socket-mode line warns about a risk proxy-v2 mode does not carry
// — the balancer's address is exactly what it stops keying on — and a warning
// that is not true where it is printed is one an operator learns to ignore.
func TestWarnClientAddrTrustSilentInProxyV2(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
	t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", "10.0.0.0/8")

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.RateLimits.SendCodeIP.Enabled() {
		t.Fatal("the per-IP limits are off: this test would prove nothing")
	}

	var buf bytes.Buffer
	cfg.WarnClientAddrTrust(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if buf.Len() != 0 {
		t.Errorf("warned about socket keying in %s mode:\n%s", config.ClientAddrProxyV2, buf.String())
	}
}

// TestLoadClientAddrProxyRejectsDefaultRoute is the third startup failure, and
// the only allowlist mistake that would otherwise boot cleanly. A default route
// makes every peer on the internet a trusted balancer, so any client may name
// any source address: rotate it to leave its own bucket, or pin it to an
// innocent address to hold that address in flood wait. Both families, because
// an operator who writes one usually writes the other.
func TestLoadClientAddrProxyRejectsDefaultRoute(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cidrs string
	}{
		{name: "ipv4 default route", cidrs: "0.0.0.0/0"},
		{name: "ipv6 default route", cidrs: "::/0"},
		{name: "alongside a real balancer", cidrs: "10.0.0.0/8,0.0.0.0/0"},
		{name: "both families", cidrs: "0.0.0.0/0,::/0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
			t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", tt.cidrs)

			_, err := config.Load(discardLog())
			if err == nil {
				t.Fatal("a default route was accepted as a balancer allowlist: every client may now name any address")
			}
			if !strings.Contains(err.Error(), "TG_CLIENT_ADDR_PROXY_CIDRS") {
				t.Errorf("error %q does not name the variable", err)
			}
			// The offending entry, not just the variable: an operator with a
			// list of balancers needs to be told which one is the problem.
			if !strings.Contains(err.Error(), "/0") {
				t.Errorf("error %q does not name the offending entry", err)
			}
		})
	}
}

// TestLoadClientAddrProxyMatchesPeerAddresses is the invariant the mapped-CIDR
// finding turned up: no accepted entry may boot and never match.
//
// The listener unmaps every peer address before it looks, so a prefix left in
// IPv4-mapped form is 128 bits wide against a 32-bit address and matches
// nothing. That entry starts the server and then refuses every connection from
// the balancer it was written to name — an outage that reads as the limiter
// working. So each entry here is checked against the address a peer would
// actually arrive as, rather than against how it was spelled.
func TestLoadClientAddrProxyMatchesPeerAddresses(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
	t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", "10.0.0.0/8, ::ffff:192.0.2.0/120, 2001:db8::/32, ::ffff:198.51.100.7")

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		// Normalized: the mapped /120 is the IPv4 /24 it means.
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("2001:db8::/32"),
		netip.MustParsePrefix("198.51.100.7/32"),
	}
	if !slices.Equal(cfg.ClientAddrProxies, want) {
		t.Fatalf("ClientAddrProxies = %v, want %v", cfg.ClientAddrProxies, want)
	}

	// The peers, as the listener reports them: unmapped, which is the whole
	// point of the normalization above.
	for _, peer := range []netip.Addr{
		netip.MustParseAddr("10.1.2.3"),
		netip.MustParseAddr("192.0.2.9"),
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("198.51.100.7"),
	} {
		if !slices.ContainsFunc(cfg.ClientAddrProxies, func(p netip.Prefix) bool { return p.Contains(peer) }) {
			t.Errorf("no accepted entry matches peer %s: it would boot and refuse that balancer forever", peer)
		}
	}
}

// TestLoadClientAddrProxyRejectsUnusableMappedCIDR covers the two mapped forms
// that must not start the server: a disguised default route, which the
// zero-length guard has to catch after normalization rather than before, and a
// mapped prefix too short to name an IPv4 network at all.
func TestLoadClientAddrProxyRejectsUnusableMappedCIDR(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cidrs string
	}{
		{name: "disguised default route", cidrs: "::ffff:0.0.0.0/96"},
		{name: "shorter than the mapped range", cidrs: "::ffff:0.0.0.0/64"},
		{name: "alongside a real balancer", cidrs: "10.0.0.0/8,::ffff:0.0.0.0/96"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			t.Setenv("TG_CLIENT_ADDR_TRUST", string(config.ClientAddrProxyV2))
			t.Setenv("TG_CLIENT_ADDR_PROXY_CIDRS", tt.cidrs)

			_, err := config.Load(discardLog())
			if err == nil {
				t.Fatal("an unusable mapped prefix started the server")
			}
			if !strings.Contains(err.Error(), "TG_CLIENT_ADDR_PROXY_CIDRS") {
				t.Errorf("error %q does not name the variable", err)
			}
			if !strings.Contains(err.Error(), "::ffff:0.0.0.0") {
				t.Errorf("error %q does not name the offending entry", err)
			}
		})
	}
}
