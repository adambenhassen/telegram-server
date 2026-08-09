package store_test

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestIPBucketKey pins the width each address family is limited at. The widths
// are the limit: a /128 key would let one IPv6 host mint an unlimited supply of
// buckets inside its own allocation, and an over-wide IPv4 key would sweep
// unrelated subscribers into one.
func TestIPBucketKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "ipv4 is keyed at the host", addr: "203.0.113.7", want: "203.0.113.7/32"},
		{name: "ipv6 is keyed at the /64", addr: "2001:db8:1:2:3:4:5:6", want: "2001:db8:1:2::/64"},
		{name: "ipv6 host bits are masked off", addr: "2001:db8:1:2::ffff", want: "2001:db8:1:2::/64"},
		{name: "a 4-in-6 address is the same host as its ipv4 form", addr: "::ffff:203.0.113.7", want: "203.0.113.7/32"},
		{name: "a zone names an interface, not a network", addr: "fe80::1%eth0", want: "fe80::/64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			addr, err := netip.ParseAddr(tt.addr)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.addr, err)
			}
			key, ok := store.IPBucketKey(addr)
			if !ok {
				t.Fatalf("IPBucketKey(%s) reported no key", tt.addr)
			}
			if key.String() != tt.want {
				t.Errorf("IPBucketKey(%s) = %s, want %s", tt.addr, key, tt.want)
			}
		})
	}
}

// TestIPBucketKeyRejectsUnusableAddress covers the one address that must never
// become a key. A shared bucket for everything unattributable would let a single
// such peer spend the limit for every other one.
func TestIPBucketKeyRejectsUnusableAddress(t *testing.T) {
	t.Parallel()
	if key, ok := store.IPBucketKey(netip.Addr{}); ok {
		t.Errorf("the zero address produced key %s, want no key", key)
	}
}

// TestSendCodeIPCallLimit proves the per-network call counter denies past its
// limit, that the denial names a real remaining wait, and that a different
// network is untouched by it.
func TestSendCodeIPCallLimit(t *testing.T) {
	t.Parallel()
	s := openSendCodeStore(t)
	cfg := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 3, Window: time.Hour}}

	limited := netip.MustParseAddr("203.0.113.10")
	for i := range 3 {
		if res := charge(t, s, limited, phoneN(i), cfg); res != nil {
			t.Fatalf("call %d denied: wait %s", i+1, res.Wait)
		}
	}
	res := charge(t, s, limited, phoneN(3), cfg)
	if res == nil {
		t.Fatal("call 4 from the same network was allowed past the limit")
	}
	if res.Wait < time.Second {
		t.Errorf("wait = %s, want at least 1s", res.Wait)
	}

	// A different network carries its own budget.
	if res := charge(t, s, netip.MustParseAddr("203.0.113.11"), phoneN(0), cfg); res != nil {
		t.Errorf("an unrelated network was denied: wait %s", res.Wait)
	}
}

// TestSendCodeIPCallLimitSharesTheIPv6Bucket proves the /64 is the unit: two
// addresses a single host can hold at the same time share one budget, and two
// in different /64s do not.
func TestSendCodeIPCallLimitSharesTheIPv6Bucket(t *testing.T) {
	t.Parallel()
	s := openSendCodeStore(t)
	cfg := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 1, Window: time.Hour}}

	if res := charge(t, s, netip.MustParseAddr("2001:db8:a:b::1"), phoneN(0), cfg); res != nil {
		t.Fatalf("first call denied: wait %s", res.Wait)
	}
	if res := charge(t, s, netip.MustParseAddr("2001:db8:a:b::2"), phoneN(1), cfg); res == nil {
		t.Error("a second address inside the same /64 got its own budget")
	}
	if res := charge(t, s, netip.MustParseAddr("2001:db8:a:c::1"), phoneN(2), cfg); res != nil {
		t.Errorf("an address in a different /64 was denied: wait %s", res.Wait)
	}
}

// TestSendCodeIPPhoneQuota proves the distinct-number counter: a network may
// reach its quota of numbers and no more, while a number it has already spent a
// slot on stays free to repeat. This is the counter that bounds a spray, which
// the call counter alone would only slow down.
func TestSendCodeIPPhoneQuota(t *testing.T) {
	t.Parallel()
	s := openSendCodeStore(t)
	cfg := store.SendCodeIPLimits{Phones: store.RateLimitConfig{Limit: 2, Window: 24 * time.Hour}}
	addr := netip.MustParseAddr("203.0.113.20")

	for i := range 2 {
		if res := charge(t, s, addr, phoneN(i), cfg); res != nil {
			t.Fatalf("number %d denied: wait %s", i+1, res.Wait)
		}
	}
	res := charge(t, s, addr, phoneN(2), cfg)
	if res == nil {
		t.Fatal("a third distinct number was allowed past a quota of two")
	}
	if res.Wait < time.Second {
		t.Errorf("wait = %s, want at least 1s", res.Wait)
	}
	// Already charged: repeating it costs nothing.
	if res := charge(t, s, addr, phoneN(0), cfg); res != nil {
		t.Errorf("repeating an already-counted number was denied: wait %s", res.Wait)
	}
}

// TestSendCodeIPPhoneQuotaCountsNormalizedNumbers proves the two spellings of
// one number are one slot. Counting them separately would double every
// attacker's quota for free.
func TestSendCodeIPPhoneQuotaCountsNormalizedNumbers(t *testing.T) {
	t.Parallel()
	s := openSendCodeStore(t)
	cfg := store.SendCodeIPLimits{Phones: store.RateLimitConfig{Limit: 1, Window: 24 * time.Hour}}
	addr := netip.MustParseAddr("203.0.113.21")

	if res := charge(t, s, addr, "+15551240000", cfg); res != nil {
		t.Fatalf("first number denied: wait %s", res.Wait)
	}
	if res := charge(t, s, addr, "15551240000", cfg); res != nil {
		t.Errorf("the same number without its + was charged a second slot: wait %s", res.Wait)
	}
}

// TestSendCodeIPDeniedCallConsumesNothing is the invariant that makes a denial
// free: both counters are charged in one transaction, so a call the number
// quota refuses must not leave the call counter's token spent. Written as a
// budget the caller can still spend afterwards, because that is the only way
// the difference is observable.
func TestSendCodeIPDeniedCallConsumesNothing(t *testing.T) {
	t.Parallel()
	s := openSendCodeStore(t)
	cfg := store.SendCodeIPLimits{
		Calls:  store.RateLimitConfig{Limit: 3, Window: time.Hour},
		Phones: store.RateLimitConfig{Limit: 1, Window: 24 * time.Hour},
	}
	addr := netip.MustParseAddr("203.0.113.30")
	first, second := phoneN(0), phoneN(1)

	// Spends one call token and the single number slot.
	if res := charge(t, s, addr, first, cfg); res != nil {
		t.Fatalf("call 1 denied: wait %s", res.Wait)
	}
	// Refused by the number quota. Had it spent a token anyway, the third call
	// below would be the one denied instead of the fourth.
	if res := charge(t, s, addr, second, cfg); res == nil {
		t.Fatal("a second distinct number was allowed past a quota of one")
	}
	for i := range 2 {
		if res := charge(t, s, addr, first, cfg); res != nil {
			t.Fatalf("call %d on the already-counted number denied: wait %s — the refused call spent a token", i+2, res.Wait)
		}
	}
	if res := charge(t, s, addr, first, cfg); res == nil {
		t.Error("a fourth call was allowed past a call limit of three")
	}
}

// TestSendCodeIPRefusesAnUnkeyableRequest proves an address the transport could
// not parse is reported rather than folded into a shared bucket, and that a
// disabled limiter does not care about the address at all.
func TestSendCodeIPRefusesAnUnkeyableRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openSendCodeStore(t)
	enabled := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 1, Window: time.Hour}}

	_, err := s.CheckAndChargeSendCodeIP(ctx, netip.Addr{}, phoneN(0), enabled)
	if !errors.Is(err, store.ErrNoClientAddr) {
		t.Errorf("err = %v, want ErrNoClientAddr", err)
	}

	res, err := s.CheckAndChargeSendCodeIP(ctx, netip.Addr{}, phoneN(0), store.SendCodeIPLimits{})
	if err != nil || res != nil {
		t.Errorf("disabled limits: res=%v err=%v, want both nil", res, err)
	}
}

// TestSweepExpiredSendCodeIPLimits proves the retention floor: rows past their
// deadline are deleted, and rows still inside their window are not. These rows
// join a network to a phone number, so nothing may outlive its window.
func TestSweepExpiredSendCodeIPLimits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openSendCodeStore(t)

	// A window short enough to lapse inside the test.
	brief := store.SendCodeIPLimits{
		Calls:  store.RateLimitConfig{Limit: 5, Window: 300 * time.Millisecond},
		Phones: store.RateLimitConfig{Limit: 5, Window: 300 * time.Millisecond},
	}
	if res := charge(t, s, netip.MustParseAddr("203.0.113.40"), phoneN(0), brief); res != nil {
		t.Fatalf("seeding call denied: wait %s", res.Wait)
	}
	// A second network whose window outlives the sweep.
	lasting := store.SendCodeIPLimits{
		Calls:  store.RateLimitConfig{Limit: 5, Window: time.Hour},
		Phones: store.RateLimitConfig{Limit: 5, Window: time.Hour},
	}
	if res := charge(t, s, netip.MustParseAddr("203.0.113.41"), phoneN(1), lasting); res != nil {
		t.Fatalf("seeding call denied: wait %s", res.Wait)
	}

	time.Sleep(400 * time.Millisecond)
	n, err := s.SweepExpiredSendCodeIPLimits(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// One counter row and one phone row from the lapsed network.
	if n != 2 {
		t.Errorf("swept %d rows, want 2 (the lapsed network's counter and number)", n)
	}
	if n, err := s.SweepExpiredSendCodeIPLimits(ctx); err != nil || n != 0 {
		t.Errorf("second sweep deleted %d rows (err %v), want 0", n, err)
	}
}

// charge runs one per-IP check and fails the test on a storage error, so each
// case reads as the allow/deny decision it is about.
func charge(t *testing.T, s *store.Store, addr netip.Addr, phone string, cfg store.SendCodeIPLimits) *store.RateLimitResult {
	t.Helper()
	res, err := s.CheckAndChargeSendCodeIP(context.Background(), addr, phone, cfg)
	if err != nil {
		t.Fatalf("check send code ip for %s: %v", addr, err)
	}
	return res
}

// phoneN builds distinct valid numbers without each test inventing its own.
func phoneN(i int) string {
	return fmt.Sprintf("+1555124%04d", i)
}

func openSendCodeStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}
