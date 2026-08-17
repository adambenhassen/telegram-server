package api_test

import (
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
)

// TestGetConfigStampsCurrentTime covers the two time fields a client reads off
// the config: date is the server's clock, expires is a point ahead of it. A zero
// in either is what made a real client treat the config as expired at the epoch.
func TestGetConfigStampsCurrentTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	getConfig := api.GetConfigSeqForTest(1, "127.0.0.1", 443, func() time.Time { return now })

	cfg, err := getConfig()
	if err != nil {
		t.Fatalf("help.getConfig: %v", err)
	}
	if cfg.Date != int(now.Unix()) {
		t.Errorf("date = %d, want %d (the server clock)", cfg.Date, now.Unix())
	}
	if want := int(now.Add(api.ConfigTTL).Unix()); cfg.Expires != want {
		t.Errorf("expires = %d, want %d", cfg.Expires, want)
	}
	if cfg.Expires <= cfg.Date {
		t.Errorf("expires %d is not ahead of date %d", cfg.Expires, cfg.Date)
	}
}

// TestGetConfigTimesFollowTheClock is what a pair computed once — at process
// start, or as a literal — cannot pass: a server that has been up for a while
// must still answer with the time it is now, not the time it booted.
func TestGetConfigTimesFollowTheClock(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	getConfig := api.GetConfigSeqForTest(1, "127.0.0.1", 443, func() time.Time { return now })

	first, err := getConfig()
	if err != nil {
		t.Fatalf("help.getConfig: %v", err)
	}

	const uptime = 30 * 24 * time.Hour
	now = now.Add(uptime)

	second, err := getConfig()
	if err != nil {
		t.Fatalf("help.getConfig: %v", err)
	}
	if got, want := second.Date-first.Date, int(uptime.Seconds()); got != want {
		t.Errorf("date advanced by %ds over %s of uptime, want %ds", got, uptime, want)
	}
	if got, want := second.Expires-first.Expires, int(uptime.Seconds()); got != want {
		t.Errorf("expires advanced by %ds over %s of uptime, want %ds", got, uptime, want)
	}
	if second.Expires <= second.Date {
		t.Errorf("expires %d is not ahead of date %d after %s of uptime", second.Expires, second.Date, uptime)
	}
}

// TestIdleClientRefetchesOncePerExpiryWindow is the load regression itself. It
// drives the rule a real client applies — refetch when the cached config has
// expired — over a simulated idle span, and counts what reaches the handler.
// With expires at zero every single tick refetches, which is the observed flood;
// with a real window the count is one per window plus the initial fetch.
func TestIdleClientRefetchesOncePerExpiryWindow(t *testing.T) {
	t.Parallel()

	const (
		span = 6 * time.Hour
		tick = 30 * time.Second
	)

	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	getConfig := api.GetConfigSeqForTest(1, "127.0.0.1", 443, func() time.Time { return now })

	calls := 0
	expires := 0
	for elapsed := time.Duration(0); elapsed <= span; elapsed += tick {
		now = now.Add(tick)
		if int(now.Unix()) < expires {
			continue
		}
		cfg, err := getConfig()
		if err != nil {
			t.Fatalf("help.getConfig: %v", err)
		}
		calls++
		expires = cfg.Expires
	}

	t.Logf("%d help.getConfig calls over %s idle, from %d client wakeups", calls, span, int(span/tick)+1)

	// One per window, plus the initial fetch, plus a tick's worth of slack at the
	// last boundary. Anything keyed on a stale expiry lands at one call per tick.
	limit := int(span/api.ConfigTTL) + 2
	if calls < 1 || calls > limit {
		t.Errorf("idle client issued %d help.getConfig calls over %s, want between 1 and %d (one per %s window)", calls, span, limit, api.ConfigTTL)
	}
}
