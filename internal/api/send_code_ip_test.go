package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// generousIPLimits is wide enough that no case reaches it by accident, for the
// tests that are about some other rejection.
var generousIPLimits = store.SendCodeIPLimits{
	Calls:  store.RateLimitConfig{Limit: 1000, Window: time.Hour},
	Phones: store.RateLimitConfig{Limit: 1000, Window: 24 * time.Hour},
}

// TestSendCodeIPCallLimitDeniesTheSprayAndIssuesNoCode is the shape of the
// attack this limit exists for: one network walking a list of phone numbers.
// The call past the limit must be refused, and refused early enough that no
// login code was created for the number it named.
func TestSendCodeIPCallLimitDeniesTheSprayAndIssuesNoCode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	const limit = 4
	limits := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: limit, Window: time.Hour}}
	addr := netip.MustParseAddr("198.51.100.10")

	for i := range limit {
		if _, err := api.SendCodeForTest(s, addr, limits, sprayPhone(t, i)); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}

	blocked := sprayPhone(t, limit)
	_, err := api.SendCodeForTest(s, addr, limits, blocked)
	if secs := floodWaitSeconds(t, err); secs < 1 {
		t.Errorf("FLOOD_WAIT_%d, want at least 1 second", secs)
	}

	// Nothing was written for the number the refused call named. A code row
	// would have started the per-phone resend cooldown, so an issue that
	// succeeds now is the proof there is none.
	if _, _, err := s.IssueCode(ctx, blocked); err != nil {
		t.Errorf("issue code for the refused number: %v — the denied call wrote login-code state", err)
	}
}

// TestSendCodeIPLimitIsPerNetwork proves the budget belongs to the network and
// not to the server: an unrelated address is unaffected by a neighbour that has
// spent its own.
func TestSendCodeIPLimitIsPerNetwork(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	limits := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 1, Window: time.Hour}}

	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("198.51.100.20"), limits, sprayPhone(t, 0)); err != nil {
		t.Fatalf("first network: %v", err)
	}
	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("198.51.100.20"), limits, sprayPhone(t, 1)); err == nil {
		t.Error("the same network was allowed past its limit")
	}
	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("198.51.100.21"), limits, sprayPhone(t, 2)); err != nil {
		t.Errorf("an unrelated network was denied: %v", err)
	}
}

// TestSendCodeIPv6BucketIsThe64 proves the bucket an IPv6 client is limited in
// is the one it cannot get more of. Keying on the full address would hand every
// v6 client an unlimited supply of budgets inside its own allocation.
func TestSendCodeIPv6BucketIsThe64(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	limits := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 1, Window: time.Hour}}

	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("2001:db8:100:1::1"), limits, sprayPhone(t, 0)); err != nil {
		t.Fatalf("first address: %v", err)
	}
	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("2001:db8:100:1::2"), limits, sprayPhone(t, 1)); err == nil {
		t.Error("a second address inside the same /64 got a fresh budget")
	}
	if _, err := api.SendCodeForTest(s, netip.MustParseAddr("2001:db8:100:2::1"), limits, sprayPhone(t, 2)); err != nil {
		t.Errorf("an address in a different /64 was denied: %v", err)
	}
}

// TestSendCodeDistinctPhoneQuota proves the second counter: a network may ask
// for codes on only so many distinct numbers, while the numbers it has already
// spent a slot on stay available to it under the call limit. Without it, a
// sprayer bounded only by calls per hour still walks a fresh list every hour.
func TestSendCodeDistinctPhoneQuota(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	const quota = 3
	limits := store.SendCodeIPLimits{
		Calls:  store.RateLimitConfig{Limit: 1000, Window: time.Hour},
		Phones: store.RateLimitConfig{Limit: quota, Window: 24 * time.Hour},
	}
	addr := netip.MustParseAddr("198.51.100.30")

	for i := range quota {
		if _, err := api.SendCodeForTest(s, addr, limits, sprayPhone(t, i)); err != nil {
			t.Fatalf("number %d: %v", i+1, err)
		}
	}
	_, err := api.SendCodeForTest(s, addr, limits, sprayPhone(t, quota))
	if secs := floodWaitSeconds(t, err); secs < 1 {
		t.Errorf("FLOOD_WAIT_%d, want at least 1 second", secs)
	}

	// A number already charged to this key is still reachable. Only the
	// per-phone resend cooldown holds it back, which is a different limit with
	// its own fixed wait.
	_, err = api.SendCodeForTest(s, addr, limits, sprayPhone(t, 0))
	if got := floodWaitSeconds(t, err); got != 60 {
		t.Errorf("repeating an already-counted number: FLOOD_WAIT_%d, want the 60s per-phone cooldown", got)
	}
}

// TestSendCodeOverLimitTellsRegisteredAndUnregisteredApartNot is the
// no-registration-oracle property on this path: once a network is over its
// limit, the answer must be the same whether the number it names has an account
// or not. The check runs before the number is looked up at all, so there is
// nothing for the answer to vary with.
func TestSendCodeOverLimitTellsRegisteredAndUnregisteredApartNot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	limits := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 1, Window: time.Hour}}
	addr := netip.MustParseAddr("198.51.100.40")

	registered, unregistered := sprayPhone(t, 0), sprayPhone(t, 1)
	if _, err := s.CreateUser(ctx, registered); err != nil {
		t.Fatalf("create user: %v", err)
	}
	// Spend the network's single call.
	if _, err := api.SendCodeForTest(s, addr, limits, sprayPhone(t, 2)); err != nil {
		t.Fatalf("seeding call: %v", err)
	}

	_, err1 := api.SendCodeForTest(s, addr, limits, registered)
	_, errU := api.SendCodeForTest(s, addr, limits, unregistered)
	_, err2 := api.SendCodeForTest(s, addr, limits, registered)

	// The wait counts down in whole seconds, so consecutive calls may straddle a
	// tick. What must never happen is the unregistered number getting an answer
	// neither of its registered neighbours gave: that would be a difference the
	// account, not the clock, produced.
	got, want1, want2 := rpcString(t, errU), rpcString(t, err1), rpcString(t, err2)
	if got != want1 && got != want2 {
		t.Errorf("unregistered number answered %q, while the registered one got %q then %q", got, want1, want2)
	}
}

// TestSendCodePerPhoneCooldownSurvivesTheIPLimits proves the older per-phone
// cooldown still works on its own: a network well inside its per-IP budget is
// still held to one code per number per minute.
func TestSendCodePerPhoneCooldownSurvivesTheIPLimits(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.50")
	phone := sprayPhone(t, 0)

	if _, err := api.SendCodeForTest(s, addr, generousIPLimits, phone); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, err := api.SendCodeForTest(s, addr, generousIPLimits, phone)
	if got := floodWaitSeconds(t, err); got != 60 {
		t.Errorf("repeat on the same number: FLOOD_WAIT_%d, want the 60s per-phone cooldown", got)
	}
}

// TestSendCodeWithoutAClientAddressIsRefused pins the fail-closed choice. A
// connection with no address cannot be held to a per-network limit, and waving
// it through would make "arrive without an address" the way around the limit.
// With the limits off there is nothing keyed on an address, so the same request
// proceeds.
func TestSendCodeWithoutAClientAddressIsRefused(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	limits := store.SendCodeIPLimits{Calls: store.RateLimitConfig{Limit: 10, Window: time.Hour}}

	_, err := api.SendCodeForTest(s, netip.Addr{}, limits, sprayPhone(t, 0))
	if secs := floodWaitSeconds(t, err); secs < 1 {
		t.Errorf("FLOOD_WAIT_%d, want at least 1 second", secs)
	}
	if _, err := api.SendCodeForTest(s, netip.Addr{}, store.SendCodeIPLimits{}, sprayPhone(t, 1)); err != nil {
		t.Errorf("with the per-IP limits disabled: %v", err)
	}
}

// floodWaitSeconds asserts err is the uniform 420 flood wait and returns the
// seconds it names.
func floodWaitSeconds(t *testing.T, err error) int {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("err = %v, want a 420 FLOOD_WAIT", err)
	}
	if rpc.Code != 420 || rpc.Type != "FLOOD_WAIT" {
		t.Fatalf("err = %d %s, want a 420 FLOOD_WAIT", rpc.Code, rpc.Message)
	}
	return rpc.Argument
}

// rpcString renders an RPC error as the client sees it: code and message, and
// nothing else.
func rpcString(t *testing.T, err error) string {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("err = %v, want an RPC error", err)
	}
	return fmt.Sprintf("%d %s", rpc.Code, rpc.Message)
}

// sprayPhone builds the distinct valid numbers a spray walks through, so no
// case has to invent its own.
func sprayPhone(t *testing.T, i int) string {
	t.Helper()
	if i > 99 {
		t.Fatalf("sprayPhone(%d): the range holds 100 numbers", i)
	}
	return fmt.Sprintf("+1555126%04d", i)
}
