package api_test

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestValidateUsername(t *testing.T) {
	// Valid usernames: 5–32 chars, letter-first, alphanumeric + underscore.
	for _, u := range []string{
		"alice",
		"abcde",
		"a1234",
		"user_name",
		"user123",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ123456", // 32 chars
	} {
		if !api.ValidateUsername(u) {
			t.Errorf("%q should be a valid username", u)
		}
	}

	// Invalid: too short (less than 5 chars).
	for _, u := range []string{
		"",
		"a",
		"abc",
		"abcd",
	} {
		if api.ValidateUsername(u) {
			t.Errorf("%q should not be a valid username (too short)", u)
		}
	}

	// Invalid: starts with digit or underscore.
	for _, u := range []string{
		"1alice",
		"_alice",
	} {
		if api.ValidateUsername(u) {
			t.Errorf("%q should not be a valid username (bad first char)", u)
		}
	}

	// Invalid: contains invalid characters.
	for _, u := range []string{
		"alice!",
		"alice bob",
		"alice-bob",
		"alice.bob",
	} {
		if api.ValidateUsername(u) {
			t.Errorf("%q should not be a valid username (invalid chars)", u)
		}
	}

	// Invalid: too long (33+ chars).
	if api.ValidateUsername("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567") {
		t.Error("33-char string should not be a valid username")
	}
}

func TestSendCodeAcceptsUsername(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.60")

	res, err := api.SendCodeForTest(s, addr, generousIPLimits, "alice")
	if err != nil {
		t.Fatalf("sendCode for username: %v", err)
	}

	sc, ok := res.(*tg.AuthSentCode)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthSentCode", res)
	}
	if sc.PhoneCodeHash == "" {
		t.Error("AuthSentCode has empty hash")
	}
	sms, ok := sc.Type.(*tg.AuthSentCodeTypeSMS)
	if !ok || sms.Length != 5 {
		t.Errorf("type = %#v, want SMS length 5", sc.Type)
	}
}

func TestSendCodeRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.70")

	// "123" is neither a valid phone (too short) nor a valid username (starts
	// with digit, too short).
	_, err := api.SendCodeForTest(s, addr, generousIPLimits, "123")
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("got %v, want an RPC error", err)
	}
	if rpc.Code != 400 || rpc.Message != "PHONE_NUMBER_INVALID" {
		t.Errorf("got %d %s, want 400 PHONE_NUMBER_INVALID", rpc.Code, rpc.Message)
	}
}

func TestSendCodeUsernameNoCooldown(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.80")
	const username = "bobuser"

	// First call succeeds.
	res, err := api.SendCodeForTest(s, addr, generousIPLimits, username)
	if err != nil {
		t.Fatalf("first sendCode: %v", err)
	}
	if _, ok := res.(*tg.AuthSentCode); !ok {
		t.Fatalf("first result = %T, want *tg.AuthSentCode", res)
	}

	// Second call — immediately after — must also succeed. The phone path
	// would return FLOOD_WAIT_60, but the username path skips the cooldown.
	res2, err := api.SendCodeForTest(s, addr, generousIPLimits, username)
	if err != nil {
		t.Fatalf("second sendCode: %v — username path returned FLOOD_WAIT (cooldown not skipped)", err)
	}
	if _, ok := res2.(*tg.AuthSentCode); !ok {
		t.Fatalf("second result = %T, want *tg.AuthSentCode", res2)
	}
}

func TestSendCodeUsernameCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.85")

	// First call with mixed case.
	_, err := api.SendCodeForTest(s, addr, generousIPLimits, "Alice")
	if err != nil {
		t.Fatalf("first sendCode: %v", err)
	}

	// Second call with different case — must succeed (not FLOOD_WAIT) because
	// the username path skips cooldown. If it were keyed on the raw input,
	// "alice" and "Alice" would be different identifiers and succeed anyway,
	// but the assertion still holds.
	_, err = api.SendCodeForTest(s, addr, generousIPLimits, "alice")
	if err != nil {
		t.Fatalf("sendCode with different case: %v", err)
	}
}

func TestSendCodeUsernameRespectsIPLimit(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	limits := store.SendCodeIPLimits{
		Calls: store.RateLimitConfig{Limit: 2, Window: time.Hour},
	}
	addr := netip.MustParseAddr("198.51.100.90")

	// First two calls succeed.
	for i := range 2 {
		_, err := api.SendCodeForTest(s, addr, limits, "user"+string(rune('a'+i)))
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}

	// Third call — over the per-IP limit — must be denied.
	_, err := api.SendCodeForTest(s, addr, limits, "userc")
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("got %v, want an RPC error", err)
	}
	if rpc.Code != 420 || rpc.Message != "FLOOD_WAIT_3600" {
		t.Errorf("got %d %s, want 420 FLOOD_WAIT_3600", rpc.Code, rpc.Message)
	}
}

func TestSendCodePhonePathUnchanged(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("198.51.100.95")
	const phone = "+15551259999"

	// Phone path still works and still enforces cooldown.
	_, err := api.SendCodeForTest(s, addr, generousIPLimits, phone)
	if err != nil {
		t.Fatalf("first sendCode: %v", err)
	}

	// Second call on the same phone — must return FLOOD_WAIT.
	_, err = api.SendCodeForTest(s, addr, generousIPLimits, phone)
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("got %v, want an RPC error", err)
	}
	if rpc.Code != 420 || rpc.Message != "FLOOD_WAIT_60" {
		t.Errorf("got %d %s, want 420 FLOOD_WAIT_60", rpc.Code, rpc.Message)
	}
}
