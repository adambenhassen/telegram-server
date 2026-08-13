package api_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// isPhoneCodeInvalid reports whether err is a 400 PHONE_CODE_INVALID error.
func isPhoneCodeInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "PHONE_CODE_INVALID"
}

// TestSignInFailRateLimit proves that exhausting the per-IP failure budget
// via repeated wrong codes produces a FLOOD_WAIT error on the next attempt
// from the same address.
func TestSignInFailRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296001"
	hash, _, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.1")

	// Limit of 3 failures per 10s.
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// 3 wrong guesses from the same IP — each charges the per-IP failure counter.
	for i := range 3 {
		_, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
			PhoneNumber:   phone,
			PhoneCodeHash: hash,
			PhoneCode:     "00000", // wrong
		})
		if isFloodWait(err) {
			t.Fatalf("wrong guess %d: unexpected FLOOD_WAIT: %v", i+1, err)
		}
		if !isPhoneCodeInvalid(err) {
			t.Fatalf("wrong guess %d: expected PHONE_CODE_INVALID, got %v", i+1, err)
		}
	}

	// The 4th wrong guess from the same IP should be denied with FLOOD_WAIT.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("4th wrong guess: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSignInFailIndependentIPs proves that two different IPs each have their
// own failure budget. Exhausting one IP's budget does not affect another.
func TestSignInFailIndependentIPs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296101"
	hash, _, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	addrA := netip.MustParseAddr("10.0.0.1")
	addrB := netip.MustParseAddr("10.0.0.2")

	// Limit of 1 failure per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Exhaust IP A's budget.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addrA, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("IP A first guess: expected PHONE_CODE_INVALID, got %v", err)
	}

	// IP A should be denied.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addrA, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("IP A second guess: expected FLOOD_WAIT, got %v", err)
	}

	// IP B should be unaffected — first wrong guess returns PHONE_CODE_INVALID.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addrB, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("IP B first guess: expected PHONE_CODE_INVALID, got %v", err)
	}
}

// TestSignInFailSameIPDifferentPhones proves that two different phones from
// the same IP share that IP's failure budget.
func TestSignInFailSameIPDifferentPhones(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phoneA := "+15551296201"
	phoneB := "+15551296202"
	hashA, _, err := s.IssueCode(ctx, phoneA)
	if err != nil {
		t.Fatal(err)
	}
	hashB, _, err := s.IssueCode(ctx, phoneB)
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.3")

	// Limit of 1 failure per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Wrong guess on phone A from this IP — exhausts the budget.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phoneA,
		PhoneCodeHash: hashA,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("phone A wrong: expected PHONE_CODE_INVALID, got %v", err)
	}

	// Wrong guess on phone B from the same IP — should be denied (shared budget).
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phoneB,
		PhoneCodeHash: hashB,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("phone B from same IP: expected FLOOD_WAIT (shared budget), got %v", err)
	}
}

// TestSignInFailRateLimitWindowExpiry proves that after the failure budget
// window expires, the IP can attempt signIn again — verifying recovery, not
// just denial.
func TestSignInFailRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	phone := "+15551296301"
	hash, _, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.4")

	// Long window.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	// One wrong guess — exhausts the limit.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("first wrong guess: expected PHONE_CODE_INVALID, got %v", err)
	}

	// Denied.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Age the window past expiry so the IP can try again.
	if err := api.AgeSignInFailWindowForTest(dsn, addr, time.Hour+time.Second); err != nil {
		t.Fatal(err)
	}

	// After window expiry, a wrong guess is allowed again (PHONE_CODE_INVALID,
	// not FLOOD_WAIT) — proving the new window is charged.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("after window expiry: expected PHONE_CODE_INVALID (access restored), got %v", err)
	}

	// The new window is now exhausted — next attempt should be denied again.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("after recovery: expected FLOOD_WAIT (new window exhausted), got %v", err)
	}
}

// TestSignInFailDisabled proves that a zero limit disables enforcement.
func TestSignInFailDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296401"
	hash, _, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.5")

	// Zero limit = disabled.
	cfg := store.RateLimitConfig{}

	// Many wrong guesses should all return PHONE_CODE_INVALID, not FLOOD_WAIT.
	for i := range 20 {
		_, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
			PhoneNumber:   phone,
			PhoneCodeHash: hash,
			PhoneCode:     "00000",
		})
		if isFloodWait(err) {
			t.Fatalf("wrong guess %d: unexpected FLOOD_WAIT with disabled limit", i+1)
		}
	}
}

// TestSignInFailCorrectCodeDifferentAddress proves the acceptance criterion:
// a correct code from a different address succeeds even after the attacker's
// IP budget is exhausted.
func TestSignInFailCorrectCodeDifferentAddress(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296601"
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so BindAuthKeyUser succeeds on a correct sign-in.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	attackerAddr := netip.MustParseAddr("10.0.0.100")
	legitAddr := netip.MustParseAddr("10.0.0.200")

	// Limit of 1 failure per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Attacker exhausts their IP budget with one wrong guess.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, attackerAddr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("attacker wrong guess: expected PHONE_CODE_INVALID, got %v", err)
	}

	// Attacker is now blocked — even with the correct code.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, attackerAddr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if !isFloodWait(err) {
		t.Fatalf("attacker with correct code: expected FLOOD_WAIT, got %v", err)
	}

	// Legitimate user from a different IP with the correct code succeeds.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, legitAddr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if err != nil {
		t.Fatalf("legit user with correct code from different IP: expected success, got %v", err)
	}
}

// TestSignInFailIPAndSendCodeIndependent proves that the per-IP signIn-fail
// limit and the per-IP sendCode limit are independent.
func TestSignInFailIPAndSendCodeIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296501"
	addr := netip.MustParseAddr("10.0.0.6")

	// Very restrictive sign-in fail limit: 1 per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Issue a code first.
	hash, _, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	// One wrong guess — exhausts the per-IP failure budget.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("first wrong guess: expected PHONE_CODE_INVALID, got %v", err)
	}

	// Second wrong guess should be denied by the per-IP limit.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000",
	})
	if !isFloodWait(err) {
		t.Fatalf("second wrong guess: expected FLOOD_WAIT, got %v", err)
	}

	// sendCode for a different phone should still work — the signIn-fail limit
	// does not block it. A separate phone avoids the IssueCode resend cooldown.
	_, err = api.SendCodeForTest(s, addr, store.SendCodeIPLimits{}, "+15551296502")
	if isFloodWait(err) {
		t.Fatalf("sendCode after signIn-fail exhaustion: unexpected FLOOD_WAIT, got %v", err)
	}

	// Reverse direction: exhaust sendCode budget with a one-call limit, then
	// prove signIn still works (the sendCode budget does not spend signIn budget).
	sendCodeCfg := store.SendCodeIPLimits{
		Calls: store.RateLimitConfig{Limit: 1, Window: 10 * time.Second},
	}
	_, err = api.SendCodeForTest(s, addr, sendCodeCfg, "+15551296503")
	if isFloodWait(err) {
		t.Fatalf("first sendCode: unexpected FLOOD_WAIT, got %v", err)
	}
	_, err = api.SendCodeForTest(s, addr, sendCodeCfg, "+15551296504")
	if !isFloodWait(err) {
		t.Fatalf("second sendCode: expected FLOOD_WAIT (sendCode budget exhausted), got %v", err)
	}

	// signIn from the same IP should still work — sendCode exhaustion is
	// independent of signIn-fail budget. Use a fresh config with a higher limit
	// so the signIn-fail budget is not exhausted by the earlier wrong guess.
	freshCfg := store.RateLimitConfig{Limit: 10, Window: 10 * time.Second}
	hash2, _, err := s.IssueCode(ctx, "+15551296505")
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, freshCfg, &tg.AuthSignInRequest{
		PhoneNumber:   "+15551296505",
		PhoneCodeHash: hash2,
		PhoneCode:     "00000",
	})
	if isFloodWait(err) {
		t.Fatalf("signIn after sendCode exhaustion: unexpected FLOOD_WAIT, got %v", err)
	}
}

// TestSignInFailCorrectCodeRefunded proves the reserve-and-refund model:
// a correct code from a non-exhausted IP succeeds and the token_count is
// unchanged after refund (net zero charge on success).
func TestSignInFailCorrectCodeRefunded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296701"
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so BindAuthKeyUser succeeds on a correct sign-in.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.70")

	// Limit of 1 failure per 10s: after a correct sign-in that refunds
	// the slot, token_count should be 0/1 — so a subsequent wrong guess
	// gets PHONE_CODE_INVALID. Without the refund, counter sits at 1/1
	// and the wrong guess gets FLOOD_WAIT instead.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Correct code — should succeed and refund the reserved slot.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if err != nil {
		t.Fatalf("correct code: expected success, got %v", err)
	}

	// The counter should be at 0 (reserved 1, refunded 1 = net zero).
	// Verify by checking that a subsequent wrong guess is still allowed
	// (budget not consumed by the successful sign-in).
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000", // wrong
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("wrong guess after correct sign-in: expected PHONE_CODE_INVALID, got %v", err)
	}
}

// TestSignInFailCorrectCodeAtExactLimit proves that a correct code from an
// exactly-exhausted IP returns FLOOD_WAIT (the reserve finds count >= limit
// before VerifyCode even runs).
func TestSignInFailCorrectCodeAtExactLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296801"
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so BindAuthKeyUser succeeds on a correct sign-in.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.80")

	// Limit of 1 failure per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// One wrong guess — reserves the only slot, keeps it.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     "00000", // wrong
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("wrong guess: expected PHONE_CODE_INVALID, got %v", err)
	}

	// Now the budget is at 1/1. A correct code from the same IP should
	// get FLOOD_WAIT — the reserve finds count >= limit.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code, // correct
	})
	if !isFloodWait(err) {
		t.Fatalf("correct code at limit: expected FLOOD_WAIT, got %v", err)
	}
}
