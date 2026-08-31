package api_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSignInUnknownPhoneRefusesWithoutCreatingOrBinding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	phone := "+15551298001"
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}
	const keyID = int64(0x1)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	before := countUsers(t, dsn)
	addr := netip.MustParseAddr("10.0.0.1")
	limits := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, limits, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("unknown phone signIn: expected PHONE_CODE_INVALID, got %v", err)
	}

	if after := countUsers(t, dsn); after != before {
		t.Fatalf("users table grew from %d to %d", before, after)
	}
	key, ok, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("auth key disappeared")
	}
	if key.UserID != 0 || key.PendingUserID != 0 {
		t.Fatalf("unknown phone signIn changed key binding: user_id=%d pending_user_id=%d", key.UserID, key.PendingUserID)
	}

	// A second unknown-identity attempt from the same address must see the
	// same failure budget as a bad code, rather than getting a free lookup.
	secondPhone := "+15551298002"
	secondHash, secondCode, err := s.IssueCode(ctx, secondPhone)
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.SignInForTestWithLimits(s, [8]byte{2}, addr, limits, &tg.AuthSignInRequest{
		PhoneNumber:   secondPhone,
		PhoneCodeHash: secondHash,
		PhoneCode:     secondCode,
	})
	if !isFloodWait(err) {
		t.Fatalf("second unknown phone signIn: expected FLOOD_WAIT, got %v", err)
	}
}

func TestSignInKnownPhoneStillAuthorizes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551298003"
	user, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, int64(0x2), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	res, err := api.SignInForTestWithLimits(s, [8]byte{2}, netip.MustParseAddr("10.0.0.2"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if err != nil {
		t.Fatalf("known phone signIn: %v", err)
	}
	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthAuthorization", res)
	}
	got, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("authorization user = %T, want *tg.User", auth.User)
	}
	if got.ID != user.ID {
		t.Errorf("authorized user id = %d, want %d", got.ID, user.ID)
	}
}
