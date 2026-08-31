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
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// isInputRequestInvalid reports whether err is a 400 INPUT_REQUEST_INVALID error.
func isInputRequestInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "INPUT_REQUEST_INVALID"
}

func TestSignUpClosedAndInviteRefuseWithoutStateChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	const keyID = int64(0x1)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}

	before := countUsers(t, dsn)
	addr := netip.MustParseAddr("10.0.0.1")
	limits := store.RateLimitConfig{}
	request := &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: hash,
		FirstName:     "Alice",
	}

	for _, mode := range []config.RegistrationMode{
		config.RegistrationClosed,
		config.RegistrationInvite,
	} {
		if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, request); !isInputRequestInvalid(err) {
			t.Fatalf("signUp with %q registration: expected INPUT_REQUEST_INVALID, got %v", mode, err)
		}

		if after := countUsers(t, dsn); after != before {
			t.Fatalf("signUp with %q registration changed users from %d to %d", mode, before, after)
		}
		key, ok, err := s.AuthKeyByID(ctx, keyID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("auth key disappeared")
		}
		if key.UserID != 0 || key.PendingUserID != 0 {
			t.Fatalf("signUp with %q registration changed key binding: user_id=%d pending_user_id=%d", mode, key.UserID, key.PendingUserID)
		}
	}
}

func TestSignUpGatePrecedesRequestValidation(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("10.0.0.2")
	limits := store.RateLimitConfig{}

	for _, mode := range []config.RegistrationMode{
		config.RegistrationClosed,
		config.RegistrationInvite,
	} {
		_, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, &tg.AuthSignUpRequest{})
		if !isInputRequestInvalid(err) {
			t.Fatalf("empty signUp with %q registration: expected INPUT_REQUEST_INVALID, got %v", mode, err)
		}
	}
}

func TestSignUpRateLimitAppliesToClosedAndInvite(t *testing.T) {
	t.Parallel()

	for _, mode := range []config.RegistrationMode{
		config.RegistrationClosed,
		config.RegistrationInvite,
	} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			s := openStore(t)
			addr := netip.MustParseAddr("10.0.0.3")
			limits := store.RateLimitConfig{Limit: 1, Window: time.Hour}
			request := &tg.AuthSignUpRequest{
				PhoneNumber:   "alice",
				PhoneCodeHash: "hash",
				FirstName:     "Alice",
			}

			if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, request); !isInputRequestInvalid(err) {
				t.Fatalf("first signUp: expected INPUT_REQUEST_INVALID, got %v", err)
			}
			if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, request); !isFloodWait(err) {
				t.Fatalf("second signUp: expected FLOOD_WAIT, got %v", err)
			}
		})
	}
}
