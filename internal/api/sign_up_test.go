package api_test

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// isInputRequestInvalid reports whether err is a 400 INPUT_REQUEST_INVALID error.
func isInputRequestInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "INPUT_REQUEST_INVALID"
}

func TestSignUpInviteAdmissionCreatesProvisionalAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	const keyID = int64(0x1)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	invite, secret, err := s.IssueInvite(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.1"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: hash,
		PhoneCode:     secret,
	})
	if err != nil {
		t.Fatalf("signIn: %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("signIn result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to inspect code: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close code inspection connection: %v", err)
		}
	})
	var storedCode string
	if err := conn.QueryRow(ctx, `SELECT code FROM phone_codes WHERE code_hash = $1`, hash).Scan(&storedCode); err != nil {
		t.Fatalf("read code handoff: %v", err)
	}
	if storedCode != secret {
		t.Fatalf("stored code = %q, want invite secret", storedCode)
	}

	before := countUsers(t, dsn)
	res, err = api.SignUpForTest(s, [8]byte{1}, netip.MustParseAddr("10.0.0.1"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: hash,
		FirstName:     "Alice",
	})
	if err != nil {
		t.Fatalf("signUp: %v", err)
	}
	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("signUp result = %T, want *tg.AuthAuthorization", res)
	}
	user, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("authorization user = %T, want *tg.User", auth.User)
	}
	if user.FirstName != "Alice" {
		t.Errorf("first name = %q, want Alice", user.FirstName)
	}
	if after := countUsers(t, dsn); after != before+1 {
		t.Fatalf("users table grew from %d to %d, want +1", before, after)
	}

	resolved, found, err := s.UserByUsernameWithLoginMode(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !found || resolved.ID != user.ID || resolved.LoginMode != "username" {
		t.Fatalf("resolved user = %#v found=%v, want username-mode user %d", resolved, found, user.ID)
	}
	key, found, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || key.UserID != user.ID || !key.Provisional {
		t.Fatalf("auth key = %#v found=%v, want provisional binding to user %d", key, found, user.ID)
	}
	invites, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invites) != 1 || invites[0].ID != invite.ID || invites[0].State != store.InviteConsumed {
		t.Fatalf("invites = %#v, want invite %d consumed", invites, invite.ID)
	}
	if err := conn.QueryRow(ctx, `SELECT code FROM phone_codes WHERE code_hash = $1`, hash).Scan(&storedCode); err != nil {
		t.Fatalf("read cleared code handoff: %v", err)
	}
	if storedCode != "" {
		t.Fatalf("stored code after admission = %q, want empty", storedCode)
	}
}

func TestSignUpRegistrationModes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	addr := netip.MustParseAddr("10.0.0.11")
	limits := store.RateLimitConfig{}

	if err := s.SaveAuthKey(ctx, 1, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	before := countUsers(t, dsn)
	if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, config.RegistrationClosed, &tg.AuthSignUpRequest{
		PhoneNumber: "closeduser",
		FirstName:   "Closed",
	}); !isInputRequestInvalid(err) {
		t.Fatalf("closed signUp: expected INPUT_REQUEST_INVALID, got %v", err)
	}
	if after := countUsers(t, dsn); after != before {
		t.Fatalf("closed signUp changed users from %d to %d", before, after)
	}

	invite, _, err := s.IssueInvite(ctx, "inviteuser")
	if err != nil {
		t.Fatal(err)
	}
	inviteHash, _, err := s.IssueCodeForUsername(ctx, "inviteuser")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, 2, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignInForTestWithLimits(s, [8]byte{2}, addr, limits, &tg.AuthSignInRequest{
		PhoneNumber:   "inviteuser",
		PhoneCodeHash: inviteHash,
		PhoneCode:     "wrong-secret",
	}); err != nil {
		t.Fatalf("invite signIn: %v", err)
	}
	if _, err := api.SignUpForTest(s, [8]byte{2}, addr, limits, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "inviteuser",
		PhoneCodeHash: inviteHash,
		FirstName:     "Invite",
	}); !isInputRequestInvalid(err) {
		t.Fatalf("invite signUp without valid secret: expected INPUT_REQUEST_INVALID, got %v", err)
	}

	openHash, _, err := s.IssueCodeForUsername(ctx, "openuser")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, 3, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignInForTestWithLimits(s, [8]byte{3}, addr, limits, &tg.AuthSignInRequest{
		PhoneNumber:   "openuser",
		PhoneCodeHash: openHash,
		PhoneCode:     "open-proof",
	}); err != nil {
		t.Fatalf("open signIn: %v", err)
	}
	res, err := api.SignUpForTest(s, [8]byte{3}, addr, limits, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "openuser",
		PhoneCodeHash: openHash,
		FirstName:     "Open",
	})
	if err != nil {
		t.Fatalf("open signUp: %v", err)
	}
	if _, ok := res.(*tg.AuthAuthorization); !ok {
		t.Fatalf("open signUp result = %T, want *tg.AuthAuthorization", res)
	}
	if after := countUsers(t, dsn); after != before+1 {
		t.Fatalf("open signUp changed users from %d to %d, want %d", before, after, before+1)
	}
	resolved, found, err := s.UserByUsernameWithLoginMode(ctx, "openuser")
	if err != nil {
		t.Fatal(err)
	}
	if !found || resolved.LoginMode != "username" {
		t.Fatalf("open user lookup = %#v found=%v, want username-mode user", resolved, found)
	}
	key, found, err := s.AuthKeyByID(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !found || key.UserID != resolved.ID || !key.Provisional {
		t.Fatalf("open auth key = %#v found=%v, want provisional binding to user %d", key, found, resolved.ID)
	}
	invites, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invites) != 1 || invites[0].ID != invite.ID || invites[0].State != store.InviteIssued {
		t.Fatalf("invites = %#v, want invite %d still issued", invites, invite.ID)
	}
}

func TestSignUpRejectsOverlongOrNulDisplayNames(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	addr := netip.MustParseAddr("10.0.0.10")
	limits := store.RateLimitConfig{}
	cases := []struct {
		handle    string
		firstName string
		lastName  string
	}{
		{handle: "longfirst", firstName: strings.Repeat("a", 65), lastName: "Valid"},
		{handle: "longlast", firstName: "Valid", lastName: strings.Repeat("界", 65)},
		{handle: "nulname", firstName: "Bad\x00Name", lastName: "Valid"},
	}

	for i, tc := range cases {
		invite, secret, err := s.IssueInvite(ctx, tc.handle)
		if err != nil {
			t.Fatal(err)
		}
		hash, _, err := s.IssueCodeForUsername(ctx, tc.handle)
		if err != nil {
			t.Fatal(err)
		}
		keyID := int64(i + 1)
		if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
		if _, err := api.SignInForTestWithLimits(s, [8]byte{byte(i + 1)}, addr, limits, &tg.AuthSignInRequest{
			PhoneNumber:   tc.handle,
			PhoneCodeHash: hash,
			PhoneCode:     secret,
		}); err != nil {
			t.Fatalf("signIn %q: %v", tc.handle, err)
		}

		_, err = api.SignUpForTest(s, [8]byte{byte(i + 1)}, addr, limits, config.RegistrationInvite, &tg.AuthSignUpRequest{
			PhoneNumber:   tc.handle,
			PhoneCodeHash: hash,
			FirstName:     tc.firstName,
			LastName:      tc.lastName,
		})
		if !isInputRequestInvalid(err) {
			t.Fatalf("signUp %q: expected INPUT_REQUEST_INVALID, got %v", tc.handle, err)
		}

		key, ok, err := s.AuthKeyByID(ctx, keyID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || key.UserID != 0 || key.PendingUserID != 0 {
			t.Fatalf("signUp %q changed auth key: %#v found=%v", tc.handle, key, ok)
		}
		invites, err := s.ListInvites(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(invites) != i+1 || invites[i].ID != invite.ID || invites[i].State != store.InviteIssued {
			t.Fatalf("signUp %q changed invite state: %#v", tc.handle, invites)
		}
	}
}

func TestConcurrentSignUpWithOneInviteHasExactlyOneWinner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s := openStore(t)

	invite, secret, err := s.IssueInvite(ctx, "racing")
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "racing")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.2"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "racing",
		PhoneCodeHash: hash,
		PhoneCode:     secret,
	}); err != nil {
		t.Fatalf("signIn: %v", err)
	}

	const attempts = 10
	authKeyIDs := [attempts][8]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}}
	for i := range attempts {
		if err := s.SaveAuthKey(ctx, int64(i+1), make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
	}
	results := make([]error, attempts)
	ready := make(chan struct{})
	var readyWG sync.WaitGroup
	var wg sync.WaitGroup
	readyWG.Add(attempts)
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			readyWG.Done()
			<-ready
			_, results[i] = api.SignUpForTest(s, authKeyIDs[i], netip.MustParseAddr("10.0.0.2"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
				PhoneNumber:   "racing",
				PhoneCodeHash: hash,
				FirstName:     "Racer",
			})
		}(i)
	}
	readyWG.Wait()
	close(ready)
	wg.Wait()

	var successes, invalids int
	for i, err := range results {
		switch {
		case err == nil:
			successes++
		case isInputRequestInvalid(err):
			invalids++
		default:
			t.Errorf("attempt %d: unexpected error: %v", i, err)
		}
	}
	if successes != 1 || invalids != attempts-1 {
		t.Fatalf("signUp results: successes=%d invalids=%d, want 1/%d", successes, invalids, attempts-1)
	}
	invites, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invites) != 1 || invites[0].ID != invite.ID || invites[0].State != store.InviteConsumed {
		t.Fatalf("invites = %#v, want invite %d consumed", invites, invite.ID)
	}
}

func TestSignUpFailureAfterInviteConsumptionRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	invite, secret, err := s.IssueInvite(ctx, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		PhoneCode:     secret,
	}); err != nil {
		t.Fatalf("signIn: %v", err)
	}

	before := countUsers(t, dsn)
	_, err = api.SignUpForTest(s, [8]byte{1}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		FirstName:     "Rollback",
	})
	if !isInputRequestInvalid(err) {
		t.Fatalf("signUp without auth key: expected INPUT_REQUEST_INVALID, got %v", err)
	}
	if after := countUsers(t, dsn); after != before {
		t.Fatalf("users table grew from %d to %d after rollback, want unchanged", before, after)
	}
	invites, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invites) != 1 || invites[0].ID != invite.ID || invites[0].State != store.InviteIssued {
		t.Fatalf("invites = %#v, want invite %d issued after rollback", invites, invite.ID)
	}
	if err := s.SaveAuthKey(ctx, 2, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignUpForTest(s, [8]byte{2}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		FirstName:     "Rollback",
	}); err != nil {
		t.Fatalf("signUp after rollback: invite was not still consumable: %v", err)
	}
}

func TestSignUpRateLimitChargesBeforeIdentifierLookup(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	limits := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	for _, tc := range []struct {
		mode      config.RegistrationMode
		addr      netip.Addr
		firstKey  [8]byte
		secondKey [8]byte
	}{
		{
			mode:      config.RegistrationInvite,
			addr:      netip.MustParseAddr("10.0.0.4"),
			firstKey:  [8]byte{1},
			secondKey: [8]byte{2},
		},
		{
			mode:      config.RegistrationOpen,
			addr:      netip.MustParseAddr("10.0.0.5"),
			firstKey:  [8]byte{3},
			secondKey: [8]byte{4},
		},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			request := &tg.AuthSignUpRequest{
				PhoneNumber:   "unknownhandle",
				PhoneCodeHash: "invalid-hash",
				FirstName:     "Alice",
			}
			if _, err := api.SignUpForTest(s, tc.firstKey, tc.addr, limits, tc.mode, request); !isInputRequestInvalid(err) {
				t.Fatalf("first signUp: expected INPUT_REQUEST_INVALID, got %v", err)
			}
			if _, err := api.SignUpForTest(s, tc.secondKey, tc.addr, limits, tc.mode, request); !isFloodWait(err) {
				t.Fatalf("second signUp: expected FLOOD_WAIT, got %v", err)
			}
		})
	}
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
