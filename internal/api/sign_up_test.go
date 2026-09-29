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

func signUpRPCMessage(err error) string {
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		return ""
	}
	return rpc.Message
}

func TestSignUpInviteAdmissionCreatesProvisionalAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	const keyID = int64(0x1)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	invite, secret, err := s.IssueInvite(ctx, "ab")
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "ab")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.1"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "Ab",
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
		PhoneNumber:   "Ab",
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

	resolved, found, err := s.UserByUsernameWithLoginMode(ctx, "ab")
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
	administrator, err := s.IsServerAdministrator(ctx, user.ID)
	if err != nil {
		t.Fatalf("check elected administrator: %v", err)
	}
	if !administrator {
		t.Fatal("first invite admission did not elect the new user")
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

func TestSignUpRejectsReservedHandles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	addr := netip.MustParseAddr("10.0.0.13")
	limits := store.RateLimitConfig{}

	for i, handle := range []string{"help", "ADMIN", "me"} {
		keyID := int64(i + 1)
		authKeyID := [8]byte{byte(i + 1)}
		if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
			t.Fatalf("save auth key %q: %v", handle, err)
		}

		sent, err := api.SendCodeForTest(s, addr, store.SendCodeIPLimits{}, handle)
		if err != nil {
			t.Fatalf("sendCode %q: %v", handle, err)
		}
		sentCode, ok := sent.(*tg.AuthSentCode)
		if !ok {
			t.Fatalf("sendCode result = %T, want *tg.AuthSentCode", sent)
		}

		_, err = api.SignInForTestWithLimits(s, authKeyID, addr, limits, &tg.AuthSignInRequest{
			PhoneNumber:   handle,
			PhoneCodeHash: sentCode.PhoneCodeHash,
			PhoneCode:     "admission-secret",
		})
		if err != nil {
			t.Fatalf("signIn %q: %v", handle, err)
		}

		_, err = api.SignUpForTest(s, authKeyID, addr, limits, config.RegistrationOpen, &tg.AuthSignUpRequest{
			PhoneNumber:   handle,
			PhoneCodeHash: sentCode.PhoneCodeHash,
			FirstName:     "Reserved",
		})
		if got := signUpRPCMessage(err); got != "USERNAME_INVALID" {
			t.Fatalf("signUp %q: expected USERNAME_INVALID, got %v", handle, err)
		}

		if _, found, err := s.UserByUsernameWithLoginMode(ctx, strings.ToLower(handle)); err != nil {
			t.Fatalf("lookup reserved handle %q: %v", handle, err)
		} else if found {
			t.Errorf("reserved handle %q created a user", handle)
		}
		key, found, err := s.AuthKeyByID(ctx, keyID)
		if err != nil {
			t.Fatalf("lookup auth key %q: %v", handle, err)
		}
		if !found || key.UserID != 0 || key.PendingUserID != 0 {
			t.Errorf("signUp %q changed auth key binding: %#v found=%v", handle, key, found)
		}
	}
}

func TestOpenSignUpElectsOnlyTheFirstNewUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	addr := netip.MustParseAddr("10.0.0.12")
	limits := store.RateLimitConfig{}

	for i, handle := range []string{"owner1", "member1"} {
		keyID := [8]byte{byte(i + 1)}
		if err := s.SaveAuthKey(ctx, int64(i+1), make([]byte, 256)); err != nil {
			t.Fatalf("save auth key %s: %v", handle, err)
		}
		hash, _, err := s.IssueCodeForUsername(ctx, handle)
		if err != nil {
			t.Fatalf("issue code %s: %v", handle, err)
		}
		if _, err := api.SignInForTestWithLimits(s, keyID, addr, limits, &tg.AuthSignInRequest{
			PhoneNumber:   handle,
			PhoneCodeHash: hash,
			PhoneCode:     "open-proof",
		}); err != nil {
			t.Fatalf("signIn %s: %v", handle, err)
		}
		res, err := api.SignUpForTest(s, keyID, addr, limits, config.RegistrationOpen, &tg.AuthSignUpRequest{
			PhoneNumber:   handle,
			PhoneCodeHash: hash,
			FirstName:     handle,
		})
		if err != nil {
			t.Fatalf("signUp %s: %v", handle, err)
		}
		auth, ok := res.(*tg.AuthAuthorization)
		if !ok {
			t.Fatalf("signUp %s result = %T, want *tg.AuthAuthorization", handle, res)
		}
		user, ok := auth.User.(*tg.User)
		if !ok {
			t.Fatalf("signUp %s user = %T, want *tg.User", handle, auth.User)
		}
		administrator, err := s.IsServerAdministrator(ctx, user.ID)
		if err != nil {
			t.Fatalf("check %s administrator: %v", handle, err)
		}
		if administrator != (i == 0) {
			t.Fatalf("%s administrator = %v, want %v", handle, administrator, i == 0)
		}
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
	}); signUpRPCMessage(err) != "INVITE_HASH_INVALID" {
		t.Fatalf("invite signUp without valid secret: expected INVITE_HASH_INVALID, got %v", err)
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
		want := "FIRSTNAME_INVALID"
		if tc.handle == "longlast" {
			want = "LASTNAME_INVALID"
		}
		if got := signUpRPCMessage(err); got != want {
			t.Fatalf("signUp %q: expected %s, got %v", tc.handle, want, err)
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
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close inspection connection: %v", err)
		}
	})

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
	readAdmissionState := func() (users, usernameClaims, updateStates, administrators, boundKeys, pendingKeys int64) {
		t.Helper()
		if err := conn.QueryRow(ctx, `
			SELECT
				(SELECT count(*) FROM users),
				(SELECT count(*) FROM usernames WHERE handle = 'racing'),
				(SELECT count(*) FROM update_state),
				(SELECT count(*) FROM server_administration WHERE administrator_user_id IS NOT NULL),
				(SELECT count(*) FROM auth_keys WHERE user_id IS NOT NULL),
				(SELECT count(*) FROM auth_keys WHERE pending_user_id IS NOT NULL)
		`).Scan(&users, &usernameClaims, &updateStates, &administrators, &boundKeys, &pendingKeys); err != nil {
			t.Fatalf("read concurrent admission state: %v", err)
		}
		return
	}
	beforeUsers, beforeClaims, beforeUpdateStates, beforeAdministrators, beforeBoundKeys, beforePendingKeys := readAdmissionState()
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

	var successes, losers int
	for i, err := range results {
		switch {
		case err == nil:
			successes++
		case signUpRPCMessage(err) == "INVITE_HASH_INVALID",
			signUpRPCMessage(err) == "PHONE_CODE_INVALID",
			signUpRPCMessage(err) == "USERNAME_OCCUPIED":
			losers++
		default:
			t.Errorf("attempt %d: unexpected error: %v", i, err)
		}
	}
	if successes != 1 || losers != attempts-1 {
		t.Fatalf("signUp results: successes=%d losers=%d, want 1/%d", successes, losers, attempts-1)
	}
	afterUsers, afterClaims, afterUpdateStates, afterAdministrators, afterBoundKeys, afterPendingKeys := readAdmissionState()
	if afterUsers != beforeUsers+1 || afterClaims != beforeClaims+1 || afterUpdateStates != beforeUpdateStates+1 || afterAdministrators != beforeAdministrators+1 || afterBoundKeys != beforeBoundKeys+1 || afterPendingKeys != beforePendingKeys {
		t.Fatalf("same-invite admission state changed by more than winner: users %d/%d claims %d/%d update_states %d/%d administrators %d/%d bound_keys %d/%d pending_keys %d/%d", afterUsers, beforeUsers+1, afterClaims, beforeClaims+1, afterUpdateStates, beforeUpdateStates+1, afterAdministrators, beforeAdministrators+1, afterBoundKeys, beforeBoundKeys+1, afterPendingKeys, beforePendingKeys)
	}
	resolved, found, err := s.UserByUsernameWithLoginMode(ctx, "racing")
	if err != nil {
		t.Fatal(err)
	}
	if !found || resolved.LoginMode != "username" {
		t.Fatalf("racing handle = %#v found=%v, want one username-mode winner", resolved, found)
	}
	var administratorID int64
	if err := conn.QueryRow(ctx, `SELECT administrator_user_id FROM server_administration WHERE singleton_id = 1`).Scan(&administratorID); err != nil {
		t.Fatalf("read race administrator: %v", err)
	}
	if administratorID != resolved.ID {
		t.Fatalf("administrator user = %d, want sole admitted user %d", administratorID, resolved.ID)
	}
	var codeConsumed bool
	if err := conn.QueryRow(ctx, `SELECT consumed_at IS NOT NULL FROM phone_codes WHERE phone = 'racing'`).Scan(&codeConsumed); err != nil {
		t.Fatalf("read race code state: %v", err)
	}
	if !codeConsumed {
		t.Fatal("winning admission did not consume the sign-up code")
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
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close inspection connection: %v", err)
		}
	})

	invite, secret, err := s.IssueInvite(ctx, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, "rollback")
	if err != nil {
		t.Fatal(err)
	}
	const keyID = int64(1)
	const authKeyValue = "rollback-auth-key"
	if err := s.SaveAuthKey(ctx, keyID, []byte(authKeyValue)); err != nil {
		t.Fatal(err)
	}
	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		PhoneCode:     secret,
	})
	if err != nil {
		t.Fatalf("signIn: %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("signIn result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}

	// Remove the election singleton so electServerAdministrator fails only
	// after invite and code consumption, user creation, the update-state row,
	// and the username claim have all been attempted in AdmitUsername's
	// transaction. The deleted singleton is the baseline state restored by the
	// rollback assertion below.
	if _, err := conn.Exec(ctx, `DELETE FROM server_administration`); err != nil {
		t.Fatal(err)
	}
	readCounts := func() (users, claims, updateStates, administrationRows, administrators int64) {
		t.Helper()
		if err := conn.QueryRow(ctx, `
			SELECT
				(SELECT count(*) FROM users),
				(SELECT count(*) FROM usernames WHERE handle = 'rollback'),
				(SELECT count(*) FROM update_state),
				(SELECT count(*) FROM server_administration),
				(SELECT count(*) FROM server_administration WHERE administrator_user_id IS NOT NULL)
		`).Scan(&users, &claims, &updateStates, &administrationRows, &administrators); err != nil {
			t.Fatalf("read rollback counts: %v", err)
		}
		return
	}
	readCode := func() (code string, consumed bool) {
		t.Helper()
		if err := conn.QueryRow(ctx, `SELECT code, consumed_at IS NOT NULL FROM phone_codes WHERE phone = 'rollback'`).Scan(&code, &consumed); err != nil {
			t.Fatalf("read rollback code: %v", err)
		}
		return
	}
	readKey := func() store.AuthKey {
		t.Helper()
		key, found, err := s.AuthKeyByID(ctx, keyID)
		if err != nil {
			t.Fatalf("read rollback auth key: %v", err)
		}
		if !found {
			t.Fatal("rollback auth key disappeared")
		}
		return key
	}

	beforeUsers, beforeClaims, beforeUpdateStates, beforeAdministrationRows, beforeAdministrators := readCounts()
	beforeCode, beforeConsumed := readCode()
	beforeKey := readKey()
	if beforeCode != secret || beforeConsumed {
		t.Fatalf("pre-admission code = %q consumed=%v, want invite secret and unconsumed", beforeCode, beforeConsumed)
	}
	if beforeKey.UserID != 0 || beforeKey.PendingUserID != 0 || string(beforeKey.Value) != authKeyValue {
		t.Fatalf("pre-admission auth key = %#v, want unbound saved key", beforeKey)
	}

	_, err = api.SignUpForTest(s, [8]byte{1}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		FirstName:     "Rollback",
	})
	if signUpRPCMessage(err) != "INTERNAL" {
		t.Fatalf("signUp after admission writes: expected INTERNAL, got %v", err)
	}
	afterUsers, afterClaims, afterUpdateStates, afterAdministrationRows, afterAdministrators := readCounts()
	if afterUsers != beforeUsers || afterClaims != beforeClaims || afterUpdateStates != beforeUpdateStates || afterAdministrationRows != beforeAdministrationRows || afterAdministrators != beforeAdministrators {
		t.Fatalf("rollback counts = users %d/%d claims %d/%d update_states %d/%d administration rows %d/%d administrators %d/%d, want unchanged", afterUsers, beforeUsers, afterClaims, beforeClaims, afterUpdateStates, beforeUpdateStates, afterAdministrationRows, beforeAdministrationRows, afterAdministrators, beforeAdministrators)
	}
	afterCode, afterConsumed := readCode()
	if afterCode != beforeCode || afterConsumed != beforeConsumed {
		t.Fatalf("rollback code = %q consumed=%v, want %q consumed=%v", afterCode, afterConsumed, beforeCode, beforeConsumed)
	}
	key := readKey()
	if key.UserID != beforeKey.UserID || key.PendingUserID != beforeKey.PendingUserID || string(key.Value) != string(beforeKey.Value) || !key.CreatedAt.Equal(beforeKey.CreatedAt) || !key.LastSeenAt.Equal(beforeKey.LastSeenAt) {
		t.Fatalf("rollback auth key = %#v, want unchanged binding and value", key)
	}
	invites, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invites) != 1 || invites[0].ID != invite.ID || invites[0].State != store.InviteIssued {
		t.Fatalf("invites = %#v, want invite %d issued and unconsumed after rollback", invites, invite.ID)
	}
	if invites[0].ConsumedAt != nil || invites[0].RevokedAt != nil {
		t.Fatalf("invite after rollback = %#v, want no terminal timestamps", invites[0])
	}

	// Restore the singleton so the same invite can prove that the failed
	// transaction left every admission input reusable.
	if _, err := conn.Exec(ctx, `INSERT INTO server_administration (singleton_id, election_closed) VALUES (1, FALSE)`); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, 2, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	res, err = api.SignUpForTest(s, [8]byte{2}, netip.MustParseAddr("10.0.0.3"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   "rollback",
		PhoneCodeHash: hash,
		FirstName:     "Rollback",
	})
	if err != nil {
		t.Fatalf("signUp after rollback: invite was not still consumable: %v", err)
	}
	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("signUp after rollback result = %T, want *tg.AuthAuthorization", res)
	}
	user, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("signUp after rollback user = %T, want *tg.User", auth.User)
	}
	if administrator, err := s.IsServerAdministrator(ctx, user.ID); err != nil {
		t.Fatalf("check winner after rollback: %v", err)
	} else if !administrator {
		t.Fatal("waiting sign-up did not win after the first transaction rolled back")
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
			if err := s.SaveAuthKey(context.Background(), int64(tc.firstKey[0]), make([]byte, 256)); err != nil {
				t.Fatal(err)
			}
			request := &tg.AuthSignUpRequest{
				PhoneNumber:   "unknownhandle",
				PhoneCodeHash: "invalid-hash",
				FirstName:     "Alice",
			}
			if _, err := api.SignUpForTest(s, tc.firstKey, tc.addr, limits, tc.mode, request); signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
				t.Fatalf("first signUp: expected PHONE_CODE_INVALID, got %v", err)
			}
			if _, err := api.SignUpForTest(s, tc.secondKey, tc.addr, limits, tc.mode, request); !isFloodWait(err) {
				t.Fatalf("second signUp: expected FLOOD_WAIT, got %v", err)
			}
		})
	}
}

func TestSignUpRateLimitVariantsReachSameThirdAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	known, err := s.CreateUsernameUser(ctx, "knownrate", "Known", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimUsername(ctx, known.ID, "knownrate"); err != nil {
		t.Fatal(err)
	}
	knownHash, _, err := s.IssueCodeForUsername(ctx, "knownrate")
	if err != nil {
		t.Fatal(err)
	}
	unknownHash, _, err := s.IssueCodeForUsername(ctx, "unknownrate")
	if err != nil {
		t.Fatal(err)
	}

	limits := store.RateLimitConfig{Limit: 2, Window: time.Hour}
	for _, tc := range []struct {
		name      string
		handle    string
		hash      string
		want      string
		authKeyID [8]byte
		addr      netip.Addr
	}{
		{
			name:      "known-valid-hash",
			handle:    "knownrate",
			hash:      knownHash,
			want:      "USERNAME_OCCUPIED",
			authKeyID: [8]byte{0x40},
			addr:      netip.MustParseAddr("10.0.0.40"),
		},
		{
			name:      "known-invalid-hash",
			handle:    "knownrate",
			hash:      "invalid-known-rate",
			want:      "PHONE_CODE_INVALID",
			authKeyID: [8]byte{0x41},
			addr:      netip.MustParseAddr("10.0.0.41"),
		},
		{
			name:      "unknown-valid-hash",
			handle:    "unknownrate",
			hash:      unknownHash,
			want:      "INVITE_HASH_INVALID",
			authKeyID: [8]byte{0x42},
			addr:      netip.MustParseAddr("10.0.0.42"),
		},
		{
			name:      "unknown-invalid-hash",
			handle:    "unknownrate",
			hash:      "invalid-unknown-rate",
			want:      "PHONE_CODE_INVALID",
			authKeyID: [8]byte{0x43},
			addr:      netip.MustParseAddr("10.0.0.43"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.SaveAuthKey(ctx, int64(tc.authKeyID[0]), make([]byte, 256)); err != nil {
				t.Fatal(err)
			}
			request := &tg.AuthSignUpRequest{
				PhoneNumber:   tc.handle,
				PhoneCodeHash: tc.hash,
				FirstName:     "Rate",
			}
			for attempt := 1; attempt <= 2; attempt++ {
				if _, err := api.SignUpForTest(s, tc.authKeyID, tc.addr, limits, config.RegistrationInvite, request); signUpRPCMessage(err) != tc.want {
					t.Fatalf("attempt %d: expected %s, got %v", attempt, tc.want, err)
				}
			}
			if _, err := api.SignUpForTest(s, tc.authKeyID, tc.addr, limits, config.RegistrationInvite, request); !isFloodWait(err) {
				t.Fatalf("third attempt: expected FLOOD_WAIT, got %v", err)
			}
		})
	}
}

func TestSignUpClosedModesDoNotChargeEnabledRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	limits := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	for _, tc := range []struct {
		name      string
		mode      config.RegistrationMode
		handle    string
		authKeyID [8]byte
		addr      netip.Addr
	}{
		{
			name:      "closed",
			mode:      config.RegistrationClosed,
			handle:    "closedrate",
			authKeyID: [8]byte{0x50},
			addr:      netip.MustParseAddr("10.0.0.50"),
		},
		{
			name:      "unknown",
			mode:      config.RegistrationMode("unknown"),
			handle:    "unknownmode",
			authKeyID: [8]byte{0x51},
			addr:      netip.MustParseAddr("10.0.0.51"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hash, _, err := s.IssueCodeForUsername(ctx, tc.handle)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SaveAuthKey(ctx, int64(tc.authKeyID[0]), make([]byte, 256)); err != nil {
				t.Fatal(err)
			}
			request := &tg.AuthSignUpRequest{
				PhoneNumber:   tc.handle,
				PhoneCodeHash: hash,
				FirstName:     "Rate",
			}
			if _, err := api.SignUpForTest(s, tc.authKeyID, tc.addr, limits, tc.mode, request); signUpRPCMessage(err) != "INPUT_REQUEST_INVALID" {
				t.Fatalf("%s registration: expected INPUT_REQUEST_INVALID, got %v", tc.mode, err)
			}

			res, err := api.SignUpForTest(s, tc.authKeyID, tc.addr, limits, config.RegistrationOpen, request)
			if err != nil {
				t.Fatalf("open registration after %s gate: %v", tc.mode, err)
			}
			if auth, ok := res.(*tg.AuthAuthorization); !ok || auth.User == nil {
				t.Fatalf("open registration after %s gate result = %T, want authorization", tc.mode, res)
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
		config.RegistrationMode("unknown"),
	} {
		want := "INPUT_REQUEST_INVALID"
		if mode == config.RegistrationInvite {
			want = "INVITE_HASH_INVALID"
		}
		if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, request); signUpRPCMessage(err) != want {
			t.Fatalf("signUp with %q registration: expected %s, got %v", mode, want, err)
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
		config.RegistrationMode("unknown"),
	} {
		_, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, mode, &tg.AuthSignUpRequest{})
		want := "INPUT_REQUEST_INVALID"
		if mode == config.RegistrationInvite {
			want = "SESSION_STATE_INVALID"
		}
		if signUpRPCMessage(err) != want {
			t.Fatalf("empty signUp with %q registration: expected %s, got %v", mode, want, err)
		}
	}
}

func TestSignUpInviteTerminalStatesUseInviteHashInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close inspection connection: %v", err)
		}
	})

	states := []string{"expired", "revoked", "consumed"}
	keyIDs := []int64{0x60, 0x61, 0x62}
	authKeyIDs := [][8]byte{{0x60}, {0x61}, {0x62}}
	for i, state := range states {
		t.Run(state, func(t *testing.T) {
			handle := "invite_" + state
			invite, secret, err := s.IssueInvite(ctx, handle)
			if err != nil {
				t.Fatal(err)
			}
			hash, _, err := s.IssueCodeForUsername(ctx, handle)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SetCodeForUsername(ctx, handle, hash, secret); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "expired":
				if _, err := conn.Exec(ctx, `UPDATE registration_invites SET state = 'expired' WHERE id = $1`, invite.ID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := s.RevokeInvite(ctx, invite.ID); err != nil {
					t.Fatal(err)
				}
			case "consumed":
				if _, err := conn.Exec(ctx, `UPDATE registration_invites SET state = 'consumed', consumed_at = now() WHERE id = $1`, invite.ID); err != nil {
					t.Fatal(err)
				}
			}
			keyID := keyIDs[i]
			if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
				t.Fatal(err)
			}
			_, err = api.SignUpForTest(s, authKeyIDs[i], netip.MustParseAddr("10.0.0.60"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
				PhoneNumber:   handle,
				PhoneCodeHash: hash,
				FirstName:     "Invite",
			})
			if got := signUpRPCMessage(err); got != "INVITE_HASH_INVALID" {
				t.Fatalf("%s invite: expected INVITE_HASH_INVALID, got %v", state, err)
			}
		})
	}
}

func TestSignUpConsumedCodeHashCannotBeReplayed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	addr := netip.MustParseAddr("10.0.0.30")
	hash, _, err := s.IssueCodeForUsername(ctx, "replayuser")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, 1, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	before := countUsers(t, dsn)
	if _, err := api.SignUpForTest(s, [8]byte{1}, addr, store.RateLimitConfig{}, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "replayuser",
		PhoneCodeHash: hash,
		FirstName:     "First",
	}); err != nil {
		t.Fatalf("first signUp: %v", err)
	}

	if err := s.SaveAuthKey(ctx, 2, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignUpForTest(s, [8]byte{2}, addr, store.RateLimitConfig{}, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "replayuser",
		PhoneCodeHash: hash,
		FirstName:     "Second",
	}); signUpRPCMessage(err) != "PHONE_CODE_INVALID" {
		t.Fatalf("replayed signUp: expected PHONE_CODE_INVALID, got %v", err)
	}
	if after := countUsers(t, dsn); after != before+1 {
		t.Fatalf("replayed signUp changed users from %d to %d, want %d", before, after, before+1)
	}
	key, ok, err := s.AuthKeyByID(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || key.UserID != 0 || key.PendingUserID != 0 {
		t.Fatalf("replayed signUp changed losing key: %#v found=%v", key, ok)
	}
}

func TestSignUpBoundKeyCannotBeRebound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	const keyID = int64(0x43)
	const handle = "boundretry"
	hash, _, err := s.IssueCodeForUsername(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	request := &tg.AuthSignUpRequest{
		PhoneNumber:   handle,
		PhoneCodeHash: hash,
		FirstName:     "First",
	}
	if _, err := api.SignUpForTest(s, [8]byte{0x43}, netip.MustParseAddr("10.0.0.43"), store.RateLimitConfig{}, config.RegistrationOpen, request); err != nil {
		t.Fatalf("first signUp: %v", err)
	}
	users := countUsers(t, dsn)
	if _, err := api.SignUpForTest(s, [8]byte{0x43}, netip.MustParseAddr("10.0.0.43"), store.RateLimitConfig{}, config.RegistrationOpen, request); signUpRPCMessage(err) != "SESSION_STATE_INVALID" {
		t.Fatalf("bound-key retry: expected SESSION_STATE_INVALID, got %v", err)
	}
	if got := countUsers(t, dsn); got != users {
		t.Fatalf("bound-key retry changed users from %d to %d", users, got)
	}
}

func TestConcurrentSignUpWithOneKeyHasOneWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	const keyID = int64(0x44)
	authKeyID := [8]byte{0x44}
	handles := []string{"samekeyone", "samekeytwo"}
	hashes := make([]string, len(handles))
	for i, handle := range handles {
		hash, _, err := s.IssueCodeForUsername(ctx, handle)
		if err != nil {
			t.Fatal(err)
		}
		hashes[i] = hash
	}
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	requests := make([]*tg.AuthSignUpRequest, len(handles))
	for i, handle := range handles {
		requests[i] = &tg.AuthSignUpRequest{
			PhoneNumber:   handle,
			PhoneCodeHash: hashes[i],
			FirstName:     "Racer",
		}
	}
	results := make([]error, len(requests))
	ready := make(chan struct{})
	var readyWG sync.WaitGroup
	var wg sync.WaitGroup
	readyWG.Add(len(requests))
	wg.Add(len(requests))
	for i := range requests {
		go func(i int) {
			defer wg.Done()
			readyWG.Done()
			<-ready
			_, results[i] = api.SignUpForTest(s, authKeyID, netip.MustParseAddr("10.0.0.44"), store.RateLimitConfig{}, config.RegistrationOpen, requests[i])
		}(i)
	}
	readyWG.Wait()
	close(ready)
	wg.Wait()

	var successes, stateInvalid int
	for i, err := range results {
		switch {
		case err == nil:
			successes++
		case signUpRPCMessage(err) == "SESSION_STATE_INVALID":
			stateInvalid++
		default:
			t.Errorf("attempt %d: unexpected error: %v", i, err)
		}
	}
	if successes != 1 || stateInvalid != 1 {
		t.Fatalf("signUp results: successes=%d state-invalid=%d, want 1/1", successes, stateInvalid)
	}
	if got := countUsers(t, dsn); got != 1 {
		t.Fatalf("users after same-key race = %d, want 1", got)
	}
	claimed := 0
	for _, handle := range handles {
		_, found, err := s.UserByUsernameWithLoginMode(ctx, handle)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("same-key race claimed %d handles, want 1", claimed)
	}
}

func TestSignUpTerminalCodeStatesUsePhoneCodeInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close inspection connection: %v", err)
		}
	})

	states := []string{"expired", "exhausted", "clear"}
	keyIDs := []int64{0x50, 0x51, 0x52}
	authKeyIDs := [][8]byte{{0x50}, {0x51}, {0x52}}
	for i, state := range states {
		t.Run(state, func(t *testing.T) {
			handle := "code_" + state
			hash, _, err := s.IssueCodeForUsername(ctx, handle)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "expired":
				if _, err := conn.Exec(ctx, `UPDATE phone_codes SET expires_at = now() - interval '1 second' WHERE code_hash = $1`, hash); err != nil {
					t.Fatal(err)
				}
			case "exhausted":
				if _, err := conn.Exec(ctx, `UPDATE phone_codes SET attempts = 3 WHERE code_hash = $1`, hash); err != nil {
					t.Fatal(err)
				}
			case "clear":
				if _, err := conn.Exec(ctx, `UPDATE phone_codes SET code = '' WHERE code_hash = $1`, hash); err != nil {
					t.Fatal(err)
				}
			}
			keyID := keyIDs[i]
			if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
				t.Fatal(err)
			}
			_, err = api.SignUpForTest(s, authKeyIDs[i], netip.MustParseAddr("10.0.0.50"), store.RateLimitConfig{}, config.RegistrationOpen, &tg.AuthSignUpRequest{
				PhoneNumber:   handle,
				PhoneCodeHash: hash,
				FirstName:     "Invalid",
			})
			if got := signUpRPCMessage(err); got != "PHONE_CODE_INVALID" {
				t.Fatalf("%s code: expected PHONE_CODE_INVALID, got %v", state, err)
			}
		})
	}
}

func TestSignUpClearsUnboundPendingStateAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	const keyID = int64(0x31)
	const handle = "pending_signup"
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	pending, err := s.CreateUser(ctx, "+15551249999")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, pending.ID); err != nil {
		t.Fatal(err)
	}
	invite, secret, err := s.IssueInvite(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetCodeForUsername(ctx, handle, hash, secret); err != nil {
		t.Fatal(err)
	}

	res, err := api.SignUpForTest(s, [8]byte{0x31}, netip.MustParseAddr("10.0.0.31"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   handle,
		PhoneCodeHash: hash,
		FirstName:     "Pending",
	})
	if err != nil {
		t.Fatalf("signUp with unbound pending key: %v", err)
	}
	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("signUp result = %T, want *tg.AuthAuthorization", res)
	}
	user, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("authorization user = %T, want *tg.User", auth.User)
	}
	key, ok, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || key.UserID != user.ID || key.PendingUserID != 0 {
		t.Fatalf("key after signUp = %#v found=%v, want user %d with no pending state", key, ok, user.ID)
	}
	invitations, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(invitations) != 1 || invitations[0].ID != invite.ID || invitations[0].State != store.InviteConsumed {
		t.Fatalf("invites = %#v, want invite %d consumed", invitations, invite.ID)
	}
}

func TestSignUpActiveModeChargesInvalidHandleBeforeValidation(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	addr := netip.MustParseAddr("10.0.0.32")
	limits := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	if err := s.SaveAuthKey(context.Background(), 1, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SignUpForTest(s, [8]byte{1}, addr, limits, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber: "1invalid",
		FirstName:   "First",
	}); signUpRPCMessage(err) != "USERNAME_INVALID" {
		t.Fatalf("invalid handle: expected USERNAME_INVALID, got %v", err)
	}
	if _, err := api.SignUpForTest(s, [8]byte{2}, addr, limits, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "validhandle",
		PhoneCodeHash: "missing",
		FirstName:     "First",
	}); !isFloodWait(err) {
		t.Fatalf("second signUp: expected FLOOD_WAIT, got %v", err)
	}
}

func TestSignUpKeyStatePrecedesInputOutcomes(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	for name, req := range map[string]*tg.AuthSignUpRequest{
		"invalid handle":     {PhoneNumber: "1invalid", FirstName: "First"},
		"invalid first name": {PhoneNumber: "validhandle", FirstName: strings.Repeat("a", 65)},
		"invalid code":       {PhoneNumber: "validhandle", PhoneCodeHash: "missing", FirstName: "First"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := api.SignUpForTest(s, [8]byte{0x41}, netip.MustParseAddr("10.0.0.41"), store.RateLimitConfig{}, config.RegistrationOpen, req)
			if got := signUpRPCMessage(err); got != "SESSION_STATE_INVALID" {
				t.Fatalf("signUp: expected SESSION_STATE_INVALID before input outcome, got %v", err)
			}
		})
	}
}

func TestSignUpInviteModeReportsOccupiedHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	const handle = "occupied_signup"
	occupied, err := s.CreateUsernameUser(ctx, handle, "Existing", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimUsername(ctx, occupied.ID, handle); err != nil {
		t.Fatal(err)
	}
	hash, _, err := s.IssueCodeForUsername(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveAuthKey(ctx, 0x42, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	_, err = api.SignUpForTest(s, [8]byte{0x42}, netip.MustParseAddr("10.0.0.42"), store.RateLimitConfig{}, config.RegistrationInvite, &tg.AuthSignUpRequest{
		PhoneNumber:   handle,
		PhoneCodeHash: hash,
		FirstName:     "New",
	})
	if got := signUpRPCMessage(err); got != "USERNAME_OCCUPIED" {
		t.Fatalf("occupied invite signUp: expected USERNAME_OCCUPIED, got %v", err)
	}
}
