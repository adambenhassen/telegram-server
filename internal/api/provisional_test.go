package api_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestProvisionalAllowListContainsExpectedMethods(t *testing.T) {
	t.Parallel()
	// The allow-list must contain exactly the four methods that a provisional
	// session is permitted to call. help.getConfig is needed because gotd's
	// connection handshake calls it unconditionally.
	want := map[uint32]bool{
		tg.HelpGetConfigRequestTypeID:                 true,
		tg.AccountGetPasswordRequestTypeID:            true,
		tg.AccountUpdatePasswordSettingsRequestTypeID: true,
		tg.AuthLogOutRequestTypeID:                    true,
	}
	got := api.ProvisionalAllowList
	if len(got) != len(want) {
		t.Fatalf("allow-list has %d entries, want %d", len(got), len(want))
	}
	for id, wantAllowed := range want {
		if got[id] != wantAllowed {
			t.Errorf("method %#x: allowed=%v, want %v", id, got[id], wantAllowed)
		}
	}
}

func TestProvisionalAllowListBlocksOtherMethods(t *testing.T) {
	t.Parallel()
	// Methods not in the allow-list must be blocked for provisional sessions.
	blocked := []uint32{
		tg.MessagesSendMessageRequestTypeID,
		tg.MessagesGetDialogsRequestTypeID,
		tg.UpdatesGetStateRequestTypeID,
		tg.UsersGetUsersRequestTypeID,
		tg.AccountGetAuthorizationsRequestTypeID,
	}
	for _, id := range blocked {
		if api.ProvisionalAllowList[id] {
			t.Errorf("method %#x is in allow-list but should be blocked", id)
		}
	}
}

func TestProvisionalGateBlocksNonAllowListedMethod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "gateuser", "Gate", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "gateuser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x1), user.ID); err != nil {
		t.Fatal(err)
	}

	// Verify the session is provisional.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x1))
	if err != nil || !ok {
		t.Fatalf("AuthKeyByID: ok=%v err=%v", ok, err)
	}
	if !key.Provisional {
		t.Fatal("expected provisional session")
	}

	// The gate predicate is the same function called by registerRevoke and
	// handleUnknownGated — not a copy. If the gate is removed from either
	// site, this test still asserts the predicate that both sites call.
	req := &mtproto.Request{
		Ctx:         ctx,
		UserID:      user.ID,
		Provisional: true,
		AuthKeyID:   [8]byte{1},
	}
	if !api.ProvisionalBlocked(uint32(tg.MessagesSendMessageRequestTypeID), req) {
		t.Fatal("gate did not block sendMessage for provisional session")
	}
}

func TestProvisionalGateAllowsAllowListedMethods(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "allowuser", "Allow", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "allowuser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x2), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x2), user.ID); err != nil {
		t.Fatal(err)
	}

	req := &mtproto.Request{
		Ctx:         ctx,
		UserID:      user.ID,
		Provisional: true,
		AuthKeyID:   [8]byte{2},
	}

	// All four allow-listed methods must not be blocked by the gate predicate.
	allowed := []uint32{
		uint32(tg.HelpGetConfigRequestTypeID),
		uint32(tg.AccountGetPasswordRequestTypeID),
		uint32(tg.AccountUpdatePasswordSettingsRequestTypeID),
		uint32(tg.AuthLogOutRequestTypeID),
	}
	for _, id := range allowed {
		if api.ProvisionalBlocked(id, req) {
			t.Errorf("gate blocked allow-listed method %#x", id)
		}
	}
}

func TestProvisionalGateDoesNotApplyToUnauthenticated(t *testing.T) {
	t.Parallel()
	// Unauthenticated requests (UserID == 0) must never be blocked by the gate.
	req := &mtproto.Request{
		Ctx:         context.Background(),
		UserID:      0,
		Provisional: true,
	}
	if api.ProvisionalBlocked(uint32(tg.MessagesSendMessageRequestTypeID), req) {
		t.Fatal("gate blocked unauthenticated request")
	}
}

func TestProvisionalGateAllowsGetPassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "gpuser", "GetPw", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "gpuser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x3), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x3), user.ID); err != nil {
		t.Fatal(err)
	}

	// getPassword is in the allow-list, so the gate does not block it.
	// Verify the handler itself works for a provisional account.
	var buf bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
		t.Fatal(err)
	}
	res, err := api.GetPasswordForTest(s, user.ID, &mtproto.Request{
		Ctx:       ctx,
		UserID:    user.ID,
		AuthKeyID: [8]byte{3},
		Buf:       &buf,
	})
	if err != nil {
		t.Fatalf("getPassword: expected success, got %v", err)
	}
	pw, ok := res.(*tg.AccountPassword)
	if !ok {
		t.Fatalf("result = %T, want *tg.AccountPassword", res)
	}
	if pw.HasPassword {
		t.Error("expected has_password=false for provisional account")
	}
}

func TestProvisionalGateAllowsLogOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "logoutuser", "LogOut", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "logoutuser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x4), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x4), user.ID); err != nil {
		t.Fatal(err)
	}

	key := [8]byte{4}
	// logOut from a provisional session must succeed.
	res, afterReply, err := api.LogOutForTest(s, key)
	if err != nil {
		t.Fatalf("logOut: expected success, got %v", err)
	}
	if _, ok := res.(*tg.AuthLoggedOut); !ok {
		t.Fatalf("result = %T, want *tg.AuthLoggedOut", res)
	}
	if afterReply != nil {
		afterReply()
	}

	// Key should be deleted.
	_, ok, err := s.AuthKeyByID(ctx, int64(0x4))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("auth key should be deleted after logOut")
	}
}

func TestProvisionalGateLiftsAfterSettingVerifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "setverifier", "Set", "Verifier")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "setverifier"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x5), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x5), user.ID); err != nil {
		t.Fatal(err)
	}

	// Verify session is provisional before setting verifier.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x5))
	if err != nil || !ok {
		t.Fatalf("AuthKeyByID: ok=%v err=%v", ok, err)
	}
	if !key.Provisional {
		t.Fatal("expected provisional before setting verifier")
	}

	// Set a verifier via updatePasswordSettings.
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifier := make([]byte, 256)
	verifier[0] = 1 // valid group element (non-zero)

	var buf bin.Buffer
	req := &tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordEmpty{},
		NewSettings: tg.AccountPasswordInputSettings{
			NewAlgo: &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
				Salt1: salt1,
				Salt2: salt2,
			},
			NewPasswordHash: verifier,
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdatePasswordSettingsForTest(s, user.ID, &buf)
	if err != nil {
		t.Fatalf("updatePasswordSettings: expected success, got %v", err)
	}

	// Verify the session is no longer provisional: the JOIN in AuthKeyByID
	// now finds a passwords row, so Provisional derives to false.
	key, ok, err = s.AuthKeyByID(ctx, int64(0x5))
	if err != nil || !ok {
		t.Fatalf("AuthKeyByID: ok=%v err=%v", ok, err)
	}
	if key.Provisional {
		t.Fatal("expected non-provisional after setting verifier")
	}

	// Confirm a non-allow-listed RPC would succeed (gate no longer fires
	// because Provisional is false).
	reqAfter := &mtproto.Request{
		Ctx:         ctx,
		UserID:      user.ID,
		Provisional: false,
		AuthKeyID:   [8]byte{5},
	}
	if api.ProvisionalBlocked(uint32(tg.MessagesSendMessageRequestTypeID), reqAfter) {
		t.Fatal("sendMessage after setting verifier: gate still blocks")
	}
}

func TestUsernameModeCannotRemovePassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user WITH a verifier.
	user, err := s.CreateUsernameUser(ctx, "noremove", "No", "Remove")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "noremove"); err != nil {
		t.Fatal(err)
	}

	// Set a verifier.
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifier := make([]byte, 256)
	verifier[0] = 1

	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   user.ID,
		Salt1:    salt1,
		Salt2:    salt2,
		Verifier: verifier,
	}); err != nil {
		t.Fatal(err)
	}

	// Try to remove the password with PasswordKdfAlgoUnknown.
	var buf bin.Buffer
	req := &tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordEmpty{},
		NewSettings: tg.AccountPasswordInputSettings{
			NewAlgo: &tg.PasswordKdfAlgoUnknown{},
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdatePasswordSettingsForTest(s, user.ID, &buf)
	if err == nil {
		t.Fatal("expected error when removing password for username-mode account")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("expected RPC error, got %v", err)
	}
	if rpc.Code != 400 {
		t.Errorf("expected 400 error, got %d", rpc.Code)
	}

	// Verify the password was NOT deleted.
	_, hasPw, err := s.PasswordByUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPw {
		t.Error("password should still exist after rejected removal")
	}
}

func TestUsernameModeLoginModeCheckBeforeSRPProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user WITH a verifier (non-provisional).
	user, err := s.CreateUsernameUser(ctx, "removetest", "Remove", "Test")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "removetest"); err != nil {
		t.Fatal(err)
	}

	// Set a verifier so the account is non-provisional.
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifier := make([]byte, 256)
	verifier[0] = 1

	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   user.ID,
		Salt1:    salt1,
		Salt2:    salt2,
		Verifier: verifier,
	}); err != nil {
		t.Fatal(err)
	}

	// Attempting to remove password: the login_mode check rejects before SRP
	// proof because the handler checks NewAlgo for removal before requiring proof.
	var buf bin.Buffer
	req := &tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordEmpty{}, // bypass SRP
		NewSettings: tg.AccountPasswordInputSettings{
			NewAlgo: &tg.PasswordKdfAlgoUnknown{},
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdatePasswordSettingsForTest(s, user.ID, &buf)
	if err == nil {
		t.Fatal("expected error when username-mode user attempts password removal")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("expected RPC error, got %v", err)
	}
	if rpc.Code != 400 {
		t.Errorf("expected 400 error, got %d", rpc.Code)
	}

	// Verify the password was NOT deleted.
	_, hasPw, err := s.PasswordByUser(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPw {
		t.Error("password should still exist after rejected removal")
	}
}

func TestProvisionalGateBlocksResetAuthorization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "resetuser", "Reset", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "resetuser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x8), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x8), user.ID); err != nil {
		t.Fatal(err)
	}

	// resetAuthorization is registered via registerRevoke, not register.
	// The gate must still apply because registerRevoke calls provisionalBlocked.
	req := &mtproto.Request{
		Ctx:         ctx,
		UserID:      user.ID,
		Provisional: true,
		AuthKeyID:   [8]byte{8},
	}
	if !api.ProvisionalBlocked(uint32(tg.AccountResetAuthorizationRequestTypeID), req) {
		t.Fatal("gate did not block resetAuthorization for provisional session")
	}
}

func TestProvisionalAccountCannotRemovePassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user WITHOUT a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "provremove", "Prov", "Remove")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "provremove"); err != nil {
		t.Fatal(err)
	}

	// Verify the session is provisional.
	if err := s.SaveAuthKey(ctx, int64(0x9), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x9), user.ID); err != nil {
		t.Fatal(err)
	}
	key, ok, err := s.AuthKeyByID(ctx, int64(0x9))
	if err != nil || !ok {
		t.Fatalf("AuthKeyByID: ok=%v err=%v", ok, err)
	}
	if !key.Provisional {
		t.Fatal("expected provisional session")
	}

	// A provisional account sending PasswordKdfAlgoUnknown must be rejected
	// even though hasCur is false (no password to remove).
	var buf bin.Buffer
	req := &tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordEmpty{},
		NewSettings: tg.AccountPasswordInputSettings{
			NewAlgo: &tg.PasswordKdfAlgoUnknown{},
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdatePasswordSettingsForTest(s, user.ID, &buf)
	if err == nil {
		t.Fatal("expected error when provisional account attempts password removal")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("expected RPC error, got %v", err)
	}
	if rpc.Code != 400 {
		t.Errorf("expected 400 error, got %d", rpc.Code)
	}
}

func TestProvisionalGateBlocksUnimplementedMethod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a verifier (provisional).
	user, err := s.CreateUsernameUser(ctx, "unimpluser", "Unimpl", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "unimpluser"); err != nil {
		t.Fatal(err)
	}

	// Save and bind the auth key.
	if err := s.SaveAuthKey(ctx, int64(0xa), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0xa), user.ID); err != nil {
		t.Fatal(err)
	}

	// handleUnknownGated is the fallback for unimplemented methods.
	// It applies the gate for all provisional sessions (unimplemented methods
	// are never in the allow-list). The predicate is the same function called
	// by both registerRevoke and handleUnknownGated.
	req := &mtproto.Request{
		Ctx:         ctx,
		UserID:      user.ID,
		Provisional: true,
		AuthKeyID:   [8]byte{10},
	}
	if !api.ProvisionalBlocked(tg.HelpGetNearestDCRequestTypeID, req) {
		t.Fatal("gate did not block unimplemented method for provisional session")
	}
}
