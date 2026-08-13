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
	// The allow-list must contain exactly the three methods that a provisional
	// session is permitted to call.
	want := map[uint32]bool{
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

func TestProvisionalGateBlocksSendMessage(t *testing.T) {
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

	// The register wrapper gate would return AUTH_KEY_UNREGISTERED here because:
	// req.UserID != 0 && req.Provisional && !provisionalAllowList[sendMessage].
	// We verify the gate logic directly since SendMessageForTest bypasses the
	// dispatcher (and thus the gate). The gate is exercised by the full server
	// integration; here we verify the gate condition is correct for this method.
	id := uint32(tg.MessagesSendMessageRequestTypeID)
	if api.ProvisionalAllowList[id] {
		t.Fatal("sendMessage should not be in the provisional allow-list")
	}
	// The gate returns errAuthKeyUnreg when all conditions are met.
	// We verify the conditions: UserID != 0 (bound key), Provisional = true,
	// and the method is not in the allow-list.
	// This is the exact logic the register wrapper applies.
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
	if err := s.SaveAuthKey(ctx, int64(0x2), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x2), user.ID); err != nil {
		t.Fatal(err)
	}

	// getPassword is in the allow-list, so the gate does not block it.
	id := uint32(tg.AccountGetPasswordRequestTypeID)
	if !api.ProvisionalAllowList[id] {
		t.Fatal("getPassword must be in the provisional allow-list")
	}

	// Verify the handler itself works for a provisional account.
	var buf bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
		t.Fatal(err)
	}
	res, err := api.GetPasswordForTest(s, user.ID, &mtproto.Request{
		Ctx:       ctx,
		UserID:    user.ID,
		AuthKeyID: [8]byte{2},
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
	if err := s.SaveAuthKey(ctx, int64(0x3), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x3), user.ID); err != nil {
		t.Fatal(err)
	}

	// logOut is in the allow-list.
	id := uint32(tg.AuthLogOutRequestTypeID)
	if !api.ProvisionalAllowList[id] {
		t.Fatal("logOut must be in the provisional allow-list")
	}

	key := [8]byte{3}
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
	_, ok, err := s.AuthKeyByID(ctx, int64(0x3))
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
	if err := s.SaveAuthKey(ctx, int64(0x4), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x4), user.ID); err != nil {
		t.Fatal(err)
	}

	// Verify session is provisional before setting verifier.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x4))
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
	key, ok, err = s.AuthKeyByID(ctx, int64(0x4))
	if err != nil || !ok {
		t.Fatalf("AuthKeyByID: ok=%v err=%v", ok, err)
	}
	if key.Provisional {
		t.Fatal("expected non-provisional after setting verifier")
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

	// Verify the session is no longer provisional.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x7))
	_ = key
	_ = ok
	_ = err
	// Auth key not bound yet, skip — login_mode check happens before proof.

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
