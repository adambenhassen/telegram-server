package api

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// saltLen is the length of each server-generated KDF salt half.
const saltLen = 32

// passwordAlgo builds the SRP KDF algo descriptor advertised to clients: the
// canonical Telegram group plus the given salts.
func passwordAlgo(salt1, salt2 []byte) tg.PasswordKdfAlgoClass {
	return &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
		Salt1: salt1,
		Salt2: salt2,
		G:     srp.G,
		P:     srp.PBytes(),
	}
}

func randBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	return b, nil
}

// userTL maps a stored user to the tg.User returned in an authorization.
// Self always carries UserStatusRecently per Telegram's privacy model.
func userTL(u store.User) *tg.User {
	return &tg.User{
		ID:         u.ID,
		Self:       true,
		Phone:      u.Phone,
		FirstName:  u.FirstName,
		AccessHash: u.ID, // self access hash placeholder
		Status:     &tg.UserStatusRecently{},
	}
}

// handleGetPassword serves account.getPassword. It advertises the KDF params and
// an SRP challenge for the target user: the bound user when the key is already
// authorized (setting/changing a password), otherwise the half-authorized
// pending user mid-login. It always includes new_algo so a client can set a new
// password, and adds current_algo + srp_B + srp_id only when a password exists.
func (h *handlers) handleGetPassword(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountGetPasswordRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}

	target, rpc := h.resolvePasswordUser(r)
	if rpc != nil {
		return nil, rpc
	}
	pw, hasPw, err := h.store.PasswordByUser(r.Ctx, target)
	if err != nil {
		h.log.Error("get password: lookup", "user_id", target, "err", err)
		return nil, errInternal
	}

	newSalt1, err := randBytes(saltLen)
	if err != nil {
		return nil, errInternal
	}
	newSalt2, err := randBytes(saltLen)
	if err != nil {
		return nil, errInternal
	}
	secureRandom, err := randBytes(256)
	if err != nil {
		return nil, errInternal
	}

	resp := &tg.AccountPassword{
		NewAlgo:       passwordAlgo(newSalt1, newSalt2),
		NewSecureAlgo: &tg.SecurePasswordKdfAlgoUnknown{},
		SecureRandom:  secureRandom,
	}
	resp.HasPassword = hasPw
	if hasPw {
		srpID, srpB, ierr := h.srp.Issue(mtproto.AuthKeyIDInt64(r.AuthKeyID), target, pw.Verifier)
		if ierr != nil {
			h.log.Error("get password: issue challenge", "user_id", target, "err", ierr)
			return nil, errInternal
		}
		resp.SetCurrentAlgo(passwordAlgo(pw.Salt1, pw.Salt2))
		resp.SetSRPB(srpB)
		resp.SetSRPID(srpID)
		resp.SetHint(pw.Hint)
		resp.HasRecovery = pw.HasRecovery
	}
	return resp, nil
}

// resolvePasswordUser picks the user a password operation targets: the bound
// user if the key is authorized, else the pending user staged by signIn. It
// fails with AUTH_KEY_UNREGISTERED when the key is neither authorized nor
// mid-login, so an anonymous key cannot probe password state.
func (h *handlers) resolvePasswordUser(r *mtproto.Request) (int64, *tgerr.Error) {
	if r.UserID != 0 {
		return r.UserID, nil
	}
	key, ok, err := h.store.AuthKeyByID(r.Ctx, mtproto.AuthKeyIDInt64(r.AuthKeyID))
	if err != nil {
		h.log.Error("resolve password user: lookup key", "err", err)
		return 0, errInternal
	}
	if !ok || key.PendingUserID == 0 {
		return 0, errAuthKeyUnreg
	}
	return key.PendingUserID, nil
}

// handleCheckPassword serves auth.checkPassword: the SRP password step of a 2FA
// login. On a valid proof it promotes the key's pending user to a full binding
// (the authorization signIn deferred) and returns auth.Authorization.
func (h *handlers) handleCheckPassword(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AuthCheckPasswordRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	proof, ok := req.Password.(*tg.InputCheckPasswordSRP)
	if !ok {
		return nil, errPasswordHashInvalid
	}
	userID, rpc := h.consumeAndVerify(r.Ctx, mtproto.AuthKeyIDInt64(r.AuthKeyID), proof)
	if rpc != nil {
		return nil, rpc
	}

	keyID := mtproto.AuthKeyIDInt64(r.AuthKeyID)
	if err := h.store.PromotePendingUser(r.Ctx, keyID, userID); err != nil {
		if !errors.Is(err, store.ErrAuthKeyNotFound) {
			h.log.Error("check password: promote", "user_id", userID, "err", err)
			return nil, errInternal
		}
		// No pending to promote: accept only if the key is already bound to this
		// exact user (idempotent re-check); otherwise fail closed.
		key, found, gerr := h.store.AuthKeyByID(r.Ctx, keyID)
		if gerr != nil {
			h.log.Error("check password: verify binding", "err", gerr)
			return nil, errInternal
		}
		if !found || key.UserID != userID {
			return nil, errPasswordHashInvalid
		}
	}

	user, found, err := h.store.UserByID(r.Ctx, userID)
	if err != nil || !found {
		h.log.Error("check password: load user", "user_id", userID, "ok", found, "err", err)
		return nil, errInternal
	}
	return &tg.AuthAuthorization{User: userTL(user)}, nil
}

// consumeAndVerify consumes the SRP challenge named by proof.SRPID and checks
// the client's (A, M1) against the stored verifier for the user the challenge
// was issued to. It returns that user id on success. Every failure path is
// fail-closed: a missing/expired challenge is SRP_ID_INVALID, a bad proof or
// absent password is PASSWORD_HASH_INVALID.
func (h *handlers) consumeAndVerify(ctx context.Context, authKeyID int64, proof *tg.InputCheckPasswordSRP) (int64, *tgerr.Error) {
	pending, ok := h.srp.Consume(proof.SRPID, authKeyID)
	if !ok {
		return 0, errSRPIDInvalid
	}
	pw, ok, err := h.store.PasswordByUser(ctx, pending.UserID)
	if err != nil {
		h.log.Error("srp verify: load password", "user_id", pending.UserID, "err", err)
		return 0, errInternal
	}
	if !ok {
		return 0, errPasswordHashInvalid
	}
	if !srp.Verify(pw.Verifier, pw.Salt1, pw.Salt2, proof.A, proof.M1, pending.BPublic, pending.BSecret) {
		return 0, errPasswordHashInvalid
	}
	return pending.UserID, nil
}

// handleUpdatePasswordSettings serves account.updatePasswordSettings: set,
// change, or remove the caller's 2FA password. A change or remove is gated by a
// valid SRP proof of the current password; an initial set has no current proof.
// An empty new_password_hash signals removal.
func (h *handlers) handleUpdatePasswordSettings(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req tg.AccountUpdatePasswordSettingsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}

	_, hasCur, err := h.store.PasswordByUser(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("update password: lookup current", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	ns := req.NewSettings
	// Check for removal early: for username-mode accounts, removing the verifier
	// is irreversible lockout, so we reject before requiring any SRP proof.
	if _, isRemoval := ns.NewAlgo.(*tg.PasswordKdfAlgoUnknown); isRemoval && hasCur {
		loginMode, err := h.store.UserLoginMode(r.Ctx, r.UserID)
		if err != nil {
			h.log.Error("update password: lookup login mode", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if loginMode == "username" {
			return nil, errPasswordCannotBeRemoved
		}
	}

	// Changing or removing an existing password requires proving the current one.
	if hasCur {
		proof, ok := req.Password.(*tg.InputCheckPasswordSRP)
		if !ok {
			return nil, errPasswordHashInvalid
		}
		uid, rpc := h.consumeAndVerify(r.Ctx, mtproto.AuthKeyIDInt64(r.AuthKeyID), proof)
		if rpc != nil {
			return nil, rpc
		}
		if uid != r.UserID {
			return nil, errPasswordHashInvalid
		}
	}

	switch algo := ns.NewAlgo.(type) {
	case *tg.PasswordKdfAlgoUnknown:
		// Explicit removal: the client signals it with the unknown algo. An empty
		// new_algo/new_password_hash alone (e.g. an email-only update) must NOT
		// delete the password. The username-mode check above already rejected
		// removal for login_mode='username' accounts before requiring proof.
		if _, derr := h.store.DeletePassword(r.Ctx, r.UserID); derr != nil {
			h.log.Error("update password: delete", "user_id", r.UserID, "err", derr)
			return nil, errInternal
		}
		return &tg.BoolTrue{}, nil

	case *tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow:
		// Set or change: the client-computed verifier and its salts must be present.
		if len(algo.Salt1) == 0 || len(algo.Salt2) == 0 {
			return nil, errNewSaltInvalid
		}
		// The verifier is v padded to 256 bytes and must be a valid group element;
		// a degenerate v (e.g. 0) would make the SRP proof forgeable.
		if len(ns.NewPasswordHash) != srp.PadLen || !srp.ValidVerifier(ns.NewPasswordHash) {
			return nil, errNewPasswordBad
		}
		if err := h.store.UpsertPassword(r.Ctx, store.UserPassword{
			UserID:        r.UserID,
			Salt1:         algo.Salt1,
			Salt2:         algo.Salt2,
			Verifier:      ns.NewPasswordHash,
			Hint:          ns.Hint,
			RecoveryEmail: ns.Email,
			HasRecovery:   ns.Email != "",
		}); err != nil {
			h.log.Error("update password: upsert", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		return &tg.BoolTrue{}, nil

	default:
		// No password change requested (no/unsupported new_algo). Never destructive:
		// leave the existing password untouched. Email/hint-only passthrough updates
		// are out of scope (recovery is Non-Goal), so this is an accepted no-op.
		return &tg.BoolTrue{}, nil
	}
}

// handleGetPasswordSettings serves account.getPasswordSettings: the stored
// recovery settings, gated behind a valid SRP proof of the current password.
func (h *handlers) handleGetPasswordSettings(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req tg.AccountGetPasswordSettingsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	proof, ok := req.Password.(*tg.InputCheckPasswordSRP)
	if !ok {
		return nil, errPasswordHashInvalid
	}
	uid, rpc := h.consumeAndVerify(r.Ctx, mtproto.AuthKeyIDInt64(r.AuthKeyID), proof)
	if rpc != nil {
		return nil, rpc
	}
	if uid != r.UserID {
		return nil, errPasswordHashInvalid
	}
	pw, hasPw, err := h.store.PasswordByUser(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get password settings: lookup", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	settings := &tg.AccountPasswordSettings{}
	if hasPw && pw.HasRecovery {
		settings.SetEmail(pw.RecoveryEmail)
	}
	return settings, nil
}
