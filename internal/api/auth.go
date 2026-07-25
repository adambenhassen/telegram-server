package api

import (
	"errors"
	"regexp"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

var phoneRE = regexp.MustCompile(`^\+?[0-9]{5,15}$`)

func validatePhone(phone string) error {
	if !phoneRE.MatchString(phone) {
		return errPhoneInvalid
	}
	return nil
}

func newSentCode(hash string) *tg.AuthSentCode {
	return &tg.AuthSentCode{
		Type:          &tg.AuthSentCodeTypeSMS{Length: 5},
		PhoneCodeHash: hash,
	}
}

func verifyToRPC(err error) *tgerr.Error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrCodeInvalid):
		return errCodeInvalid
	case errors.Is(err, store.ErrCodeExpired):
		return errCodeExpired
	// An exhausted code maps to the generic invalid error so the attempt cap is
	// not observable to the client.
	case errors.Is(err, store.ErrCodeExhausted):
		return errCodeInvalid
	default:
		return errInternal
	}
}

func (h *handlers) handleSendCode(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AuthSendCodeRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if err := validatePhone(req.PhoneNumber); err != nil {
		return nil, err
	}
	hash, code, err := h.store.IssueCode(r.Ctx, req.PhoneNumber)
	if err != nil {
		if errors.Is(err, store.ErrResendTooSoon) {
			return nil, errFloodWait
		}
		h.log.Error("issue code", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	h.logIssuedCode(req.PhoneNumber, code)
	return newSentCode(hash), nil
}

// logIssuedCode writes the code to the log only when the operator opted in
// via TG_LOG_LOGIN_CODES; the log is the only delivery channel this server has.
func (h *handlers) logIssuedCode(phone, code string) {
	if !h.logLoginCodes {
		return
	}
	h.log.Info("login code issued", "phone", phone, "code", code)
}

func (h *handlers) handleSignIn(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AuthSignInRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	code, _ := req.GetPhoneCode()
	if err := h.store.VerifyCode(r.Ctx, req.PhoneNumber, req.PhoneCodeHash, code); err != nil {
		if rpc := verifyToRPC(err); rpc != errInternal {
			return nil, rpc
		}
		h.log.Error("verify code", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	user, err := h.store.CreateUser(r.Ctx, req.PhoneNumber)
	if err != nil {
		h.log.Error("create user", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	keyID := mtproto.AuthKeyIDInt64(r.AuthKeyID)

	// If the account has a 2FA cloud password, the phone code alone does not
	// authorize: stage the key as half-authorized (pending, never user_id) and
	// require the SRP password step via auth.checkPassword.
	_, hasPassword, err := h.store.PasswordByUser(r.Ctx, user.ID)
	if err != nil {
		h.log.Error("sign in: password lookup", "user_id", user.ID, "err", err)
		return nil, errInternal
	}
	if hasPassword {
		if err := h.store.SetPendingUser(r.Ctx, keyID, user.ID); err != nil {
			h.log.Error("sign in: set pending", "user_id", user.ID, "err", err)
			return nil, errInternal
		}
		return nil, errSessionPasswordNeeded
	}

	// No password: bind the auth key to the user so it stays authorized across
	// reconnects and server restarts.
	if err := h.store.BindAuthKeyUser(r.Ctx, keyID, user.ID); err != nil {
		h.log.Error("bind auth key", "user_id", user.ID, "err", err)
		return nil, errInternal
	}
	return &tg.AuthAuthorization{User: userTL(user)}, nil
}

// handleLogOut serves auth.logOut. It deletes the auth key the request arrived
// on, so the client must re-handshake and is no longer authorized. Telegram
// allows logOut regardless of authorization state; deleting an unbound key is a
// no-op, so no UserID check is needed. Returns auth.loggedOut on success.
func (h *handlers) handleLogOut(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AuthLogOutRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if err := h.store.DeleteAuthKey(r.Ctx, mtproto.AuthKeyIDInt64(r.AuthKeyID)); err != nil {
		h.log.Error("logout: delete auth key", "err", err)
		return nil, errInternal
	}
	return &tg.AuthLoggedOut{}, nil
}
