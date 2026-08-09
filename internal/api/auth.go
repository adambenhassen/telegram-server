package api

import (
	"errors"
	"regexp"
	"time"

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
	// The per-IP limits run before the phone is touched at all: nothing is read
	// or written about the account, so a denial cannot vary with whether the
	// number is registered or already holds a live code.
	if err := h.checkSendCodeIP(r, req.PhoneNumber); err != nil {
		return nil, err
	}
	hash, code, err := h.store.IssueCode(r.Ctx, req.PhoneNumber)
	if err != nil {
		if errors.Is(err, store.ErrResendTooSoon) {
			return nil, errFloodWait
		}
		h.log.Error("issue code", "err", err)
		return nil, errInternal
	}
	h.logIssuedCode(req.PhoneNumber, code)
	return newSentCode(hash), nil
}

// checkSendCodeIP charges the per-IP sendCode counters for the network the
// request's connection came from. It returns nil when the call may proceed, and
// a flood wait when it may not.
//
// Every rejection is the same 420 with the wait the network's own counters
// imply, so the error tells a caller nothing except how long its own address is
// held back. Neither the address nor the phone number reaches the log here: the
// two together are exactly the join the short-lived limiter rows exist to avoid
// keeping, and a log line would outlive them by whatever the retention is.
func (h *handlers) checkSendCodeIP(r *mtproto.Request, phone string) error {
	if !h.rateLimitSendCodeIP.Enabled() {
		return nil
	}
	res, err := h.store.CheckAndChargeSendCodeIP(r.Ctx, r.ClientAddr, phone, h.rateLimitSendCodeIP)
	if err != nil {
		if errors.Is(err, store.ErrNoClientAddr) {
			// The connection carries no address to attribute the call to, so the
			// limit cannot hold for it, and it is refused rather than waved
			// through: an attacker who could provoke this would otherwise have
			// found the way around the limit.
			//
			// Not an error condition, though. In proxy-v2 mode a balancer states
			// that a connection has no client address — its own health check
			// does exactly that — so this is a request arriving on a connection
			// that was never meant to carry one, not a fault in the server.
			h.log.Info("send code: connection carries no client address")
			return FloodWaitError(int(h.sendCodeIPRetry() / time.Second))
		}
		h.log.Error("send code: ip rate limit", "err", err)
		return errInternal
	}
	if res != nil {
		return FloodWaitError(int(res.Wait / time.Second))
	}
	return nil
}

// sendCodeIPRetry is the wait handed out when the limits are enforced but the
// request could not be keyed at all. It is the shortest enabled window, so an
// operator who hits this in a healthy deployment sees clients retrying rather
// than locked out for a day.
func (h *handlers) sendCodeIPRetry() time.Duration {
	var shortest time.Duration
	for _, c := range []store.RateLimitConfig{h.rateLimitSendCodeIP.Calls, h.rateLimitSendCodeIP.Phones} {
		if c.Limit > 0 && c.Window > 0 && (shortest == 0 || c.Window < shortest) {
			shortest = c.Window
		}
	}
	return max(shortest, time.Second)
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
		h.log.Error("verify code", "err", err)
		return nil, errInternal
	}
	user, err := h.store.CreateUser(r.Ctx, req.PhoneNumber)
	if err != nil {
		h.log.Error("create user", "err", err)
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
// no-op, so no UserID check is needed. Returns auth.loggedOut on success, and
// the evict announcement to emit once that reply is on the wire: logOut always
// revokes the key the request arrived on, so it is a self-revocation by
// definition and its eviction closes the socket the reply goes out on.
func (h *handlers) handleLogOut(r *mtproto.Request) (bin.Encoder, func(), error) {
	var req tg.AuthLogOutRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, nil, errMethodNotImpl
	}
	keyID := mtproto.AuthKeyIDInt64(r.AuthKeyID)
	// The evict notification is addressed to the user the key was bound to, and
	// only the row carries that: an unauthorized logOut has no r.UserID. Read it
	// before the delete removes it.
	key, ok, err := h.store.AuthKeyByID(r.Ctx, keyID)
	if err != nil {
		h.log.Error("logout: lookup auth key", "err", err)
		return nil, nil, errInternal
	}
	if err := h.store.DeleteAuthKey(r.Ctx, keyID); err != nil {
		h.log.Error("logout: delete auth key", "err", err)
		return nil, nil, errInternal
	}
	// An unbound key is registered under nobody, so there is nothing to evict.
	if !ok || key.UserID == 0 {
		return &tg.AuthLoggedOut{}, nil, nil
	}
	return &tg.AuthLoggedOut{}, func() {
		h.notifyEvict(r.Ctx, key.UserID, keyID)
	}, nil
}
