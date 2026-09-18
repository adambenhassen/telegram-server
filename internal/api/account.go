package api

import (
	"errors"
	"regexp"
	"strings"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// authorizationTTLDays is the session lifetime advertised to clients in
// account.getAuthorizations. The server does not auto-expire sessions, so this
// is a non-misleading policy value (~1 year) rather than a real expiry, and is
// not enforced by any sweep.
const authorizationTTLDays = 365

// handleGetAuthorizations serves account.getAuthorizations. It lists the auth
// keys bound to the caller's user as sessions. An unbound key (UserID 0) is
// reported as unregistered so the client starts the auth flow. The session
// matching the request's own auth key is flagged Current.
func (h *handlers) handleGetAuthorizations(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountGetAuthorizationsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	keys, err := h.store.AuthKeysByUser(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get authorizations: list keys", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	current := mtproto.AuthKeyIDInt64(r.AuthKeyID)
	auths := make([]tg.Authorization, len(keys))
	for i, k := range keys {
		auths[i] = tg.Authorization{
			Current:     k.ID == current,
			Hash:        k.ID,
			DateCreated: int(k.CreatedAt.Unix()),
			DateActive:  int(k.LastSeenAt.Unix()),
		}
	}
	return &tg.AccountAuthorizations{
		AuthorizationTTLDays: authorizationTTLDays,
		Authorizations:       auths,
	}, nil
}

// handleResetAuthorization serves account.resetAuthorization. It revokes the
// caller's session identified by Hash (an auth key id) by deleting that auth
// key. The key is deleted only when it belongs to the caller, so a user cannot
// reset another user's session; a foreign or unknown hash returns HASH_INVALID.
// Resetting another session publishes the eviction before the reply, so a client
// that has seen success cannot trigger an update that overtakes it. Only a caller
// resetting its own current session defers, since there the eviction closes the
// socket the reply goes out on.
func (h *handlers) handleResetAuthorization(r *mtproto.Request) (bin.Encoder, func(), error) {
	var req tg.AccountResetAuthorizationRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, nil, errAuthKeyUnreg
	}
	key, ok, err := h.store.AuthKeyByID(r.Ctx, req.Hash)
	if err != nil {
		h.log.Error("reset authorization: lookup key", "user_id", r.UserID, "err", err)
		return nil, nil, errInternal
	}
	// Scope check: reject unless the target key is one of the caller's own.
	if !ok || key.UserID != r.UserID {
		return nil, nil, errHashInvalid
	}
	if err := h.store.DeleteAuthKey(r.Ctx, req.Hash); err != nil {
		h.log.Error("reset authorization: delete key", "user_id", r.UserID, "err", err)
		return nil, nil, errInternal
	}
	// Emitted after the delete has committed, so the evicted client cannot
	// reconnect on the same key and find the row still present.
	evict := func() { h.notifyEvict(r.Ctx, key.UserID, req.Hash) }
	if selfRevocation(r, req.Hash) {
		return &tg.BoolTrue{}, evict, nil
	}
	evict()
	return &tg.BoolTrue{}, nil, nil
}

// handleUpdateStatus serves account.updateStatus. An authenticated caller sets
// their own online/offline state. Offline=true marks the user offline;
// Offline=false marks them online. Returns tg.BoolTrue.
func (h *handlers) handleUpdateStatus(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountUpdateStatusRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	online := !req.Offline
	if err := h.store.SetUserStatus(r.Ctx, r.UserID, online); err != nil {
		h.log.Error("update status", "user_id", r.UserID, "online", online, "err", err)
		return nil, errInternal
	}
	if err := h.store.Notify(r.Ctx, store.ChannelStatus, store.StatusPayload(r.UserID, online)); err != nil {
		h.log.Error("notify status", "user_id", r.UserID, "err", err)
	}
	return &tg.BoolTrue{}, nil
}

// reservedUsernames is the blocklist of handles that must never be claimed.
var reservedUsernames = map[string]bool{
	"admin":    true,
	"support":  true,
	"help":     true,
	"me":       true,
	"settings": true,
	"telegram": true,
	"channel":  true,
	"channels": true,
	"bot":      true,
	"bots":     true,
	"login":    true,
	"signup":   true,
}

// usernameRe validates a non-empty username: 5–32 chars, ASCII letters/digits/
// underscore, first char must be a letter.
var usernameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{4,31}$`)

// handleUpdateUsername serves account.updateUsername. An authenticated caller
// sets or clears their own username.
//
// An empty string clears the current username. A non-empty string must pass
// validation (length, character set, first char, blocklist) before the store
// is consulted. Returns the updated user (UserClass) on success, matching the
// gotd schema for account.updateUsername.
func (h *handlers) handleUpdateUsername(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountUpdateUsernameRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	username := req.Username
	// Non-empty usernames must pass validation before any DB access.
	if username != "" {
		if !usernameRe.MatchString(username) {
			return nil, errUsernameInvalid
		}
		normalized := strings.ToLower(username)
		if reservedUsernames[normalized] {
			return nil, errUsernameInvalid
		}
	}

	if err := h.store.UpdateUsername(r.Ctx, r.UserID, username); err != nil {
		switch {
		case errors.Is(err, store.ErrUsernameOccupied):
			return nil, errUsernameOccupied
		case errors.Is(err, store.ErrUsernameFloodWait):
			return nil, errUsernameFloodWait
		case errors.Is(err, store.ErrUsernameIsLoginCredential):
			return nil, errUsernameNotModified
		default:
			h.log.Error("update username", "user_id", r.UserID, "username", username, "err", err)
			return nil, errInternal
		}
	}

	// Return the updated user, not BoolTrue: gotd v0.161.0 declares
	// account.updateUsername as returning UserClass.
	updatedUser, ok, err := h.store.UserByID(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("update username: load user", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errInternal
	}
	return h.userToTL(updatedUser, r.UserID, true), nil
}

// handleUpdateProfile serves account.updateProfile. An authenticated caller
// changes their own first and/or last name. About is accepted on the wire and
// ignored: bio storage is out of scope. Name validation is the same predicate
// auth.signUp uses, so a name that registration would accept cannot be rejected
// here.
func (h *handlers) handleUpdateProfile(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountUpdateProfileRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	first, hasFirst := req.GetFirstName()
	last, hasLast := req.GetLastName()
	if hasFirst && !validateSignUpName(first) {
		return nil, errInputRequestInvalid
	}
	if hasLast && !validateSignUpName(last) {
		return nil, errInputRequestInvalid
	}

	if hasFirst || hasLast {
		if err := h.checkRateLimit(r, "update_profile", h.rateLimitUpdateProfile); err != nil {
			return nil, err
		}
		var firstName, lastName *string
		if hasFirst {
			firstName = &first
		}
		if hasLast {
			lastName = &last
		}
		if err := h.store.UpdateProfile(r.Ctx, r.UserID, firstName, lastName); err != nil {
			h.log.Error("update profile", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
	}

	updatedUser, ok, err := h.store.UserByID(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("update profile: load user", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errInternal
	}
	return h.userToTL(updatedUser, r.UserID, true), nil
}
