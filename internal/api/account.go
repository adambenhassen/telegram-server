package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

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
	return &tg.AccountAuthorizations{Authorizations: auths}, nil
}

// handleResetAuthorization serves account.resetAuthorization. It revokes the
// caller's session identified by Hash (an auth key id) by deleting that auth
// key. The key is deleted only when it belongs to the caller, so a user cannot
// reset another user's session; a foreign or unknown hash returns HASH_INVALID.
func (h *handlers) handleResetAuthorization(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountResetAuthorizationRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	key, ok, err := h.store.AuthKeyByID(r.Ctx, req.Hash)
	if err != nil {
		h.log.Error("reset authorization: lookup key", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// Scope check: reject unless the target key is one of the caller's own.
	if !ok || key.UserID != r.UserID {
		return nil, errHashInvalid
	}
	if err := h.store.DeleteAuthKey(r.Ctx, req.Hash); err != nil {
		h.log.Error("reset authorization: delete key", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.BoolTrue{}, nil
}
