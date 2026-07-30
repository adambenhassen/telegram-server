package api

import (
	"errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// handleResolvePhone serves contacts.resolvePhone: resolve a phone number to a
// peer. Only authorized callers may use it; half-authorized (pending 2FA) keys
// are refused. Quota is checked before the phone is looked up; on miss the same
// error is returned as on any target-side refusal (indistinguishability).
func (h *handlers) handleResolvePhone(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsResolvePhoneRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	if err := validatePhone(req.Phone); err != nil {
		return nil, err
	}

	phone := store.NormalizePhone(req.Phone)

	if err := h.store.CheckAndChargeLookup(r.Ctx, r.UserID, phone); err != nil {
		if errors.Is(err, store.ErrLookupQuotaExceeded) {
			return nil, errLookupFloodWait
		}
		h.log.Error("resolve phone: quota", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	user, ok, err := h.store.UserByPhone(r.Ctx, phone)
	if err != nil {
		h.log.Error("resolve phone: lookup", "phone", phone, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errPhoneNotOccupied
	}

	return &tg.ContactsResolvedPeer{
		Peer:  &tg.PeerUser{UserID: user.ID},
		Users: []tg.UserClass{userToTL(user, false)},
	}, nil
}

// handleGetUsers serves users.getUsers. The request's auth key is resolved to a
// bound user (req.UserID); an unbound key (0) is reported as unregistered, which
// the client treats as "not logged in" and starts the auth flow. A bound key
// returns the account, so a client keeps its authorization across reconnects and
// server restarts without a new handshake.
func (h *handlers) handleGetUsers(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UsersGetUsersRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	user, ok, err := h.store.UserByID(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get users: load user", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errAuthKeyUnreg
	}
	return &tg.UserClassVector{Elems: []tg.UserClass{
		&tg.User{
			ID:         user.ID,
			Self:       true,
			Phone:      user.Phone,
			FirstName:  user.FirstName,
			LastName:   user.LastName,
			AccessHash: user.ID, // M1: self access hash placeholder
		},
	}}, nil
}
