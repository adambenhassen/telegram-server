package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

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
