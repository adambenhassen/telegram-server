package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

// handleGetUsers serves users.getUsers. In M1 there is no session-to-user
// mapping, so a self lookup on an unauthorized connection always reports the
// key as unregistered, which the client treats as "not logged in" and then
// starts the auth flow.
func (h *handlers) handleGetUsers(_ context.Context, in *bin.Buffer) (bin.Encoder, error) {
	var req tg.UsersGetUsersRequest
	if err := req.Decode(in); err != nil {
		return nil, errMethodNotImpl
	}
	return nil, errAuthKeyUnreg
}
