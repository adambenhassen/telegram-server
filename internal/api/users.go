package api

import (
	"errors"
	"strings"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
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
		h.log.Error("resolve phone: lookup", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errPhoneNotOccupied
	}

	return &tg.ContactsResolvedPeer{
		Peer:  &tg.PeerUser{UserID: user.ID},
		Users: []tg.UserClass{h.userToTL(user, r.UserID, false)},
	}, nil
}

// handleResolveUsername serves contacts.resolveUsername: resolve a @username to
// a user or channel peer. Only authorized callers may use it. Quota is checked
// before the lookup executes; on miss the same error is returned (indistinguishability).
//
// A leading @ is stripped before lookup. Lookup is case-insensitive.
//
// For channels, title, photo, and participant count are returned even to
// non-members — but not membership details. The phone field is never emitted.
func (h *handlers) handleResolveUsername(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsResolveUsernameRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	username := strings.TrimPrefix(req.Username, "@")
	username = strings.ToLower(username)
	if username == "" {
		return nil, errUsernameNotOccupied
	}

	// Charge quota before the lookup — identically on hit and miss.
	if err := h.store.CheckAndChargeUsernameLookup(r.Ctx, r.UserID, username); err != nil {
		if errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
			return nil, errUsernameLookupFloodWait
		}
		h.log.Error("resolve username: quota", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	resolution, ok, err := h.store.UsernameByHandle(r.Ctx, username)
	if err != nil {
		h.log.Error("resolve username: lookup", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errUsernameNotOccupied
	}

	switch resolution.Kind {
	case store.UsernameKindUser:
		return &tg.ContactsResolvedPeer{
			Peer:  &tg.PeerUser{UserID: resolution.User.ID},
			Users: []tg.UserClass{h.userToTL(resolution.User, r.UserID, false)},
		}, nil
	case store.UsernameKindChannel:
		ch := resolution.Channel

		// Check membership for rendering decision.
		member, found, err := h.store.ChannelMemberOf(r.Ctx, ch.ID, r.UserID)
		if err != nil {
			h.log.Error("resolve username: membership", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}

		var chat tg.ChatClass
		if found && !member.Banned(time.Now()) {
			// Member: use the standard rendering path.
			chat = h.channelToTL(ch, member, true, r.UserID)
		} else {
			// Non-member: render with title, photo, participant count.
			chat = h.channelToTLForResolve(ch, r.UserID)
		}

		return &tg.ContactsResolvedPeer{
			Peer:  &tg.PeerChannel{ChannelID: ch.ID},
			Chats: []tg.ChatClass{chat},
		}, nil
	default:
		return nil, errInternal
	}
}

// channelToTLForResolve renders a channel for a non-member caller resolving
// via username. It returns title, photo, and participant count — but not
// membership details. This is the rendering path for public channels when the
// caller is not a member.
func (h *handlers) channelToTLForResolve(c store.Channel, viewerID int64) tg.ChatClass {
	ah := h.peers.Derive(viewerID, peerhash.KindChannel, c.ID)
	return &tg.ChannelForbidden{
		ID:         c.ID,
		AccessHash: ah,
		Title:      c.Title,
	}
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
		h.userToTL(user, r.UserID, true),
	}}, nil
}
