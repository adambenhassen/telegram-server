package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

		// Public rendering for all callers — regardless of membership.
		// contacts.resolveUsername always returns the same public view so the
		// response does not leak membership state.
		chat, err := h.channelToTLForResolve(r.Ctx, ch, r.UserID)
		if err != nil {
			h.log.Error("resolve username: render", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}

		return &tg.ContactsResolvedPeer{
			Peer:  &tg.PeerChannel{ChannelID: ch.ID},
			Chats: []tg.ChatClass{chat},
		}, nil
	default:
		return nil, errInternal
	}
}

// channelToTLForResolve renders a channel for contacts.resolveUsername.
// All callers see the same public view — membership is never checked.
// Returns title, photo, participant count, and kind flags. No membership
// details are included.
func (h *handlers) channelToTLForResolve(ctx context.Context, c store.Channel, viewerID int64) (tg.ChatClass, error) {
	a := h.peers.Derive(viewerID, peerhash.KindChannel, c.ID)
	count, err := h.store.CountChannelParticipants(ctx, c.ID)
	if err != nil {
		return nil, fmt.Errorf("count participants: %w", err)
	}
	return &tg.Channel{
		ID:                c.ID,
		Title:             c.Title,
		AccessHash:        a,
		Date:              int(c.Date.Unix()),
		Megagroup:         c.Megagroup,
		Broadcast:         !c.Megagroup,
		Left:              true,
		Photo:             &tg.ChatPhotoEmpty{},
		ParticipantsCount: int(count),
	}, nil
}

// handleContactsSearch serves contacts.search: find users by name substring,
// restricted to the caller's existing dialog partners. Only authorized callers
// may use it.
//
// An empty query returns SEARCH_QUERY_EMPTY. The limit defaults to 10 when
// zero and is capped at 50. Results are scoped to users with whom the caller
// has an existing 1:1 dialog — no global search, no cross-dialog leak.
func (h *handlers) handleContactsSearch(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsSearchRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	if req.Q == "" {
		return nil, errSearchQueryEmpty
	}
	const maxContactsSearchQuery = 256
	if len(req.Q) > maxContactsSearchQuery {
		return nil, errSearchQueryTooLong
	}

	limit := int32(req.Limit) //nolint:gosec // limit is a small validated page size
	if limit <= 0 {
		limit = 10
	}
	const maxContactsSearchLimit = 50
	if limit > maxContactsSearchLimit {
		limit = maxContactsSearchLimit
	}

	// Rate limit: pre-charged, before the search query.
	if err := h.checkRateLimit(r, "contacts_search", h.rateLimitSearchContacts); err != nil {
		return nil, err
	}

	contacts, err := h.store.SearchContacts(r.Ctx, r.UserID, req.Q, limit)
	if err != nil {
		h.log.Error("contacts.search: store query", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	if len(contacts) == 0 {
		return &tg.ContactsFound{}, nil
	}

	myResults := make([]tg.PeerClass, len(contacts))
	users := make([]tg.UserClass, len(contacts))
	for i, c := range contacts {
		myResults[i] = &tg.PeerUser{UserID: c.ID}
		users[i] = h.userToTL(c, r.UserID, c.ID == r.UserID)
	}

	return &tg.ContactsFound{
		MyResults: myResults,
		Results:   nil, // M13 has no global user search
		Chats:     nil, // out of scope
		Users:     users,
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
		h.userToTL(user, r.UserID, true),
	}}, nil
}
