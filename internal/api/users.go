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
func (h *handlers) channelToTLForResolve(ctx context.Context, c store.Channel, viewerID int64) (tg.ChatClass, error) {
	count, err := h.store.CountChannelParticipants(ctx, c.ID)
	if err != nil {
		return nil, fmt.Errorf("count participants: %w", err)
	}
	return h.channelToTLPublic(c, count, viewerID), nil
}

// channelToTLPublic is the public view of a channel: what an account that is
// not a member may be told about one, on resolveUsername and on the discovery
// arm of contacts.search alike. Title, photo, participant count, username and
// kind flags; no membership details, and Left is set because the viewer is not
// a member as far as this rendering knows.
//
// AccessHash is derived for (viewerID, c.ID). Handing it to a non-member is
// what makes the result usable — the hash names a peer and authorizes nothing,
// and every peer-taking RPC still runs its own membership check.
//
// The participant count is passed in rather than read here: the search arm
// already has it from the query that matched, and a per-row count query on a
// search page is the kind of extra work the M14 threat model asks this path not
// to grow.
func (h *handlers) channelToTLPublic(c store.Channel, participants int64, viewerID int64) *tg.Channel {
	ch := &tg.Channel{
		ID:                c.ID,
		Title:             c.Title,
		AccessHash:        h.peers.Derive(viewerID, peerhash.KindChannel, c.ID),
		Date:              int(c.Date.Unix()),
		Megagroup:         c.Megagroup,
		Broadcast:         !c.Megagroup,
		Left:              true,
		Photo:             &tg.ChatPhotoEmpty{},
		ParticipantsCount: int(participants),
	}
	if c.Username != nil {
		ch.Username = *c.Username
	}
	return ch
}

// handleContactsSearch serves contacts.search: find users and channels by name.
// Only authorized callers may use it.
//
// An empty query returns SEARCH_QUERY_EMPTY. The limit defaults to 10 when
// zero and is capped at 50. It bounds each returned vector: MyResults spends
// one budget across the two arms it unions, and Results has its own.
//
// Three arms, each with its own predicate and none relaxed to match another:
//
//   - MyResults users — users the caller has an existing 1:1 dialog with. No
//     global user search, no cross-dialog leak.
//   - MyResults channels — channels the caller is an unbanned member of,
//     private ones included, since membership is what entitles them.
//   - Results channels — channels holding a public username. This arm takes no
//     viewer at all: public discovery is the same answer for every account, so
//     nothing in it varies with the caller and can be read back. A channel
//     without a username is filtered inside the SQL, before the LIMIT, so it
//     never occupies a row that a caller could count as evidence it exists.
//
// A channel that is both public and the caller's own matches two arms and is
// named in both peer vectors, but is rendered once in Chats, as the member view
// — a client indexes Chats by id and cannot hold two renderings of one channel.
// Which vector named it does not decide that rendering and neither does the
// budget: membership does, so a caller is never handed their own channel marked
// Left because a page boundary fell in the wrong place.
//
// The rate limit is charged once for the whole call, before any arm runs, so
// one query costs one quota unit whatever it matches.
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

	// Rate limit before the lookup, charged identically whether or not the
	// query matches, so the quota cannot be read as an existence oracle.
	if err := h.checkRateLimit(r, "contacts_search", h.rateLimitSearchContacts); err != nil {
		return nil, err
	}

	contacts, err := h.store.SearchContacts(r.Ctx, r.UserID, req.Q, limit)
	if err != nil {
		h.log.Error("contacts.search: store query", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	myChannels, err := h.store.SearchMemberChannels(r.Ctx, r.UserID, req.Q, limit)
	if err != nil {
		h.log.Error("contacts.search: member channels", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	publicChannels, err := h.store.SearchPublicChannels(r.Ctx, req.Q, limit)
	if err != nil {
		h.log.Error("contacts.search: public channels", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Nothing matched anywhere: one empty response, the same one a query naming
	// a private channel produces. "No match", "a private channel matched" and
	// "no such channel" must not be three answers.
	if len(contacts) == 0 && len(myChannels) == 0 && len(publicChannels) == 0 {
		return &tg.ContactsFound{}, nil
	}

	// MyResults unions two arms, so the limit is one budget across both rather
	// than one each: each store call is capped at limit on its own, and
	// concatenating them would return up to twice what the caller asked for and
	// twice what M13 promised. Users are taken first and channels fill what is
	// left, which is a rule rather than a preference — both arms come back
	// ordered by id, so the surviving page is the same on every identical
	// search. The cost is that a caller whose contacts already fill the budget
	// sees none of their channels for that query; raising the limit is the
	// client's lever, and it is capped at 50.
	//
	// This budget is MyResults's alone. Results is a separate vector with its
	// own limit: sharing one budget across both would let the caller's own
	// memberships shrink public discovery, which is caller-independent by
	// construction.
	// Membership is decided from every member match, not from the ones that
	// survive the budget. The budget shortens the peer vector; it says nothing
	// about whether the caller is in a channel, so it must not reach the
	// rendering. A public channel the caller belongs to that is squeezed out of
	// MyResults is still named in Results, and rendering it from the truncated
	// set served the caller their own channel as Left, which a client caches as
	// one they left.
	memberOf := make(map[int64]store.ChannelMember, len(myChannels))
	for _, m := range myChannels {
		memberOf[m.Channel.ID] = m.Member
	}

	// The member arm is itself capped at limit, so a caller in more matching
	// channels than that has memberships it never returned — and a public match
	// outside its page would render as Left for a channel the caller is in, the
	// same defect one level down. Membership for the channels Results names is
	// therefore answered directly, in one bounded query rather than one per row.
	//
	// It is a separate query and not a viewer joined into the discovery arm on
	// purpose: which rows Results contains stays caller-independent, and
	// membership is applied afterwards, as a rendering input only.
	unknown := make([]int64, 0, len(publicChannels))
	for _, p := range publicChannels {
		if _, ok := memberOf[p.Channel.ID]; !ok {
			unknown = append(unknown, p.Channel.ID)
		}
	}
	extra, err := h.store.ChannelMembershipsOf(r.Ctx, r.UserID, unknown)
	if err != nil {
		h.log.Error("contacts.search: memberships", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	for id, m := range extra {
		memberOf[id] = m
	}

	budget := max(int(limit)-len(contacts), 0)
	namedChannels := myChannels
	if len(namedChannels) > budget {
		namedChannels = namedChannels[:budget]
	}

	myResults := make([]tg.PeerClass, 0, len(contacts)+len(namedChannels))
	users := make([]tg.UserClass, 0, len(contacts))
	for _, c := range contacts {
		myResults = append(myResults, &tg.PeerUser{UserID: c.ID})
		users = append(users, h.userToTL(c, r.UserID, c.ID == r.UserID))
	}

	// Only a channel some vector names is rendered, and each is rendered once.
	chats := make([]tg.ChatClass, 0, len(namedChannels)+len(publicChannels))
	rendered := make(map[int64]bool, len(namedChannels)+len(publicChannels))
	for _, m := range namedChannels {
		myResults = append(myResults, &tg.PeerChannel{ChannelID: m.Channel.ID})
		chats = append(chats, h.channelToTL(m.Channel, m.Member, true, r.UserID))
		rendered[m.Channel.ID] = true
	}

	results := make([]tg.PeerClass, 0, len(publicChannels))
	for _, p := range publicChannels {
		results = append(results, &tg.PeerChannel{ChannelID: p.Channel.ID})
		if rendered[p.Channel.ID] {
			continue
		}
		rendered[p.Channel.ID] = true
		if m, ok := memberOf[p.Channel.ID]; ok {
			chats = append(chats, h.channelToTL(p.Channel, m, true, r.UserID))
			continue
		}
		chats = append(chats, h.channelToTLPublic(p.Channel, p.ParticipantsCount, r.UserID))
	}

	return &tg.ContactsFound{
		MyResults: myResults,
		Results:   results,
		Chats:     chats,
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
