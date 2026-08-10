package api

import (
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// handleSearchGlobal serves messages.searchGlobal: keyword search across every
// dialog the caller is in, user, chat and channel peers in one reply. Only
// InputMessagesFilterEmpty is accepted, and an empty query is refused, the same
// two rejections messages.search makes.
//
// The result set is the union of two authorized sets and widens neither: owned
// rows for user and chat peers, membership-gated shared rows for channel peers.
// The store composes them; what this handler owns is the cursor, the quota and
// the hydration a client needs to render a hit in a peer it has no dialog open
// on.
//
// folder_id, min_date/max_date and the broadcasts/groups/users-only flags are
// accepted and ignored, the way messages.search ignores the offset fields it
// does not implement. Ignoring the "only" flags returns a superset of what the
// client asked to see, never more than the caller may read.
func (h *handlers) handleSearchGlobal(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSearchGlobalRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.Q == "" {
		return nil, errSearchQueryEmpty
	}
	if utf8.RuneCountInString(req.Q) > maxSearchQueryLen {
		return nil, errMessageTooLong
	}
	if _, ok := req.Filter.(*tg.InputMessagesFilterEmpty); !ok {
		return nil, errInputFilterInvalid
	}
	// Classifying the cursor's peer is the access_hash check and nothing else —
	// pure derivation, no database access — so it belongs above the quota with
	// the rest of the input validation, exactly where messages.search puts its
	// own peer.
	cursorPeerType, cursorPeerID, hasCursor, err := h.globalCursorPeer(req.OffsetPeer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Charge before any lookup. Every call costs the same token whatever it
	// matches and whatever the caller is a member of, so the quota answers no
	// question about what exists.
	if err = h.checkRateLimit(r, "messages_search_global", h.rateLimitSearchGlobal); err != nil {
		return nil, err
	}
	// A cursor is re-authorized on every page rather than trusted because the
	// server issued it: membership can end between two pages, and the peer in
	// the cursor is client-supplied whatever its provenance. The predicate it is
	// re-authorized against is the one the arm that served the row uses, and
	// only that one.
	//
	// Channel posts are shared rows gated on membership, so a channel cursor is
	// refused the moment that membership ends. User and chat cursors name rows
	// the owned arm reaches through owner_id alone, and that arm's predicate
	// cannot be failed by a peer id: a peer the caller has nothing in only moves
	// the keyset within the caller's own rows. Gating a chat cursor on
	// requireMember instead would authorize nothing and would dead-end paging for
	// a caller who left the chat — page 1 serves their retained copy by design,
	// and page 2 would refuse the cursor derived from it, stranding every older
	// match in every other peer.
	if hasCursor && cursorPeerType == store.PeerTypeChannel {
		if _, err = h.requireChannelMember(r.Ctx, cursorPeerID, r.UserID); err != nil {
			return nil, err
		}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	var cursor *store.GlobalSearchCursor
	if hasCursor {
		cursor = &store.GlobalSearchCursor{
			Rate:     int64(req.OffsetRate),
			PeerType: cursorPeerType,
			PeerID:   cursorPeerID,
			MsgID:    int64(req.OffsetID),
		}
	}
	hits, err := h.store.SearchGlobal(r.Ctx, r.UserID, req.Q, cursor, limit)
	if err != nil {
		h.log.Error("search global", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return h.globalSearchSlice(r, hits)
}

// globalCursorPeer classifies the offset_peer of a searchGlobal request. An
// empty peer is the first page and carries no cursor; anything else must be a
// peer whose access_hash was derived for this caller.
//
// offset_rate and offset_id are read only alongside a real peer. The cursor is
// the whole triple or it is nothing: a rate without the peer that produced it
// cannot resume a page, because the peer is what orders rows that share a
// second.
func (h *handlers) globalCursorPeer(peer tg.InputPeerClass, viewerID int64) (store.PeerType, int64, bool, error) {
	switch peer.(type) {
	case nil, *tg.InputPeerEmpty:
		return 0, 0, false, nil
	}
	peerType, peerID, err := h.inputPeer(peer, viewerID)
	if err != nil {
		return 0, 0, false, err
	}
	return peerType, peerID, true, nil
}

// searchCreateUsers loads the member list every chat-create service row on the
// page renders with, keyed by chat.
//
// A create row is reachable from a keyword search: createChat writes it with the
// chat title as its text (chats.go), so a query matching the title matches the
// row, and tg.MessageActionChatCreate carries the members. Without them the
// same row a client gets from messages.search with its participant list arrives
// here with an empty one. chatSearch takes the identical lookup for the identical
// reason; only the batching differs, because a global page spans chats.
func (h *handlers) searchCreateUsers(r *mtproto.Request, hits []store.GlobalSearchHit) (map[int64][]int64, error) {
	out := map[int64][]int64{}
	for _, hit := range hits {
		if hit.Owned == nil || hit.Owned.Action != store.ChatActionCreate {
			continue
		}
		if _, done := out[hit.PeerID]; done {
			continue
		}
		parts, err := h.store.Participants(r.Ctx, hit.PeerID)
		if err != nil {
			return nil, err
		}
		ids := make([]int64, len(parts))
		for i, p := range parts {
			ids[i] = p.UserID
		}
		out[hit.PeerID] = ids
	}
	return out, nil
}

// globalSearchSlice renders a cross-dialog page into messages.messagesSlice: the
// only reply shape that can carry hits from several peers at once plus the
// next_rate a client pages on.
//
// Count is the page, not the corpus, and Inexact says so. A true total would
// mean counting every match the caller may read on every call, which is
// unbounded work for a number no client needs to page.
//
// Users and Chats are collected off the returned rows alone, so a peer appears
// only when a row from it does.
func (h *handlers) globalSearchSlice(r *mtproto.Request, hits []store.GlobalSearchHit) (bin.Encoder, error) {
	var owned []store.Message
	var posts []store.ChannelMessage
	for _, hit := range hits {
		switch {
		case hit.Owned != nil:
			owned = append(owned, *hit.Owned)
		case hit.Post != nil:
			posts = append(posts, *hit.Post)
		}
	}
	ownedFiles, err := h.loadFiles(r.Ctx, owned)
	if err != nil {
		h.log.Error("search global files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	postFiles, err := h.loadChannelFiles(r.Ctx, posts)
	if err != nil {
		h.log.Error("search global post files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	createUsers, err := h.searchCreateUsers(r, hits)
	if err != nil {
		h.log.Error("search global participants", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	msgs := make([]tg.MessageClass, 0, len(hits))
	users := map[int64]bool{r.UserID: true}
	chatIDs := map[int64]bool{}
	channelIDs := map[int64]bool{}
	for _, hit := range hits {
		switch {
		case hit.Post != nil:
			msgs = append(msgs, channelMessageToTL(*hit.Post, r.UserID, postFiles))
			users[hit.Post.FromID] = true
			channelIDs[hit.PeerID] = true
		case hit.Owned != nil:
			msgs = append(msgs, messageToTL(*hit.Owned, createUsers[hit.PeerID], ownedFiles, nil, nil))
			users[hit.Owned.FromID] = true
			// A service row names people the row itself does not author, and a
			// client renders none of them from an id alone.
			switch hit.Owned.Action {
			case store.ChatActionAddUser, store.ChatActionDeleteUser:
				users[hit.Owned.ActionUserID] = true
			case store.ChatActionCreate:
				for _, id := range createUsers[hit.PeerID] {
					users[id] = true
				}
			}
			if hit.PeerType == store.PeerTypeChat {
				chatIDs[hit.PeerID] = true
			} else {
				users[hit.PeerID] = true
			}
		}
	}

	tlUsers, err := h.loadUsers(r.Ctx, users, r.UserID)
	if err != nil {
		h.log.Error("search global users", "err", err)
		return nil, errInternal
	}
	chats, err := h.loadChats(r.Ctx, chatIDs, r.UserID)
	if err != nil {
		h.log.Error("search global chats", "err", err)
		return nil, errInternal
	}
	channels, err := h.loadChannels(r.Ctx, channelIDs, r.UserID)
	if err != nil {
		h.log.Error("search global channels", "err", err)
		return nil, errInternal
	}

	res := &tg.MessagesMessagesSlice{
		Inexact:  true,
		Count:    len(msgs),
		Messages: msgs,
		Chats:    append(chats, channels...),
		Users:    tlUsers,
	}
	// next_rate is the date of the last row served, so the cursor a client sends
	// back is derived from a row it was actually given. An empty page ends the
	// sequence and offers no rate to resume from.
	if n := len(hits); n > 0 {
		res.SetNextRate(int(hits[n-1].Rate))
	}
	return res, nil
}
