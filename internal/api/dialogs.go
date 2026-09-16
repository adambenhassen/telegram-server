package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// chatMembership holds the precomputed membership and participants result
// from a create-action user lookup, so loadChats can reuse it and skip
// duplicate queries.
type chatMembership struct {
	member    bool
	userIDs   []int64
	partCount int
}

// createUsersForDialog fetches the participant list for a ChatActionCreate row
// when the viewer is still a member. A removed viewer gets nil, matching the
// empty user list getDifference serves for the same event. It also returns
// the membership result so the caller can thread it into loadChats and avoid
// a duplicate IsMember + Participants query.
func (h *handlers) createUsersForDialog(ctx context.Context, chatID, viewerID int64) ([]int64, *chatMembership, error) {
	member, err := h.store.IsMember(ctx, chatID, viewerID)
	if err != nil {
		return nil, nil, err
	}
	if !member {
		return nil, &chatMembership{member: false}, nil
	}
	parts, err := h.store.Participants(ctx, chatID)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]int64, len(parts))
	for i, p := range parts {
		ids[i] = p.UserID
	}
	return ids, &chatMembership{member: true, userIDs: ids, partCount: len(parts)}, nil
}

// maxChannelDialogs caps the channels one getDialogs reply carries.
//
// Channel dialogs are deliberately NOT paged: channels write no dialogs row, so
// there is no page key to offset them by, and every page of the dialog list
// carries the caller's channels again. The account cap is 500 channels, so this
// is bounded but not free — several queries per channel per call — and the
// shape is the M6 chat-list deferral again (ROADMAP.md:242). Paging them needs
// a sort key channels do not yet have. Batching the reads and paging the block
// is MAIN-109.
const maxChannelDialogs = 100

// maxPeerDialogs bounds one messages.getPeerDialogs request and all of the
// selected-row and hydration work it can trigger.
const maxPeerDialogs = 100

// channelDialogs builds the dialog entries for the channels userID belongs to,
// returning the channel ids referenced, the entries, and the top post of each so
// the caller can hydrate media for the whole reply in one query. A channel with
// no posts is skipped: a dialog must name a top message and there is none.
//
// Single query via ChannelDialogsForUser replaces the previous per-channel
// ChannelHistory + ChannelState loop (2N queries).
func (h *handlers) channelDialogs(ctx context.Context, userID int64) (map[int64]bool, []tg.DialogClass, []store.ChannelMessage, error) {
	rows, err := h.store.ChannelDialogsForUser(ctx, userID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(rows) > maxChannelDialogs {
		rows = rows[:maxChannelDialogs]
	}

	ids := make(map[int64]bool, len(rows))
	dialogs := make([]tg.DialogClass, 0, len(rows))
	tops := make([]store.ChannelMessage, 0, len(rows))
	for _, r := range rows {
		if r.Top == nil {
			continue
		}
		d := &tg.Dialog{
			Peer:       &tg.PeerChannel{ChannelID: r.Channel.ID},
			TopMessage: int(r.Top.LocalID),
			// Channels keep no per-member read state in M7, so there is no read
			// marker and no honest unread count to report.
			UnreadCount: 0,
		}
		d.SetPts(r.Pts)
		ids[r.Channel.ID] = true
		dialogs = append(dialogs, d)
		tops = append(tops, *r.Top)
	}
	return ids, dialogs, tops, nil
}

// handleGetDialogs serves messages.getDialogs: the caller's conversation list
// with each dialog's top message and the referenced peer users.
func (h *handlers) handleGetDialogs(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetDialogsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultDialogsLimit
	}
	if limit > maxDialogsLimit {
		limit = maxDialogsLimit
	}

	// req.OffsetDate and req.OffsetPeer are ignored deliberately. The list is
	// ordered by top_message, the owner's own monotonic local_id, so offset_id
	// alone is a total order over the page key; the other two would only mean
	// something under a different sort.
	dialogs, err := h.store.Dialogs(r.Ctx, r.UserID, int64(req.OffsetID), limit)
	if err != nil {
		h.log.Error("get dialogs", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	tlDialogs := make([]tg.DialogClass, 0, len(dialogs))
	// Top messages are collected first and mapped after the loop, so the whole
	// page's media is hydrated in one query rather than one per dialog.
	tops := make([]store.Message, 0, len(dialogs))
	topCreateUsers := make([][]int64, 0, len(dialogs))
	peerIDs := map[int64]bool{r.UserID: true}
	chatIDs := map[int64]bool{}
	// Precomputed membership/participants results from create-action lookups,
	// threaded into loadChats so it skips duplicate queries.
	chatCache := map[int64]*chatMembership{}
	for _, d := range dialogs {
		tlDialogs = append(tlDialogs, &tg.Dialog{
			Peer:            peerToTL(d.PeerType, d.PeerID),
			TopMessage:      int(d.TopMessage),
			ReadInboxMaxID:  int(d.ReadInboxMaxID),
			ReadOutboxMaxID: int(d.ReadOutboxMaxID),
			UnreadCount:     d.UnreadCount,
		})
		if d.PeerType == store.PeerTypeChat {
			chatIDs[d.PeerID] = true
		} else {
			peerIDs[d.PeerID] = true
		}

		m, ok, err := h.store.MessageByOwnerLocal(r.Ctx, r.UserID, d.TopMessage)
		if err != nil {
			h.log.Error("get dialogs top message", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if ok {
			// The author is the peer in a 1:1 but any member in a group, so it is
			// taken off the message rather than off the dialog's peer id.
			peerIDs[m.FromID] = true
			var createUsers []int64
			switch m.Action {
			case store.ChatActionAddUser, store.ChatActionDeleteUser:
				// ActionUserID is stored on the viewer's own row, so no membership
				// gate is needed — the row exists because fan-out wrote it here.
				peerIDs[m.ActionUserID] = true
			case store.ChatActionCreate:
				cu, cm, cerr := h.createUsersForDialog(r.Ctx, m.PeerID, r.UserID)
				if cerr != nil {
					h.log.Error("get dialogs create users", "user_id", r.UserID, "err", cerr)
					return nil, errInternal
				}
				createUsers = cu
				chatCache[m.PeerID] = cm
				for _, id := range cu {
					peerIDs[id] = true
				}
			}
			tops = append(tops, m)
			topCreateUsers = append(topCreateUsers, createUsers)
		}
	}

	files, err := h.loadFiles(r.Ctx, tops)
	if err != nil {
		h.log.Error("get dialogs files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	tlMsgs := make([]tg.MessageClass, 0, len(tops))
	for i, m := range tops {
		tlMsgs = append(tlMsgs, messageToTL(m, topCreateUsers[i], files, nil, nil))
	}

	// Channels write no dialogs row — they keep one message row per channel, not
	// one per member — so the caller's channels are prepended to the paged rows
	// rather than read out of the page. Prepend (not append) is deliberate: the
	// client derives its next offset_id from the last dialog's top_message, and a
	// channel's top_message lives in a different id space (channel local_id vs
	// dialogs local_id). By placing channels first, the last entry is always a
	// dialogs row, so the cursor stays valid.
	//
	// First page only: the block is not part of the paged sequence, so repeating
	// it on every page would hand a client that pages to the end one copy per
	// page. offset_id == 0 is the only honest test for "first page".
	var channelIDs map[int64]bool
	var channelDialogs []tg.DialogClass
	if req.OffsetID == 0 {
		ids, ds, channelTops, cerr := h.channelDialogs(r.Ctx, r.UserID)
		if cerr != nil {
			h.log.Error("get dialogs channels", "user_id", r.UserID, "err", cerr)
			return nil, errInternal
		}
		channelFiles, ferr := h.loadChannelFiles(r.Ctx, channelTops)
		if ferr != nil {
			h.log.Error("get dialogs channel files", "user_id", r.UserID, "err", ferr)
			return nil, errInternal
		}
		channelIDs, channelDialogs = ids, ds
		tlDialogs = append(channelDialogs, tlDialogs...)
		channelMsgs := make([]tg.MessageClass, 0, len(channelTops)+len(tlMsgs))
		for _, m := range channelTops {
			channelMsgs = append(channelMsgs, channelMessageToTL(m, r.UserID, channelFiles))
			peerIDs[m.FromID] = true
		}
		tlMsgs = append(channelMsgs, tlMsgs...)
	}

	users, err := h.loadUsers(r.Ctx, peerIDs, r.UserID)
	if err != nil {
		h.log.Error("get dialogs users", "err", err)
		return nil, errInternal
	}
	// No membership check on the list itself: a dialog row exists only because a
	// fan-out wrote it for this owner, so an attacker-chosen id never reaches here.
	// A removed member keeps their dialog row by design, which is why the viewer is
	// passed down — loadChats degrades those to tg.ChatForbidden, which carries
	// the id and an empty title and nothing else — no live title, version or
	// participant count reaches a removed member.
	chats, err := h.loadChats(r.Ctx, chatIDs, r.UserID, chatCache)
	if err != nil {
		h.log.Error("get dialogs chats", "err", err)
		return nil, errInternal
	}
	channels, err := h.loadChannels(r.Ctx, channelIDs, r.UserID)
	if err != nil {
		h.log.Error("get dialogs channel peers", "err", err)
		return nil, errInternal
	}
	chats = append(chats, channels...)
	// A short page reached the end of the list, so the plain reply is accurate. A
	// full page may have more behind it and must say so, the way getDifference
	// returns differenceSlice when it truncates. The count is only paid for on
	// that branch.
	if len(dialogs) < limit {
		return &tg.MessagesDialogs{Dialogs: tlDialogs, Messages: tlMsgs, Users: users, Chats: chats}, nil
	}
	total, err := h.store.CountDialogs(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get dialogs count", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// CountDialogs counts dialogs rows, and channels have none, so the appended
	// block has to be added or this page would advertise a total smaller than the
	// list it is shipping. channelDialogs is empty on every page but the first,
	// where the block is not shipped either, so no page ever ships more rows than
	// it counts. The count does still shrink by the channel count after the first
	// page, which is the honest reading of a block that is served once and not
	// paged; making it constant would mean paying the per-channel reads on every
	// page purely to count them.
	total += len(channelDialogs)
	return &tg.MessagesDialogsSlice{Count: total, Dialogs: tlDialogs, Messages: tlMsgs, Users: users, Chats: chats}, nil
}

// handleGetPeerDialogs serves messages.getPeerDialogs. Unlike getDialogs, the
// request names an exact peer set; missing or unauthorized peers are omitted
// from the same read snapshot rather than answered with a distinguishable
// existence error.
func (h *handlers) handleGetPeerDialogs(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetPeerDialogsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if len(req.Peers) == 0 {
		return nil, errInputPeersEmpty
	}
	if len(req.Peers) > maxPeerDialogs {
		return nil, errLimitInvalid
	}

	peers := make([]store.PeerDialogKey, 0, len(req.Peers))
	seen := make(map[store.PeerDialogKey]bool, len(req.Peers))
	for _, input := range req.Peers {
		peer, ok := input.(*tg.InputDialogPeer)
		if !ok || peer.Peer == nil {
			return nil, errPeerIDInvalid
		}
		peerType, peerID, err := h.inputPeer(peer.Peer, r.UserID)
		if err != nil {
			return nil, err
		}
		key := store.PeerDialogKey{PeerType: peerType, PeerID: peerID}
		if seen[key] {
			continue
		}
		seen[key] = true
		peers = append(peers, key)
	}

	snapshot, err := h.store.PeerDialogsSnapshot(r.Ctx, r.UserID, peers)
	if err != nil {
		h.log.Error("get peer dialogs", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return h.peerDialogsToTL(snapshot, r.UserID), nil
}

// peerDialogsToTL is the response-only half of the snapshot path. Every map
// here was populated by the store's repeatable-read selection and the same
// viewer-aware entitlement gates used by getDialogs; this function performs no
// further database reads.
func (h *handlers) peerDialogsToTL(snapshot store.PeerDialogsSnapshot, viewerID int64) *tg.MessagesPeerDialogs {
	tlDialogs := make([]tg.DialogClass, 0, len(snapshot.Dialogs))
	tlMsgs := make([]tg.MessageClass, 0, len(snapshot.Dialogs))
	files := make(map[int64]*tg.Document, len(snapshot.Files))
	for id, file := range snapshot.Files {
		files[id] = h.documentToTL(file)
	}
	chatIDs := make([]int64, 0, len(snapshot.Dialogs))
	channelIDs := make([]int64, 0, len(snapshot.Dialogs))
	seenChats := map[int64]bool{}
	seenChannels := map[int64]bool{}

	for _, selected := range snapshot.Dialogs {
		d := selected.Dialog
		tlDialog := &tg.Dialog{
			Peer:            peerToTL(d.PeerType, d.PeerID),
			TopMessage:      int(d.TopMessage),
			ReadInboxMaxID:  int(d.ReadInboxMaxID),
			ReadOutboxMaxID: int(d.ReadOutboxMaxID),
			UnreadCount:     d.UnreadCount,
		}
		switch d.PeerType {
		case store.PeerTypeChannel:
			tlDialog.SetPts(selected.Pts)
			if !seenChannels[d.PeerID] {
				seenChannels[d.PeerID] = true
				channelIDs = append(channelIDs, d.PeerID)
			}
		case store.PeerTypeChat:
			if !seenChats[d.PeerID] {
				seenChats[d.PeerID] = true
				chatIDs = append(chatIDs, d.PeerID)
			}
		}
		tlDialogs = append(tlDialogs, tlDialog)

		switch {
		case selected.Message != nil:
			var createUsers []int64
			if selected.Message.Action == store.ChatActionCreate && snapshot.ChatMembership[d.PeerID] {
				for _, participant := range snapshot.ChatMembers[d.PeerID] {
					createUsers = append(createUsers, participant.UserID)
				}
			}
			tlMsgs = append(tlMsgs, messageToTL(*selected.Message, createUsers, files, nil, nil))
		case selected.ChannelMessage != nil:
			tlMsgs = append(tlMsgs, channelMessageToTL(*selected.ChannelMessage, viewerID, files))
		}
	}

	users := make([]tg.UserClass, 0, len(snapshot.Users))
	for id, user := range snapshot.Users {
		if id != viewerID && !snapshot.EntitledUsers[id] {
			users = append(users, &tg.UserEmpty{ID: id})
			continue
		}
		users = append(users, h.userToTL(user, viewerID, id == viewerID))
	}

	chats := make([]tg.ChatClass, 0, len(chatIDs)+len(channelIDs))
	for _, chatID := range chatIDs {
		chat, ok := snapshot.Chats[chatID]
		if !ok {
			continue
		}
		if !snapshot.ChatMembership[chatID] {
			chats = append(chats, &tg.ChatForbidden{ID: chat.ID, Title: ""})
			continue
		}
		chats = append(chats, chatToTL(chat, len(snapshot.ChatMembers[chatID]), viewerID))
	}
	for _, channelID := range channelIDs {
		channel, ok := snapshot.Channels[channelID]
		if !ok {
			continue
		}
		chats = append(chats, h.channelToTL(channel, snapshot.ChannelMembers[channelID], true, viewerID))
	}

	return &tg.MessagesPeerDialogs{
		Dialogs:  tlDialogs,
		Messages: tlMsgs,
		Chats:    chats,
		Users:    users,
		State:    *stateToTL(snapshot.State),
	}
}
