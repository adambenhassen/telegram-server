package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// createUsersForDialog fetches the participant list for a ChatActionCreate row
// when the viewer is still a member. A removed viewer gets nil, matching the
// empty user list getDifference serves for the same event.
func (h *handlers) createUsersForDialog(ctx context.Context, chatID, viewerID int64) ([]int64, error) {
	member, err := h.store.IsMember(ctx, chatID, viewerID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, nil
	}
	parts, err := h.store.Participants(ctx, chatID)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, len(parts))
	for i, p := range parts {
		ids[i] = p.UserID
	}
	return ids, nil
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

// channelDialogs builds the dialog entries for the channels userID belongs to,
// returning the channel ids referenced, the entries, and the top post of each so
// the caller can hydrate media for the whole reply in one query. A channel with
// no posts is skipped: a dialog must name a top message and there is none.
func (h *handlers) channelDialogs(ctx context.Context, userID int64) (map[int64]bool, []tg.DialogClass, []store.ChannelMessage, error) {
	chans, err := h.store.ChannelsForUser(ctx, userID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(chans) > maxChannelDialogs {
		chans = chans[:maxChannelDialogs]
	}

	ids := make(map[int64]bool, len(chans))
	dialogs := make([]tg.DialogClass, 0, len(chans))
	tops := make([]store.ChannelMessage, 0, len(chans))
	for _, ch := range chans {
		// The newest post is the top message. A membership row is what put this
		// channel in the list, so the read is already gated.
		newest, herr := h.store.ChannelHistory(ctx, ch.ID, 0, 1)
		if herr != nil {
			return nil, nil, nil, herr
		}
		if len(newest) == 0 {
			continue
		}
		pts, perr := h.store.ChannelState(ctx, ch.ID)
		if perr != nil {
			return nil, nil, nil, perr
		}
		d := &tg.Dialog{
			Peer:       &tg.PeerChannel{ChannelID: ch.ID},
			TopMessage: int(newest[0].LocalID),
			// Channels keep no per-member read state in M7, so there is no read
			// marker and no honest unread count to report.
			UnreadCount: 0,
		}
		d.SetPts(pts)
		ids[ch.ID] = true
		dialogs = append(dialogs, d)
		tops = append(tops, newest[0])
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
				cu, cerr := h.createUsersForDialog(r.Ctx, m.PeerID, r.UserID)
				if cerr != nil {
					h.log.Error("get dialogs create users", "user_id", r.UserID, "err", cerr)
					return nil, errInternal
				}
				createUsers = cu
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
		tlMsgs = append(tlMsgs, messageToTL(m, topCreateUsers[i], files))
	}

	// Channels write no dialogs row — they keep one message row per channel, not
	// one per member — so the caller's channels are appended after the paged
	// rows rather than read out of the page.
	//
	// First page only, and that is what keeps the reply honest rather than a
	// saving: the block is not part of the paged sequence, so appending it to
	// every page would repeat the same channels on each one and leave a client
	// that pages to the end holding as many copies as it fetched pages.
	// offset_id is the whole page key here (see above — offset_date and
	// offset_peer are ignored), so offset_id == 0 is the only honest test for
	// "first page"; adding offset_date to it would hide the channels entirely
	// from a client that sets a field this handler does not page on.
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
		tlDialogs = append(tlDialogs, channelDialogs...)
		for _, m := range channelTops {
			tlMsgs = append(tlMsgs, channelMessageToTL(m, r.UserID, channelFiles))
			peerIDs[m.FromID] = true
		}
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
	chats, err := h.loadChats(r.Ctx, chatIDs, r.UserID)
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
