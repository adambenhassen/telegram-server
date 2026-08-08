package api

import (
	"context"
	"encoding/binary"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// replySnippet returns a short preview of the quoted message text, truncated
// to Telegram snippet width. Uses word boundaries where possible.
func replySnippet(s string) string {
	if len(s) <= 25 {
		return s
	}
	trunc := s[:25]
	if r, size := utf8.DecodeLastRuneInString(trunc); r == utf8.RuneError && size == 1 {
		// single byte of a multi-byte rune—drop it
		trunc = trunc[:len(trunc)-1]
	} else if r == utf8.RuneError {
		// multi-byte rune started inside trunc; remove it entirely
		trunc = trunc[:len(trunc)-size]
	}
	return trunc
}

// messageToTL maps a stored message to the wire message. The peer follows
// peer_type; from is always PeerUser, since the author of a chat message is a
// user either way. ids are cast to the wire int space. EditDate populates its
// flag via tg.Message.SetFlags at encode time.
//
// A row carrying an action renders as tg.MessageService instead. createUsers is
// the participant list for a ChatActionCreate row and nil for everything else:
// the mapper stays pure, so the one action that needs a member set is handed it
// rather than fetching it. files is the same pattern for media, keyed by file
// id; a row whose file id is absent from it renders as a plain message.
// reactions, when non-nil, populates the message's Reactions field.
func messageToTL(m store.Message, createUsers []int64, files map[int64]*tg.Document, replyTexts map[int32]string, reactions []store.Reaction) tg.MessageClass {
	if m.Action != store.ChatActionNone {
		return &tg.MessageService{
			ID:     int(m.LocalID),
			Out:    m.Out,
			PeerID: peerToTL(m.PeerType, m.PeerID),
			FromID: &tg.PeerUser{UserID: m.FromID},
			Date:   int(m.Date.Unix()),
			Action: actionToTL(m, createUsers),
		}
	}
	msg := &tg.Message{
		ID:      int(m.LocalID),
		Out:     m.Out,
		PeerID:  peerToTL(m.PeerType, m.PeerID),
		FromID:  &tg.PeerUser{UserID: m.FromID},
		Message: m.Text,
		Date:    int(m.Date.Unix()),
	}
	if m.EditDate != nil {
		msg.EditDate = int(m.EditDate.Unix())
	}
	// SetMedia rather than a plain assignment: Media is a conditional field and
	// encodes only when its flag is set with it.
	if m.ReplyToMsgID > 0 {
		hdr := new(tg.MessageReplyHeader)
		hdr.SetReplyToMsgID(int(m.ReplyToMsgID))
		hdr.SetReplyToPeerID(peerToTL(m.PeerType, m.PeerID))
		if txt := replyTexts[m.ReplyToMsgID]; txt != "" {
			hdr.SetQuoteText(replySnippet(txt))
		}
		msg.SetReplyTo(hdr)
	}
	if d, ok := files[m.FileID]; ok && m.FileID != 0 {
		msg.SetMedia(&tg.MessageMediaDocument{Document: d})
	}
	if m.FwdFromID != 0 || !m.FwdDate.IsZero() {
		fwd := tg.MessageFwdHeader{
			Date: int(m.FwdDate.Unix()),
		}
		if m.FwdChannelID != 0 {
			// Channel source: FromID is the channel, not a user.
			fwd.SetFromID(&tg.PeerChannel{ChannelID: m.FwdChannelID})
			fwd.SetChannelPost(int(m.FwdChannelPost))
		} else if m.FwdFromID != 0 {
			fwd.SetFromID(&tg.PeerUser{UserID: m.FwdFromID})
		}
		msg.SetFwdFrom(fwd)
	}
	if len(reactions) > 0 {
		reactionClasses := make([]tg.ReactionClass, len(reactions))
		for i, r := range reactions {
			reactionClasses[i] = &tg.ReactionEmoji{Emoticon: r.Reaction}
		}
		mr := &tg.MessageReactions{
			Results: make([]tg.ReactionCount, len(reactionClasses)),
		}
		for i, rc := range reactionClasses {
			mr.Results[i] = tg.ReactionCount{Reaction: rc, Count: 1}
		}
		msg.SetReactions(*mr)
	}
	return msg
}

// channelMessageToTL maps a stored channel post to the wire message, the
// channel counterpart of messageToTL. A channel keeps one row per post rather
// than one per member, so Out is derived from the viewer here instead of being
// read off the row, and the peer is always the channel.
//
// Channel posts carry no service actions in M7, so this always renders a
// tg.Message. files is keyed by file id exactly as messageToTL's is, but the
// "no media" sentinel differs and the trap is worth naming:
// channel_messages.file_id is NULL for no media, while messages.file_id is 0.
func channelMessageToTL(m store.ChannelMessage, viewerID int64, files map[int64]*tg.Document) *tg.Message {
	msg := &tg.Message{
		ID:      int(m.LocalID),
		Out:     m.FromID == viewerID,
		PeerID:  &tg.PeerChannel{ChannelID: m.ChannelID},
		FromID:  &tg.PeerUser{UserID: m.FromID},
		Message: m.Message,
		Date:    int(m.Date.Unix()),
	}
	if m.EditDate != nil {
		msg.EditDate = int(m.EditDate.Unix())
	}
	// SetMedia rather than a plain assignment: Media is a conditional field and
	// encodes only when its flag is set with it.
	if m.FileID != nil {
		if d, ok := files[*m.FileID]; ok {
			msg.SetMedia(&tg.MessageMediaDocument{Document: d})
		}
	}
	return msg
}

// documentToTL names a stored file on the wire. Attributes carry only the file
// name: M5 stores no other document attribute, and it never decodes an uploaded
// file, so it cannot honestly claim an image size or a duration it did not
// measure.
//
// FileReference is the 8-byte big-endian file id — a placeholder, the same
// posture as the peer access_hash. It is echoed deterministically and ignored
// entirely on input. Half-validating it would make it an oracle; ignoring it
// does not.
func (h *handlers) documentToTL(f store.File) *tg.Document {
	d := &tg.Document{
		ID:            f.ID,
		AccessHash:    f.AccessHash,
		FileReference: binary.BigEndian.AppendUint64(nil, uint64(f.ID)), //nolint:gosec // G115: opaque 64-bit id, sign irrelevant
		Date:          int(f.Date.Unix()),
		MimeType:      f.MimeType,
		Size:          f.Size,
		DCID:          h.dcID,
	}
	if f.FileName != "" {
		d.Attributes = []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: f.FileName}}
	}
	return d
}

// actionToTL maps a service message's action. Create and EditTitle carry the
// title in the message text; Add/DeleteUser carry their subject in action_user_id.
func actionToTL(m store.Message, createUsers []int64) tg.MessageActionClass {
	switch m.Action {
	case store.ChatActionCreate:
		return &tg.MessageActionChatCreate{Title: m.Text, Users: createUsers}
	case store.ChatActionAddUser:
		return &tg.MessageActionChatAddUser{Users: []int64{m.ActionUserID}}
	case store.ChatActionDeleteUser:
		return &tg.MessageActionChatDeleteUser{UserID: m.ActionUserID}
	case store.ChatActionEditTitle:
		return &tg.MessageActionChatEditTitle{Title: m.Text}
	default:
		return &tg.MessageActionEmpty{}
	}
}

// chatToTL maps a stored chat to the wire tg.Chat. selfID marks the creator flag
// for the recipient of this batch. ParticipantsCount comes from the caller
// because the mapper stays pure.
//
// Deactivated is left false: chats has no such column and store.Chat no longer
// carries the field, so false is the only honest answer until one has a reader.
//
// Photo is mandatory on the wire, not optional: (*tg.Chat).EncodeBare fails the
// whole reply when it is nil, so a chat with no photo must say so explicitly.
func chatToTL(c store.Chat, participantsCount int, selfID int64) *tg.Chat {
	return &tg.Chat{
		ID:                c.ID,
		Title:             c.Title,
		Creator:           c.CreatorID == selfID,
		ParticipantsCount: participantsCount,
		Date:              int(c.Date.Unix()),
		Version:           c.Version,
		Photo:             &tg.ChatPhotoEmpty{},
	}
}

// channelToTL maps a stored channel to the wire. member says whether the viewer
// is currently entitled to the channel's metadata; a non-member and a banned
// member are the same answer, since a ban that still served the live title would
// be cosmetic.
//
// AccessHash is derived for (viewerID, c.ID) so only the viewer can use it.
// The forbidden form carries an empty title on purpose, the rule M6 settled for
// ChatForbidden: the row keeps changing after someone leaves, and any remaining
// member may rename it, so serving the live title would leave a writable channel
// into a client that is no longer entitled to it.
//
// M7 stores no username, participants count or admin rights, so none is ever set
// here. store.Channel.Version has no wire counterpart either: unlike tg.Chat, the
// current tg.Channel schema carries no version field.
//
// Photo is chatPhotoEmpty rather than left nil, the same as chatToTL: the field
// is mandatory in the channel constructor, so a nil one encodes to an error in
// Conn.SendResult and takes down every reply carrying a channel — after the
// mutation has committed. "M7 stores no photo" is said with the empty
// constructor, not by omitting the field.
func (h *handlers) channelToTL(c store.Channel, m store.ChannelMember, member bool, viewerID int64) tg.ChatClass {
	ah := h.peers.Derive(viewerID, peerhash.KindChannel, c.ID)
	if !member {
		return &tg.ChannelForbidden{ID: c.ID, AccessHash: ah, Title: ""}
	}
	return &tg.Channel{
		ID:         c.ID,
		Title:      c.Title,
		AccessHash: ah,
		Date:       int(c.Date.Unix()),
		Megagroup:  c.Megagroup,
		Broadcast:  !c.Megagroup,
		Creator:    m.Role == 2,
		Left:       false,
		Photo:      &tg.ChatPhotoEmpty{},
	}
}

// userToTL maps a stored user to the wire tg.User. AccessHash is derived for
// (viewerID, u.ID) so only the viewer can use it. self marks the update
// recipient's own account. The phone number is private to its owner, so it is
// emitted only on the self entry — names stay for every peer, since a client
// needs them to render a conversation.
func (h *handlers) userToTL(u store.User, viewerID int64, self bool) *tg.User {
	tlUser := &tg.User{
		ID:         u.ID,
		Self:       self,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		AccessHash: h.peers.Derive(viewerID, peerhash.KindUser, u.ID),
		Status:     userStatusToTL(u, self),
	}
	if self {
		tlUser.Phone = u.Phone
	}
	if u.Username != nil {
		tlUser.Username = *u.Username
	}
	return tlUser
}

// userStatusToTL maps a store.User's presence fields to the wire status.
// Self always gets UserStatusRecently — Telegram's canonical sentinel for
// "this is your own account; your last-seen is not disclosed to yourself."
func userStatusToTL(u store.User, self bool) tg.UserStatusClass {
	if self {
		return &tg.UserStatusRecently{}
	}
	if u.IsOnline {
		return &tg.UserStatusOnline{Expires: int(time.Now().Add(5 * time.Minute).Unix())}
	}
	if u.LastSeenAt != nil {
		return &tg.UserStatusOffline{WasOnline: int(u.LastSeenAt.Unix())}
	}
	return &tg.UserStatusEmpty{}
}

func stateToTL(s store.State) *tg.UpdatesState {
	return &tg.UpdatesState{
		Pts:         s.Pts,
		Qts:         s.Qts,
		Date:        s.Date,
		Seq:         s.Seq,
		UnreadCount: s.UnreadCount,
	}
}

// maxDiffEvents caps the events hydrated into one difference/push, bounding the
// work a stale client can force. A truncated batch is returned as a slice with
// an intermediate state so the client re-requests the remainder.
const maxDiffEvents = 500

// updateBatch is one user's hydrated updates plus everything a client must be
// told alongside them. pts[i] is the pts of ups[i], ascending, so one batch can
// serve several of the user's connections without re-querying per connection.
//
// more reports that the batch hit the maxDiffEvents cap and state is an
// intermediate one the client must re-request from.
type updateBatch struct {
	ups   []tg.UpdateClass
	pts   []int
	users []tg.UserClass
	chats []tg.ChatClass
	state store.State
	// head is the user's pts as read for this batch, before any truncation
	// clamp on state. It is what a reader within maxDiffEvents of it can be
	// brought up to by a single further window.
	head int
	more bool
}

// above returns the updates whose pts exceeds fromPts. Events are ordered, so
// the answer is a suffix: a reader already served up to fromPts gets exactly
// the gap between it and the batch, with no duplicate and no hole.
func (b updateBatch) above(fromPts int) []tg.UpdateClass {
	return b.ups[sort.SearchInts(b.pts, fromPts+1):]
}

// buildUpdates hydrates userID's events after fromPts into wire updates plus the
// referenced users and the state to advertise. It is the single delivery path
// shared by updates.getDifference and real-time push.
//
// State is read first, then events are bounded to (fromPts, state.pts], so the
// advertised pts never runs past an event omitted from the response (events and
// their pts bump commit atomically per owner).
func (h *handlers) buildUpdates(ctx context.Context, userID int64, fromPts int) (updateBatch, error) {
	state, err := h.store.State(ctx, userID)
	if err != nil {
		return updateBatch{}, err
	}
	// Fetch one past the cap to detect truncation.
	events, err := h.store.EventsWindow(ctx, userID, fromPts, state.Pts, maxDiffEvents+1)
	if err != nil {
		return updateBatch{}, err
	}
	b := updateBatch{state: state, head: state.Pts}
	if len(events) > maxDiffEvents {
		events = events[:maxDiffEvents]
		b.more = true
	}

	msgs, err := h.batchMessages(ctx, userID, events)
	if err != nil {
		return updateBatch{}, err
	}
	rows := make([]store.Message, 0, len(msgs))
	for _, m := range msgs {
		rows = append(rows, m)
	}
	files, err := h.loadFiles(ctx, rows)
	if err != nil {
		return updateBatch{}, err
	}

	peers := map[int64]bool{}
	basicChats := map[int64]bool{}
	channels := map[int64]bool{}
	for _, ev := range events {
		up, refs, chatRefs, channelRefs, uerr := h.eventToUpdate(ctx, userID, ev, msgs, files)
		if uerr != nil {
			return updateBatch{}, uerr
		}
		if up == nil {
			continue
		}
		b.ups = append(b.ups, up)
		b.pts = append(b.pts, ev.Pts)
		for _, id := range refs {
			peers[id] = true
		}
		for _, id := range chatRefs {
			basicChats[id] = true
		}
		for _, id := range channelRefs {
			channels[id] = true
		}
	}
	// When truncated, advertise only through the last included event's pts.
	if b.more && len(events) > 0 {
		b.state.Pts = events[len(events)-1].Pts
	}

	b.users, err = h.loadUsers(ctx, peers, userID)
	if err != nil {
		return updateBatch{}, err
	}
	b.chats, err = h.loadChats(ctx, basicChats, userID)
	if err != nil {
		return updateBatch{}, err
	}
	if len(channels) > 0 {
		channelTL, cerr := h.loadChannels(ctx, channels, userID)
		if cerr != nil {
			return updateBatch{}, cerr
		}
		b.chats = append(b.chats, channelTL...)
	}
	return b, nil
}

// batchMessages loads, once per distinct local id, the message rows the batch's
// events name, keyed by local id. A row that has since vanished is absent, which
// is the same skip its per-event lookup used to produce. Loading them up front is
// what lets the batch's media be hydrated in a single query.
func (h *handlers) batchMessages(ctx context.Context, userID int64, events []store.Event) (map[int64]store.Message, error) {
	msgs := make(map[int64]store.Message, len(events))
	for _, ev := range events {
		switch ev.Type {
		case store.EventNewMessage, store.EventEdit, store.EventReadIn, store.EventReadOut:
		default:
			continue
		}
		if ev.LocalID == 0 {
			continue
		}
		if _, done := msgs[ev.LocalID]; done {
			continue
		}
		m, ok, err := h.store.MessageByOwnerLocal(ctx, userID, ev.LocalID)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		msgs[ev.LocalID] = m
	}
	return msgs, nil
}

// eventToUpdate builds the wire update for one event owned by userID, returning
// the update, the user ids it references, the chat ids (basic chats only) and
// the channel ids it references. A nil update (message vanished, or an empty
// read marker) is skipped by the caller.
// msgs and files are the batch's pre-loaded rows and their media.
func (h *handlers) eventToUpdate(ctx context.Context, userID int64, ev store.Event, msgs map[int64]store.Message, files map[int64]*tg.Document) (tg.UpdateClass, []int64, []int64, []int64, error) {
	switch ev.Type {
	case store.EventNewMessage, store.EventEdit:
		m, ok := msgs[ev.LocalID]
		if !ok {
			return nil, nil, nil, nil, nil
		}
		// The create action's user list is current member ids, so it is the same
		// disclosure loadChats gates: a viewer removed from the chat still replays
		// their retained copy of the event, and must not learn who is in it now.
		// An empty list is what a non-member gets.
		var createUsers []int64
		if m.Action == store.ChatActionCreate {
			member, merr := h.store.IsMember(ctx, m.PeerID, userID)
			if merr != nil {
				return nil, nil, nil, nil, merr
			}
			if member {
				parts, perr := h.store.Participants(ctx, m.PeerID)
				if perr != nil {
					return nil, nil, nil, nil, perr
				}
				createUsers = make([]int64, len(parts))
				for i, p := range parts {
					createUsers[i] = p.UserID
				}
			}
		}
		tlMsg := messageToTL(m, createUsers, files, nil, nil)
		refs := []int64{m.FromID}
		var chatRefs, channelRefs []int64
		if m.PeerType == store.PeerTypeChat {
			// The peer is the chat, not a user, so the owner is named explicitly:
			// a client rendering a group needs itself in the user list.
			chatRefs = []int64{m.PeerID}
			refs = append(refs, userID)
			// A service message names user ids in its action; without them in the
			// enclosing Users a client renders the add, the removal or the create
			// as unknown users. createUsers is already F6-gated and stays nil for
			// a non-member, so appending it discloses nothing new.
			switch m.Action {
			case store.ChatActionAddUser, store.ChatActionDeleteUser:
				refs = append(refs, m.ActionUserID)
			case store.ChatActionCreate:
				refs = append(refs, createUsers...)
			}
		} else {
			refs = append(refs, m.PeerID)
		}
		// Forwarded messages reference the original sender and optionally a channel.
		if m.FwdFromID != 0 && m.FwdChannelID == 0 {
			refs = append(refs, m.FwdFromID)
		}
		if m.FwdChannelID != 0 {
			channelRefs = []int64{m.FwdChannelID}
		}
		if ev.Type == store.EventEdit {
			return &tg.UpdateEditMessage{Message: tlMsg, Pts: ev.Pts, PtsCount: 1}, refs, chatRefs, channelRefs, nil
		}
		return &tg.UpdateNewMessage{Message: tlMsg, Pts: ev.Pts, PtsCount: 1}, refs, chatRefs, channelRefs, nil

	case store.EventDelete:
		return &tg.UpdateDeleteMessages{Messages: []int{int(ev.LocalID)}, Pts: ev.Pts, PtsCount: 1}, nil, nil, nil, nil

	case store.EventReadIn, store.EventReadOut:
		if ev.LocalID == 0 {
			return nil, nil, nil, nil, nil
		}
		m, ok := msgs[ev.LocalID]
		if !ok {
			return nil, nil, nil, nil, nil
		}
		peer := peerToTL(m.PeerType, m.PeerID)
		var refs, chatRefs []int64
		if m.PeerType == store.PeerTypeChat {
			chatRefs = []int64{m.PeerID}
		} else {
			refs = []int64{m.PeerID}
		}
		if ev.Type == store.EventReadOut {
			return &tg.UpdateReadHistoryOutbox{Peer: peer, MaxID: int(ev.LocalID), Pts: ev.Pts, PtsCount: 1}, refs, chatRefs, nil, nil
		}
		return &tg.UpdateReadHistoryInbox{Peer: peer, MaxID: int(ev.LocalID), StillUnreadCount: 0, Pts: ev.Pts, PtsCount: 1}, refs, chatRefs, nil, nil

	default:
		return nil, nil, nil, nil, nil
	}
}

// loadUsers hydrates the given user ids into wire users, marking selfID as
// Self. viewerID is the account receiving this response; it is used to derive
// the per-viewer access hash for each user.
func (h *handlers) loadUsers(ctx context.Context, ids map[int64]bool, viewerID int64) ([]tg.UserClass, error) {
	users := make([]tg.UserClass, 0, len(ids))
	for id := range ids {
		u, ok, err := h.store.UserByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		users = append(users, h.userToTL(u, viewerID, id == viewerID))
	}
	return users, nil
}

// loadChats hydrates chat ids for viewerID. A chat the viewer is no longer a
// member of still reaches here — their dialog row and their retained message
// copies survive removal by design — so it must not keep serving live metadata.
// tg.ChatForbidden carries the id and an empty title and nothing else, which is
// what tells a client to stop rendering the chat as active. The title is blanked
// deliberately: the live row keeps changing after removal, and any remaining
// member may rename the chat freely, so serving it would leave a writable
// channel into an account that was ejected.
//
// The membership check is one query per chat per batch. A batch references very
// few distinct chats, so it stays a straight loop with no cache.
func (h *handlers) loadChats(ctx context.Context, ids map[int64]bool, viewerID int64) ([]tg.ChatClass, error) {
	chats := make([]tg.ChatClass, 0, len(ids))
	for id := range ids {
		c, ok, err := h.store.ChatByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		member, err := h.store.IsMember(ctx, id, viewerID)
		if err != nil {
			return nil, err
		}
		if !member {
			chats = append(chats, &tg.ChatForbidden{ID: c.ID, Title: ""})
			continue
		}
		parts, err := h.store.Participants(ctx, id)
		if err != nil {
			return nil, err
		}
		chats = append(chats, chatToTL(c, len(parts), viewerID))
	}
	return chats, nil
}

// loadChannels hydrates channel ids for viewerID, the channel counterpart of
// loadChats. An id with no channel row is skipped. A viewer who is not a member,
// or whose membership is banned as of now, gets tg.ChannelForbidden — a ban is
// not cosmetic, so it must revoke metadata the same way leaving does.
//
// Two queries per channel per batch, no cache, for the reason loadChats states:
// a batch names very few distinct channels.
func (h *handlers) loadChannels(ctx context.Context, ids map[int64]bool, viewerID int64) ([]tg.ChatClass, error) {
	channels := make([]tg.ChatClass, 0, len(ids))
	for id := range ids {
		ch, ok, err := h.store.ChannelByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		member, found, err := h.store.ChannelMemberOf(ctx, id, viewerID)
		if err != nil {
			return nil, err
		}
		channels = append(channels, h.channelToTL(ch, member, found && !member.Banned(time.Now()), viewerID))
	}
	return channels, nil
}

// channelBatch is one channel's hydrated updates plus everything a client must
// be told alongside them. pts[i] is the pts of ups[i], ascending.
type channelBatch struct {
	ups        []tg.UpdateClass
	pts        []int
	users      []tg.UserClass
	chats      []tg.ChatClass
	currentPts int
	more       bool
}

// buildChannelUpdates hydrates channelID's events after fromPts into wire
// updates plus the referenced users and channels. It is the single delivery
// path for updates.getChannelDifference; the live push (MAIN-96) must call
// this same function, never a second serialisation path. Limit is clamped into
// [1, maxDiffEvents] before this is called.
//
// State is read first, then events are bounded to (fromPts, currentPts], so the
// advertised pts never runs past an event omitted from the response. This is
// the same ordering buildUpdates uses for the per-account stream.
func (h *handlers) buildChannelUpdates(ctx context.Context, channelID, viewerID int64, fromPts, limit, currentPts int) (channelBatch, error) {
	// Fetch one past the cap to detect truncation.
	events, err := h.store.ChannelEventsWindow(ctx, channelID, fromPts, currentPts, limit+1)
	if err != nil {
		return channelBatch{}, err
	}
	b := channelBatch{currentPts: currentPts}
	if len(events) > limit {
		events = events[:limit]
		b.more = true
	}

	// Collect local ids for batched message load.
	localIDs := make([]int64, 0, len(events))
	for _, ev := range events {
		localIDs = append(localIDs, ev.LocalID)
	}
	msgs, err := h.store.ChannelMessages(ctx, channelID, localIDs)
	if err != nil {
		return channelBatch{}, err
	}

	// Load files for all messages in the batch.
	chMsgs := make([]store.ChannelMessage, 0, len(msgs))
	for _, m := range msgs {
		chMsgs = append(chMsgs, m)
	}
	files, err := h.loadChannelFiles(ctx, chMsgs)
	if err != nil {
		return channelBatch{}, err
	}

	peers := map[int64]bool{}
	for _, ev := range events {
		up, refs := h.channelEventToUpdate(ctx, channelID, viewerID, ev, msgs, files)
		if up == nil {
			continue
		}
		b.ups = append(b.ups, up)
		b.pts = append(b.pts, ev.Pts)
		for _, id := range refs {
			peers[id] = true
		}
	}
	// When truncated, pts advertised is the last included event's pts, not
	// the channel's current pts — that is the whole point of the bound.
	if b.more && len(events) > 0 {
		b.currentPts = events[len(events)-1].Pts
	}

	b.users, err = h.loadUsers(ctx, peers, viewerID)
	if err != nil {
		return channelBatch{}, err
	}
	b.chats, err = h.loadChannels(ctx, map[int64]bool{channelID: true}, viewerID)
	if err != nil {
		return channelBatch{}, err
	}
	return b, nil
}

// channelEventToUpdate builds the wire update for one channel event, returning
// the update and the user ids it references. Only event type 1 (new message)
// is rendered in M7; types 2 and 3 are skipped with a debug log. A nil update
// is returned when the message row is not found.
func (h *handlers) channelEventToUpdate(_ context.Context, channelID, viewerID int64, ev store.ChannelEvent, msgs map[int64]store.ChannelMessage, files map[int64]*tg.Document) (tg.UpdateClass, []int64) {
	switch ev.Type {
	case store.EventNewMessage:
		m, ok := msgs[ev.LocalID]
		if !ok {
			h.log.Debug("channel message row not found", "local_id", ev.LocalID, "channel_id", channelID, "pts", ev.Pts)
			return nil, nil
		}
		return &tg.UpdateNewChannelMessage{
			Message:  channelMessageToTL(m, viewerID, files),
			Pts:      ev.Pts,
			PtsCount: 1,
		}, []int64{m.FromID}
	default:
		// Types 2 (edit) and 3 (delete) are unused in M7. Skip rather than
		// fail the batch; log at debug so a future implementation can trace
		// how often this fires.
		h.log.Debug("unknown channel event type", "type", ev.Type, "channel_id", channelID, "pts", ev.Pts)
		return nil, nil
	}
}

// handleGetState serves updates.getState.
func (h *handlers) handleGetState(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	st, err := h.store.State(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get state", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return stateToTL(st), nil
}

// handleGetDifference serves updates.getDifference: it replays the caller's
// missed pts events and qts-gapped encrypted messages. When caught up it
// returns differenceEmpty; a client ahead of the server is clamped to empty.
func (h *handlers) handleGetDifference(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UpdatesGetDifferenceRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	b, err := h.buildUpdates(r.Ctx, r.UserID, req.Pts)
	if err != nil {
		h.log.Error("get difference", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Qts gap: fill encrypted messages the client has not yet seen.
	// req.Qts > state.Qts is the client-ahead case — same clamp as pts, treat
	// as caught up. req.Qts == state.Qts means no gap.
	var encMsgs []tg.EncryptedMessageClass
	encMore := false
	newQts := b.state.Qts
	if req.Qts < b.state.Qts {
		evts, eerr := h.store.EncryptedEventsWindow(r.Ctx, r.UserID, req.Qts, b.state.Qts, maxDiffEvents+1)
		if eerr != nil {
			h.log.Error("get difference qts", "user_id", r.UserID, "err", eerr)
			return nil, errInternal
		}
		if len(evts) > maxDiffEvents {
			evts = evts[:maxDiffEvents]
			encMore = true
		}
		for _, ev := range evts {
			encMsgs = append(encMsgs, &tg.EncryptedMessage{
				RandomID: ev.RandomID,
				ChatID:   int(ev.ChatID),
				Date:     int(ev.Date.Unix()),
				Bytes:    ev.Bytes,
			})
		}
		if encMore && len(evts) > 0 {
			newQts = evts[len(evts)-1].Qts
		}
	}

	// updateEncryption: secret chats whose state changed after req.Date.
	// These carry no qts and are at-least-once; deliver in other_updates.
	clientDate := time.Unix(int64(req.Date), 0)
	secretChats, serr := h.store.SecretChatsAfterDate(r.Ctx, r.UserID, clientDate)
	if serr != nil {
		h.log.Error("get difference secret chats", "user_id", r.UserID, "err", serr)
		return nil, errInternal
	}

	if !b.more && !encMore && len(b.ups) == 0 && len(encMsgs) == 0 && len(secretChats) == 0 {
		return &tg.UpdatesDifferenceEmpty{Date: b.state.Date, Seq: b.state.Seq}, nil
	}

	var newMessages []tg.MessageClass
	var other []tg.UpdateClass
	for _, u := range b.ups {
		if nm, ok := u.(*tg.UpdateNewMessage); ok {
			newMessages = append(newMessages, nm.Message)
		} else {
			other = append(other, u)
		}
	}
	for _, sc := range secretChats {
		other = append(other, &tg.UpdateEncryption{
			Chat: h.encryptedChatFor(sc, r.UserID),
			Date: int(sc.Date.Unix()),
		})
	}

	// The intermediate/final state advertises the qts of the last included
	// encrypted event when truncated, or state.Qts when the gap is closed.
	st := b.state
	st.Qts = newQts

	if b.more || encMore {
		return &tg.UpdatesDifferenceSlice{
			NewMessages:          newMessages,
			NewEncryptedMessages: encMsgs,
			OtherUpdates:         other,
			Users:                b.users,
			Chats:                b.chats,
			IntermediateState:    *stateToTL(st),
		}, nil
	}
	return &tg.UpdatesDifference{
		NewMessages:          newMessages,
		NewEncryptedMessages: encMsgs,
		OtherUpdates:         other,
		Users:                b.users,
		Chats:                b.chats,
		State:                *stateToTL(st),
	}, nil
}
