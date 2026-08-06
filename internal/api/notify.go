package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// Updater turns LISTEN/NOTIFY nudges into server-initiated pushes. It is the
// consumer end of the delivery path: given a userID it computes that user's
// pending updates once (shared with getDifference) and writes them to each of
// the user's live connections in this process.
type Updater struct {
	h        *handlers
	registry *mtproto.SessionRegistry
	log      *slog.Logger
}

// NewUpdater builds an Updater over the store and the server's session registry.
func NewUpdater(s *store.Store, registry *mtproto.SessionRegistry, log *slog.Logger, peers *peerhash.Deriver) *Updater {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Updater{
		h:        &handlers{store: s, log: log, peers: peers},
		registry: registry,
		log:      log,
	}
}

// pushConn is the part of *mtproto.Conn the fan-out uses: its push watermark
// and the owner-checked write.
type pushConn interface {
	LastPushedPts() int
	PushTo(ctx context.Context, owner int64, enc bin.Encoder, pts int) (bool, error)
}

// Deliver pushes userID's not-yet-delivered events to each of its live conns,
// advancing each conn's last-pushed pts. It is best-effort: a push failure is
// logged and the client's next getDifference backfills.
func (u *Updater) Deliver(ctx context.Context, userID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) == 0 {
		return
	}
	targets := make([]pushConn, len(conns))
	for i, c := range conns {
		targets[i] = c
	}
	u.deliver(ctx, userID, targets, func(fromPts int) (updateBatch, error) {
		return u.h.buildUpdates(ctx, userID, fromPts)
	})
}

// maxDeliveryRounds caps the store round trips one notification may cost. Two
// windows cover the case that matters — a socket that just registered sits at
// watermark 0 while the account's other sockets sit at the head, and a backlog
// past maxDiffEvents puts them a batch apart — without letting watermarks
// spread across many batches turn into one window per socket, which is the
// amplification this path exists to avoid.
const maxDeliveryRounds = 2

// deliver builds one batch from the lowest watermark among conns and gives each
// conn the suffix above its own, so the store work one notification costs does
// not multiply with the number of sockets a user holds. The window is
// contiguous from that lowest watermark, so every conn's events are inside it.
//
// Each conn is told the batch's own pts, and receives every update the batch
// holds above its watermark, so the advertised pts still never runs past an
// update the conn was not given — the invariant a push shares with a poll. A
// conn whose watermark sits below the window start is left alone rather than
// pushed a batch with a hole in front of it.
//
// A batch truncated at maxDiffEvents ends below the watermark of any conn that
// was already further ahead, so those take a second window. That one is
// anchored at the head, not at a conn: a window reaches the head only if it
// starts within maxDiffEvents of it and may not start past a conn it serves, so
// starting at head-maxDiffEvents brings every conn within one batch of the head
// — every socket a live push is for — up to date in one go. Past
// maxDeliveryRounds the fan-out stops, and a conn deeper than that is picked up
// by a later notification, whose window has advanced, or by its own
// getDifference — push is the optimisation, not the guarantee.
func (u *Updater) deliver(ctx context.Context, userID int64, conns []pushConn, build func(fromPts int) (updateBatch, error)) {
	var head int
	for round := 0; round < maxDeliveryRounds && len(conns) > 0; round++ {
		lo, hi := conns[0].LastPushedPts(), conns[0].LastPushedPts()
		for _, c := range conns[1:] {
			w := c.LastPushedPts()
			lo, hi = min(lo, w), max(hi, w)
		}
		from := lo
		if round > 0 {
			tail := head - maxDiffEvents
			from = max(lo, tail)
			if hi < tail {
				// Not even the furthest-along conn is within a batch of the
				// head, so no window can both reach the head and start below
				// one of these conns: they are all still catching up. Spend the
				// round on the one nearest the head rather than on a window
				// that would serve nobody.
				from = hi
			}
		}
		b, err := build(from)
		if err != nil {
			u.log.Error("deliver build updates", "user_id", userID, "err", err)
			return
		}
		head = b.head
		// Nothing pending for the furthest-behind conn means nothing pending
		// for any of them.
		if len(b.ups) == 0 && !b.more {
			return
		}

		var ahead []pushConn
		for _, c := range conns {
			watermark := c.LastPushedPts()
			if watermark < from {
				// This window starts past the conn: the batch is missing the
				// events between the two, so it is not this round's to serve.
				continue
			}
			if watermark >= b.state.Pts {
				if b.more {
					ahead = append(ahead, c)
				}
				continue
			}
			ups := b.above(watermark)
			if len(ups) == 0 {
				continue
			}
			// Addressed to userID: this snapshot was taken before the batch was
			// built, and the conn's auth key can rebind to another user in between.
			// A push dropped for that reason costs nothing — the user's next poll
			// backfills it.
			//
			// users covers the whole batch, so a conn taking a suffix gets a
			// superset of the users it needs, which a client ignores.
			if _, err := c.PushTo(ctx, userID, wrapUpdates(ups, b.users, b.chats, b.state), b.state.Pts); err != nil {
				u.log.Info("deliver push", "user_id", userID, "err", err)
			}
		}
		conns = ahead
	}
}

// DeliverTyping pushes a transient updateUserTyping to the peer's live conns. It
// is never persisted and never appears in getDifference.
func (u *Updater) DeliverTyping(ctx context.Context, peerID, fromID int64) {
	conns := u.registry.Conns(peerID)
	if len(conns) == 0 {
		return
	}
	short := &tg.UpdateShort{
		Update: &tg.UpdateUserTyping{UserID: fromID, Action: &tg.SendMessageTypingAction{}},
		Date:   int(time.Now().Unix()),
	}
	for _, c := range conns {
		// Carries no pts, so a conn that changed hands simply drops it.
		if _, err := c.PushTo(ctx, peerID, short, 0); err != nil {
			u.log.Info("deliver typing", "peer_id", peerID, "err", err)
		}
	}
}

// DeliverChannelPost pushes a newly-committed channel post to every non-banned
// member that holds a live connection on this replica. It is the channelPost
// callback for StartListener (part 2 of MAIN-96 / MAIN-114).
//
// ChannelState is fetched lazily — only on the first member found with a live
// conn. A replica where nobody is home skips it entirely after one
// ChannelMembers query and O(members) in-memory registry lookups.
//
// The per-connection channel-pts watermark does not exist on Conn. The window
// is anchored one step below currentPts (or at JoinPts if that is higher),
// so the triggering event is always included. A duplicate UpdateNewChannelMessage
// is safe (the client dedups on pts); a missed one is backfilled by the next
// getChannelDifference.
func (u *Updater) DeliverChannelPost(ctx context.Context, channelID int64) {
	members, err := u.h.store.ChannelMembers(ctx, channelID)
	if err != nil {
		u.log.Error("deliver channel post members", "channel_id", channelID, "err", err)
		return
	}
	now := time.Now()
	var (
		currentPts int
		ptsFetched bool
	)
	u.deliverChannel(ctx, members, now,
		func(userID int64) []pushConn {
			raw := u.registry.Conns(userID)
			if len(raw) == 0 {
				return nil
			}
			cs := make([]pushConn, len(raw))
			for i, c := range raw {
				cs[i] = c
			}
			return cs
		},
		func(memberID int64, fromPts int) (channelBatch, error) {
			if !ptsFetched {
				pts, serr := u.h.store.ChannelState(ctx, channelID)
				if serr != nil {
					u.log.Error("deliver channel post state", "channel_id", channelID, "err", serr)
					return channelBatch{}, serr
				}
				currentPts = pts
				ptsFetched = true
			}
			return u.h.buildChannelUpdates(ctx, channelID, memberID, max(fromPts, currentPts-1), maxDiffEvents, currentPts)
		},
	)
}

// deliverChannel is the testable core of DeliverChannelPost. For each
// non-banned member that has live conns it calls build once (with the member's
// JoinPts as the floor hint) and pushes the resulting updates to each conn.
//
// The callback runs on the listener's single goroutine; it must not block. All
// pushes are inline. The bound that makes this acceptable: the number of members
// with live conns on this replica is typically small (the replica serves a
// fraction of all members). If that fraction grows too large for inline work,
// the callback would need to hand off to a worker pool.
func (u *Updater) deliverChannel(
	ctx context.Context,
	members []store.ChannelMember,
	now time.Time,
	connsFor func(userID int64) []pushConn,
	build func(memberID int64, fromPts int) (channelBatch, error),
) {
	for _, m := range members {
		if m.Banned(now) {
			continue
		}
		conns := connsFor(m.UserID)
		if len(conns) == 0 {
			continue
		}
		b, err := build(m.UserID, m.JoinPts)
		if err != nil {
			u.log.Error("deliver channel build", "user_id", m.UserID, "err", err)
			continue
		}
		if len(b.ups) == 0 {
			continue
		}
		env := &tg.Updates{
			Updates: b.ups,
			Users:   b.users,
			Chats:   b.chats,
			Date:    int(now.Unix()),
			Seq:     0,
		}
		for _, c := range conns {
			// Pass pts=0: this is a channel update; the conn's lastPushedPts
			// tracks the per-account stream and must not be corrupted by a
			// channel pts value.
			if _, err := c.PushTo(ctx, m.UserID, env, 0); err != nil {
				u.log.Info("deliver channel push", "user_id", m.UserID, "err", err)
			}
		}
	}
}

// DeliverEncryption pushes a secret chat's new state to userID's live conns as
// updateEncryption. It is the encryption callback for StartListener.
//
// It carries no pts and is never persisted, exactly like DeliverTyping: key
// exchange spends no pts, so there is nothing for getDifference to replay. That
// makes this the one delivery path where a missed push is a real loss rather
// than a deferred one — until the qts stream lands (MAIN-140), a client that was
// offline for the push learns the new state only by acting on the chat. The
// alternative, spending pts on a secret chat, would put encrypted-chat state
// into the per-account event log the issue explicitly holds unchanged.
//
// The row is read only after a live conn is found, so a replica where neither
// party is connected pays one registry lookup and no query.
func (u *Updater) DeliverEncryption(ctx context.Context, userID, chatID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) == 0 {
		return
	}
	chat, err := u.h.store.SecretChatByID(ctx, int32(chatID)) //nolint:gosec // chat_id is int32 on the wire
	if err != nil {
		u.log.Error("deliver encryption load", "user_id", userID, "chat_id", chatID, "err", err)
		return
	}
	if !chat.Party(userID) {
		// A payload naming a non-party is either corrupt or forged. Dropping it
		// is what keeps one NOTIFY line from disclosing g_a to a stranger.
		u.log.Warn("deliver encryption to non-party", "user_id", userID, "chat_id", chatID)
		return
	}
	short := &tg.UpdateShort{
		Update: &tg.UpdateEncryption{
			Chat: u.h.encryptedChatFor(chat, userID),
			Date: int(time.Now().Unix()),
		},
		Date: int(time.Now().Unix()),
	}
	for _, c := range conns {
		// Carries no pts, so a conn that changed hands simply drops it.
		if _, err := c.PushTo(ctx, userID, short, 0); err != nil {
			u.log.Info("deliver encryption", "user_id", userID, "err", err)
		}
	}
}

// DeliverStatus pushes updateUserStatus to every dialog partner of userID whose
// live connections exist on this replica. It is the status callback for StartListener.
//
// The changed user's own connections are never pushed to — clients update their
// own display via the account.updateStatus response, not a push.
//
// online=true → UserStatusOnline with a 5-minute expires window (Telegram canonical).
// online=false → UserStatusOffline with last_seen_at from the user row.
func (u *Updater) DeliverStatus(ctx context.Context, userID int64, online bool) {
	partners, err := u.h.store.DialogPartners(ctx, userID)
	if err != nil {
		u.log.Error("deliver status partners", "user_id", userID, "err", err)
		return
	}
	if len(partners) == 0 {
		return
	}

	var status tg.UserStatusClass
	if online {
		status = &tg.UserStatusOnline{Expires: int(time.Now().Add(5 * time.Minute).Unix())}
	} else {
		user, ok, err := u.h.store.UserByID(ctx, userID)
		if err != nil {
			u.log.Error("deliver status user", "user_id", userID, "err", err)
			return
		}
		if !ok {
			u.log.Warn("deliver status user not found", "user_id", userID)
			return
		}
		wasOnline := int64(0)
		if user.LastSeenAt != nil {
			wasOnline = user.LastSeenAt.Unix()
		}
		status = &tg.UserStatusOffline{WasOnline: int(wasOnline)}
	}

	for _, partnerID := range partners {
		conns := u.registry.Conns(partnerID)
		for _, c := range conns {
			short := &tg.UpdateShort{
				Update: &tg.UpdateUserStatus{UserID: userID, Status: status},
				Date:   int(time.Now().Unix()),
			}
			// Carries no pts, so a conn that changed hands simply drops it.
			if _, err := c.PushTo(ctx, partnerID, short, 0); err != nil {
				u.log.Info("deliver status push", "partner_id", partnerID, "err", err)
			}
		}
	}
}

// DeliverEncryptedMsg pushes updateNewEncryptedMessage to the recipient's live
// connections. It is the encryptedMsg callback for StartListener.
//
// The event row is fetched after finding a live conn, so a replica where the
// recipient is offline pays one registry lookup and no query.
func (u *Updater) DeliverEncryptedMsg(ctx context.Context, recipientID int64, qts int) {
	conns := u.registry.Conns(recipientID)
	if len(conns) == 0 {
		return
	}
	event, err := u.h.store.GetEncryptedEvent(ctx, recipientID, qts)
	if err != nil {
		u.log.Error("deliver encrypted msg load", "recipient_id", recipientID, "qts", qts, "err", err)
		return
	}
	update := &tg.UpdateShort{
		Update: &tg.UpdateNewEncryptedMessage{
			Message: &tg.EncryptedMessage{
				RandomID: event.RandomID,
				ChatID:   int(event.ChatID),
				Date:     int(event.Date.Unix()),
				Bytes:    event.Bytes,
			},
			Qts: qts,
		},
		Date: int(event.Date.Unix()),
	}
	for _, c := range conns {
		if _, err := c.PushTo(ctx, recipientID, update, 0); err != nil {
			u.log.Info("deliver encrypted msg push", "recipient_id", recipientID, "err", err)
		}
	}
}

// Evict closes the connections of userID that still hold authKeyID, which the
// revoking replica has just deleted from auth_keys. It is the cross-replica half
// of revocation: without it a socket that sends no further frame keeps its
// cached key, and keeps receiving message bodies, until the read timeout.
//
// It is deliberately narrow. Nothing stored is touched, the registry is left to
// the serve goroutine's own cleanup, and a key id matching no live conn is
// ignored: closing every conn of the user instead would turn one forged NOTIFY
// line into a whole-account disconnect.
func (u *Updater) Evict(_ context.Context, userID, authKeyID int64) {
	for _, c := range u.registry.Conns(userID) {
		if c.AuthKeyID() != authKeyID {
			continue
		}
		// Closing the transport unblocks that conn's Recv; the serve goroutine
		// deregisters and disowns it on the way out. Errors are informational:
		// the socket is going away either way.
		if err := c.Close(); err != nil {
			u.log.Info("evict close", "user_id", userID, "err", err)
		}
	}
}

// DeliverReactions pushes updateMessageReactions to userID's live conns for
// the single message identified by (ownerID, localID). It is the reactions
// callback for StartListener.
//
// Reactions carry no pts and are never persisted in the event log, exactly like
// typing and status: they are transient pushes. A client that missed the push
// sees the current reaction state on the next getHistory (via messageReactions
// on the message object), so a missed push is not a loss.
func (u *Updater) DeliverReactions(ctx context.Context, ownerID, localID, userID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) == 0 {
		return
	}

	// Load the message for the peer reference.
	msgs, err := u.h.store.MessagesByOwnerLocalIDs(ctx, ownerID, []int64{localID})
	if err != nil {
		u.log.Error("deliver reactions message", "user_id", userID, "local_id", localID, "err", err)
		return
	}
	msg, ok := msgs[localID]
	if !ok {
		return
	}

	// Load current reactions for this message copy.
	reactions, err := u.h.store.ReactionsByOwnerLocal(ctx, ownerID, localID)
	if err != nil {
		u.log.Error("deliver reactions load", "user_id", userID, "local_id", localID, "err", err)
		return
	}

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
	update := &tg.UpdateShort{
		Update: &tg.UpdateMessageReactions{
			Peer:      peerToTL(msg.PeerType, msg.PeerID),
			MsgID:     int(msg.LocalID),
			Reactions: *mr,
		},
		Date: int(time.Now().Unix()),
	}
	for _, c := range conns {
		// Carries no pts, so a conn that changed hands simply drops it.
		if _, err := c.PushTo(ctx, userID, update, 0); err != nil {
			u.log.Info("deliver reactions push", "user_id", userID, "err", err)
		}
	}
}

// wrapUpdates envelopes hydrated updates into a tg.Updates for a live push.
func wrapUpdates(ups []tg.UpdateClass, users []tg.UserClass, chats []tg.ChatClass, state store.State) *tg.Updates {
	return &tg.Updates{
		Updates: ups,
		Users:   users,
		Chats:   chats,
		Date:    state.Date,
		Seq:     state.Seq,
	}
}
