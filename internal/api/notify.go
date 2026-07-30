package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
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
func NewUpdater(s *store.Store, registry *mtproto.SessionRegistry, log *slog.Logger) *Updater {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Updater{
		h:        &handlers{store: s, log: log},
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
// Delivery cost is bounded: one ChannelMembers query, then for each non-banned
// member O(1) in-memory registry lookups, and only then one ChannelState query
// and one buildChannelUpdates call per member with a live conn. A replica where
// nobody is home for the channel skips the state query entirely.
//
// The per-connection channel-pts watermark does not exist on Conn. Push from
// each member's JoinPts floor instead. A duplicate UpdateNewChannelMessage is
// safe (the client dedups on pts); a missed one is backfilled by the next
// getChannelDifference. This is the accepted trade-off until per-channel
// per-conn watermarks are added to the registry.
func (u *Updater) DeliverChannelPost(ctx context.Context, channelID int64) {
	members, err := u.h.store.ChannelMembers(ctx, channelID)
	if err != nil {
		u.log.Error("deliver channel post members", "channel_id", channelID, "err", err)
		return
	}
	// Fast pre-check against the in-memory registry: if no non-banned member
	// has a live conn here, skip the state query entirely. On a replica serving
	// 2 of a 10 000-member channel, this is 10 000 in-memory lookups and then
	// one state query, not 10 000 DB round-trips.
	now := time.Now()
	anyActive := false
	for _, m := range members {
		if !m.Banned(now) && len(u.registry.Conns(m.UserID)) > 0 {
			anyActive = true
			break
		}
	}
	if !anyActive {
		return
	}
	currentPts, err := u.h.store.ChannelState(ctx, channelID)
	if err != nil {
		u.log.Error("deliver channel post state", "channel_id", channelID, "err", err)
		return
	}
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
			return u.h.buildChannelUpdates(ctx, channelID, memberID, fromPts, maxDiffEvents, currentPts)
		},
	)
}

// deliverChannel is the testable core of DeliverChannelPost. For each
// non-banned member that has live conns, it calls build once (from the member's
// JoinPts floor) and pushes the resulting updates to each conn.
//
// The callback runs on the listener's single goroutine; it must not block. All
// pushes are inline. The bound that makes this acceptable: the number of members
// with live conns on this replica is typically small (the replica serves a
// fraction of all members), so the number of build calls is small. If a channel
// grows so large that even the active-member fraction exceeds a safe inline
// limit, the callback would need to hand work to a worker pool instead.
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
