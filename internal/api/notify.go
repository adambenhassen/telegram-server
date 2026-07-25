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

// deliver builds one batch from the lowest watermark among conns and gives each
// conn the suffix above its own, so the store work one notification costs does
// not multiply with the number of sockets a user holds. The window is
// contiguous from that lowest watermark, so every conn's events are inside it.
//
// Each conn is told the batch's own pts, and receives every update the batch
// holds above its watermark, so the advertised pts still never runs past an
// update the conn was not given — the invariant a push shares with a poll.
//
// A batch truncated at maxDiffEvents can end below the watermark of a conn that
// was already further ahead. Those conns take another round, whose window
// starts at or past this batch's end: the window advances strictly, so the loop
// runs once in the common case and terminates in all of them.
func (u *Updater) deliver(ctx context.Context, userID int64, conns []pushConn, build func(fromPts int) (updateBatch, error)) {
	for len(conns) > 0 {
		from := conns[0].LastPushedPts()
		for _, c := range conns[1:] {
			from = min(from, c.LastPushedPts())
		}
		b, err := build(from)
		if err != nil {
			u.log.Error("deliver build updates", "user_id", userID, "err", err)
			return
		}
		// Nothing pending for the furthest-behind conn means nothing pending
		// for any of them.
		if len(b.ups) == 0 && !b.more {
			return
		}

		var ahead []pushConn
		for _, c := range conns {
			watermark := c.LastPushedPts()
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
			if _, err := c.PushTo(ctx, userID, wrapUpdates(ups, b.users, b.state), b.state.Pts); err != nil {
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
func wrapUpdates(ups []tg.UpdateClass, users []tg.UserClass, state store.State) *tg.Updates {
	return &tg.Updates{
		Updates: ups,
		Users:   users,
		Date:    state.Date,
		Seq:     state.Seq,
	}
}
