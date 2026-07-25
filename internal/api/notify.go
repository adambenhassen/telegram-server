package api

import (
	"context"
	"log/slog"
	"time"

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

// Deliver pushes userID's not-yet-delivered events to each of its live conns,
// advancing each conn's last-pushed pts. It is best-effort: a push failure is
// logged and the client's next getDifference backfills.
func (u *Updater) Deliver(ctx context.Context, userID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) == 0 {
		return
	}
	for _, c := range conns {
		ups, users, state, _, err := u.h.buildUpdates(ctx, userID, c.LastPushedPts())
		if err != nil {
			u.log.Error("deliver build updates", "user_id", userID, "err", err)
			continue
		}
		if len(ups) == 0 {
			continue
		}
		// Addressed to userID: this snapshot was taken before the batch was
		// built, and the conn's auth key can rebind to another user in between.
		// A push dropped for that reason costs nothing — the user's next poll
		// backfills it.
		if _, err := c.PushTo(ctx, userID, wrapUpdates(ups, users, state), state.Pts); err != nil {
			u.log.Info("deliver push", "user_id", userID, "err", err)
		}
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
