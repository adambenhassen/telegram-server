package api

import (
	"context"
	"log/slog"
	"sort"
	"sync"
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
	h           *handlers
	registry    *mtproto.SessionRegistry
	log         *slog.Logger
	pushMetrics *store.NotificationMetrics
	// pushRecorder is a test-only failure injection seam. Production uses the
	// fixed recorder method through pushMetrics.
	pushRecorder func(store.PushOutcome, time.Time) error
}

// NewUpdater builds an Updater over the store and the server's session registry.
func NewUpdater(s *store.Store, registry *mtproto.SessionRegistry, log *slog.Logger, peers *peerhash.Deriver, pushMetrics ...*store.NotificationMetrics) *Updater {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var metrics *store.NotificationMetrics
	if len(pushMetrics) > 0 {
		metrics = pushMetrics[0]
	}
	return &Updater{
		h:           &handlers{store: s, log: log, peers: peers},
		registry:    registry,
		log:         log,
		pushMetrics: metrics,
	}
}

// pushConn is the part of *mtproto.Conn the fan-out uses: its push watermark
// and the owner-checked write.
type pushConn interface {
	LastPushedPts() int
	PushTo(ctx context.Context, owner int64, enc bin.Encoder, pts int) (bool, error)
}

type orderedPushConn interface {
	PushToAtWatermark(ctx context.Context, owner int64, expectedPts int, enc bin.Encoder, pts int) (pushed, stale bool, err error)
}

type rpcUpdatePushConn interface {
	pushConn
	AuthKeyID() int64
	MarkRPCUpdate(owner, authKeyID int64, pts int) bool
}

type pendingRPCUpdateConn interface {
	PendingRPCUpdate(owner int64) (pts int, pending bool)
}

type pendingRPCReadyConn interface {
	PendingRPCUpdateReady(owner int64) bool
}

// Deliver pushes userID's not-yet-delivered events to each of its live conns,
// advancing each conn's last-pushed pts. It is best-effort: a push failure is
// logged and the client's next getDifference backfills.
func (u *Updater) Deliver(ctx context.Context, userID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) > 0 {
		suppressed, _ := store.SuppressedUpdateFromContext(ctx)
		targets := make([]pushConn, len(conns))
		for i, c := range conns {
			targets[i] = c
		}
		acceptedAt, _ := store.NotificationAcceptedAt(ctx)
		u.deliverAtSuppressed(ctx, userID, targets, func(fromPts int) (updateBatch, error) {
			return u.h.buildUpdates(ctx, userID, fromPts)
		}, acceptedAt, suppressed)
	}
	if channelID, ok := store.ChannelMembershipUpdateFromContext(ctx); ok {
		u.deliverChannelMembership(ctx, userID, channelID)
	}
}

// deliverChannelMembership pushes a newly invited user their own channel peer
// hash. Membership is re-read so a ban or removal committed before delivery
// suppresses the update.
func (u *Updater) deliverChannelMembership(ctx context.Context, userID, channelID int64) {
	conns := u.registry.Conns(userID)
	if len(conns) == 0 {
		return
	}
	channel, found, err := u.h.store.ChannelByID(ctx, channelID)
	if err != nil {
		u.log.Error("deliver channel membership channel", "channel_id", channelID, "user_id", userID, "err", err)
		return
	}
	if !found {
		return
	}
	member, found, err := u.h.store.ChannelMemberOf(ctx, channelID, userID)
	if err != nil {
		u.log.Error("deliver channel membership participant", "channel_id", channelID, "user_id", userID, "err", err)
		return
	}
	if !found || member.Banned(time.Now()) {
		return
	}
	chat, ok := u.h.channelToTL(channel, member, true, userID).(*tg.Channel)
	if !ok {
		return
	}
	env := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: channelID}},
		Chats:   []tg.ChatClass{chat},
		Date:    int(time.Now().Unix()),
		Seq:     0,
	}
	pushes := make([]transientPush, 0, len(conns))
	for _, conn := range conns {
		pushes = append(pushes, transientPush{
			owner: userID,
			conn:  conn,
			enc:   env,
			onError: func(err error) {
				u.log.Info("deliver channel membership", "channel_id", channelID, "user_id", userID, "err", err)
			},
		})
	}
	u.pushTransientFanout(ctx, pushes)
}

// maxDeliveryRounds caps the store round trips one notification may cost. Two
// windows cover the case that matters — a socket that just registered sits at
// watermark 0 while the account's other sockets sit at the head, and a backlog
// past maxDiffEvents puts them a batch apart — without letting watermarks
// spread across many batches turn into one window per socket, which is the
// amplification this path exists to avoid.
const maxDeliveryRounds = 2

const maxDeliveryRetries = 4

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
	u.deliverAt(ctx, userID, conns, build, time.Time{})
}

func (u *Updater) deliverAt(ctx context.Context, userID int64, conns []pushConn, build func(fromPts int) (updateBatch, error), acceptedAt time.Time) {
	u.deliverAtSuppressed(ctx, userID, conns, build, acceptedAt, store.SuppressedUpdate{})
}

func (u *Updater) deliverAtSuppressed(ctx context.Context, userID int64, conns []pushConn, build func(fromPts int) (updateBatch, error), acceptedAt time.Time, suppressed store.SuppressedUpdate) {
	var head int
	retries := 0
	for round := 0; round < maxDeliveryRounds && len(conns) > 0; {
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

		results := make([]deliveryConnResult, len(conns))
		encoders := &deliveryEncoder{}
		var fanout sync.WaitGroup
		for i, c := range conns {
			fanout.Add(1)
			go func(i int, c pushConn) {
				defer fanout.Done()
				results[i] = u.deliverConn(ctx, userID, c, from, b, acceptedAt, suppressed, encoders)
			}(i, c)
		}
		fanout.Wait()

		var ahead []pushConn
		retry := false
		for _, result := range results {
			if result.retry {
				retry = true
			}
			if result.ahead {
				ahead = append(ahead, result.conn)
			}
		}
		if retry {
			retries++
			if retries > maxDeliveryRetries {
				u.log.Error("deliver retries exhausted", "user_id", userID, "retries", retries)
				return
			}
			continue
		}
		retries = 0
		conns = ahead
		round++
	}
}

type deliveryConnResult struct {
	conn  pushConn
	ahead bool
	retry bool
}

type deliveryEncoder struct {
	mu sync.Mutex
}

type synchronizedEncoder struct {
	mu  *sync.Mutex
	enc bin.Encoder
}

func (e synchronizedEncoder) Encode(b *bin.Buffer) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(b)
}

// Transient fan-out uses a fixed worker set so one callback cannot create a
// goroutine per socket or partner. The callback deadline is one socket write
// timeout; sockets still waiting in the bounded queue are left untouched.
const (
	transientFanoutWorkerCount    = 8
	transientFallbackWriteTimeout = 30 * time.Second
)

type transientPush struct {
	owner   int64
	conn    pushConn
	enc     bin.Encoder
	onError func(error)
}

type transientWriteTimeoutConn interface {
	WriteTimeout() time.Duration
}

func transientFanoutTimeout(pushes []transientPush) time.Duration {
	timeout := transientFallbackWriteTimeout
	found := false
	for _, push := range pushes {
		conn, ok := push.conn.(transientWriteTimeoutConn)
		if !ok {
			continue
		}
		writeTimeout := conn.WriteTimeout()
		if writeTimeout <= 0 {
			continue
		}
		if !found || writeTimeout < timeout {
			timeout = writeTimeout
			found = true
		}
	}
	return timeout
}

func (u *Updater) pushTransientFanout(ctx context.Context, pushes []transientPush) {
	if len(pushes) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, transientFanoutTimeout(pushes))
	defer cancel()

	jobs := make(chan transientPush)
	encoders := &deliveryEncoder{}
	workers := min(transientFanoutWorkerCount, len(pushes))
	var fanout sync.WaitGroup
	for range workers {
		fanout.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case push, ok := <-jobs:
					if !ok || ctx.Err() != nil {
						return
					}
					if _, err := u.pushTransientEncoded(ctx, push.owner, push.conn, push.enc, encoders); err != nil && push.onError != nil {
						push.onError(err)
					}
				}
			}
		})
	}

producer:
	for _, push := range pushes {
		select {
		case <-ctx.Done():
			break producer
		case jobs <- push:
		}
	}
	close(jobs)
	fanout.Wait()
}

// deliverConn handles one socket in a delivery window. Sockets for one owner
// are independent write streams, so the caller runs these bounded by the
// registry's per-owner connection cap. The sender barrier and per-connection
// watermark still serialize the ordered work on each individual socket.
func (u *Updater) deliverConn(ctx context.Context, userID int64, conn pushConn, from int, b updateBatch, acceptedAt time.Time, suppressed store.SuppressedUpdate, encoders *deliveryEncoder) deliveryConnResult {
	result := deliveryConnResult{conn: conn}
	watermark := conn.LastPushedPts()
	if watermark < from {
		// This window starts past the conn: the batch is missing the events
		// between the two, so it is not this round's to serve.
		return result
	}
	if watermark >= b.state.Pts {
		result.ahead = b.more
		return result
	}
	ups := b.above(watermark)
	if pendingConn, ok := conn.(pendingRPCUpdateConn); ok {
		pendingPts, pending := pendingConn.PendingRPCUpdate(userID)
		if pending {
			pendingReady := true
			if readyConn, ok := conn.(pendingRPCReadyConn); ok {
				pendingReady = readyConn.PendingRPCUpdateReady(userID)
			}
			pendingRetry := false
			for pending {
				keyedPending := false
				if rpcConn, ok := conn.(rpcUpdatePushConn); ok && pendingPts > 0 && suppressed.AuthKeyID != 0 && suppressed.Pts == pendingPts && suppressed.Pts > watermark && rpcConn.AuthKeyID() == suppressed.AuthKeyID {
					// Let the keyed notification account the sender event after
					// its prefix has been delivered. A non-contiguous RPC result
					// deliberately keeps this barrier active.
					keyedPending = true
				}
				if keyedPending || !pendingReady || pendingPts == 0 || pendingPts > b.state.Pts {
					break
				}

				if rpcConn, ok := conn.(rpcUpdatePushConn); ok {
					if nextWatermark, accounted := accountPendingRPCUpdate(userID, rpcConn, b, watermark, pendingPts); accounted {
						watermark = nextWatermark
						ups = b.above(watermark)
						pendingPts, pending = pendingConn.PendingRPCUpdate(userID)
						if pending {
							if readyConn, ok := conn.(pendingRPCReadyConn); ok {
								pendingReady = readyConn.PendingRPCUpdateReady(userID)
							}
						}
						continue
					}
				}

				if pendingPts > watermark {
					target := sort.SearchInts(b.pts, pendingPts)
					if target < len(b.pts) && b.pts[target] == pendingPts {
						if isRPCResultUpdate(b.ups[target]) {
							if u.deliverBeforePendingRPC(ctx, userID, conn, b, watermark, target, acceptedAt, encoders) {
								pendingRetry = true
								break
							}
							if nextWatermark := conn.LastPushedPts(); nextWatermark > watermark {
								watermark = nextWatermark
								ups = b.above(watermark)
								pendingPts, pending = pendingConn.PendingRPCUpdate(userID)
								if pending {
									if readyConn, ok := conn.(pendingRPCReadyConn); ok {
										pendingReady = readyConn.PendingRPCUpdateReady(userID)
									}
								}
								continue
							}
						}
					}
				}
				break
			}
			if pendingRetry {
				result.retry = true
				return result
			}
			keyedPending := false
			if rpcConn, ok := conn.(rpcUpdatePushConn); ok && pending && pendingPts > 0 && suppressed.AuthKeyID != 0 && suppressed.Pts == pendingPts && suppressed.Pts > watermark && rpcConn.AuthKeyID() == suppressed.AuthKeyID {
				keyedPending = true
			}
			if pending && !keyedPending && (pendingPts == 0 || pendingPts <= b.state.Pts) {
				// Do not put this origin in the next delivery window while
				// its result barrier is active. Advancing past the skipped
				// event would create a pts gap on the wire.
				return result
			}
		}
	}
	if len(ups) == 0 {
		result.ahead = b.more && conn.LastPushedPts() < b.state.Pts
		return result
	}
	if rpcConn, ok := conn.(rpcUpdatePushConn); ok && suppressed.AuthKeyID != 0 && suppressed.Pts > watermark && rpcConn.AuthKeyID() == suppressed.AuthKeyID {
		start := sort.SearchInts(b.pts, watermark+1)
		target := sort.SearchInts(b.pts, suppressed.Pts)
		if target < len(b.pts) && b.pts[target] == suppressed.Pts && target >= start {
			if isRPCResultUpdate(b.ups[target]) {
				if u.deliverWithSuppression(ctx, userID, rpcConn, b, watermark, start, target, suppressed, acceptedAt, encoders) {
					result.retry = true
					return result
				}
				result.ahead = b.more && rpcConn.LastPushedPts() < b.state.Pts
				return result
			}
		}
	}
	// Addressed to userID: this snapshot was taken before the batch was
	// built, and the conn's auth key can rebind to another user in between.
	// A push dropped for that reason costs nothing — the user's next poll
	// backfills it.
	//
	// users covers the whole batch, so a conn taking a suffix gets a
	// superset of the users it needs, which a client ignores.
	pushed, stale, err := u.pushOrdered(ctx, conn, userID, watermark, wrapUpdates(ups, b.users, b.chats, b.state), b.state.Pts, encoders)
	if stale {
		result.retry = true
		return result
	}
	u.recordPushOutcome(acceptedAt, pushed, err)
	if err != nil {
		u.log.Info("deliver push", "user_id", userID, "err", err)
	}
	return result
}

// accountPendingRPCUpdate releases a known sender-result barrier when generic
// delivery has reached the event's immediate prefix. The RPC result already
// delivered that event to this conn, so accounting it advances the watermark
// without writing a duplicate update; the caller then pushes the remaining
// suffix. A barrier farther ahead still needs its prefix pushed first.
func accountPendingRPCUpdate(owner int64, conn rpcUpdatePushConn, b updateBatch, watermark, pendingPts int) (int, bool) {
	if pendingPts <= 0 {
		return watermark, false
	}
	if pendingPts <= watermark {
		if !conn.MarkRPCUpdate(owner, conn.AuthKeyID(), pendingPts) {
			return watermark, false
		}
		return conn.LastPushedPts(), true
	}
	if pendingPts != watermark+1 {
		return watermark, false
	}
	target := sort.SearchInts(b.pts, pendingPts)
	start := sort.SearchInts(b.pts, watermark+1)
	if target >= len(b.pts) || b.pts[target] != pendingPts || target < start {
		return watermark, false
	}
	if !isRPCResultUpdate(b.ups[target]) {
		return watermark, false
	}
	if !conn.MarkRPCUpdate(owner, conn.AuthKeyID(), pendingPts) {
		return watermark, false
	}
	return conn.LastPushedPts(), true
}

func isRPCResultUpdate(up tg.UpdateClass) bool {
	switch up.(type) {
	case *tg.UpdateNewMessage, *tg.UpdateEditMessage:
		return true
	default:
		return false
	}
}

func (u *Updater) pushOrdered(ctx context.Context, conn pushConn, owner int64, expectedPts int, enc bin.Encoder, pts int, encoders *deliveryEncoder) (pushed, stale bool, err error) {
	if _, ok := conn.(*mtproto.Conn); ok && encoders != nil {
		enc = synchronizedEncoder{mu: &encoders.mu, enc: enc}
	}
	if guarded, ok := conn.(orderedPushConn); ok {
		pushed, stale, err = guarded.PushToAtWatermark(ctx, owner, expectedPts, enc, pts)
	} else {
		pushed, err = conn.PushTo(ctx, owner, enc, pts)
	}
	u.handlePushFailure(owner, conn, err)
	return pushed, stale, err
}

func (u *Updater) handlePushFailure(owner int64, conn pushConn, err error) {
	if err == nil || mtproto.IsPushEncodeError(err) || mtproto.IsPushNotAttempted(err) || u.registry == nil {
		return
	}
	c, ok := conn.(*mtproto.Conn)
	if !ok {
		return
	}
	// Remove before closing so a queued notification cannot take another
	// write-deadline hold while the serve goroutine is unwinding the socket.
	u.registry.Remove(owner, c)
	if closeErr := c.Close(); closeErr != nil {
		u.log.Info("close failed push connection", "user_id", owner, "err", closeErr)
	}
}

func (u *Updater) pushTransientEncoded(ctx context.Context, owner int64, conn pushConn, enc bin.Encoder, encoders *deliveryEncoder) (bool, error) {
	if _, ok := conn.(*mtproto.Conn); ok && encoders != nil {
		enc = synchronizedEncoder{mu: &encoders.mu, enc: enc}
	}
	pushed, err := conn.PushTo(ctx, owner, enc, 0)
	u.handlePushFailure(owner, conn, err)
	return pushed, err
}

func (u *Updater) deliverBeforePendingRPC(ctx context.Context, userID int64, conn pushConn, b updateBatch, watermark, target int, acceptedAt time.Time, encoders *deliveryEncoder) bool {
	start := sort.SearchInts(b.pts, watermark+1)
	if target <= start {
		return false
	}
	prefixPts := b.pts[target-1]
	pushed, stale, err := u.pushOrdered(ctx, conn, userID, watermark, wrapUpdates(b.ups[start:target], b.users, b.chats, b.state), prefixPts, encoders)
	if stale {
		return true
	}
	u.recordPushOutcome(acceptedAt, pushed, err)
	if err != nil {
		u.log.Info("deliver push before pending RPC update", "user_id", userID, "err", err)
	}
	return false
}

func (u *Updater) deliverWithSuppression(ctx context.Context, userID int64, conn rpcUpdatePushConn, b updateBatch, watermark, start, target int, suppressed store.SuppressedUpdate, acceptedAt time.Time, encoders *deliveryEncoder) bool {
	if target > start {
		prefixPts := b.pts[target-1]
		pushed, stale, err := u.pushOrdered(ctx, conn, userID, watermark, wrapUpdates(b.ups[start:target], b.users, b.chats, b.state), prefixPts, encoders)
		if stale {
			return true
		}
		u.recordPushOutcome(acceptedAt, pushed, err)
		if err != nil {
			u.log.Info("deliver push before RPC update", "user_id", userID, "err", err)
			return true
		}
		if !pushed {
			return false
		}
	}
	if !conn.MarkRPCUpdate(userID, suppressed.AuthKeyID, suppressed.Pts) {
		return false
	}
	suffixStart := target + 1
	suffixEnd := len(b.ups)
	if pendingConn, ok := conn.(pendingRPCUpdateConn); ok {
		if nextPts, pending := pendingConn.PendingRPCUpdate(userID); pending && nextPts > suppressed.Pts {
			nextTarget := sort.SearchInts(b.pts, nextPts)
			if nextTarget < suffixEnd && nextTarget >= suffixStart {
				suffixEnd = nextTarget
			}
		}
	}
	if suffixStart >= suffixEnd {
		return false
	}
	pushed, stale, err := u.pushOrdered(ctx, conn, userID, suppressed.Pts, wrapUpdates(b.ups[suffixStart:suffixEnd], b.users, b.chats, b.state), b.pts[suffixEnd-1], encoders)
	if stale {
		return true
	}
	u.recordPushOutcome(acceptedAt, pushed, err)
	if err != nil {
		u.log.Info("deliver push after RPC update", "user_id", userID, "err", err)
	}
	return false
}

func (u *Updater) recordPushOutcome(acceptedAt time.Time, pushed bool, err error) {
	if (u.pushMetrics == nil && u.pushRecorder == nil) || acceptedAt.IsZero() {
		return
	}
	var outcome store.PushOutcome
	switch {
	case err != nil && mtproto.IsPushEncodeError(err):
		outcome = store.PushOutcomeEncodeFailure
	case err != nil:
		outcome = store.PushOutcomeWriteFailure
	case !pushed:
		outcome = store.PushOutcomeOwnerMismatch
	default:
		outcome = store.PushOutcomeSuccess
	}
	var failed bool
	if u.pushRecorder != nil {
		failed = store.InvokeRecorder(func() error { return u.pushRecorder(outcome, acceptedAt) })
	} else {
		failed = store.InvokeRecorder(func() error {
			return u.pushMetrics.RecordPushOutcomeResult(outcome, acceptedAt)
		})
	}
	if failed {
		store.ReportRecorderFailure(u.log, u.pushMetrics, store.RecorderFailurePushOutcome)
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
	pushes := make([]transientPush, 0, len(conns))
	for _, c := range conns {
		pushes = append(pushes, transientPush{
			owner: peerID,
			conn:  c,
			enc:   short,
			onError: func(err error) {
				u.log.Info("deliver typing", "peer_id", peerID, "err", err)
			},
		})
	}
	u.pushTransientFanout(ctx, pushes)
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
// The callback runs on one bounded listener worker; all pushes for this channel
// event are inline. The number of members with live conns on this replica is
// typically small (the replica serves a fraction of all members), and the
// scheduler keeps a blocked channel key from stalling unrelated notifications.
func (u *Updater) deliverChannel(
	ctx context.Context,
	members []store.ChannelMember,
	now time.Time,
	connsFor func(userID int64) []pushConn,
	build func(memberID int64, fromPts int) (channelBatch, error),
) {
	var pushes []transientPush
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
		memberID := m.UserID
		for _, c := range conns {
			// Pass pts=0: this is a channel update; the conn's lastPushedPts
			// tracks the per-account stream and must not be corrupted by a
			// channel pts value.
			pushes = append(pushes, transientPush{
				owner: memberID,
				conn:  c,
				enc:   env,
				onError: func(err error) {
					u.log.Info("deliver channel push", "user_id", memberID, "err", err)
				},
			})
		}
	}
	u.pushTransientFanout(ctx, pushes)
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
	pushes := make([]transientPush, 0, len(conns))
	for _, c := range conns {
		pushes = append(pushes, transientPush{
			owner: userID,
			conn:  c,
			enc:   short,
			onError: func(err error) {
				u.log.Info("deliver encryption", "user_id", userID, "err", err)
			},
		})
	}
	u.pushTransientFanout(ctx, pushes)
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

	var pushes []transientPush
	for _, partnerID := range partners {
		conns := u.registry.Conns(partnerID)
		if len(conns) == 0 {
			continue
		}
		short := &tg.UpdateShort{
			Update: &tg.UpdateUserStatus{UserID: userID, Status: status},
			Date:   int(time.Now().Unix()),
		}
		for _, c := range conns {
			partner := partnerID
			pushes = append(pushes, transientPush{
				owner: partner,
				conn:  c,
				enc:   short,
				onError: func(err error) {
					u.log.Info("deliver status push", "partner_id", partner, "err", err)
				},
			})
		}
	}
	u.pushTransientFanout(ctx, pushes)
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
	pushes := make([]transientPush, 0, len(conns))
	for _, c := range conns {
		pushes = append(pushes, transientPush{
			owner: recipientID,
			conn:  c,
			enc:   update,
			onError: func(err error) {
				u.log.Info("deliver encrypted msg push", "recipient_id", recipientID, "err", err)
			},
		})
	}
	u.pushTransientFanout(ctx, pushes)
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
	// A copy its owner soft-deleted is gone from every read surface they have,
	// so pushing a reaction on it would name a message they cannot open. The
	// reaction row outlives the delete and is deliberately left in place; only
	// this owner's push is dropped, and the other parties still get theirs.
	if !ok || msg.Deleted {
		return
	}

	// Load current reactions for this message copy.
	reactions, err := u.h.store.ReactionsByOwnerLocal(ctx, ownerID, localID)
	if err != nil {
		u.log.Error("deliver reactions load", "user_id", userID, "local_id", localID, "err", err)
		return
	}

	update := &tg.UpdateShort{
		Update: &tg.UpdateMessageReactions{
			Peer:      peerToTL(msg.PeerType, msg.PeerID),
			MsgID:     int(msg.LocalID),
			Reactions: reactionsToTL(reactions),
		},
		Date: int(time.Now().Unix()),
	}
	pushes := make([]transientPush, 0, len(conns))
	for _, c := range conns {
		pushes = append(pushes, transientPush{
			owner: userID,
			conn:  c,
			enc:   update,
			onError: func(err error) {
				u.log.Info("deliver reactions push", "user_id", userID, "err", err)
			},
		})
	}
	u.pushTransientFanout(ctx, pushes)
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

// DeliverPinned pushes updatePinnedMessages to every member of peerID that
// holds a live connection on this replica. It is the pinned callback for
// StartListener.
//
// The peerID can be a chat id or a channel id. Members are resolved from the
// store, and a tg.UpdatePinnedMessages is pushed to each member with live
// connections. The update carries no pts (transient push, same model as
// reactions).
//
// The pinned state is reloaded from the store rather than trusted from the
// payload: a concurrent pin/unpin between commit and Notify could leave the
// payload carrying a stale message id. Reloading ensures the push reflects
// the authoritative committed state.
func (u *Updater) DeliverPinned(ctx context.Context, peerType store.PeerType, peerID int64, _ int32) {
	var members []int64
	var peer tg.PeerClass
	var pinnedMsgID int32

	switch peerType {
	case store.PeerTypeChat:
		chatMembers, err := u.h.store.Participants(ctx, peerID)
		if err != nil {
			u.log.Error("deliver pinned chat lookup", "peer_id", peerID, "err", err)
			return
		}
		members = make([]int64, len(chatMembers))
		for i, p := range chatMembers {
			members[i] = p.UserID
		}
		peer = &tg.PeerChat{ChatID: peerID}

		// Reload authoritative pinned state to avoid stale payload.
		if id, err := u.h.store.ChatPinnedMessage(ctx, peerID); err != nil {
			u.log.Error("deliver pinned chat reload", "peer_id", peerID, "err", err)
			return
		} else if id != nil {
			pinnedMsgID = *id
		}

	case store.PeerTypeChannel:
		chMembers, err := u.h.store.ChannelMembers(ctx, peerID)
		if err != nil {
			u.log.Error("deliver pinned channel lookup", "peer_id", peerID, "err", err)
			return
		}
		members = make([]int64, len(chMembers))
		for i, m := range chMembers {
			members[i] = m.UserID
		}
		peer = &tg.PeerChannel{ChannelID: peerID}

		// Reload authoritative pinned state to avoid stale payload.
		if id, err := u.h.store.ChannelPinnedMessage(ctx, peerID); err != nil {
			u.log.Error("deliver pinned channel reload", "peer_id", peerID, "err", err)
			return
		} else if id != nil {
			pinnedMsgID = *id
		}

	default:
		u.log.Warn("deliver pinned unknown peer type", "peer_type", peerType)
		return
	}

	u.deliverPinnedToUsers(ctx, peer, members, pinnedMsgID)
}

// deliverPinnedToUsers pushes the pinned update to each member with live conns.
// pinnedMsgID is nonzero on pin, zero on unpin.
func (u *Updater) deliverPinnedToUsers(ctx context.Context, peer tg.PeerClass, members []int64, pinnedMsgID int32) {
	var pushes []transientPush
	for _, memberID := range members {
		conns := u.registry.Conns(memberID)
		if len(conns) == 0 {
			continue
		}
		var messages []int
		if pinnedMsgID != 0 {
			messages = []int{int(pinnedMsgID)}
		}
		update := &tg.UpdateShort{
			Update: &tg.UpdatePinnedMessages{
				Pinned:   pinnedMsgID != 0,
				Peer:     peer,
				Messages: messages,
			},
			Date: int(time.Now().Unix()),
		}
		for _, c := range conns {
			member := memberID
			pushes = append(pushes, transientPush{
				owner: member,
				conn:  c,
				enc:   update,
				onError: func(err error) {
					u.log.Info("deliver pinned push", "user_id", member, "err", err)
				},
			})
		}
	}
	u.pushTransientFanout(ctx, pushes)
}
