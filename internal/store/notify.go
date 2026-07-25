package store

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Postgres LISTEN/NOTIFY channels used for cross-replica update delivery.
const (
	ChannelUpdates = "tg_updates" // payload: "<userID>"
	ChannelTyping  = "tg_typing"  // payload: "<peerUserID>|<fromUserID>"
	ChannelEvict   = "tg_evict"   // payload: "<userID>|<authKeyID>"
)

// Notify emits a Postgres NOTIFY on channel with payload. It is the cross-replica
// nudge that wakes each process's Listener after an event transaction commits.
func (s *Store) Notify(ctx context.Context, channel, payload string) error {
	if _, err := s.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload); err != nil {
		return fmt.Errorf("notify %s: %w", channel, err)
	}
	return nil
}

// Reconnect backoff bounds for the listener loop. A database that stays down is
// retried on a growing delay, never in a tight loop.
const (
	listenerBackoffMin = 100 * time.Millisecond
	listenerBackoffMax = 30 * time.Second
)

// Listener runs LISTEN on a dedicated pgx connection and dispatches each
// notification to the delivery callbacks, reconnecting when that connection
// breaks.
type Listener struct {
	log *slog.Logger

	// closeErr is the loop's final connection close error. The loop goroutine
	// writes it before returning and stop reads it after wg.Wait, so the
	// WaitGroup orders the two accesses: the connection needs no lock because
	// only the loop goroutine ever touches it.
	closeErr error
}

// StartListener opens a dedicated connection, subscribes to the update, typing
// and evict channels, and runs the notification loop until the returned stop
// function is called (which cancels the loop, drains it, and returns the error
// from closing the connection). deliver receives a userID whose events changed;
// typing receives (peerUserID, fromUserID) for a transient typing notification;
// evict receives (userID, authKeyID) for a session revoked on any replica.
//
// A broken connection is reconnected with bounded backoff rather than ending
// delivery for the life of the process. Notifications emitted while the
// listener is reconnecting are lost, which push already tolerates: the client's
// next getDifference backfills them.
//
// The callbacks run on this one goroutine, so none of them may block: a stalled
// callback holds up every other user's delivery.
func StartListener(
	ctx context.Context,
	dsn string,
	deliver func(ctx context.Context, userID int64),
	typing func(ctx context.Context, peerID, fromID int64),
	evict func(ctx context.Context, userID, authKeyID int64),
	log *slog.Logger,
) (*Listener, func() error, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	conn, err := connectAndListen(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}

	l := &Listener{log: log}
	loopCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { l.run(loopCtx, conn, dsn, deliver, typing, evict) })

	stop := func() error {
		cancel()
		wg.Wait()
		return l.closeErr
	}
	return l, stop, nil
}

// connectAndListen opens a connection subscribed to every channel. The three
// are always subscribed together: a connection carrying only some of them
// silently drops a whole class of delivery.
func connectAndListen(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("listener connect: %w", err)
	}
	for _, ch := range []string{ChannelUpdates, ChannelTyping, ChannelEvict} {
		// ch is a constant channel identifier, never user input (no injection).
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			_ = conn.Close(ctx) //nolint:errcheck // best-effort close on setup failure
			return nil, fmt.Errorf("listen %s: %w", ch, err)
		}
	}
	return conn, nil
}

// run dispatches notifications until ctx is canceled, reconnecting whenever the
// connection breaks. The loop goroutine owns conn exclusively — it is the only
// thing that replaces or closes it — so a reconnect needs no lock and no lock
// ordering against writeMu or the session registry.
func (l *Listener) run(
	ctx context.Context,
	conn *pgx.Conn,
	dsn string,
	deliver func(ctx context.Context, userID int64),
	typing func(ctx context.Context, peerID, fromID int64),
	evict func(ctx context.Context, userID, authKeyID int64),
) {
	backoff := listenerBackoffMin
	for {
		if conn == nil {
			if !sleepCtx(ctx, backoff) {
				return // canceled mid-backoff: nothing open to close
			}
			backoff = min(2*backoff, listenerBackoffMax)
			c, err := connectAndListen(ctx, dsn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				l.log.Error("listener reconnect", "err", err, "retry_in", backoff)
				continue
			}
			l.log.Info("listener reconnected")
			conn = c
		}

		dispatched, err := l.dispatch(ctx, conn, deliver, typing, evict)
		closeErr := conn.Close(context.Background())
		conn = nil
		if ctx.Err() != nil {
			l.closeErr = closeErr
			return // canceled: clean shutdown
		}
		l.log.Error("listener connection lost", "err", err)
		if dispatched {
			// The connection carried traffic before breaking, so this is a
			// fresh failure rather than a database that never came back.
			backoff = listenerBackoffMin
		}
	}
}

// dispatch consumes notifications on conn until ctx is canceled or the
// connection fails, reporting whether any notification arrived on it.
func (l *Listener) dispatch(
	ctx context.Context,
	conn *pgx.Conn,
	deliver func(ctx context.Context, userID int64),
	typing func(ctx context.Context, peerID, fromID int64),
	evict func(ctx context.Context, userID, authKeyID int64),
) (dispatched bool, err error) {
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return dispatched, err
		}
		dispatched = true
		switch n.Channel {
		case ChannelUpdates:
			userID, perr := strconv.ParseInt(n.Payload, 10, 64)
			if perr != nil {
				l.log.Warn("bad tg_updates payload", "payload", n.Payload)
				continue
			}
			deliver(ctx, userID)
		case ChannelTyping:
			peerID, fromID, perr := parsePairPayload(n.Payload)
			if perr != nil {
				l.log.Warn("bad tg_typing payload", "payload", n.Payload)
				continue
			}
			typing(ctx, peerID, fromID)
		case ChannelEvict:
			userID, authKeyID, perr := parsePairPayload(n.Payload)
			if perr != nil {
				// Dropped, never widened: an evict that cannot be read must not
				// escalate into closing every connection of some user.
				l.log.Warn("bad tg_evict payload", "payload", n.Payload)
				continue
			}
			evict(ctx, userID, authKeyID)
		}
	}
}

// sleepCtx waits d, reporting false if ctx ended first. Shutdown must not have
// to wait out a backoff.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// TypingPayload formats a tg_typing NOTIFY payload from the peer and sender ids.
func TypingPayload(peerID, fromID int64) string {
	return pairPayload(peerID, fromID)
}

// EvictPayload formats a tg_evict NOTIFY payload naming the revoked session: the
// user the deleted auth key was bound to, and the auth key id itself.
func EvictPayload(userID, authKeyID int64) string {
	return pairPayload(userID, authKeyID)
}

// pairPayload encodes the two-id payload shape shared by the pair channels.
func pairPayload(first, second int64) string {
	return strconv.FormatInt(first, 10) + "|" + strconv.FormatInt(second, 10)
}

func parsePairPayload(payload string) (first, second int64, err error) {
	firstStr, secondStr, ok := strings.Cut(payload, "|")
	if !ok {
		return 0, 0, fmt.Errorf("malformed pair payload %q", payload)
	}
	first, err = strconv.ParseInt(firstStr, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	second, err = strconv.ParseInt(secondStr, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return first, second, nil
}
