package store

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

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

// Listener owns a dedicated pgx connection running LISTEN and dispatches each
// notification to the delivery callbacks.
type Listener struct {
	conn *pgx.Conn
	log  *slog.Logger
}

// StartListener opens a dedicated connection, subscribes to the update, typing
// and evict channels, and runs the notification loop until the returned stop
// function is called (which cancels the loop, drains it, and closes the
// connection). deliver receives a userID whose events changed; typing receives
// (peerUserID, fromUserID) for a transient typing notification; evict receives
// (userID, authKeyID) for a session revoked on any replica.
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
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("listener connect: %w", err)
	}
	for _, ch := range []string{ChannelUpdates, ChannelTyping, ChannelEvict} {
		// ch is a constant channel identifier, never user input (no injection).
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			_ = conn.Close(ctx) //nolint:errcheck // best-effort close on setup failure
			return nil, nil, fmt.Errorf("listen %s: %w", ch, err)
		}
	}

	l := &Listener{conn: conn, log: log}
	loopCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { l.loop(loopCtx, deliver, typing, evict) })

	stop := func() error {
		cancel()
		wg.Wait()
		return conn.Close(context.Background())
	}
	return l, stop, nil
}

func (l *Listener) loop(
	ctx context.Context,
	deliver func(ctx context.Context, userID int64),
	typing func(ctx context.Context, peerID, fromID int64),
	evict func(ctx context.Context, userID, authKeyID int64),
) {
	for {
		n, err := l.conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // canceled: clean shutdown
			}
			l.log.Error("listener wait notification", "err", err)
			return // connection broken; best-effort delivery ends (getDifference backstops)
		}
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
