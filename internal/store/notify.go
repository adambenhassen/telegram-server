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
	ChannelUpdates = "tg_updates"      // payload: "<userID>"
	ChannelTyping  = "tg_typing"       // payload: "<peerUserID>|<fromUserID>"
	ChannelEvict   = "tg_evict"        // payload: "<userID>|<authKeyID>"
	ChannelPost    = "tg_channel_post" // payload: "<channelID>"
	// ChannelEncryption carries a secret-chat state change to one party.
	// payload: "<userID>|<chatID>". It is transient like ChannelTyping: no pts
	// is spent on key exchange, so nothing backfills a missed one until the qts
	// stream lands (MAIN-140).
	ChannelEncryption = "tg_encryption" // payload: "<userID>|<chatID>"
	ChannelStatus     = "tg_status"     // payload: "<userID>|<1 or 0>"
	// ChannelEncryptedMsg carries one encrypted message event to its recipient.
	// payload: "<recipientID>|<qts>". The handler fetches the full event from
	// encrypted_events and pushes updateNewEncryptedMessage.
	ChannelEncryptedMsg = "tg_encrypted_msg" // payload: "<recipientID>|<qts>"
	// ChannelReactions carries a reaction change to all parties holding a message.
	// payload: "<userID>". The handler pushes updateMessageReactions (transient,
	// no pts, same model as updateUserStatus).
	ChannelReactions = "tg_reactions" // payload: "<userID>"
)

// Notify emits a Postgres NOTIFY on channel with payload. It is the cross-replica
// nudge that wakes each process's Listener after an event transaction commits.
func (s *Store) Notify(ctx context.Context, channel, payload string) error {
	if _, err := s.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload); err != nil {
		return fmt.Errorf("notify %s: %w", channel, err)
	}
	return nil
}

// Reconnect pacing for the listener loop. A database that stays down, or one
// that flaps, is retried on a growing delay and never in a tight loop.
const (
	listenerBackoffMin = 100 * time.Millisecond
	listenerBackoffMax = 30 * time.Second
	// listenerStableFor is how long a connection must survive for its failure
	// to count as a fresh one. Below it the database is still unhealthy, so the
	// delay keeps growing; above it the loop starts over at the floor, whether
	// or not that connection ever carried a notification.
	listenerStableFor = 30 * time.Second
)

// nextBackoff returns the delay before the next reconnect attempt: the floor
// when the connection that just failed had been up long enough to count as
// healthy, otherwise the previous delay doubled up to the cap. uptime is zero
// for an attempt that never connected.
func nextBackoff(prev, uptime time.Duration) time.Duration {
	if uptime >= listenerStableFor {
		return listenerBackoffMin
	}
	return min(2*prev, listenerBackoffMax)
}

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

// StartListener opens a dedicated connection, subscribes to the update, typing,
// evict, channel-post, encryption, status, encrypted-message, and reactions
// channels, and runs the notification loop until the returned stop function is
// called (which cancels the loop, drains it, and returns the error from closing
// the connection). deliver receives a userID whose events changed; typing
// receives (peerUserID, fromUserID) for a transient typing notification; evict
// receives (userID, authKeyID) for a session revoked on any replica; channelPost
// receives a channelID whose post was just committed; encryption receives
// (userID, chatID) for a secret-chat state change; status receives (userID,
// online) for a status change; encryptedMsg receives (recipientID, qts) for a
// secret-chat message; reactions receives (ownerID, localID, userID) for a
// reaction change on a specific message copy.
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
	channelPost func(ctx context.Context, channelID int64),
	encryption func(ctx context.Context, userID, chatID int64),
	status func(ctx context.Context, userID int64, online bool),
	encryptedMsg func(ctx context.Context, recipientID int64, qts int),
	reactions func(ctx context.Context, ownerID, localID, userID int64),
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
	wg.Go(func() {
		l.run(loopCtx, conn, dsn, deliver, typing, evict, channelPost, encryption, status, encryptedMsg, reactions)
	})

	stop := func() error {
		cancel()
		wg.Wait()
		return l.closeErr
	}
	return l, stop, nil
}

// connectAndListen opens a connection subscribed to every channel. They are
// always subscribed together: a connection carrying only some of them silently
// drops a whole class of delivery.
func connectAndListen(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("listener connect: %w", err)
	}
	for _, ch := range []string{ChannelUpdates, ChannelTyping, ChannelEvict, ChannelPost, ChannelEncryption, ChannelStatus, ChannelEncryptedMsg, ChannelReactions} {
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
	channelPost func(ctx context.Context, channelID int64),
	encryption func(ctx context.Context, userID, chatID int64),
	status func(ctx context.Context, userID int64, online bool),
	encryptedMsg func(ctx context.Context, recipientID int64, qts int),
	reactions func(ctx context.Context, ownerID, localID, userID int64),
) {
	backoff := listenerBackoffMin
	for {
		if conn == nil {
			if !sleepCtx(ctx, backoff) {
				return // canceled mid-backoff: nothing open to close
			}
			c, err := connectAndListen(ctx, dsn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				backoff = nextBackoff(backoff, 0)
				l.log.Error("listener reconnect", "err", err, "retry_in", backoff)
				continue
			}
			l.log.Info("listener reconnected")
			conn = c
		}

		up := time.Now()
		err := l.dispatch(ctx, conn, deliver, typing, evict, channelPost, encryption, status, encryptedMsg, reactions)
		closeErr := conn.Close(context.Background())
		conn = nil
		if ctx.Err() != nil {
			l.closeErr = closeErr
			return // canceled: clean shutdown
		}
		backoff = nextBackoff(backoff, time.Since(up))
		l.log.Error("listener connection lost", "err", err, "retry_in", backoff)
	}
}

// dispatch consumes notifications on conn until ctx is canceled or the
// connection fails, returning the error that ended it.
func (l *Listener) dispatch(
	ctx context.Context,
	conn *pgx.Conn,
	deliver func(ctx context.Context, userID int64),
	typing func(ctx context.Context, peerID, fromID int64),
	evict func(ctx context.Context, userID, authKeyID int64),
	channelPost func(ctx context.Context, channelID int64),
	encryption func(ctx context.Context, userID, chatID int64),
	status func(ctx context.Context, userID int64, online bool),
	encryptedMsg func(ctx context.Context, recipientID int64, qts int),
	reactions func(ctx context.Context, ownerID, localID, userID int64),
) error {
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
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
		case ChannelPost:
			channelID, perr := strconv.ParseInt(n.Payload, 10, 64)
			if perr != nil {
				l.log.Warn("bad tg_channel_post payload", "payload", n.Payload)
				continue
			}
			channelPost(ctx, channelID)
		case ChannelEncryption:
			userID, chatID, perr := parsePairPayload(n.Payload)
			if perr != nil {
				l.log.Warn("bad tg_encryption payload", "payload", n.Payload)
				continue
			}
			encryption(ctx, userID, chatID)
		case ChannelStatus:
			userID, onlineID, perr := parsePairPayload(n.Payload)
			if perr != nil {
				l.log.Warn("bad tg_status payload", "payload", n.Payload)
				continue
			}
			status(ctx, userID, onlineID == 1)
		case ChannelEncryptedMsg:
			recipientID, qts64, perr := parsePairPayload(n.Payload)
			if perr != nil {
				l.log.Warn("bad tg_encrypted_msg payload", "payload", n.Payload)
				continue
			}
			encryptedMsg(ctx, recipientID, int(qts64))
		case ChannelReactions:
			opts, perr := parseReactionPayload(n.Payload)
			if perr != nil {
				l.log.Warn("bad tg_reactions payload", "payload", n.Payload)
				continue
			}
			reactions(ctx, opts.ownerID, opts.localID, opts.userID)
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

// ChannelPostPayload formats a tg_channel_post NOTIFY payload from channelID.
func ChannelPostPayload(channelID int64) string {
	return strconv.FormatInt(channelID, 10)
}

// EncryptionPayload formats a tg_encryption NOTIFY payload naming the party to
// notify and the secret chat whose state changed.
func EncryptionPayload(userID, chatID int64) string {
	return pairPayload(userID, chatID)
}

// EncryptedMsgPayload formats a tg_encrypted_msg NOTIFY payload naming the
// recipient and the qts of the event they must push.
func EncryptedMsgPayload(recipientID int64, qts int) string {
	return pairPayload(recipientID, int64(qts))
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

// StatusPayload formats a tg_status NOTIFY payload. The second id is 1 for
// online, 0 for offline.
func StatusPayload(userID int64, online bool) string {
	second := "0"
	if online {
		second = "1"
	}
	return strconv.FormatInt(userID, 10) + "|" + second
}

// ReactionPayload formats a tg_reactions NOTIFY payload carrying the
// owner, local message id, and the target user to push to.
// Payload: "ownerID|localID|userID".
func ReactionPayload(ownerID, localID, userID int64) string {
	return strconv.FormatInt(ownerID, 10) + "|" +
		strconv.FormatInt(localID, 10) + "|" +
		strconv.FormatInt(userID, 10)
}

// reactionPayload carries the parsed fields of a tg_reactions NOTIFY payload.
type reactionPayload struct {
	ownerID int64
	localID int64
	userID  int64
}

// parseReactionPayload decodes a tg_reactions NOTIFY payload into its three
// components: ownerID, localID, and userID.
func parseReactionPayload(payload string) (reactionPayload, error) {
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		return reactionPayload{}, fmt.Errorf("malformed reaction payload %q", payload)
	}
	ownerID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return reactionPayload{}, err
	}
	localID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return reactionPayload{}, err
	}
	userID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return reactionPayload{}, err
	}
	return reactionPayload{ownerID: ownerID, localID: localID, userID: userID}, nil
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

// WaitForNotificationListener blocks until n backends in the current database
// are parked on a ClientRead wait — the pg_stat_activity signature of a
// connection blocked in WaitForNotification. Tests that emit NOTIFY must
// confirm the listener is actually reading first; otherwise the notification
// races the initial WaitForNotification call and is lost.
func WaitForNotificationListener(ctx context.Context, s *Store, n int) error {
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		var got int
		err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database()
			   AND wait_event_type = 'Client'
			   AND wait_event = 'ClientRead'`).Scan(&got)
		if err != nil {
			return err
		}
		if got >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waited %s for %d notification listeners, saw %d", timeout, n, got)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
