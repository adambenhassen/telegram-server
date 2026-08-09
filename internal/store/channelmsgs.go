package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ChannelEvent is one entry from the per-channel ordered event log. It mirrors
// Event: channel_events.type uses the same 1 new / 2 edit / 3 delete codes as
// message_events, so EventType carries both.
type ChannelEvent struct {
	Pts     int
	Type    EventType
	LocalID int64
}

// ChannelMessage is a persisted channel post. Unlike Message there is one row
// per channel rather than one per member, so it carries no owner and no Out.
// FileID is nil for "no media" — channel_messages.file_id is a nullable FK,
// where the older messages.file_id uses 0 as that sentinel.
//
// Field order is load-bearing: channelMessageFromFields constructs this type
// with a positional literal, so the field sequence here is the column mapping
// contract. Do not reorder fields without updating that function.
type ChannelMessage struct {
	ChannelID int64
	LocalID   int64
	FromID    int64
	Date      time.Time
	Message   string
	EditDate  *time.Time
	Deleted   bool
	RandomID  int64
	FileID    *int64
}

// channelMsgFields is a layout-identical copy of the four sqlc channel-message
// row types. Any of them converts to this type via a plain type conversion, so
// the single channelMessageFromFields function below is the only place that maps
// database columns to ChannelMessage fields.
type channelMsgFields struct {
	ChannelID int64
	LocalID   int64
	FromID    int64
	Date      pgtype.Timestamptz
	Message   string
	EditDate  pgtype.Timestamptz
	Deleted   bool
	RandomID  int64
	FileID    *int64
}

// channelMessageFromFields is the sole row-to-struct converter for channel
// messages. The positional ChannelMessage literal is intentional: it causes a
// compile error if a new field is added to ChannelMessage without being wired
// in here, rather than silently leaving it at its zero value.
func channelMessageFromFields(r channelMsgFields) ChannelMessage {
	var editDate *time.Time
	if r.EditDate.Valid {
		t := r.EditDate.Time
		editDate = &t
	}
	return ChannelMessage{
		r.ChannelID,
		r.LocalID,
		r.FromID,
		r.Date.Time,
		r.Message,
		editDate,
		r.Deleted,
		r.RandomID,
		r.FileID,
	}
}

// ChannelState returns the channel's current pts. A channel that has never been
// posted to has no channel_state row yet and reports 0, so a difference against
// it works from the moment the channel exists.
//
// A channel id that does not exist reports 0 as well: this is not an existence
// check and cannot be used as one. Callers gate on the channel and on the
// caller's membership of it before they get here, as everywhere else in this
// package.
func (s *Store) ChannelState(ctx context.Context, channelID int64) (int, error) {
	row, err := s.q.GetChannelState(ctx, channelID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get channel state: %w", err)
	}
	return int(row.Pts), nil
}

// PostChannelMessage appends one post to the channel: the channel_state bump
// that allocates its local_id and pts, the channel_messages row and the
// channel_events row, all in one transaction. Keeping the event and the bump in
// one transaction is what makes the log's contract hold — an event row present
// implies channel_state.pts is at least that event's pts.
//
// A repeat carrying the same non-zero randomID returns the already-stored post
// with dup == true, writing nothing and leaving pts where it was.
//
// This layer trusts its caller: it does not check that fromID may post here.
//
// Locking: no Go-level lock and no advisory lock, only the channel_state row
// lock, taken by the LockChannelState read ahead of the dedup lookup and held to
// commit. That is the ordering every future channel write inherits — the
// channel's own row first, before any channel_messages or channel_events write.
func (s *Store) PostChannelMessage(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64,
) (ChannelMessage, int, bool, error) {
	return s.postChannelMessage(ctx, channelID, fromID, text, randomID, fileID, false)
}

// PostChannelMessageAs is PostChannelMessage with the post-rights check
// performed inside the same transaction, under the channel_state row lock. It is
// the entry point a handler uses; PostChannelMessage stays the unchecked
// primitive underneath it.
//
// The rule, and it fails closed on anything not listed: a broadcast channel
// (megagroup = false) admits role >= 1 only, a megagroup admits any participant
// row, and a member banned at now() is admitted by neither. No participant row
// is a rejection. Every rejection is ErrNotMember, the same error the chat
// fan-out uses, because a distinct "you are banned" tells an outsider the
// channel exists.
//
// Locking and ordering, which is the point of this function existing rather than
// the check living in a handler: the channel row and the caller's participant
// row are read AFTER LockChannelState and before any write, inside the
// transaction that does the insert. It takes no new lock — the channel_state row
// lock is already held and the two reads join under it. A handler-level check
// runs in its own transaction, so a member banned concurrently would still land
// a post. That ordering is what every future channel write inherits.
func (s *Store) PostChannelMessageAs(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64,
) (ChannelMessage, int, bool, error) {
	return s.postChannelMessage(ctx, channelID, fromID, text, randomID, fileID, true)
}

func (s *Store) postChannelMessage(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64, checkRights bool,
) (ChannelMessage, int, bool, error) {
	if channelID == 0 || fromID == 0 {
		return ChannelMessage{}, 0, false, ErrMessageInvalid
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Early reject, ahead of EnsureChannelState. It decides nothing — the
	// authoritative check is the identical call under the row lock below — but
	// channel_state.channel_id REFERENCES channels (id), so letting a caller with
	// no rights reach the insert turns a channel that does not exist into an FK
	// error instead of ErrNotMember, and that distinct error is exactly the
	// existence oracle the one-error rule closes. Same shape as the fan-out's
	// pre-lock IsChatMember reject in fanout.go.
	if checkRights {
		if err = checkPostRights(ctx, qtx, channelID, fromID); err != nil {
			return ChannelMessage{}, 0, false, err
		}
	}

	if err = qtx.EnsureChannelState(ctx, channelID); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("ensure channel state: %w", err)
	}
	st, err := qtx.LockChannelState(ctx, channelID)
	if err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("lock channel state: %w", err)
	}

	// The authoritative check: under the row lock taken above, before the dedup
	// read and before any write, so a ban committing concurrently is seen. A
	// caller with no right to post here must not be able to probe random_ids
	// either.
	if checkRights {
		if err = checkPostRights(ctx, qtx, channelID, fromID); err != nil {
			return ChannelMessage{}, 0, false, err
		}
	}

	// Idempotency: a resend with the same random_id returns the original post,
	// with no row, no event and no pts movement — same rule as the chat and 1:1
	// send paths, read under the row lock so two concurrent resends agree.
	if randomID != 0 {
		existing, e := qtx.ChannelMessageByRandomID(ctx, db.ChannelMessageByRandomIDParams{
			ChannelID: channelID, RandomID: randomID,
		})
		switch {
		case e == nil:
			return channelMessageFromFields(channelMsgFields(existing)), int(st.Pts), true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return ChannelMessage{}, 0, false, fmt.Errorf("random_id lookup: %w", e)
		}
	}

	b, err := qtx.BumpChannelState(ctx, channelID)
	if err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("bump channel state: %w", err)
	}
	if err = qtx.InsertChannelMessage(ctx, db.InsertChannelMessageParams{
		ChannelID: channelID, LocalID: b.LocalID, FromID: fromID,
		Message: text, RandomID: randomID, FileID: fileID,
	}); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("insert channel message: %w", err)
	}
	if err = qtx.InsertChannelEvent(ctx, db.InsertChannelEventParams{
		ChannelID: channelID, Pts: b.Pts, Type: int16(EventNewMessage), LocalID: b.LocalID,
	}); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("insert channel event: %w", err)
	}

	stored, err := qtx.ChannelMessageByLocal(ctx, db.ChannelMessageByLocalParams{
		ChannelID: channelID, LocalID: b.LocalID,
	})
	if err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("reload channel message: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("commit: %w", err)
	}
	return channelMessageFromFields(channelMsgFields(stored)), int(b.Pts), false, nil
}

// checkPostRights answers whether fromID may post to channelID, reading both
// rows on the caller's transaction so they are the state the insert lands in.
// Every rejection is ErrNotMember; see PostChannelMessageAs for why they are not
// distinguishable.
func checkPostRights(ctx context.Context, qtx *db.Queries, channelID, fromID int64) error {
	ch, err := qtx.ChannelByID(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotMember
	case err != nil:
		return fmt.Errorf("channel by id: %w", err)
	}

	row, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID, UserID: fromID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotMember
	case err != nil:
		return fmt.Errorf("channel participant: %w", err)
	}

	member := channelMemberFromRow(row)
	if member.Banned(time.Now()) {
		return ErrNotMember
	}
	// Broadcast: posting is an admin right. Megagroup: any unbanned participant.
	if !ch.Megagroup && member.Role < 1 {
		return ErrNotMember
	}
	return nil
}

// ChannelEventsWindow returns the channel's events in (fromPts, toPts] ordered
// ascending, at most limit of them. Bounding by toPts (a previously-read state
// pts) keeps the difference from advertising a pts past an event it did not
// return.
func (s *Store) ChannelEventsWindow(ctx context.Context, channelID int64, fromPts, toPts, limit int) ([]ChannelEvent, error) {
	rows, err := s.q.ChannelEventsWindow(ctx, db.ChannelEventsWindowParams{
		ChannelID: channelID,
		FromPts:   int64(fromPts),
		ToPts:     int64(toPts),
		Lim:       int32(limit), //nolint:gosec // limit is a small server-set cap
	})
	if err != nil {
		return nil, fmt.Errorf("channel events window: %w", err)
	}
	events := make([]ChannelEvent, len(rows))
	for i, r := range rows {
		events[i] = ChannelEvent{Pts: int(r.Pts), Type: EventType(r.Type), LocalID: r.LocalID}
	}
	return events, nil
}

// ChannelMessages returns the requested posts keyed by local_id, deleted rows
// included — deliberately, and unlike ChannelHistory, which excludes them. This
// is the hydration read behind an event log, and a delete event has to be able
// to name the post it removed. Ids with no row
// are simply absent from the map, which is what hydrating an event log against a
// channel someone has pruned looks like.
func (s *Store) ChannelMessages(ctx context.Context, channelID int64, localIDs []int64) (map[int64]ChannelMessage, error) {
	if len(localIDs) == 0 {
		return map[int64]ChannelMessage{}, nil
	}
	rows, err := s.q.ChannelMessagesByLocalIDs(ctx, db.ChannelMessagesByLocalIDsParams{
		ChannelID: channelID, LocalIds: localIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("channel messages: %w", err)
	}
	out := make(map[int64]ChannelMessage, len(rows))
	for _, r := range rows {
		out[r.LocalID] = channelMessageFromFields(channelMsgFields(r))
	}
	return out, nil
}

// ChannelMessageByRandomID looks up a channel post by random_id. Returns
// ok=false when absent. It is the read half of the dedup token for channel
// posts, used by the handler to catch transport retries before the rate limit.
func (s *Store) ChannelMessageByRandomID(ctx context.Context, channelID, randomID int64) (ChannelMessage, bool, error) {
	if randomID == 0 {
		return ChannelMessage{}, false, nil
	}
	row, err := s.q.ChannelMessageByRandomID(ctx, db.ChannelMessageByRandomIDParams{
		ChannelID: channelID, RandomID: randomID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ChannelMessage{}, false, nil
	case err != nil:
		return ChannelMessage{}, false, fmt.Errorf("channel message by random id: %w", err)
	}
	return channelMessageFromFields(channelMsgFields(row)), true, nil
}

// ChannelHistory returns the channel's posts newest-first, excluding deleted.
// offsetID > 0 pages strictly older than that local_id (0 = from the newest),
// the same convention History uses for user peers.
func (s *Store) ChannelHistory(ctx context.Context, channelID int64, offsetID int64, limit int) ([]ChannelMessage, error) {
	rows, err := s.q.ChannelHistoryPage(ctx, db.ChannelHistoryPageParams{
		ChannelID: channelID,
		OffsetID:  offsetID,
		Lim:       int32(limit), //nolint:gosec // limit is a small validated page size
	})
	if err != nil {
		return nil, fmt.Errorf("channel history page: %w", err)
	}
	msgs := make([]ChannelMessage, len(rows))
	for i, r := range rows {
		msgs[i] = channelMessageFromFields(channelMsgFields(r))
	}
	return msgs, nil
}
