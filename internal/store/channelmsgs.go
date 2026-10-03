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

// SlowModeWaitError reports how many seconds an ordinary megagroup member must
// wait before their next distinct post.
type SlowModeWaitError struct {
	Seconds int
}

func (e *SlowModeWaitError) Error() string {
	return fmt.Sprintf("SLOWMODE_WAIT_%d", e.Seconds)
}

// ChannelMessage is a persisted channel message. Unlike Message there is one row
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
	// ReplyToMsgID is the local_id of the post this post replies to; 0 = no reply.
	ReplyToMsgID int32
	Action       ChannelMessageAction
}

// ChannelMessageAction identifies a service message stored in a channel's
// shared message stream. Zero is a regular post.
type ChannelMessageAction int16

const (
	ChannelMessageActionNone ChannelMessageAction = iota
	ChannelMessageActionCreate
)

// channelMsgFields is a layout-identical copy of the sqlc channel-message
// row types. Any of them converts to this type via a plain type conversion, so
// the single channelMessageFromFields function below is the only place that maps
// database columns to ChannelMessage fields.
type channelMsgFields struct {
	ChannelID    int64
	LocalID      int64
	FromID       int64
	Date         pgtype.Timestamptz
	Message      string
	EditDate     pgtype.Timestamptz
	Deleted      bool
	RandomID     int64
	FileID       *int64
	ReplyToMsgID *int32
	ActionType   int16
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
	var replyToMsgID int32
	if r.ReplyToMsgID != nil {
		replyToMsgID = *r.ReplyToMsgID
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
		replyToMsgID,
		ChannelMessageAction(r.ActionType),
	}
}

// ChannelState returns the channel's current pts. A channel that has never been
// created before channel-create service messages were added may have pts 0, so
// a difference against them works from the moment the channel exists.
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

// newChannelPostPts returns the pts at which the channel's localID entered the
// log. Same contract as newMessagePts, including ErrPtsUnknown.
func newChannelPostPts(ctx context.Context, q *db.Queries, channelID, localID int64) (int, error) {
	pts, err := q.NewChannelPostPts(ctx, db.NewChannelPostPtsParams{ChannelID: channelID, LocalID: localID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, fmt.Errorf("%w: channel %d post %d", ErrPtsUnknown, channelID, localID)
	case err != nil:
		return 0, fmt.Errorf("new channel post pts: %w", err)
	}
	return int(pts), nil
}

// ChannelPostPts returns the pts at which localID entered the channel's log —
// the only pts a resend of that post may report. ErrPtsUnknown when the log has
// no such event.
func (s *Store) ChannelPostPts(ctx context.Context, channelID, localID int64) (int, error) {
	return newChannelPostPts(ctx, s.q, channelID, localID)
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
// replyToMsgID is the local_id of the post this post replies to; 0 = no reply.
// A non-zero replyToMsgID that names a deleted or absent post returns ErrMessageInvalid.
//
// This layer trusts its caller: it does not check that fromID may post here.
//
// Locking: no Go-level lock and no advisory lock, only the channel_state row
// lock, taken by the LockChannelState read ahead of the dedup lookup and held to
// commit. The channel_messages foreign key checks channel_id with KEY SHARE on
// channels; that is compatible with LockChannel's NO KEY UPDATE lock, allowing
// the post to commit while an invite waits for channel_state.
func (s *Store) PostChannelMessage(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64, replyToMsgID int64,
) (ChannelMessage, int, bool, error) {
	return s.postChannelMessage(ctx, channelID, fromID, text, randomID, fileID, replyToMsgID, false)
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
// transaction that does the insert. The same state lock serializes slow-mode
// checks and marker updates with posts; the marker row is updated only after
// authorization, deduplication, restrictions, reply validation and event
// creation succeed. Role, ban and leave mutations can hold channels before a
// participant row, but do not then acquire channel_state; admission takes
// channel_state before participant insertion. A handler-level check runs in its
// own transaction, so a member banned concurrently would still land a post.
// That ordering is what every future channel write inherits.
func (s *Store) PostChannelMessageAs(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64, replyToMsgID int64,
) (ChannelMessage, int, bool, error) {
	return s.postChannelMessage(ctx, channelID, fromID, text, randomID, fileID, replyToMsgID, true)
}

func (s *Store) postChannelMessage(
	ctx context.Context, channelID, fromID int64, text string, randomID int64, fileID *int64, replyToMsgID int64, checkRights bool,
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
		if _, err = checkPostRights(ctx, qtx, channelID, fromID); err != nil {
			return ChannelMessage{}, 0, false, err
		}
	}

	if err = qtx.EnsureChannelState(ctx, channelID); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("ensure channel state: %w", err)
	}
	if _, err = qtx.LockChannelState(ctx, channelID); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("lock channel state: %w", err)
	}

	// The authoritative check: under the row lock taken above, before the dedup
	// read and before any write, so a ban committing concurrently is seen. A
	// caller with no right to post here must not be able to probe random_ids or
	// slow-mode state either.
	var role int
	if checkRights {
		if role, err = checkPostRights(ctx, qtx, channelID, fromID); err != nil {
			return ChannelMessage{}, 0, false, err
		}
	}

	// Idempotency: a resend with the same random_id returns the original post,
	// with no row, no event and no pts movement — same rule as the chat and 1:1
	// send paths, read under the row lock so two concurrent resends agree.
	//
	// The pts reported is the one that post occupies, not the channel's current
	// pts: a subscriber applies updateNewChannelMessage by pts, so naming a
	// newer slot for an old post is how it skips whatever really sits there.
	if randomID != 0 {
		existing, e := qtx.ChannelMessageByRandomID(ctx, db.ChannelMessageByRandomIDParams{
			ChannelID: channelID, RandomID: randomID,
		})
		switch {
		case e == nil:
			pts, e2 := newChannelPostPts(ctx, qtx, channelID, existing.LocalID)
			if e2 != nil {
				return ChannelMessage{}, 0, false, e2
			}
			return channelMessageFromFields(channelMsgFields(existing)), pts, true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return ChannelMessage{}, 0, false, fmt.Errorf("random_id lookup: %w", e)
		}
	}
	var megagroup bool
	if checkRights {
		channel, e := qtx.ChannelPostDefaults(ctx, channelID)
		switch {
		case errors.Is(e, pgx.ErrNoRows):
			return ChannelMessage{}, 0, false, ErrNotMember
		case e != nil:
			return ChannelMessage{}, 0, false, fmt.Errorf("channel post defaults: %w", e)
		}
		megagroup = channel.Megagroup
		if channel.Megagroup {
			if err = checkDefaultMessageRestriction(channel.DefaultBannedRights, role >= channelRoleAdmin, fileID != nil, nil); err != nil {
				return ChannelMessage{}, 0, false, err
			}
		}
	}

	if checkRights && megagroup && role == channelRoleMember {
		state, e := qtx.ChannelSlowModePostState(ctx, db.ChannelSlowModePostStateParams{
			ChannelID: channelID,
			UserID:    fromID,
		})
		switch {
		case errors.Is(e, pgx.ErrNoRows):
			return ChannelMessage{}, 0, false, ErrNotMember
		case e != nil:
			return ChannelMessage{}, 0, false, fmt.Errorf("channel slow-mode post state: %w", e)
		}
		if state.SlowmodeSeconds > 0 && state.LastPostAt.Valid {
			remaining := time.Duration(state.SlowmodeSeconds)*time.Second - state.CheckedAt.Time.Sub(state.LastPostAt.Time)
			if remaining > 0 {
				waitSeconds := int((remaining + time.Second - 1) / time.Second)
				return ChannelMessage{}, 0, false, &SlowModeWaitError{Seconds: waitSeconds}
			}
		}
	}

	// Validate reply_to_msg_id under the lock, before any write. A deleted or
	// absent post returns ErrMessageInvalid without distinguishing the two cases,
	// so the channel's post ids are not an existence oracle for another channel.
	var replyTo *int32
	if replyToMsgID > 0 {
		_, verr := qtx.ChannelPostExistsActive(ctx, db.ChannelPostExistsActiveParams{
			ChannelID: channelID, LocalID: replyToMsgID,
		})
		if errors.Is(verr, pgx.ErrNoRows) {
			return ChannelMessage{}, 0, false, ErrMessageInvalid
		}
		if verr != nil {
			return ChannelMessage{}, 0, false, fmt.Errorf("reply_to_msg_id check: %w", verr)
		}
		v := int32(replyToMsgID) //nolint:gosec // G115: local_id fits int32 wire space
		replyTo = &v
	}

	b, err := qtx.BumpChannelState(ctx, channelID)
	if err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("bump channel state: %w", err)
	}
	if err = qtx.InsertChannelMessage(ctx, db.InsertChannelMessageParams{
		ChannelID: channelID, LocalID: b.LocalID, FromID: fromID,
		Message: text, RandomID: randomID, FileID: fileID, ReplyToMsgID: replyTo,
	}); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("insert channel message: %w", err)
	}
	if err = qtx.InsertChannelEvent(ctx, db.InsertChannelEventParams{
		ChannelID: channelID, Pts: b.Pts, Type: int16(EventNewMessage), LocalID: b.LocalID,
	}); err != nil {
		return ChannelMessage{}, 0, false, fmt.Errorf("insert channel event: %w", err)
	}
	if checkRights {
		n, e := qtx.UpdateChannelPostMarker(ctx, db.UpdateChannelPostMarkerParams{
			ChannelID: channelID,
			UserID:    fromID,
		})
		if e != nil {
			return ChannelMessage{}, 0, false, fmt.Errorf("update channel post marker: %w", e)
		}
		if n != 1 {
			return ChannelMessage{}, 0, false, ErrNotMember
		}
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

// checkPostRights answers whether fromID is a current, unbanned channel poster.
// No slow-mode comparison occurs until post authorization and default
// restrictions have passed.
// Default message restrictions are checked after the dedup read so a committed
// retry remains idempotent. Every rejection here is ErrNotMember; see
// PostChannelMessageAs for why they are not distinguishable.
func checkPostRights(ctx context.Context, qtx *db.Queries, channelID, fromID int64) (int, error) {
	row, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID, UserID: fromID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, ErrNotMember
	case err != nil:
		return 0, fmt.Errorf("channel participant: %w", err)
	}

	member := channelMemberFromRow(row)
	if member.Banned(time.Now()) {
		return 0, ErrNotMember
	}

	megagroup, err := qtx.ChannelMegagroup(ctx, channelID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, ErrNotMember
	case err != nil:
		return 0, fmt.Errorf("channel kind: %w", err)
	}
	// Broadcast: posting is an admin right. Megagroup: any unbanned participant.
	if !megagroup && member.Role < 1 {
		return 0, ErrNotMember
	}
	return member.Role, nil
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

// SearchChannelPosts returns the channel's posts matching query, newest-first
// and excluding deleted, paged by offsetID exactly as ChannelHistory pages.
//
// It takes no caller id on purpose. A channel post is one shared row rather
// than a per-owner copy, so there is nothing here to scope to the reader:
// whether this caller may read the channel at all is decided by the membership
// check the handler runs before calling, and nothing below narrows it further.
func (s *Store) SearchChannelPosts(ctx context.Context, channelID int64, query string, offsetID int64, limit int) ([]ChannelMessage, error) {
	rows, err := s.q.SearchChannelPostsPage(ctx, db.SearchChannelPostsPageParams{
		ChannelID: channelID,
		Query:     query,
		OffsetID:  offsetID,
		Lim:       int32(limit), //nolint:gosec // limit is a small validated page size
	})
	if err != nil {
		return nil, fmt.Errorf("search channel posts page: %w", err)
	}
	msgs := make([]ChannelMessage, len(rows))
	for i, r := range rows {
		msgs[i] = channelMessageFromFields(channelMsgFields(r))
	}
	return msgs, nil
}

// SearchFilteredChannelPosts returns one authorized channel member's matching
// media page and the exact count from the same database snapshot. Membership is
// checked here and included in both queries; limit zero requests only the count.
func (s *Store) SearchFilteredChannelPosts(
	ctx context.Context,
	ownerID, channelID int64,
	query string,
	filter MediaSearchFilter,
	offsetID int64,
	limit int,
) ([]ChannelMessage, int, error) {
	if filter != MediaSearchFilterDocument && filter != MediaSearchFilterPhoto && filter != MediaSearchFilterURL {
		return nil, 0, fmt.Errorf("unsupported channel media search filter %d", filter)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("begin filtered channel search snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	participant, err := qtx.ChannelParticipantByUser(ctx, db.ChannelParticipantByUserParams{
		ChannelID: channelID,
		UserID:    ownerID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, 0, ErrNotMember
	case err != nil:
		return nil, 0, fmt.Errorf("check filtered channel search membership: %w", err)
	}
	if channelMemberFromRow(participant).Banned(time.Now()) {
		return nil, 0, ErrNotMember
	}

	if filter == MediaSearchFilterPhoto {
		if err := tx.Commit(ctx); err != nil {
			return nil, 0, fmt.Errorf("commit empty channel photo search: %w", err)
		}
		return []ChannelMessage{}, 0, nil
	}

	count, err := qtx.CountFilteredChannelPosts(ctx, db.CountFilteredChannelPostsParams{
		ChannelID: channelID,
		OwnerID:   ownerID,
		Filter:    int16(filter),
		Query:     query,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("count filtered channel posts: %w", err)
	}

	var rows []db.SearchFilteredChannelPostsPageRow
	if limit > 0 {
		rows, err = qtx.SearchFilteredChannelPostsPage(ctx, db.SearchFilteredChannelPostsPageParams{
			ChannelID: channelID,
			OwnerID:   ownerID,
			Filter:    int16(filter),
			Query:     query,
			OffsetID:  offsetID,
			Lim:       int32(limit), //nolint:gosec // caller caps this to maxHistoryLimit
		})
		if err != nil {
			return nil, 0, fmt.Errorf("page filtered channel posts: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("commit filtered channel search snapshot: %w", err)
	}
	msgs := make([]ChannelMessage, len(rows))
	for i, row := range rows {
		msgs[i] = channelMessageFromFields(channelMsgFields(row))
	}
	return msgs, int(count), nil
}

// SearchPinnedChannelPost returns the active pinned post for channelID while
// ownerID still has an unbanned participant row. The channel membership join
// and post lookup happen in one statement so a departed or newly banned viewer
// cannot receive channel content through the pin filter.
func (s *Store) SearchPinnedChannelPost(ctx context.Context, ownerID, channelID int64, query string, offsetID int64, limit int) ([]ChannelMessage, error) {
	rows, err := s.q.SearchPinnedChannelPostForMember(ctx, db.SearchPinnedChannelPostForMemberParams{
		OwnerID:   ownerID,
		ChannelID: channelID,
		Query:     query,
		OffsetID:  offsetID,
		Lim:       int32(limit), //nolint:gosec // limit is a small validated page size
	})
	if err != nil {
		return nil, fmt.Errorf("search pinned channel post: %w", err)
	}
	msgs := make([]ChannelMessage, len(rows))
	for i, row := range rows {
		msgs[i] = channelMessageFromFields(channelMsgFields(row))
	}
	return msgs, nil
}
