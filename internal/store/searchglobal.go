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

// ErrSearchContended reports that a search page kept selecting rows that were
// gone by the time their bodies were read, and gave up rather than answer with
// the empty page a client reads as exhaustion. It is transient by construction:
// the caller retries and the next attempt sees the settled state.
var ErrSearchContended = errors.New("search page contended")

// GlobalSearchCursor is a cross-dialog search cursor as the wire carries it.
// resolveCursor turns it into the keyset the page query actually orders by,
// which is searchGlobalBound. Every field must come from a row the caller was
// served: a cursor computed over the unfiltered union would let a caller paging
// one row at a time count messages in peers they cannot read.
//
// Rate is the served row's date in whole seconds, the finest offset_rate can
// carry. It is not what positions the scan — the cursor row's own stored
// timestamp is, read back server-side — and is used as a bound only when the
// cursor names a row that does not exist.
//
// What that ordering does and does not promise, since the difference is a
// wire contract: `date` defaults to now(), which Postgres evaluates at
// transaction start, so a row whose transaction opened before a sequence
// started and committed during it becomes visible with a date behind the
// cursor and may be served mid-sequence. Exactly-once binds over the rows
// committed when the sequence started: those are each served once, never
// duplicated, never skipped, and never displaced. A row committing late is
// served at most once, and perturbs none of them. Closing that last gap needs a
// commit-ordered sequence this schema does not have — per-owner local_id spaces
// and the shared channel post-id space are not comparable — so it is a
// migration, and deliberately not this ticket.
type GlobalSearchCursor struct {
	// Rate is the message date truncated to whole seconds — the value the wire
	// carries as offset_rate.
	Rate     int64
	PeerType PeerType
	PeerID   int64
	// MsgID is the message id in its own peer's id space: the caller's local_id
	// for an owned row, the shared post id for a channel row.
	MsgID int64
}

// GlobalSearchHit is one hit of a cross-dialog search page. Exactly one of Owned
// and Post is set: the two peer kinds keep their rows in different tables and
// render through different mappers, and collapsing them into one shape here
// would mean inventing an owner for a channel post that has none.
type GlobalSearchHit struct {
	Rate     int64
	PeerType PeerType
	PeerID   int64
	Owned    *Message
	Post     *ChannelMessage
}

// SearchGlobal returns one page of the caller's cross-dialog keyword search,
// newest-first by date, resuming after cursor when one is given.
//
// The result set is the union of two authorized sets and widens neither. Owned
// rows in user and chat peers are the caller's own copies, which for a chat is
// already the membership check. Channel posts are shared rows, so they are gated
// on an unbanned participant row, and that gate is applied twice: once inside
// the page query, and again here per channel before any post body is read, so a
// ban landing between the two reads still stops the post. Both re-run on every
// page — nothing about authorization is carried in the cursor.
//
// A caller in no dialogs gets an empty page, not an error.
//
// A page whose every key vanishes between the key read and the body read is
// refilled rather than returned empty: an empty page ends the client's sequence,
// so returning one there would silently drop every match behind it. The refill
// resumes from the last key read, which never reaches the wire — the cursor a
// client is handed still comes only from a row it was served.
func (s *Store) SearchGlobal(ctx context.Context, userID int64, query string, cursor *GlobalSearchCursor, limit int) ([]GlobalSearchHit, error) {
	// The caller's own channels, read once per call off the index on
	// channel_participants (user_id). It scopes the posts arm; the membership
	// EXISTS inside the page query is what authorizes it.
	channelIDs, err := s.q.MemberChannelIDs(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("member channel ids: %w", err)
	}
	if channelIDs == nil {
		channelIDs = []int64{}
	}

	var bound *searchGlobalBound
	if cursor != nil {
		b, err := s.resolveCursor(ctx, userID, *cursor)
		if err != nil {
			return nil, err
		}
		bound = &b
	}

	for attempt := 0; ; attempt++ {
		keys, err := s.searchGlobalKeys(ctx, userID, query, channelIDs, bound, limit)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, nil
		}
		hits, err := s.hydrateKeys(ctx, userID, keys)
		if err != nil {
			return nil, err
		}
		if len(hits) > 0 {
			return hits, nil
		}
		// Nothing survived hydration. A short key page means the scan already
		// reached the end of what the caller can read, so there is nothing behind
		// it and the sequence really is over.
		if len(keys) < limit {
			return nil, nil
		}
		// A full key page that hydrated to nothing means authorized matches are
		// still behind it, so running out of refills may not answer the way true
		// exhaustion does. An empty page ends the client's sequence; this
		// condition is transient, so it has to be a retryable failure instead.
		if attempt+1 >= maxSearchGlobalRefills {
			return nil, ErrSearchContended
		}
		// Resume from the last key. Its timestamp came out of the page query, so
		// the refill needs no lookup to place it exactly.
		last := keys[len(keys)-1]
		bound = &searchGlobalBound{
			tieLo:    last.Date,
			tieHi:    last.Date,
			peerType: last.PeerType,
			peerID:   last.PeerID,
			msgID:    last.MsgID,
		}
	}
}

// searchGlobalBound is a wire cursor resolved into the keyset the page query
// takes: a tie window on the stored timestamp plus the peer tuple that breaks
// ties inside it.
type searchGlobalBound struct {
	tieLo, tieHi pgtype.Timestamptz
	peerType     int16
	peerID       int64
	msgID        int64
}

// resolveCursor turns the client's (offset_rate, offset_peer, offset_id) into
// that keyset by reading the cursor row's own timestamp.
//
// Resolving it server-side rather than trusting offset_rate is what makes a page
// sequence stable. offset_rate is whole seconds, the finest the wire carries, and
// a whole-second bound admits a message that arrived after the sequence started
// as long as it lands in that second with a lower peer tuple. The row's stored
// timestamp has no such slack: anything created later sorts strictly ahead of it
// and can only appear at the newest end of a fresh search.
//
// The lookup reads deleted rows too, so a sequence whose last served row is
// deleted mid-sequence still resumes exactly. Only a cursor naming a row that
// never existed falls back to the whole second offset_rate names — there is no
// sequence for an invented cursor to stay consistent with, and the fallback can
// reach no row the keyset could not already reach.
func (s *Store) resolveCursor(ctx context.Context, userID int64, cursor GlobalSearchCursor) (searchGlobalBound, error) {
	b := searchGlobalBound{
		peerType: int16(cursor.PeerType),
		peerID:   cursor.PeerID,
		msgID:    cursor.MsgID,
	}
	exact, found, err := s.cursorDate(ctx, userID, cursor)
	if err != nil {
		return searchGlobalBound{}, err
	}
	if found {
		b.tieLo, b.tieHi = exact, exact
		return b, nil
	}
	start := time.Unix(cursor.Rate, 0)
	b.tieLo = pgtype.Timestamptz{Time: start, Valid: true}
	b.tieHi = pgtype.Timestamptz{Time: start.Add(time.Second - time.Microsecond), Valid: true}
	return b, nil
}

// cursorDate reads the timestamp of the row a cursor names, from the table that
// peer kind stores its rows in. Each read carries the predicate that arm is
// gated on, so a cursor can resolve nothing the search itself would not serve:
// an owned row only within the caller's own rows, a channel post only for a
// channel the handler has already established the caller is an unbanned member
// of.
func (s *Store) cursorDate(ctx context.Context, userID int64, cursor GlobalSearchCursor) (pgtype.Timestamptz, bool, error) {
	var (
		at  pgtype.Timestamptz
		err error
	)
	if cursor.PeerType == PeerTypeChannel {
		at, err = s.q.ChannelPostDate(ctx, db.ChannelPostDateParams{
			ChannelID: cursor.PeerID, LocalID: cursor.MsgID,
		})
	} else {
		at, err = s.q.OwnedMessageDate(ctx, db.OwnedMessageDateParams{
			OwnerID: userID, LocalID: cursor.MsgID,
		})
	}
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return pgtype.Timestamptz{}, false, nil
	case err != nil:
		return pgtype.Timestamptz{}, false, fmt.Errorf("search global cursor date: %w", err)
	}
	return at, at.Valid, nil
}

// maxSearchGlobalRefills caps how many key reads one call may make. Each refill
// costs a bounded page query, and the case it covers is a race, so a small cap
// keeps the cost of one RPC bounded while still surviving a page that a delete
// or a ban emptied.
const maxSearchGlobalRefills = 3

// searchGlobalKeys reads one ordered page of keys: which peer each hit belongs
// to and its id there. This is where both arms' predicates and the keyset live.
func (s *Store) searchGlobalKeys(ctx context.Context, userID int64, query string, channelIDs []int64, bound *searchGlobalBound, limit int) ([]db.SearchGlobalPageRow, error) {
	params := db.SearchGlobalPageParams{
		OwnerID:    userID,
		Query:      query,
		ChannelIds: channelIDs,
		Lim:        int32(limit), //nolint:gosec // limit is a small validated page size
	}
	if bound != nil {
		params.HasCursor = true
		params.TieLo = bound.tieLo
		params.TieHi = bound.tieHi
		params.OffsetPeerType = bound.peerType
		params.OffsetPeerID = bound.peerID
		params.OffsetID = bound.msgID
	}
	keys, err := s.q.SearchGlobalPage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("search global page: %w", err)
	}
	return keys, nil
}

// hydrateKeys reads the bodies behind a page of keys, each arm through its own
// query and its own gate, and rebuilds the page in the order the key read fixed.
//
// A key whose row is gone by now — deleted, or in a channel this caller was just
// banned from — simply drops out: the page is short, never padded with a row the
// second read did not authorize.
func (s *Store) hydrateKeys(ctx context.Context, userID int64, keys []db.SearchGlobalPageRow) ([]GlobalSearchHit, error) {
	// Test hook: fires between the key read and the body read, where a delete or
	// a ban would land.
	if s.searchPageHook != nil {
		s.searchPageHook()
	}
	owned, err := s.ownedHits(ctx, userID, keys)
	if err != nil {
		return nil, err
	}
	posts, err := s.postHits(ctx, userID, keys)
	if err != nil {
		return nil, err
	}
	hits := make([]GlobalSearchHit, 0, len(keys))
	for _, k := range keys {
		// Rate is the wire form of the row's date: whole seconds, the value
		// offset_rate carries back on the next page.
		hit := GlobalSearchHit{Rate: k.Date.Time.Unix(), PeerType: PeerType(k.PeerType), PeerID: k.PeerID}
		if PeerType(k.PeerType) == PeerTypeChannel {
			m, ok := posts[channelPostKey{channelID: k.PeerID, localID: k.MsgID}]
			if !ok {
				continue
			}
			hit.Post = &m
		} else {
			m, ok := owned[k.MsgID]
			if !ok {
				continue
			}
			hit.Owned = &m
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

// channelPostKey identifies one post across channels: a post id is unique only
// within its own channel.
type channelPostKey struct {
	channelID int64
	localID   int64
}

// ownedHits reads the bodies behind the owned keys of a page, keyed by local_id.
func (s *Store) ownedHits(ctx context.Context, userID int64, keys []db.SearchGlobalPageRow) (map[int64]Message, error) {
	var localIDs []int64
	for _, k := range keys {
		if PeerType(k.PeerType) != PeerTypeChannel {
			localIDs = append(localIDs, k.MsgID)
		}
	}
	if len(localIDs) == 0 {
		return map[int64]Message{}, nil
	}
	rows, err := s.q.OwnedMessagesByLocalIDs(ctx, db.OwnedMessagesByLocalIDsParams{
		OwnerID: userID, LocalIds: localIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("search global owned messages: %w", err)
	}
	out := make(map[int64]Message, len(rows))
	for _, r := range rows {
		out[r.LocalID] = messageFromRow(r)
	}
	return out, nil
}

// postHits reads the bodies behind the channel keys of a page, one channel at a
// time, and only for channels this caller is an unbanned member of right now.
// One membership read plus one post read per distinct channel, no cache: a page
// names very few distinct channels, the same reason loadChannels gives for its
// own per-channel loop.
func (s *Store) postHits(ctx context.Context, userID int64, keys []db.SearchGlobalPageRow) (map[channelPostKey]ChannelMessage, error) {
	byChannel := map[int64][]int64{}
	for _, k := range keys {
		if PeerType(k.PeerType) == PeerTypeChannel {
			byChannel[k.PeerID] = append(byChannel[k.PeerID], k.MsgID)
		}
	}
	out := map[channelPostKey]ChannelMessage{}
	if len(byChannel) == 0 {
		return out, nil
	}
	now := time.Now()
	for channelID, localIDs := range byChannel {
		member, found, err := s.ChannelMemberOf(ctx, channelID, userID)
		if err != nil {
			return nil, err
		}
		if !found || member.Banned(now) {
			continue
		}
		msgs, err := s.ChannelMessages(ctx, channelID, localIDs)
		if err != nil {
			return nil, err
		}
		for localID, m := range msgs {
			// ChannelMessages serves deleted rows too — it is the hydration read
			// behind the event log, which has to be able to name a removed post.
			// A search page may not carry one.
			if m.Deleted {
				continue
			}
			out[channelPostKey{channelID: channelID, localID: localID}] = m
		}
	}
	return out, nil
}
