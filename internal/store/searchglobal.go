package store

import (
	"context"
	"fmt"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// GlobalSearchCursor is the keyset one cross-dialog search page resumes from.
// It is the tuple the page query orders by, and every field of it must come
// from a row the caller was actually served: a cursor computed over the
// unfiltered union would let a caller paging one row at a time count messages
// in peers they cannot read.
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
func (s *Store) SearchGlobal(ctx context.Context, userID int64, query string, cursor *GlobalSearchCursor, limit int) ([]GlobalSearchHit, error) {
	params := db.SearchGlobalPageParams{
		OwnerID: userID,
		Query:   query,
		Lim:     int32(limit), //nolint:gosec // limit is a small validated page size
	}
	if cursor != nil {
		params.HasCursor = true
		params.OffsetRate = cursor.Rate
		params.OffsetPeerType = int16(cursor.PeerType)
		params.OffsetPeerID = cursor.PeerID
		params.OffsetID = cursor.MsgID
	}
	keys, err := s.q.SearchGlobalPage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("search global page: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	owned, err := s.ownedHits(ctx, userID, keys)
	if err != nil {
		return nil, err
	}
	posts, err := s.postHits(ctx, userID, keys)
	if err != nil {
		return nil, err
	}

	// Rebuild the page in the order the page query fixed. A key whose row is
	// gone by now — deleted, or in a channel this caller was just banned from —
	// simply drops out: the page is short, never padded with a row the second
	// read did not authorize.
	hits := make([]GlobalSearchHit, 0, len(keys))
	for _, k := range keys {
		hit := GlobalSearchHit{Rate: k.Rate, PeerType: PeerType(k.PeerType), PeerID: k.PeerID}
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
