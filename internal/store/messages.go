package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrMessageInvalid is returned when an edit/delete targets a message the caller
// does not own or that does not exist.
var ErrMessageInvalid = errors.New("message id invalid")

// PeerType discriminates the peer_id namespace. Chat ids and user ids come from
// different sequences and can collide, so peer_id is meaningless without it.
type PeerType int16

const (
	PeerTypeUser PeerType = 1
	PeerTypeChat PeerType = 2
	// PeerTypeChannel is a channel peer; its ids come from the channels sequence.
	PeerTypeChannel PeerType = 3
)

// Message is a persisted message row (one side of a two-sided pair).
type Message struct {
	OwnerID     int64
	LocalID     int64
	PeerType    PeerType
	PeerID      int64
	FromID      int64
	Date        time.Time
	Text        string
	Out         bool
	EditDate    *time.Time
	Deleted     bool
	RandomID    int64
	PeerLocalID int64
	// FanoutID links every per-member copy of one chat message; 0 on 1:1 rows,
	// where the linkage is PeerLocalID instead.
	FanoutID int64
	// FileID is the uploaded file attached to this message; 0 = no media.
	FileID       int64
	Action       ChatAction
	ActionUserID int64
	// ReplyToMsgID is the local_id of the message this message replies to,
	// in this row's owner's local_id space.
	ReplyToMsgID int32
	// FwdFromID is the user id of the original sender when this is a forwarded
	// message; 0 when not forwarded.
	FwdFromID int64
	// FwdDate is the date of the original message when this is a forwarded
	// message; zero time when not forwarded.
	FwdDate time.Time
	// FwdChannelID is the channel id when the source is a channel post; 0 otherwise.
	FwdChannelID int64
	// FwdChannelPost is the local_id of the channel post when the source is a
	// channel; 0 otherwise.
	FwdChannelPost int32
}

func messageFromRow(r db.Message) Message {
	m := Message{
		OwnerID:     r.OwnerID,
		LocalID:     r.LocalID,
		PeerType:    PeerType(r.PeerType),
		PeerID:      r.PeerID,
		FromID:      r.FromID,
		Date:        r.Date.Time,
		Text:        r.Message,
		Out:         r.Out,
		Deleted:     r.Deleted,
		RandomID:    r.RandomID,
		PeerLocalID: r.PeerLocalID,

		FanoutID:     r.FanoutID,
		FileID:       r.FileID,
		Action:       ChatAction(r.ActionType),
		ActionUserID: r.ActionUserID,
	}
	if r.EditDate.Valid {
		t := r.EditDate.Time
		m.EditDate = &t
	}
	if r.ReplyToMsgID != nil {
		m.ReplyToMsgID = *r.ReplyToMsgID
	}
	if r.FwdFromID != nil {
		m.FwdFromID = *r.FwdFromID
	}
	if r.FwdDate.Valid {
		m.FwdDate = r.FwdDate.Time
	}
	if r.FwdChannelID != nil {
		m.FwdChannelID = *r.FwdChannelID
	}
	if r.FwdChannelPost != nil {
		m.FwdChannelPost = *r.FwdChannelPost
	}
	return m
}

// MessageByOwnerLocal returns one message by its (owner, local_id); ok=false when absent.
func (s *Store) MessageByOwnerLocal(ctx context.Context, ownerID, localID int64) (Message, bool, error) {
	r, err := s.q.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Message{}, false, nil
	case err != nil:
		return Message{}, false, fmt.Errorf("message by owner local: %w", err)
	}
	return messageFromRow(r), true, nil
}

// MessageByRandomID returns the sender's own copy of the message carrying
// randomID; ok=false when absent. It is the read half of the dedup token both
// send paths write, for a caller that has to know whether a send is a resend
// before it does work the send itself cannot undo.
func (s *Store) MessageByRandomID(ctx context.Context, ownerID, randomID int64) (Message, bool, error) {
	if randomID == 0 {
		return Message{}, false, nil
	}
	r, err := s.q.MessageByRandomID(ctx, db.MessageByRandomIDParams{OwnerID: ownerID, RandomID: randomID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Message{}, false, nil
	case err != nil:
		return Message{}, false, fmt.Errorf("message by random id: %w", err)
	}
	return messageFromRow(r), true, nil
}

// SendMessage persists both sides of a 1:1 message in one transaction. Each side
// gets its own local_id and its own pts++. A repeated randomID (per sender) is
// deduped: the original sender message is returned with dup=true and no new rows
// or events. replyToMsgID is the sender's local_id of the message being replied to
// (0 if no reply). Returns the sender's stored copy plus both owners' resulting pts.
func (s *Store) SendMessage(ctx context.Context, fromID, toID int64, text string, randomID, fileID, replyToMsgID int64) (sender Message, senderPts, recipientPts int, dup bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Take the sorted advisory locks before any per-owner insert. Provisioning
	// state rows before locking could deadlock two opposite-direction first sends
	// on the update_state unique-key insert; locking first serializes them.
	if err = lockOwners(ctx, tx, fromID, toID); err != nil {
		return Message{}, 0, 0, false, err
	}
	if err = qtx.EnsureUpdateState(ctx, fromID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure sender state: %w", err)
	}
	if err = qtx.EnsureUpdateState(ctx, toID); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("ensure recipient state: %w", err)
	}

	// Idempotency: a resend with the same random_id returns the original, at the
	// pts each side's stored copy occupies rather than that side's current pts.
	// The client applies updateNewMessage by pts, so an old message carrying a
	// current pts lands in a newer update's slot and silently displaces it.
	if randomID != 0 {
		existing, e := qtx.MessageByRandomID(ctx, db.MessageByRandomIDParams{OwnerID: fromID, RandomID: randomID})
		switch {
		case e == nil:
			sPts, e2 := newMessagePts(ctx, qtx, fromID, existing.LocalID)
			if e2 != nil {
				return Message{}, 0, 0, false, e2
			}
			rPts, e2 := mirrorPts(ctx, qtx, s.log, existing, toID)
			if e2 != nil {
				return Message{}, 0, 0, false, e2
			}
			return messageFromRow(existing), sPts, rPts, true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return Message{}, 0, 0, false, fmt.Errorf("random_id lookup: %w", e)
		}
	}

	sb, err := qtx.BumpState(ctx, fromID)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("bump sender: %w", err)
	}
	rb, err := qtx.BumpState(ctx, toID)
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("bump recipient: %w", err)
	}

	// Sender outbox copy (dedup token lives here) + recipient inbox copy.
	var senderReplyTo *int32
	if replyToMsgID > 0 {
		v := int32(replyToMsgID) //nolint:gosec // G115: local_id fits int32 wire space
		senderReplyTo = &v
	}
	if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
		OwnerID: fromID, LocalID: sb.LocalID, PeerType: int16(PeerTypeUser), PeerID: toID, FromID: fromID,
		Message: text, Out: true, RandomID: randomID, PeerLocalID: rb.LocalID,
		FanoutID: 0, ActionType: 0, ActionUserID: 0, FileID: fileID, ReplyToMsgID: senderReplyTo,
		FwdFromID: nil, FwdDate: pgtype.Timestamptz{}, FwdChannelID: nil, FwdChannelPost: nil,
	}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("insert sender message: %w", err)
	}

	// For the recipient's row, the reply must point to the same physical message
	// but in the recipient's local_id space. Resolve by looking up the sender's
	// reply target and finding its peer_local_id (which is the recipient's local_id).
	var recipientReplyTo *int32
	if replyToMsgID > 0 {
		if ref, e := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: fromID, LocalID: replyToMsgID}); e == nil {
			v := int32(ref.PeerLocalID) //nolint:gosec // G115: local_id fits int32 wire space
			recipientReplyTo = &v
		}
	}

	if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
		OwnerID: toID, LocalID: rb.LocalID, PeerType: int16(PeerTypeUser), PeerID: fromID, FromID: fromID,
		Message: text, Out: false, RandomID: 0, PeerLocalID: sb.LocalID,
		FanoutID: 0, ActionType: 0, ActionUserID: 0, FileID: fileID, ReplyToMsgID: recipientReplyTo,
		FwdFromID: nil, FwdDate: pgtype.Timestamptz{}, FwdChannelID: nil, FwdChannelPost: nil,
	}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("insert recipient message: %w", err)
	}

	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: fromID, Pts: sb.Pts, Type: int16(EventNewMessage), LocalID: sb.LocalID}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("sender event: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: toID, Pts: rb.Pts, Type: int16(EventNewMessage), LocalID: rb.LocalID}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("recipient event: %w", err)
	}

	// Sender read their own message (unread +0); recipient gains one unread.
	if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: fromID, PeerType: int16(PeerTypeUser), PeerID: toID, TopMessage: sb.LocalID, UnreadCount: 0}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("sender dialog: %w", err)
	}
	if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: toID, PeerType: int16(PeerTypeUser), PeerID: fromID, TopMessage: rb.LocalID, UnreadCount: 1}); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("recipient dialog: %w", err)
	}

	stored, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: fromID, LocalID: sb.LocalID})
	if err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("reload sender message: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Message{}, 0, 0, false, fmt.Errorf("commit: %w", err)
	}
	return messageFromRow(stored), int(sb.Pts), int(rb.Pts), false, nil
}

// History returns owner's messages with peer, newest-first, excluding deleted.
// offsetID > 0 pages strictly older than that local_id (0 = from newest).
func (s *Store) History(ctx context.Context, ownerID int64, peerType PeerType, peerID int64, offsetID, limit int) ([]Message, error) {
	rows, err := s.q.HistoryPage(ctx, db.HistoryPageParams{
		OwnerID:  ownerID,
		PeerType: int16(peerType),
		PeerID:   peerID,
		OffsetID: int64(offsetID),
		Lim:      int32(limit), //nolint:gosec // limit is a small validated page size

	})
	if err != nil {
		return nil, fmt.Errorf("history page: %w", err)
	}
	msgs := make([]Message, len(rows))
	for i, r := range rows {
		msgs[i] = messageFromRow(r)
	}
	return msgs, nil
}

// SearchMessages returns the caller's messages in the named peer whose text
// matches query, ordered newest-first (descending local_id), with pagination by
// offsetID. Only non-deleted messages owned by ownerID are returned.
func (s *Store) SearchMessages(ctx context.Context, ownerID int64, peerType PeerType, peerID int64, query string, offsetID, limit int) ([]Message, error) {
	rows, err := s.q.SearchMessages(ctx, db.SearchMessagesParams{
		OwnerID:  ownerID,
		PeerType: int16(peerType),
		PeerID:   peerID,
		Query:    query,
		OffsetID: int64(offsetID),
		Lim:      int32(limit), //nolint:gosec // limit is a small validated page size
	})
	if err != nil {
		return nil, fmt.Errorf("search messages: %w", err)
	}
	msgs := make([]Message, len(rows))
	for i, r := range rows {
		msgs[i] = messageFromRow(r)
	}
	return msgs, nil
}

// EditMessage edits the caller's own outgoing message and its mirror on the
// peer's side, bumping both owners' pts with an edit event. Returns the peer id
// and the editor's new pts. ErrMessageInvalid if the message is absent, deleted,
// or not an outgoing message the caller authored.
//
// On a chat message the mirror is every per-member copy of the fan-out rather
// than one peer row, the returned peer id is the chat id, and the caller must
// still be a member of that chat.
func (s *Store) EditMessage(ctx context.Context, ownerID, localID int64, text string) (peerID int64, newPts int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Discover the peer to lock; validated again after the lock (fail closed).
	pre, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, ErrMessageInvalid
	}
	if err != nil {
		return 0, 0, fmt.Errorf("load message: %w", err)
	}
	peerID = pre.PeerID
	if PeerType(pre.PeerType) == PeerTypeChat {
		if newPts, err = editChatMessage(ctx, tx, qtx, pre, text); err != nil {
			return 0, 0, err
		}
		if err = tx.Commit(ctx); err != nil {
			return 0, 0, fmt.Errorf("commit: %w", err)
		}
		return peerID, newPts, nil
	}
	if err = lockOwners(ctx, tx, ownerID, peerID); err != nil {
		return 0, 0, err
	}

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: localID})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, ErrMessageInvalid
	}
	if err != nil {
		return 0, 0, fmt.Errorf("reload message: %w", err)
	}
	if !msg.Out || msg.Deleted {
		return 0, 0, ErrMessageInvalid
	}

	if err = qtx.SetEditedText(ctx, db.SetEditedTextParams{OwnerID: ownerID, LocalID: localID, Message: text}); err != nil {
		return 0, 0, fmt.Errorf("edit owner row: %w", err)
	}
	if err = qtx.SetEditedText(ctx, db.SetEditedTextParams{OwnerID: peerID, LocalID: msg.PeerLocalID, Message: text}); err != nil {
		return 0, 0, fmt.Errorf("edit mirror row: %w", err)
	}

	ownerPts, err := qtx.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump owner: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: ownerID, Pts: ownerPts, Type: int16(EventEdit), LocalID: localID}); err != nil {
		return 0, 0, fmt.Errorf("owner edit event: %w", err)
	}
	peerPts, err := qtx.BumpPtsOnly(ctx, peerID)
	if err != nil {
		return 0, 0, fmt.Errorf("bump peer: %w", err)
	}
	if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: peerID, Pts: peerPts, Type: int16(EventEdit), LocalID: msg.PeerLocalID}); err != nil {
		return 0, 0, fmt.Errorf("peer edit event: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	return peerID, int(ownerPts), nil
}

// chatCopies loads every per-member copy of the chat message pre belongs to.
//
// A chat-peer row carrying fanout_id = 0 has no fan-out to walk: 0 is the "not a
// chat message" sentinel every 1:1 row carries, so treating it as an id would
// reach the entire 1:1 table. Fail closed rather than query on it.
func chatCopies(ctx context.Context, qtx *db.Queries, pre db.Message) ([]db.Message, error) {
	if pre.FanoutID == 0 {
		return nil, ErrMessageInvalid
	}
	copies, err := qtx.MessagesByFanout(ctx, pre.FanoutID)
	if err != nil {
		return nil, fmt.Errorf("fanout copies: %w", err)
	}
	if len(copies) == 0 {
		return nil, ErrMessageInvalid
	}
	return copies, nil
}

// chatMembers returns the chat's current member set.
//
// Both of the things it feeds depend on it being read inside the transaction with
// the per-owner advisory locks already held, never before them. One is the
// authorization check: editMessage and deleteMessages take no peer — they are
// keyed on (owner_id, local_id) alone — and a removed member keeps their old
// copies, so without it they reach every current member's rows through one of
// them. The other is the filter that keeps the write out of a removed member's
// own rows, whose copies are theirs and frozen from the moment they left.
//
// A membership mutation announces itself with a fan-out that writes a row for the
// affected user, so once every copy owner's lock is held a removal has either
// committed and is visible here, or is blocked behind this transaction. Reading
// the set any earlier reopens exactly the window both uses exist to close.
func chatMembers(ctx context.Context, qtx *db.Queries, chatID int64) (map[int64]bool, error) {
	parts, err := qtx.ChatParticipants(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("chat participants: %w", err)
	}
	members := make(map[int64]bool, len(parts))
	for _, p := range parts {
		members[p.UserID] = true
	}
	return members, nil
}

func copyOwners(copies []db.Message) []int64 {
	ids := make([]int64, len(copies))
	for i, c := range copies {
		ids[i] = c.OwnerID
	}
	return ids
}

// editChatMessage applies an edit to every per-member copy of a chat message,
// with one edit event and one pts bump per affected owner. Returns the author's
// new pts.
//
// The copy set comes from fanout_id and the member set from chatMembers, and the
// write is their intersection: everyone who holds a copy and is still in the chat.
//
// No chats row lock is taken here, deliberately. The member set is read after the
// per-owner advisory locks, and every copy owner's lock is held by then, which is
// what makes the read authoritative — see chatMembers for why that is as strong
// as the chats row lock here. Leaving the chats row out is what lets
// DeleteMessages accept a batch spanning several chats without breaking the
// one-chats-row-per-transaction rule.
func editChatMessage(ctx context.Context, tx pgx.Tx, qtx *db.Queries, pre db.Message, text string) (int, error) {
	copies, err := chatCopies(ctx, qtx, pre)
	if err != nil {
		return 0, err
	}
	// Service messages are server-authored. Their subject's copy still carries
	// out = true, so without this an add/remove/rename announcement would be
	// rewritable in every member's history by whoever triggered it.
	if pre.ActionType != 0 {
		return 0, ErrMessageInvalid
	}
	if err = lockOwners(ctx, tx, copyOwners(copies)...); err != nil {
		return 0, err
	}

	msg, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: pre.OwnerID, LocalID: pre.LocalID})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrMessageInvalid
	}
	if err != nil {
		return 0, fmt.Errorf("reload message: %w", err)
	}
	if !msg.Out || msg.Deleted {
		return 0, ErrMessageInvalid
	}
	members, err := chatMembers(ctx, qtx, pre.PeerID)
	if err != nil {
		return 0, err
	}
	// The author must still be in the chat. ErrMessageInvalid rather than a
	// distinct error keeps the RPC surface unchanged.
	if !members[pre.OwnerID] {
		return 0, ErrMessageInvalid
	}

	var authorPts int64
	for _, c := range copies {
		// A member removed after the send keeps the copy they had. A later edit
		// must not push new text, an event or a pts bump into it: that is content
		// delivered to a non-member, and their row stays as it was when they left.
		if !members[c.OwnerID] {
			continue
		}
		if err = qtx.SetEditedText(ctx, db.SetEditedTextParams{OwnerID: c.OwnerID, LocalID: c.LocalID, Message: text}); err != nil {
			return 0, fmt.Errorf("edit copy %d: %w", c.OwnerID, err)
		}
		pts, e := qtx.BumpPtsOnly(ctx, c.OwnerID)
		if e != nil {
			return 0, fmt.Errorf("bump %d: %w", c.OwnerID, e)
		}
		if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: c.OwnerID, Pts: pts, Type: int16(EventEdit), LocalID: c.LocalID}); e != nil {
			return 0, fmt.Errorf("edit event %d: %w", c.OwnerID, e)
		}
		if c.OwnerID == pre.OwnerID {
			authorPts = pts
		}
	}
	return int(authorPts), nil
}

// DeleteMessages marks the given owner-local messages deleted on both sides,
// emitting one delete event per message per affected owner. It fails closed
// (ErrMessageInvalid, no changes) if any id is absent. Returns the resulting
// pts per affected user (owner and peers) for the caller to notify.
//
// A chat message deletes every per-member copy of its fan-out instead of one
// mirror row, so only its author may delete it and a service message may not be
// deleted at all; the caller must also still be a member of every chat the batch
// touches. A batch may span several chats and both peer types.
func (s *Store) DeleteMessages(ctx context.Context, ownerID int64, localIDs []int64) (map[int64]int, error) {
	if len(localIDs) == 0 {
		return map[int64]int{}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Load every target first; a missing id fails the whole batch (fail closed).
	// A chat target brings its whole fan-out along: every copy is deleted, so
	// every copy's owner is locked and notified.
	msgs := make([]db.Message, 0, len(localIDs))
	owners := map[int64]bool{ownerID: true}
	chats := map[int64]bool{}
	fanouts := map[int64][]db.Message{}
	for _, id := range localIDs {
		m, e := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: ownerID, LocalID: id})
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, ErrMessageInvalid
		}
		if e != nil {
			return nil, fmt.Errorf("load message %d: %w", id, e)
		}
		msgs = append(msgs, m)
		if PeerType(m.PeerType) == PeerTypeChat {
			// Author only, service messages never, exactly as an edit. A chat
			// delete walks the same copy set an edit does, so without this a
			// member deletes any message she holds a copy of out of every other
			// member's history, and the edit path's service-message guard buys
			// nothing — the announcement she cannot rewrite she can destroy.
			if !m.Out || m.ActionType != 0 {
				return nil, ErrMessageInvalid
			}
			copies, e2 := chatCopies(ctx, qtx, m)
			if e2 != nil {
				return nil, e2
			}
			fanouts[m.FanoutID] = copies
			for _, c := range copies {
				owners[c.OwnerID] = true
			}
			chats[m.PeerID] = true
			continue
		}
		owners[m.PeerID] = true
	}

	lockIDs := make([]int64, 0, len(owners))
	for id := range owners {
		lockIDs = append(lockIDs, id)
	}
	if err = lockOwners(ctx, tx, lockIDs...); err != nil {
		return nil, err
	}
	members := make(map[int64]map[int64]bool, len(chats))
	for chatID := range chats {
		set, e := chatMembers(ctx, qtx, chatID)
		if e != nil {
			return nil, e
		}
		if !set[ownerID] {
			return nil, ErrMessageInvalid
		}
		members[chatID] = set
	}

	perOwner := map[int64]int{}
	for _, m := range msgs {
		if PeerType(m.PeerType) == PeerTypeChat {
			for _, c := range fanouts[m.FanoutID] {
				// Same rule as an edit: a member removed after the send keeps
				// their copy, so the delete and its event stop at the current
				// member set rather than reaching into rows that are theirs.
				if !members[m.PeerID][c.OwnerID] {
					continue
				}
				if err = qtx.SetDeleted(ctx, db.SetDeletedParams{OwnerID: c.OwnerID, LocalID: c.LocalID}); err != nil {
					return nil, fmt.Errorf("delete copy %d: %w", c.OwnerID, err)
				}
				pts, e := qtx.BumpPtsOnly(ctx, c.OwnerID)
				if e != nil {
					return nil, fmt.Errorf("bump %d: %w", c.OwnerID, e)
				}
				if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: c.OwnerID, Pts: pts, Type: int16(EventDelete), LocalID: c.LocalID}); e != nil {
					return nil, fmt.Errorf("delete event %d: %w", c.OwnerID, e)
				}
				perOwner[c.OwnerID] = int(pts)
			}
			continue
		}
		if err = qtx.SetDeleted(ctx, db.SetDeletedParams{OwnerID: ownerID, LocalID: m.LocalID}); err != nil {
			return nil, fmt.Errorf("delete owner row: %w", err)
		}
		if err = qtx.SetDeleted(ctx, db.SetDeletedParams{OwnerID: m.PeerID, LocalID: m.PeerLocalID}); err != nil {
			return nil, fmt.Errorf("delete mirror row: %w", err)
		}
		ownerPts, e := qtx.BumpPtsOnly(ctx, ownerID)
		if e != nil {
			return nil, fmt.Errorf("bump owner: %w", e)
		}
		if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: ownerID, Pts: ownerPts, Type: int16(EventDelete), LocalID: m.LocalID}); e != nil {
			return nil, fmt.Errorf("owner delete event: %w", e)
		}
		perOwner[ownerID] = int(ownerPts)
		peerPts, e := qtx.BumpPtsOnly(ctx, m.PeerID)
		if e != nil {
			return nil, fmt.Errorf("bump peer: %w", e)
		}
		if e = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: m.PeerID, Pts: peerPts, Type: int16(EventDelete), LocalID: m.PeerLocalID}); e != nil {
			return nil, fmt.Errorf("peer delete event: %w", e)
		}
		perOwner[m.PeerID] = int(peerPts)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return perOwner, nil
}

// mirrorPts returns the recipient's pts for the stored counterpart of a deduped
// 1:1 send: the pts toID's inbox copy occupies.
//
// The dedup key is (sender, random_id) with no destination in it, so a sender
// reusing one random_id across peers arrives here holding a row that is not this
// send's pair at all. That row has no counterpart on toID's side to name a pts
// from, and the recipient reports its current pts exactly as it did before —
// nothing this function can return reaches a client today, since only the
// sender's value is answered to a caller on the dedup path. That is a property
// of the callers, not of this branch, and a comment is the only thing holding
// it: the record is what keeps the branch observable if a caller ever starts
// reading the value.
func mirrorPts(ctx context.Context, q *db.Queries, log *slog.Logger, existing db.Message, toID int64) (int, error) {
	if PeerType(existing.PeerType) != PeerTypeUser || existing.PeerID != toID || existing.PeerLocalID == 0 {
		st, err := q.GetState(ctx, toID)
		if err != nil {
			return 0, fmt.Errorf("recipient state: %w", err)
		}
		log.Warn("dedup pts fallback",
			"path", "1:1 mirror",
			"from_id", existing.OwnerID,
			"to_id", toID,
			"random_id", existing.RandomID,
			"stored_local_id", existing.LocalID,
			"stored_peer_type", existing.PeerType,
			"stored_peer_id", existing.PeerID,
		)
		return int(st.Pts), nil
	}
	return newMessagePts(ctx, q, toID, existing.PeerLocalID)
}

// currentPts returns both owners' current pts without advancing them.
func currentPts(ctx context.Context, q *db.Queries, aID, bID int64) (int, int, error) {
	as, err := q.GetState(ctx, aID)
	if err != nil {
		return 0, 0, fmt.Errorf("state a: %w", err)
	}
	bs, err := q.GetState(ctx, bID)
	if err != nil {
		return 0, 0, fmt.Errorf("state b: %w", err)
	}
	return int(as.Pts), int(bs.Pts), nil
}

// lockOwners takes per-owner advisory locks in ascending id order (deadlock-free)
// so concurrent sends touching the same two owners serialize.
//
// The argument space is user ids only. Never pass a chat id: advisory locks are
// one flat int64 space, so chat 7 and user 7 would falsely serialize against each
// other. A chat serializes on its chats row lock instead, which is always taken
// before any of these and never inside one.
func lockOwners(ctx context.Context, tx pgx.Tx, ids ...int64) error {
	seen := make(map[int64]bool, len(ids))
	var uniq []int64
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	for i := 0; i < len(uniq); i++ {
		for j := i + 1; j < len(uniq); j++ {
			if uniq[j] < uniq[i] {
				uniq[i], uniq[j] = uniq[j], uniq[i]
			}
		}
	}
	for _, id := range uniq {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, id); err != nil {
			return fmt.Errorf("advisory lock %d: %w", id, err)
		}
	}
	return nil
}

// ForwardSource describes one message to forward, resolved from its source.
type ForwardSource struct {
	// FromID is the original author.
	FromID int64
	// Date is the original message date.
	Date time.Time
	// Text is the message body.
	Text string
	// ChannelID is non-zero when the source is a channel post.
	ChannelID int64
	// ChannelPost is the local_id of the channel post when the source is a channel.
	ChannelPost int32
	// FileID is the file attached to the source message; 0 = no media.
	FileID int64
}

// ForwardedMessage is one forwarded message returned by ForwardMessages, carrying
// the stored row and the pts at which it was inserted for the caller.
type ForwardedMessage struct {
	Message Message
	Pts     int
}

// ForwardMessages forwards one or more messages owned by userID to a destination.
// The destination is a 1:1 peer (peerType=PeerTypeUser) or a chat (peerType=PeerTypeChat).
// Each forwarded message is a new message row with FwdFrom populated.
// Returns the per-owner pts for each affected user and a slice of sent IDs.
//
// Ownership: the caller must own every source message (be its sender or a
// recipient/recipient-member). A missing message, one the caller does not own,
// or one in a secret chat returns ErrMessageInvalid.
//
// Dedup: a repeated random_id (per sender, per destination peer) returns the
// previously created forwarded message id without re-inserting.
func (s *Store) ForwardMessages(ctx context.Context, fromID int64, destPeerType PeerType, destPeerID int64, sources []ForwardSource, randomIDs []int64) (map[int64]int, []ForwardedMessage, error) {
	if len(sources) == 0 || len(randomIDs) != len(sources) {
		return nil, nil, ErrMessageInvalid
	}

	if destPeerType == PeerTypeUser && destPeerID == fromID {
		// Forwarding to self is not supported; the wire does not define it.
		return nil, nil, ErrMessageInvalid
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Collect all owners to lock: fromID + destPeerID (for 1:1) or chat members.
	var lockIDs []int64
	lockIDs = append(lockIDs, fromID)
	if destPeerType == PeerTypeUser {
		lockIDs = append(lockIDs, destPeerID)
	}

	// For chat destinations, we need the member set.
	var chatMembers map[int64]bool
	if destPeerType == PeerTypeChat {
		// Lock the chat row first.
		if _, err = qtx.ChatByIDForUpdate(ctx, destPeerID); errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotMember
		} else if err != nil {
			return nil, nil, fmt.Errorf("lock chat: %w", err)
		}
		// Check sender membership.
		member, e := qtx.IsChatMember(ctx, db.IsChatMemberParams{ChatID: destPeerID, UserID: fromID})
		if e != nil {
			return nil, nil, fmt.Errorf("is chat member: %w", e)
		}
		if !member {
			return nil, nil, ErrNotMember
		}
		parts, e := qtx.ChatParticipants(ctx, destPeerID)
		if e != nil {
			return nil, nil, fmt.Errorf("chat participants: %w", e)
		}
		chatMembers = make(map[int64]bool, len(parts))
		for _, p := range parts {
			chatMembers[p.UserID] = true
			lockIDs = append(lockIDs, p.UserID)
		}
	}

	if err = lockOwners(ctx, tx, lockIDs...); err != nil {
		return nil, nil, err
	}

	// Ensure state for all affected owners.
	if destPeerType == PeerTypeUser {
		if err = qtx.EnsureUpdateState(ctx, fromID); err != nil {
			return nil, nil, fmt.Errorf("ensure sender state: %w", err)
		}
		if err = qtx.EnsureUpdateState(ctx, destPeerID); err != nil {
			return nil, nil, fmt.Errorf("ensure dest state: %w", err)
		}
	} else {
		for uid := range chatMembers {
			if err = qtx.EnsureUpdateState(ctx, uid); err != nil {
				return nil, nil, fmt.Errorf("ensure state %d: %w", uid, err)
			}
		}
	}

	perOwner := make(map[int64]int)
	var sentMsgs []ForwardedMessage

	for i, src := range sources {
		randomID := randomIDs[i]

		// Dedup: check if this random_id was already used for a forward to this
		// destination by this sender.
		if randomID != 0 {
			existing, e := qtx.MessageByRandomID(ctx, db.MessageByRandomIDParams{
				OwnerID: fromID, RandomID: randomID,
			})
			switch {
			case e == nil:
				// Already forwarded — return the existing message at the pts it
				// occupies, never the sender's current one, for the reason
				// SendMessage's dedup branch records.
				pts, e2 := newMessagePts(ctx, qtx, fromID, existing.LocalID)
				if e2 != nil {
					return nil, nil, e2
				}
				sentMsgs = append(sentMsgs, ForwardedMessage{Message: messageFromRow(existing), Pts: pts})
				continue
			case !errors.Is(e, pgx.ErrNoRows):
				return nil, nil, fmt.Errorf("random_id lookup: %w", e)
			}
		}

		// Build forwarding fields.
		fwdFromID := src.FromID
		fwdDate := pgtype.Timestamptz{Time: src.Date, Valid: true}
		var fwdChannelID *int64
		var fwdChannelPost *int32
		if src.ChannelID != 0 {
			fwdChannelID = &src.ChannelID
			fwdChannelPost = &src.ChannelPost
		}

		if destPeerType == PeerTypeUser {
			// 1:1 forward: insert sender + recipient rows.
			sb, err := qtx.BumpState(ctx, fromID)
			if err != nil {
				return nil, nil, fmt.Errorf("bump sender: %w", err)
			}
			rb, err := qtx.BumpState(ctx, destPeerID)
			if err != nil {
				return nil, nil, fmt.Errorf("bump dest: %w", err)
			}

			if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
				OwnerID: fromID, LocalID: sb.LocalID, PeerType: int16(destPeerType), PeerID: destPeerID,
				FromID: fromID, Message: src.Text, Out: true, RandomID: randomID,
				PeerLocalID: rb.LocalID, FanoutID: 0, ActionType: 0, ActionUserID: 0,
				FileID: src.FileID, ReplyToMsgID: nil,
				FwdFromID: &fwdFromID, FwdDate: fwdDate, FwdChannelID: fwdChannelID, FwdChannelPost: fwdChannelPost,
			}); err != nil {
				return nil, nil, fmt.Errorf("insert sender forward: %w", err)
			}
			if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
				OwnerID: destPeerID, LocalID: rb.LocalID, PeerType: int16(destPeerType), PeerID: fromID,
				FromID: fromID, Message: src.Text, Out: false, RandomID: 0,
				PeerLocalID: sb.LocalID, FanoutID: 0, ActionType: 0, ActionUserID: 0,
				FileID: src.FileID, ReplyToMsgID: nil,
				FwdFromID: &fwdFromID, FwdDate: fwdDate, FwdChannelID: fwdChannelID, FwdChannelPost: fwdChannelPost,
			}); err != nil {
				return nil, nil, fmt.Errorf("insert dest forward: %w", err)
			}
			if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: fromID, Pts: sb.Pts, Type: int16(EventNewMessage), LocalID: sb.LocalID}); err != nil {
				return nil, nil, fmt.Errorf("sender event: %w", err)
			}
			if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: destPeerID, Pts: rb.Pts, Type: int16(EventNewMessage), LocalID: rb.LocalID}); err != nil {
				return nil, nil, fmt.Errorf("dest event: %w", err)
			}
			if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: fromID, PeerType: int16(destPeerType), PeerID: destPeerID, TopMessage: sb.LocalID, UnreadCount: 0}); err != nil {
				return nil, nil, fmt.Errorf("sender dialog: %w", err)
			}
			if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: destPeerID, PeerType: int16(destPeerType), PeerID: fromID, TopMessage: rb.LocalID, UnreadCount: 1}); err != nil {
				return nil, nil, fmt.Errorf("dest dialog: %w", err)
			}
			perOwner[fromID] = int(sb.Pts)
			perOwner[destPeerID] = int(rb.Pts)

			// Reload the sender's copy for the reply.
			stored, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: fromID, LocalID: sb.LocalID})
			if err != nil {
				return nil, nil, fmt.Errorf("reload sender forward: %w", err)
			}
			sentMsgs = append(sentMsgs, ForwardedMessage{Message: messageFromRow(stored), Pts: int(sb.Pts)})
		} else {
			// Chat forward: fan out to all members.
			fanoutID, err := qtx.NextFanoutID(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("next fanout id: %w", err)
			}
			var senderLocalID int64
			var senderPts int64
			for uid := range chatMembers {
				b, err := qtx.BumpState(ctx, uid)
				if err != nil {
					return nil, nil, fmt.Errorf("bump %d: %w", uid, err)
				}
				out := uid == fromID
				rID := int64(0)
				unread := int32(1)
				if out {
					rID = randomID
					unread = 0
					senderLocalID = b.LocalID
					senderPts = b.Pts
				}
				if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
					OwnerID: uid, LocalID: b.LocalID, PeerType: int16(PeerTypeChat), PeerID: destPeerID,
					FromID: fromID, Message: src.Text, Out: out, RandomID: rID,
					PeerLocalID: 0, FanoutID: fanoutID, ActionType: 0, ActionUserID: 0,
					FileID: src.FileID, ReplyToMsgID: nil,
					FwdFromID: &fwdFromID, FwdDate: fwdDate, FwdChannelID: fwdChannelID, FwdChannelPost: fwdChannelPost,
				}); err != nil {
					return nil, nil, fmt.Errorf("insert forward %d: %w", uid, err)
				}
				if err = qtx.InsertEvent(ctx, db.InsertEventParams{OwnerID: uid, Pts: b.Pts, Type: int16(EventNewMessage), LocalID: b.LocalID}); err != nil {
					return nil, nil, fmt.Errorf("event %d: %w", uid, err)
				}
				if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{OwnerID: uid, PeerType: int16(PeerTypeChat), PeerID: destPeerID, TopMessage: b.LocalID, UnreadCount: unread}); err != nil {
					return nil, nil, fmt.Errorf("dialog %d: %w", uid, err)
				}
				perOwner[uid] = int(b.Pts)
			}
			if senderLocalID != 0 {
				// Reload the sender's copy for the reply.
				stored, err := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: fromID, LocalID: senderLocalID})
				if err != nil {
					return nil, nil, fmt.Errorf("reload sender forward: %w", err)
				}
				sentMsgs = append(sentMsgs, ForwardedMessage{Message: messageFromRow(stored), Pts: int(senderPts)})
			}
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit: %w", err)
	}
	return perOwner, sentMsgs, nil
}
