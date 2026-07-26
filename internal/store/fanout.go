package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ChatAction classifies a chat message. 0 is a plain text message; the rest are
// the service messages M6 emits. Values match messages.action_type.
type ChatAction int16

const (
	ChatActionNone       ChatAction = 0
	ChatActionCreate     ChatAction = 1
	ChatActionAddUser    ChatAction = 2
	ChatActionDeleteUser ChatAction = 3
	ChatActionEditTitle  ChatAction = 4
)

// FanOut is one chat write to deliver to every member.
type FanOut struct {
	ChatID       int64
	FromID       int64
	Text         string // message body, or the title for Create/EditTitle
	Action       ChatAction
	ActionUserID int64 // subject of AddUser/DeleteUser; 0 otherwise
	RandomID     int64 // sender dedup token; 0 for service messages

	// Extra is a member id to include in this fan-out even though the chat's
	// current member set may not contain them — needed so a removed user
	// receives the service message announcing their own removal. Nil otherwise.
	//
	// It writes a message row, an event and a pts bump into a user id that is
	// not in the member set, which makes it an arbitrary-write primitive by
	// construction. Two constraints hold it shut: it is server-set only and is
	// never derived from request input, and it is deduped against the member set
	// so no owner can take two rows out of one fan-out.
	Extra []int64
}

// SendChatMessage writes one chat message to every member of the chat: one
// messages row, one message_events row and one pts bump per member, all in one
// transaction, all sharing one fanout_id. Returns the sender's own stored copy,
// each member's resulting pts keyed by user id, and dup=true for a repeated
// RandomID.
//
// The sender's copy is the zero Message when FromID is not among the recipients,
// which happens only for a service message announcing its own sender's removal.
func (s *Store) SendChatMessage(ctx context.Context, f FanOut) (sender Message, perOwner map[int64]int, dup bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Message{}, nil, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	sender, perOwner, dup, err = fanOut(ctx, tx, s.q.WithTx(tx), f)
	if err != nil {
		return Message{}, nil, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Message{}, nil, false, fmt.Errorf("commit: %w", err)
	}
	return sender, perOwner, dup, nil
}

// fanOut is the in-transaction fan-out primitive. It is what a caller that has
// already opened a transaction composes with — MAIN-49's membership mutations
// change the member set and announce the change in one transaction, so they call
// this after their own write rather than going through SendChatMessage.
//
// Lock order, and why it is this way round:
//
//   - The chats row is taken FOR UPDATE first, the member set is read under it,
//     and only then are the per-owner advisory locks taken, ascending. Reading
//     the member set before the lock would let a send that is already in flight
//     deliver to a member whose removal has since committed — message content
//     to a non-member, which is the one thing chat membership exists to prevent.
//     The opposite race, a member added mid-send receiving no row, is harmless
//     and consistent with M6 forwarding no history to a new member; nothing here
//     compensates for it.
//   - A chats row lock is never taken inside an advisory lock, and one
//     transaction locks at most one chats row. A caller that already holds this
//     chat's row lock pays one redundant no-op FOR UPDATE, not a second lock.
//
// Sender membership (the F4 exception), and why it is derived rather than a flag:
// a plain text message is the only fan-out a client causes directly, and the
// handler's membership check ran in a different transaction, so a removal can
// commit between it and this write — the re-check under the chats row lock is the
// actual authorization boundary. A service message carries a non-zero Action, is
// constructed by the server, and its caller has already taken this chat's row
// lock and made its own authorization decision under it; checking those here as
// well would reject the announcement of a self-removal, whose sender is
// deliberately no longer a member by the time it is written. Keying the check on
// Action rather than on a RequireSenderMember field means no caller can open the
// hole by forgetting to ask for it, at the cost of every future non-text action
// owing its own in-transaction check.
func fanOut(ctx context.Context, tx pgx.Tx, qtx *db.Queries, f FanOut) (sender Message, perOwner map[int64]int, dup bool, err error) {
	if f.ChatID == 0 || f.FromID == 0 {
		return Message{}, nil, false, ErrMessageInvalid
	}

	// An absent chat and a chat the sender is not in report the same error: the
	// pair is what keeps chat ids unprobeable over a dense id space.
	if _, err = qtx.ChatByIDForUpdate(ctx, f.ChatID); errors.Is(err, pgx.ErrNoRows) {
		return Message{}, nil, false, ErrNotMember
	} else if err != nil {
		return Message{}, nil, false, fmt.Errorf("lock chat: %w", err)
	}

	parts, err := qtx.ChatParticipants(ctx, f.ChatID)
	if err != nil {
		return Message{}, nil, false, fmt.Errorf("chat participants: %w", err)
	}
	owners := make([]int64, 0, len(parts)+len(f.Extra))
	seen := make(map[int64]bool, len(parts)+len(f.Extra))
	for _, p := range parts {
		seen[p.UserID] = true
		owners = append(owners, p.UserID)
	}
	// Sender membership, before Extra is merged in and before any advisory lock.
	// Reading it off the member set rather than a second IsChatMember query keeps
	// one read as the source of truth, and placing it here has two consequences
	// that are the point rather than a side effect: Extra can never vouch for its
	// own sender, and a non-member neither pays for nor serialises anyone behind
	// up to 200 advisory locks before being turned away. The chats row lock, not
	// the advisory locks, is what makes this read authoritative.
	if f.Action == ChatActionNone && !seen[f.FromID] {
		return Message{}, nil, false, ErrNotMember
	}
	for _, id := range f.Extra {
		if id == 0 || seen[id] {
			continue
		}
		seen[id] = true
		owners = append(owners, id)
	}
	if len(owners) == 0 {
		return Message{}, nil, false, ErrNotMember
	}
	// The cap bounds this transaction's row, event, lock and round-trip count.
	// The membership mutations enforce it too; this is the last place it can be
	// caught before an unbounded transaction is already open.
	if len(owners) > maxChatParticipants {
		return Message{}, nil, false, ErrChatFull
	}

	if err = lockOwners(ctx, tx, owners...); err != nil {
		return Message{}, nil, false, err
	}

	// Idempotency: a resend with the same random_id returns the original. The
	// token lives on the sender's copy only, so the 1:1 contract carries over
	// unchanged — no new rows, no new events, no pts movement.
	if f.RandomID != 0 {
		existing, e := qtx.MessageByRandomID(ctx, db.MessageByRandomIDParams{OwnerID: f.FromID, RandomID: f.RandomID})
		switch {
		case e == nil:
			pts, e2 := ptsFor(ctx, qtx, owners)
			if e2 != nil {
				return Message{}, nil, false, e2
			}
			return messageFromRow(existing), pts, true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return Message{}, nil, false, fmt.Errorf("random_id lookup: %w", e)
		}
	}

	fanoutID, err := qtx.NextFanoutID(ctx)
	if err != nil {
		return Message{}, nil, false, fmt.Errorf("next fanout id: %w", err)
	}

	perOwner = make(map[int64]int, len(owners))
	var senderLocalID int64
	for _, owner := range owners {
		if err = qtx.EnsureUpdateState(ctx, owner); err != nil {
			return Message{}, nil, false, fmt.Errorf("ensure state %d: %w", owner, err)
		}
		var b db.BumpStateRow
		if b, err = qtx.BumpState(ctx, owner); err != nil {
			return Message{}, nil, false, fmt.Errorf("bump %d: %w", owner, err)
		}

		// The sender authored the message wherever it lands, so from_id and the
		// chat peer are identical on every copy; only out and the dedup token
		// distinguish the sender's own row.
		out := owner == f.FromID
		randomID := int64(0)
		unread := int32(1)
		if out {
			randomID = f.RandomID
			unread = 0
			senderLocalID = b.LocalID
		}
		if err = qtx.InsertMessage(ctx, db.InsertMessageParams{
			OwnerID: owner, LocalID: b.LocalID, PeerType: int16(PeerTypeChat), PeerID: f.ChatID, FromID: f.FromID,
			Message: f.Text, Out: out, RandomID: randomID, PeerLocalID: 0,
			FanoutID: fanoutID, ActionType: int16(f.Action), ActionUserID: f.ActionUserID,
		}); err != nil {
			return Message{}, nil, false, fmt.Errorf("insert message %d: %w", owner, err)
		}
		if err = qtx.InsertEvent(ctx, db.InsertEventParams{
			OwnerID: owner, Pts: b.Pts, Type: int16(EventNewMessage), LocalID: b.LocalID,
		}); err != nil {
			return Message{}, nil, false, fmt.Errorf("event %d: %w", owner, err)
		}
		if err = qtx.UpsertDialog(ctx, db.UpsertDialogParams{
			OwnerID: owner, PeerType: int16(PeerTypeChat), PeerID: f.ChatID,
			TopMessage: b.LocalID, UnreadCount: unread,
		}); err != nil {
			return Message{}, nil, false, fmt.Errorf("dialog %d: %w", owner, err)
		}
		perOwner[owner] = int(b.Pts)
	}

	if senderLocalID != 0 {
		stored, e := qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: f.FromID, LocalID: senderLocalID})
		if e != nil {
			return Message{}, nil, false, fmt.Errorf("reload sender message: %w", e)
		}
		sender = messageFromRow(stored)
	}
	return sender, perOwner, false, nil
}

// ptsFor reads each owner's current pts without advancing it.
func ptsFor(ctx context.Context, qtx *db.Queries, owners []int64) (map[int64]int, error) {
	out := make(map[int64]int, len(owners))
	for _, owner := range owners {
		st, err := qtx.GetState(ctx, owner)
		if err != nil {
			return nil, fmt.Errorf("state %d: %w", owner, err)
		}
		out[owner] = int(st.Pts)
	}
	return out, nil
}
