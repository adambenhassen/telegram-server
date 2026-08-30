package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// A chats row lock (SELECT ... FROM chats WHERE id = $1 FOR UPDATE) is the
// serialisation point for everything that touches one chat's member set. It is
// always taken BEFORE any lockOwners advisory lock and never inside one, and a
// transaction locks at most one chats row. lockOwners' argument space is user
// ids only — never pass a chat id, or chat 7 and user 7 falsely serialise.
//
// CreateChat does not take it — the chat does not exist yet. The fan-out in
// fanout.go takes it, and every membership mutation acquires it after
// beginChatMutation's row-lock prelude and its operation-specific authority
// check, before any write or fan-out.

// maxChatParticipants caps a basic chat at Telegram's own limit. It is what
// bounds the fan-out transaction: one send to a full chat writes 200 message
// rows, 200 events and 200 pts bumps under 200 advisory locks.
const maxChatParticipants = 200

// Chat is a basic group chat.
type Chat struct {
	ID              int64
	Title           string
	CreatorID       int64
	Version         int
	Date            time.Time
	PinnedMessageID *int32
}

// Participant is one member of a chat.
type Participant struct {
	UserID    int64
	InviterID int64
	Date      time.Time
}

func chatFromRow(r db.Chat) Chat {
	return Chat{
		ID:              r.ID,
		Title:           r.Title,
		CreatorID:       r.CreatorID,
		Version:         int(r.Version),
		Date:            r.Date.Time,
		PinnedMessageID: r.PinnedMessageID,
	}
}

// CreateChat creates a chat owned by creatorID with the creator and each
// unblocked memberID as participants, all in one transaction. Duplicate ids in
// memberIDs and creatorID appearing in memberIDs are deduped rather than
// rejected. ErrChatFull if the deduped requested member count exceeds
// maxChatParticipants; no rows are written then.
func (s *Store) CreateChat(ctx context.Context, creatorID int64, title string, memberIDs []int64) (Chat, error) {
	// The cap bounds the allocation, not just the transaction: memberIDs arrives
	// from a client vector and is only capped at the transport frame size, so
	// neither the deduped set nor its index may be sized from it. The set only
	// grows, so stopping at the first id past the cap decides ErrChatFull on the
	// same condition the full scan would.
	members := make([]int64, 0, maxChatParticipants+1)
	seen := make(map[int64]bool, maxChatParticipants+1)
	members = append(members, creatorID)
	seen[creatorID] = true
	for _, id := range memberIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		members = append(members, id)
		if len(members) > maxChatParticipants {
			return Chat{}, ErrChatFull
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Chat{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// There is no chats row lock here: the chat does not exist yet. The same
	// sorted owner locks used by BlockUser are the serialization point for the
	// directed block check below. They are taken before any participant row is
	// written, so a block that committed before this pass cannot be missed.
	if err := lockOwners(ctx, tx, members...); err != nil {
		return Chat{}, err
	}
	filtered := members[:0]
	for _, id := range members {
		if id != creatorID {
			blocked, err := qtx.IsBlocked(ctx, db.IsBlockedParams{
				BlockerID: id,
				BlockedID: creatorID,
			})
			if err != nil {
				return Chat{}, fmt.Errorf("check blocked invitee %d: %w", id, err)
			}
			if blocked {
				continue
			}
		}
		filtered = append(filtered, id)
	}
	members = filtered

	row, err := qtx.InsertChat(ctx, db.InsertChatParams{Title: title, CreatorID: creatorID})
	if err != nil {
		return Chat{}, fmt.Errorf("insert chat: %w", err)
	}
	for _, id := range members {
		if err = qtx.InsertChatParticipant(ctx, db.InsertChatParticipantParams{
			ChatID: row.ID, UserID: id, InviterID: creatorID,
		}); err != nil {
			return Chat{}, fmt.Errorf("insert participant %d: %w", id, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return Chat{}, fmt.Errorf("commit: %w", err)
	}
	return chatFromRow(row), nil
}

// ChatByID returns one chat; ok=false when absent.
func (s *Store) ChatByID(ctx context.Context, chatID int64) (Chat, bool, error) {
	r, err := s.q.ChatByID(ctx, chatID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Chat{}, false, nil
	case err != nil:
		return Chat{}, false, fmt.Errorf("chat by id: %w", err)
	}
	return chatFromRow(r), true, nil
}

// Participants lists the chat's members ordered by user_id ascending.
func (s *Store) Participants(ctx context.Context, chatID int64) ([]Participant, error) {
	rows, err := s.q.ChatParticipants(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("chat participants: %w", err)
	}
	out := make([]Participant, len(rows))
	for i, r := range rows {
		out[i] = Participant{UserID: r.UserID, InviterID: r.InviterID, Date: r.Date.Time}
	}
	return out, nil
}

// IsMember reports whether userID is a participant of chatID. This is the whole
// authorization boundary for the chat RPCs: an unknown chat id and a non-member
// both report false, and nothing but a chat_participants row makes it true.
func (s *Store) IsMember(ctx context.Context, chatID, userID int64) (bool, error) {
	ok, err := s.q.IsChatMember(ctx, db.IsChatMemberParams{ChatID: chatID, UserID: userID})
	if err != nil {
		return false, fmt.Errorf("is chat member: %w", err)
	}
	return ok, nil
}

// chatMutation is the open transaction a membership change runs in: the chats
// row locked FOR UPDATE and the member set read under that lock. Its operation
// acquires the needed advisory locks after checking operation-specific authority.
type chatMutation struct {
	tx        pgx.Tx
	qtx       *db.Queries
	creatorID int64
	members   []int64        // ascending, as read under the chats row lock
	seen      map[int64]bool // membership of members, for O(1) tests
}

// beginChatMutation opens the transaction AddChatUser, RemoveChatUser and
// SetChatTitle share, and is where their separate obligations are discharged
// once instead of three times.
//
// F4: the caller's membership is re-checked here, inside this transaction and
// under the chats row lock. The handler's check ran in a different transaction
// and is an early error only — a caller removed in the window between the two
// would otherwise get one action through. It is also the only membership check
// these mutations get downstream: fanOut deliberately skips the sender check for
// a non-zero Action, so that a self-removal can announce itself. An outsider is
// filtered out before the row lock is taken, but that filter decides nothing —
// see both comments in the body. The locked creator id is returned with the
// mutation so RemoveChatUser and SetChatTitle make their authority decisions
// under the same lock as their writes.
//
// No owner advisory lock is taken here. The operation-specific authority checks
// must run first, so a refused member cannot hold a lock set proportional to
// the chat size. An authorized operation then calls chatMutation.lockOwners
// once with every owner it will touch, including any added or removed user,
// before mutation and fan-out. Taking that whole set in one sorted pass is what
// keeps concurrent removals from deadlocking; removeParticipant and fanOut only
// reacquire subsets of the already-held set.
//
// The caller owns the transaction from here: it must roll back or commit.
func (s *Store) beginChatMutation(ctx context.Context, chatID, callerID int64) (*chatMutation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	qtx := s.q.WithTx(tx)

	ok := false
	defer func() {
		if !ok {
			_ = tx.Rollback(ctx) //nolint:errcheck // best effort on the error path
		}
	}()

	// Early reject, ahead of the chats row lock. A caller who takes that lock
	// holds it for the rest of the transaction, so letting a non-member reach it
	// hands an outsider two things: the members' own renames, adds and removals
	// serialised behind them, and a wait whose length answers whether the chat
	// exists — the timing half of the oracle the uniform ErrNotMember closes.
	// This is a filter and not the authorization decision: it reads outside the
	// row lock, so a caller removed after it passes still gets through, and the
	// re-check below decides. Same error either way, so an absent chat stays
	// indistinguishable from one the caller is not in.
	member, err := qtx.IsChatMember(ctx, db.IsChatMemberParams{ChatID: chatID, UserID: callerID})
	if err != nil {
		return nil, fmt.Errorf("is chat member: %w", err)
	}
	if !member {
		return nil, ErrNotMember
	}

	// An absent chat and a chat the caller is not in report the same error, as
	// fanOut does: the pair is what keeps chat ids unprobeable over a dense id
	// space, and a distinct "no such chat" here would reopen that oracle from the
	// mutation side.
	chat, err := qtx.ChatByIDForUpdate(ctx, chatID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotMember
	} else if err != nil {
		return nil, fmt.Errorf("lock chat: %w", err)
	}

	// Read under the lock, and this is the membership authorization decision. Store.Participants
	// and Store.IsMember run on the pool, outside this transaction's snapshot and
	// outside the row lock, so a re-check through either would be decoration — and
	// so would the early reject above if it were read as replacing this one. It
	// runs on qtx and still ahead of the lock, which is what makes it a filter:
	// it can turn away a caller who was never a member, and nothing else.
	parts, err := qtx.ChatParticipants(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("chat participants: %w", err)
	}
	m := &chatMutation{
		tx:        tx,
		qtx:       qtx,
		creatorID: chat.CreatorID,
		members:   make([]int64, len(parts)),
		seen:      make(map[int64]bool, len(parts)),
	}
	for i, p := range parts {
		m.members[i] = p.UserID
		m.seen[p.UserID] = true
	}
	if !m.seen[callerID] {
		return nil, ErrNotMember
	}

	ok = true
	return m, nil
}

// lockOwners acquires every owner lock this mutation will touch in one sorted
// pass. Callers must invoke it only after their operation-specific authority
// checks and before any write or fan-out.
func (m *chatMutation) lockOwners(ctx context.Context, extra ...int64) error {
	return lockOwners(ctx, m.tx, append(append([]int64(nil), m.members...), extra...)...)
}

// removeParticipant deletes one chat_participants row and takes that user's
// per-owner advisory lock in the same transaction. It is the only path in the
// tree that deletes a chat_participants row, and that is the point.
//
// The invariant is: any transaction that deletes U's chat_participants row holds
// lockOwners(U) in that same transaction. EditMessage and DeleteMessages filter a
// chat message's copy set against the current member set so a member removed
// after the send keeps a frozen copy and receives no further content — and they
// take no chats row lock. Their membership read is authoritative for exactly one
// reason: a removal is assumed to hold the removed user's advisory lock, so it
// either committed before that read or is blocked behind the edit until the edit
// commits. A removal that commits without that lock lets an in-flight edit read a
// member set still containing U, then write edited content and a pts bump into
// U's row after U was removed — content delivered to a non-member.
//
// The obligation is on the delete, not on any announcement. Stated as "a removal
// announces with Extra" it breaks silently whenever a removal and its
// announcement split across transactions, whenever a removal path announces
// nothing (chat or account deletion, a purge, a self-leave someone decides needs
// no service message), or whenever an announcement's owner set omits U. Binding
// it to the delete survives all three.
//
// Advisory locks here are pg_advisory_xact_lock, held to commit, so the order of
// the delete and the announcement within the transaction does not matter. The
// caller must have acquired U along with every other owner in one sorted
// lockOwners pass — see chatMutation.lockOwners — because two acquisitions in one
// transaction is what reintroduces deadlock. The acquisition here is that set's
// subset and re-entrant; it is kept so the invariant holds for any future caller.
func removeParticipant(ctx context.Context, tx pgx.Tx, qtx *db.Queries, chatID, userID int64) (bool, error) {
	if err := lockOwners(ctx, tx, userID); err != nil {
		return false, err
	}
	n, err := qtx.DeleteChatParticipant(ctx, db.DeleteChatParticipantParams{ChatID: chatID, UserID: userID})
	if err != nil {
		return false, fmt.Errorf("delete participant: %w", err)
	}
	return n > 0, nil
}

// AddChatUser adds target to the chat and announces it, in one transaction.
// added=false means target was already a member: nothing was written, no service
// message was emitted, and chats.version did not move.
func (s *Store) AddChatUser(ctx context.Context, chatID, target, callerID int64) (added bool, sender Message, perOwner map[int64]int, err error) {
	m, err := s.beginChatMutation(ctx, chatID, callerID)
	if err != nil {
		return false, Message{}, nil, err
	}
	defer func() { _ = m.tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err = m.lockOwners(ctx, target); err != nil {
		return false, Message{}, nil, err
	}

	// Counting under the chats row lock is what stops two concurrent adds both
	// seeing 199. An add of an existing member changes no count, so it is not the
	// cap's business.
	if !m.seen[target] && len(m.members) >= maxChatParticipants {
		return false, Message{}, nil, ErrChatFull
	}
	if !m.seen[target] {
		blocked, err := m.qtx.IsBlocked(ctx, db.IsBlockedParams{
			BlockerID: target,
			BlockedID: callerID,
		})
		if err != nil {
			return false, Message{}, nil, fmt.Errorf("check blocked caller: %w", err)
		}
		if blocked {
			// A blocked add is the same refusal as every other add the caller
			// cannot make. Nothing has been written in this transaction yet.
			return false, Message{}, nil, ErrNotMember
		}
	}
	n, err := m.qtx.InsertChatParticipantIfAbsent(ctx, db.InsertChatParticipantIfAbsentParams{
		ChatID: chatID, UserID: target, InviterID: callerID,
	})
	if err != nil {
		return false, Message{}, nil, fmt.Errorf("insert participant: %w", err)
	}
	if n == 0 {
		// Already a member: no version bump, no service message, no pts movement.
		if err = m.tx.Commit(ctx); err != nil {
			return false, Message{}, nil, fmt.Errorf("commit: %w", err)
		}
		return false, Message{}, nil, nil
	}
	if _, err = m.qtx.BumpChatVersion(ctx, chatID); err != nil {
		return false, Message{}, nil, fmt.Errorf("bump version: %w", err)
	}

	// target is in the member set by now, so their copy comes for free — and that
	// copy is what puts the chat in their dialog list.
	sender, perOwner, _, err = fanOut(ctx, m.tx, m.qtx, s.log, FanOut{
		ChatID: chatID, FromID: callerID, Action: ChatActionAddUser, ActionUserID: target,
	})
	if err != nil {
		return false, Message{}, nil, err
	}
	if err = m.tx.Commit(ctx); err != nil {
		return false, Message{}, nil, fmt.Errorf("commit: %w", err)
	}
	return true, sender, perOwner, nil
}

// RemoveChatUser removes target and announces it to the remaining members and to
// target, in one transaction. removed=false means target was not a member.
//
// target == callerID is allowed for non-creators: it is how leaving a chat works,
// and it is the case fanOut's F4 exception exists for — the announcement's sender
// is, by the time it is written, deliberately not a member. The creator is never
// removable, including by the creator itself.
func (s *Store) RemoveChatUser(ctx context.Context, chatID, target, callerID int64) (removed bool, sender Message, perOwner map[int64]int, err error) {
	m, err := s.beginChatMutation(ctx, chatID, callerID)
	if err != nil {
		return false, Message{}, nil, err
	}
	defer func() { _ = m.tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	// Only the creator may remove another member. Any member may leave, but the
	// creator is never removable, including by the creator itself.
	if target == m.creatorID || (callerID != m.creatorID && target != callerID) {
		return false, Message{}, nil, ErrNotMember
	}
	if err = m.lockOwners(ctx, target); err != nil {
		return false, Message{}, nil, err
	}

	removed, err = removeParticipant(ctx, m.tx, m.qtx, chatID, target)
	if err != nil {
		return false, Message{}, nil, err
	}
	if !removed {
		if err = m.tx.Commit(ctx); err != nil {
			return false, Message{}, nil, fmt.Errorf("commit: %w", err)
		}
		return false, Message{}, nil, nil
	}
	if _, err = m.qtx.BumpChatVersion(ctx, chatID); err != nil {
		return false, Message{}, nil, fmt.Errorf("bump version: %w", err)
	}

	// Extra carries the announcement to target, who is out of the member set by
	// now — without it the removed client shows the chat forever.
	sender, perOwner, _, err = fanOut(ctx, m.tx, m.qtx, s.log, FanOut{
		ChatID: chatID, FromID: callerID, Action: ChatActionDeleteUser,
		ActionUserID: target, Extra: []int64{target},
	})
	if err != nil {
		return false, Message{}, nil, err
	}
	if err = m.tx.Commit(ctx); err != nil {
		return false, Message{}, nil, fmt.Errorf("commit: %w", err)
	}
	return true, sender, perOwner, nil
}

// SetChatTitle renames the chat and announces it, in one transaction. Only the
// creator may rename it; the authority check uses the locked chat row.
func (s *Store) SetChatTitle(ctx context.Context, chatID, callerID int64, title string) (chat Chat, sender Message, perOwner map[int64]int, err error) {
	m, err := s.beginChatMutation(ctx, chatID, callerID)
	if err != nil {
		return Chat{}, Message{}, nil, err
	}
	defer func() { _ = m.tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if callerID != m.creatorID {
		return Chat{}, Message{}, nil, ErrNotMember
	}
	if err = m.lockOwners(ctx); err != nil {
		return Chat{}, Message{}, nil, err
	}

	row, err := m.qtx.SetChatTitle(ctx, db.SetChatTitleParams{ID: chatID, Title: title})
	if err != nil {
		return Chat{}, Message{}, nil, fmt.Errorf("set title: %w", err)
	}
	sender, perOwner, _, err = fanOut(ctx, m.tx, m.qtx, s.log, FanOut{
		ChatID: chatID, FromID: callerID, Text: title, Action: ChatActionEditTitle,
	})
	if err != nil {
		return Chat{}, Message{}, nil, err
	}
	if err = m.tx.Commit(ctx); err != nil {
		return Chat{}, Message{}, nil, fmt.Errorf("commit: %w", err)
	}
	return chatFromRow(row), sender, perOwner, nil
}

// ChatsForUser returns every chat the user participates in.
func (s *Store) ChatsForUser(ctx context.Context, userID int64) ([]Chat, error) {
	rows, err := s.q.ChatsForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("chats for user: %w", err)
	}
	out := make([]Chat, len(rows))
	for i, r := range rows {
		out[i] = chatFromRow(r)
	}
	return out, nil
}

// ChatPinnedMessage returns the current pinned message id for chatID.
// Returns nil when no message is pinned.
func (s *Store) ChatPinnedMessage(ctx context.Context, chatID int64) (*int32, error) {
	id, err := s.q.GetChatPinnedMessage(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("chat pinned message: %w", err)
	}
	return id, nil
}

// SetChatPinnedMessage sets or clears the pinned message id on chatID.
// pinnedID is the local_id of the message to pin (identical across members for
// a given fanout). Passing nil clears the pin. The caller is responsible for
// having checked admin rights — this method does not authorise.
//
// When pinnedID is non-nil the message is validated inside this transaction:
// the caller's copy must exist, belong to chatID, and not be deleted. This
// prevents the TOCTOU window where a delete commits between an out-of-
// transaction check and the pin mutation.
//
// Returns the chat with updated pinned_message_id and version.
// Returns the member set (participant user ids) so the caller can fan out the
// update.
// ErrNotMember when no chat exists or caller is not a member — indistinguishable
// on purpose to keep chat ids unprobeable.
// ErrMessageInvalid when pinnedID names a message that does not exist, is
// deleted, or does not belong to chatID.
func (s *Store) SetChatPinnedMessage(ctx context.Context, chatID, callerID int64, pinnedID *int32) (chat Chat, members []int64, err error) {
	m, err := s.beginChatMutation(ctx, chatID, callerID)
	if err != nil {
		return Chat{}, nil, err
	}
	defer func() { _ = m.tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err = m.lockOwners(ctx); err != nil {
		return Chat{}, nil, err
	}

	// Validate the pinned message under the chats row lock so a concurrent
	// delete cannot slip between the check and the mutation.
	if pinnedID != nil {
		msg, err := m.qtx.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{
			OwnerID: callerID, LocalID: int64(*pinnedID),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return Chat{}, nil, ErrMessageInvalid
		}
		if err != nil {
			return Chat{}, nil, fmt.Errorf("validate chat pin: %w", err)
		}
		if msg.Deleted || PeerType(msg.PeerType) != PeerTypeChat || msg.PeerID != chatID {
			return Chat{}, nil, ErrMessageInvalid
		}
	}

	row, err := m.qtx.SetChatPinnedMessage(ctx, db.SetChatPinnedMessageParams{
		ID: chatID, PinnedMessageID: pinnedID,
	})
	if err != nil {
		return Chat{}, nil, fmt.Errorf("set chat pinned message: %w", err)
	}
	if err = m.tx.Commit(ctx); err != nil {
		return Chat{}, nil, fmt.Errorf("commit: %w", err)
	}
	return chatFromRow(row), m.members, nil
}
