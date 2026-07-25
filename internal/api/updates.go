package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// messageToTL maps a stored message to the wire tg.Message. Peer/from are
// PeerUser; ids are cast to the wire int space. EditDate populates its flag via
// tg.Message.SetFlags at encode time.
func messageToTL(m store.Message) *tg.Message {
	msg := &tg.Message{
		ID:      int(m.LocalID),
		Out:     m.Out,
		PeerID:  &tg.PeerUser{UserID: m.PeerID},
		FromID:  &tg.PeerUser{UserID: m.FromID},
		Message: m.Text,
		Date:    int(m.Date.Unix()),
	}
	if m.EditDate != nil {
		msg.EditDate = int(m.EditDate.Unix())
	}
	return msg
}

// userToTL maps a stored user to the wire tg.User. AccessHash is the M1 self-id
// placeholder; self marks the update recipient's own account. The phone number
// is private to its owner, so it is emitted only on the self entry — names stay
// for every peer, since a client needs them to render a conversation.
func userToTL(u store.User, self bool) *tg.User {
	tlUser := &tg.User{
		ID:         u.ID,
		Self:       self,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		AccessHash: u.ID,
	}
	if self {
		tlUser.Phone = u.Phone
	}
	return tlUser
}

func stateToTL(s store.State) *tg.UpdatesState {
	return &tg.UpdatesState{
		Pts:         s.Pts,
		Qts:         0,
		Date:        s.Date,
		Seq:         s.Seq,
		UnreadCount: s.UnreadCount,
	}
}

// maxDiffEvents caps the events hydrated into one difference/push, bounding the
// work a stale client can force. A truncated batch is returned as a slice with
// an intermediate state so the client re-requests the remainder.
const maxDiffEvents = 500

// buildUpdates hydrates userID's events after fromPts into wire updates plus the
// referenced users and the state to advertise. It is the single delivery path
// shared by updates.getDifference and real-time push.
//
// State is read first, then events are bounded to (fromPts, state.pts], so the
// advertised pts never runs past an event omitted from the response (events and
// their pts bump commit atomically per owner). more reports that the batch hit
// the cap and the advertised state is an intermediate one.
func (h *handlers) buildUpdates(ctx context.Context, userID int64, fromPts int) (ups []tg.UpdateClass, users []tg.UserClass, state store.State, more bool, err error) {
	state, err = h.store.State(ctx, userID)
	if err != nil {
		return nil, nil, store.State{}, false, err
	}
	// Fetch one past the cap to detect truncation.
	events, err := h.store.EventsWindow(ctx, userID, fromPts, state.Pts, maxDiffEvents+1)
	if err != nil {
		return nil, nil, store.State{}, false, err
	}
	if len(events) > maxDiffEvents {
		events = events[:maxDiffEvents]
		more = true
	}

	peers := map[int64]bool{}
	for _, ev := range events {
		up, refs, uerr := h.eventToUpdate(ctx, userID, ev)
		if uerr != nil {
			return nil, nil, store.State{}, false, uerr
		}
		if up == nil {
			continue
		}
		ups = append(ups, up)
		for _, id := range refs {
			peers[id] = true
		}
	}
	// When truncated, advertise only through the last included event's pts.
	if more && len(events) > 0 {
		state.Pts = events[len(events)-1].Pts
	}

	users, err = h.loadUsers(ctx, peers, userID)
	if err != nil {
		return nil, nil, store.State{}, false, err
	}
	return ups, users, state, more, nil
}

// eventToUpdate builds the wire update for one event owned by userID, returning
// the update and the user ids it references. A nil update (message vanished, or
// an empty read marker) is skipped by the caller.
func (h *handlers) eventToUpdate(ctx context.Context, userID int64, ev store.Event) (tg.UpdateClass, []int64, error) {
	switch ev.Type {
	case store.EventNewMessage, store.EventEdit:
		m, ok, err := h.store.MessageByOwnerLocal(ctx, userID, ev.LocalID)
		if err != nil || !ok {
			return nil, nil, err
		}
		tlMsg := messageToTL(m)
		refs := []int64{m.FromID, m.PeerID}
		if ev.Type == store.EventEdit {
			return &tg.UpdateEditMessage{Message: tlMsg, Pts: ev.Pts, PtsCount: 1}, refs, nil
		}
		return &tg.UpdateNewMessage{Message: tlMsg, Pts: ev.Pts, PtsCount: 1}, refs, nil

	case store.EventDelete:
		return &tg.UpdateDeleteMessages{Messages: []int{int(ev.LocalID)}, Pts: ev.Pts, PtsCount: 1}, nil, nil

	case store.EventReadIn, store.EventReadOut:
		if ev.LocalID == 0 {
			return nil, nil, nil
		}
		m, ok, err := h.store.MessageByOwnerLocal(ctx, userID, ev.LocalID)
		if err != nil || !ok {
			return nil, nil, err
		}
		peer := &tg.PeerUser{UserID: m.PeerID}
		if ev.Type == store.EventReadOut {
			return &tg.UpdateReadHistoryOutbox{Peer: peer, MaxID: int(ev.LocalID), Pts: ev.Pts, PtsCount: 1}, []int64{m.PeerID}, nil
		}
		return &tg.UpdateReadHistoryInbox{Peer: peer, MaxID: int(ev.LocalID), StillUnreadCount: 0, Pts: ev.Pts, PtsCount: 1}, []int64{m.PeerID}, nil

	default:
		return nil, nil, nil
	}
}

// loadUsers hydrates the given user ids into wire users, marking selfID as Self.
func (h *handlers) loadUsers(ctx context.Context, ids map[int64]bool, selfID int64) ([]tg.UserClass, error) {
	users := make([]tg.UserClass, 0, len(ids))
	for id := range ids {
		u, ok, err := h.store.UserByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		users = append(users, userToTL(u, id == selfID))
	}
	return users, nil
}

// handleGetState serves updates.getState.
func (h *handlers) handleGetState(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	st, err := h.store.State(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get state", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return stateToTL(st), nil
}

// handleGetDifference serves updates.getDifference: it replays the caller's
// events after the client's pts. When the client is caught up it returns
// differenceEmpty; a client pts ahead of the server (impossible under a single
// writer) is clamped to empty rather than trusted.
func (h *handlers) handleGetDifference(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UpdatesGetDifferenceRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	ups, users, state, more, err := h.buildUpdates(r.Ctx, r.UserID, req.Pts)
	if err != nil {
		h.log.Error("get difference", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// No pending events (caught up, or a client pts at/ahead of the server which
	// the single-writer invariant clamps to empty): report empty.
	if !more && len(ups) == 0 {
		return &tg.UpdatesDifferenceEmpty{Date: state.Date, Seq: state.Seq}, nil
	}

	var newMessages []tg.MessageClass
	var other []tg.UpdateClass
	for _, u := range ups {
		if nm, ok := u.(*tg.UpdateNewMessage); ok {
			newMessages = append(newMessages, nm.Message)
		} else {
			other = append(other, u)
		}
	}
	if more {
		// Partial batch: the client applies it and re-requests from the
		// intermediate state's pts until caught up.
		return &tg.UpdatesDifferenceSlice{
			NewMessages:       newMessages,
			OtherUpdates:      other,
			Users:             users,
			IntermediateState: *stateToTL(state),
		}, nil
	}
	return &tg.UpdatesDifference{
		NewMessages:  newMessages,
		OtherUpdates: other,
		Users:        users,
		State:        *stateToTL(state),
	}, nil
}
