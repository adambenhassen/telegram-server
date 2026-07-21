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
// placeholder; self marks the update recipient's own account.
func userToTL(u store.User, self bool) *tg.User {
	return &tg.User{
		ID:         u.ID,
		Self:       self,
		Phone:      u.Phone,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		AccessHash: u.ID,
	}
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

// buildUpdates hydrates userID's events after fromPts into wire updates plus the
// referenced users and the user's current state. It is the single delivery path
// shared by updates.getDifference and real-time push.
func (h *handlers) buildUpdates(ctx context.Context, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, store.State, error) {
	events, err := h.store.EventsSince(ctx, userID, fromPts)
	if err != nil {
		return nil, nil, store.State{}, err
	}
	state, err := h.store.State(ctx, userID)
	if err != nil {
		return nil, nil, store.State{}, err
	}

	var ups []tg.UpdateClass
	peers := map[int64]bool{}
	for _, ev := range events {
		up, refs, err := h.eventToUpdate(ctx, userID, ev)
		if err != nil {
			return nil, nil, store.State{}, err
		}
		if up == nil {
			continue
		}
		ups = append(ups, up)
		for _, id := range refs {
			peers[id] = true
		}
	}

	users, err := h.loadUsers(ctx, peers, userID)
	if err != nil {
		return nil, nil, store.State{}, err
	}
	return ups, users, state, nil
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
	ups, users, state, err := h.buildUpdates(r.Ctx, r.UserID, req.Pts)
	if err != nil {
		h.log.Error("get difference", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if req.Pts >= state.Pts {
		return &tg.UpdatesDifferenceEmpty{Date: state.Date, Seq: state.Seq}, nil
	}
	diff := &tg.UpdatesDifference{State: *stateToTL(state), Users: users}
	for _, u := range ups {
		if nm, ok := u.(*tg.UpdateNewMessage); ok {
			diff.NewMessages = append(diff.NewMessages, nm.Message)
		} else {
			diff.OtherUpdates = append(diff.OtherUpdates, u)
		}
	}
	return diff, nil
}
