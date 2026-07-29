package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	// maxChatTitle bounds the title a client may set. chats.title is an unbounded
	// TEXT column and every rename copies the title into one messages row per
	// member, so an unbounded title is a 200x write amplifier. The store
	// deliberately does not validate, so the bound belongs here.
	maxChatTitle = 255
	// maxChatUsers caps the invite vector before the store sees it. The store
	// bounds its own allocation and returns ErrChatFull regardless, so this is
	// defence in depth — but a Vector<InputUser> can carry ~1.3M ids inside
	// gotd's 16 MB frame, and rejecting here spares even the per-id lookups.
	maxChatUsers = 200
)

// chatTitle validates a client-supplied chat title and returns the trimmed form
// that gets stored. Empty, whitespace-only, over-length and text the server
// cannot store are one error: the client is not owed a distinction it cannot act
// on differently.
func chatTitle(raw string) (string, error) {
	title := strings.TrimSpace(raw)
	if title == "" || !validText(title) || utf8.RuneCountInString(title) > maxChatTitle {
		return "", errChatTitleInvalid
	}
	return title, nil
}

// resolveInvitees maps the request's input users to ids, splitting them into
// members to create the chat with and invitees that do not exist. A user id with
// no users row is reported as missing rather than failing the call — Telegram's
// own behaviour — but a malformed input peer fails the whole call, since it is a
// client bug rather than a fact about an account.
func (h *handlers) resolveInvitees(ctx context.Context, users []tg.InputUserClass, selfID int64) (members []int64, missing []tg.MissingInvitee, err error) {
	for _, u := range users {
		id, err := inputUserID(u, selfID)
		if err != nil {
			return nil, nil, err
		}
		_, ok, err := h.store.UserByID(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			missing = append(missing, tg.MissingInvitee{UserID: id})
			continue
		}
		members = append(members, id)
	}
	return members, missing, nil
}

// chatUpdate builds the envelope a chat mutation returns to its caller: the
// caller's own copy of the service message, at the caller's own new pts, plus
// the users and the chat the client needs to render it.
//
// perOwner carries every member's new pts and is server-side notification state
// only — never echoed to the caller. Only perOwner[callerID] leaves this
// function, and its key set is used as the member set.
func (h *handlers) chatUpdate(ctx context.Context, callerID int64, chat store.Chat, sender store.Message, perOwner map[int64]int, createUsers []int64) (*tg.Updates, error) {
	ids := make(map[int64]bool, len(perOwner))
	for uid := range perOwner {
		ids[uid] = true
	}
	users, err := h.loadUsers(ctx, ids, callerID)
	if err != nil {
		return nil, err
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				// A service message never carries media.
				Message:  messageToTL(sender, createUsers, nil),
				Pts:      perOwner[callerID],
				PtsCount: 1,
			},
		},
		Users: users,
		Chats: []tg.ChatClass{chatToTL(chat, len(perOwner), callerID)},
		Date:  int(sender.Date.Unix()),
	}, nil
}

// memberIDs returns the fan-out's owners in ascending order, matching the order
// store.Participants reads them so the create action's user list is identical
// whether a client sees it here or replays it through getDifference.
func memberIDs(perOwner map[int64]int) []int64 {
	ids := make([]int64, 0, len(perOwner))
	for uid := range perOwner {
		ids = append(ids, uid)
	}
	slices.Sort(ids)
	return ids
}

// handleCreateChat serves messages.createChat: it creates the chat, announces it
// to every member with a service message, and returns the caller's own update.
//
// The chat and its announcement are two transactions, which is deliberate and is
// the one place a chat mutation is allowed to split them. A half-completed add,
// removal or rename is harmful because the member set and what members were told
// disagree; a chat whose create announcement failed is in nobody's dialog list,
// creator included, so it fails safe. Nothing unwinds the chat row.
func (h *handlers) handleCreateChat(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesCreateChatRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	title, err := chatTitle(req.Title)
	if err != nil {
		return nil, err
	}
	if len(req.Users) > maxChatUsers {
		return nil, errUsersTooMuch
	}
	members, missing, err := h.resolveInvitees(r.Ctx, req.Users, r.UserID)
	if errors.Is(err, errPeerIDInvalid) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("create chat: resolve invitees", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	chat, err := h.store.CreateChat(r.Ctx, r.UserID, title, members)
	if errors.Is(err, store.ErrChatFull) {
		return nil, errUsersTooMuch
	}
	if err != nil {
		h.log.Error("create chat", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	sender, perOwner, _, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
		ChatID: chat.ID, FromID: r.UserID, Text: title, Action: store.ChatActionCreate,
	})
	if err != nil {
		h.log.Error("create chat announce", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatUpdate(r.Ctx, r.UserID, chat, sender, perOwner, memberIDs(perOwner))
	if err != nil {
		h.log.Error("create chat updates", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	// TTLPeriod is accepted and ignored: M6 stores no per-chat message TTL.
	return &tg.MessagesInvitedUsers{Updates: ups, MissingInvitees: missing}, nil
}

// handleEditChatTitle serves messages.editChatTitle: one store call renames the
// chat and announces the rename atomically.
//
// There is no membership check here on purpose. The store re-checks the caller
// inside its own transaction under the chats row lock, and that is the
// authorization boundary; a check here would run in a different transaction and
// the gap between the two is exactly what an attacker removed from the chat
// mid-call would ride. store.ErrNotMember covers both "not a member" and "no
// such chat", and both must stay one wire error so chat ids are not enumerable.
func (h *handlers) handleEditChatTitle(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesEditChatTitleRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	title, err := chatTitle(req.Title)
	if err != nil {
		return nil, err
	}

	chat, sender, perOwner, err := h.store.SetChatTitle(r.Ctx, req.ChatID, r.UserID, title)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("edit chat title", "chat_id", req.ChatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatUpdate(r.Ctx, r.UserID, chat, sender, perOwner, nil)
	if err != nil {
		h.log.Error("edit chat title updates", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	return ups, nil
}
