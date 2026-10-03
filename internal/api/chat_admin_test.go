package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func editChatAdmin(
	t *testing.T,
	h mtproto.Handler,
	callerID, targetID int64,
	chatID int64,
	isAdmin bool,
) (*tg.BoolTrue, *mt.RPCError) {
	t.Helper()
	body := dispatchChatMethod(t, h, callerID, "messages.editChatAdmin", &tg.MessagesEditChatAdminRequest{
		ChatID:  chatID,
		UserID:  api.InputUser(callerID, targetID),
		IsAdmin: isAdmin,
	})
	var result tg.BoolTrue
	if err := result.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &result, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode editChatAdmin result: %v", err)
	}
	return nil, &rpc
}

func getFullChat(t *testing.T, h mtproto.Handler, userID, chatID int64) *tg.MessagesChatFull {
	t.Helper()
	body := dispatchChatMethod(t, h, userID, "messages.getFullChat", &tg.MessagesGetFullChatRequest{ChatID: chatID})
	var result tg.MessagesChatFull
	if err := result.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode getFullChat result: %v", err)
	}
	return &result
}

func assertChatAdminDifference(
	t *testing.T,
	s *store.Store,
	userID int64,
	fromPts int,
	chatID, targetID int64,
	isAdmin bool,
	version int,
) {
	t.Helper()
	result, err := api.GetDifferenceForTest(s, userID, &tg.UpdatesGetDifferenceRequest{Pts: fromPts})
	if err != nil {
		t.Fatalf("getDifference for user %d: %v", userID, err)
	}
	difference, ok := result.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("getDifference result = %T, want *tg.UpdatesDifference", result)
	}
	if len(difference.OtherUpdates) != 1 {
		t.Fatalf("getDifference updates for user %d = %d, want one admin update", userID, len(difference.OtherUpdates))
	}
	update, ok := difference.OtherUpdates[0].(*tg.UpdateChatParticipantAdmin)
	if !ok || update.ChatID != chatID || update.UserID != targetID || update.IsAdmin != isAdmin || update.Version != version {
		t.Fatalf("getDifference update for user %d = %#v, want chat %d user %d admin %t version %d", userID, difference.OtherUpdates[0], chatID, targetID, isAdmin, version)
	}
}

func assertFullChatAdmin(t *testing.T, full *tg.MessagesChatFull, targetID int64, wantAdmin bool) {
	t.Helper()
	chat, ok := full.FullChat.(*tg.ChatFull)
	if !ok {
		t.Fatalf("getFullChat full result = %T, want *tg.ChatFull", full.FullChat)
	}
	participants, ok := chat.Participants.(*tg.ChatParticipants)
	if !ok {
		t.Fatalf("getFullChat participants = %T, want *tg.ChatParticipants", chat.Participants)
	}
	for _, participant := range participants.Participants {
		if participant.GetUserID() != targetID {
			continue
		}
		_, gotAdmin := participant.(*tg.ChatParticipantAdmin)
		if gotAdmin != wantAdmin {
			t.Fatalf("participant %d admin = %t, want %t", targetID, gotAdmin, wantAdmin)
		}
		return
	}
	t.Fatalf("getFullChat omitted participant %d", targetID)
}

func TestCreatorPromotesAndDemotesBasicGroupAdmin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 731)
	target := chatUser(t, s, 732)
	observer := chatUser(t, s, 733)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin lifecycle", []int64{target.ID, observer.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	users := []int64{creator.ID, target.ID, observer.ID}

	result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, true)
	if result == nil || rpc != nil {
		t.Fatalf("promote admin result = %v, rpc = %v", result, rpc)
	}
	for _, userID := range users {
		assertChatAdminDifference(t, s, userID, 0, chat.ID, target.ID, true, 2)
		assertFullChatAdmin(t, getFullChat(t, h, userID, chat.ID), target.ID, true)
	}

	fromPts := make(map[int64]int, len(users))
	for _, userID := range users {
		fromPts[userID] = apiPts(t, s, userID)
	}
	result, rpc = editChatAdmin(t, h, creator.ID, target.ID, chat.ID, false)
	if result == nil || rpc != nil {
		t.Fatalf("demote admin result = %v, rpc = %v", result, rpc)
	}
	for _, userID := range users {
		assertChatAdminDifference(t, s, userID, fromPts[userID], chat.ID, target.ID, false, 3)
		assertFullChatAdmin(t, getFullChat(t, h, userID, chat.ID), target.ID, false)
	}
}

func TestEditChatAdminRequiresCreator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 741)
	member := chatUser(t, s, 742)
	target := chatUser(t, s, 743)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin authority", []int64{member.ID, target.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	_, rpc := editChatAdmin(t, fullChannelDispatcher(s), member.ID, target.ID, chat.ID, true)
	if rpc == nil || rpc.ErrorMessage != "CHAT_ADMIN_REQUIRED" {
		t.Fatalf("non-creator editChatAdmin error = %v, want CHAT_ADMIN_REQUIRED", rpc)
	}
}

func TestEditChatAdminRejectsNonParticipantTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 751)
	member := chatUser(t, s, 752)
	outsider := chatUser(t, s, 753)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin target", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	_, rpc := editChatAdmin(t, fullChannelDispatcher(s), creator.ID, outsider.ID, chat.ID, true)
	if rpc == nil || rpc.ErrorMessage != "USER_NOT_PARTICIPANT" {
		t.Fatalf("non-participant editChatAdmin error = %v, want USER_NOT_PARTICIPANT", rpc)
	}
}

func TestEditChatAdminRejectsCreatorTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 761)
	member := chatUser(t, s, 762)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin creator", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	_, rpc := editChatAdmin(t, fullChannelDispatcher(s), creator.ID, creator.ID, chat.ID, true)
	if rpc == nil || rpc.ErrorMessage != "USER_ID_INVALID" {
		t.Fatalf("creator editChatAdmin error = %v, want USER_ID_INVALID", rpc)
	}
}
