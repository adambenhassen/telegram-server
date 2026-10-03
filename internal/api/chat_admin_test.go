package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
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
	var unchanged tg.BoolFalse
	if err := unchanged.Decode(&bin.Buffer{Buf: body}); err == nil {
		return nil, nil
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

func assertChatAdminDifference(t *testing.T, s *store.Store, userID int64, fromPts int, chatID, targetID int64, version int, wantAdmin bool) {
	t.Helper()
	result, err := api.GetDifferenceForTest(s, userID, &tg.UpdatesGetDifferenceRequest{Pts: fromPts})
	if err != nil {
		t.Fatalf("getDifference for user %d: %v", userID, err)
	}
	difference, ok := result.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("getDifference result for user %d = %T, want difference with durable admin state", userID, result)
	}
	for _, update := range difference.OtherUpdates {
		admin, ok := update.(*tg.UpdateChatParticipantAdmin)
		if !ok || admin.ChatID != chatID || admin.UserID != targetID {
			continue
		}
		if admin.Version != version || admin.IsAdmin != wantAdmin {
			t.Fatalf("getDifference admin state = chat %d user %d admin %t version %d, want chat %d user %d admin %t version %d", admin.ChatID, admin.UserID, admin.IsAdmin, admin.Version, chatID, targetID, wantAdmin, version)
		}
		return
	}
	t.Fatalf("getDifference for user %d omitted durable admin state for chat %d user %d", userID, chatID, targetID)
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
	fromPts := make(map[int64]int, len(users))
	for _, userID := range users {
		fromPts[userID] = apiPts(t, s, userID)
	}

	result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, true)
	if result == nil || rpc != nil {
		t.Fatalf("promote admin result = %v, rpc = %v", result, rpc)
	}
	promoted, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok {
		t.Fatalf("chat after promotion: ok=%v err=%v", ok, err)
	}
	for _, userID := range users {
		if got := apiPts(t, s, userID); got != fromPts[userID] {
			t.Errorf("owner %d pts after promotion = %d, want unchanged %d", userID, got, fromPts[userID])
		}
		assertChatAdminDifference(t, s, userID, fromPts[userID], chat.ID, target.ID, promoted.Version, true)
		assertFullChatAdmin(t, getFullChat(t, h, userID, chat.ID), target.ID, true)
	}

	// The live admin notification has no pts. The next real message must still
	// occupy the very next pts slot, and getDifference must replay it gap-free.
	message, err := api.SendMessageForTest(s, target.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(target.ID, chat.ID), Message: "after promotion", RandomID: 1175001,
	})
	if err != nil {
		t.Fatalf("send message after promotion: %v", err)
	}
	updates, ok := message.(*tg.Updates)
	if !ok {
		t.Fatalf("send result after promotion = %T, want *tg.Updates", message)
	}
	foundNextPts := false
	for _, update := range updates.Updates {
		if created, ok := update.(*tg.UpdateNewMessage); ok && created.Pts == fromPts[target.ID]+1 && created.PtsCount == 1 {
			foundNextPts = true
		}
	}
	if !foundNextPts {
		t.Fatalf("message after promotion did not use pts %d with count 1: %+v", fromPts[target.ID]+1, updates.Updates)
	}
	afterMessageState, err := s.State(ctx, target.ID)
	if err != nil {
		t.Fatalf("state after message following promotion: %v", err)
	}
	difference, err := api.GetDifferenceForTest(s, target.ID, &tg.UpdatesGetDifferenceRequest{
		Pts:  fromPts[target.ID],
		Date: afterMessageState.Date,
	})
	if err != nil {
		t.Fatalf("getDifference after message following promotion: %v", err)
	}
	diff, ok := difference.(*tg.UpdatesDifference)
	if !ok || diff.State.Pts != fromPts[target.ID]+1 || len(diff.NewMessages) != 1 {
		t.Fatalf("getDifference after message following promotion = %#v, want one message and pts %d", difference, fromPts[target.ID]+1)
	}
	foundRoleSnapshot := false
	for _, update := range diff.OtherUpdates {
		if admin, ok := update.(*tg.UpdateChatParticipantAdmin); ok && admin.ChatID == chat.ID && admin.UserID == target.ID && admin.IsAdmin {
			foundRoleSnapshot = true
		}
	}
	if !foundRoleSnapshot {
		t.Fatal("getDifference omitted durable admin state after the member date advanced past the missed push")
	}

	result, rpc = editChatAdmin(t, h, creator.ID, target.ID, chat.ID, false)
	if result == nil || rpc != nil {
		t.Fatalf("demote admin result = %v, rpc = %v", result, rpc)
	}
	demoted, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok {
		t.Fatalf("chat after demotion: ok=%v err=%v", ok, err)
	}
	for _, userID := range users {
		wantPts := fromPts[userID] + 1
		if got := apiPts(t, s, userID); got != wantPts {
			t.Errorf("owner %d pts after demotion = %d, want %d", userID, got, wantPts)
		}
		assertChatAdminDifference(t, s, userID, fromPts[userID], chat.ID, target.ID, demoted.Version, false)
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

func TestEditChatAdminNonMemberAndMissingChatHaveSameError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 744)
	target := chatUser(t, s, 745)
	outsider := chatUser(t, s, 746)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin boundary", []int64{target.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	_, outsiderRPC := editChatAdmin(t, h, outsider.ID, target.ID, chat.ID, true)
	_, missingRPC := editChatAdmin(t, h, creator.ID, target.ID, chat.ID+100_000, true)
	if outsiderRPC == nil || missingRPC == nil || outsiderRPC.ErrorMessage != "PEER_ID_INVALID" || missingRPC.ErrorMessage != outsiderRPC.ErrorMessage {
		t.Fatalf("non-member/missing-chat errors = %v/%v, want the same PEER_ID_INVALID", outsiderRPC, missingRPC)
	}
}

func TestEditChatAdminNoOpKeepsVersionAndUpdatesStable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 747)
	target := chatUser(t, s, 748)
	observer := chatUser(t, s, 749)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin no-op", []int64{target.ID, observer.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	if result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, false); result != nil || rpc != nil {
		t.Fatalf("unchanged initial demotion result = %v, rpc = %v, want BoolFalse", result, rpc)
	}
	initial, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok || initial.Version != chat.Version {
		t.Fatalf("chat after unchanged initial demotion = %+v ok=%v err=%v, want version %d", initial, ok, err, chat.Version)
	}
	for _, userID := range []int64{creator.ID, target.ID, observer.ID} {
		if got := apiPts(t, s, userID); got != 0 {
			t.Errorf("owner %d pts after unchanged initial demotion = %d, want 0", userID, got)
		}
	}
	if result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, true); result == nil || rpc != nil {
		t.Fatalf("promote admin result = %v, rpc = %v", result, rpc)
	}

	versioned, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok {
		t.Fatalf("chat after promotion: ok=%v err=%v", ok, err)
	}
	users := []int64{creator.ID, target.ID, observer.ID}
	pts := make(map[int64]int, len(users))
	for _, userID := range users {
		pts[userID] = apiPts(t, s, userID)
	}
	if result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, true); result != nil || rpc != nil {
		t.Fatalf("unchanged promotion result = %v, rpc = %v, want BoolFalse", result, rpc)
	}
	unchanged, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok || unchanged.Version != versioned.Version {
		t.Fatalf("chat after no-op = %+v, ok=%v err=%v; want version %d", unchanged, ok, err, versioned.Version)
	}
	for _, userID := range users {
		if got := apiPts(t, s, userID); got != pts[userID] {
			t.Errorf("owner %d pts after no-op = %d, want unchanged %d", userID, got, pts[userID])
		}
		assertChatAdminDifference(t, s, userID, 0, chat.ID, target.ID, versioned.Version, true)
	}
}

func TestEditChatAdminRollsBackWhenUpdateDeliveryCannotPersist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(ctx, `DROP TRIGGER IF EXISTS fail_chat_admin_delivery ON chat_admin_event_recipients`); err != nil {
			t.Errorf("drop event failure trigger: %v", err)
		}
		if _, err := conn.Exec(ctx, `DROP FUNCTION IF EXISTS fail_chat_admin_delivery()`); err != nil {
			t.Errorf("drop event failure function: %v", err)
		}
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})

	creator := chatUser(t, s, 750)
	target := chatUser(t, s, 751)
	observer := chatUser(t, s, 752)
	chat, err := s.CreateChat(ctx, creator.ID, "Atomic admin", []int64{target.ID, observer.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	pts := map[int64]int{
		creator.ID:  apiPts(t, s, creator.ID),
		target.ID:   apiPts(t, s, target.ID),
		observer.ID: apiPts(t, s, observer.ID),
	}
	if _, err = conn.Exec(ctx, `
		CREATE FUNCTION fail_chat_admin_delivery() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected chat admin delivery failure';
			RETURN NEW;
		END;
		$$`); err != nil {
		t.Fatalf("create event failure function: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		CREATE TRIGGER fail_chat_admin_delivery
		BEFORE INSERT ON chat_admin_event_recipients
		FOR EACH ROW EXECUTE FUNCTION fail_chat_admin_delivery()`); err != nil {
		t.Fatalf("create recipient failure trigger: %v", err)
	}
	if result, rpc := editChatAdmin(t, fullChannelDispatcher(s), creator.ID, target.ID, chat.ID, true); result != nil || rpc == nil || rpc.ErrorMessage != "INTERNAL" {
		t.Fatalf("promotion with failed delivery = result:%v rpc:%v, want INTERNAL", result, rpc)
	}

	stored, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok || stored.Version != chat.Version {
		t.Fatalf("chat after failed delivery = %+v ok=%v err=%v, want version %d", stored, ok, err, chat.Version)
	}
	participants, err := s.Participants(ctx, chat.ID)
	if err != nil {
		t.Fatalf("participants after failed delivery: %v", err)
	}
	for _, participant := range participants {
		if participant.UserID == target.ID && participant.Admin {
			t.Fatal("role change persisted after update delivery failed")
		}
	}
	var adminEvents, recipientMarkers int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM chat_admin_events WHERE chat_id = $1`, chat.ID).Scan(&adminEvents); err != nil {
		t.Fatalf("count admin events: %v", err)
	}
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM chat_admin_event_recipients WHERE event_id IN (SELECT id FROM chat_admin_events WHERE chat_id = $1)`, chat.ID).Scan(&recipientMarkers); err != nil {
		t.Fatalf("count recipient markers: %v", err)
	}
	if adminEvents != 0 || recipientMarkers != 0 {
		t.Fatalf("persisted state after failed recipient marker = admin events:%d recipient markers:%d, want none", adminEvents, recipientMarkers)
	}
	for userID, wantPts := range pts {
		if got := apiPts(t, s, userID); got != wantPts {
			t.Errorf("owner %d pts after failed delivery = %d, want unchanged %d", userID, got, wantPts)
		}
	}
}

func TestEditChatAdminResetsRoleWhenMemberIsReAdded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 754)
	target := chatUser(t, s, 755)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin re-add", []int64{target.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	if result, rpc := editChatAdmin(t, h, creator.ID, target.ID, chat.ID, true); result == nil || rpc != nil {
		t.Fatalf("promote admin result = %v, rpc = %v", result, rpc)
	}
	if _, err = api.DeleteChatUserForTest(s, creator.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(creator.ID, target.ID),
	}); err != nil {
		t.Fatalf("remove admin: %v", err)
	}
	if _, err = api.AddChatUserForTest(s, creator.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(creator.ID, target.ID),
	}); err != nil {
		t.Fatalf("re-add former admin: %v", err)
	}
	assertFullChatAdmin(t, getFullChat(t, h, creator.ID, chat.ID), target.ID, false)
}

func TestGetFullChatKeepsRoleAndVersionInOneSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 756)
	viewer := chatUser(t, s, 757)
	target := chatUser(t, s, 758)
	chat, err := s.CreateChat(ctx, creator.ID, "Admin snapshot", []int64{viewer.ID, target.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	var changed bool
	var mutationErr error
	store.SetChatInfoSnapshotHook(s, func() {
		changed, _, mutationErr = s.SetChatAdmin(ctx, chat.ID, target.ID, creator.ID, true)
	})
	t.Cleanup(func() { store.SetChatInfoSnapshotHook(s, nil) })

	full := getFullChat(t, fullChannelDispatcher(s), viewer.ID, chat.ID)
	if mutationErr != nil || !changed {
		t.Fatalf("promote during snapshot: changed=%v err=%v", changed, mutationErr)
	}
	wireFull, ok := full.FullChat.(*tg.ChatFull)
	if !ok {
		t.Fatalf("full chat = %T, want *tg.ChatFull", full.FullChat)
	}
	participants, ok := wireFull.Participants.(*tg.ChatParticipants)
	if !ok || participants.Version != chat.Version {
		t.Fatalf("snapshot participant version = %v, want %d", wireFull.Participants, chat.Version)
	}
	assertFullChatAdmin(t, full, target.ID, false)

	current := getFullChat(t, fullChannelDispatcher(s), viewer.ID, chat.ID)
	currentFull, ok := current.FullChat.(*tg.ChatFull)
	if !ok {
		t.Fatalf("current full chat = %T, want *tg.ChatFull", current.FullChat)
	}
	currentParticipants, ok := currentFull.Participants.(*tg.ChatParticipants)
	if !ok || currentParticipants.Version != chat.Version+1 {
		t.Fatalf("current participant version = %v, want %d", currentFull.Participants, chat.Version+1)
	}
	assertFullChatAdmin(t, current, target.ID, true)
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
