package api_test

import (
	"context"
	"slices"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func dispatchChatMethod(t *testing.T, h mtproto.Handler, userID int64, name string, req bin.Encoder) []byte {
	t.Helper()
	return dispatchSettings(t, h, settingsHandler{
		name: name,
		request: func() bin.Encoder {
			return req
		},
	}, userID, false)
}

func editChatDefaultRights(t *testing.T, h mtproto.Handler, userID int64, chatID int64, rights tg.ChatBannedRights) (*tg.Updates, *mt.RPCError) {
	t.Helper()
	body := dispatchChatMethod(t, h, userID, "messages.editChatDefaultBannedRights", &tg.MessagesEditChatDefaultBannedRightsRequest{
		Peer:         api.InputPeerChat(userID, chatID),
		BannedRights: rights,
	})
	var updates tg.Updates
	if err := updates.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &updates, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode editChatDefaultBannedRights result: %v", err)
	}
	return nil, &rpc
}

func chatRightsFromResponse(t *testing.T, response tg.ChatClass, chatID int64) tg.ChatBannedRights {
	t.Helper()
	chat, ok := response.(*tg.Chat)
	if !ok || chat.ID != chatID {
		t.Fatalf("chat response = %T/%v, want basic chat %d", response, response, chatID)
	}
	rights, ok := chat.GetDefaultBannedRights()
	if !ok {
		t.Fatalf("chat %d has no default banned rights", chatID)
	}
	return rights
}

func TestEditChatDefaultBannedRightsSavesAndReadsBasicGroupRights(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close conn: %v", err)
		}
	}()

	creator := chatUser(t, s, 8101)
	member := chatUser(t, s, 8102)
	chat, err := s.CreateChat(ctx, creator.ID, "Default rights", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	beforeWrites := basicChatWriteStats(t, conn, chat.ID)
	creatorPts := apiPts(t, s, creator.ID)
	memberPts := apiPts(t, s, member.ID)

	updates, rpc := editChatDefaultRights(t, h, creator.ID, chat.ID, tg.ChatBannedRights{SendPolls: true})
	if rpc != nil {
		t.Fatalf("edit default rights: %s", rpc.ErrorMessage)
	}
	if len(updates.Updates) != 1 {
		t.Fatalf("updates = %d entries, want one committed rights update", len(updates.Updates))
	}
	changed, ok := updates.Updates[0].(*tg.UpdateChatDefaultBannedRights)
	if !ok {
		t.Fatalf("update = %T, want *tg.UpdateChatDefaultBannedRights", updates.Updates[0])
	}
	peer, ok := changed.Peer.(*tg.PeerChat)
	if !ok || peer.ChatID != chat.ID {
		t.Fatalf("updated peer = %T/%v, want basic group %d", changed.Peer, changed.Peer, chat.ID)
	}
	if !changed.DefaultBannedRights.SendPolls || changed.DefaultBannedRights.SendMessages || changed.DefaultBannedRights.SendMedia {
		t.Fatalf("changed rights = %+v, want SendPolls alone", changed.DefaultBannedRights)
	}
	if changed.Version != chat.Version+1 {
		t.Fatalf("update version = %d, want %d", changed.Version, chat.Version+1)
	}

	var version int
	var storedRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM chats WHERE id = $1`, chat.ID).Scan(&version, &storedRights); err != nil {
		t.Fatalf("read stored rights: %v", err)
	}
	if version != chat.Version+1 || len(storedRights) != 1 || storedRights[0] != "send_polls" {
		t.Fatalf("stored version/rights = %d/%v, want %d/[send_polls]", version, storedRights, chat.Version+1)
	}
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 || got[0] != creator.ID || got[1] != member.ID {
		t.Fatalf("participants after rights save = %v, want creator and member unchanged", got)
	}
	assertChatWriteStats(t, conn, chat.ID, beforeWrites)
	if got := apiPts(t, s, creator.ID); got != creatorPts {
		t.Errorf("creator pts = %d after rights save, want unchanged %d", got, creatorPts)
	}
	if got := apiPts(t, s, member.ID); got != memberPts {
		t.Errorf("member pts = %d after rights save, want unchanged %d", got, memberPts)
	}

	chatReadBody := dispatchChatMethod(t, h, member.ID, "messages.getChats", &tg.MessagesGetChatsRequest{ID: []int64{chat.ID}})
	var chats tg.MessagesChats
	if err = chats.Decode(&bin.Buffer{Buf: chatReadBody}); err != nil {
		t.Fatalf("decode member getChats result: %v", err)
	}
	if len(chats.Chats) != 1 {
		t.Fatalf("member getChats returned %d chats, want 1", len(chats.Chats))
	}
	if rights := chatRightsFromResponse(t, chats.Chats[0], chat.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("member getChats default rights = %+v, want only SendPolls", rights)
	}

	fullChatBody := dispatchChatMethod(t, h, member.ID, "messages.getFullChat", &tg.MessagesGetFullChatRequest{ChatID: chat.ID})
	var full tg.MessagesChatFull
	if err = full.Decode(&bin.Buffer{Buf: fullChatBody}); err != nil {
		t.Fatalf("decode fresh member getFullChat result: %v", err)
	}
	if len(full.Chats) != 1 {
		t.Fatalf("member getFullChat returned %d chats, want 1", len(full.Chats))
	}
	if rights := chatRightsFromResponse(t, full.Chats[0], chat.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("fresh member getFullChat default rights = %+v, want only SendPolls", rights)
	}

	updates, rpc = editChatDefaultRights(t, h, creator.ID, chat.ID, tg.ChatBannedRights{SendPolls: true})
	if updates != nil || rpc == nil || rpc.ErrorMessage != "CHAT_NOT_MODIFIED" {
		t.Fatalf("unchanged rights result = updates:%v rpc:%v, want CHAT_NOT_MODIFIED", updates, rpc)
	}
	if err = conn.QueryRow(ctx, `SELECT version FROM chats WHERE id = $1`, chat.ID).Scan(&version); err != nil {
		t.Fatalf("read version after unchanged save: %v", err)
	}
	if version != chat.Version+1 {
		t.Errorf("version after unchanged save = %d, want unchanged %d", version, chat.Version+1)
	}
	assertChatWriteStats(t, conn, chat.ID, beforeWrites)
	if got := apiPts(t, s, creator.ID); got != creatorPts {
		t.Errorf("creator pts after unchanged save = %d, want unchanged %d", got, creatorPts)
	}
	if got := apiPts(t, s, member.ID); got != memberPts {
		t.Errorf("member pts after unchanged save = %d, want unchanged %d", got, memberPts)
	}
}

func TestEditChatDefaultBannedRightsChecksCreatorBeforeEquality(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close conn: %v", err)
		}
	}()

	creator := chatUser(t, s, 8201)
	member := chatUser(t, s, 8202)
	removed := chatUser(t, s, 8203)
	outsider := chatUser(t, s, 8204)
	chat, err := s.CreateChat(ctx, creator.ID, "Private rights", []int64{member.ID, removed.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	if updates, rpc := editChatDefaultRights(t, h, creator.ID, chat.ID, tg.ChatBannedRights{SendPolls: true}); rpc != nil || updates == nil {
		t.Fatalf("set comparison value: updates:%v rpc:%v", updates, rpc)
	}
	if _, _, _, err = s.RemoveChatUser(ctx, chat.ID, removed.ID, creator.ID); err != nil {
		t.Fatalf("remove member: %v", err)
	}

	var version int
	if err = conn.QueryRow(ctx, `SELECT version FROM chats WHERE id = $1`, chat.ID).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	beforeWrites := basicChatWriteStats(t, conn, chat.ID)
	creatorPts := apiPts(t, s, creator.ID)
	memberPts := apiPts(t, s, member.ID)
	removedPts := apiPts(t, s, removed.ID)
	outsiderPts := apiPts(t, s, outsider.ID)

	for _, tc := range []struct {
		name   string
		caller int64
		peerID int64
	}{
		{name: "unknown chat", caller: creator.ID, peerID: chat.ID + 1000000},
		{name: "outsider", caller: outsider.ID, peerID: chat.ID},
		{name: "removed member", caller: removed.ID, peerID: chat.ID},
		{name: "non-creator member", caller: member.ID, peerID: chat.ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpc := editChatDefaultRights(t, h, tc.caller, tc.peerID, tg.ChatBannedRights{SendPolls: true})
			if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Fatalf("same-value write by %s = %v, want PEER_ID_INVALID", tc.name, rpc)
			}
		})
	}

	var gotVersion int
	var gotRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM chats WHERE id = $1`, chat.ID).Scan(&gotVersion, &gotRights); err != nil {
		t.Fatalf("read rights after denied writes: %v", err)
	}
	if gotVersion != version || !slices.Equal(gotRights, []string{"send_polls"}) {
		t.Errorf("chat after denied writes = version %d rights %v, want %d/[send_polls]", gotVersion, gotRights, version)
	}
	assertChatWriteStats(t, conn, chat.ID, beforeWrites)
	for _, tc := range []struct {
		name   string
		userID int64
		before int
	}{
		{name: "creator", userID: creator.ID, before: creatorPts},
		{name: "member", userID: member.ID, before: memberPts},
		{name: "removed", userID: removed.ID, before: removedPts},
		{name: "outsider", userID: outsider.ID, before: outsiderPts},
	} {
		if got := apiPts(t, s, tc.userID); got != tc.before {
			t.Errorf("%s pts after denied writes = %d, want unchanged %d", tc.name, got, tc.before)
		}
	}
}

func TestEditChatDefaultBannedRightsRejectsInvalidRightsWithoutWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close conn: %v", err)
		}
	}()

	creator := chatUser(t, s, 8301)
	member := chatUser(t, s, 8302)
	chat, err := s.CreateChat(ctx, creator.ID, "Validate rights", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	h := fullChannelDispatcher(s)
	if updates, rpc := editChatDefaultRights(t, h, creator.ID, chat.ID, tg.ChatBannedRights{SendPolls: true}); rpc != nil || updates == nil {
		t.Fatalf("set previous rights: updates:%v rpc:%v", updates, rpc)
	}
	beforeWrites := basicChatWriteStats(t, conn, chat.ID)
	creatorPts := apiPts(t, s, creator.ID)
	memberPts := apiPts(t, s, member.ID)
	var beforeVersion int
	var beforeRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM chats WHERE id = $1`, chat.ID).Scan(&beforeVersion, &beforeRights); err != nil {
		t.Fatalf("read initial rights: %v", err)
	}

	for _, tc := range []struct {
		name   string
		rights tg.ChatBannedRights
	}{
		{name: "view messages", rights: tg.ChatBannedRights{ViewMessages: true}},
		{name: "expiry", rights: tg.ChatBannedRights{SendPolls: true, UntilDate: 1}},
		{name: "edit rank", rights: tg.ChatBannedRights{EditRank: true}},
		{name: "send reactions", rights: tg.ChatBannedRights{SendReactions: true}},
		{name: "manage linked peers", rights: tg.ChatBannedRights{ManageLinkedPeers: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpc := editChatDefaultRights(t, h, creator.ID, chat.ID, tc.rights)
			if rpc == nil || rpc.ErrorMessage != "BANNED_RIGHTS_INVALID" {
				t.Fatalf("invalid %s rights result = %v, want BANNED_RIGHTS_INVALID", tc.name, rpc)
			}
		})
	}

	var afterVersion int
	var afterRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM chats WHERE id = $1`, chat.ID).Scan(&afterVersion, &afterRights); err != nil {
		t.Fatalf("read rights after invalid writes: %v", err)
	}
	if afterVersion != beforeVersion || !slices.Equal(afterRights, beforeRights) {
		t.Errorf("chat after invalid rights = version %d rights %v, want unchanged version %d rights %v", afterVersion, afterRights, beforeVersion, beforeRights)
	}
	assertChatWriteStats(t, conn, chat.ID, beforeWrites)
	if got := apiPts(t, s, creator.ID); got != creatorPts {
		t.Errorf("creator pts after invalid writes = %d, want unchanged %d", got, creatorPts)
	}
	if got := apiPts(t, s, member.ID); got != memberPts {
		t.Errorf("member pts after invalid writes = %d, want unchanged %d", got, memberPts)
	}
}

func TestEditChatDefaultBannedRightsSupportsMegagroupPeers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 8401)
	channel, err := s.CreateChannel(ctx, creator.ID, "Rights peer type", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	h := fullChannelDispatcher(s)
	body := dispatchChatMethod(t, h, creator.ID, "messages.editChatDefaultBannedRights", &tg.MessagesEditChatDefaultBannedRightsRequest{
		Peer:         api.InputPeerChannel(creator.ID, channel.ID),
		BannedRights: tg.ChatBannedRights{SendPolls: true},
	})
	var updates tg.Updates
	if err := updates.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode megagroup rights result: %v", err)
	}
	if len(updates.Updates) != 1 {
		t.Fatalf("megagroup rights updates = %d, want one committed channel update", len(updates.Updates))
	}
	changed, ok := updates.Updates[0].(*tg.UpdateChatDefaultBannedRights)
	if !ok {
		t.Fatalf("megagroup rights update = %T, want *tg.UpdateChatDefaultBannedRights", updates.Updates[0])
	}
	peer, ok := changed.Peer.(*tg.PeerChannel)
	if !ok || peer.ChannelID != channel.ID {
		t.Fatalf("megagroup rights update peer = %T/%v, want channel %d", changed.Peer, changed.Peer, channel.ID)
	}
	if !changed.DefaultBannedRights.SendPolls || changed.Version != channel.Version+1 {
		t.Fatalf("megagroup rights/version = %+v/%d, want SendPolls/version %d", changed.DefaultBannedRights, changed.Version, channel.Version+1)
	}
}
