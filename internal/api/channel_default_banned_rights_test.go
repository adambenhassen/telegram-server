package api_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
)

func editChannelDefaultRights(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	peer tg.InputPeerClass,
	rights tg.ChatBannedRights,
) (*tg.Updates, *mt.RPCError) {
	t.Helper()
	body := dispatchChatMethod(t, h, userID, "messages.editChatDefaultBannedRights", &tg.MessagesEditChatDefaultBannedRightsRequest{
		Peer:         peer,
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

func channelDefaultRightsFromResponse(t *testing.T, response tg.ChatClass, channelID int64) tg.ChatBannedRights {
	t.Helper()
	channel, ok := response.(*tg.Channel)
	if !ok || channel.ID != channelID {
		t.Fatalf("channel response = %T/%v, want channel %d", response, response, channelID)
	}
	rights, ok := channel.GetDefaultBannedRights()
	if !ok {
		t.Fatalf("channel %d has no default banned rights", channelID)
	}
	return rights
}

func TestEditChatDefaultBannedRightsSavesAndReadsMegagroupRights(t *testing.T) {
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

	creator := chatUser(t, s, 8501)
	admin := chatUser(t, s, 8502)
	member := chatUser(t, s, 8503)
	outsider := chatUser(t, s, 8504)
	channel, err := s.CreateChannel(ctx, creator.ID, "Default rights", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, channel, admin.ID)
	joinChannelByInvite(t, s, channel, member.ID)
	if err = s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	if err = s.EditChannelUsername(ctx, channel.ID, creator.ID, "pollprivacy"); err != nil {
		t.Fatalf("set public username: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, channel.ID, "seed dialog", 85001); err != nil {
		t.Fatalf("seed channel dialog: %v", err)
	}

	h := fullChannelDispatcher(s)
	beforePts, err := s.ChannelState(ctx, channel.ID)
	if err != nil {
		t.Fatalf("channel state before save: %v", err)
	}
	updates, rpc := editChannelDefaultRights(t, h, admin.ID, api.InputPeerChannel(admin.ID, channel.ID), tg.ChatBannedRights{SendPolls: true})
	if rpc != nil {
		t.Fatalf("admin save default rights: %s", rpc.ErrorMessage)
	}
	if len(updates.Updates) != 1 {
		t.Fatalf("updates = %d, want one committed rights update", len(updates.Updates))
	}
	changed, ok := updates.Updates[0].(*tg.UpdateChatDefaultBannedRights)
	if !ok {
		t.Fatalf("update = %T, want *tg.UpdateChatDefaultBannedRights", updates.Updates[0])
	}
	peer, ok := changed.Peer.(*tg.PeerChannel)
	if !ok || peer.ChannelID != channel.ID {
		t.Fatalf("updated peer = %T/%v, want channel %d", changed.Peer, changed.Peer, channel.ID)
	}
	if !changed.DefaultBannedRights.SendPolls || changed.DefaultBannedRights.SendMessages || changed.DefaultBannedRights.SendMedia {
		t.Fatalf("changed rights = %+v, want SendPolls alone", changed.DefaultBannedRights)
	}
	if changed.Version != channel.Version+1 {
		t.Fatalf("update version = %d, want %d", changed.Version, channel.Version+1)
	}

	var version int
	var storedRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM channels WHERE id = $1`, channel.ID).Scan(&version, &storedRights); err != nil {
		t.Fatalf("read stored rights: %v", err)
	}
	if version != channel.Version+1 || len(storedRights) != 1 || storedRights[0] != "send_polls" {
		t.Fatalf("stored version/rights = %d/%v, want %d/[send_polls]", version, storedRights, channel.Version+1)
	}
	if got, err := s.ChannelState(ctx, channel.ID); err != nil || got != beforePts {
		t.Errorf("channel pts after rights save = %d, err %v; want unchanged %d", got, err, beforePts)
	}

	channelsBody := dispatchChatMethod(t, h, member.ID, "channels.getChannels", &tg.ChannelsGetChannelsRequest{
		ID: []tg.InputChannelClass{api.InputChannel(member.ID, channel.ID)},
	})
	var channels tg.MessagesChats
	if err = channels.Decode(&bin.Buffer{Buf: channelsBody}); err != nil {
		t.Fatalf("decode member getChannels result: %v", err)
	}
	if len(channels.Chats) != 1 {
		t.Fatalf("member getChannels returned %d channels, want 1", len(channels.Chats))
	}
	if rights := channelDefaultRightsFromResponse(t, channels.Chats[0], channel.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("member getChannels default rights = %+v, want only SendPolls", rights)
	}

	fullBody := dispatchChatMethod(t, h, member.ID, "channels.getFullChannel", &tg.ChannelsGetFullChannelRequest{
		Channel: api.InputChannel(member.ID, channel.ID),
	})
	var full tg.MessagesChatFull
	if err = full.Decode(&bin.Buffer{Buf: fullBody}); err != nil {
		t.Fatalf("decode fresh member getFullChannel result: %v", err)
	}
	if len(full.Chats) != 1 {
		t.Fatalf("member getFullChannel returned %d channels, want 1", len(full.Chats))
	}
	if rights := channelDefaultRightsFromResponse(t, full.Chats[0], channel.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("member getFullChannel default rights = %+v, want only SendPolls", rights)
	}

	dialogResult, err := api.GetDialogsForTest(s, member.ID)
	if err != nil {
		t.Fatalf("member getDialogs: %v", err)
	}
	dialogs, ok := dialogResult.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("member getDialogs result = %T, want *tg.MessagesDialogs", dialogResult)
	}
	if len(dialogs.Chats) != 1 {
		t.Fatalf("member getDialogs returned %d chats, want one channel", len(dialogs.Chats))
	}
	if rights := channelDefaultRightsFromResponse(t, dialogs.Chats[0], channel.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("member getDialogs default rights = %+v, want only SendPolls", rights)
	}

	peerDialogResult, err := api.GetPeerDialogsForTest(s, member.ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{&tg.InputDialogPeer{Peer: api.InputPeerChannel(member.ID, channel.ID)}},
	})
	if err != nil {
		t.Fatalf("member getPeerDialogs: %v", err)
	}
	peerDialogs, ok := peerDialogResult.(*tg.MessagesPeerDialogs)
	if !ok {
		t.Fatalf("member getPeerDialogs result = %T, want *tg.MessagesPeerDialogs", peerDialogResult)
	}
	if len(peerDialogs.Chats) != 1 {
		t.Fatalf("member getPeerDialogs returned %d chats, want one channel", len(peerDialogs.Chats))
	}
	if rights := channelDefaultRightsFromResponse(t, peerDialogs.Chats[0], channel.ID); !rights.SendPolls || rights.SendMessages {
		t.Fatalf("member getPeerDialogs default rights = %+v, want only SendPolls", rights)
	}

	previewBody := dispatchChatMethod(t, h, outsider.ID, "contacts.resolveUsername", &tg.ContactsResolveUsernameRequest{Username: "pollprivacy"})
	var preview tg.ContactsResolvedPeer
	if err = preview.Decode(&bin.Buffer{Buf: previewBody}); err != nil {
		t.Fatalf("decode public username preview: %v", err)
	}
	if len(preview.Chats) != 1 {
		t.Fatalf("outsider preview returned %d channels, want 1", len(preview.Chats))
	}
	publicChannel, ok := preview.Chats[0].(*tg.Channel)
	if !ok || publicChannel.ID != channel.ID {
		t.Fatalf("outsider preview channel = %T/%v, want channel %d", preview.Chats[0], preview.Chats[0], channel.ID)
	}
	if rights, ok := publicChannel.GetDefaultBannedRights(); ok {
		t.Fatalf("public username preview disclosed default rights: %+v", rights)
	}

	updates, rpc = editChannelDefaultRights(t, h, admin.ID, api.InputPeerChannel(admin.ID, channel.ID), tg.ChatBannedRights{SendPolls: true})
	if updates != nil || rpc == nil || rpc.ErrorMessage != "CHAT_NOT_MODIFIED" {
		t.Fatalf("unchanged admin save = updates:%v rpc:%v, want CHAT_NOT_MODIFIED", updates, rpc)
	}
	if err = conn.QueryRow(ctx, `SELECT version FROM channels WHERE id = $1`, channel.ID).Scan(&version); err != nil {
		t.Fatalf("read version after unchanged save: %v", err)
	}
	if version != channel.Version+1 {
		t.Errorf("version after unchanged save = %d, want unchanged %d", version, channel.Version+1)
	}
}

func TestEditChatDefaultBannedRightsChecksMegagroupAuthorityBeforeEquality(t *testing.T) {
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

	creator := chatUser(t, s, 8601)
	admin := chatUser(t, s, 8602)
	member := chatUser(t, s, 8603)
	outsider := chatUser(t, s, 8604)
	removed := chatUser(t, s, 8605)
	banned := chatUser(t, s, 8606)
	channel, err := s.CreateChannel(ctx, creator.ID, "Private rights", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	for _, user := range []store.User{admin, member, removed, banned} {
		joinChannelByInvite(t, s, channel, user.ID)
	}
	if err = s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	h := fullChannelDispatcher(s)
	updates, rpc := editChannelDefaultRights(t, h, creator.ID, api.InputPeerChannel(creator.ID, channel.ID), tg.ChatBannedRights{SendPolls: true})
	if rpc != nil || updates == nil {
		t.Fatalf("set comparison value: updates:%v rpc:%v", updates, rpc)
	}
	if left, err := s.LeaveChannel(ctx, channel.ID, removed.ID); err != nil || !left {
		t.Fatalf("remove member: left=%v err=%v", left, err)
	}
	banUntil := time.Now().Add(time.Hour)
	if err = s.SetChannelBan(ctx, channel.ID, creator.ID, banned.ID, &banUntil, false); err != nil {
		t.Fatalf("ban member: %v", err)
	}

	var beforeVersion int
	var beforeRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM channels WHERE id = $1`, channel.ID).Scan(&beforeVersion, &beforeRights); err != nil {
		t.Fatalf("read initial rights: %v", err)
	}
	wrongHashInput := api.InputPeerChannel(admin.ID, channel.ID)
	wrongHashInput.AccessHash++
	var wrongHashPeer tg.InputPeerClass = wrongHashInput
	for _, tc := range []struct {
		name   string
		caller int64
		peer   tg.InputPeerClass
	}{
		{name: "wrong access hash", caller: admin.ID, peer: wrongHashPeer},
		{name: "absent channel", caller: admin.ID, peer: api.InputPeerChannel(admin.ID, 1<<62)},
		{name: "outsider", caller: outsider.ID, peer: api.InputPeerChannel(outsider.ID, channel.ID)},
		{name: "removed member", caller: removed.ID, peer: api.InputPeerChannel(removed.ID, channel.ID)},
		{name: "banned member", caller: banned.ID, peer: api.InputPeerChannel(banned.ID, channel.ID)},
		{name: "underprivileged member", caller: member.ID, peer: api.InputPeerChannel(member.ID, channel.ID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpc := editChannelDefaultRights(t, h, tc.caller, tc.peer, tg.ChatBannedRights{SendPolls: true})
			if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Fatalf("same-value write by %s = %v, want PEER_ID_INVALID", tc.name, rpc)
			}
		})
	}

	broadcast, err := s.CreateChannel(ctx, creator.ID, "Broadcast", "", false)
	if err != nil {
		t.Fatalf("create broadcast: %v", err)
	}
	if _, rpc = editChannelDefaultRights(t, h, creator.ID, api.InputPeerChannel(creator.ID, broadcast.ID), tg.ChatBannedRights{}); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("same-value broadcast save = %v, want PEER_ID_INVALID", rpc)
	}

	invalidPeerID := int64(1 << 62)
	_, rpc = editChannelDefaultRights(t, h, admin.ID, api.InputPeerChannel(admin.ID, invalidPeerID), tg.ChatBannedRights{SendPolls: true, UntilDate: 1})
	if rpc == nil || rpc.ErrorMessage != "BANNED_RIGHTS_INVALID" {
		t.Fatalf("invalid defaults for absent channel = %v, want BANNED_RIGHTS_INVALID", rpc)
	}

	var afterVersion int
	var afterRights []string
	if err = conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM channels WHERE id = $1`, channel.ID).Scan(&afterVersion, &afterRights); err != nil {
		t.Fatalf("read rights after denied writes: %v", err)
	}
	if afterVersion != beforeVersion || !slices.Equal(afterRights, beforeRights) {
		t.Errorf("channel after denied writes = version %d rights %v, want unchanged %d/%v", afterVersion, afterRights, beforeVersion, beforeRights)
	}
}

func TestEditChatDefaultBannedRightsRejectsSaveAfterConcurrentDemotion(t *testing.T) {
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

	creator := chatUser(t, s, 8701)
	admin := chatUser(t, s, 8702)
	channel, err := s.CreateChannel(ctx, creator.ID, "Demotion race", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, channel, admin.ID)
	if err = s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	h := fullChannelDispatcher(s)
	updates, rpc := editChannelDefaultRights(t, h, creator.ID, api.InputPeerChannel(creator.ID, channel.ID), tg.ChatBannedRights{SendPolls: true})
	if rpc != nil || updates == nil {
		t.Fatalf("set unchanged comparison value: updates:%v rpc:%v", updates, rpc)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin demotion barrier: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // best effort cleanup
	if _, err = tx.Exec(ctx, `SELECT id FROM channels WHERE id = $1 FOR NO KEY UPDATE`, channel.ID); err != nil {
		t.Fatalf("hold channel mutation lock: %v", err)
	}

	if _, err = tx.Exec(ctx, `UPDATE channel_participants SET role = 0 WHERE channel_id = $1 AND user_id = $2`, channel.ID, admin.ID); err != nil {
		t.Fatalf("demote admin under channel lock: %v", err)
	}

	saveDone := make(chan struct {
		updates *tg.Updates
		rpc     *mt.RPCError
	}, 1)
	saveStarted := make(chan struct{})
	go func() {
		close(saveStarted)
		got, rpc := editChannelDefaultRights(t, h, admin.ID, api.InputPeerChannel(admin.ID, channel.ID), tg.ChatBannedRights{SendPolls: true})
		saveDone <- struct {
			updates *tg.Updates
			rpc     *mt.RPCError
		}{got, rpc}
	}()
	<-saveStarted
	waitForChannelLockWaiters(t, ctx, conn, 1)
	select {
	case result := <-saveDone:
		t.Fatalf("save completed before demotion committed: updates:%v rpc:%v", result.updates, result.rpc)
	default:
	}

	if err = tx.Commit(ctx); err != nil {
		t.Fatalf("commit lock-ordered demotion: %v", err)
	}
	result := <-saveDone
	if result.updates != nil || result.rpc == nil || result.rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("demoted admin save of unchanged rights = updates:%v rpc:%v, want PEER_ID_INVALID", result.updates, result.rpc)
	}
}

func waitForChannelLockWaiters(t *testing.T, ctx context.Context, conn *pgx.Conn, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
		`).Scan(&got); err != nil {
			t.Fatalf("inspect channel lock waiters: %v", err)
		}
		if got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("channel mutation had fewer than %d lock waiters", want)
}
