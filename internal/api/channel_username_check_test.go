package api_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func checkChannelUsername(
	t *testing.T, s *store.Store, userID int64, provisional bool, req *tg.ChannelsCheckUsernameRequest,
) (bool, *mt.RPCError) {
	t.Helper()
	body := dispatchSettings(t, fullChannelDispatcher(s), settingsHandler{
		name: "channels.checkUsername",
		request: func() bin.Encoder {
			return req
		},
	}, userID, provisional)

	buffer := &bin.Buffer{Buf: body}
	var available tg.BoolTrue
	if err := available.Decode(buffer); err == nil {
		return true, nil
	}
	buffer = &bin.Buffer{Buf: body}
	var unavailable tg.BoolFalse
	if err := unavailable.Decode(buffer); err == nil {
		return false, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode channels.checkUsername response: %v", err)
	}
	return false, &rpc
}

func getAdminedPublicChannels(
	t *testing.T, s *store.Store, userID int64, provisional bool,
) (*tg.MessagesChats, *mt.RPCError) {
	t.Helper()
	body := dispatchSettings(t, fullChannelDispatcher(s), settingsHandler{
		name: "channels.getAdminedPublicChannels",
		request: func() bin.Encoder {
			return &tg.ChannelsGetAdminedPublicChannelsRequest{}
		},
	}, userID, provisional)

	var chats tg.MessagesChats
	if err := chats.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &chats, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode channels.getAdminedPublicChannels response: %v", err)
	}
	return nil, &rpc
}

func TestCheckUsernameAvailabilityAndAdminedChannelFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	owner, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	channel, err := s.CreateChannel(ctx, owner.ID, "Garden Weekly", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	available, rpc := checkChannelUsername(t, s, owner.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel:  &tg.InputChannelEmpty{},
		Username: "gardenweekly",
	})
	if rpc != nil || !available {
		t.Fatalf("check free username = %t, %v; want available", available, rpc)
	}

	res, err := api.EditChannelUsernameForTest(s, owner.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(owner.ID, channel.ID),
		Username: "gardenweekly",
	})
	if err != nil {
		t.Fatalf("save checked username: %v", err)
	}
	assertEncodes(t, res)

	second, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("second account: %v", err)
	}
	available, rpc = checkChannelUsername(t, s, second.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel:  &tg.InputChannelEmpty{},
		Username: "gardenweekly",
	})
	if rpc != nil || available {
		t.Fatalf("second account check = %t, %v; want unavailable", available, rpc)
	}

	chats, rpc := getAdminedPublicChannels(t, s, owner.ID, false)
	if rpc != nil {
		t.Fatalf("get administered public channels: %v", rpc)
	}
	if len(chats.Chats) != 1 {
		t.Fatalf("admined channels = %d, want 1", len(chats.Chats))
	}
	got, ok := chats.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("admined channel = %T, want *tg.Channel", chats.Chats[0])
	}
	if got.ID != channel.ID || got.Username != "gardenweekly" {
		t.Errorf("admined channel = %d @%q, want %d @gardenweekly", got.ID, got.Username, channel.ID)
	}
	if got.AccessHash != api.DeriveChannelHash(owner.ID, channel.ID) {
		t.Errorf("admined channel access_hash = %d, want caller-derived hash", got.AccessHash)
	}

	secondChats, rpc := getAdminedPublicChannels(t, s, second.ID, false)
	if rpc != nil {
		t.Fatalf("second account get administered public channels: %v", rpc)
	}
	if len(secondChats.Chats) != 0 {
		t.Errorf("second account sees %d administered channels, want none", len(secondChats.Chats))
	}
}

func TestCheckUsernameReportsUserAndChannelHandlesUnavailable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "+15551298003")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}
	userOwner, err := s.CreateUser(ctx, "+15551298004")
	if err != nil {
		t.Fatalf("user owner: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, userOwner.ID, "userhandle"); err != nil {
		t.Fatalf("claim user handle: %v", err)
	}
	channelOwner, err := s.CreateUser(ctx, "+15551298005")
	if err != nil {
		t.Fatalf("channel owner: %v", err)
	}
	channel, err := s.CreateChannel(ctx, channelOwner.ID, "Public", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, channelOwner.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel: api.InputChannel(channelOwner.ID, channel.ID), Username: "channelhandle",
	}); err != nil {
		t.Fatalf("claim channel handle: %v", err)
	}

	for _, username := range []string{"userhandle", "channelhandle"} {
		available, rpc := checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
			Channel: &tg.InputChannelEmpty{}, Username: username,
		})
		if rpc != nil || available {
			t.Errorf("check %q = %t, %v; want unavailable", username, available, rpc)
		}
	}
}

func TestCheckUsernameValidatesBeforeCharging(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "+15551298006")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}

	for _, username := range []string{"x", "admin", "bad-name"} {
		if _, rpc := checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
			Channel: &tg.InputChannelEmpty{}, Username: username,
		}); rpc == nil || rpc.ErrorMessage != "USERNAME_INVALID" {
			t.Errorf("check %q error = %v, want USERNAME_INVALID", username, rpc)
		}
	}
	if _, rpc := checkChannelUsername(t, s, 0, false, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "validname",
	}); rpc == nil || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("unauthenticated check error = %v, want AUTH_KEY_UNREGISTERED", rpc)
	}
	if _, rpc := checkChannelUsername(t, s, caller.ID, true, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "validname",
	}); rpc == nil || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("provisional check error = %v, want AUTH_KEY_UNREGISTERED", rpc)
	}

	for i := range store.UsernameLookupBurstLimit {
		available, rpc := checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
			Channel: &tg.InputChannelEmpty{}, Username: fmt.Sprintf("validname%d", i),
		})
		if rpc != nil || !available {
			t.Fatalf("valid check %d = %t, %v; invalid attempts must not charge the quota", i, available, rpc)
		}
	}
}

func TestCheckUsernameSharesLookupQuotaAcrossTakenAndFreeChecks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "+15551298014")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}
	resolveOwner, err := s.CreateUser(ctx, "+15551298015")
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, resolveOwner.ID, "resolvedhandle"); err != nil {
		t.Fatalf("claim resolve handle: %v", err)
	}
	claimOwner, err := s.CreateUser(ctx, "+15551298016")
	if err != nil {
		t.Fatalf("claim owner: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, claimOwner.ID, "claimedhandle"); err != nil {
		t.Fatalf("claim occupied handle: %v", err)
	}
	checkOwner, err := s.CreateUser(ctx, "+15551298017")
	if err != nil {
		t.Fatalf("check owner: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, checkOwner.ID, "checkedhandle"); err != nil {
		t.Fatalf("claim checked handle: %v", err)
	}
	finalOwner, err := s.CreateUser(ctx, "+15551298018")
	if err != nil {
		t.Fatalf("final owner: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, finalOwner.ID, "overquotaowner"); err != nil {
		t.Fatalf("claim over-quota handle: %v", err)
	}

	if _, rpc := checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "admin",
	}); rpc == nil || rpc.ErrorMessage != "USERNAME_INVALID" {
		t.Fatalf("reserved name error = %v, want USERNAME_INVALID", rpc)
	}
	if _, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "resolvedhandle"}); err != nil {
		t.Fatalf("resolve username: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, caller.ID, "claimedhandle"); err == nil || rpcMessage(t, err) != "USERNAME_OCCUPIED" {
		t.Fatalf("claim occupied handle error = %v, want USERNAME_OCCUPIED", err)
	}
	available, rpc := checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "checkedhandle",
	})
	if rpc != nil || available {
		t.Fatalf("taken check = %t, %v; want unavailable", available, rpc)
	}
	available, rpc = checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "CHECKEDHANDLE",
	})
	if rpc != nil || available {
		t.Fatalf("repeated taken check = %t, %v; want unavailable", available, rpc)
	}

	for i := range store.UsernameLookupBurstLimit - 3 {
		available, rpc = checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
			Channel: &tg.InputChannelEmpty{}, Username: fmt.Sprintf("freecheck%d", i),
		})
		if rpc != nil || !available {
			t.Fatalf("free check %d = %t, %v; want available within shared quota", i, available, rpc)
		}
	}
	if _, rpc = checkChannelUsername(t, s, caller.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: &tg.InputChannelEmpty{}, Username: "overquotaowner",
	}); rpc == nil || rpc.ErrorMessage != "FLOOD_WAIT_86400" {
		t.Fatalf("over-quota taken check error = %v, want FLOOD_WAIT_86400 before lookup", rpc)
	}
}

func TestCheckUsernameRequiresNonBannedAdminForConcreteChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551298007")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298008")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	stranger, err := s.CreateUser(ctx, "+15551298009")
	if err != nil {
		t.Fatalf("stranger: %v", err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "Private", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannel(t, ctx, dsn, channel.ID, member.ID)

	available, rpc := checkChannelUsername(t, s, creator.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: api.InputChannel(creator.ID, channel.ID), Username: "creatorcheck",
	})
	if rpc != nil || !available {
		t.Fatalf("creator check = %t, %v; want available", available, rpc)
	}
	if _, rpc = checkChannelUsername(t, s, member.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: api.InputChannel(member.ID, channel.ID), Username: "membercheck",
	}); rpc == nil || rpc.ErrorMessage != "CHAT_ADMIN_REQUIRED" {
		t.Errorf("member check error = %v, want CHAT_ADMIN_REQUIRED", rpc)
	}
	if _, rpc = checkChannelUsername(t, s, stranger.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: api.InputChannel(stranger.ID, channel.ID), Username: "strangercheck",
	}); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("stranger check error = %v, want PEER_ID_INVALID", rpc)
	}

	if err := s.SetChannelRole(ctx, channel.ID, creator.ID, member.ID, 1); err != nil {
		t.Fatalf("promote member: %v", err)
	}
	banUntil := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, channel.ID, creator.ID, member.ID, &banUntil, false); err != nil {
		t.Fatalf("ban admin: %v", err)
	}
	if _, rpc = checkChannelUsername(t, s, member.ID, false, &tg.ChannelsCheckUsernameRequest{
		Channel: api.InputChannel(member.ID, channel.ID), Username: "bannedcheck",
	}); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("banned admin check error = %v, want PEER_ID_INVALID", rpc)
	}
}

func TestGetAdminedPublicChannelsFiltersMembershipAndBan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	caller, err := s.CreateUser(ctx, "+15551298010")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551298011")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	creator, err := s.CreateUser(ctx, "+15551298012")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	bannedCreator, err := s.CreateUser(ctx, "+15551298013")
	if err != nil {
		t.Fatalf("banned creator: %v", err)
	}

	_, err = s.CreateChannel(ctx, caller.ID, "Private", "", false)
	if err != nil {
		t.Fatalf("create private channel: %v", err)
	}
	callerPublic, err := s.CreateChannel(ctx, caller.ID, "Caller public", "", false)
	if err != nil {
		t.Fatalf("create caller public channel: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, caller.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel: api.InputChannel(caller.ID, callerPublic.ID), Username: "callerspublic",
	}); err != nil {
		t.Fatalf("make caller channel public: %v", err)
	}

	admined, err := s.CreateChannel(ctx, creator.ID, "Admined", "", false)
	if err != nil {
		t.Fatalf("create admined channel: %v", err)
	}
	joinChannel(t, ctx, dsn, admined.ID, caller.ID)
	if err := s.SetChannelRole(ctx, admined.ID, creator.ID, caller.ID, 1); err != nil {
		t.Fatalf("promote caller: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel: api.InputChannel(creator.ID, admined.ID), Username: "adminedpublic",
	}); err != nil {
		t.Fatalf("make admined channel public: %v", err)
	}

	banned, err := s.CreateChannel(ctx, bannedCreator.ID, "Banned admin", "", false)
	if err != nil {
		t.Fatalf("create banned admin channel: %v", err)
	}
	joinChannel(t, ctx, dsn, banned.ID, caller.ID)
	if err := s.SetChannelRole(ctx, banned.ID, bannedCreator.ID, caller.ID, 1); err != nil {
		t.Fatalf("promote caller in banned channel: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, bannedCreator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel: api.InputChannel(bannedCreator.ID, banned.ID), Username: "bannedpublic",
	}); err != nil {
		t.Fatalf("make banned-admin channel public: %v", err)
	}
	banUntil := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, banned.ID, bannedCreator.ID, caller.ID, &banUntil, false); err != nil {
		t.Fatalf("ban caller in public channel: %v", err)
	}

	otherPublic, err := s.CreateChannel(ctx, other.ID, "Other public", "", false)
	if err != nil {
		t.Fatalf("create other public channel: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, other.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel: api.InputChannel(other.ID, otherPublic.ID), Username: "otherspublic",
	}); err != nil {
		t.Fatalf("make other account channel public: %v", err)
	}
	if _, err := s.CreateChannel(ctx, other.ID, "Other private", "", false); err != nil {
		t.Fatalf("create other private channel: %v", err)
	}
	joinChannel(t, ctx, dsn, otherPublic.ID, caller.ID)
	joinChannel(t, ctx, dsn, callerPublic.ID, other.ID)

	chats, rpc := getAdminedPublicChannels(t, s, caller.ID, false)
	if rpc != nil {
		t.Fatalf("get administered public channels: %v", rpc)
	}
	want := map[int64]string{callerPublic.ID: "callerspublic", admined.ID: "adminedpublic"}
	if len(chats.Chats) != len(want) {
		t.Fatalf("admined public channel count = %d, want %d", len(chats.Chats), len(want))
	}
	for _, chat := range chats.Chats {
		got, ok := chat.(*tg.Channel)
		if !ok {
			t.Fatalf("admined result = %T, want *tg.Channel", chat)
		}
		username, ok := want[got.ID]
		if !ok {
			t.Errorf("unexpected administered channel id %d", got.ID)
			continue
		}
		if got.Username != username {
			t.Errorf("channel %d username = %q, want %q", got.ID, got.Username, username)
		}
		if got.AccessHash != api.DeriveChannelHash(caller.ID, got.ID) {
			t.Errorf("channel %d access_hash is not derived for caller", got.ID)
		}
	}

	otherChats, rpc := getAdminedPublicChannels(t, s, other.ID, false)
	if rpc != nil {
		t.Fatalf("other account get administered public channels: %v", rpc)
	}
	for _, chat := range otherChats.Chats {
		if got, ok := chat.(*tg.Channel); ok && got.ID == callerPublic.ID {
			t.Errorf("other account received caller's administered channel")
		}
	}
}
