package api_test

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/config"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

// channelExec runs one statement against the test database. The store's own
// SetChannelBan / SetChannelCaps helpers live in internal/store/export_test.go,
// which compiles only into that package's test binary, so they are unreachable
// from api_test; raw SQL on the same DSN is the way this package reaches state
// no M7 RPC can produce yet — the pattern media_test.go already uses.
func channelExec(t *testing.T, ctx context.Context, dsn, sql string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if cerr := conn.Close(ctx); cerr != nil {
			t.Errorf("close conn: %v", cerr)
		}
	}()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// joinChannel writes a plain participant row. Joining is MAIN-91's RPC; this is
// how a second member exists today.
func joinChannel(t *testing.T, ctx context.Context, dsn string, channelID, userID int64) {
	t.Helper()
	channelExec(t, ctx, dsn,
		`INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, 0, 0)`,
		channelID, userID)
}

// banChannelMember sets banned_until. Ban mutation is a later ticket's RPC.
func banChannelMember(t *testing.T, ctx context.Context, dsn string, channelID, userID int64, until time.Time) {
	t.Helper()
	channelExec(t, ctx, dsn,
		`UPDATE channel_participants SET banned_until = $3 WHERE channel_id = $1 AND user_id = $2`,
		channelID, userID, until)
}

func inputChannels(viewerID int64, ids ...int64) []tg.InputChannelClass {
	out := make([]tg.InputChannelClass, len(ids))
	for i, id := range ids {
		out[i] = api.InputChannel(viewerID, id)
	}
	return out
}

// createChannel runs the handler and returns the channel it rendered, failing
// the test on any error or on a reply that is not a member's own view.
func createChannel(t *testing.T, s *store.Store, userID int64, req *tg.ChannelsCreateChannelRequest) *tg.Channel {
	t.Helper()
	res, err := api.CreateChannelForTest(s, userID, req)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	assertEncodes(t, res)
	ups, ok := res.(*tg.Updates)
	if !ok {
		t.Fatalf("create channel: got %T, want *tg.Updates", res)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("create channel: %d chats, want 1", len(ups.Chats))
	}
	ch, ok := ups.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("create channel: got %T, want *tg.Channel", ups.Chats[0])
	}
	return ch
}

const testPublicLinkPrefix = "https://test.example/"

func fullChannelDispatcher(s *store.Store, linkPrefixes ...string) mtproto.Handler {
	linkPrefix := testPublicLinkPrefix
	if len(linkPrefixes) > 0 {
		linkPrefix = linkPrefixes[0]
	}
	return api.New(
		s,
		2,
		&tg.Config{MeURLPrefix: linkPrefix},
		slog.New(slog.DiscardHandler),
		false,
		api.TestMaxFileBytes,
		nil,
		1,
		pgtest.PeerDeriver(),
		config.RateLimitsConfig{},
		config.RegistrationInvite,
	)
}

func getFullChannelViaDispatcher(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	provisional bool,
	channel tg.InputChannelClass,
) (*tg.MessagesChatFull, *mt.RPCError) {
	t.Helper()
	method := settingsHandler{
		name: "channels.getFullChannel",
		request: func() bin.Encoder {
			return &tg.ChannelsGetFullChannelRequest{Channel: channel}
		},
	}
	body := dispatchSettings(t, h, method, userID, provisional)
	var full tg.MessagesChatFull
	if err := full.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &full, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode getFullChannel response: %v", err)
	}
	return nil, &rpc
}

func activeChannelInviteCount(t *testing.T, ctx context.Context, dsn string, channelID int64) int {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close conn: %v", err)
		}
	}()
	var count int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM channel_invites WHERE channel_id = $1 AND revoked_at IS NULL`,
		channelID,
	).Scan(&count); err != nil {
		t.Fatalf("count active invites: %v", err)
	}
	return count
}

func requireFullChannelRPCError(t *testing.T, rpc *mt.RPCError, want string) {
	t.Helper()
	if rpc == nil {
		t.Fatalf("got full-channel response, want %s", want)
	}
	if rpc.ErrorMessage != want {
		t.Fatalf("RPC error = %d %q, want %q", rpc.ErrorCode, rpc.ErrorMessage, want)
	}
}

func fullChatChannel(t *testing.T, response *tg.MessagesChatFull) *tg.Channel {
	t.Helper()
	if len(response.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(response.Chats))
	}
	channel, ok := response.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("chat = %T, want *tg.Channel", response.Chats[0])
	}
	return channel
}

func fullChannelInfo(t *testing.T, response *tg.MessagesChatFull) *tg.ChannelFull {
	t.Helper()
	full, ok := response.FullChat.(*tg.ChannelFull)
	if !ok {
		t.Fatalf("full chat = %T, want *tg.ChannelFull", response.FullChat)
	}
	return full
}

func TestHandleGetFullChannelImmediatelyAfterCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551294901")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
		Broadcast: true,
		Title:     "Setup",
		About:     "Channel setup info",
	})

	response, rpc := getFullChannelViaDispatcher(t, fullChannelDispatcher(s), creator.ID, false, api.InputChannel(creator.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	assertEncodes(t, response)
	full := fullChannelInfo(t, response)
	if full.ID != channel.ID || full.About != "Channel setup info" {
		t.Errorf("full id/about = %d/%q, want %d/%q", full.ID, full.About, channel.ID, "Channel setup info")
	}
	if count, ok := full.GetParticipantsCount(); !ok || count != 1 {
		t.Errorf("participant count = %d present=%v, want 1", count, ok)
	}
	if !full.GetCanViewParticipants() {
		t.Error("broadcast creator cannot view participants")
	}
	if full.Pts != 1 {
		t.Errorf("channel pts = %d, want creation pts 1", full.Pts)
	}
	chat := fullChatChannel(t, response)
	if chat.ID != channel.ID || chat.AccessHash != api.DeriveChannelHash(creator.ID, channel.ID) || !chat.Creator {
		t.Errorf("creator chat = id %d hash %d creator %v", chat.ID, chat.AccessHash, chat.Creator)
	}
	if _, ok := full.GetExportedInvite(); ok {
		t.Error("getFullChannel exposed an invite before one was created")
	}
	if got := activeChannelInviteCount(t, ctx, dsn, channel.ID); got != 0 {
		t.Errorf("getFullChannel created %d active invites, want 0", got)
	}
}

func TestHandleGetFullChannelAppliesViewerPolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551294911")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551294912")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551294913")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551294914")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	broadcast := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
		Broadcast: true,
		Title:     "Public setup",
		About:     "Visible description",
	})
	joinChannel(t, ctx, dsn, broadcast.ID, admin.ID)
	joinChannel(t, ctx, dsn, broadcast.ID, member.ID)
	if err := s.SetChannelRole(ctx, broadcast.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	if err := api.ClaimChannelUsernameForTest(s, broadcast.ID, "publicsetup"); err != nil {
		t.Fatalf("claim username: %v", err)
	}
	if _, _, _, err := s.PostChannelMessageAs(ctx, broadcast.ID, creator.ID, "current post", 94911, nil, 0); err != nil {
		t.Fatalf("post channel message: %v", err)
	}
	invite, err := s.CreateChannelInvite(ctx, broadcast.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	h := fullChannelDispatcher(s)
	creatorResponse, rpc := getFullChannelViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, broadcast.ID))
	if rpc != nil {
		t.Fatalf("creator getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	creatorInvite, ok := fullChannelInfo(t, creatorResponse).GetExportedInvite()
	if !ok {
		t.Fatal("creator full info is missing the existing invite")
	}
	if got, ok := creatorInvite.(*tg.ChatInviteExported); !ok || got.Link != inviteLinkForTest(invite) {
		t.Fatalf("creator invite = %#v, want existing link", creatorInvite)
	}

	adminResponse, rpc := getFullChannelViaDispatcher(t, h, admin.ID, false, api.InputChannel(admin.ID, broadcast.ID))
	if rpc != nil {
		t.Fatalf("admin getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	adminFull := fullChannelInfo(t, adminResponse)
	adminChannel := fullChatChannel(t, adminResponse)
	if adminFull.About != "Visible description" || !adminFull.GetCanViewParticipants() || adminFull.Pts != 2 {
		t.Errorf("admin full metadata: about=%q can_view_participants=%v pts=%d", adminFull.About, adminFull.GetCanViewParticipants(), adminFull.Pts)
	}
	if adminChannel.Username != "publicsetup" || adminChannel.AccessHash != api.DeriveChannelHash(admin.ID, broadcast.ID) {
		t.Errorf("admin channel username/hash = %q/%d", adminChannel.Username, adminChannel.AccessHash)
	}
	if count, ok := adminFull.GetAdminsCount(); !ok || count != 2 {
		t.Errorf("admin count = %d present=%v, want 2", count, ok)
	}
	exported, ok := adminFull.GetExportedInvite()
	if !ok {
		t.Fatal("admin full info is missing the existing invite")
	}
	exportedInvite, ok := exported.(*tg.ChatInviteExported)
	if !ok || exportedInvite.Link != inviteLinkForTest(invite) || !exportedInvite.Permanent {
		t.Fatalf("exported invite = %#v, want existing permanent link", exported)
	}
	var creatorUser *tg.User
	for _, item := range adminResponse.Users {
		if user, ok := item.(*tg.User); ok && user.ID == creator.ID {
			creatorUser = user
		}
	}
	if creatorUser == nil || creatorUser.AccessHash != api.DeriveUserHash(admin.ID, creator.ID) || creatorUser.Phone != "" {
		t.Errorf("invite creator user rendered for admin: %#v", creatorUser)
	}
	if got := activeChannelInviteCount(t, ctx, dsn, broadcast.ID); got != 1 {
		t.Errorf("getFullChannel changed active invite count to %d, want 1", got)
	}

	memberResponse, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID))
	if rpc != nil {
		t.Fatalf("member getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	memberFull := fullChannelInfo(t, memberResponse)
	memberChannel := fullChatChannel(t, memberResponse)
	if memberFull.About != "Visible description" || memberFull.GetCanViewParticipants() || memberFull.Pts != 2 {
		t.Errorf("broadcast member metadata: about=%q can_view_participants=%v pts=%d", memberFull.About, memberFull.GetCanViewParticipants(), memberFull.Pts)
	}
	if _, ok := memberFull.GetAdminsCount(); ok {
		t.Error("broadcast member received administrative counts")
	}
	if _, ok := memberFull.GetExportedInvite(); ok {
		t.Error("broadcast member received an invite")
	}
	if memberChannel.AccessHash != api.DeriveChannelHash(member.ID, broadcast.ID) || memberChannel.Username != "publicsetup" {
		t.Errorf("member channel hash/username = %d/%q", memberChannel.AccessHash, memberChannel.Username)
	}

	publicResponse, rpc := getFullChannelViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, broadcast.ID))
	if rpc != nil {
		t.Fatalf("public outsider getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	publicFull := fullChannelInfo(t, publicResponse)
	publicChannel := fullChatChannel(t, publicResponse)
	if publicFull.About != "Visible description" {
		t.Errorf("public about = %q, want visible description", publicFull.About)
	}
	if publicFull.Pts != 0 {
		t.Errorf("public preview exposed channel pts %d", publicFull.Pts)
	}
	if count, ok := publicFull.GetParticipantsCount(); !ok || count != 3 {
		t.Errorf("public participant count = %d present=%v, want 3", count, ok)
	}
	if publicChannel.Title != "Public setup" || publicChannel.Username != "publicsetup" || !publicChannel.Left {
		t.Errorf("public channel preview = title %q username %q left %v", publicChannel.Title, publicChannel.Username, publicChannel.Left)
	}
	if publicChannel.AccessHash != api.DeriveChannelHash(outsider.ID, broadcast.ID) || publicFull.GetCanViewParticipants() {
		t.Errorf("public preview hash/can_view_participants = %d/%v", publicChannel.AccessHash, publicFull.GetCanViewParticipants())
	}
	if _, ok := publicFull.GetExportedInvite(); ok {
		t.Error("public outsider received an invite")
	}
	if _, ok := publicFull.GetAdminsCount(); ok {
		t.Error("public outsider received administrative counts")
	}
	if len(publicResponse.Users) != 0 {
		t.Errorf("public preview returned %d user references, want none", len(publicResponse.Users))
	}

	private := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Private"})
	if _, rpc = getFullChannelViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, private.ID)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("private outsider error = %v, want PEER_ID_INVALID", rpc)
	}
	if _, rpc = getFullChannelViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, private.ID+1)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("absent channel error = %v, want PEER_ID_INVALID", rpc)
	}
	if _, rpc = getFullChannelViaDispatcher(t, h, outsider.ID, false, api.InputChannel(creator.ID, broadcast.ID)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("other viewer's hash error = %v, want PEER_ID_INVALID", rpc)
	}
	if _, rpc = getFullChannelViaDispatcher(t, h, creator.ID, false, &tg.InputChannel{ChannelID: broadcast.ID, AccessHash: broadcast.ID}); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("placeholder hash error = %v, want PEER_ID_INVALID", rpc)
	}
	if err := s.SetChannelBan(ctx, broadcast.ID, creator.ID, member.ID, nil, true); err != nil {
		t.Fatalf("ban member: %v", err)
	}
	if _, rpc = getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("banned public member error = %v, want PEER_ID_INVALID", rpc)
	}

	for _, session := range []struct {
		name        string
		userID      int64
		provisional bool
	}{
		{name: "unauthenticated"},
		{name: "provisional", userID: creator.ID, provisional: true},
	} {
		t.Run(session.name, func(t *testing.T) {
			_, rpc := getFullChannelViaDispatcher(t, h, session.userID, session.provisional, api.InputChannel(creator.ID, broadcast.ID))
			requireFullChannelRPCError(t, rpc, "AUTH_KEY_UNREGISTERED")
		})
	}

	megagroup := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Group"})
	joinChannel(t, ctx, dsn, megagroup.ID, member.ID)
	groupResponse, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, megagroup.ID))
	if rpc != nil {
		t.Fatalf("megagroup member getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if !fullChannelInfo(t, groupResponse).GetCanViewParticipants() {
		t.Error("megagroup member cannot view participants")
	}
}

func TestHandleGetFullChannelKeepsAuthorizationAndMetadataInOneSnapshot(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		mutate func(context.Context, *store.Store, int64, int64, int64) error
	}{
		{
			name: "leave",
			mutate: func(ctx context.Context, s *store.Store, channelID, _, memberID int64) error {
				left, err := s.LeaveChannel(ctx, channelID, memberID)
				if err != nil {
					return err
				}
				if !left {
					return errors.New("member did not leave")
				}
				return nil
			},
		},
		{
			name: "ban",
			mutate: func(ctx context.Context, s *store.Store, channelID, creatorID, memberID int64) error {
				until := time.Now().Add(time.Hour)
				return s.SetChannelBan(ctx, channelID, creatorID, memberID, &until, false)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			creator, err := s.CreateUser(ctx, "+15551294921")
			if err != nil {
				t.Fatalf("creator: %v", err)
			}
			member, err := s.CreateUser(ctx, "+15551294922")
			if err != nil {
				t.Fatalf("member: %v", err)
			}
			channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Snapshot"})
			joinChannel(t, ctx, dsn, channel.ID, member.ID)

			entered := make(chan struct{})
			release := make(chan struct{})
			released := false
			store.SetChannelFullInfoSnapshotHook(s, func() {
				close(entered)
				<-release
			})
			defer func() {
				if !released {
					close(release)
				}
				store.SetChannelFullInfoSnapshotHook(s, nil)
			}()

			type outcome struct {
				response *tg.MessagesChatFull
				rpc      *mt.RPCError
			}
			done := make(chan outcome, 1)
			h := fullChannelDispatcher(s)
			go func() {
				response, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID))
				done <- outcome{response: response, rpc: rpc}
			}()

			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("full-channel snapshot did not reach its read barrier")
			}
			if err := tt.mutate(ctx, s, channel.ID, creator.ID, member.ID); err != nil {
				t.Fatalf("change membership during read: %v", err)
			}
			close(release)
			released = true

			var result outcome
			select {
			case result = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("full-channel read did not finish after membership change")
			}
			if result.rpc != nil {
				t.Fatalf("in-flight snapshot response: %d %s", result.rpc.ErrorCode, result.rpc.ErrorMessage)
			}
			store.SetChannelFullInfoSnapshotHook(s, nil)
			assertEncodes(t, result.response)
			full := fullChannelInfo(t, result.response)
			if count, ok := full.GetParticipantsCount(); !ok || count != 2 {
				t.Errorf("snapshot participant count = %d present=%v, want 2", count, ok)
			}
			if got := fullChatChannel(t, result.response); got.ID != channel.ID || got.Left {
				t.Errorf("snapshot chat = %#v, want the authorized member view", got)
			}
			if _, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Errorf("post-change getFullChannel error = %v, want PEER_ID_INVALID", rpc)
			}
		})
	}
}

func inviteLinkForTest(hash string) string { return testPublicLinkPrefix + "+" + hash }

func TestHandleCreateChannelBroadcast(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294001")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	ch := createChannel(t, s, u.ID, &tg.ChannelsCreateChannelRequest{
		Broadcast: true, Title: "  News  ", About: "  daily  ",
	})
	if !ch.Creator {
		t.Error("creator flag not set on the creator's own view")
	}
	if !ch.Broadcast || ch.Megagroup {
		t.Errorf("broadcast=%v megagroup=%v, want true/false", ch.Broadcast, ch.Megagroup)
	}
	if ch.Title != "News" {
		t.Errorf("title = %q, want %q", ch.Title, "News")
	}
	if ch.AccessHash != api.DeriveChannelHash(u.ID, ch.ID) {
		t.Errorf("access hash = %d, want %d", ch.AccessHash, api.DeriveChannelHash(u.ID, ch.ID))
	}

	stored, ok, err := s.ChannelByID(ctx, ch.ID)
	if err != nil || !ok {
		t.Fatalf("channel by id: ok=%v err=%v", ok, err)
	}
	if stored.About != "daily" {
		t.Errorf("stored about = %q, want %q", stored.About, "daily")
	}
}

func TestHandleCreateChannelMegagroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294002")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	ch := createChannel(t, s, u.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Team"})
	if !ch.Megagroup || ch.Broadcast {
		t.Errorf("megagroup=%v broadcast=%v, want true/false", ch.Megagroup, ch.Broadcast)
	}
}

func TestCreateChannelServiceMessageAppearsOnAllReadPaths(t *testing.T) {
	for _, tc := range []struct {
		name      string
		broadcast bool
		megagroup bool
	}{
		{name: "broadcast", broadcast: true},
		{name: "megagroup", megagroup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := openStore(t)
			user, err := s.CreateUser(ctx, "+15551294020")
			if err != nil {
				t.Fatalf("user: %v", err)
			}
			const title = "First day"
			created, err := api.CreateChannelForTest(s, user.ID, &tg.ChannelsCreateChannelRequest{
				Broadcast: tc.broadcast,
				Megagroup: tc.megagroup,
				Title:     title,
			})
			if err != nil {
				t.Fatalf("create channel: %v", err)
			}
			assertEncodes(t, created)
			updates, ok := created.(*tg.Updates)
			if !ok {
				t.Fatalf("create response = %T, want *tg.Updates", created)
			}
			channel, ok := updates.Chats[0].(*tg.Channel)
			if !ok {
				t.Fatalf("create response chat = %T, want *tg.Channel", updates.Chats[0])
			}

			assertCreateService := func(messages []tg.MessageClass, surface string) {
				t.Helper()
				if len(messages) != 1 {
					t.Fatalf("%s messages = %d, want the one creation message", surface, len(messages))
				}
				service, ok := messages[0].(*tg.MessageService)
				if !ok {
					t.Fatalf("%s message = %T, want *tg.MessageService", surface, messages[0])
				}
				if service.ID != 1 {
					t.Errorf("%s message id = %d, want 1", surface, service.ID)
				}
				action, ok := service.Action.(*tg.MessageActionChannelCreate)
				if !ok {
					t.Fatalf("%s action = %T, want *tg.MessageActionChannelCreate", surface, service.Action)
				}
				if action.Title != title {
					t.Errorf("%s title = %q, want %q", surface, action.Title, title)
				}
			}

			var createMessage tg.MessageClass
			for _, update := range updates.Updates {
				newMessage, ok := update.(*tg.UpdateNewChannelMessage)
				if !ok {
					continue
				}
				if newMessage.Pts != 1 || newMessage.PtsCount != 1 {
					t.Errorf("create update pts = %d/%d, want 1/1", newMessage.Pts, newMessage.PtsCount)
				}
				createMessage = newMessage.Message
				break
			}
			if createMessage == nil {
				t.Fatal("create response has no UpdateNewChannelMessage")
			}
			assertCreateService([]tg.MessageClass{createMessage}, "create response")

			dialogs, err := api.GetDialogsForTest(s, user.ID)
			if err != nil {
				t.Fatalf("get dialogs: %v", err)
			}
			assertEncodes(t, dialogs)
			gotDialogs, ok := dialogs.(*tg.MessagesDialogs)
			if !ok {
				t.Fatalf("get dialogs response = %T, want *tg.MessagesDialogs", dialogs)
			}
			var foundChannel bool
			for _, item := range gotDialogs.Dialogs {
				dialog, ok := item.(*tg.Dialog)
				if !ok {
					continue
				}
				peer, ok := dialog.Peer.(*tg.PeerChannel)
				if ok && peer.ChannelID == channel.ID {
					foundChannel = true
					if dialog.TopMessage != 1 {
						t.Errorf("channel dialog top message = %d, want 1", dialog.TopMessage)
					}
				}
			}
			if !foundChannel {
				t.Fatal("get dialogs omitted the channel before its first post")
			}
			assertCreateService(gotDialogs.Messages, "get dialogs")

			history, err := api.GetHistoryForTest(s, user.ID, &tg.MessagesGetHistoryRequest{
				Peer:  api.InputPeerChannel(user.ID, channel.ID),
				Limit: 10,
			})
			if err != nil {
				t.Fatalf("get history: %v", err)
			}
			assertEncodes(t, history)
			gotHistory, ok := history.(*tg.MessagesChannelMessages)
			if !ok {
				t.Fatalf("get history response = %T, want *tg.MessagesChannelMessages", history)
			}
			assertCreateService(gotHistory.Messages, "get history")

			difference, err := api.GetChannelDifferenceForTest(s, user.ID, &tg.UpdatesGetChannelDifferenceRequest{
				Channel: api.InputChannel(user.ID, channel.ID),
				Filter:  &tg.ChannelMessagesFilterEmpty{},
				Pts:     0,
				Limit:   10,
			})
			if err != nil {
				t.Fatalf("get channel difference: %v", err)
			}
			assertEncodes(t, difference)
			gotDifference, ok := difference.(*tg.UpdatesChannelDifference)
			if !ok {
				t.Fatalf("get channel difference response = %T, want *tg.UpdatesChannelDifference", difference)
			}
			assertCreateService(gotDifference.NewMessages, "get channel difference")
		})
	}
}

func TestBackfillEmptyChannelCreationServiceMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551294029")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "Legacy empty", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Rewind this freshly created channel to the state of an empty channel on
	// the old schema so the migration can be exercised against realistic rows.
	channelExec(t, ctx, dsn, `DELETE FROM channel_events WHERE channel_id = $1`, channel.ID)
	channelExec(t, ctx, dsn, `DELETE FROM channel_messages WHERE channel_id = $1`, channel.ID)
	channelExec(t, ctx, dsn, `UPDATE channel_state SET pts = 0, next_local_id = 1 WHERE channel_id = $1`, channel.ID)

	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "20261003000050_backfill_empty_channel_create_messages.sql"))
	if err != nil {
		t.Fatalf("read channel creation backfill migration: %v", err)
	}
	channelExec(t, ctx, dsn, string(migration))

	assertBackfilled := func() {
		t.Helper()
		if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 1 {
			t.Fatalf("channel state = %d, err=%v; want 1", pts, err)
		}
		history, err := s.ChannelHistory(ctx, channel.ID, 0, 10)
		if err != nil || len(history) != 1 || history[0].LocalID != 1 || history[0].Action != store.ChannelMessageActionCreate || history[0].Message != channel.Title {
			t.Fatalf("backfilled history = %+v, err=%v; want creation service message 1", history, err)
		}
		events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, 1, 10)
		if err != nil || len(events) != 1 || events[0].Pts != 1 || events[0].LocalID != 1 {
			t.Fatalf("backfilled events = %+v, err=%v; want creation event at pts 1", events, err)
		}
		encoded, err := api.GetDialogsForTest(s, creator.ID)
		if err != nil {
			t.Fatalf("get dialogs after backfill: %v", err)
		}
		dialogs, ok := encoded.(*tg.MessagesDialogs)
		if !ok {
			t.Fatalf("get dialogs = %T, want *tg.MessagesDialogs", encoded)
		}
		if len(dialogs.Messages) != 1 {
			t.Fatalf("dialog messages = %v, want one creation service message", dialogs.Messages)
		}
		service, ok := dialogs.Messages[0].(*tg.MessageService)
		if !ok || service.ID != 1 {
			t.Fatalf("dialog message = %T (%v), want creation service message 1", dialogs.Messages[0], dialogs.Messages[0])
		}
	}

	assertBackfilled()
	channelExec(t, ctx, dsn, string(migration))
	assertBackfilled()
}

func TestHandleCreateChannelRejectsAmbiguousKind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294003")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	for name, req := range map[string]*tg.ChannelsCreateChannelRequest{
		"both":    {Broadcast: true, Megagroup: true, Title: "Both"},
		"neither": {Title: "Neither"},
	} {
		_, err := api.CreateChannelForTest(s, u.ID, req)
		if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s: got %s, want PEER_ID_INVALID", name, msg)
		}
	}

	channels, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(channels) != 0 {
		t.Fatalf("created %d channels, want 0", len(channels))
	}
}

func TestHandleCreateChannelRejectsBadMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294004")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	for name, req := range map[string]*tg.ChannelsCreateChannelRequest{
		"empty title":    {Broadcast: true, Title: "   "},
		"long title":     {Broadcast: true, Title: strings.Repeat("a", 256)},
		"nul in title":   {Broadcast: true, Title: "a\x00b"},
		"long about":     {Broadcast: true, Title: "News", About: strings.Repeat("a", 256)},
		"nul in about":   {Broadcast: true, Title: "News", About: "a\x00b"},
		"bad utf8 about": {Broadcast: true, Title: "News", About: "\xff"},
	} {
		_, err := api.CreateChannelForTest(s, u.ID, req)
		if msg := rpcMessage(t, err); msg != "CHAT_TITLE_EMPTY" {
			t.Errorf("%s: got %s, want CHAT_TITLE_EMPTY", name, msg)
		}
	}

	channels, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(channels) != 0 {
		t.Fatalf("created %d channels, want 0", len(channels))
	}
}

func TestHandleGetChannelsHidesMetadataFromStrangers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	owner, err := s.CreateUser(ctx, "+15551294005")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	stranger, err := s.CreateUser(ctx, "+15551294006")
	if err != nil {
		t.Fatalf("stranger: %v", err)
	}
	ch := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Secret"})

	res, err := api.GetChannelsForTest(s, stranger.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(stranger.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get channels: %v", err)
	}
	assertEncodes(t, res)
	chats, ok := res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("got %T, want *tg.MessagesChats", res)
	}
	if len(chats.Chats) != 1 {
		t.Fatalf("%d chats, want 1", len(chats.Chats))
	}
	forbidden, ok := chats.Chats[0].(*tg.ChannelForbidden)
	if !ok {
		t.Fatalf("got %T, want *tg.ChannelForbidden", chats.Chats[0])
	}
	if forbidden.Title != "" {
		t.Errorf("forbidden title = %q, want empty", forbidden.Title)
	}

	// The member's own view still carries the title.
	res, err = api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(owner.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get channels as owner: %v", err)
	}
	ownChats, ok := res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("owner view: got %T, want *tg.MessagesChats", res)
	}
	own, ok := ownChats.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("owner view: got %T, want *tg.Channel", ownChats.Chats[0])
	}
	if own.Title != "Secret" {
		t.Errorf("owner title = %q, want %q", own.Title, "Secret")
	}
}

func TestHandleGetChannelsRejectsOversizedVector(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294007")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	ids := make([]int64, api.MaxGetChannels+1)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	_, err = api.GetChannelsForTest(s, u.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(u.ID, ids...)})
	if msg := rpcMessage(t, err); msg != "USERS_TOO_MUCH" {
		t.Fatalf("got %s, want USERS_TOO_MUCH", msg)
	}
}

func TestHandleGetChannelsRejectsBadInputChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294008")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	for name, in := range map[string]tg.InputChannelClass{
		"empty":      &tg.InputChannelEmpty{},
		"zero id":    &tg.InputChannel{ChannelID: 0, AccessHash: 0},
		"wrong hash": &tg.InputChannel{ChannelID: 7, AccessHash: 8},
	} {
		_, err := api.GetChannelsForTest(s, u.ID, &tg.ChannelsGetChannelsRequest{ID: []tg.InputChannelClass{in}})
		if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s: got %s, want PEER_ID_INVALID", name, msg)
		}
	}
}

func TestHandleLeaveChannelRevokesMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	owner, err := s.CreateUser(ctx, "+15551294009")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ch := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "News"})

	res, err := api.LeaveChannelForTest(s, owner.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: api.InputChannel(owner.ID, ch.ID),
	})
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	assertEncodes(t, res)
	ups, ok := res.(*tg.Updates)
	if !ok {
		t.Fatalf("got %T, want *tg.Updates", res)
	}
	if _, ok := ups.Chats[0].(*tg.ChannelForbidden); !ok {
		t.Fatalf("leave reply: got %T, want *tg.ChannelForbidden", ups.Chats[0])
	}

	// The channel row survives its creator leaving; only the membership goes.
	if _, ok, err := s.ChannelByID(ctx, ch.ID); err != nil || !ok {
		t.Fatalf("channel after creator left: ok=%v err=%v", ok, err)
	}
	after, err := api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(owner.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get channels after leave: %v", err)
	}
	afterChats, ok := after.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("after leave: got %T, want *tg.MessagesChats", after)
	}
	if _, ok := afterChats.Chats[0].(*tg.ChannelForbidden); !ok {
		t.Fatalf("after leave: got %T, want *tg.ChannelForbidden", afterChats.Chats[0])
	}

	// Leaving twice is the same error as leaving a channel that never existed.
	_, err = api.LeaveChannelForTest(s, owner.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: api.InputChannel(owner.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("second leave: got %s, want PEER_ID_INVALID", msg)
	}
	_, err = api.LeaveChannelForTest(s, owner.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID + 1000, AccessHash: ch.ID + 1000},
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("unknown channel: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestChannelHandlersRejectUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	calls := map[string]func() error{
		"createChannel": func() error {
			_, err := api.CreateChannelForTest(s, 0, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "News"})
			return err
		},
		"getChannels": func() error {
			_, err := api.GetChannelsForTest(s, 0, &tg.ChannelsGetChannelsRequest{ID: inputChannels(0, 1)})
			return err
		},
		"leaveChannel": func() error {
			_, err := api.LeaveChannelForTest(s, 0, &tg.ChannelsLeaveChannelRequest{
				Channel: api.InputChannel(0, 1),
			})
			return err
		},
	}
	for name, call := range calls {
		if msg := rpcMessage(t, call()); msg != "AUTH_KEY_UNREGISTERED" {
			t.Errorf("%s: got %s, want AUTH_KEY_UNREGISTERED", name, msg)
		}
	}
}

func TestHandleCreateChannelAtPerAccountCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	u, err := s.CreateUser(ctx, "+15551294010")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	// 500 is the store's default per-account cap (defaultMaxChannelsPerUser,
	// internal/store/channels.go:41). Filling it with one statement instead of 500
	// handler calls keeps the test cheap; the store's own lowered-cap helper is not
	// reachable from this package.
	channelExec(t, ctx, dsn, `
		WITH created AS (
			INSERT INTO channels (id, title, creator_id)
			SELECT g, 'filler ' || g, $1 FROM generate_series(1, 500) g
			RETURNING id
		)
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		SELECT id, $1, 2, 0 FROM created`, u.ID)

	_, err = api.CreateChannelForTest(s, u.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "One more"})
	if msg := rpcMessage(t, err); msg != "USERS_TOO_MUCH" {
		t.Fatalf("at cap: got %s, want USERS_TOO_MUCH", msg)
	}
	channels, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(channels) != 500 {
		t.Fatalf("holds %d channels, want 500 — the rejected create wrote a row", len(channels))
	}
}

func TestHandleGetChannelsHidesMetadataFromBannedMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	owner, err := s.CreateUser(ctx, "+15551294011")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551294012")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Secret"})
	joinChannel(t, ctx, dsn, ch.ID, member.ID)

	// While the ban is live the member is a stranger to the metadata.
	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))
	res, err := api.GetChannelsForTest(s, member.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(member.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get channels while banned: %v", err)
	}
	assertEncodes(t, res)
	chats, ok := res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("got %T, want *tg.MessagesChats", res)
	}
	forbidden, ok := chats.Chats[0].(*tg.ChannelForbidden)
	if !ok {
		t.Fatalf("banned member: got %T, want *tg.ChannelForbidden", chats.Chats[0])
	}
	if forbidden.Title != "" {
		t.Errorf("banned member title = %q, want empty", forbidden.Title)
	}

	// An expired ban is not a ban: the same row gets the title back.
	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(-time.Hour))
	res, err = api.GetChannelsForTest(s, member.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(member.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get channels after ban expiry: %v", err)
	}
	assertEncodes(t, res)
	chats, ok = res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("got %T, want *tg.MessagesChats", res)
	}
	live, ok := chats.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("expired ban: got %T, want *tg.Channel", chats.Chats[0])
	}
	if live.Title != "Secret" {
		t.Errorf("title = %q, want %q", live.Title, "Secret")
	}
}

func TestHandleGetChannelsDropsUnknownIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	owner, err := s.CreateUser(ctx, "+15551294013")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ch := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "News"})

	// An id with no channel row is dropped, not reported: a distinguishable
	// not-found would answer "does this channel exist" for any id submitted.
	res, err := api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{
		ID: inputChannels(owner.ID, ch.ID, ch.ID+100000),
	})
	if err != nil {
		t.Fatalf("get channels: %v", err)
	}
	assertEncodes(t, res)
	chats, ok := res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("got %T, want *tg.MessagesChats", res)
	}
	if len(chats.Chats) != 1 {
		t.Fatalf("%d chats, want 1", len(chats.Chats))
	}
	if got := chats.Chats[0].GetID(); got != ch.ID {
		t.Errorf("chat id = %d, want %d", got, ch.ID)
	}
}

// TestHandleCreateChannelDrawsSparseIDs is the wire half of the sparse-id draw:
// the id the handler hands the client is the drawn one, whole and unadjacent.
// The store's own tests cover the draw; what only shows up here is a truncation
// on the encode path, because the range sits well past int32 and a truncated id
// would read as a plausible small one.
func TestHandleCreateChannelDrawsSparseIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294017")
	if err != nil {
		t.Fatalf("user: %v", err)
	}

	first := createChannel(t, s, u.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "First"})
	second := createChannel(t, s, u.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Second"})

	// The floor of the documented range (internal/store/channels.go). Anything
	// below it came from the sequence, or arrived here truncated.
	const idFloor = int64(1) << 31
	for _, ch := range []*tg.Channel{first, second} {
		if ch.ID < idFloor {
			t.Errorf("channel %q id = %d, below the range floor %d", ch.Title, ch.ID, idFloor)
		}
		if _, ok, err := s.ChannelByID(ctx, ch.ID); err != nil || !ok {
			t.Errorf("channel %q id %d does not resolve in the store: ok=%v err=%v", ch.Title, ch.ID, ok, err)
		}
	}
	// Absolute distance, not second == first+1: a descending counter is as much
	// a creation-order oracle as an ascending one.
	if diff := second.ID - first.ID; diff == 1 || diff == -1 {
		t.Errorf("consecutive creations got adjacent ids %d, %d", first.ID, second.ID)
	}
}

// TestGetChannelsEnumerationClosed verifies that submitting channel ids with
// guessed access hashes returns the same PEER_ID_INVALID for both live and
// non-existent channels. This is the AC requirement: "Submitting 100 channel ids
// with guessed hashes yields the same response for live channels and
// non-existent ones."
//
// The live channels have to stay in the probe set. A live id paired with a wrong
// hash is the only probe that separates "the hash gate refused" from "no such
// channel" — a sweep that lands on nothing live proves only the second, whatever
// its length. access_hash == channel_id is the guess: it was the M1 placeholder,
// and it is now simply a hash the per-viewer derivation never produces.
func TestGetChannelsEnumerationClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	owner, err := s.CreateUser(ctx, "+15551294016")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	// Create a few live channels.
	ch1 := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "A"})
	ch2 := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "B"})

	// 100 ids: both live channels, then a walk from one of them. Channel ids are
	// random draws, so the walk is all misses and the two live ids are placed
	// deliberately rather than swept into the batch by adjacency.
	ids := make([]tg.InputChannelClass, 0, 100)
	for _, id := range []int64{ch1.ID, ch2.ID} {
		ids = append(ids, &tg.InputChannel{ChannelID: id, AccessHash: id})
	}
	for i := range 98 {
		id := ch1.ID + int64(i) + 1
		ids = append(ids, &tg.InputChannel{ChannelID: id, AccessHash: id})
	}

	res, err := api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{ID: ids})
	if err == nil {
		t.Fatal("get channels with guessed hashes: expected error, got nil")
	}
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
	// Response must be empty — no channels leaked.
	if res != nil {
		chats, ok := res.(*tg.MessagesChats)
		if ok && len(chats.Chats) > 0 {
			t.Fatalf("enumeration leaked %d channels", len(chats.Chats))
		}
	}
}

func TestHandleLeaveChannelRejectsBannedMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	owner, err := s.CreateUser(ctx, "+15551294014")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551294015")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch := createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "News"})
	joinChannel(t, ctx, dsn, ch.ID, member.ID)
	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))

	// Leaving under a live ban must not delete the row: the join path admits any
	// account without one, so a successful leave here is a ban reset.
	_, err = api.LeaveChannelForTest(s, member.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: api.InputChannel(member.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("banned leave: got %s, want PEER_ID_INVALID", msg)
	}
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("participant row after rejected leave: found=%v err=%v", found, err)
	}
	if !m.Banned(time.Now()) {
		t.Fatal("ban cleared by the rejected leave")
	}

	// Once the ban has expired the same member leaves normally.
	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(-time.Hour))
	if _, err := api.LeaveChannelForTest(s, member.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: api.InputChannel(member.ID, ch.ID),
	}); err != nil {
		t.Fatalf("leave after ban expiry: %v", err)
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID); err != nil || found {
		t.Fatalf("participant row after leave: found=%v err=%v", found, err)
	}
}

// joinChannelByInvite adds userID to ch through the invite path, the only admission
// route a channel has, and returns the participant row it created.
func joinChannelByInvite(t *testing.T, s *store.Store, ch store.Channel, userID int64) store.ChannelMember {
	t.Helper()
	ctx := context.Background()
	hash, err := s.CreateChannelInvite(ctx, ch.ID, ch.CreatorID)
	if err != nil {
		t.Fatalf("export invite: %v", err)
	}
	_, m, err := s.JoinChannelByInvite(ctx, hash, userID)
	if err != nil {
		t.Fatalf("join channel: %v", err)
	}
	return m
}

func channelPeer(viewerID, id int64) tg.InputPeerClass {
	return api.InputPeerChannel(viewerID, id)
}

func sendToChannel(t *testing.T, s *store.Store, userID, channelID int64, text string, randomID int64) (bin.Encoder, error) {
	t.Helper()
	return api.SendMessageForTest(s, userID, &tg.MessagesSendMessageRequest{
		Peer: channelPeer(userID, channelID), Message: text, RandomID: randomID,
	})
}

func updatesOf(t *testing.T, enc bin.Encoder) *tg.Updates {
	t.Helper()
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("reply = %T, want *tg.Updates", enc)
	}
	return ups
}

// newChannelMessage pulls the post announcement out of a send reply.
func newChannelMessage(t *testing.T, enc bin.Encoder) *tg.UpdateNewChannelMessage {
	t.Helper()
	ups := updatesOf(t, enc)
	for _, u := range ups.Updates {
		if nm, isNew := u.(*tg.UpdateNewChannelMessage); isNew {
			return nm
		}
	}
	t.Fatalf("no updateNewChannelMessage in %+v", ups.Updates)
	return nil
}

func TestSendMessageToChannelAnnouncesTheChannelPost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	res, err := sendToChannel(t, s, creator.ID, ch.ID, "first", 111)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	assertEncodes(t, res)

	nm := newChannelMessage(t, res)
	if nm.Pts != 2 || nm.PtsCount != 1 {
		t.Errorf("pts = %d/%d, want 2/1", nm.Pts, nm.PtsCount)
	}
	msg, ok := nm.Message.(*tg.Message)
	if !ok {
		t.Fatalf("message = %T, want *tg.Message", nm.Message)
	}
	if msg.Message != "first" || msg.ID != 2 || !msg.Out {
		t.Errorf("message = %+v, want out post 2 %q", msg, "first")
	}
	if peer, isChan := msg.PeerID.(*tg.PeerChannel); !isChan || peer.ChannelID != ch.ID {
		t.Errorf("peer = %+v, want channel %d", msg.PeerID, ch.ID)
	}
	if from, isUser := msg.FromID.(*tg.PeerUser); !isUser || from.UserID != creator.ID {
		t.Errorf("from = %+v, want user %d", msg.FromID, creator.ID)
	}

	ups := updatesOf(t, res)
	if len(ups.Chats) != 1 {
		t.Fatalf("chats = %d, want 1", len(ups.Chats))
	}
	if _, isChan := ups.Chats[0].(*tg.Channel); !isChan {
		t.Errorf("chat = %T, want *tg.Channel", ups.Chats[0])
	}
}

func TestSendMessageToBroadcastRejectsAPlainMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292011")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551292012")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if m := joinChannelByInvite(t, s, ch, member.ID); m.Role != 0 {
		t.Fatalf("joined at role %d, want 0", m.Role)
	}

	if _, err = sendToChannel(t, s, member.ID, ch.ID, "hi", 222); err == nil {
		t.Fatal("expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}

	// Rejected posts write nothing, so only the creation event remains.
	pts, err := s.ChannelState(ctx, ch.ID)
	if err != nil || pts != 1 {
		t.Fatalf("pts = %d err=%v, want creation pts 1", pts, err)
	}
}

func TestSendMessageToMegagroupAcceptsAPlainMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292021")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551292022")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	res, err := sendToChannel(t, s, member.ID, ch.ID, "hello", 333)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	assertEncodes(t, res)
	if nm := newChannelMessage(t, res); nm.Pts != 2 {
		t.Errorf("pts = %d, want 2", nm.Pts)
	}
}

func TestSendMessageToChannelResendIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292031")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	first, err := sendToChannel(t, s, creator.ID, ch.ID, "once", 444)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	again, err := sendToChannel(t, s, creator.ID, ch.ID, "once", 444)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)

	a, b := newChannelMessage(t, first), newChannelMessage(t, again)
	if a.Message.GetID() != b.Message.GetID() {
		t.Errorf("resend id = %d, want %d", b.Message.GetID(), a.Message.GetID())
	}
	pts, err := s.ChannelState(ctx, ch.ID)
	if err != nil || pts != 2 {
		t.Fatalf("pts = %d err=%v, want 2 (resend must not advance it)", pts, err)
	}
}

func TestChannelReadsRejectANonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292041")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551292042")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "secret", 555); err != nil {
		t.Fatalf("seed post: %v", err)
	}

	_, err = api.GetChannelMessagesForTest(s, outsider.ID, &tg.ChannelsGetMessagesRequest{
		Channel: api.InputChannel(outsider.ID, ch.ID),
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: 1}},
	})
	if err == nil {
		t.Fatal("getMessages: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getMessages: got %s, want PEER_ID_INVALID", msg)
	}

	_, err = api.GetHistoryForTest(s, outsider.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(outsider.ID, ch.ID)})
	if err == nil {
		t.Fatal("getHistory: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getHistory: got %s, want PEER_ID_INVALID", msg)
	}
}

// A ban revokes reads, not just writes. M7 has no ban-writing store method yet
// — that lands in MAIN-93 — so the row is set directly here rather than leaving
// requireChannelMember's Banned(now) branch with no test at all: a regression
// that dropped the ban check would otherwise pass green.
func TestChannelReadsRejectABannedMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551292081")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551292082")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "before the ban", 900); err != nil {
		t.Fatalf("seed post: %v", err)
	}

	// The member reads fine right up to the ban, so the rejection below is the
	// ban and not a membership problem.
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)}); err != nil {
		t.Fatalf("history before ban: %v", err)
	}

	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))

	_, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)})
	if err == nil {
		t.Fatal("getHistory: expected PEER_ID_INVALID for a banned member, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getHistory: got %s, want PEER_ID_INVALID", msg)
	}

	_, err = api.GetChannelMessagesForTest(s, member.ID, &tg.ChannelsGetMessagesRequest{
		Channel: api.InputChannel(member.ID, ch.ID),
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: 1}},
	})
	if err == nil {
		t.Fatal("getMessages: expected PEER_ID_INVALID for a banned member, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getMessages: got %s, want PEER_ID_INVALID", msg)
	}
}

// A member reads the channel's whole history, including the posts made before
// they joined. join_pts bounds the difference path's replay only; it is a cost
// control and never a confidentiality one, so this is asserted rather than
// assumed.
func TestChannelHistoryServesPostsFromBeforeTheMemberJoined(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292051")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	latecomer, err := s.CreateUser(ctx, "+15551292052")
	if err != nil {
		t.Fatalf("create latecomer: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "one", 661); err != nil {
		t.Fatalf("post one: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "two", 662); err != nil {
		t.Fatalf("post two: %v", err)
	}

	m := joinChannelByInvite(t, s, ch, latecomer.ID)
	if m.JoinPts != 3 {
		t.Fatalf("join_pts = %d, want 3 (the latecomer joined after both posts)", m.JoinPts)
	}

	res, err := api.GetHistoryForTest(s, latecomer.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(latecomer.ID, ch.ID)})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	assertEncodes(t, res)
	got, ok := res.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", res)
	}
	if got.Pts != 3 || got.Count != 3 || len(got.Messages) != 3 {
		t.Fatalf("pts=%d count=%d messages=%d, want 3/3/3", got.Pts, got.Count, len(got.Messages))
	}
	// Newest first.
	if got.Messages[0].GetID() != 3 || got.Messages[1].GetID() != 2 || got.Messages[2].GetID() != 1 {
		t.Errorf("ids = %d,%d,%d, want 3,2,1", got.Messages[0].GetID(), got.Messages[1].GetID(), got.Messages[2].GetID())
	}
	first, isMsg := got.Messages[1].(*tg.Message)
	if !isMsg || first.Message != "one" || first.Out {
		t.Errorf("second newest = %+v, want inbound %q", got.Messages[1], "one")
	}
}

func TestGetChannelMessagesReturnsTheNamedPosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292061")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for i, text := range []string{"one", "two", "three"} {
		if _, err = sendToChannel(t, s, creator.ID, ch.ID, text, int64(700+i)); err != nil {
			t.Fatalf("post %s: %v", text, err)
		}
	}

	res, err := api.GetChannelMessagesForTest(s, creator.ID, &tg.ChannelsGetMessagesRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		// The fourth id has no row and must simply be absent from the reply.
		ID: []tg.InputMessageClass{&tg.InputMessageID{ID: 4}, &tg.InputMessageID{ID: 2}, &tg.InputMessageID{ID: 99}},
	})
	if err != nil {
		t.Fatalf("get channel messages: %v", err)
	}
	assertEncodes(t, res)
	got, ok := res.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", res)
	}
	if got.Pts != 4 || got.Count != 2 || len(got.Messages) != 2 {
		t.Fatalf("pts=%d count=%d messages=%d, want 4/2/2", got.Pts, got.Count, len(got.Messages))
	}
	if got.Messages[0].GetID() != 4 || got.Messages[1].GetID() != 2 {
		t.Errorf("ids = %d,%d, want the requested order 4,2", got.Messages[0].GetID(), got.Messages[1].GetID())
	}
}

// Channels are not part of the paged dialogs sequence, so they ship once, on
// the first page. A later page repeating them would hand a client that pages to
// the end one copy per page, and a page whose Count omitted them would advertise
// a total smaller than the list it ships.
func TestGetDialogsShipsChannelsOnTheFirstPageOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	me, err := s.CreateUser(ctx, "+15551292091")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551292092")
	if err != nil {
		t.Fatalf("create peer: %v", err)
	}
	if _, err = api.SendMessageForTest(s, me.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(me.ID, other.ID), Message: "hi", RandomID: 910,
	}); err != nil {
		t.Fatalf("seed 1:1 dialog: %v", err)
	}
	ch, err := s.CreateChannel(ctx, me.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, me.ID, ch.ID, "post", 911); err != nil {
		t.Fatalf("seed post: %v", err)
	}

	// Limit 1 with one dialog row is a full page, so the reply is the slice form
	// that carries a count.
	first, err := api.GetDialogsPageForTest(s, me.ID, &tg.MessagesGetDialogsRequest{
		Limit: 1, OffsetPeer: &tg.InputPeerEmpty{},
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	assertEncodes(t, first)
	slice, ok := first.(*tg.MessagesDialogsSlice)
	if !ok {
		t.Fatalf("first page = %T, want *tg.MessagesDialogsSlice", first)
	}
	if len(slice.Dialogs) != 2 {
		t.Fatalf("first page dialogs = %d, want the 1:1 row plus the channel", len(slice.Dialogs))
	}
	if slice.Count != len(slice.Dialogs) {
		t.Errorf("count = %d, want %d — a page may not advertise fewer than it ships", slice.Count, len(slice.Dialogs))
	}

	// Second page: the paged sequence is exhausted and the channel block must not
	// come back with it.
	second, err := api.GetDialogsPageForTest(s, me.ID, &tg.MessagesGetDialogsRequest{
		Limit: 1, OffsetID: 1, OffsetPeer: &tg.InputPeerEmpty{},
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	assertEncodes(t, second)
	page2, ok := second.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("second page = %T, want *tg.MessagesDialogs", second)
	}
	for _, d := range page2.Dialogs {
		if dlg, isDialog := d.(*tg.Dialog); isDialog {
			if _, isChan := dlg.Peer.(*tg.PeerChannel); isChan {
				t.Errorf("channel repeated on page 2: %+v", dlg.Peer)
			}
		}
	}
}

func TestGetDialogsListsTheCallersChannels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551292071")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551292072")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	// This empty channel has no membership for member and must not appear.
	if _, err = s.CreateChannel(ctx, creator.ID, "Empty", "", false); err != nil {
		t.Fatalf("create empty channel: %v", err)
	}
	for i, text := range []string{"one", "two"} {
		if _, err = sendToChannel(t, s, creator.ID, ch.ID, text, int64(800+i)); err != nil {
			t.Fatalf("post %s: %v", text, err)
		}
	}

	res, err := api.GetDialogsForTest(s, member.ID)
	if err != nil {
		t.Fatalf("get dialogs: %v", err)
	}
	assertEncodes(t, res)
	got, ok := res.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesDialogs", res)
	}
	if len(got.Dialogs) != 1 {
		t.Fatalf("dialogs = %d, want 1", len(got.Dialogs))
	}
	d, isDialog := got.Dialogs[0].(*tg.Dialog)
	if !isDialog {
		t.Fatalf("dialog = %T, want *tg.Dialog", got.Dialogs[0])
	}
	peer, isChan := d.Peer.(*tg.PeerChannel)
	if !isChan || peer.ChannelID != ch.ID {
		t.Fatalf("peer = %+v, want channel %d", d.Peer, ch.ID)
	}
	if d.TopMessage != 3 {
		t.Errorf("top message = %d, want 3", d.TopMessage)
	}
	if pts, hasPts := d.GetPts(); !hasPts || pts != 3 {
		t.Errorf("pts = %d present=%v, want 3", pts, hasPts)
	}
	if len(got.Messages) != 1 || got.Messages[0].GetID() != 3 {
		t.Errorf("messages = %+v, want the channel's post 3", got.Messages)
	}
	if len(got.Chats) != 1 {
		t.Fatalf("chats = %d, want the channel", len(got.Chats))
	}
	if _, isChannel := got.Chats[0].(*tg.Channel); !isChannel {
		t.Errorf("chat = %T, want *tg.Channel", got.Chats[0])
	}
}

// unknownHash is a well-formed hash of the right shape that was never issued —
// 22 base64url characters, the width store.CreateChannelInvite emits.
const unknownHash = "0000000000000000000000"

// inviteHash pulls the hash back out of the exported link, which is the only
// form a client ever sees it in.
func inviteHash(t *testing.T, link string) string {
	t.Helper()
	hash, ok := strings.CutPrefix(link, testPublicLinkPrefix+"+")
	if !ok {
		t.Fatalf("link %q has no invite prefix", link)
	}
	return hash
}

// channelWith creates one broadcast channel owned by a fresh account, and
// returns the creator, the channel and a second account that is not in it.
func channelWith(t *testing.T, s *store.Store, creatorPhone, otherPhone string) (creator, other store.User, ch store.Channel) {
	t.Helper()
	ctx := context.Background()
	creator, err := s.CreateUser(ctx, creatorPhone)
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	other, err = s.CreateUser(ctx, otherPhone)
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	ch, err = s.CreateChannel(ctx, creator.ID, "Broadcast", "about", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return creator, other, ch
}

// exportInvite exports an invite as userID and returns its hash.
func exportInvite(t *testing.T, s *store.Store, userID int64, ch store.Channel) string {
	t.Helper()
	res, err := api.ExportChatInviteForTest(s, userID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(userID, ch.ID),
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	assertEncodes(t, res)
	exported, ok := res.(*tg.ChatInviteExported)
	if !ok {
		t.Fatalf("export: got %T, want *tg.ChatInviteExported", res)
	}
	if exported.AdminID != userID {
		t.Errorf("admin id: got %d, want %d", exported.AdminID, userID)
	}
	if !exported.Permanent {
		t.Error("invite is not marked permanent, but M7 stores no expiry")
	}
	return inviteHash(t, exported.Link)
}

func TestExportAndImportChannelInvite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551293001", "+15551293002")

	hash := exportInvite(t, s, creator.ID, ch)

	res, err := api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	assertEncodes(t, res)
	joinResult, ok := res.(*tg.MessagesChatInviteJoinResultOk)
	if !ok {
		t.Fatalf("import: got %T, want *tg.MessagesChatInviteJoinResultOk", res)
	}
	upd, ok := joinResult.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("import updates: got %T, want *tg.Updates", joinResult.Updates)
	}
	if len(upd.Updates) != 1 {
		t.Fatalf("updates: got %d, want 1", len(upd.Updates))
	}
	if u, ok := upd.Updates[0].(*tg.UpdateChannel); !ok || u.ChannelID != ch.ID {
		t.Errorf("update: got %#v, want UpdateChannel for %d", upd.Updates[0], ch.ID)
	}
	if len(upd.Chats) != 1 {
		t.Fatalf("chats: got %d, want 1", len(upd.Chats))
	}
	if c, ok := upd.Chats[0].(*tg.Channel); !ok || c.ID != ch.ID || c.Title != ch.Title {
		t.Errorf("chat: got %#v, want live channel %d", upd.Chats[0], ch.ID)
	}

	member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !found || member.Role != 0 {
		t.Fatalf("joiner membership: found=%v role=%d, want found role 0", found, member.Role)
	}
}

func TestConfiguredPublicLinkPrefixControlsConfigAndChannelInvites(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551293021", "+15551293022")
	const prefixA = "https://links.example.test/"
	const prefixB = "https://community.example.test/"

	var hashA, fullLinkA string
	for _, prefix := range []string{prefixA, prefixB} {
		getConfig := api.GetConfigSeqForTest(2, "127.0.0.1", 443, time.Now, prefix)
		cfg, err := getConfig()
		if err != nil {
			t.Fatalf("help.getConfig for %q: %v", prefix, err)
		}
		if cfg.MeURLPrefix != prefix {
			t.Errorf("help.getConfig me_url_prefix = %q, want %q", cfg.MeURLPrefix, prefix)
		}
		if cfg.DCTxtDomainName != "" {
			t.Errorf("help.getConfig dc_txt_domain_name = %q, want empty", cfg.DCTxtDomainName)
		}
		exported, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{
			Peer: api.InputPeerChannel(creator.ID, ch.ID),
		}, prefix)
		if err != nil {
			t.Fatalf("export under %q: %v", prefix, err)
		}
		invite, ok := exported.(*tg.ChatInviteExported)
		if !ok {
			t.Fatalf("export under %q = %T, want *tg.ChatInviteExported", prefix, exported)
		}
		hash, ok := strings.CutPrefix(invite.Link, prefix+"+")
		if !ok || !isInviteHash(hash) {
			t.Fatalf("export under %q link = %q, want configured prefix plus a 22-character base64url hash", prefix, invite.Link)
		}
		if prefix == prefixA {
			hashA = hash
			fullLinkA = invite.Link
		}

		response, rpc := getFullChannelViaDispatcher(t, fullChannelDispatcher(s, prefix), creator.ID, false, api.InputChannel(creator.ID, ch.ID))
		if rpc != nil {
			t.Fatalf("getFullChannel under %q: %d %s", prefix, rpc.ErrorCode, rpc.ErrorMessage)
		}
		exportedInvite, ok := fullChannelInfo(t, response).GetExportedInvite()
		if !ok {
			t.Fatalf("getFullChannel under %q omitted the exported invite", prefix)
		}
		gotInvite, ok := exportedInvite.(*tg.ChatInviteExported)
		if !ok {
			t.Fatalf("getFullChannel under %q invite = %T, want *tg.ChatInviteExported", prefix, exportedInvite)
		}
		link := gotInvite.Link
		if want := prefix + "+" + hash; link != want {
			t.Errorf("getFullChannel under %q link = %q, want %q", prefix, link, want)
		}
	}

	if _, err := api.CheckChatInviteForTest(s, joiner.ID, &tg.MessagesCheckChatInviteRequest{Hash: fullLinkA}, prefixB); rpcMessage(t, err) != "PEER_ID_INVALID" {
		t.Errorf("check full link under origin B: got %v, want PEER_ID_INVALID", err)
	}
	if _, err := api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: fullLinkA}, prefixB); rpcMessage(t, err) != "PEER_ID_INVALID" {
		t.Errorf("import full link under origin B: got %v, want PEER_ID_INVALID", err)
	}
	if _, err := api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hashA}, prefixB); err != nil {
		t.Fatalf("import hash minted under origin A while using origin B: %v", err)
	}
}

func isInviteHash(hash string) bool {
	if len(hash) != 22 {
		return false
	}
	for i := range len(hash) {
		c := hash[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func TestExportChatInviteRejectsUnauthorized(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, member, ch := channelWith(t, s, "+15551293011", "+15551293012")

	// A role-0 member: joined through the creator's invite, exactly as a real one
	// arrives.
	hash := exportInvite(t, s, creator.ID, ch)
	if _, err := api.ExportChatInviteForTest(s, member.ID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(member.ID, ch.ID),
	}); err == nil {
		t.Error("non-member exported an invite")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("non-member export: got %s, want PEER_ID_INVALID", msg)
	}
	if _, err := api.ImportChatInviteForTest(s, member.ID, &tg.MessagesImportChatInviteRequest{Hash: hash}); err != nil {
		t.Fatalf("import: %v", err)
	}

	peer := api.InputPeerChannel(member.ID, ch.ID)
	if _, err := api.ExportChatInviteForTest(s, member.ID, &tg.MessagesExportChatInviteRequest{Peer: peer}); err == nil {
		t.Error("role-0 member exported an invite")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("role-0 member: got %s, want PEER_ID_INVALID", msg)
	}

	// A banned admin. The creator is role 2, so banning them covers "banned" while
	// the role check is satisfied — the ban has to be what rejects it.
	banChannelMember(t, ctx, dsn, ch.ID, creator.ID, time.Now().Add(time.Hour))
	if _, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{Peer: api.InputPeerChannel(creator.ID, ch.ID)}); err == nil {
		t.Error("banned admin exported an invite")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("banned admin: got %s, want PEER_ID_INVALID", msg)
	}
}

// TestExportChatInviteRejectsNonChannelPeer pins that the peer type gate holds:
// chats have no invites in M7, and a user peer never had any.
func TestExportChatInviteRejectsNonChannelPeer(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+15551293051", "+15551293052")

	for name, peer := range map[string]tg.InputPeerClass{
		"chat": &tg.InputPeerChat{ChatID: ch.ID},
		"user": api.InputPeerUser(creator.ID, creator.ID),
		"self": &tg.InputPeerSelf{},
	} {
		_, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{Peer: peer})
		if err == nil {
			t.Errorf("%s peer: exported an invite", name)
		} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s peer: got %s, want PEER_ID_INVALID", name, msg)
		}
	}
}

// TestBannedMemberSeesForbiddenChannel covers the one behaviour chosen against
// the ticket text: check and import hand a banned member the forbidden channel
// form, not the live one. A ban revokes metadata the same way leaving does, so a
// re-join must not restore the title the ban took away — and JoinChannelByInvite
// returns a banned member's row untouched, so import is reachable while banned.
func TestBannedMemberSeesForbiddenChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, member, ch := channelWith(t, s, "+15551293061", "+15551293062")
	hash := exportInvite(t, s, creator.ID, ch)

	if _, err := api.ImportChatInviteForTest(s, member.ID, &tg.MessagesImportChatInviteRequest{Hash: hash}); err != nil {
		t.Fatalf("import: %v", err)
	}
	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))

	res, err := api.CheckChatInviteForTest(s, member.ID, &tg.MessagesCheckChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("check as banned: %v", err)
	}
	assertEncodes(t, res)
	already, ok := res.(*tg.ChatInviteAlready)
	if !ok {
		t.Fatalf("check as banned: got %T, want *tg.ChatInviteAlready", res)
	}
	if f, ok := already.Chat.(*tg.ChannelForbidden); !ok {
		t.Errorf("check as banned: got %#v, want *tg.ChannelForbidden", already.Chat)
	} else if f.Title != "" {
		t.Errorf("check as banned: forbidden channel leaked title %q", f.Title)
	}

	res, err = api.ImportChatInviteForTest(s, member.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import as banned: %v", err)
	}
	assertEncodes(t, res)
	joinResult, ok := res.(*tg.MessagesChatInviteJoinResultOk)
	if !ok {
		t.Fatalf("import as banned: got %T, want *tg.MessagesChatInviteJoinResultOk", res)
	}
	bannedUpd, ok := joinResult.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("import as banned updates: got %T, want *tg.Updates", joinResult.Updates)
	}
	if len(bannedUpd.Chats) != 1 {
		t.Fatalf("chats: got %d, want 1", len(bannedUpd.Chats))
	}
	if f, ok := bannedUpd.Chats[0].(*tg.ChannelForbidden); !ok {
		t.Errorf("import as banned: got %#v, want *tg.ChannelForbidden", bannedUpd.Chats[0])
	} else if f.Title != "" {
		t.Errorf("import as banned: forbidden channel leaked title %q", f.Title)
	}

	// The ban survives the re-join: it must not be cleared by importing again.
	after, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !found || !after.Banned(time.Now()) {
		t.Errorf("re-join cleared the ban: found=%v banned_until=%v", found, after.BannedUntil)
	}
}

// TestInviteFailuresAreIndistinguishable pins the whole point of the admission
// boundary: a wrong hash on check and on import, and an export against a channel
// that does not exist, must all be the same wire error. Anything finer turns a
// channel id or the invite space into an existence oracle.
func TestInviteFailuresAreIndistinguishable(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, stranger, ch := channelWith(t, s, "+15551293021", "+15551293022")

	_, checkErr := api.CheckChatInviteForTest(s, stranger.ID, &tg.MessagesCheckChatInviteRequest{Hash: unknownHash})
	_, importErr := api.ImportChatInviteForTest(s, stranger.ID, &tg.MessagesImportChatInviteRequest{Hash: unknownHash})
	// An id past every channel this test created, so the row genuinely is absent.
	// Derived hash lets the peer pass validation; the channel row is still missing.
	_, exportErr := api.ExportChatInviteForTest(s, stranger.ID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(stranger.ID, ch.ID+1_000_000),
	})
	// A channel that DOES exist but the caller is not in, which must not be
	// distinguishable from one that does not exist.
	_, strangerErr := api.ExportChatInviteForTest(s, stranger.ID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(stranger.ID, ch.ID),
	})

	for name, err := range map[string]error{
		"check unknown hash":  checkErr,
		"import unknown hash": importErr,
		"export unknown chan": exportErr,
		"export non-member":   strangerErr,
	} {
		if err == nil {
			t.Fatalf("%s: got nil error", name)
		}
		if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s: got %s, want PEER_ID_INVALID", name, msg)
		}
	}
}

func TestCheckChatInviteWritesNothing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, viewer, ch := channelWith(t, s, "+15551293031", "+15551293032")
	hash := exportInvite(t, s, creator.ID, ch)

	before, err := s.ChannelMembers(ctx, ch.ID)
	if err != nil {
		t.Fatalf("members before: %v", err)
	}

	res, err := api.CheckChatInviteForTest(s, viewer.ID, &tg.MessagesCheckChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	assertEncodes(t, res)
	invite, ok := res.(*tg.ChatInvite)
	if !ok {
		t.Fatalf("check: got %T, want *tg.ChatInvite", res)
	}
	if invite.Title != ch.Title {
		t.Errorf("title: got %q, want %q", invite.Title, ch.Title)
	}
	if len(invite.Participants) != 0 {
		t.Errorf("participants: got %d, want none", len(invite.Participants))
	}

	after, err := s.ChannelMembers(ctx, ch.ID)
	if err != nil {
		t.Fatalf("members after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("check seated someone: %d participants before, %d after", len(before), len(after))
	}

	// A member gets the already-joined form instead, still without writing.
	res, err = api.CheckChatInviteForTest(s, creator.ID, &tg.MessagesCheckChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("check as member: %v", err)
	}
	assertEncodes(t, res)
	already, ok := res.(*tg.ChatInviteAlready)
	if !ok {
		t.Fatalf("check as member: got %T, want *tg.ChatInviteAlready", res)
	}
	if c, ok := already.Chat.(*tg.Channel); !ok || c.ID != ch.ID {
		t.Errorf("already chat: got %#v, want live channel %d", already.Chat, ch.ID)
	}
}

func TestImportChatInviteIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551293041", "+15551293042")
	hash := exportInvite(t, s, creator.ID, ch)

	if _, err := api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hash}); err != nil {
		t.Fatalf("first import: %v", err)
	}
	first, _, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}

	// A post between the two joins moves the channel's pts, so a second join that
	// rewrote join_pts would seat the joiner above history they already hold.
	if _, _, _, err = s.PostChannelMessageAs(ctx, ch.ID, creator.ID, "hello", 7, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	if _, err = api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hash}); err != nil {
		t.Fatalf("second import: %v", err)
	}
	second, _, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil {
		t.Fatalf("member of after: %v", err)
	}
	if second.JoinPts != first.JoinPts {
		t.Errorf("join_pts moved on re-join: %d then %d", first.JoinPts, second.JoinPts)
	}

	members, err := s.ChannelMembers(ctx, ch.ID)
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("participants: got %d, want 2", len(members))
	}
}

// --- updates.getChannelDifference tests ---

func TestGetChannelDifferenceThreePosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555140001", "+1555140002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=0): %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", diff.Pts)
	}
	if len(diff.NewMessages) != 4 {
		t.Fatalf("NewMessages = %d, want creation message plus 3 posts", len(diff.NewMessages))
	}
}

func TestGetChannelDifferencePartialPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555141001", "+1555141002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     3,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=3): %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", diff.Pts)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1", len(diff.NewMessages))
	}
}

func TestGetChannelDifferenceCaughtUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555142001", "+1555142002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     4,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=4): %v", err)
	}
	assertEncodes(t, enc)
	empty, ok := enc.(*tg.UpdatesChannelDifferenceEmpty)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifferenceEmpty", enc)
	}
	if !empty.Final {
		t.Fatal("Final = false, want true")
	}
	if empty.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", empty.Pts)
	}
}

func TestGetChannelDifferenceAheadOfServer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555143001", "+1555143002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     99,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=99): %v", err)
	}
	assertEncodes(t, enc)
	empty, ok := enc.(*tg.UpdatesChannelDifferenceEmpty)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifferenceEmpty", enc)
	}
	if empty.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", empty.Pts)
	}
}

func TestGetChannelDifferenceJoinPtsClamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, _, ch := channelWith(t, s, "+1555144001", "+1555144002")

	for i := range 2 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	lateUser, err := s.CreateUser(ctx, "+1555144003")
	if err != nil {
		t.Fatalf("late user: %v", err)
	}
	hash := exportInvite(t, s, creator.ID, ch)
	_, err = api.ImportChatInviteForTest(s, lateUser.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "after join", 3, nil, 0); err != nil {
		t.Fatalf("post after join: %v", err)
	}

	enc, err := api.GetChannelDifferenceForTest(s, lateUser.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(lateUser.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=0): %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1 (clamped to join_pts=2)", len(diff.NewMessages))
	}
	if diff.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", diff.Pts)
	}
	_ = dsn
}

func TestGetChannelDifferenceNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555145001", "+1555145002")

	if _, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	outsider, err := s.CreateUser(ctx, "+1555145003")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	_, err = api.GetChannelDifferenceForTest(s, outsider.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(outsider.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err == nil {
		t.Fatal("expected error for non-member, got nil")
	}
}

func TestGetChannelDifferenceBanned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, other, ch := channelWith(t, s, "+1555146001", "+1555146002")

	joinChannel(t, ctx, dsn, ch.ID, other.ID)

	if _, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	banUntil := time.Now().Add(time.Hour)
	banChannelMember(t, ctx, dsn, ch.ID, other.ID, banUntil)

	_, err := api.GetChannelDifferenceForTest(s, other.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(other.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err == nil {
		t.Fatal("expected error for banned member, got nil")
	}
}

func TestGetChannelDifferenceTruncated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555147001", "+1555147002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	for i := range 5 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil, 0)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(limit=2): %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if diff.Final {
		t.Fatal("Final = true, want false (truncated)")
	}
	if len(diff.NewMessages) != 2 {
		t.Fatalf("NewMessages = %d, want 2", len(diff.NewMessages))
	}
	if diff.Pts != 2 {
		t.Fatalf("Pts = %d, want 2 (last included event's pts)", diff.Pts)
	}
}

// inputUser names a target for editAdmin. Uses the test deriver with callerID
// as viewer.
func inputUser(callerID, id int64) tg.InputUserClass {
	return api.InputUser(callerID, id)
}

// promote runs channels.editAdmin as callerID with a single admin right set,
// which the coarse role collapses to role 1.
func promote(t *testing.T, s *store.Store, callerID int64, ch store.Channel, targetID int64) (bin.Encoder, error) {
	t.Helper()
	return api.EditAdminForTest(s, callerID, &tg.ChannelsEditAdminRequest{
		Channel:     api.InputChannel(callerID, ch.ID),
		UserID:      inputUser(callerID, targetID),
		AdminRights: tg.ChatAdminRights{BanUsers: true},
		Rank:        "boss",
	})
}

// banForever runs channels.editBanned as callerID with the permanent form: view
// messages revoked and no until date.
func banForever(t *testing.T, s *store.Store, callerID int64, ch store.Channel, targetID int64) (bin.Encoder, error) {
	t.Helper()
	return api.EditBannedForTest(s, callerID, &tg.ChannelsEditBannedRequest{
		Channel:      api.InputChannel(callerID, ch.ID),
		Participant:  api.InputPeerUser(callerID, targetID),
		BannedRights: tg.ChatBannedRights{ViewMessages: true},
	})
}

func channelRole(t *testing.T, s *store.Store, channelID, userID int64) int {
	t.Helper()
	m, found, err := s.ChannelMemberOf(context.Background(), channelID, userID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !found {
		t.Fatalf("user %d has no participant row in channel %d", userID, channelID)
	}
	return m.Role
}

// The whole promotion chain: the creator makes a member an admin, and that new
// admin can then reach a role-0 member with a ban. A single admin right is set
// on the request, which is what pins the coarse collapse — BanUsers alone
// produces a full admin.
func TestEditAdminPromotesAndThePromotedAdminCanBan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298003")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)
	joinChannelByInvite(t, s, ch, member.ID)

	res, err := promote(t, s, creator.ID, ch, admin.ID)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	assertEncodes(t, res)
	if got := channelRole(t, s, ch.ID, admin.ID); got != 1 {
		t.Fatalf("role after promote = %d, want 1", got)
	}

	res, err = banForever(t, s, admin.ID, ch, member.ID)
	if err != nil {
		t.Fatalf("promoted admin ban: %v", err)
	}
	assertEncodes(t, res)
	banned, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if !banned.Banned(time.Now()) || !banned.Forever() {
		t.Errorf("ban is not permanent: banned=%v forever=%v", banned.Banned(time.Now()), banned.Forever())
	}
}

// Only the creator grants rights. A plain member and an admin both get the one
// error, and neither leaves a role behind.
func TestEditAdminRejectsEveryoneButTheCreator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298011")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298012")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298013")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = promote(t, s, creator.ID, ch, admin.ID); err != nil {
		t.Fatalf("seed promote: %v", err)
	}

	// A role-0 member may promote nobody, not even themselves through another.
	if _, err = promote(t, s, member.ID, ch, admin.ID); err == nil {
		t.Fatal("member promote: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("member promote: got %s, want PEER_ID_INVALID", msg)
	}
	// An admin may ban, but may not grant rights to anyone.
	if _, err = promote(t, s, admin.ID, ch, member.ID); err == nil {
		t.Fatal("admin promote: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("admin promote: got %s, want PEER_ID_INVALID", msg)
	}
	if got := channelRole(t, s, ch.ID, member.ID); got != 0 {
		t.Errorf("member role = %d, want 0 — a rejected editAdmin wrote a role", got)
	}
	// The creator is not a target either, whoever asks.
	if _, err = promote(t, s, admin.ID, ch, creator.ID); err == nil {
		t.Fatal("promote creator: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("promote creator: got %s, want PEER_ID_INVALID", msg)
	}
}

// A retried promotion must not report failure. SetChannelRole rejects a
// transition to the role the target already holds with the same ErrNotMember a
// rights rejection returns, and MTProto clients retry.
func TestEditAdminRetryIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298021")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298022")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)

	if _, err = promote(t, s, creator.ID, ch, admin.ID); err != nil {
		t.Fatalf("promote: %v", err)
	}
	res, err := promote(t, s, creator.ID, ch, admin.ID)
	if err != nil {
		t.Fatalf("retried promote: %v", err)
	}
	assertEncodes(t, res)
	if got := channelRole(t, s, ch.ID, admin.ID); got != 1 {
		t.Errorf("role after retry = %d, want 1", got)
	}

	// The demotion back to role 0 is a real transition and still works.
	if _, err = api.EditAdminForTest(s, creator.ID, &tg.ChannelsEditAdminRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		UserID:  inputUser(creator.ID, admin.ID),
	}); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if got := channelRole(t, s, ch.ID, admin.ID); got != 0 {
		t.Errorf("role after demote = %d, want 0", got)
	}
}

// A permanent ban revokes reads, and a zero rights struct gives them back.
func TestEditBannedBansPermanentlyAndUnbans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298031")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298032")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "before the ban", 8100); err != nil {
		t.Fatalf("seed post: %v", err)
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)}); err != nil {
		t.Fatalf("history before ban: %v", err)
	}

	res, err := banForever(t, s, creator.ID, ch, member.ID)
	if err != nil {
		t.Fatalf("ban: %v", err)
	}
	assertEncodes(t, res)
	// The caller's own view of the channel is unaffected by banning someone else.
	if chats := updatesOf(t, res).Chats; len(chats) != 1 {
		t.Fatalf("ban reply: %d chats, want 1", len(chats))
	} else if _, ok := chats[0].(*tg.Channel); !ok {
		t.Fatalf("ban reply: got %T, want *tg.Channel for the caller", chats[0])
	}

	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)}); err == nil {
		t.Fatal("history after ban: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("history after ban: got %s, want PEER_ID_INVALID", msg)
	}

	// The zero rights struct is the unban.
	if _, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     api.InputChannel(creator.ID, ch.ID),
		Participant: api.InputPeerUser(creator.ID, member.ID),
	}); err != nil {
		t.Fatalf("unban: %v", err)
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)}); err != nil {
		t.Fatalf("history after unban: %v", err)
	}
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if m.Banned(time.Now()) {
		t.Error("member is still banned after the unban")
	}
}

// An until_date that has already passed is a client mistake, not a ban: writing
// it would report a ban that ChannelMember.Banned already reads as lapsed.
func TestEditBannedRejectsAPastUntilDate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298041")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298042")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     api.InputChannel(creator.ID, ch.ID),
		Participant: api.InputPeerUser(creator.ID, member.ID),
		BannedRights: tg.ChatBannedRights{
			ViewMessages: true,
			UntilDate:    int(time.Now().Add(-time.Hour).Unix()),
		},
	})
	if err == nil {
		t.Fatal("past until_date: expected UNTIL_DATE_INVALID, got nil")
	}
	if msg := rpcMessage(t, err); msg != "UNTIL_DATE_INVALID" {
		t.Errorf("past until_date: got %s, want UNTIL_DATE_INVALID", msg)
	}
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if m.BannedUntil != nil {
		t.Error("a rejected until_date still wrote banned_until")
	}
}

// A target with no participant row is rejected and no row is created: neither
// RPC is a push primitive re-entering through the side door.
func TestChannelEditsRejectATargetWithNoRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, outsider, ch := channelWith(t, s, "+15551298051", "+15551298052")

	if _, err := promote(t, s, creator.ID, ch, outsider.ID); err == nil {
		t.Fatal("editAdmin on a non-participant: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editAdmin on a non-participant: got %s, want PEER_ID_INVALID", msg)
	}
	if _, err := banForever(t, s, creator.ID, ch, outsider.ID); err == nil {
		t.Fatal("editBanned on a non-participant: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editBanned on a non-participant: got %s, want PEER_ID_INVALID", msg)
	}

	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, outsider.ID); err != nil {
		t.Fatalf("member of: %v", err)
	} else if found {
		t.Error("a rejected edit created a participant row")
	}
}

// The enumeration oracle the threat model closes: a stranger naming a channel
// that does not exist and a member with insufficient rights must get the
// byte-identical error, on both RPCs. Anything else confirms that a channel id
// is live, or what a target's role is.
func TestChannelEditFailuresAreIndistinguishable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, stranger, ch := channelWith(t, s, "+15551298061", "+15551298062")
	member, err := s.CreateUser(ctx, "+15551298063")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551298064")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	joinChannelByInvite(t, s, ch, other.ID)

	// An id past every channel this test created, so the row genuinely is absent.
	absent := store.Channel{ID: ch.ID + 1_000_000, CreatorID: creator.ID}

	_, adminNoChannel := promote(t, s, stranger.ID, absent, other.ID)
	_, adminNoRights := promote(t, s, member.ID, ch, other.ID)
	_, banNoChannel := banForever(t, s, stranger.ID, absent, other.ID)
	_, banNoRights := banForever(t, s, member.ID, ch, other.ID)

	for name, err := range map[string]error{
		"editAdmin unknown channel":  adminNoChannel,
		"editAdmin insufficient":     adminNoRights,
		"editBanned unknown channel": banNoChannel,
		"editBanned insufficient":    banNoRights,
	} {
		if err == nil {
			t.Fatalf("%s: got nil error", name)
		}
		if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s: got %s, want PEER_ID_INVALID", name, msg)
		}
	}
}

// Neither RPC serves an unauthenticated connection, and neither accepts a
// participant that is not a user.
func TestChannelEditsRejectBadInput(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+15551298071", "+15551298072")

	if _, err := api.EditAdminForTest(s, 0, &tg.ChannelsEditAdminRequest{
		Channel: api.InputChannel(0, ch.ID), UserID: inputUser(0, creator.ID),
	}); err == nil {
		t.Fatal("editAdmin unauthenticated: got nil")
	} else if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("editAdmin unauthenticated: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
	if _, err := api.EditBannedForTest(s, 0, &tg.ChannelsEditBannedRequest{
		Channel:     api.InputChannel(0, ch.ID),
		Participant: api.InputPeerUser(0, creator.ID),
	}); err == nil {
		t.Fatal("editBanned unauthenticated: got nil")
	} else if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("editBanned unauthenticated: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
	// A chat peer names no channel participant row.
	if _, err := api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     api.InputChannel(creator.ID, ch.ID),
		Participant: &tg.InputPeerChat{ChatID: 1},
	}); err == nil {
		t.Fatal("editBanned on a chat peer: got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editBanned on a chat peer: got %s, want PEER_ID_INVALID", msg)
	}
}

// A rights struct that revokes something other than view_messages has nothing
// M7 can store, and it must not fall through to the unban path: a caller
// tightening a restriction on a banned member would otherwise clear the ban and
// be told it worked.
func TestEditBannedRejectsAPartialRestriction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298081")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298082")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = banForever(t, s, creator.ID, ch, member.ID); err != nil {
		t.Fatalf("seed ban: %v", err)
	}

	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:      api.InputChannel(creator.ID, ch.ID),
		Participant:  api.InputPeerUser(creator.ID, member.ID),
		BannedRights: tg.ChatBannedRights{SendMessages: true},
	})
	if err == nil {
		t.Fatal("partial restriction: expected BANNED_RIGHTS_INVALID, got nil")
	}
	if msg := rpcMessage(t, err); msg != "BANNED_RIGHTS_INVALID" {
		t.Errorf("partial restriction: got %s, want BANNED_RIGHTS_INVALID", msg)
	}

	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if !m.Banned(time.Now()) || !m.Forever() {
		t.Fatalf("ban was cleared by a rejected edit: banned=%v forever=%v", m.Banned(time.Now()), m.Forever())
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(member.ID, ch.ID)}); err == nil {
		t.Fatal("history after a rejected edit: the ban no longer revokes reads")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("history after a rejected edit: got %s, want PEER_ID_INVALID", msg)
	}

	// The rejection is decided on the rights struct alone, before any channel or
	// membership lookup — which keeps it off the post-read error collapse.
	// Non-existent channel with derived hash proves the check never reaches the store.
	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:      api.InputChannel(creator.ID, ch.ID+1_000_000),
		Participant:  api.InputPeerUser(creator.ID, member.ID),
		BannedRights: tg.ChatBannedRights{SendMessages: true},
	})
	if err == nil {
		t.Fatal("partial restriction: got nil")
	} else if msg := rpcMessage(t, err); msg != "BANNED_RIGHTS_INVALID" {
		t.Errorf("partial restriction: got %s, want BANNED_RIGHTS_INVALID", msg)
	}
}

func TestGetChannelDifferenceSkippedEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, _, ch := channelWith(t, s, "+1555148001", "+1555148002")

	hash := exportInvite(t, s, creator.ID, ch)
	_, err := api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	// Insert a channel event with type 2 (edit) that references a non-existent
	// message row, to exercise the skipped event path in channelEventToUpdate.
	channelExec(t, ctx, dsn,
		`INSERT INTO channel_events (channel_id, pts, "type", local_id) VALUES ($1, $2, 2, 99999)`,
		ch.ID, 3)
	channelExec(t, ctx, dsn,
		`UPDATE channel_state SET pts = $2 WHERE channel_id = $1`,
		ch.ID, 3)

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg2", 3, nil, 0); err != nil {
		t.Fatalf("post2: %v", err)
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference: %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 4 {
		t.Fatalf("Pts = %d, want 4", diff.Pts)
	}
	// Skipped event (type 2) produces no update; only the two real messages appear.
	if len(diff.NewMessages) != 3 {
		t.Fatalf("NewMessages = %d, want creation plus 2 posts (skipped event type 2)", len(diff.NewMessages))
	}
}

// TestGetDialogsMultiChannel verifies that the batched channel dialog path
// renders a correct multi-channel response: 2 channels + 1 DM, channels
// prepended, each channel's TopMessage/pts correct, Messages and Chats
// populated, DM peer in Users (not Chats).
func TestGetDialogsMultiChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551297001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551297002")
	if err != nil {
		t.Fatalf("member: %v", err)
	}

	// Create two channels, join member, post in each.
	channels := make([]store.Channel, 2)
	for i := range channels {
		ch, cerr := s.CreateChannel(ctx, creator.ID, fmt.Sprintf("Ch%d", i), "", false)
		if cerr != nil {
			t.Fatalf("create channel %d: %v", i, cerr)
		}
		channels[i] = ch
		joinChannelByInvite(t, s, ch, member.ID)
		if _, cerr = sendToChannel(t, s, creator.ID, ch.ID, fmt.Sprintf("post%d", i), int64(i+1)); cerr != nil {
			t.Fatalf("post %d: %v", i, cerr)
		}
	}

	// Seed a 1:1 DM so the member has a non-channel dialog too.
	other, err := s.CreateUser(ctx, "+15551297003")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	if _, err = api.SendMessageForTest(s, other.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(other.ID, member.ID), Message: "hi", RandomID: 999,
	}); err != nil {
		t.Fatalf("dm: %v", err)
	}

	enc, err := api.GetDialogsForTest(s, member.ID)
	if err != nil {
		t.Fatalf("get dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesDialogs", enc)
	}

	// 3 dialogs: 2 channels (prepended) + 1 DM.
	if len(res.Dialogs) != 3 {
		t.Fatalf("dialogs = %d, want 3", len(res.Dialogs))
	}

	// First two are channels, prepended. The dialog list is ordered by channel
	// id, which channel ids being random draws no longer makes the same thing as
	// creation order — the expectation is sorted rather than assumed.
	slices.SortFunc(channels, func(a, b store.Channel) int { return cmp.Compare(a.ID, b.ID) })
	channelIDs := make(map[int64]bool)
	for i, ch := range channels {
		dlg, ok := res.Dialogs[i].(*tg.Dialog)
		if !ok {
			t.Fatalf("dialog[%d] = %T, want *tg.Dialog", i, res.Dialogs[i])
		}
		peer, ok := dlg.Peer.(*tg.PeerChannel)
		if !ok {
			t.Fatalf("dialog[%d].peer = %T, want *tg.PeerChannel", i, dlg.Peer)
		}
		if peer.ChannelID != ch.ID {
			t.Errorf("dialog[%d] channel id = %d, want %d", i, peer.ChannelID, ch.ID)
		}
		channelIDs[peer.ChannelID] = true
		// Each channel has its create service message and 1 post.
		if dlg.TopMessage != 2 {
			t.Errorf("dialog[%d] top_message = %d, want 2", i, dlg.TopMessage)
		}
		if pts, hasPts := dlg.GetPts(); !hasPts || pts != 2 {
			t.Errorf("dialog[%d] pts = %d present=%v, want 2", i, pts, hasPts)
		}
	}

	// Third dialog is the DM.
	dmDlg, ok := res.Dialogs[2].(*tg.Dialog)
	if !ok {
		t.Fatalf("dialog[2] = %T, want *tg.Dialog", res.Dialogs[2])
	}
	dmPeer, ok := dmDlg.Peer.(*tg.PeerUser)
	if !ok {
		t.Fatalf("dialog[2].peer = %T, want *tg.PeerUser", dmDlg.Peer)
	}
	if dmPeer.UserID != other.ID {
		t.Errorf("dialog[2] user id = %d, want %d", dmPeer.UserID, other.ID)
	}

	// Messages: 2 channel tops + 1 DM top = 3.
	if len(res.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(res.Messages))
	}
	// Collect channel-message peer IDs, reject duplicates, assert set equals
	// both expected channel IDs.
	msgChannelIDs := make(map[int64]bool)
	for _, m := range res.Messages {
		msg, ok := m.(*tg.Message)
		if !ok {
			continue
		}
		peer, ok := msg.PeerID.(*tg.PeerChannel)
		if !ok {
			continue
		}
		if msgChannelIDs[peer.ChannelID] {
			t.Errorf("duplicate channel top message for channel %d", peer.ChannelID)
		}
		msgChannelIDs[peer.ChannelID] = true
		if msg.ID != 2 {
			t.Errorf("channel %d message id = %d, want 2", peer.ChannelID, msg.ID)
		}
	}
	if len(msgChannelIDs) != len(channels) {
		t.Errorf("channel messages = %d, want %d", len(msgChannelIDs), len(channels))
	}
	for _, ch := range channels {
		if !msgChannelIDs[ch.ID] {
			t.Errorf("missing channel top message for channel %d", ch.ID)
		}
	}

	// Chats: 2 channels, DM peer must NOT appear.
	if len(res.Chats) != 2 {
		t.Fatalf("chats = %d, want 2", len(res.Chats))
	}
	for _, c := range res.Chats {
		ch, ok := c.(*tg.Channel)
		if !ok {
			t.Errorf("chat = %T, want *tg.Channel", c)
			continue
		}
		if !channelIDs[ch.ID] {
			t.Errorf("chat id %d not in channel set", ch.ID)
		}
	}

	// Users: DM peer must appear.
	gotUsers := make(map[int64]bool)
	for _, u := range res.Users {
		gotUsers[u.GetID()] = true
	}
	if !gotUsers[other.ID] {
		t.Error("users missing DM peer")
	}
}

// TestGetDialogsChannelCursorSafe verifies that channels are prepended (not
// appended) so the last dialog in the reply is always a dialogs row. This keeps
// offset_id valid across pages even when the page is truncated.
func TestGetDialogsChannelCursorSafe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("member: %v", err)
	}

	// Create a channel with a post.
	ch, err := s.CreateChannel(ctx, creator.ID, "CursorTest", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "post", 1); err != nil {
		t.Fatalf("post: %v", err)
	}

	// Create 21 DM dialogs so the first page (limit 20) is truncated.
	for i := range 21 {
		peer, perr := s.CreateUser(ctx, fmt.Sprintf("+15551298%03d", 100+i))
		if perr != nil {
			t.Fatalf("peer %d: %v", i, perr)
		}
		if _, perr = api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
			Peer: api.InputPeerUser(peer.ID, member.ID), Message: "hi", RandomID: int64(i + 1),
		}); perr != nil {
			t.Fatalf("dm %d: %v", i, perr)
		}
	}

	// First page (limit 20) is truncated — channels are prepended, so the LAST
	// dialog must be a dialogs row (not a channel) for cursor to stay valid.
	enc, err := api.GetDialogsPageForTest(s, member.ID, &tg.MessagesGetDialogsRequest{
		Limit: 20, OffsetPeer: &tg.InputPeerEmpty{},
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	slice, ok := enc.(*tg.MessagesDialogsSlice)
	if !ok {
		t.Fatalf("first page = %T, want *tg.MessagesDialogsSlice", enc)
	}
	// Last dialog must NOT be a channel — that's the cursor invariant.
	lastDialog, ok := slice.Dialogs[len(slice.Dialogs)-1].(*tg.Dialog)
	if !ok {
		t.Fatalf("last dialog type = %T", slice.Dialogs[len(slice.Dialogs)-1])
	}
	if _, isChan := lastDialog.Peer.(*tg.PeerChannel); isChan {
		t.Error("last dialog is a channel — cursor would break on pagination")
	}
	// First dialog should be the channel (prepended).
	firstDialog, ok := slice.Dialogs[0].(*tg.Dialog)
	if !ok {
		t.Fatalf("first dialog type = %T", slice.Dialogs[0])
	}
	if _, isChan := firstDialog.Peer.(*tg.PeerChannel); !isChan {
		t.Error("first dialog is not a channel — channels should be prepended")
	}

	// Second page: page from the last dialog's TopMessage (the cursor the client
	// would use), and verify the exact remaining dialogs with no overlap.
	enc, err = api.GetDialogsPageForTest(s, member.ID, &tg.MessagesGetDialogsRequest{
		Limit: 20, OffsetID: lastDialog.TopMessage, OffsetPeer: &tg.InputPeerEmpty{},
	})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	var page2Dialogs []tg.DialogClass
	switch p := enc.(type) {
	case *tg.MessagesDialogs:
		page2Dialogs = p.Dialogs
	case *tg.MessagesDialogsSlice:
		page2Dialogs = p.Dialogs
	default:
		t.Fatalf("second page = %T, want *tg.MessagesDialogs or *tg.MessagesDialogsSlice", enc)
	}
	// No channel on page 2.
	for _, d := range page2Dialogs {
		if dlg, isDialog := d.(*tg.Dialog); isDialog {
			if _, isChan := dlg.Peer.(*tg.PeerChannel); isChan {
				t.Error("channel appeared on non-first page")
			}
		}
	}
	// Page 2 must carry exactly the remaining dialogs (21 total - 20 on page 1 = 1).
	if len(page2Dialogs) != 1 {
		t.Fatalf("page 2 dialogs = %d, want 1", len(page2Dialogs))
	}
	// No overlap: page 2's top_message must be strictly less than the cursor.
	p2Dlg, ok := page2Dialogs[0].(*tg.Dialog)
	if !ok {
		t.Fatalf("page 2 dialog type = %T", page2Dialogs[0])
	}
	if p2Dlg.TopMessage >= lastDialog.TopMessage {
		t.Errorf("page 2 top_message = %d, want < %d (no overlap with page 1)", p2Dlg.TopMessage, lastDialog.TopMessage)
	}
}

func TestRevokeExportedChatInvite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551294001", "+15551294002")

	// Export two invites.
	hash1 := exportInvite(t, s, creator.ID, ch)
	hash2 := exportInvite(t, s, creator.ID, ch)

	// Revoke first invite.
	res, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, hash1)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	_, ok := res.(*tg.BoolTrue)
	if !ok {
		t.Fatalf("revoke: got %T, want *tg.BoolTrue", res)
	}

	// Second invite still works.
	res, err = api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hash2})
	if err != nil {
		t.Fatalf("import second: %v", err)
	}
	assertEncodes(t, res)

	// First invite now refused with same error as unknown hash.
	_, err = api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: hash1})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("import revoked: got %s, want PEER_ID_INVALID", msg)
	}
	_, err = api.ImportChatInviteForTest(s, creator.ID, &tg.MessagesImportChatInviteRequest{Hash: unknownHash})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("import unknown: got %s, want PEER_ID_INVALID", msg)
	}

	// Joiner still a member.
	member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || !found || member.Role != 0 {
		t.Fatalf("member: found=%v role=%d err=%v", found, member.Role, err)
	}
}

func TestRevokedAndUnknownInvitesReturnIdenticalErrors(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, stranger, ch := channelWith(t, s, "+15551294421", "+15551294422")
	revokedHash := exportInvite(t, s, creator.ID, ch)
	if _, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, revokedHash); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	_, checkRevokedErr := api.CheckChatInviteForTest(s, stranger.ID, &tg.MessagesCheckChatInviteRequest{Hash: revokedHash})
	_, checkUnknownErr := api.CheckChatInviteForTest(s, stranger.ID, &tg.MessagesCheckChatInviteRequest{Hash: unknownHash})
	_, importRevokedErr := api.ImportChatInviteForTest(s, stranger.ID, &tg.MessagesImportChatInviteRequest{Hash: revokedHash})
	_, importUnknownErr := api.ImportChatInviteForTest(s, stranger.ID, &tg.MessagesImportChatInviteRequest{Hash: unknownHash})
	checks := []struct {
		name string
		err  error
	}{
		{name: "check revoked", err: checkRevokedErr},
		{name: "check unknown", err: checkUnknownErr},
		{name: "import revoked", err: importRevokedErr},
		{name: "import unknown", err: importUnknownErr},
	}
	var want []byte
	for _, check := range checks {
		got := rpcErrorWireBytes(t, check.err)
		if want == nil {
			want = got
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s RPC error bytes = %x, want %x", check.name, got, want)
		}
	}
}

func rpcErrorWireBytes(t *testing.T, err error) []byte {
	t.Helper()
	rpcErr, ok := tgerr.As(err)
	if !ok {
		t.Fatalf("error = %v, want an RPC error", err)
	}
	if rpcErr.Code != 400 || rpcErr.Message != "PEER_ID_INVALID" {
		t.Fatalf("RPC error = %d %q, want 400 PEER_ID_INVALID", rpcErr.Code, rpcErr.Message)
	}
	var buf bin.Buffer
	if err := (&mt.RPCError{ErrorCode: rpcErr.Code, ErrorMessage: rpcErr.Message}).Encode(&buf); err != nil {
		t.Fatalf("encode RPC error: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestRevokeExportedChatInviteAcceptsOnlyConfiguredAndLegacyLinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551294401", "+15551294402")
	const prefixA = "https://links.example.test/"
	const prefixB = "https://community.example.test/"

	for _, tc := range []struct {
		name   string
		prefix string
		form   string
	}{
		{name: "configured prefix", prefix: prefixA, form: prefixA + "+"},
		{name: "other configured prefix", prefix: prefixB, form: prefixB + "+"},
		{name: "legacy prefix", prefix: prefixA, form: "https://t.me/+"},
		{name: "bare hash", prefix: prefixA, form: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exported, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{
				Peer: api.InputPeerChannel(creator.ID, ch.ID),
			}, tc.prefix)
			if err != nil {
				t.Fatalf("export: %v", err)
			}
			invite, ok := exported.(*tg.ChatInviteExported)
			if !ok {
				t.Fatalf("export response = %T, want *tg.ChatInviteExported", exported)
			}
			hash, ok := strings.CutPrefix(invite.Link, tc.prefix+"+")
			if !ok || !isInviteHash(hash) {
				t.Fatalf("exported link = %q, want configured prefix plus a valid hash", invite.Link)
			}
			revokeValue := hash
			if tc.form != "" {
				revokeValue = tc.form + hash
			}
			if _, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, revokeValue, tc.prefix); err != nil {
				t.Fatalf("revoke %s: %v", tc.name, err)
			}
			if _, err := api.CheckChatInviteForTest(s, joiner.ID, &tg.MessagesCheckChatInviteRequest{Hash: hash}, tc.prefix); rpcMessage(t, err) != "PEER_ID_INVALID" {
				t.Errorf("check revoked invite: got %v, want PEER_ID_INVALID", err)
			}
		})
	}

	exported, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(creator.ID, ch.ID),
	}, prefixA)
	if err != nil {
		t.Fatalf("export for foreign-origin revoke: %v", err)
	}
	invite, ok := exported.(*tg.ChatInviteExported)
	if !ok {
		t.Fatalf("export response = %T, want *tg.ChatInviteExported", exported)
	}
	hash := strings.TrimPrefix(invite.Link, prefixA+"+")
	if _, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, "https://evil.example/+"+hash, prefixA); err != nil {
		t.Fatalf("revoke foreign-origin link: %v", err)
	}
	if _, err := api.ImportChatInviteForTest(s, joiner.ID, &tg.MessagesImportChatInviteRequest{Hash: hash}, prefixB); err != nil {
		t.Fatalf("foreign-origin revoke retired invite: %v", err)
	}
	if member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID); err != nil || !found || member.Role != 0 {
		t.Fatalf("member after foreign-origin revoke: found=%v role=%d err=%v", found, member.Role, err)
	}
}

func TestRevokeFailureLogOmitsInviteCredential(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, _, ch := channelWith(t, s, "+15551294411", "+15551294412")
	exported, err := api.ExportChatInviteForTest(s, creator.ID, &tg.MessagesExportChatInviteRequest{
		Peer: api.InputPeerChannel(creator.ID, ch.ID),
	}, testPublicLinkPrefix)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	invite, ok := exported.(*tg.ChatInviteExported)
	if !ok {
		t.Fatalf("export response = %T, want *tg.ChatInviteExported", exported)
	}
	link := invite.Link
	hash := strings.TrimPrefix(link, testPublicLinkPrefix+"+")
	if !isInviteHash(hash) {
		t.Fatalf("exported link = %q, want a valid configured invite link", link)
	}
	channelExec(t, ctx, dsn, `
		CREATE FUNCTION fail_channel_invite_revoke() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'controlled revoke failure';
		END;
		$$ LANGUAGE plpgsql
	`)
	channelExec(t, ctx, dsn, `
		CREATE TRIGGER fail_channel_invite_revoke
		BEFORE UPDATE ON channel_invites
		FOR EACH STATEMENT EXECUTE FUNCTION fail_channel_invite_revoke()
	`)

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	if _, err := api.RevokeExportedChatInviteWithLoggerForTest(s, creator.ID, ch.ID, link, logger, testPublicLinkPrefix); err == nil {
		t.Fatal("revoke succeeded despite the injected database failure")
	}
	if !strings.Contains(logs.String(), "revoke exported chat invite") {
		t.Fatalf("captured logs = %q, want revoke failure log", logs.String())
	}
	if strings.Contains(logs.String(), hash) || strings.Contains(logs.String(), link) {
		t.Fatalf("revoke failure log contains invite credential: %q", logs.String())
	}
}

func TestRevokeExportedChatInviteRejectsNonAdmin(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	creator, _, ch := channelWith(t, s, "+15551294101", "+15551294102")

	// Add member as role 0.
	member, err := s.CreateUser(context.Background(), "+15551294103")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	hash := exportInvite(t, s, creator.ID, ch)
	_, err = api.ImportChatInviteForTest(s, member.ID, &tg.MessagesImportChatInviteRequest{Hash: hash})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	// Member cannot revoke.
	_, err = api.RevokeExportedChatInviteForTest(s, member.ID, ch.ID, hash)
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("member revoke: got %s, want PEER_ID_INVALID", msg)
	}
	banChannelMember(t, context.Background(), dsn, ch.ID, creator.ID, time.Now().Add(time.Hour))
	if _, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, hash); err == nil {
		t.Fatal("banned admin revoked an invite")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("banned admin revoke: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestRevokeExportedChatInviteIdempotent(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+15551294201", "+15551294202")

	hash := exportInvite(t, s, creator.ID, ch)

	// Revoke twice.
	res1, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, hash)
	if err != nil {
		t.Fatalf("first revoke: %v", err)
	}
	_, ok := res1.(*tg.BoolTrue)
	if !ok {
		t.Fatalf("first revoke: got %T, want *tg.BoolTrue", res1)
	}

	res2, err := api.RevokeExportedChatInviteForTest(s, creator.ID, ch.ID, hash)
	if err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	_, ok = res2.(*tg.BoolTrue)
	if !ok {
		t.Fatalf("second revoke: got %T, want *tg.BoolTrue", res2)
	}
}

func TestRevokeExportedChatInviteNonMemberRefused(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, stranger, ch := channelWith(t, s, "+15551294301", "+15551294302")

	hash := exportInvite(t, s, ch.CreatorID, ch)

	// Stranger cannot revoke.
	_, err := api.RevokeExportedChatInviteForTest(s, stranger.ID, ch.ID, hash)
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("stranger revoke: got %s, want PEER_ID_INVALID", msg)
	}
}

// --- channels.joinChannel tests ---

func TestJoinChannelPublicSucceeds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	_, joiner, ch := channelWith(t, s, "+15551295001", "+15551295002")

	// Claim a username for the channel.
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Join by username.
	res, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	assertEncodes(t, res)
	joinResult, ok := res.(*tg.MessagesChatInviteJoinResultOk)
	if !ok {
		t.Fatalf("join: got %T, want *tg.MessagesChatInviteJoinResultOk", res)
	}
	ups, ok := joinResult.Updates.(*tg.Updates)
	if !ok {
		t.Fatalf("join updates: got %T, want *tg.Updates", joinResult.Updates)
	}
	if len(ups.Chats) != 1 {
		t.Fatalf("chats: got %d, want 1", len(ups.Chats))
	}
	c, ok := ups.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("chat: got %T, want *tg.Channel", ups.Chats[0])
	}
	if c.ID != ch.ID {
		t.Errorf("channel id = %d, want %d", c.ID, ch.ID)
	}
	if c.Title != ch.Title {
		t.Errorf("title = %q, want %q", c.Title, ch.Title)
	}

	// Verify participant row was created.
	member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || !found {
		t.Fatalf("member: found=%v err=%v", found, err)
	}
	if member.Role != 0 {
		t.Errorf("role = %d, want 0", member.Role)
	}
}

func TestJoinChannelPrivateRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	_, joiner, ch := channelWith(t, s, "+15551295011", "+15551295012")
	// Channel has no username (private).

	_, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("private join: got %s, want PEER_ID_INVALID", msg)
	}

	// No participant row created.
	_, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || found {
		t.Fatalf("member row: found=%v err=%v", found, err)
	}
}

func TestJoinChannelUnknownChannelRefused(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	u, err := s.CreateUser(context.Background(), "+15551295021")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Non-existent channel.
	_, err = api.JoinChannelForTest(s, u.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(u.ID, 999999),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("unknown channel: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestJoinChannelIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551295031", "+15551295032")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// First join.
	if _, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("first join: %v", err)
	}
	first, _, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil {
		t.Fatalf("member: %v", err)
	}

	// Post between joins to advance pts.
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "hello", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	// Second join — must not change join_pts.
	if _, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("second join: %v", err)
	}
	second, _, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	if second.JoinPts != first.JoinPts {
		t.Errorf("join_pts changed: %d -> %d", first.JoinPts, second.JoinPts)
	}

	// Only one participant row.
	members, err := s.ChannelMembers(ctx, ch.ID)
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("participants = %d, want 2", len(members))
	}
}

func TestJoinChannelBannedRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	_, joiner, ch := channelWith(t, s, "+15551295041", "+15551295042")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Join first.
	if _, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("first join: %v", err)
	}

	// Ban the member.
	banChannelMember(t, ctx, dsn, ch.ID, joiner.ID, time.Now().Add(time.Hour))

	// Re-join while banned — must return the same error as a stranger.
	_, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("banned rejoin: got %s, want PEER_ID_INVALID", msg)
	}

	// Verify ban survived — not cleared by re-join attempt.
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || !found {
		t.Fatalf("member: found=%v err=%v", found, err)
	}
	if !m.Banned(time.Now()) {
		t.Error("ban cleared by re-join attempt")
	}
}

func TestJoinChannelWrongAccessHashRefused(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551295051", "+15551295052")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Use creator's access hash instead of joiner's.
	creatorHash := api.DeriveChannelHash(creator.ID, ch.ID)
	_, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: creatorHash},
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("wrong hash: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestJoinChannelSetsJoinPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551295061", "+15551295062")

	err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed")
	if err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Post before join.
	for i := range 3 {
		if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("pre %d", i), int64(i+1), nil, 0); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// Join.
	if _, err = api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || !found {
		t.Fatalf("member: found=%v err=%v", found, err)
	}
	if member.JoinPts != 4 {
		t.Errorf("join_pts = %d, want 4 (creation plus 3 posts before join)", member.JoinPts)
	}

	// Post after join.
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "post join", 10, nil, 0); err != nil {
		t.Fatalf("post after join: %v", err)
	}

	// getChannelDifference from join_pts should return only the post-after-join.
	enc, err := api.GetChannelDifferenceForTest(s, joiner.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     member.JoinPts,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference: %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("diff type: %T", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1", len(diff.NewMessages))
	}
}

func TestJoinChannelAtPerChannelCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	_, joiner, ch := channelWith(t, s, "+15551295071", "+15551295072")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Fill the channel to the default cap (10000) minus 1 with raw SQL.
	// Creator already has one slot, so we create 9998 fake users and fill
	// their seats, then joiner fills the last one.
	channelExec(t, ctx, dsn, `
		WITH filler_users AS (
			INSERT INTO users (phone)
			SELECT 'filler' || g FROM generate_series(1, 9998) g
			RETURNING id
		)
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		SELECT $1, id, 0, 0 FROM filler_users`, ch.ID)

	// joiner should now take the last available seat (total 10000 = 1 creator + 9998 filled + 1 joiner).
	if _, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("last join: %v", err)
	}

	// A new user — should be refused.
	overflow, err := s.CreateUser(ctx, "+15551295074")
	if err != nil {
		t.Fatalf("overflow: %v", err)
	}
	_, err = api.JoinChannelForTest(s, overflow.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(overflow.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "USERS_TOO_MUCH" {
		t.Fatalf("overflow: got %s, want USERS_TOO_MUCH", msg)
	}

	// Verify no row was created for the overflow user.
	_, found, err := s.ChannelMemberOf(ctx, ch.ID, overflow.ID)
	if err != nil || found {
		t.Fatalf("overflow member: found=%v err=%v", found, err)
	}
}

func TestJoinChannelAtPerUserCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, joiner, ch := channelWith(t, s, "+15551295081", "+15551295082")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Fill joiner's per-user cap (500) by joining 500 other channels through raw SQL,
	// so that the next join attempt is the 501st and fails.
	channelExec(t, ctx, dsn, `
		WITH filler_channels AS (
			INSERT INTO channels (id, title, creator_id, megagroup)
			SELECT g, 'filler ' || g, $1, false
			FROM generate_series(1, 500) g
			RETURNING id
		),
		filler_state AS (
			INSERT INTO channel_state (channel_id)
			SELECT id FROM filler_channels
		),
		filler_username AS (
			INSERT INTO usernames (handle, owner_type, owner_id)
			SELECT 'filler' || id, 'channel', id FROM filler_channels
		)
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		SELECT fc.id, $2, 0, 0 FROM filler_channels fc`, creator.ID, joiner.ID)

	// Join the target channel — should be refused (joiner is already in 500 channels).
	_, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "CHANNELS_TOO_MUCH" {
		t.Fatalf("per-user cap: got %s, want CHANNELS_TOO_MUCH", msg)
	}
}

func TestJoinChannelAppearsInGetChannels(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, joiner, ch := channelWith(t, s, "+15551295091", "+15551295092")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Before join: stranger sees ChannelForbidden.
	res, err := api.GetChannelsForTest(s, joiner.ID, &tg.ChannelsGetChannelsRequest{
		ID: inputChannels(joiner.ID, ch.ID),
	})
	if err != nil {
		t.Fatalf("get channels before: %v", err)
	}
	assertEncodes(t, res)
	chats, ok := res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("before: got %T, want *tg.MessagesChats", res)
	}
	if _, ok := chats.Chats[0].(*tg.ChannelForbidden); !ok {
		t.Fatalf("before: got %T, want *tg.ChannelForbidden", chats.Chats[0])
	}

	// Join.
	if _, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// After join: member sees live channel.
	res, err = api.GetChannelsForTest(s, joiner.ID, &tg.ChannelsGetChannelsRequest{
		ID: inputChannels(joiner.ID, ch.ID),
	})
	if err != nil {
		t.Fatalf("get channels after: %v", err)
	}
	assertEncodes(t, res)
	chats, ok = res.(*tg.MessagesChats)
	if !ok {
		t.Fatalf("after: got %T, want *tg.MessagesChats", res)
	}
	c, ok := chats.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("after: got %T, want *tg.Channel", chats.Chats[0])
	}
	if c.Title != ch.Title {
		t.Errorf("title = %q, want %q", c.Title, ch.Title)
	}
}

func TestJoinChannelRejectsUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	_, err := api.JoinChannelForTest(s, 0, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(0, 1),
	})
	if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unauthenticated: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
}

func TestJoinChannelRaceWithUsernameClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	_, joiner, ch := channelWith(t, s, "+15551295101", "+15551295102")

	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Clear the username (make private).
	channelExec(t, ctx, dsn, `UPDATE channels SET username = NULL WHERE id = $1`, ch.ID)
	channelExec(t, ctx, dsn, `DELETE FROM usernames WHERE owner_id = $1 AND owner_type = 'channel'`, ch.ID)

	// Attempt join after the username is cleared — should fail with uniform error.
	_, err := api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("clear then join: got %s, want PEER_ID_INVALID", msg)
	}
}

func TestJoinChannelDeliversNewPosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, joiner, ch := channelWith(t, s, "+15551295111", "+15551295112")

	err := api.ClaimChannelUsernameForTest(s, ch.ID, "newsfeed")
	if err != nil {
		t.Fatalf("claim username: %v", err)
	}

	// Join.
	if _, err = api.JoinChannelForTest(s, joiner.ID, &tg.ChannelsJoinChannelRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
	}); err != nil {
		t.Fatalf("join: %v", err)
	}

	// Post after join.
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "after join", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}

	// getChannelDifference should return the new post.
	member, found, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID)
	if err != nil || !found {
		t.Fatalf("member: found=%v err=%v", found, err)
	}
	enc, err := api.GetChannelDifferenceForTest(s, joiner.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(joiner.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     member.JoinPts,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference: %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("diff type: %T", enc)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1", len(diff.NewMessages))
	}
	msg, ok := diff.NewMessages[0].(*tg.Message)
	if !ok {
		t.Fatalf("message type: %T", diff.NewMessages[0])
	}
	if msg.Message != "after join" {
		t.Errorf("message = %q, want %q", msg.Message, "after join")
	}
}

func toggleSlowModeViaDispatcher(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	channel tg.InputChannelClass,
	seconds int,
) (*tg.Updates, *mt.RPCError) {
	t.Helper()
	method := settingsHandler{
		name: "channels.toggleSlowMode",
		request: func() bin.Encoder {
			return &tg.ChannelsToggleSlowModeRequest{Channel: channel, Seconds: seconds}
		},
	}
	body := dispatchSettings(t, h, method, userID, false)
	var updates tg.Updates
	if err := updates.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &updates, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode toggleSlowMode response: %v", err)
	}
	return nil, &rpc
}

func requireSlowModeChannelUpdate(t *testing.T, updates *tg.Updates, channelID int64) {
	t.Helper()
	if updates == nil {
		t.Fatal("toggleSlowMode returned nil Updates")
	}
	count := 0
	for _, update := range updates.Updates {
		if channel, ok := update.(*tg.UpdateChannel); ok && channel.ChannelID == channelID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("toggleSlowMode channel updates = %d, want one for %d", count, channelID)
	}
}

func TestHandleToggleSlowModeRoundTripsMemberInterval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295201")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551295202")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551295204")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551295203")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
		Megagroup: true,
		Title:     "Slow mode settings",
	})
	joinChannel(t, ctx, dsn, channel.ID, member.ID)
	joinChannel(t, ctx, dsn, channel.ID, admin.ID)
	if err := s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	if err := api.ClaimChannelUsernameForTest(s, channel.ID, "slowmodesettings"); err != nil {
		t.Fatalf("claim public username: %v", err)
	}
	h := fullChannelDispatcher(s)

	updates, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 10)
	if rpc != nil {
		t.Fatalf("set slow mode: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	requireSlowModeChannelUpdate(t, updates, channel.ID)
	response, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("member getFullChannel after save: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds(); !ok || seconds != 10 {
		t.Fatalf("member slowmode_seconds = %d present=%v, want 10", seconds, ok)
	}
	if _, rpc := toggleSlowModeViaDispatcher(t, h, admin.ID, api.InputChannel(admin.ID, channel.ID), 10); rpc == nil || rpc.ErrorMessage != "CHAT_NOT_MODIFIED" {
		t.Fatalf("admin unchanged slow mode result = %v, want CHAT_NOT_MODIFIED", rpc)
	}
	for _, seconds := range []int{30, 60, 300, 900, 3600} {
		updates, rpc = toggleSlowModeViaDispatcher(t, h, admin.ID, api.InputChannel(admin.ID, channel.ID), seconds)
		if rpc != nil {
			t.Fatalf("admin set slow mode to %d: %d %s", seconds, rpc.ErrorCode, rpc.ErrorMessage)
		}
		requireSlowModeChannelUpdate(t, updates, channel.ID)
	}
	response, rpc = getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("member getFullChannel after supported intervals: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds(); !ok || seconds != 3600 {
		t.Fatalf("member slowmode_seconds after supported intervals = %d present=%v, want 3600", seconds, ok)
	}
	updates, rpc = toggleSlowModeViaDispatcher(t, h, admin.ID, api.InputChannel(admin.ID, channel.ID), 10)
	if rpc != nil {
		t.Fatalf("admin restore slow mode to 10: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	requireSlowModeChannelUpdate(t, updates, channel.ID)
	if _, _, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "first eligible post", 95201, nil, 0); err != nil || duplicate {
		t.Fatalf("first member post duplicate=%v err=%v", duplicate, err)
	}
	left, err := s.LeaveChannel(ctx, channel.ID, member.ID)
	if err != nil || !left {
		t.Fatalf("member leave = %v err=%v", left, err)
	}
	for _, seconds := range []int{30, 0, 10} {
		updates, rpc = toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), seconds)
		if rpc != nil {
			t.Fatalf("set slow mode to %d: %d %s", seconds, rpc.ErrorCode, rpc.ErrorMessage)
		}
		requireSlowModeChannelUpdate(t, updates, channel.ID)
	}
	if _, _, err := s.JoinChannelByUsername(ctx, channel.ID, member.ID); err != nil {
		t.Fatalf("member rejoin: %v", err)
	}
	_, _, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "after rejoin", 95202, nil, 0)
	waitErr, wait := errors.AsType[*store.SlowModeWaitError](err)
	if !wait || waitErr == nil || duplicate {
		t.Fatalf("member post after rejoin duplicate=%v err=%v, want slow-mode wait", duplicate, err)
	}

	preview, rpc := getFullChannelViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("public getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if seconds, ok := fullChannelInfo(t, preview).GetSlowmodeSeconds(); ok {
		t.Fatalf("public preview exposed slowmode_seconds %d", seconds)
	}

	updates, rpc = toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 10)
	if updates != nil || rpc == nil || rpc.ErrorMessage != "CHAT_NOT_MODIFIED" {
		t.Fatalf("unchanged slow mode result = updates %v, rpc %v; want CHAT_NOT_MODIFIED", updates, rpc)
	}

	updates, rpc = toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 0)
	if rpc != nil {
		t.Fatalf("disable slow mode: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	requireSlowModeChannelUpdate(t, updates, channel.ID)
	response, rpc = getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("member getFullChannel after disable: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds(); !ok || seconds != 0 {
		t.Fatalf("member slowmode_seconds after disable = %d present=%v, want 0", seconds, ok)
	}
}

func TestHandleToggleSlowModeRejectsUnauthorizedAndInvalidRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295211")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551295212")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	removed, err := s.CreateUser(ctx, "+15551295213")
	if err != nil {
		t.Fatalf("removed member: %v", err)
	}
	banned, err := s.CreateUser(ctx, "+15551295214")
	if err != nil {
		t.Fatalf("banned member: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551295215")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
		Megagroup: true,
		Title:     "Slow mode authorization",
	})
	joinChannel(t, ctx, dsn, channel.ID, member.ID)
	joinChannel(t, ctx, dsn, channel.ID, removed.ID)
	joinChannel(t, ctx, dsn, channel.ID, banned.ID)
	channelExec(t, ctx, dsn, `DELETE FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channel.ID, removed.ID)
	channelExec(t, ctx, dsn, `UPDATE channel_participants SET banned_until = now() + interval '1 hour' WHERE channel_id = $1 AND user_id = $2`, channel.ID, banned.ID)
	h := fullChannelDispatcher(s)
	updates, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 10)
	if rpc != nil {
		t.Fatalf("set initial slow mode: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	requireSlowModeChannelUpdate(t, updates, channel.ID)
	if _, rpc := getFullChannelViaDispatcher(t, h, banned.ID, false, api.InputChannel(banned.ID, channel.ID)); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("banned getFullChannel error = %v, want PEER_ID_INVALID", rpc)
	}

	wrongChannelHash := &tg.InputChannel{ChannelID: channel.ID, AccessHash: channel.ID}
	missingChannelID := channel.ID + 1
	cases := []struct {
		name    string
		userID  int64
		channel tg.InputChannelClass
		seconds int
	}{
		{name: "member", userID: member.ID, channel: api.InputChannel(member.ID, channel.ID), seconds: 10},
		{name: "removed member", userID: removed.ID, channel: api.InputChannel(removed.ID, channel.ID), seconds: 10},
		{name: "banned member", userID: banned.ID, channel: api.InputChannel(banned.ID, channel.ID), seconds: 10},
		{name: "nonmember", userID: outsider.ID, channel: api.InputChannel(outsider.ID, channel.ID), seconds: 10},
		{name: "wrong hash", userID: creator.ID, channel: wrongChannelHash, seconds: 10},
		{name: "absent channel", userID: outsider.ID, channel: api.InputChannel(outsider.ID, missingChannelID), seconds: 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, rpc := toggleSlowModeViaDispatcher(t, h, tc.userID, tc.channel, tc.seconds); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Fatalf("toggleSlowMode error = %v, want PEER_ID_INVALID", rpc)
			}
		})
	}

	broadcast := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
		Broadcast: true,
		Title:     "Broadcast slow mode",
	})
	if _, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, broadcast.ID), 0); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("broadcast toggleSlowMode error = %v, want PEER_ID_INVALID", rpc)
	}
	basicGroup, err := s.CreateChat(ctx, creator.ID, "Basic group slow mode", nil)
	if err != nil {
		t.Fatalf("create basic group: %v", err)
	}
	if _, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, basicGroup.ID), 0); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("basic-group toggleSlowMode error = %v, want PEER_ID_INVALID", rpc)
	}
	if _, found, err := s.ChatByID(ctx, basicGroup.ID); err != nil || !found {
		t.Fatalf("basic group after toggle = found %v err %v, want unchanged", found, err)
	}
	if _, found, err := s.ChannelByID(ctx, basicGroup.ID); err != nil || found {
		t.Fatalf("channel at basic-group id = found %v err %v, want no migration", found, err)
	}
	if _, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, &tg.InputChannel{}, 11); rpc == nil || rpc.ErrorMessage != "SECONDS_INVALID" {
		t.Fatalf("unsupported interval error = %v, want SECONDS_INVALID before peer validation", rpc)
	}
	response, rpc := getFullChannelViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("member getFullChannel after refused writes: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds(); !ok || seconds != 10 {
		t.Fatalf("slowmode_seconds after refused writes = %d present=%v, want 10", seconds, ok)
	}
}

func TestHandleToggleSlowModeSerializesAgainstAdminDemotion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStoreWithApplicationName(t, dsn, "slow_mode_save_test")
	demotionStore := openStoreWithApplicationName(t, dsn, "slow_mode_demotion_test")
	creator, err := s.CreateUser(ctx, "+15551295221")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551295222")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	h := fullChannelDispatcher(s)
	type toggleResult struct {
		updates *tg.Updates
		rpc     *mt.RPCError
	}
	for _, tc := range []struct {
		name      string
		saveFirst bool
	}{
		{name: "demotion first"},
		{name: "save first", saveFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
				Megagroup: true,
				Title:     "Slow mode " + tc.name,
			})
			joinChannel(t, ctx, dsn, channel.ID, admin.ID)
			if err := s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
				t.Fatalf("promote admin: %v", err)
			}
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() {
				if err := conn.Close(ctx); err != nil {
					t.Errorf("close conn: %v", err)
				}
			}()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatalf("begin channel mutation barrier: %v", err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }() //nolint:errcheck // best-effort cleanup
			if _, err := tx.Exec(ctx, `SELECT id FROM channels WHERE id = $1 FOR NO KEY UPDATE`, channel.ID); err != nil {
				t.Fatalf("hold channel mutation lock: %v", err)
			}
			var barrierPID int
			if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&barrierPID); err != nil {
				t.Fatalf("read channel barrier backend pid: %v", err)
			}

			saveDone := make(chan toggleResult, 1)
			demotionDone := make(chan error, 1)
			startSave := func() {
				go func() {
					updates, rpc := toggleSlowModeViaDispatcher(t, h, admin.ID, api.InputChannel(admin.ID, channel.ID), 30)
					saveDone <- toggleResult{updates: updates, rpc: rpc}
				}()
			}
			startDemotion := func() {
				go func() {
					demotionDone <- demotionStore.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 0)
				}()
			}
			if tc.saveFirst {
				startSave()
				savePID := waitForChannelLockWaiter(t, ctx, conn, "slow_mode_save_test", barrierPID, 0)
				startDemotion()
				waitForChannelLockWaiter(t, ctx, conn, "slow_mode_demotion_test", barrierPID, savePID)
			} else {
				startDemotion()
				demotionPID := waitForChannelLockWaiter(t, ctx, conn, "slow_mode_demotion_test", barrierPID, 0)
				startSave()
				waitForChannelLockWaiter(t, ctx, conn, "slow_mode_save_test", barrierPID, demotionPID)
			}
			select {
			case result := <-saveDone:
				t.Fatalf("Save completed before the channel lock was released: updates:%v rpc:%v", result.updates, result.rpc)
			default:
			}
			select {
			case err := <-demotionDone:
				t.Fatalf("demotion completed before the channel lock was released: %v", err)
			default:
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("release channel mutation barrier: %v", err)
			}
			var save toggleResult
			var demotionErr error
			select {
			case save = <-saveDone:
			case <-time.After(10 * time.Second):
				t.Fatal("toggleSlowMode did not finish after releasing channel lock")
			}
			select {
			case demotionErr = <-demotionDone:
			case <-time.After(10 * time.Second):
				t.Fatal("admin demotion did not finish after releasing channel lock")
			}
			if demotionErr != nil {
				t.Fatalf("demote admin: %v", demotionErr)
			}
			if tc.saveFirst {
				if save.rpc != nil {
					t.Fatalf("save before demotion: %d %s", save.rpc.ErrorCode, save.rpc.ErrorMessage)
				}
				requireSlowModeChannelUpdate(t, save.updates, channel.ID)
			} else if save.updates != nil || save.rpc == nil || save.rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Fatalf("already-started save after demotion = updates:%v rpc:%v, want PEER_ID_INVALID", save.updates, save.rpc)
			}

			response, rpc := getFullChannelViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, channel.ID))
			if rpc != nil {
				t.Fatalf("creator getFullChannel after race: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
			}
			seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds()
			want := 0
			if save.updates != nil && save.rpc == nil {
				want = 30
			}
			if !ok || seconds != want {
				t.Fatalf("slowmode_seconds after demotion race = %d present=%v, want %d", seconds, ok, want)
			}
		})
	}
}

func openStoreWithApplicationName(t *testing.T, dsn, applicationName string) *store.Store {
	t.Helper()
	separator := "&"
	if !strings.Contains(dsn, "?") {
		separator = "?"
	}
	s, err := store.Open(context.Background(), dsn+separator+"application_name="+applicationName, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open store %q: %v", applicationName, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store %q: %v", applicationName, err)
		}
	})
	return s
}

func waitForChannelLockWaiter(t *testing.T, ctx context.Context, conn *pgx.Conn, applicationName string, blockerA, blockerB int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		if err := conn.QueryRow(ctx, `
			SELECT pid FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
			  AND wait_event_type = 'Lock'
			  AND ($2 = ANY(pg_blocking_pids(pid)) OR $3 = ANY(pg_blocking_pids(pid)))
			LIMIT 1
		`, applicationName, blockerA, blockerB).Scan(&pid); err == nil {
			return pid
		} else if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("inspect %s channel lock waiter: %v", applicationName, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	rows, err := conn.Query(ctx, `
		SELECT pid, application_name, coalesce(wait_event_type, ''), coalesce(wait_event, ''), coalesce(left(query, 200), '')
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
	`)
	if err != nil {
		t.Fatalf("%s did not wait on the channel mutation lock; inspect sessions: %v", applicationName, err)
	}
	defer rows.Close()
	var sessions []string
	for rows.Next() {
		var pid int
		var app, eventType, event, query string
		if err := rows.Scan(&pid, &app, &eventType, &event, &query); err != nil {
			t.Fatalf("%s did not wait on the channel mutation lock; scan sessions: %v", applicationName, err)
		}
		sessions = append(sessions, fmt.Sprintf("pid=%d app=%q %s/%s query=%q", pid, app, eventType, event, query))
	}
	t.Fatalf("%s did not wait on the channel mutation lock: %s", applicationName, strings.Join(sessions, "; "))
	return 0
}

func TestHandleToggleSlowModeSaveFailuresPreservePostsAndCooldown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		deferred bool
	}{
		{name: "update failure"},
		{name: "deferred commit failure", deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			creator, err := s.CreateUser(ctx, "+15551295231")
			if err != nil {
				t.Fatalf("creator: %v", err)
			}
			member, err := s.CreateUser(ctx, "+15551295232")
			if err != nil {
				t.Fatalf("member: %v", err)
			}
			channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{
				Megagroup: true,
				Title:     "Slow mode save failure",
			})
			joinChannel(t, ctx, dsn, channel.ID, member.ID)
			h := fullChannelDispatcher(s)
			updates, rpc := toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 10)
			if rpc != nil {
				t.Fatalf("set initial slow mode: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
			}
			requireSlowModeChannelUpdate(t, updates, channel.ID)

			const randomID = int64(95231)
			first, firstPts, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "committed before failed save", randomID, nil, 0)
			if err != nil || duplicate || firstPts != 2 {
				t.Fatalf("first member post = %+v pts=%d duplicate=%v err=%v", first, firstPts, duplicate, err)
			}
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect to inspect committed post marker: %v", err)
			}
			defer func() {
				if err := conn.Close(ctx); err != nil {
					t.Errorf("close marker connection: %v", err)
				}
			}()
			var markerBefore time.Time
			if err := conn.QueryRow(ctx, `SELECT last_post_at FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channel.ID, member.ID).Scan(&markerBefore); err != nil {
				t.Fatalf("read committed last-post marker: %v", err)
			}

			removeFailure := installSlowModeSaveFailure(t, dsn, channel.ID, tc.deferred)
			updates, rpc = toggleSlowModeViaDispatcher(t, h, creator.ID, api.InputChannel(creator.ID, channel.ID), 30)
			removeFailure()
			if updates != nil || rpc == nil || rpc.ErrorMessage != "INTERNAL" {
				t.Fatalf("failed slow-mode save = updates:%v rpc:%v, want INTERNAL without Updates", updates, rpc)
			}

			response, rpc := getFullChannelViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, channel.ID))
			if rpc != nil {
				t.Fatalf("creator getFullChannel after failed save: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
			}
			if seconds, ok := fullChannelInfo(t, response).GetSlowmodeSeconds(); !ok || seconds != 10 {
				t.Fatalf("slowmode_seconds after failed save = %d present=%v, want 10", seconds, ok)
			}
			var markerAfter time.Time
			if err := conn.QueryRow(ctx, `SELECT last_post_at FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channel.ID, member.ID).Scan(&markerAfter); err != nil {
				t.Fatalf("read last-post marker after failed save: %v", err)
			}
			if !markerAfter.Equal(markerBefore) {
				t.Fatalf("failed save changed committed last-post marker from %v to %v", markerBefore, markerAfter)
			}
			if _, _, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "distinct post during cooldown", randomID+1, nil, 0); err == nil || duplicate || !strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_") {
				t.Fatalf("distinct post after failed save duplicate=%v err=%v, want slow-mode cooldown", duplicate, err)
			}
			retry, retryPts, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "different retry payload", randomID, nil, 0)
			if err != nil || !duplicate || retry.LocalID != first.LocalID || retry.Message != first.Message || retryPts != firstPts {
				t.Fatalf("committed random-id retry = %+v pts=%d duplicate=%v err=%v, want original %+v pts=%d", retry, retryPts, duplicate, err, first, firstPts)
			}
			if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != firstPts {
				t.Fatalf("channel state after failed save and retry = %d err=%v, want %d", pts, err, firstPts)
			}
			events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, firstPts, 10)
			if err != nil || len(events) != 2 || events[1].Pts != firstPts || events[1].LocalID != first.LocalID || events[1].Type != store.EventNewMessage {
				t.Fatalf("events after failed save and retry = %+v err=%v, want creation and original post events", events, err)
			}
		})
	}
}

func installSlowModeSaveFailure(t *testing.T, dsn string, channelID int64, deferred bool) func() {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect failure injector: %v", err)
	}
	const triggerName = "test_slow_mode_save_failure"
	const functionName = "test_slow_mode_save_failure"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanupCtx, `DROP TRIGGER IF EXISTS test_slow_mode_save_failure ON channels`); err != nil {
			t.Errorf("drop slow-mode failure trigger: %v", err)
		}
		if _, err := conn.Exec(cleanupCtx, `DROP FUNCTION IF EXISTS test_slow_mode_save_failure()`); err != nil {
			t.Errorf("drop slow-mode failure function: %v", err)
		}
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close failure injector: %v", err)
		}
	})
	functionSQL := fmt.Sprintf(`
CREATE FUNCTION %s() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
  IF OLD.id = %d AND NEW.slowmode_seconds IS DISTINCT FROM OLD.slowmode_seconds THEN
    RAISE EXCEPTION 'injected slow-mode save failure';
  END IF;
  RETURN NEW;
END;
$body$;`, functionName, channelID)
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		t.Fatalf("create slow-mode failure function: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
CREATE TRIGGER %s
BEFORE UPDATE OF slowmode_seconds ON channels
FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, functionName)
	if deferred {
		triggerSQL = fmt.Sprintf(`
CREATE CONSTRAINT TRIGGER %s
AFTER UPDATE ON channels
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, functionName)
	}
	if _, err := conn.Exec(ctx, triggerSQL); err != nil {
		t.Fatalf("create slow-mode failure trigger: %v", err)
	}
	return func() {
		if _, err := conn.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_slow_mode_save_failure ON channels`); err != nil {
			t.Errorf("drop slow-mode failure trigger: %v", err)
		}
		if _, err := conn.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_slow_mode_save_failure()`); err != nil {
			t.Errorf("drop slow-mode failure function: %v", err)
		}
	}
}
