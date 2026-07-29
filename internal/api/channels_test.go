package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
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

func inputChannels(ids ...int64) []tg.InputChannelClass {
	out := make([]tg.InputChannelClass, len(ids))
	for i, id := range ids {
		out[i] = &tg.InputChannel{ChannelID: id, AccessHash: id}
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
	if ch.AccessHash != ch.ID {
		t.Errorf("access hash = %d, want %d", ch.AccessHash, ch.ID)
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

	res, err := api.GetChannelsForTest(s, stranger.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ch.ID)})
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
	res, err = api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ch.ID)})
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
	_, err = api.GetChannelsForTest(s, u.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ids...)})
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
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
	after, err := api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ch.ID)})
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
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
			_, err := api.GetChannelsForTest(s, 0, &tg.ChannelsGetChannelsRequest{ID: inputChannels(1)})
			return err
		},
		"leaveChannel": func() error {
			_, err := api.LeaveChannelForTest(s, 0, &tg.ChannelsLeaveChannelRequest{
				Channel: &tg.InputChannel{ChannelID: 1, AccessHash: 1},
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
			INSERT INTO channels (title, creator_id)
			SELECT 'filler ' || g, $1 FROM generate_series(1, 500) g
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
	res, err := api.GetChannelsForTest(s, member.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ch.ID)})
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
	res, err = api.GetChannelsForTest(s, member.ID, &tg.ChannelsGetChannelsRequest{ID: inputChannels(ch.ID)})
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
	// not-found would make the dense BIGSERIAL id space enumerable.
	res, err := api.GetChannelsForTest(s, owner.ID, &tg.ChannelsGetChannelsRequest{
		ID: inputChannels(ch.ID, ch.ID+100000),
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
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

func channelPeer(id int64) tg.InputPeerClass {
	return &tg.InputPeerChannel{ChannelID: id, AccessHash: id}
}

func sendToChannel(t *testing.T, s *store.Store, userID, channelID int64, text string, randomID int64) (bin.Encoder, error) {
	t.Helper()
	return api.SendMessageForTest(s, userID, &tg.MessagesSendMessageRequest{
		Peer: channelPeer(channelID), Message: text, RandomID: randomID,
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
	if nm.Pts != 1 || nm.PtsCount != 1 {
		t.Errorf("pts = %d/%d, want 1/1", nm.Pts, nm.PtsCount)
	}
	msg, ok := nm.Message.(*tg.Message)
	if !ok {
		t.Fatalf("message = %T, want *tg.Message", nm.Message)
	}
	if msg.Message != "first" || msg.ID != 1 || !msg.Out {
		t.Errorf("message = %+v, want out post 1 %q", msg, "first")
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

	// Rejected posts write nothing, so the channel is still at pts 0.
	pts, err := s.ChannelState(ctx, ch.ID)
	if err != nil || pts != 0 {
		t.Fatalf("pts = %d err=%v, want 0", pts, err)
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
	if nm := newChannelMessage(t, res); nm.Pts != 1 {
		t.Errorf("pts = %d, want 1", nm.Pts)
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
	if err != nil || pts != 1 {
		t.Fatalf("pts = %d err=%v, want 1 (resend must not advance it)", pts, err)
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: 1}},
	})
	if err == nil {
		t.Fatal("getMessages: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getMessages: got %s, want PEER_ID_INVALID", msg)
	}

	_, err = api.GetHistoryForTest(s, outsider.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)})
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
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)}); err != nil {
		t.Fatalf("history before ban: %v", err)
	}

	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))

	_, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)})
	if err == nil {
		t.Fatal("getHistory: expected PEER_ID_INVALID for a banned member, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("getHistory: got %s, want PEER_ID_INVALID", msg)
	}

	_, err = api.GetChannelMessagesForTest(s, member.ID, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
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
	if m.JoinPts != 2 {
		t.Fatalf("join_pts = %d, want 2 (the latecomer joined after both posts)", m.JoinPts)
	}

	res, err := api.GetHistoryForTest(s, latecomer.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	assertEncodes(t, res)
	got, ok := res.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", res)
	}
	if got.Pts != 2 || got.Count != 2 || len(got.Messages) != 2 {
		t.Fatalf("pts=%d count=%d messages=%d, want 2/2/2", got.Pts, got.Count, len(got.Messages))
	}
	// Newest first.
	if got.Messages[0].GetID() != 2 || got.Messages[1].GetID() != 1 {
		t.Errorf("ids = %d,%d, want 2,1", got.Messages[0].GetID(), got.Messages[1].GetID())
	}
	first, isMsg := got.Messages[1].(*tg.Message)
	if !isMsg || first.Message != "one" || first.Out {
		t.Errorf("oldest = %+v, want inbound %q", got.Messages[1], "one")
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
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		// The third id has no row and must simply be absent from the reply.
		ID: []tg.InputMessageClass{&tg.InputMessageID{ID: 3}, &tg.InputMessageID{ID: 1}, &tg.InputMessageID{ID: 99}},
	})
	if err != nil {
		t.Fatalf("get channel messages: %v", err)
	}
	assertEncodes(t, res)
	got, ok := res.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", res)
	}
	if got.Pts != 3 || got.Count != 2 || len(got.Messages) != 2 {
		t.Fatalf("pts=%d count=%d messages=%d, want 3/2/2", got.Pts, got.Count, len(got.Messages))
	}
	if got.Messages[0].GetID() != 3 || got.Messages[1].GetID() != 1 {
		t.Errorf("ids = %d,%d, want the requested order 3,1", got.Messages[0].GetID(), got.Messages[1].GetID())
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
		Peer: &tg.InputPeerUser{UserID: other.ID, AccessHash: other.ID}, Message: "hi", RandomID: 910,
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
	// An empty channel has no top message and must not appear in the list.
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
	if d.TopMessage != 2 {
		t.Errorf("top message = %d, want 2", d.TopMessage)
	}
	if pts, hasPts := d.GetPts(); !hasPts || pts != 2 {
		t.Errorf("pts = %d present=%v, want 2", pts, hasPts)
	}
	if len(got.Messages) != 1 || got.Messages[0].GetID() != 2 {
		t.Errorf("messages = %+v, want the channel's post 2", got.Messages)
	}
	if len(got.Chats) != 1 {
		t.Fatalf("chats = %d, want the channel", len(got.Chats))
	}
	if _, isChannel := got.Chats[0].(*tg.Channel); !isChannel {
		t.Errorf("chat = %T, want *tg.Channel", got.Chats[0])
	}
}
