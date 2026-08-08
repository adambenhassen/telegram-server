package api_test

import (
	"context"
	"fmt"
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
	// not-found would make the dense BIGSERIAL id space enumerable.
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

// TestGetChannelsEnumerationClosed verifies that submitting channel ids with
// guessed hashes (M1 placeholder: access_hash == channel_id) returns the same
// PEER_ID_INVALID for both live and non-existent channels. This is the AC
// requirement: "Submitting 100 sequential channel ids with guessed hashes yields
// the same response for live channels and non-existent ones."
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
	_ = createChannel(t, s, owner.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "B"})

	// Build a batch of 100 sequential ids with M1 placeholder hashes.
	// Includes the live channels plus non-existent ones.
	ids := make([]tg.InputChannelClass, 100)
	baseID := ch1.ID
	for i := range 100 {
		id := baseID + int64(i)
		ids[i] = &tg.InputChannel{ChannelID: id, AccessHash: id}
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
	if m.JoinPts != 2 {
		t.Fatalf("join_pts = %d, want 2 (the latecomer joined after both posts)", m.JoinPts)
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
		Channel: api.InputChannel(creator.ID, ch.ID),
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

// unknownHash is a well-formed hash of the right shape that was never issued —
// 22 base64url characters, the width store.CreateChannelInvite emits.
const unknownHash = "0000000000000000000000"

// inviteHash pulls the hash back out of the exported link, which is the only
// form a client ever sees it in.
func inviteHash(t *testing.T, link string) string {
	t.Helper()
	hash, ok := strings.CutPrefix(link, "https://t.me/+")
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

func TestExportChatInviteRejectsUnauthorized(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, member, ch := channelWith(t, s, "+15551293011", "+15551293012")

	// A role-0 member: joined through the creator's invite, exactly as a real one
	// arrives.
	hash := exportInvite(t, s, creator.ID, ch)
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
// that does not exist, must all be the same wire error. Anything finer turns the
// dense channels.id space or the invite space into an existence oracle.
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
	if _, _, _, err = s.PostChannelMessageAs(ctx, ch.ID, creator.ID, "hello", 7, nil); err != nil {
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
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
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
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
	if len(diff.NewMessages) != 3 {
		t.Fatalf("NewMessages = %d, want 3", len(diff.NewMessages))
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
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	enc, err := api.GetChannelDifferenceForTest(s, creator.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     2,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=2): %v", err)
	}
	assertEncodes(t, enc)
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
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
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
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
	empty, ok := enc.(*tg.UpdatesChannelDifferenceEmpty)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifferenceEmpty", enc)
	}
	if !empty.Final {
		t.Fatal("Final = false, want true")
	}
	if empty.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", empty.Pts)
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
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
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
	if empty.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", empty.Pts)
	}
}

func TestGetChannelDifferenceJoinPtsClamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, _, ch := channelWith(t, s, "+1555144001", "+1555144002")

	for i := range 2 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
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

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "after join", 3, nil); err != nil {
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
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
	_ = dsn
}

func TestGetChannelDifferenceNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+1555145001", "+1555145002")

	if _, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil); err != nil {
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

	if _, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil); err != nil {
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
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
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

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil); err != nil {
		t.Fatalf("post: %v", err)
	}

	// Insert a channel event with type 2 (edit) that references a non-existent
	// message row, to exercise the skipped event path in channelEventToUpdate.
	channelExec(t, ctx, dsn,
		`INSERT INTO channel_events (channel_id, pts, "type", local_id) VALUES ($1, $2, 2, 99999)`,
		ch.ID, 2)
	channelExec(t, ctx, dsn,
		`UPDATE channel_state SET pts = $2 WHERE channel_id = $1`,
		ch.ID, 2)

	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg2", 3, nil); err != nil {
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
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
	// Skipped event (type 2) produces no update; only the two real messages appear.
	if len(diff.NewMessages) != 2 {
		t.Fatalf("NewMessages = %d, want 2 (skipped event type 2)", len(diff.NewMessages))
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

	// First two are channels, prepended.
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
		// Each channel has 1 post, so TopMessage == 1 and pts == 1.
		if dlg.TopMessage != 1 {
			t.Errorf("dialog[%d] top_message = %d, want 1", i, dlg.TopMessage)
		}
		if pts, hasPts := dlg.GetPts(); !hasPts || pts != 1 {
			t.Errorf("dialog[%d] pts = %d present=%v, want 1", i, pts, hasPts)
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
		if msg.ID != 1 {
			t.Errorf("channel %d message id = %d, want 1", peer.ChannelID, msg.ID)
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

func TestRevokeExportedChatInviteRejectsNonAdmin(t *testing.T) {
	t.Parallel()
	s := openStore(t)
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
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "hello", 1, nil); err != nil {
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
		if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("pre %d", i), int64(i+1), nil); err != nil {
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
	if member.JoinPts != 3 {
		t.Errorf("join_pts = %d, want 3 (3 posts before join)", member.JoinPts)
	}

	// Post after join.
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "post join", 10, nil); err != nil {
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
			INSERT INTO channels (title, creator_id, megagroup)
			SELECT 'filler ' || g, $1, false
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
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "after join", 1, nil); err != nil {
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
