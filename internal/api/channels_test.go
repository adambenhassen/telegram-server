package api_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

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
