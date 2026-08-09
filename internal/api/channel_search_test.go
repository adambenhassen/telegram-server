package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// searchChannel runs messages.search against a channel peer for the caller.
func searchChannel(s *store.Store, userID, channelID int64, q string, offsetID, limit int) (bin.Encoder, error) {
	return api.SearchForTest(s, userID, &tg.MessagesSearchRequest{
		Peer:     channelPeer(userID, channelID),
		Q:        q,
		Filter:   &tg.InputMessagesFilterEmpty{},
		OffsetID: offsetID,
		Limit:    limit,
	})
}

// channelSearchIDs asserts the reply is the channel form and returns the post
// ids it carries, in order.
func channelSearchIDs(t *testing.T, enc bin.Encoder) []int {
	t.Helper()
	assertEncodes(t, enc)
	res, ok := enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", enc)
	}
	if res.Count != len(res.Messages) {
		t.Errorf("count = %d, want %d", res.Count, len(res.Messages))
	}
	ids := make([]int, len(res.Messages))
	for i, m := range res.Messages {
		ids[i] = m.GetID()
	}
	return ids
}

// deleteChannelPost marks one post deleted. Channel post deletion has no RPC
// yet, so the row is written directly, the way the ban tests write bans.
func deleteChannelPost(t *testing.T, ctx context.Context, dsn string, channelID int64, localID int64) {
	t.Helper()
	channelExec(t, ctx, dsn,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`,
		channelID, localID)
}

// A member searching a channel gets the matching posts in the channel's shared
// post-id space, newest-first, with the same offset_id paging and the same
// deleted-row exclusion channel getHistory has.
func TestSearchChannelReturnsMatchingPostsNewestFirst(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551297011")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551297012")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	for i, text := range []string{"budget approved", "unrelated text", "the budget again", "budget draft"} {
		if _, err = sendToChannel(t, s, creator.ID, ch.ID, text, int64(7100+i)); err != nil {
			t.Fatalf("post %q: %v", text, err)
		}
	}
	// Post 4 matches the query but is deleted, so it must never come back.
	deleteChannelPost(t, ctx, dsn, ch.ID, 4)

	enc, err := searchChannel(s, member.ID, ch.ID, "budget", 0, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 2 || ids[0] != 3 || ids[1] != 1 {
		t.Fatalf("ids = %v, want [3 1] (newest-first, deleted excluded)", ids)
	}
	res, ok := enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesChannelMessages", enc)
	}
	// The reply carries the channel's pts so a client can place the batch in
	// that channel's own update stream, exactly as the history path does.
	if res.Pts != 4 {
		t.Errorf("pts = %d, want 4", res.Pts)
	}
	if len(res.Chats) != 1 || res.Chats[0].GetID() != ch.ID {
		t.Errorf("chats = %+v, want the searched channel", res.Chats)
	}

	// offset_id pages strictly older, in the shared post-id space.
	enc, err = searchChannel(s, member.ID, ch.ID, "budget", 3, 0)
	if err != nil {
		t.Fatalf("search paged: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("paged ids = %v, want [1]", ids)
	}

	// limit bounds the page.
	enc, err = searchChannel(s, member.ID, ch.ID, "budget", 0, 1)
	if err != nil {
		t.Fatalf("search limited: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 1 || ids[0] != 3 {
		t.Fatalf("limited ids = %v, want [3]", ids)
	}

	// A word no post carries is an empty page, not an error.
	enc, err = searchChannel(s, member.ID, ch.ID, "zzzznothingmatchesthis", 0, 0)
	if err != nil {
		t.Fatalf("search miss: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 0 {
		t.Fatalf("miss ids = %v, want none", ids)
	}
}

// A member searches the channel's whole history, posts from before they joined
// included. join_pts bounds the difference path's replay only — a cost control,
// never a confidentiality one (threat model G5) — and search follows
// getHistory, which already serves those posts.
func TestSearchChannelServesPostsFromBeforeTheMemberJoined(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551297021")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	latecomer, err := s.CreateUser(ctx, "+15551297022")
	if err != nil {
		t.Fatalf("create latecomer: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "quarterly budget", 7201); err != nil {
		t.Fatalf("post one: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "unrelated", 7202); err != nil {
		t.Fatalf("post two: %v", err)
	}

	m := joinChannelByInvite(t, s, ch, latecomer.ID)
	if m.JoinPts != 2 {
		t.Fatalf("join_pts = %d, want 2 (the latecomer joined after both posts)", m.JoinPts)
	}

	enc, err := searchChannel(s, latecomer.ID, ch.ID, "budget", 0, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("ids = %v, want [1] (the pre-join post)", ids)
	}
}

// Every caller who may not read the channel gets the identical error search and
// getHistory already give for the same channel: banned, departed, never a
// member, and no such channel are one answer, so search does not become a way
// to tell them apart or to probe the dense channels.id space.
func TestSearchChannelRejectionsMatchGetHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551297031")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	banned, err := s.CreateUser(ctx, "+15551297032")
	if err != nil {
		t.Fatalf("create banned: %v", err)
	}
	leaver, err := s.CreateUser(ctx, "+15551297033")
	if err != nil {
		t.Fatalf("create leaver: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551297034")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "secret budget", 7301); err != nil {
		t.Fatalf("seed post: %v", err)
	}

	joinChannelByInvite(t, s, ch, banned.ID)
	banChannelMember(t, ctx, dsn, ch.ID, banned.ID, time.Now().Add(time.Hour))

	joinChannelByInvite(t, s, ch, leaver.ID)
	if _, err = api.LeaveChannelForTest(s, leaver.ID, &tg.ChannelsLeaveChannelRequest{
		Channel: api.InputChannel(leaver.ID, ch.ID),
	}); err != nil {
		t.Fatalf("leave channel: %v", err)
	}

	cases := []struct {
		name      string
		userID    int64
		channelID int64
	}{
		{"banned member", banned.ID, ch.ID},
		{"departed member", leaver.ID, ch.ID},
		{"never a member", outsider.ID, ch.ID},
		{"no such channel", outsider.ID, ch.ID + 9999},
	}
	for _, c := range cases {
		searchErr := func() error {
			_, err := searchChannel(s, c.userID, c.channelID, "budget", 0, 0)
			return err
		}()
		if searchErr == nil {
			t.Fatalf("%s: search returned nil, want a rejection", c.name)
		}
		_, historyErr := api.GetHistoryForTest(s, c.userID, &tg.MessagesGetHistoryRequest{
			Peer: channelPeer(c.userID, c.channelID),
		})
		if historyErr == nil {
			t.Fatalf("%s: getHistory returned nil, want a rejection", c.name)
		}
		if got, want := rpcMessage(t, searchErr), rpcMessage(t, historyErr); got != want {
			t.Errorf("%s: search error %s, getHistory error %s — they must be identical", c.name, got, want)
		}
		if got := rpcMessage(t, searchErr); got != "PEER_ID_INVALID" {
			t.Errorf("%s: search error = %s, want PEER_ID_INVALID", c.name, got)
		}
	}
}

// The per-viewer access_hash is checked before anything else, so a member
// naming their own channel with a hash they did not receive is refused the same
// way every other channel RPC refuses it.
func TestSearchChannelRejectsForgedAccessHash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551297041")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "budget approved", 7401); err != nil {
		t.Fatalf("seed post: %v", err)
	}

	for _, c := range []struct {
		name string
		hash int64
	}{
		{"missing hash", 0},
		{"another viewer's hash", api.DeriveChannelHash(creator.ID+1, ch.ID)},
	} {
		peer := &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: c.hash}
		_, searchErr := api.SearchForTest(s, creator.ID, &tg.MessagesSearchRequest{
			Peer:   peer,
			Q:      "budget",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		if searchErr == nil {
			t.Fatalf("%s: search returned nil, want a rejection", c.name)
		}
		_, historyErr := api.GetHistoryForTest(s, creator.ID, &tg.MessagesGetHistoryRequest{Peer: peer})
		if historyErr == nil {
			t.Fatalf("%s: getHistory returned nil, want a rejection", c.name)
		}
		if got, want := rpcMessage(t, searchErr), rpcMessage(t, historyErr); got != want {
			t.Errorf("%s: search error %s, getHistory error %s — they must be identical", c.name, got, want)
		}
		rpcError(t, searchErr, "PEER_ID_INVALID")
	}
}

// A ban landing between two pages stops the next one: membership is
// re-established on every call and is never carried in offset_id.
func TestSearchChannelRechecksMembershipBetweenPages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551297051")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551297052")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	for i, text := range []string{"budget one", "budget two"} {
		if _, err = sendToChannel(t, s, creator.ID, ch.ID, text, int64(7500+i)); err != nil {
			t.Fatalf("post %q: %v", text, err)
		}
	}

	enc, err := searchChannel(s, member.ID, ch.ID, "budget", 0, 1)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("first page ids = %v, want [2]", ids)
	}

	banChannelMember(t, ctx, dsn, ch.ID, member.ID, time.Now().Add(time.Hour))

	if _, err = searchChannel(s, member.ID, ch.ID, "budget", 2, 1); err == nil {
		t.Fatal("second page after ban: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}
}
