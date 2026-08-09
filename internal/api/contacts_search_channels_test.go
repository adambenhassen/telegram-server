package api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// searchChannels runs contacts.search for the caller and returns the response.
func searchChannels(t *testing.T, s *store.Store, callerID int64, q string, limit int) *tg.ContactsFound {
	t.Helper()
	res, err := api.ContactsSearchForTest(s, callerID, &tg.ContactsSearchRequest{Q: q, Limit: limit})
	if err != nil {
		t.Fatalf("contacts.search(%q): %v", q, err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	return found
}

// channelPeerIDs extracts the channel ids from a peer vector, ignoring users.
func channelPeerIDs(peers []tg.PeerClass) []int64 {
	var out []int64
	for _, p := range peers {
		if ch, ok := p.(*tg.PeerChannel); ok {
			out = append(out, ch.ChannelID)
		}
	}
	return out
}

// userPeerIDs extracts the user ids from a peer vector, ignoring channels.
func userPeerIDs(peers []tg.PeerClass) []int64 {
	var out []int64
	for _, p := range peers {
		if u, ok := p.(*tg.PeerUser); ok {
			out = append(out, u.UserID)
		}
	}
	return out
}

// seedMixedMyResults gives the caller users users it has a dialog with and
// channels channels it created, all matching token, so both MyResults arms have
// candidates. Both id slices come back in creation order, which is ascending id
// order — the order the arms are required to page in.
func seedMixedMyResults(
	t *testing.T, s *store.Store, dsn string, callerID int64, phonePrefix, token string, users, channels int,
) (userIDs, channelIDs []int64) {
	t.Helper()
	ctx := context.Background()
	for i := range users {
		phone := fmt.Sprintf("%s%03d", phonePrefix, i)
		if _, err := s.CreateUser(ctx, phone); err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		partner, ok, err := s.UserByPhone(ctx, phone)
		if err != nil || !ok {
			t.Fatalf("load user %d: ok=%v err=%v", i, ok, err)
		}
		if err := api.SetUserFirstNameForTest(dsn, partner.ID, token); err != nil {
			t.Fatalf("set name %d: %v", i, err)
		}
		if _, err := api.SendMessageForTest(s, callerID, &tg.MessagesSendMessageRequest{
			Peer:     api.InputPeerUser(callerID, partner.ID),
			Message:  "hello",
			RandomID: int64(i + 1),
		}); err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
		userIDs = append(userIDs, partner.ID)
	}
	for i := range channels {
		// The caller creates the channel, so it holds an unbanned role-2 row.
		ch, err := s.CreateChannel(ctx, callerID, fmt.Sprintf("%s Channel %d", token, i), "About", false)
		if err != nil {
			t.Fatalf("create channel %d: %v", i, err)
		}
		channelIDs = append(channelIDs, ch.ID)
	}
	return userIDs, channelIDs
}

// TestContactsSearchMyResultsSharedLimitDefault proves the two MyResults arms
// share one limit budget at the default limit. Each arm is capped at the limit
// on its own, so a mixed match returned up to twice the limit before this was
// one budget — the M13 contract is a maximum on MyResults, not per arm.
func TestContactsSearchMyResultsSharedLimitDefault(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(context.Background(), "15550010001")
	if err != nil {
		t.Fatal(err)
	}
	_, wantChannels := seedMixedMyResults(t, s, dsn, caller.ID, "1555001010", "Sharedtoken", 6, 6)

	found := searchChannels(t, s, caller.ID, "Sharedtoken", 0)

	if len(found.MyResults) != 10 {
		t.Fatalf("MyResults len = %d, want 10 (the default limit, shared across both arms)", len(found.MyResults))
	}
	// Users first, then channels: the order is what makes truncation
	// deterministic, so it is pinned rather than left to the arms.
	if got := len(userPeerIDs(found.MyResults)); got != 6 {
		t.Errorf("MyResults user peers = %d, want 6", got)
	}
	// The four channels kept are the four lowest ids, not an arbitrary four:
	// both arms page in id order, which is what fixes who survives truncation.
	gotChannels := channelPeerIDs(found.MyResults)
	if len(gotChannels) != 4 {
		t.Fatalf("MyResults channel peers = %v, want 4 (the remaining budget)", gotChannels)
	}
	for i, want := range wantChannels[:4] {
		if gotChannels[i] != want {
			t.Errorf("MyResults channel peer %d = %d, want %d (lowest ids first)", i, gotChannels[i], want)
		}
	}
	for i, p := range found.MyResults {
		if _, isUser := p.(*tg.PeerUser); isUser && i >= 6 {
			t.Errorf("MyResults[%d] is a user peer after the channel peers began", i)
		}
	}
	// Every named channel peer must be hydrated in Chats, and no more.
	if len(found.Chats) != 4 {
		t.Errorf("Chats len = %d, want 4 (only the channels actually named)", len(found.Chats))
	}
	if len(found.Users) != 6 {
		t.Errorf("Users len = %d, want 6", len(found.Users))
	}
}

// TestContactsSearchMyResultsSharedLimitCap proves the shared budget is the
// capped limit, not the requested one, when both arms have candidates.
func TestContactsSearchMyResultsSharedLimitCap(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(context.Background(), "15550011001")
	if err != nil {
		t.Fatal(err)
	}
	seedMixedMyResults(t, s, dsn, caller.ID, "1555001110", "Captoken", 30, 30)

	found := searchChannels(t, s, caller.ID, "Captoken", 999)

	if len(found.MyResults) != 50 {
		t.Fatalf("MyResults len = %d, want 50 (the cap, shared across both arms)", len(found.MyResults))
	}
	if got := len(userPeerIDs(found.MyResults)); got != 30 {
		t.Errorf("MyResults user peers = %d, want 30", got)
	}
	if got := len(channelPeerIDs(found.MyResults)); got != 20 {
		t.Errorf("MyResults channel peers = %d, want 20 (the remaining budget)", got)
	}
}

// TestContactsSearchMyResultsBudgetExhaustedByUsers proves the budget is spent
// in one order: users fill it first, and the channel arm is squeezed out rather
// than appended past the limit.
func TestContactsSearchMyResultsBudgetExhaustedByUsers(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(context.Background(), "15550012001")
	if err != nil {
		t.Fatal(err)
	}
	seedMixedMyResults(t, s, dsn, caller.ID, "1555001210", "Fulltoken", 12, 3)

	found := searchChannels(t, s, caller.ID, "Fulltoken", 10)

	if len(found.MyResults) != 10 {
		t.Fatalf("MyResults len = %d, want 10", len(found.MyResults))
	}
	if got := channelPeerIDs(found.MyResults); len(got) != 0 {
		t.Errorf("MyResults channel peers = %v, want none — users filled the budget", got)
	}
	if len(found.Chats) != 0 {
		t.Errorf("Chats len = %d, want 0 — no channel was named", len(found.Chats))
	}
}

// TestContactsSearchMyResultsDeterministic pins which rows survive truncation,
// not merely that successive calls agree. 14 matching users against a budget of
// 10 means four are dropped, and the ten kept must be the ten lowest ids.
//
// Honest limit of this test: at this data size the plan returns rows in id
// order with or without the ORDER BY on the user arm, so it cannot falsify the
// ordering — dropping the ORDER BY leaves it green. The ORDER BY is what makes
// the surviving set a contract rather than a property of the current plan, and
// this test states the contract; it is the shared budget below that the suite
// actually proves by failing without it.
func TestContactsSearchMyResultsDeterministic(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(context.Background(), "15550013001")
	if err != nil {
		t.Fatal(err)
	}
	wantUsers, _ := seedMixedMyResults(t, s, dsn, caller.ID, "1555001310", "Stabletoken", 14, 4)

	for call := range 5 {
		found := searchChannels(t, s, caller.ID, "Stabletoken", 10)
		if len(found.MyResults) != 10 {
			t.Fatalf("call %d: MyResults len = %d, want 10", call, len(found.MyResults))
		}
		got := userPeerIDs(found.MyResults)
		if len(got) != 10 {
			t.Fatalf("call %d: MyResults user peers = %d, want 10 (users fill the budget)", call, len(got))
		}
		for i, want := range wantUsers[:10] {
			if got[i] != want {
				t.Fatalf("call %d: MyResults user peer %d = %d, want %d (lowest ids first)",
					call, i, got[i], want)
			}
		}
	}
}

// TestContactsSearchTruncatedMemberStillRendersAsMember proves the budget
// shortens the peer vector without changing how a channel is rendered.
//
// A public channel the caller belongs to matches both arms. When the user
// matches fill the MyResults budget it is squeezed out of that vector, but it
// is still named in Results, and the caller is still a member — so the Chats
// entry must stay the member view. Deciding that from the truncated member
// matches instead of all of them rendered the caller's own channel as
// Left: true, Creator: false, which a client caches as a channel they left.
func TestContactsSearchTruncatedMemberStillRendersAsMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550015001")
	if err != nil {
		t.Fatal(err)
	}
	// The caller creates the channel, so it is theirs at role 2, and gives it a
	// username so the public arm names it too.
	ch, err := s.CreateChannel(ctx, caller.ID, "Squeezed Broadcast", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "squeezedbroadcast"); err != nil {
		t.Fatal(err)
	}
	// Ten matching contacts consume the whole default budget.
	seedMixedMyResults(t, s, dsn, caller.ID, "1555001510", "Squeezed", 10, 0)

	found := searchChannels(t, s, caller.ID, "Squeezed", 10)

	if got := channelPeerIDs(found.MyResults); len(got) != 0 {
		t.Fatalf("MyResults channels = %v, want none — the users fill the budget", got)
	}
	if len(found.MyResults) != 10 {
		t.Fatalf("MyResults len = %d, want 10", len(found.MyResults))
	}
	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != ch.ID {
		t.Fatalf("Results channels = %v, want [%d]", got, ch.ID)
	}

	var rendered *tg.Channel
	for _, c := range found.Chats {
		if channel, ok := c.(*tg.Channel); ok && channel.ID == ch.ID {
			rendered = channel
		}
	}
	if rendered == nil {
		t.Fatalf("channel %d missing from Chats", ch.ID)
	}
	if rendered.Left {
		t.Error("Chats Left = true for the caller's own channel — truncation changed the rendering")
	}
	if !rendered.Creator {
		t.Error("Chats Creator = false for a channel the caller created")
	}
	if rendered.Title != "Squeezed Broadcast" {
		t.Errorf("Chats Title = %q, want %q", rendered.Title, "Squeezed Broadcast")
	}
}

// TestContactsSearchMemberBeyondMemberArmPageRendersAsMember covers the same
// defect one level down: the member arm is itself capped at the limit in SQL, so
// a caller in more matching channels than that has memberships the arm never
// returned. A public match outside its page must still render as the member
// view — deciding membership from the arm's rows alone left the caller's own
// channel marked Left here too, with no Go-side truncation involved.
func TestContactsSearchMemberBeyondMemberArmPageRendersAsMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550016001")
	if err != nil {
		t.Fatal(err)
	}
	// Eleven private matches sort ahead of the public one, filling the member
	// arm's page of ten so the public channel falls outside it.
	for i := range 11 {
		if _, err := s.CreateChannel(ctx, caller.ID, fmt.Sprintf("Beyondpage Private %d", i), "About", false); err != nil {
			t.Fatalf("create private channel %d: %v", i, err)
		}
	}
	pub, err := s.CreateChannel(ctx, caller.ID, "Beyondpage Public", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, pub.ID, "beyondpagepublic"); err != nil {
		t.Fatal(err)
	}

	found := searchChannels(t, s, caller.ID, "Beyondpage", 10)

	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != pub.ID {
		t.Fatalf("Results channels = %v, want [%d]", got, pub.ID)
	}
	var rendered *tg.Channel
	for _, c := range found.Chats {
		if channel, ok := c.(*tg.Channel); ok && channel.ID == pub.ID {
			rendered = channel
		}
	}
	if rendered == nil {
		t.Fatalf("channel %d missing from Chats", pub.ID)
	}
	if rendered.Left || !rendered.Creator {
		t.Errorf("Chats for the caller's own channel = Left %v Creator %v, want Left false Creator true",
			rendered.Left, rendered.Creator)
	}
}

// TestContactsSearchBannedMemberRendersAsNonMember proves the membership lookup
// that feeds the rendering carries the ban with it: a banned member is a
// non-member for the public channel they can still discover, so the public view
// is the right one and the ban is not cosmetic in the rendering either.
func TestContactsSearchBannedMemberRendersAsNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	banned, err := s.CreateUser(ctx, "15550017001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550017002")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Bannedrender Club", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "bannedrenderclub"); err != nil {
		t.Fatal(err)
	}
	joinChannelByInvite(t, s, ch, banned.ID)
	if err := s.SetChannelBan(ctx, ch.ID, creator.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban: %v", err)
	}

	found := searchChannels(t, s, banned.ID, "Bannedrender", 10)

	if got := channelPeerIDs(found.MyResults); len(got) != 0 {
		t.Fatalf("banned caller MyResults channels = %v, want none", got)
	}
	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != ch.ID {
		t.Fatalf("Results channels = %v, want [%d] — the channel is still public", got, ch.ID)
	}
	rendered, ok := found.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("Chats[0] type = %T, want *tg.Channel", found.Chats[0])
	}
	if !rendered.Left {
		t.Error("Chats Left = false for a banned caller, want the public view")
	}
}

// TestContactsSearchLimitIsPerVector proves the shared budget is MyResults's
// own: the public discovery arm is a separate vector with its own limit, and
// capping them jointly would let the caller's memberships shrink public
// discovery.
func TestContactsSearchLimitIsPerVector(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550014001")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateUser(ctx, "15550014002")
	if err != nil {
		t.Fatal(err)
	}
	// The caller fills its own MyResults budget with users and one channel.
	seedMixedMyResults(t, s, dsn, caller.ID, "1555001410", "Vectortoken", 10, 1)
	// Someone else owns three public channels matching the same token.
	for i := range 3 {
		ch, err := s.CreateChannel(ctx, other.ID, fmt.Sprintf("Vectortoken Public %d", i), "About", false)
		if err != nil {
			t.Fatalf("create public channel %d: %v", i, err)
		}
		if err := api.ClaimChannelUsernameForTest(s, ch.ID, fmt.Sprintf("vectorpub%d", i)); err != nil {
			t.Fatalf("claim username %d: %v", i, err)
		}
	}

	found := searchChannels(t, s, caller.ID, "Vectortoken", 10)

	if len(found.MyResults) != 10 {
		t.Errorf("MyResults len = %d, want 10", len(found.MyResults))
	}
	if got := channelPeerIDs(found.Results); len(got) != 3 {
		t.Errorf("Results channels = %v, want 3 — Results has its own limit", got)
	}
}

// TestContactsSearchPublicChannelByTitle proves a non-member finds a channel
// with a public username by its title, in Results, carrying the access_hash
// derived for that caller — the same one contacts.resolveUsername hands them,
// which is what makes the result usable to join or resolve.
func TestContactsSearchPublicChannelByTitle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550001001")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateUser(ctx, "15550001002")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550001003")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Gardening Weekly", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "gardenweekly"); err != nil {
		t.Fatal(err)
	}

	found := searchChannels(t, s, caller.ID, "Gardening", 10)

	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != ch.ID {
		t.Fatalf("Results channels = %v, want [%d]", got, ch.ID)
	}
	if got := channelPeerIDs(found.MyResults); len(got) != 0 {
		t.Fatalf("MyResults channels = %v, want none (caller is not a member)", got)
	}
	if len(found.Chats) != 1 {
		t.Fatalf("Chats len = %d, want 1", len(found.Chats))
	}
	chat, ok := found.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("Chats[0] type = %T, want *tg.Channel", found.Chats[0])
	}
	if chat.ID != ch.ID || chat.Title != "Gardening Weekly" {
		t.Fatalf("Chats[0] = id %d title %q, want id %d title %q", chat.ID, chat.Title, ch.ID, "Gardening Weekly")
	}
	if chat.Username != "gardenweekly" {
		t.Errorf("Chats[0].Username = %q, want %q", chat.Username, "gardenweekly")
	}
	if !chat.Left {
		t.Error("Chats[0].Left = false, want true for a non-member")
	}

	// The access_hash is per-viewer: it must equal the one resolveUsername
	// derives for this caller, and must differ for a different caller.
	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "gardenweekly"})
	if err != nil {
		t.Fatal(err)
	}
	resolved, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("resolveUsername response type = %T", res)
	}
	resolvedCh, ok := resolved.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("resolveUsername Chats[0] type = %T", resolved.Chats[0])
	}
	if chat.AccessHash != resolvedCh.AccessHash {
		t.Errorf("search access_hash = %d, resolveUsername access_hash = %d, want equal",
			chat.AccessHash, resolvedCh.AccessHash)
	}

	otherFound := searchChannels(t, s, other.ID, "Gardening", 10)
	otherChat, ok := otherFound.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("other Chats[0] type = %T", otherFound.Chats[0])
	}
	if otherChat.AccessHash == chat.AccessHash {
		t.Error("access_hash is identical for two callers, want per-viewer derivation")
	}
}

// TestContactsSearchPublicChannelByUsername proves the handle itself matches,
// with or without the leading @, so a caller who knows the username finds the
// channel the same way a title match does.
func TestContactsSearchPublicChannelByUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550002001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550002002")
	if err != nil {
		t.Fatal(err)
	}
	// A title that shares no token with the handle, so only the handle can match.
	ch, err := s.CreateChannel(ctx, creator.ID, "Quiet Reading Room", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "bookclub"); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{"bookclub", "BookClub", "@bookclub"} {
		found := searchChannels(t, s, caller.ID, q, 10)
		if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != ch.ID {
			t.Errorf("search(%q) Results channels = %v, want [%d]", q, got, ch.ID)
		}
	}
}

// TestContactsSearchPrivateChannelInvisible proves a private channel never
// reaches a non-member, whatever the query matches, and that the response is
// the same one a query matching nothing at all returns.
func TestContactsSearchPrivateChannelInvisible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550003001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550003002")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateChannel(ctx, creator.ID, "Secret Cabal", "About", false); err != nil {
		t.Fatal(err)
	}

	match := searchChannels(t, s, caller.ID, "Secret Cabal", 10)
	miss := searchChannels(t, s, caller.ID, "nothingmatchesthis", 10)

	if len(match.Results) != 0 || len(match.MyResults) != 0 || len(match.Chats) != 0 || len(match.Users) != 0 {
		t.Fatalf("private channel matched: Results=%d MyResults=%d Chats=%d Users=%d, want an empty response",
			len(match.Results), len(match.MyResults), len(match.Chats), len(match.Users))
	}
	if len(miss.Results) != 0 || len(miss.MyResults) != 0 || len(miss.Chats) != 0 || len(miss.Users) != 0 {
		t.Fatalf("no-match response is not empty: Results=%d MyResults=%d Chats=%d Users=%d",
			len(miss.Results), len(miss.MyResults), len(miss.Chats), len(miss.Users))
	}
}

// TestContactsSearchPrivateChannelDoesNotConsumeLimit is the pre-LIMIT filter
// test the M14 threat model requires: a private channel matching the query must
// not occupy a row inside the limit that is then dropped, because the caller
// would read the missing row as proof the private channel exists.
//
// The private channel is created first, so it sorts ahead of every public match
// in the query's id order and would land inside the page under a Go post-filter.
func TestContactsSearchPrivateChannelDoesNotConsumeLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550004001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550004002")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.CreateChannel(ctx, creator.ID, "Kayaking Private", "About", false); err != nil {
		t.Fatal(err)
	}
	const limit = 3
	want := make([]int64, 0, limit)
	for i := range limit {
		ch, err := s.CreateChannel(ctx, creator.ID, fmt.Sprintf("Kayaking Club %d", i), "About", false)
		if err != nil {
			t.Fatalf("create public channel %d: %v", i, err)
		}
		if err := api.ClaimChannelUsernameForTest(s, ch.ID, fmt.Sprintf("kayakclub%d", i)); err != nil {
			t.Fatalf("claim username %d: %v", i, err)
		}
		want = append(want, ch.ID)
	}

	found := searchChannels(t, s, caller.ID, "Kayaking", limit)

	got := channelPeerIDs(found.Results)
	if len(got) != limit {
		t.Fatalf("Results channels = %v (%d rows), want %d — a private match consumed a row",
			got, len(got), limit)
	}
	for i, id := range want {
		if got[i] != id {
			t.Fatalf("Results[%d] = %d, want %d", i, got[i], id)
		}
	}
}

// TestContactsSearchMemberFindsPrivateChannel proves a member finds a private
// channel they belong to in MyResults, rendered with its real title, while a
// non-member searching the same title finds nothing.
func TestContactsSearchMemberFindsPrivateChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	member, err := s.CreateUser(ctx, "15550005001")
	if err != nil {
		t.Fatal(err)
	}
	outsider, err := s.CreateUser(ctx, "15550005002")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550005003")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Rowing Crew", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	found := searchChannels(t, s, member.ID, "Rowing", 10)
	if got := channelPeerIDs(found.MyResults); len(got) != 1 || got[0] != ch.ID {
		t.Fatalf("member MyResults channels = %v, want [%d]", got, ch.ID)
	}
	if got := channelPeerIDs(found.Results); len(got) != 0 {
		t.Fatalf("member Results channels = %v, want none (channel has no username)", got)
	}
	chat, ok := found.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("Chats[0] type = %T, want *tg.Channel", found.Chats[0])
	}
	if chat.Title != "Rowing Crew" {
		t.Errorf("Chats[0].Title = %q, want %q", chat.Title, "Rowing Crew")
	}
	if chat.Left {
		t.Error("Chats[0].Left = true, want false for a member")
	}

	outsiderFound := searchChannels(t, s, outsider.ID, "Rowing", 10)
	if len(outsiderFound.MyResults) != 0 || len(outsiderFound.Results) != 0 {
		t.Fatalf("outsider saw the private channel: MyResults=%d Results=%d",
			len(outsiderFound.MyResults), len(outsiderFound.Results))
	}
}

// TestContactsSearchBannedMemberExcluded proves a ban is not cosmetic: the
// retained participant row must stop yielding the channel in MyResults.
func TestContactsSearchBannedMemberExcluded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	banned, err := s.CreateUser(ctx, "15550006001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550006002")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Fencing Society", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	joinChannelByInvite(t, s, ch, banned.ID)

	before := searchChannels(t, s, banned.ID, "Fencing", 10)
	if got := channelPeerIDs(before.MyResults); len(got) != 1 {
		t.Fatalf("pre-ban MyResults channels = %v, want [%d]", got, ch.ID)
	}

	if err := s.SetChannelBan(ctx, ch.ID, creator.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban: %v", err)
	}

	after := searchChannels(t, s, banned.ID, "Fencing", 10)
	if got := channelPeerIDs(after.MyResults); len(got) != 0 {
		t.Fatalf("banned caller MyResults channels = %v, want none", got)
	}
	if got := channelPeerIDs(after.Results); len(got) != 0 {
		t.Fatalf("banned caller Results channels = %v, want none (channel has no username)", got)
	}
	if len(after.Chats) != 0 {
		t.Fatalf("banned caller Chats = %d, want 0", len(after.Chats))
	}
}

// TestContactsSearchMemberOfPublicChannel proves a public channel the caller
// belongs to is rendered once, as the member view, even though it matches both
// the public arm and the membership arm.
func TestContactsSearchMemberOfPublicChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	member, err := s.CreateUser(ctx, "15550007001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550007002")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Cycling Digest", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "cyclingdigest"); err != nil {
		t.Fatal(err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	found := searchChannels(t, s, member.ID, "Cycling", 10)
	if got := channelPeerIDs(found.MyResults); len(got) != 1 || got[0] != ch.ID {
		t.Fatalf("MyResults channels = %v, want [%d]", got, ch.ID)
	}
	if len(found.Chats) != 1 {
		t.Fatalf("Chats len = %d, want 1 (one channel, one rendering)", len(found.Chats))
	}
	chat, ok := found.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("Chats[0] type = %T, want *tg.Channel", found.Chats[0])
	}
	if chat.Left {
		t.Error("Chats[0].Left = true, want false — the member view must win")
	}
}

// TestContactsSearchUsersAndChannels proves the M13 user arm is untouched when
// channels also match: both populate the response, and users keep their own
// MyResults entries and Users hydration.
func TestContactsSearchUsersAndChannels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550008001")
	if err != nil {
		t.Fatal(err)
	}
	partner, err := s.CreateUser(ctx, "15550008002")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, partner.ID, "Sailing"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(caller.ID, partner.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, partner.ID, "Sailing Notes", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "sailingnotes"); err != nil {
		t.Fatal(err)
	}

	found := searchChannels(t, s, caller.ID, "Sailing", 10)

	userPeers := 0
	for _, p := range found.MyResults {
		if u, ok := p.(*tg.PeerUser); ok && u.UserID == partner.ID {
			userPeers++
		}
	}
	if userPeers != 1 {
		t.Errorf("MyResults user peers = %d, want 1", userPeers)
	}
	if len(found.Users) != 1 {
		t.Errorf("Users len = %d, want 1", len(found.Users))
	}
	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != ch.ID {
		t.Errorf("Results channels = %v, want [%d]", got, ch.ID)
	}
}
