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
