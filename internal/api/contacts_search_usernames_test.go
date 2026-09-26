package api_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func contactsSearchUsernames(t *testing.T, s *store.Store, callerID int64, q string) *tg.ContactsFound {
	t.Helper()
	return contactsSearchUsernamesWithLimit(t, s, callerID, q, 10)
}

func contactsSearchUsernamesWithLimit(t *testing.T, s *store.Store, callerID int64, q string, limit int) *tg.ContactsFound {
	t.Helper()
	res, err := api.ContactsSearchForTest(s, callerID, &tg.ContactsSearchRequest{Q: q, Limit: limit})
	if err != nil {
		t.Fatalf("contacts.search(%q): %v", q, err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("contacts.search(%q) result type = %T, want *tg.ContactsFound", q, res)
	}
	return found
}

func usernameLookupPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open username lookup pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func usernameLookupCount(t *testing.T, pool *pgxpool.Pool, callerID int64, handle string) int {
	t.Helper()
	var count int
	err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM username_lookups WHERE caller_id = $1 AND handle = $2",
		callerID, handle,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count username lookups for %q: %v", handle, err)
	}
	return count
}

func TestContactsSearchFindsExactUsername(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()

	caller, err := s.CreateUser(ctx, "15550009001")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009002")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "operator"); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{"operator", "@operator", "OpErAtOr"} {
		found := contactsSearchUsernames(t, s, caller.ID, q)
		if got := userPeerIDs(found.Results); len(got) != 1 || got[0] != target.ID {
			t.Errorf("search(%q) user Results = %v, want [%d]", q, got, target.ID)
		}
		if len(found.MyResults) != 0 {
			t.Errorf("search(%q) MyResults = %v, want no dialog-scoped users", q, found.MyResults)
		}
		if len(found.Users) != 1 {
			t.Fatalf("search(%q) Users len = %d, want 1", q, len(found.Users))
		}
		user, ok := found.Users[0].(*tg.User)
		if !ok {
			t.Fatalf("search(%q) Users[0] type = %T, want *tg.User", q, found.Users[0])
		}
		if user.ID != target.ID || user.Username != "operator" || user.Self {
			t.Errorf("search(%q) user = {id:%d username:%q self:%t}, want target operator", q, user.ID, user.Username, user.Self)
		}
		if user.Phone != "" {
			t.Errorf("search(%q) stranger phone = %q, want empty", q, user.Phone)
		}
		if user.AccessHash != api.DeriveUserHash(caller.ID, target.ID) {
			t.Errorf("search(%q) access hash = %d, want viewer-derived hash", q, user.AccessHash)
		}
	}

	resolved, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "OPERATOR"})
	if err != nil {
		t.Fatal(err)
	}
	resolvedPeer, ok := resolved.(*tg.ContactsResolvedPeer)
	if !ok || len(resolvedPeer.Users) != 1 {
		t.Fatalf("resolveUsername result = %T, want one user", resolved)
	}
	resolvedUser, ok := resolvedPeer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("resolveUsername Users[0] type = %T, want *tg.User", resolvedPeer.Users[0])
	}
	searched := contactsSearchUsernames(t, s, caller.ID, "operator")
	if len(searched.Users) != 1 {
		t.Fatalf("repeated search Users len = %d, want 1", len(searched.Users))
	}
	searchedUser, ok := searched.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("repeated search Users[0] type = %T, want *tg.User", searched.Users[0])
	}
	if !reflect.DeepEqual(searchedUser, resolvedUser) {
		t.Errorf("search user = %#v, resolveUsername user = %#v, want the same public rendering", searchedUser, resolvedUser)
	}
}

func TestContactsSearchFindsTwoCharacterUsername(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009003")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009004")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "ab"); err != nil {
		t.Fatal(err)
	}

	found := contactsSearchUsernames(t, s, caller.ID, "@ab")
	if got := userPeerIDs(found.Results); len(got) != 1 || got[0] != target.ID {
		t.Errorf("search(@ab) user Results = %v, want [%d]", got, target.ID)
	}
}

func TestContactsSearchUsernameRequiresExactHandle(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	pool := usernameLookupPool(t, dsn)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009101")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := s.CreateUser(ctx, "15550009102")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, operator.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	wildcardTarget, err := s.CreateUser(ctx, "15550009103")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, wildcardTarget.ID, "abcde"); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{"operato", "a____", "%"} {
		found := contactsSearchUsernames(t, s, caller.ID, q)
		if got := userPeerIDs(found.Results); len(got) != 0 {
			t.Errorf("search(%q) user Results = %v, want no pattern or prefix match", q, got)
		}
	}
	if count := usernameLookupCount(t, pool, caller.ID, "operato"); count != 1 {
		t.Errorf("prefix probe count = %d, want one exact miss charged", count)
	}
	if count := usernameLookupCount(t, pool, caller.ID, "a____"); count != 1 {
		t.Errorf("underscore probe count = %d, want one literal exact miss charged", count)
	}
	if count := usernameLookupCount(t, pool, caller.ID, "%"); count != 0 {
		t.Errorf("invalid percent query count = %d, want no quota charge", count)
	}
}

func TestContactsSearchUsernameChargesHitsMissesAndChannels(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	pool := usernameLookupPool(t, dsn)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009201")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009202")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550009203")
	if err != nil {
		t.Fatal(err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "Quiet Reading Room", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, channel.ID, "bookclub"); err != nil {
		t.Fatal(err)
	}

	if got := userPeerIDs(contactsSearchUsernames(t, s, caller.ID, "@OpErAtOr").Results); len(got) != 1 || got[0] != target.ID {
		t.Fatalf("exact hit Results = %v, want [%d]", got, target.ID)
	}
	if _, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "OPERATOR"}); err != nil {
		t.Fatalf("resolve repeated handle: %v", err)
	}
	if got := userPeerIDs(contactsSearchUsernames(t, s, caller.ID, "operator").Results); len(got) != 1 || got[0] != target.ID {
		t.Fatalf("repeated exact hit Results = %v, want [%d]", got, target.ID)
	}

	miss := contactsSearchUsernames(t, s, caller.ID, "missing")
	if len(miss.Results) != 0 || len(miss.Users) != 0 {
		t.Errorf("exact miss result = %+v, want no peer", miss)
	}
	channelResults := contactsSearchUsernames(t, s, caller.ID, "bookclub")
	if got := channelPeerIDs(channelResults.Results); len(got) != 1 || got[0] != channel.ID {
		t.Errorf("channel handle Results = %v, want [%d]", got, channel.ID)
	}
	if got := userPeerIDs(channelResults.Results); len(got) != 0 {
		t.Errorf("channel handle user Results = %v, want none", got)
	}

	for _, handle := range []string{"operator", "missing", "bookclub"} {
		if count := usernameLookupCount(t, pool, caller.ID, handle); count == 0 {
			t.Errorf("handle %q has no username quota record", handle)
		}
	}
	var distinct int
	if err := pool.QueryRow(ctx,
		"SELECT count(DISTINCT handle) FROM username_lookups WHERE caller_id = $1", caller.ID,
	).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if distinct != 3 {
		t.Errorf("distinct username quota count = %d, want 3 after repeated RPCs", distinct)
	}
}

func TestContactsSearchExactUsernameRespectsResultsBudget(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009211")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009212")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "bookclub"); err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550009213")
	if err != nil {
		t.Fatal(err)
	}
	channelIDs := make([]int64, 0, 3)
	for i := range 3 {
		channel, err := s.CreateChannel(ctx, creator.ID, fmt.Sprintf("Bookclub News %d", i), "About", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := api.ClaimChannelUsernameForTest(s, channel.ID, fmt.Sprintf("newsclub%d", i)); err != nil {
			t.Fatal(err)
		}
		channelIDs = append(channelIDs, channel.ID)
	}
	slices.Sort(channelIDs)

	found := contactsSearchUsernamesWithLimit(t, s, caller.ID, "Bookclub", 2)
	if len(found.Results) != 2 {
		t.Fatalf("Results len = %d, want limit 2 shared by user and channel", len(found.Results))
	}
	if peer, ok := found.Results[0].(*tg.PeerUser); !ok || peer.UserID != target.ID {
		t.Errorf("Results[0] = %#v, want exact user %d first", found.Results[0], target.ID)
	}
	if peer, ok := found.Results[1].(*tg.PeerChannel); !ok || peer.ChannelID != channelIDs[0] {
		t.Errorf("Results[1] = %#v, want first ordered channel %d", found.Results[1], channelIDs[0])
	}
	if len(found.Chats) != 1 {
		t.Fatalf("Chats len = %d, want only the channel retained by the Results budget", len(found.Chats))
	}
	chat, ok := found.Chats[0].(*tg.Channel)
	if !ok || chat.ID != channelIDs[0] {
		t.Errorf("Chats[0] = %#v, want channel %d", found.Chats[0], channelIDs[0])
	}
}

func TestContactsSearchExhaustedUsernameQuotaPreservesOtherResults(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	pool := usernameLookupPool(t, dsn)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009301")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009302")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	contact, err := s.CreateUser(ctx, "15550009303")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, contact.ID, "Operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(caller.ID, contact.ID), Message: "hello", RandomID: 1,
	}); err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(ctx, "15550009304")
	if err != nil {
		t.Fatal(err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "Operator Updates", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimChannelUsernameForTest(s, channel.ID, "updates"); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	for i := range store.UsernameLookupBurstLimit {
		if _, err := pool.Exec(ctx,
			"INSERT INTO username_lookups (caller_id, handle, looked_up_at) VALUES ($1, $2, $3)",
			caller.ID, fmt.Sprintf("seed%02d", i), now,
		); err != nil {
			t.Fatalf("seed lookup %d: %v", i, err)
		}
	}

	found := contactsSearchUsernames(t, s, caller.ID, "operator")
	if got := userPeerIDs(found.MyResults); len(got) != 1 || got[0] != contact.ID {
		t.Errorf("dialog results after lookup quota exhaustion = %v, want [%d]", got, contact.ID)
	}
	if got := channelPeerIDs(found.Results); len(got) != 1 || got[0] != channel.ID {
		t.Errorf("channel results after lookup quota exhaustion = %v, want [%d]", got, channel.ID)
	}
	if got := userPeerIDs(found.Results); len(got) != 0 {
		t.Errorf("exact user results after lookup quota exhaustion = %v, want none", got)
	}
	if count := usernameLookupCount(t, pool, caller.ID, "operator"); count != 0 {
		t.Errorf("rejected handle lookup rows = %d, want zero after quota rollback", count)
	}
}

func TestContactsSearchUsernameQuotaStorageFailureIsInternal(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	pool := usernameLookupPool(t, dsn)
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, "15550009311")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550009312")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, target.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE username_lookups"); err != nil {
		t.Fatal(err)
	}

	_, err = api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "operator"})
	if err == nil || !tgerr.Is(err, "INTERNAL") {
		t.Fatalf("contacts.search with quota storage failure = %v, want INTERNAL", err)
	}
}

func TestContactsSearchUsernameSelfAndDialogDeduplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	self, err := s.CreateUser(ctx, "15550009401")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, self.ID, "selfuser"); err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, self.ID, "Selfuser"); err != nil {
		t.Fatal(err)
	}
	foundSelf := contactsSearchUsernames(t, s, self.ID, "SELFUSER")
	if got := userPeerIDs(foundSelf.Results); len(got) != 1 || got[0] != self.ID {
		t.Fatalf("self search Results = %v, want [%d]", got, self.ID)
	}
	if len(foundSelf.Users) != 1 {
		t.Fatalf("self search Users len = %d, want 1", len(foundSelf.Users))
	}
	selfUser, ok := foundSelf.Users[0].(*tg.User)
	if !ok || selfUser.ID != self.ID || !selfUser.Self || selfUser.Phone != "15550009401" {
		t.Errorf("self search user = %#v, want one self entry with own phone", foundSelf.Users[0])
	}

	caller, err := s.CreateUser(ctx, "15550009402")
	if err != nil {
		t.Fatal(err)
	}
	contact, err := s.CreateUser(ctx, "15550009403")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, contact.ID, "operator"); err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, contact.ID, "Operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(caller.ID, contact.ID), Message: "hello", RandomID: 2,
	}); err != nil {
		t.Fatal(err)
	}

	foundContact := contactsSearchUsernames(t, s, caller.ID, "operator")
	if got := userPeerIDs(foundContact.MyResults); len(got) != 1 || got[0] != contact.ID {
		t.Errorf("dialog MyResults = %v, want [%d]", got, contact.ID)
	}
	if got := userPeerIDs(foundContact.Results); len(got) != 1 || got[0] != contact.ID {
		t.Errorf("exact-match Results = %v, want [%d]", got, contact.ID)
	}
	if len(foundContact.Users) != 1 {
		t.Fatalf("deduplicated Users len = %d, want 1", len(foundContact.Users))
	}
	contactUser, ok := foundContact.Users[0].(*tg.User)
	if !ok || contactUser.ID != contact.ID || contactUser.Phone != "" || contactUser.Self {
		t.Errorf("dialog rendering = %#v, want retained stranger rendering", foundContact.Users[0])
	}
}
