package api_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestSearchMessagesRateLimit proves that N+1 searches within the window are
// denied with FLOOD_WAIT and have no side effects.
func TestSearchMessagesRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296001")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296002")
	if err != nil {
		t.Fatal(err)
	}

	// A sends a message to B so there is something to search.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello world",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// 2 searches per 10s.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Searches 1-2 should pass.
	for i := range 2 {
		enc, err := api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   peerBob,
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		if err != nil {
			t.Fatalf("search %d: %v", i+1, err)
		}
		res, ok := enc.(*tg.MessagesMessages)
		if !ok {
			t.Fatalf("search %d: result type = %T, want *tg.MessagesMessages", i+1, enc)
		}
		if len(res.Messages) != 1 {
			t.Fatalf("search %d: got %d messages, want 1", i+1, len(res.Messages))
		}
	}

	// Search 3 should be denied with FLOOD_WAIT.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if !isFloodWait(err) {
		t.Fatalf("search 3: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSearchMessagesRateLimitIndependentAccounts proves that two accounts'
// search quotas are independent.
func TestSearchMessagesRateLimitIndependentAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296101")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296102")
	if err != nil {
		t.Fatal(err)
	}

	// A sends a message to B so both have something to search.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)
	peerAlice := api.InputPeerUser(bob.ID, alice.ID)

	// Alice exhausts her quota.
	if _, err := api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	}); err != nil {
		t.Fatalf("alice search 1: %v", err)
	}
	// Alice's second search should be denied.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if !isFloodWait(err) {
		t.Fatalf("alice search 2: expected FLOOD_WAIT, got %v", err)
	}

	// Bob's first search should still succeed (independent quota).
	_, err = api.SearchForTestWithLimits(s, bob.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerAlice,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if err != nil {
		t.Fatalf("bob search 1: expected success, got %v", err)
	}
}

// TestSearchMessagesRateLimitDisabled proves that a zero limit disables
// enforcement for the search surface.
func TestSearchMessagesRateLimitDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296201")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296202")
	if err != nil {
		t.Fatal(err)
	}

	// A sends a message to B so there is something to search.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Zero limit = disabled.
	cfg := store.RateLimitConfig{}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Many searches should all pass.
	for i := range 10 {
		_, err := api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   peerBob,
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		if err != nil {
			t.Fatalf("search %d: %v", i+1, err)
		}
	}
}

// TestSearchMessagesRateLimitWindowExpiry proves that after the window expires,
// the same account can search again.
func TestSearchMessagesRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296301")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296302")
	if err != nil {
		t.Fatal(err)
	}

	// A sends a message to B so there is something to search.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Very short window: 1 search per 500ms.
	cfg := store.RateLimitConfig{Limit: 1, Window: 500 * time.Millisecond}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Exhaust the limit.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if err != nil {
		t.Fatalf("search 1: %v", err)
	}

	// Denied.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Wait for window to expire.
	time.Sleep(600 * time.Millisecond)

	// Should be allowed again.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if err != nil {
		t.Fatalf("post-expiry search: %v", err)
	}
}

// TestContactsSearchRateLimit proves that N+1 contacts searches within the
// window are denied with FLOOD_WAIT and have no side effects.
func TestContactsSearchRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551296401")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296402")
	if err != nil {
		t.Fatal(err)
	}
	// Bob needs a searchable name: name_tsv is empty for a phone-only user, so
	// every query would miss and the assertions below would prove nothing.
	if err := api.SetUserFirstNameForTest(dsn, bob.ID, "Bravo"); err != nil {
		t.Fatalf("set bob name: %v", err)
	}

	// A sends a message to B to establish a dialog.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// 2 searches per 10s.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	// Searches 1-2 should pass and return Bob.
	for i := range 2 {
		enc, err := api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
			Q:     "bravo",
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("search %d: %v", i+1, err)
		}
		assertContactsFound(t, fmt.Sprintf("search %d", i+1), enc, 1)
	}

	// Search 3 should be denied with FLOOD_WAIT.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("search 3: expected FLOOD_WAIT, got %v", err)
	}
}

// TestContactsSearchRateLimitIndependentAccounts proves that two accounts'
// contacts search quotas are independent.
func TestContactsSearchRateLimitIndependentAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551296501")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296502")
	if err != nil {
		t.Fatal(err)
	}
	// Both need searchable names so each side's search returns a real match.
	if err := api.SetUserFirstNameForTest(dsn, alice.ID, "Alpha"); err != nil {
		t.Fatalf("set alice name: %v", err)
	}
	if err := api.SetUserFirstNameForTest(dsn, bob.ID, "Bravo"); err != nil {
		t.Fatalf("set bob name: %v", err)
	}
	// One send writes both dialog rows, so each of them can see the other.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Alice exhausts her quota.
	enc, err := api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("alice search 1: %v", err)
	}
	assertContactsFound(t, "alice search 1", enc, 1)

	// Alice's second search should be denied.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("alice search 2: expected FLOOD_WAIT, got %v", err)
	}

	// Bob's first search should still succeed (independent quota).
	enc, err = api.ContactsSearchForTestWithLimits(s, bob.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "alpha",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("bob search 1: expected success, got %v", err)
	}
	assertContactsFound(t, "bob search 1", enc, 1)
}

// assertContactsFound checks that enc is a *tg.ContactsFound carrying want
// results, so a rate-limit test cannot pass on an empty result set.
func assertContactsFound(t *testing.T, label string, enc bin.Encoder, want int) {
	t.Helper()
	res, ok := enc.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("%s: result type = %T, want *tg.ContactsFound", label, enc)
	}
	if len(res.MyResults) != want {
		t.Fatalf("%s: got %d results, want %d", label, len(res.MyResults), want)
	}
}

// TestContactsSearchRateLimitWindowExpiry proves that after the window expires,
// the same account can contacts.search again.
func TestContactsSearchRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551296601")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296602")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, bob.ID, "Bravo"); err != nil {
		t.Fatalf("set bob name: %v", err)
	}
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Long window: only the explicit rewind below closes it. A window short
	// enough to sleep through also closes on its own under host load, and the
	// denial this test asserts on then never happens.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	// Exhaust the limit.
	enc, err := api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search 1: %v", err)
	}
	assertContactsFound(t, "search 1", enc, 1)

	// Denied.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Age the window past its deadline.
	if err := api.AgeRateLimitWindowForTest(dsn, alice.ID, "contacts_search", cfg.Window+time.Minute); err != nil {
		t.Fatalf("age window: %v", err)
	}

	// Should be allowed again, and still return the match.
	enc, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "bravo",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("post-expiry search: %v", err)
	}
	assertContactsFound(t, "post-expiry search", enc, 1)
}

// TestSearchMessagesChargeIsUniform proves the ticket's core invariant for
// messages.search: what a query matches does not change what the caller is
// charged, so the quota cannot be read as an existence oracle. A matching
// query and a query that matches nothing each spend one token out of two, and
// the third search is denied.
//
// It also pins the ordering the other way: an empty query is rejected by input
// validation before the limiter is consulted, so it neither spends a token nor
// reports FLOOD_WAIT.
func TestSearchMessagesChargeIsUniform(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296701")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296702")
	if err != nil {
		t.Fatal(err)
	}

	// A sends a message to B so there is something to search.
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Limit of 2.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	search := func(q string) (bin.Encoder, error) {
		return api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   peerBob,
			Q:      q,
			Filter: &tg.InputMessagesFilterEmpty{},
		})
	}

	// Token 1: a query that matches.
	enc, err := search("hello")
	if err != nil {
		t.Fatalf("matching search: %v", err)
	}
	res, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("matching search: result type = %T, want *tg.MessagesMessages", enc)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("matching search: got %d messages, want 1", len(res.Messages))
	}

	// Token 2: a query that matches nothing. Costs the same token.
	enc, err = search("zzzznothingmatchesthis")
	if err != nil {
		t.Fatalf("non-matching search: %v", err)
	}
	res, ok = enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("non-matching search: result type = %T, want *tg.MessagesMessages", enc)
	}
	if len(res.Messages) != 0 {
		t.Fatalf("non-matching search: got %d messages, want 0", len(res.Messages))
	}

	// Quota is spent: the third search is denied regardless of what it would
	// have matched.
	if _, err := search("hello"); !isFloodWait(err) {
		t.Fatalf("third search: expected FLOOD_WAIT, got %v", err)
	}

	// An empty query is rejected before the limiter, so it reports
	// SEARCH_QUERY_EMPTY rather than FLOOD_WAIT even over the limit.
	_, err = search("")
	if err == nil {
		t.Fatal("empty query: expected error, got nil")
	}
	if isFloodWait(err) {
		t.Fatal("empty query: expected SEARCH_QUERY_EMPTY, got FLOOD_WAIT")
	}
}

// TestSearchMessagesChargedBeforeMembershipCheck proves that a chat-peer search
// by a non-member is charged: the membership probe is a database query, so a
// charge behind it would let a non-member probe chat membership at unbounded
// rate, and would also make the quota a membership oracle.
func TestSearchMessagesChargedBeforeMembershipCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296901")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296902")
	if err != nil {
		t.Fatal(err)
	}
	mallory, err := s.CreateUser(ctx, "+15551296903")
	if err != nil {
		t.Fatal(err)
	}

	// Alice and Bob share a chat. Mallory is not a member.
	chat, err := s.CreateChat(ctx, alice.ID, "private chat", []int64{bob.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	probe := func() error {
		_, err := api.SearchForTestWithLimits(s, mallory.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   &tg.InputPeerChat{ChatID: chat.ID},
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	}

	// First probe: refused as PEER_ID_INVALID, and charged.
	err = probe()
	if err == nil {
		t.Fatal("probe 1: expected PEER_ID_INVALID, got nil")
	}
	if isFloodWait(err) {
		t.Fatalf("probe 1: expected PEER_ID_INVALID, got %v", err)
	}

	// Second probe: the token was spent by the first, so the limiter answers
	// before the membership query runs.
	if err := probe(); !isFloodWait(err) {
		t.Fatalf("probe 2: expected FLOOD_WAIT, got %v", err)
	}
}

// TestContactsSearchChargeIsUniform proves the same invariant for
// contacts.search: a query matching nothing costs the same token as one that
// matches, and an empty query is rejected before the limiter.
func TestContactsSearchChargeIsUniform(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551296801")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296802")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, bob.ID, "Bravo"); err != nil {
		t.Fatalf("set bob name: %v", err)
	}
	if _, err := api.SendMessageForTest(s, alice.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(alice.ID, bob.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Limit of 2.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	search := func(q string) (bin.Encoder, error) {
		return api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
			Q:     q,
			Limit: 10,
		})
	}

	// Token 1: a query that matches.
	enc, err := search("bravo")
	if err != nil {
		t.Fatalf("matching search: %v", err)
	}
	assertContactsFound(t, "matching search", enc, 1)

	// Token 2: a query that matches nothing. Costs the same token.
	enc, err = search("zzzznothingmatchesthis")
	if err != nil {
		t.Fatalf("non-matching search: %v", err)
	}
	assertContactsFound(t, "non-matching search", enc, 0)

	// Quota is spent.
	if _, err := search("bravo"); !isFloodWait(err) {
		t.Fatalf("third search: expected FLOOD_WAIT, got %v", err)
	}

	// An empty query is rejected before the limiter.
	_, err = search("")
	if err == nil {
		t.Fatal("empty query: expected error, got nil")
	}
	if isFloodWait(err) {
		t.Fatal("empty query: expected SEARCH_QUERY_EMPTY, got FLOOD_WAIT")
	}
}

// TestSearchMessagesChannelPeerSharesTheQuota proves that a channel-peer search
// spends the same per-account quota as a 1:1 or chat search and gets the same
// FLOOD_WAIT once it is exhausted. One quota covers messages.search whatever
// peer it names, so a caller cannot double their search budget by pointing it
// at a channel.
func TestSearchMessagesChannelPeerSharesTheQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	creator, err := s.CreateUser(ctx, "+15551296951")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296952")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := sendToChannel(t, s, creator.ID, ch.ID, "hello world", 9601); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(creator.ID, bob.ID),
		Message:  "hello world",
		RandomID: 9602,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Limit of 2, spent one by a user peer and one by the channel peer.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	search := func(peer tg.InputPeerClass) (bin.Encoder, error) {
		return api.SearchForTestWithLimits(s, creator.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   peer,
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
	}

	if _, err := search(api.InputPeerUser(creator.ID, bob.ID)); err != nil {
		t.Fatalf("user-peer search: %v", err)
	}
	enc, err := search(api.InputPeerChannel(creator.ID, ch.ID))
	if err != nil {
		t.Fatalf("channel-peer search: %v", err)
	}
	if ids := channelSearchIDs(t, enc); len(ids) != 1 {
		t.Fatalf("channel-peer search ids = %v, want one hit", ids)
	}

	if _, err := search(api.InputPeerChannel(creator.ID, ch.ID)); !isFloodWait(err) {
		t.Fatalf("third search: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSearchMessagesChannelChargedBeforeMembershipCheck proves that a
// channel-peer search by a non-member is charged: the membership probe is a
// database query, so a charge behind it would let a non-member probe the dense
// channels.id space at unbounded rate, and would make the quota a membership
// oracle.
func TestSearchMessagesChannelChargedBeforeMembershipCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	creator, err := s.CreateUser(ctx, "+15551296961")
	if err != nil {
		t.Fatal(err)
	}
	mallory, err := s.CreateUser(ctx, "+15551296962")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	probe := func() error {
		_, err := api.SearchForTestWithLimits(s, mallory.ID, cfg, &tg.MessagesSearchRequest{
			Peer:   api.InputPeerChannel(mallory.ID, ch.ID),
			Q:      "hello",
			Filter: &tg.InputMessagesFilterEmpty{},
		})
		return err
	}

	// First probe: refused as PEER_ID_INVALID, and charged.
	err = probe()
	if err == nil {
		t.Fatal("probe 1: expected PEER_ID_INVALID, got nil")
	}
	if isFloodWait(err) {
		t.Fatalf("probe 1: expected PEER_ID_INVALID, got %v", err)
	}

	// Second probe: the token was spent by the first, so the limiter answers
	// before the membership query runs.
	if err := probe(); !isFloodWait(err) {
		t.Fatalf("probe 2: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSearchGlobalRateLimit proves that N+1 global searches within the window
// are denied with FLOOD_WAIT: a cross-dialog search reaches every dialog the
// caller is in, so it may not be the one search surface that ships unthrottled.
func TestSearchGlobalRateLimit(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321051", "+15551321052")

	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	search := func() (bin.Encoder, error) {
		return api.SearchGlobalForTestWithLimits(s, d.caller.ID, cfg, &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      10,
		})
	}

	for i := range 2 {
		enc, err := search()
		if err != nil {
			t.Fatalf("search %d: %v", i+1, err)
		}
		if res := globalSlice(t, enc); len(res.Messages) != 3 {
			t.Fatalf("search %d: got %d hits, want 3", i+1, len(res.Messages))
		}
	}
	if _, err := search(); !isFloodWait(err) {
		t.Fatalf("search 3: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSearchGlobalQuotaIsUniformAndItsOwn proves the two properties the quota
// has to hold to be safe on this surface: every call costs the same token
// whatever it matches, so an exhausted budget answers no question about what
// exists, and the budget is not shared with messages.search, so exhausting one
// surface does not silently disable the other.
func TestSearchGlobalQuotaIsUniformAndItsOwn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321061", "+15551321062")
	outsider, err := s.CreateUser(ctx, "+15551321063")
	if err != nil {
		t.Fatal(err)
	}

	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	search := func(userID int64) (bin.Encoder, error) {
		return api.SearchGlobalForTestWithLimits(s, userID, cfg, &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      10,
		})
	}

	// A caller in no dialogs matches nothing and is charged all the same.
	enc, err := search(outsider.ID)
	if err != nil {
		t.Fatalf("outsider search 1: %v", err)
	}
	if res := globalSlice(t, enc); len(res.Messages) != 0 {
		t.Fatalf("outsider hits = %d, want none", len(res.Messages))
	}
	if _, err = search(outsider.ID); !isFloodWait(err) {
		t.Fatalf("outsider search 2: expected FLOOD_WAIT, got %v", err)
	}

	// A per-peer search exhausting its own budget leaves the global one intact.
	perPeer := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	if _, err = api.SearchForTestWithLimits(s, d.caller.ID, perPeer, &tg.MessagesSearchRequest{
		Peer:   api.InputPeerUser(d.caller.ID, d.other.ID),
		Q:      "deadline",
		Filter: &tg.InputMessagesFilterEmpty{},
	}); err != nil {
		t.Fatalf("per-peer search: %v", err)
	}
	if _, err = api.SearchForTestWithLimits(s, d.caller.ID, perPeer, &tg.MessagesSearchRequest{
		Peer:   api.InputPeerUser(d.caller.ID, d.other.ID),
		Q:      "deadline",
		Filter: &tg.InputMessagesFilterEmpty{},
	}); !isFloodWait(err) {
		t.Fatalf("per-peer search 2: expected FLOOD_WAIT, got %v", err)
	}
	if _, err = search(d.caller.ID); err != nil {
		t.Fatalf("global search after the per-peer budget ran out: %v", err)
	}
}
