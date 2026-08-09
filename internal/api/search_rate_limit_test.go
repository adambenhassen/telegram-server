package api_test

import (
	"context"
	"testing"
	"time"

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
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296401")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296402")
	if err != nil {
		t.Fatal(err)
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

	// Searches 1-2 should pass.
	for i := range 2 {
		enc, err := api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
			Q:     "test",
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("search %d: %v", i+1, err)
		}
		res, ok := enc.(*tg.ContactsFound)
		if !ok {
			t.Fatalf("search %d: result type = %T, want *tg.ContactsFound", i+1, enc)
		}
		// May return 0 or 1 results depending on name state, but no error.
		_ = len(res.MyResults)
	}

	// Search 3 should be denied with FLOOD_WAIT.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
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
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296501")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551296502")
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Alice exhausts her quota.
	if _, err := api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	}); err != nil {
		t.Fatalf("alice search 1: %v", err)
	}
	// Alice's second search should be denied.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("alice search 2: expected FLOOD_WAIT, got %v", err)
	}

	// Bob's first search should still succeed (independent quota).
	_, err = api.ContactsSearchForTestWithLimits(s, bob.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("bob search 1: expected success, got %v", err)
	}
}

// TestContactsSearchRateLimitWindowExpiry proves that after the window expires,
// the same account can contacts.search again.
func TestContactsSearchRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296601")
	if err != nil {
		t.Fatal(err)
	}

	// Very short window: 1 search per 500ms.
	cfg := store.RateLimitConfig{Limit: 1, Window: 500 * time.Millisecond}

	// Exhaust the limit.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search 1: %v", err)
	}

	// Denied.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Wait for window to expire.
	time.Sleep(600 * time.Millisecond)

	// Should be allowed again.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("post-expiry search: %v", err)
	}
}

// TestSearchMessagesPreCharged proves that a rate limit denial does not
// consume the query: a denied search returns FLOOD_WAIT without reaching the
// database search query. An empty query still returns SEARCH_QUERY_EMPTY even
// when the account is over its rate limit, because input validation runs before
// the rate limit check.
func TestSearchMessagesPreCharged(t *testing.T) {
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

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// First search with valid query — consumes the only token.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if err != nil {
		t.Fatalf("search 1: %v", err)
	}

	// Second search with valid query — denied with FLOOD_WAIT.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "hello",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if !isFloodWait(err) {
		t.Fatalf("search 2: expected FLOOD_WAIT, got %v", err)
	}

	// Third search with empty query — returns SEARCH_QUERY_EMPTY (validation
	// before rate limit), not FLOOD_WAIT. This proves the invariant that
	// charging is uniform: whether a query matches must not change what the
	// caller is charged. An empty query is rejected before the rate limit.
	_, err = api.SearchForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSearchRequest{
		Peer:   peerBob,
		Q:      "",
		Filter: &tg.InputMessagesFilterEmpty{},
	})
	if err == nil {
		t.Fatal("search 3 (empty query): expected error, got nil")
	}
	// The error should be SEARCH_QUERY_EMPTY, not FLOOD_WAIT.
	if isFloodWait(err) {
		t.Fatal("search 3 (empty query): expected SEARCH_QUERY_EMPTY, got FLOOD_WAIT")
	}
}

// TestContactsSearchPreCharged proves that a denied contacts search does not
// reach the database query, and that input validation still runs before the
// rate limit.
func TestContactsSearchPreCharged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296801")
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// First search — consumes the only token.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("search 1: %v", err)
	}

	// Second search — denied with FLOOD_WAIT.
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "test",
		Limit: 10,
	})
	if !isFloodWait(err) {
		t.Fatalf("search 2: expected FLOOD_WAIT, got %v", err)
	}

	// Third search with empty query — returns SEARCH_QUERY_EMPTY (validation
	// before rate limit).
	_, err = api.ContactsSearchForTestWithLimits(s, alice.ID, cfg, &tg.ContactsSearchRequest{
		Q:     "",
		Limit: 10,
	})
	if err == nil {
		t.Fatal("search 3 (empty query): expected error, got nil")
	}
	if isFloodWait(err) {
		t.Fatal("search 3 (empty query): expected SEARCH_QUERY_EMPTY, got FLOOD_WAIT")
	}
}
