package api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestContactsSearchUnauthenticated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	_, err = api.ContactsSearchForTest(s, 0, &tg.ContactsSearchRequest{Q: "alice"})
	if err == nil {
		t.Fatal("unauthenticated request not refused")
	}
	if !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Errorf("got %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

func TestContactsSearchEmptyQuery(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	_, err = api.ContactsSearchForTest(s, 1, &tg.ContactsSearchRequest{Q: ""})
	if err == nil {
		t.Fatal("empty query not refused")
	}
	if !tgerr.Is(err, "SEARCH_QUERY_EMPTY") {
		t.Errorf("got %v, want SEARCH_QUERY_EMPTY", err)
	}
}

func TestContactsSearchLimitDefault(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000201")
	if err != nil {
		t.Fatal(err)
	}

	// Create 60 partners with matching first name "Alice".
	for i := range 60 {
		phone := fmt.Sprintf("155500002%03d", i)
		_, err := s.CreateUser(context.Background(), phone)
		if err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		partner, ok, err := s.UserByPhone(context.Background(), phone)
		if err != nil || !ok {
			t.Fatalf("load user %d: ok=%v err=%v", i, ok, err)
		}
		if err := api.SetUserFirstNameForTest(dsn, partner.ID, "Alice"); err != nil {
			t.Fatalf("set name %d: %v", i, err)
		}
		// Establish dialog: caller sends a message to partner.
		if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
			Peer:     api.InputPeerUser(caller.ID, partner.ID),
			Message:  "hello",
			RandomID: int64(i + 1),
		}); err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
	}

	// Limit=0 should default to 10.
	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "alice", Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	if len(found.MyResults) != 10 {
		t.Errorf("Limit=0: MyResults len = %d, want 10 (default)", len(found.MyResults))
	}
}

func TestContactsSearchLimitCap(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000301")
	if err != nil {
		t.Fatal(err)
	}

	// Create 60 partners with matching first name "Bob".
	for i := range 60 {
		phone := fmt.Sprintf("155500003%03d", i)
		_, err := s.CreateUser(context.Background(), phone)
		if err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		partner, ok, err := s.UserByPhone(context.Background(), phone)
		if err != nil || !ok {
			t.Fatalf("load user %d: ok=%v err=%v", i, ok, err)
		}
		if err := api.SetUserFirstNameForTest(dsn, partner.ID, "Bob"); err != nil {
			t.Fatalf("set name %d: %v", i, err)
		}
		// Establish dialog: caller sends a message to partner.
		if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
			Peer:     api.InputPeerUser(caller.ID, partner.ID),
			Message:  "hello",
			RandomID: int64(i + 1),
		}); err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
	}

	// Limit=999 should be capped to 50.
	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "bob", Limit: 999})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	if len(found.MyResults) != 50 {
		t.Errorf("Limit=999: MyResults len = %d, want 50 (cap)", len(found.MyResults))
	}
}

func TestContactsSearchQueryTooLong(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000104")
	if err != nil {
		t.Fatal(err)
	}

	// Query over 256 bytes should be rejected.
	longQuery := make([]byte, 257)
	for i := range longQuery {
		longQuery[i] = 'a'
	}
	_, err = api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: string(longQuery)})
	if err == nil {
		t.Fatal("over-length query not refused")
	}
	if !tgerr.Is(err, "SEARCH_QUERY_TOO_LONG") {
		t.Errorf("got %v, want SEARCH_QUERY_TOO_LONG", err)
	}
}

func TestContactsSearchNoMatch(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000103")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "nonexistent"})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	if len(found.MyResults) != 0 {
		t.Errorf("expected no results, got %d", len(found.MyResults))
	}
	if len(found.Results) != 0 {
		t.Errorf("expected empty Results, got %d", len(found.Results))
	}
}
