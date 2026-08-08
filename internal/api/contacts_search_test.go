package api_test

import (
	"context"
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

	caller, err := s.CreateUser(context.Background(), "15550000101")
	if err != nil {
		t.Fatal(err)
	}

	// Limit=0 should default to 10 — no error, just an empty result.
	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "x", Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	if found == nil || len(found.MyResults) != 0 {
		t.Errorf("expected empty result, got %v", found)
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

	caller, err := s.CreateUser(context.Background(), "15550000102")
	if err != nil {
		t.Fatal(err)
	}

	// Limit=999 should be capped to 50 — no error, just an empty result.
	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "x", Limit: 999})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	if found == nil || len(found.MyResults) != 0 {
		t.Errorf("expected empty result, got %v", found)
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
