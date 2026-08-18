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

func TestResolvePhoneUnauthenticated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	_, err = api.ResolvePhoneForTest(s, 0, &tg.ContactsResolvePhoneRequest{Phone: "+15550001111"})
	if err == nil {
		t.Fatal("unauthenticated request not refused")
	}
	if !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Errorf("got %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

func TestResolvePhoneInvalidPhone(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	_, err = api.ResolvePhoneForTest(s, 1, &tg.ContactsResolvePhoneRequest{Phone: "abc"})
	if err == nil {
		t.Fatal("invalid phone accepted")
	}
	if !tgerr.Is(err, "PHONE_NUMBER_INVALID") {
		t.Errorf("got %v, want PHONE_NUMBER_INVALID", err)
	}
}

func TestResolvePhoneMiss(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "+15559999999"})
	if err == nil {
		t.Fatal("miss did not return error")
	}
	if !tgerr.Is(err, "PHONE_NOT_OCCUPIED") {
		t.Errorf("got %v, want PHONE_NOT_OCCUPIED", err)
	}
}

func TestResolvePhoneHit(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(context.Background(), "15550000002")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "+15550000002"})
	if err != nil {
		t.Fatal(err)
	}

	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}

	if len(peer.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(peer.Users))
	}
	u, ok := peer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer.Users[0])
	}
	if u.ID != target.ID {
		t.Errorf("user id = %d, want %d", u.ID, target.ID)
	}
	if u.Phone != "" {
		t.Error("phone must not be emitted for non-self user")
	}
	if u.Self {
		t.Error("self must be false for resolved peer")
	}

	pu, ok := peer.Peer.(*tg.PeerUser)
	if !ok {
		t.Fatalf("peer is not PeerUser: %T", peer.Peer)
	}
	if pu.UserID != target.ID {
		t.Errorf("peer user id = %d, want %d", pu.UserID, target.ID)
	}
}

func TestResolvePhoneQuota(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust quota with distinct phones (LookupLimit = 20).
	for i := range store.LookupLimit {
		phone := fmt.Sprintf("15551%09d", i)
		_, err := api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: phone})
		// Misses are expected (PHONE_NOT_OCCUPIED), but each charges the quota.
		if err == nil {
			t.Fatalf("lookup %d should have failed (miss)", i)
		}
		if !tgerr.Is(err, "PHONE_NOT_OCCUPIED") {
			t.Fatalf("lookup %d: got %v, want PHONE_NOT_OCCUPIED", i, err)
		}
	}

	// 21st lookup should hit quota.
	_, err = api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "15551999999"})
	if err == nil {
		t.Fatal("quota exceeded did not return error")
	}
	if !tgerr.Is(err, "FLOOD_WAIT") {
		t.Errorf("got %v, want FLOOD_WAIT", err)
	}
}

func TestResolvePhoneQuotaRetrySamePhone(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Fill quota with distinct phones.
	for i := range store.LookupLimit {
		phone := fmt.Sprintf("15551%09d", i)
		_, _ = api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: phone}) //nolint:errcheck // error expected (miss)
	}

	// Retry the same phone as loop iteration 0 — distinct count stays at 20,
	// so quota passes and the miss path returns PHONE_NOT_OCCUPIED.
	retryPhone := fmt.Sprintf("15551%09d", 0)
	_, err = api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: retryPhone})
	if err == nil {
		t.Fatal("retry of same phone did not fail")
	}
	if !tgerr.Is(err, "PHONE_NOT_OCCUPIED") {
		t.Errorf("got %v, want PHONE_NOT_OCCUPIED", err)
	}
}

func TestResolvePhoneSelf(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolvePhoneForTest(s, user.ID, &tg.ContactsResolvePhoneRequest{Phone: "+15550000001"})
	if err != nil {
		t.Fatal(err)
	}

	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}

	if len(peer.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(peer.Users))
	}
	u, ok := peer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer.Users[0])
	}
	if u.ID != user.ID {
		t.Errorf("user id = %d, want %d", u.ID, user.ID)
	}
	if u.Phone != "" {
		t.Error("phone must not be emitted even for self via resolvePhone")
	}
}

func TestResolvePhoneNormalization(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(context.Background(), "15550000002")
	if err != nil {
		t.Fatal(err)
	}

	// Lookup WITH leading '+' should find target stored without it.
	res, err := api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "+15550000002"})
	if err != nil {
		t.Fatal(err)
	}

	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	u, ok := peer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer.Users[0])
	}
	if u.ID != target.ID {
		t.Errorf("user id = %d, want %d", u.ID, target.ID)
	}

	// Lookup WITHOUT '+' should also find same target.
	res2, err := api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "15550000002"})
	if err != nil {
		t.Fatal(err)
	}

	peer2, ok := res2.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res2)
	}
	u2, ok := peer2.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer2.Users[0])
	}
	if u2.ID != target.ID {
		t.Errorf("user id = %d, want %d", u2.ID, target.ID)
	}
}
