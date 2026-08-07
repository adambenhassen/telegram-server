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

func TestResolveUsernameUnauthenticated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	_, err = api.ResolveUsernameForTest(s, 0, &tg.ContactsResolveUsernameRequest{Username: "alice"})
	if err == nil {
		t.Fatal("unauthenticated request not refused")
	}
	if !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Errorf("got %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

func TestResolveUsernameEmpty(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: ""})
	if err == nil {
		t.Fatal("empty username not refused")
	}
	if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
		t.Errorf("got %v, want USERNAME_NOT_OCCUPIED", err)
	}
}

func TestResolveUserHit(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
	if err := s.UpdateUsername(context.Background(), target.ID, "alice"); err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "alice"})
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
		t.Error("phone must not be emitted for resolved user")
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

func TestResolveUserLeadingAt(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
	if err := s.UpdateUsername(context.Background(), target.ID, "alice"); err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "@alice"})
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
}

func TestResolveUserCaseInsensitive(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
	if err := s.UpdateUsername(context.Background(), target.ID, "alice"); err != nil {
		t.Fatal(err)
	}

	for _, username := range []string{"Alice", "ALICE", "@Alice"} {
		res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: username})
		if err != nil {
			t.Errorf("lookup %q: %v", username, err)
			continue
		}
		peer, ok := res.(*tg.ContactsResolvedPeer)
		if !ok {
			t.Errorf("lookup %q: unexpected response type: %T", username, res)
			continue
		}
		u, ok := peer.Users[0].(*tg.User)
		if !ok {
			t.Errorf("lookup %q: users[0] is not *tg.User: %T", username, peer.Users[0])
			continue
		}
		if u.ID != target.ID {
			t.Errorf("lookup %q: user id = %d, want %d", username, u.ID, target.ID)
		}
	}
}

func TestResolveUserMiss(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "nobody"})
	if err == nil {
		t.Fatal("miss did not return error")
	}
	if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
		t.Errorf("got %v, want USERNAME_NOT_OCCUPIED", err)
	}
}

func TestResolveUserClearedUsername(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
	if err := s.UpdateUsername(context.Background(), target.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	// Clear the username.
	if err := s.UpdateUsername(context.Background(), target.ID, ""); err != nil {
		t.Fatal(err)
	}

	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "alice"})
	if err == nil {
		t.Fatal("cleared username did not return error")
	}
	if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
		t.Errorf("got %v, want USERNAME_NOT_OCCUPIED", err)
	}
}

func TestResolveChannelHit(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := s.CreateUser(context.Background(), "15550000002")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(context.Background(), creator.ID, "Public Channel", "About", false)
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "publicchannel"})
	if err != nil {
		t.Fatal(err)
	}

	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}

	if len(peer.Chats) != 1 {
		t.Fatalf("expected 1 chat, got %d", len(peer.Chats))
	}

	pc, ok := peer.Peer.(*tg.PeerChannel)
	if !ok {
		t.Fatalf("peer is not PeerChannel: %T", peer.Peer)
	}
	if pc.ChannelID != ch.ID {
		t.Errorf("peer channel id = %d, want %d", pc.ChannelID, ch.ID)
	}
}

func TestResolveChannelMiss(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "nonesuch"})
	if err == nil {
		t.Fatal("miss did not return error")
	}
	if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
		t.Errorf("got %v, want USERNAME_NOT_OCCUPIED", err)
	}
}

func TestResolveUsernameQuota(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust quota with distinct usernames (UsernameLookupLimit = 100).
	for i := range store.UsernameLookupLimit {
		username := fmt.Sprintf("user%09d", i)
		_, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: username})
		// Misses are expected (USERNAME_NOT_OCCUPIED), but each charges the quota.
		if err == nil {
			t.Fatalf("lookup %d should have failed (miss)", i)
		}
		if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
			t.Fatalf("lookup %d: got %v, want USERNAME_NOT_OCCUPIED", i, err)
		}
	}

	// 101st lookup should hit quota.
	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "user999999999"})
	if err == nil {
		t.Fatal("quota exceeded did not return error")
	}
	if !tgerr.Is(err, "FLOOD_WAIT") {
		t.Errorf("got %v, want FLOOD_WAIT", err)
	}
}

func TestResolveUsernameQuotaRetrySame(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Fill quota with distinct usernames.
	for i := range store.UsernameLookupLimit {
		username := fmt.Sprintf("user%09d", i)
		_, _ = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: username}) //nolint:errcheck // error expected (miss)
	}

	// Retry the same username as loop iteration 0 — distinct count stays at 100,
	// so quota passes and the miss path returns USERNAME_NOT_OCCUPIED.
	retryUsername := fmt.Sprintf("user%09d", 0)
	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: retryUsername})
	if err == nil {
		t.Fatal("retry of same username did not fail")
	}
	if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
		t.Errorf("got %v, want USERNAME_NOT_OCCUPIED", err)
	}
}

func TestResolveUsernameQuotaChargesOnMiss(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust quota with nonexistent usernames — each miss still charges.
	for i := range store.UsernameLookupLimit {
		username := fmt.Sprintf("ghost%09d", i)
		_, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: username})
		if err == nil {
			t.Fatalf("lookup %d should have failed (miss)", i)
		}
		if !tgerr.Is(err, "USERNAME_NOT_OCCUPIED") {
			t.Fatalf("lookup %d: got %v, want USERNAME_NOT_OCCUPIED", i, err)
		}
	}

	// Next distinct miss is rate-limited — quota was charged on every miss.
	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "ghost999999999"})
	if err == nil {
		t.Fatal("quota not charged on miss")
	}
	if !tgerr.Is(err, "FLOOD_WAIT") {
		t.Errorf("got %v, want FLOOD_WAIT", err)
	}
}

func TestResolveUsernameBurstCap(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	caller, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Burst cap is 20 per minute. The 21st distinct lookup in the same minute
	// should be rate-limited regardless of the 24-hour counter.
	for i := range store.UsernameLookupBurstLimit {
		username := fmt.Sprintf("burst%09d", i)
		_, _ = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: username}) //nolint:errcheck // miss expected
	}

	// 21st distinct lookup in same minute should be rate-limited.
	_, err = api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "burst999999999"})
	if err == nil {
		t.Fatal("burst cap did not return error")
	}
	if !tgerr.Is(err, "FLOOD_WAIT") {
		t.Errorf("got %v, want FLOOD_WAIT", err)
	}
}

func TestResolveUsernameAccessHash(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
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
	if err := s.UpdateUsername(context.Background(), target.ID, "alice"); err != nil {
		t.Fatal(err)
	}

	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "alice"})
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

	wantHash := api.DeriveUserHash(caller.ID, target.ID)
	if u.AccessHash != wantHash {
		t.Errorf("access_hash = %d, want %d (derived for viewer %d, peer %d)",
			u.AccessHash, wantHash, caller.ID, target.ID)
	}
}
