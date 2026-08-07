package api_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
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

	// Claim the username for this channel — no shipped RPC does this yet
	// (MAIN-181), so the test helper inserts the row directly.
	if err := api.ClaimChannelUsernameForTest(s, ch.ID, "publicchannel"); err != nil {
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

	// The channel should be rendered as tg.Channel (not ChannelForbidden).
	chatCh, ok := peer.Chats[0].(*tg.Channel)
	if !ok {
		t.Fatalf("chats[0] is not *tg.Channel: %T", peer.Chats[0])
	}
	if chatCh.ID != ch.ID {
		t.Errorf("channel id = %d, want %d", chatCh.ID, ch.ID)
	}
	if chatCh.Title != "Public Channel" {
		t.Errorf("title = %q, want %q", chatCh.Title, "Public Channel")
	}
	if !chatCh.Broadcast {
		t.Error("broadcast should be true for a channel")
	}
	if chatCh.Megagroup {
		t.Error("megagroup should be false for a channel")
	}
	_ = chatCh.Left // Left=true is correct: the caller is not a member.
	// Verify per-viewer access hash.
	wantHash := pgtest.PeerDeriver().Derive(caller.ID, peerhash.KindChannel, ch.ID)
	if chatCh.AccessHash != wantHash {
		t.Errorf("access_hash = %d, want %d", chatCh.AccessHash, wantHash)
	}
	// Verify photo is ChatPhotoEmpty.
	if _, ok := chatCh.Photo.(*tg.ChatPhotoEmpty); !ok {
		t.Errorf("photo is not ChatPhotoEmpty: %T", chatCh.Photo)
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

// TestResolveUsernameBurstCap uses the store-level API to exercise the
// per-minute burst cap (20 distinct handles). It is fast because it stays
// within the burst window.
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
	// should be rate-limited.
	for i := range store.UsernameLookupBurstLimit {
		username := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if err := s.CheckAndChargeUsernameLookup(context.Background(), caller.ID, username); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}

	// 21st distinct lookup in same minute should be rate-limited.
	err = s.CheckAndChargeUsernameLookup(context.Background(), caller.ID, "zz")
	if err == nil {
		t.Fatal("burst cap did not return error")
	}
	if !errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
		t.Errorf("got %v, want ErrUsernameLookupQuotaExceeded", err)
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
