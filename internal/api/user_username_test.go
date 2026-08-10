package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// divergeUserUsername claims handle for userID through the RPC that writes both
// the usernames row and the denormalized users.username copy, then overwrites
// only the copy. The two are written in one transaction today, so a divergence
// has to be forced: what is under test is that a reader takes the handle from
// the usernames row, which stays authoritative whatever the copy says.
func divergeUserUsername(t *testing.T, s *store.Store, dsn string, userID int64, handle, stale string) {
	t.Helper()
	if _, err := api.UpdateUsernameForTest(s, userID, handle); err != nil {
		t.Fatalf("claim username: %v", err)
	}
	execDB(t, dsn, `UPDATE users SET username = $2 WHERE id = $1`, userID, stale)
}

// TestGetUsersReportsAuthoritativeUsername covers users.getUsers, which loads
// the caller by id.
func TestGetUsersReportsAuthoritativeUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	user, err := s.CreateUser(ctx, "15550014001")
	if err != nil {
		t.Fatal(err)
	}
	divergeUserUsername(t, s, dsn, user.ID, "realhandle", "stalehandle")

	res, err := api.GetUsersForTest(s, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertEncodes(t, res)
	vec, ok := res.(*tg.UserClassVector)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := vec.Elems[0].(*tg.User)
	if !ok {
		t.Fatalf("elems[0] is not *tg.User: %T", vec.Elems[0])
	}
	if got.Username != "realhandle" {
		t.Errorf("users.getUsers username = %q, want %q (the usernames row, not the copy)",
			got.Username, "realhandle")
	}
}

// TestResolvePhoneReportsAuthoritativeUsername covers contacts.resolvePhone,
// which loads the target by phone.
func TestResolvePhoneReportsAuthoritativeUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550014011")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550014012")
	if err != nil {
		t.Fatal(err)
	}
	divergeUserUsername(t, s, dsn, target.ID, "realhandle", "stalehandle")

	res, err := api.ResolvePhoneForTest(s, caller.ID, &tg.ContactsResolvePhoneRequest{Phone: "15550014012"})
	if err != nil {
		t.Fatal(err)
	}
	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := peer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer.Users[0])
	}
	if got.Username != "realhandle" {
		t.Errorf("contacts.resolvePhone username = %q, want %q (the usernames row, not the copy)",
			got.Username, "realhandle")
	}
}

// TestResolveUsernameReportsAuthoritativeUserHandle covers
// contacts.resolveUsername for a user peer: the handle that admitted the user
// to the result is the one reported back, never the copy.
func TestResolveUsernameReportsAuthoritativeUserHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550014021")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "15550014022")
	if err != nil {
		t.Fatal(err)
	}
	divergeUserUsername(t, s, dsn, target.ID, "realhandle", "stalehandle")

	res, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "realhandle"})
	if err != nil {
		t.Fatal(err)
	}
	peer, ok := res.(*tg.ContactsResolvedPeer)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := peer.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", peer.Users[0])
	}
	if got.Username != "realhandle" {
		t.Errorf("contacts.resolveUsername username = %q, want %q (the usernames row, not the copy)",
			got.Username, "realhandle")
	}
}

// TestContactsSearchReportsAuthoritativeUsername covers contacts.search, whose
// contact arm loads dialog partners by name.
func TestContactsSearchReportsAuthoritativeUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	caller, err := s.CreateUser(ctx, "15550014031")
	if err != nil {
		t.Fatal(err)
	}
	partner, err := s.CreateUser(ctx, "15550014032")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetUserFirstNameForTest(dsn, partner.ID, "Authoritative"); err != nil {
		t.Fatalf("set name: %v", err)
	}
	if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(caller.ID, partner.ID),
		Message:  "hello",
		RandomID: 1,
	}); err != nil {
		t.Fatalf("send message: %v", err)
	}
	divergeUserUsername(t, s, dsn, partner.ID, "realhandle", "stalehandle")

	res, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: "Authoritative", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	found, ok := res.(*tg.ContactsFound)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := found.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("users[0] is not *tg.User: %T", found.Users[0])
	}
	if got.Username != "realhandle" {
		t.Errorf("contacts.search username = %q, want %q (the usernames row, not the copy)",
			got.Username, "realhandle")
	}
}

// TestGetUsersReportsNoUsernameAfterRelease is the case the divergence exists
// to guard: the usernames row is gone but a writer left the copy behind. The
// account no longer holds the handle, so no RPC may keep reporting it.
func TestGetUsersReportsNoUsernameAfterRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	user, err := s.CreateUser(ctx, "15550014041")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.UpdateUsernameForTest(s, user.ID, "releasedhandle"); err != nil {
		t.Fatalf("claim username: %v", err)
	}
	if _, err := api.UpdateUsernameForTest(s, user.ID, ""); err != nil {
		t.Fatalf("release username: %v", err)
	}
	execDB(t, dsn, `UPDATE users SET username = $2 WHERE id = $1`, user.ID, "releasedhandle")

	res, err := api.GetUsersForTest(s, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	vec, ok := res.(*tg.UserClassVector)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := vec.Elems[0].(*tg.User)
	if !ok {
		t.Fatalf("elems[0] is not *tg.User: %T", vec.Elems[0])
	}
	if got.Username != "" {
		t.Errorf("users.getUsers username = %q after release, want empty", got.Username)
	}
}

// TestGetUsersReportsUsernameWhenRowAndCopyAgree pins the unchanged case: a
// handle claimed normally, with no divergence forced, still reaches the wire.
func TestGetUsersReportsUsernameWhenRowAndCopyAgree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	user, err := s.CreateUser(ctx, "15550014051")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.UpdateUsernameForTest(s, user.ID, "agreedhandle"); err != nil {
		t.Fatalf("claim username: %v", err)
	}

	res, err := api.GetUsersForTest(s, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	vec, ok := res.(*tg.UserClassVector)
	if !ok {
		t.Fatalf("unexpected response type: %T", res)
	}
	got, ok := vec.Elems[0].(*tg.User)
	if !ok {
		t.Fatalf("elems[0] is not *tg.User: %T", vec.Elems[0])
	}
	if got.Username != "agreedhandle" {
		t.Errorf("users.getUsers username = %q, want %q", got.Username, "agreedhandle")
	}
}
