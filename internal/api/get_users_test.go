package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/store"
)

func getUsersForTest(t *testing.T, s *store.Store, viewerID int64, ids ...tg.InputUserClass) *tg.UserClassVector {
	t.Helper()
	res, err := api.GetUsersForTestWithRequest(s, viewerID, &tg.UsersGetUsersRequest{ID: ids})
	if err != nil {
		t.Fatalf("users.getUsers: %v", err)
	}
	vec, ok := res.(*tg.UserClassVector)
	if !ok {
		t.Fatalf("users.getUsers result type = %T, want *tg.UserClassVector", res)
	}
	assertEncodes(t, res)
	return vec
}

func getUsersErrorBytes(t *testing.T, s *store.Store, viewerID int64, input tg.InputUserClass) []byte {
	t.Helper()
	_, err := api.GetUsersForTestWithRequest(s, viewerID, &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{input}})
	if err == nil {
		t.Fatal("users.getUsers: expected error, got nil")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("users.getUsers error = %v, want RPC error", err)
	}
	return fmt.Appendf(nil, "%d\x00%s\x00%s\x00%d", rpc.Code, rpc.Type, rpc.Message, rpc.Argument)
}

// TestGetUsersRefreshesExactSearchBeforeDialog demonstrates the reported
// regression in both directions: exact search gives the caller a handle and a
// viewer-valid reference, and getUsers must refresh that same user before the
// first message has established a dialog.
func TestGetUsersRefreshesExactSearchBeforeDialog(t *testing.T) {
	for _, tc := range []struct {
		name            string
		callerName      string
		targetName      string
		targetFirstName string
		query           string
		handle          string
	}{
		{name: "test1 to operator", callerName: "test1", targetName: "operator", targetFirstName: "Operator", query: "@operator", handle: "operator"},
		{name: "operator to test1", callerName: "operator", targetName: "test1", targetFirstName: "Test1 User", query: "@test1", handle: "test1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			caller, err := s.CreateUser(ctx, "15551290001")
			if err != nil {
				t.Fatal(err)
			}
			target, err := s.CreateUser(ctx, "15551290002")
			if err != nil {
				t.Fatal(err)
			}
			if err := api.SetUserFirstNameForTest(dsn, target.ID, tc.targetFirstName); err != nil {
				t.Fatal(err)
			}
			if err := api.ClaimUsernameForTest(s, caller.ID, tc.callerName); err != nil {
				t.Fatal(err)
			}
			if err := api.ClaimUsernameForTest(s, target.ID, tc.targetName); err != nil {
				t.Fatal(err)
			}

			before, err := s.History(ctx, caller.ID, store.PeerTypeUser, target.ID, 0, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 0 {
				t.Fatalf("history before search has %d messages, want no dialog", len(before))
			}

			found, err := api.ContactsSearchForTest(s, caller.ID, &tg.ContactsSearchRequest{Q: tc.query, Limit: 10})
			if err != nil {
				t.Fatalf("contacts.search(%q): %v", tc.query, err)
			}
			search, ok := found.(*tg.ContactsFound)
			if !ok || len(search.Results) != 1 || len(search.Users) != 1 {
				t.Fatalf("contacts.search(%q) = %#v, want one exact user", tc.query, found)
			}
			searchUser, ok := search.Users[0].(*tg.User)
			if !ok {
				t.Fatalf("contacts.search user type = %T, want *tg.User", search.Users[0])
			}
			if searchUser.ID != target.ID || searchUser.Username != tc.handle || searchUser.AccessHash != api.DeriveUserHash(caller.ID, target.ID) {
				t.Fatalf("contacts.search user = {id:%d username:%q access_hash:%d}, want target %q with viewer hash",
					searchUser.ID, searchUser.Username, searchUser.AccessHash, tc.handle)
			}

			historyEnc, err := api.GetHistoryForTest(s, caller.ID, &tg.MessagesGetHistoryRequest{
				Peer:  api.InputPeerUser(caller.ID, target.ID),
				Limit: 30,
			})
			if err != nil {
				t.Fatalf("getHistory before dialog: %v", err)
			}
			history, ok := historyEnc.(*tg.MessagesMessages)
			if !ok {
				t.Fatalf("getHistory before dialog = %T, want *tg.MessagesMessages", historyEnc)
			}
			if len(history.Messages) != 0 {
				t.Fatalf("getHistory before dialog messages = %d, want 0", len(history.Messages))
			}
			historyUser, ok := loadUsersWire(t, history.Users, target.ID).(*tg.User)
			if !ok {
				t.Fatalf("getHistory user = %T, want searched user %d", loadUsersWire(t, history.Users, target.ID), target.ID)
			}
			if historyUser.FirstName != tc.targetFirstName || historyUser.Username != tc.handle || historyUser.AccessHash != api.DeriveUserHash(caller.ID, target.ID) || historyUser.Self || historyUser.Phone != "" || historyUser.Status == nil {
				t.Errorf("getHistory user = {first_name:%q username:%q access_hash:%d self:%t phone:%q status:%T}, want public profile for %q without phone",
					historyUser.FirstName, historyUser.Username, historyUser.AccessHash, historyUser.Self, historyUser.Phone, historyUser.Status, tc.handle)
			}
			missingID := target.ID + 1_000_000
			missingEnc, err := api.GetHistoryForTest(s, caller.ID, &tg.MessagesGetHistoryRequest{
				Peer:  api.InputPeerUser(caller.ID, missingID),
				Limit: 30,
			})
			if err != nil {
				t.Fatalf("getHistory for missing account: %v", err)
			}
			missingHistory, ok := missingEnc.(*tg.MessagesMessages)
			if !ok {
				t.Fatalf("missing-account history = %T, want *tg.MessagesMessages", missingEnc)
			}
			if len(missingHistory.Messages) != 0 {
				t.Fatalf("missing-account history messages = %d, want 0", len(missingHistory.Messages))
			}
			for _, user := range missingHistory.Users {
				switch got := user.(type) {
				case *tg.User:
					if got.ID == missingID {
						t.Fatalf("missing account %d unexpectedly has a live profile", missingID)
					}
				case *tg.UserEmpty:
					if got.ID == missingID {
						t.Fatalf("missing account %d unexpectedly has a profile placeholder", missingID)
					}
				}
			}

			lookups, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatalf("open username lookup pool: %v", err)
			}
			t.Cleanup(lookups.Close)
			var lookupCount int
			if err := lookups.QueryRow(ctx, "SELECT count(*) FROM username_lookups WHERE caller_id = $1 AND handle = $2", caller.ID, tc.handle).Scan(&lookupCount); err != nil {
				t.Fatalf("count search lookup: %v", err)
			}
			if lookupCount != 1 {
				t.Fatalf("search lookup count = %d, want 1", lookupCount)
			}

			refreshed := getUsersForTest(t, s, caller.ID, &tg.InputUser{UserID: target.ID, AccessHash: searchUser.AccessHash})
			if len(refreshed.Elems) != 1 {
				t.Fatalf("users.getUsers elements = %d, want 1", len(refreshed.Elems))
			}
			refreshedUser, ok := refreshed.Elems[0].(*tg.User)
			if !ok {
				t.Fatalf("users.getUsers element = %T, want the searched user", refreshed.Elems[0])
			}
			if refreshedUser.ID != target.ID || refreshedUser.Username != tc.handle || refreshedUser.FirstName != tc.targetFirstName || refreshedUser.Self || refreshedUser.Phone != "" {
				t.Errorf("users.getUsers user = {id:%d username:%q first_name:%q self:%t phone:%q}, want public target %q with first name %q",
					refreshedUser.ID, refreshedUser.Username, refreshedUser.FirstName, refreshedUser.Self, refreshedUser.Phone, tc.handle, tc.targetFirstName)
			}

			if err := lookups.QueryRow(ctx, "SELECT count(*) FROM username_lookups WHERE caller_id = $1 AND handle = $2", caller.ID, tc.handle).Scan(&lookupCount); err != nil {
				t.Fatalf("count lookup after refresh: %v", err)
			}
			if lookupCount != 1 {
				t.Errorf("lookup count after getUsers = %d, want 1 (refresh must not charge username lookup quota)", lookupCount)
			}

			if _, err := api.SendMessageForTest(s, caller.ID, &tg.MessagesSendMessageRequest{
				Peer:     &tg.InputPeerUser{UserID: target.ID, AccessHash: historyUser.AccessHash},
				Message:  "first message",
				RandomID: 1,
			}); err != nil {
				t.Fatalf("first message to exact-search result: %v", err)
			}
			received, err := s.History(ctx, target.ID, store.PeerTypeUser, caller.ID, 0, 10)
			if err != nil {
				t.Fatalf("recipient history: %v", err)
			}
			if len(received) != 1 || received[0].Text != "first message" {
				t.Fatalf("recipient history = %#v, want the first message", received)
			}
		})
	}
}

func TestGetHistoryRejectsInvalidUserPeersBeforeStorage(t *testing.T) {
	viewerID := int64(1)
	otherViewerID := int64(2)
	existingID := int64(3)
	missingID := int64(1_000_003)
	tests := []struct {
		name string
		peer *tg.InputPeerUser
	}{
		{name: "wrong hash for existing id", peer: &tg.InputPeerUser{UserID: existingID, AccessHash: api.DeriveUserHash(viewerID, existingID) + 1}},
		{name: "wrong hash for missing id", peer: &tg.InputPeerUser{UserID: missingID, AccessHash: api.DeriveUserHash(viewerID, missingID) + 1}},
		{name: "zero hash for existing id", peer: &tg.InputPeerUser{UserID: existingID}},
		{name: "zero hash for missing id", peer: &tg.InputPeerUser{UserID: missingID}},
		{name: "another viewer's hash", peer: api.InputPeerUser(otherViewerID, existingID)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := api.GetHistoryForTest(nil, viewerID, &tg.MessagesGetHistoryRequest{Peer: tt.peer})
			if got := rpcMessage(t, err); got != "PEER_ID_INVALID" {
				t.Fatalf("error = %s, want PEER_ID_INVALID", got)
			}
		})
	}
}

func TestGetUsersRendersViewerContactState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	owner, err := s.CreateUser(ctx, "15551290011")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := s.CreateUser(ctx, "15551290012")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateUser(ctx, "15551290013")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddContact(ctx, owner.ID, peer.ID); err != nil {
		t.Fatal(err)
	}
	input := api.InputUser(owner.ID, peer.ID)

	oneSided := getUsersForTest(t, s, owner.ID, input)
	ownerPeer, ok := oneSided.Elems[0].(*tg.User)
	if !ok || !ownerPeer.Contact || ownerPeer.MutualContact {
		t.Fatalf("one-sided peer = %#v, want contact=true mutual_contact=false", oneSided.Elems[0])
	}

	if _, err := s.AddContact(ctx, peer.ID, owner.ID); err != nil {
		t.Fatal(err)
	}
	mutual := getUsersForTest(t, s, owner.ID, input)
	mutualPeer, ok := mutual.Elems[0].(*tg.User)
	if !ok || !mutualPeer.Contact || !mutualPeer.MutualContact {
		t.Fatalf("mutual peer = %#v, want contact=true mutual_contact=true", mutual.Elems[0])
	}

	otherPeer := getUsersForTest(t, s, other.ID, api.InputUser(other.ID, peer.ID))
	otherUser, ok := otherPeer.Elems[0].(*tg.User)
	if !ok || otherUser.Contact || otherUser.MutualContact {
		t.Fatalf("other viewer peer = %#v, want contact=false mutual_contact=false", otherPeer.Elems[0])
	}
}

func TestGetUsersRejectsInvalidReferencesWithoutExistenceOracle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "15551290101")
	if err != nil {
		t.Fatal(err)
	}
	otherViewer, err := s.CreateUser(ctx, "15551290102")
	if err != nil {
		t.Fatal(err)
	}
	existing, err := s.CreateUser(ctx, "15551290103")
	if err != nil {
		t.Fatal(err)
	}
	missingID := existing.ID + 1_000_000

	badExisting := &tg.InputUser{UserID: existing.ID, AccessHash: api.DeriveUserHash(caller.ID, existing.ID) + 1}
	badMissing := &tg.InputUser{UserID: missingID, AccessHash: api.DeriveUserHash(caller.ID, missingID) + 1}
	cases := []struct {
		name  string
		input tg.InputUserClass
	}{
		{name: "wrong hash for existing id", input: badExisting},
		{name: "wrong hash for missing id", input: badMissing},
		{name: "other viewer hash", input: api.InputUser(otherViewer.ID, existing.ID)},
		{name: "zero hash", input: &tg.InputUser{UserID: existing.ID}},
		{name: "zero hash for missing id", input: &tg.InputUser{UserID: missingID}},
		{name: "inputUserEmpty", input: &tg.InputUserEmpty{}},
		{name: "inputUserFromMessage", input: &tg.InputUserFromMessage{Peer: &tg.InputPeerUser{UserID: existing.ID}, MsgID: 1, UserID: existing.ID}},
	}

	var want []byte
	for i, tc := range cases {
		got := getUsersErrorBytes(t, s, caller.ID, tc.input)
		if string(got) != "400\x00PEER_ID_INVALID\x00PEER_ID_INVALID\x000" {
			t.Errorf("%s error bytes = %q, want PEER_ID_INVALID", tc.name, got)
		}
		if i == 0 {
			want = got
		} else if !bytes.Equal(got, want) {
			t.Errorf("%s error bytes = %q, want byte-identical %q", tc.name, got, want)
		}
	}

	_, err = api.GetUsersForTestWithRequest(s, caller.ID, &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{
		api.InputUser(caller.ID, existing.ID),
		badMissing,
	}})
	if err == nil || rpcMessage(t, err) != "PEER_ID_INVALID" {
		t.Fatalf("valid reference followed by bad hash error = %v, want whole-call PEER_ID_INVALID", err)
	}
}

func TestGetUsersPreservesOrderAndSelfPrivacy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "15551290201")
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := s.CreateUser(ctx, "15551290202")
	if err != nil {
		t.Fatal(err)
	}
	last, err := s.CreateUser(ctx, "15551290203")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, caller.ID, "caller"); err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, stranger.ID, "stranger"); err != nil {
		t.Fatal(err)
	}
	missingID := last.ID + 1_000_000

	vec := getUsersForTest(t, s, caller.ID,
		api.InputUser(caller.ID, last.ID),
		&tg.InputUserSelf{},
		api.InputUser(caller.ID, caller.ID),
		api.InputUser(caller.ID, stranger.ID),
		api.InputUser(caller.ID, missingID),
		api.InputUser(caller.ID, last.ID),
	)
	wantIDs := []int64{last.ID, caller.ID, caller.ID, stranger.ID, missingID, last.ID}
	if len(vec.Elems) != len(wantIDs) {
		t.Fatalf("users.getUsers elements = %d, want %d", len(vec.Elems), len(wantIDs))
	}
	for i, wantID := range wantIDs {
		switch got := vec.Elems[i].(type) {
		case *tg.User:
			if got.ID != wantID {
				t.Errorf("element %d id = %d, want %d", i, got.ID, wantID)
			}
			if wantID == caller.ID {
				if !got.Self || got.Phone != caller.Phone {
					t.Errorf("self element %d = {self:%t phone:%q}, want caller phone %q", i, got.Self, got.Phone, caller.Phone)
				}
			} else {
				if got.Self || got.Phone != "" {
					t.Errorf("stranger element %d = {self:%t phone:%q}, want no self flag or phone", i, got.Self, got.Phone)
				}
			}
		case *tg.UserEmpty:
			if wantID != missingID || got.ID != wantID {
				t.Errorf("element %d userEmpty id = %d, want user %d", i, got.ID, wantID)
			}
		default:
			t.Errorf("element %d type = %T, want *tg.User or missing *tg.UserEmpty", i, vec.Elems[i])
		}
	}

	gotStranger, ok := vec.Elems[3].(*tg.User)
	if !ok {
		t.Fatalf("stranger element type = %T, want *tg.User", vec.Elems[3])
	}
	loadedStranger, ok, err := s.UserByID(ctx, stranger.ID)
	if err != nil || !ok {
		t.Fatalf("load stranger: found=%v err=%v", ok, err)
	}
	wantPublic := api.UserToTL(loadedStranger, caller.ID, false)
	assertEncodes(t, wantPublic)
	if !reflect.DeepEqual(gotStranger, wantPublic) {
		t.Errorf("stranger rendering = %#v, want the existing public view %#v", gotStranger, wantPublic)
	}
}

func TestGetUsersRejectsOverCapAndKeepsAuthenticationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "15551290301")
	if err != nil {
		t.Fatal(err)
	}

	tooMany := make([]tg.InputUserClass, 101)
	for i := range tooMany {
		tooMany[i] = api.InputUser(caller.ID, caller.ID)
	}
	if _, err := api.GetUsersForTestWithRequest(s, caller.ID, &tg.UsersGetUsersRequest{ID: tooMany}); err == nil || rpcMessage(t, err) != "LIMIT_INVALID" {
		t.Fatalf("101 requested users error = %v, want LIMIT_INVALID", err)
	}

	if _, err := api.GetUsersForTestWithRequest(s, 0, &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{&tg.InputUserSelf{}}}); err == nil || rpcMessage(t, err) != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unbound self request error = %v, want AUTH_KEY_UNREGISTERED", err)
	}
	if _, err := api.GetUsersForTestWithRequest(s, caller.ID+1_000_000, &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{&tg.InputUserSelf{}}}); err == nil || rpcMessage(t, err) != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("missing caller self request error = %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

func TestGetUsersReturnsUserEmptyForMissingViewerValidReference(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "15551290401")
	if err != nil {
		t.Fatal(err)
	}
	missingID := caller.ID + 1_000_000

	vec := getUsersForTest(t, s, caller.ID, api.InputUser(caller.ID, missingID))
	if len(vec.Elems) != 1 {
		t.Fatalf("users.getUsers elements = %d, want 1", len(vec.Elems))
	}
	got, ok := vec.Elems[0].(*tg.UserEmpty)
	if !ok || got.ID != missingID {
		t.Fatalf("users.getUsers missing result = %#v, want userEmpty{%d}", vec.Elems[0], missingID)
	}
}

func TestGetUsersBatchDedupePreservesRepeatedSlots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	caller, err := s.CreateUser(ctx, "15551290501")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := s.CreateUser(ctx, "15551290502")
	if err != nil {
		t.Fatal(err)
	}
	ref := api.InputUser(caller.ID, peer.ID)
	vec := getUsersForTest(t, s, caller.ID, ref, ref, ref)
	if len(vec.Elems) != 3 {
		t.Fatalf("users.getUsers elements = %d, want 3", len(vec.Elems))
	}
	for i, elem := range vec.Elems {
		got, ok := elem.(*tg.User)
		if !ok || got.ID != peer.ID {
			t.Errorf("element %d = %#v, want user %d", i, elem, peer.ID)
		}
	}
}
