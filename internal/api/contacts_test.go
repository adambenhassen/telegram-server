package api_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func contactUser(t *testing.T, s *store.Store, phone, username, first, last string) store.User {
	t.Helper()
	u := mustUser(t, s, phone)
	if _, err := api.UpdateProfileForTest(s, u.ID, &tg.AccountUpdateProfileRequest{FirstName: first, LastName: last}); err != nil {
		t.Fatalf("update profile %d: %v", u.ID, err)
	}
	if username != "" {
		if err := api.ClaimUsernameForTest(s, u.ID, username); err != nil {
			t.Fatalf("claim username %d: %v", u.ID, err)
		}
	}
	u, ok, err := s.UserByID(context.Background(), u.ID)
	if err != nil || !ok {
		t.Fatalf("reload user %d after profile setup: ok=%v err=%v", u.ID, ok, err)
	}
	return u
}

type contactActivity struct {
	pts     int
	events  []store.Event
	dialogs []store.Dialog
}

func contactActivityFor(t *testing.T, s *store.Store, userID int64) contactActivity {
	t.Helper()
	ctx := context.Background()
	state, err := s.State(ctx, userID)
	if err != nil {
		t.Fatalf("state for %d: %v", userID, err)
	}
	events, err := s.EventsWindow(ctx, userID, 0, state.Pts, 100)
	if err != nil {
		t.Fatalf("events for %d: %v", userID, err)
	}
	dialogs, err := s.Dialogs(ctx, userID, 0, 100)
	if err != nil {
		t.Fatalf("dialogs for %d: %v", userID, err)
	}
	return contactActivity{pts: state.Pts, events: events, dialogs: dialogs}
}

func assertContactAddReply(t *testing.T, enc bin.Encoder, target store.User, viewerID int64, wantMutual bool) *tg.User {
	t.Helper()
	assertEncodes(t, enc)
	updates, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("add contact result = %T, want *tg.Updates", enc)
	}
	if len(updates.Updates) != 1 || len(updates.Chats) != 0 || len(updates.Users) != 1 {
		t.Fatalf("add contact updates = %+v, want one peer-settings update, no chats, and one user", updates)
	}
	settingsUpdate, ok := updates.Updates[0].(*tg.UpdatePeerSettings)
	if !ok {
		t.Fatalf("add contact update = %T, want *tg.UpdatePeerSettings", updates.Updates[0])
	}
	peer, ok := settingsUpdate.Peer.(*tg.PeerUser)
	if !ok || peer.UserID != target.ID {
		t.Errorf("peer-settings peer = %#v, want user %d", settingsUpdate.Peer, target.ID)
	}
	user, ok := updates.Users[0].(*tg.User)
	if !ok {
		t.Fatalf("add contact user = %T, want *tg.User", updates.Users[0])
	}
	if user.ID != target.ID || user.AccessHash != api.DeriveUserHash(viewerID, target.ID) {
		t.Errorf("peer settings = {id:%d hash:%d}, want {id:%d viewer hash}", user.ID, user.AccessHash, target.ID)
	}
	if !user.Contact || user.MutualContact != wantMutual || user.Self {
		t.Errorf("contact flags = {contact:%v mutual:%v self:%v}, want {true %v false}", user.Contact, user.MutualContact, user.Self, wantMutual)
	}
	if user.Phone != "" {
		t.Errorf("contact response exposed phone %q", user.Phone)
	}
	if user.FirstName != target.FirstName || user.LastName != target.LastName {
		t.Errorf("contact name = %q %q, want global name %q %q", user.FirstName, user.LastName, target.FirstName, target.LastName)
	}
	return user
}

func TestContactsAddContactReturnsSafePeerAndIgnoresPhoneAndBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := contactUser(t, s, "+15551970001", "contactowner", "Owner Global", "Name")
	b := contactUser(t, s, "+15551970002", "contacttarget", "Target Global", "Name")

	type scenario struct {
		name, phone       string
		privacyException  bool
		ownerBlocksTarget bool
		targetBlocksOwner bool
	}
	scenarios := []scenario{
		{name: "plain", phone: ""},
		{name: "privacy exception and alternate phone", phone: "+19995550123", privacyException: true},
		{name: "second phone", phone: "anything else"},
		{name: "caller block and alternate phone", phone: "not a phone", privacyException: true, ownerBlocksTarget: true},
		{name: "target block and alternate phone", phone: "+19995550123", targetBlocksOwner: true},
	}
	var baseline *tg.Updates
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			if tc.ownerBlocksTarget {
				if _, err := s.BlockUser(ctx, a.ID, b.ID); err != nil {
					t.Fatalf("owner block: %v", err)
				}
			}
			if tc.targetBlocksOwner {
				if _, err := s.BlockUser(ctx, b.ID, a.ID); err != nil {
					t.Fatalf("target block: %v", err)
				}
			}

			beforeA := contactActivityFor(t, s, a.ID)
			beforeB := contactActivityFor(t, s, b.ID)
			req := &tg.ContactsAddContactRequest{
				ID:        api.InputUser(a.ID, b.ID),
				FirstName: "Private label",
				LastName:  "Only for owner",
				Phone:     tc.phone,
			}
			req.SetAddPhonePrivacyException(tc.privacyException)
			enc, err := api.AddContactForTest(s, a.ID, req)
			if err != nil {
				t.Fatalf("add contact: %v", err)
			}
			assertContactAddReply(t, enc, b, a.ID, false)
			response, ok := enc.(*tg.Updates)
			if !ok {
				t.Fatalf("add result = %T, want *tg.Updates", enc)
			}
			normalized := *response
			normalized.Date = 0
			if baseline == nil {
				baseline = &normalized
			} else if !reflect.DeepEqual(&normalized, baseline) {
				t.Errorf("add response changed with phone, privacy flag, or block state: got %+v, want %+v", normalized, baseline)
			}
			if after := contactActivityFor(t, s, a.ID); !reflect.DeepEqual(after, beforeA) {
				t.Errorf("adding contact changed owner's pts, events, or dialogs: got %+v, want %+v", after, beforeA)
			}
			if after := contactActivityFor(t, s, b.ID); !reflect.DeepEqual(after, beforeB) {
				t.Errorf("adding contact changed target's pts, events, or dialogs: got %+v, want %+v", after, beforeB)
			}
			stored, ok, err := s.UserByID(ctx, b.ID)
			if err != nil || !ok || stored.Phone != b.Phone {
				t.Errorf("target phone after add = %q, ok=%v err=%v, want unchanged", stored.Phone, ok, err)
			}
			for _, edge := range []struct {
				ownerID, targetID int64
				want              bool
			}{{a.ID, b.ID, tc.ownerBlocksTarget}, {b.ID, a.ID, tc.targetBlocksOwner}} {
				blocked, err := s.IsBlocked(ctx, edge.ownerID, edge.targetID)
				if err != nil || blocked != edge.want {
					t.Errorf("block %d -> %d after add = %v err=%v, want %v", edge.ownerID, edge.targetID, blocked, err, edge.want)
				}
			}

			if _, err := s.RemoveContact(ctx, a.ID, b.ID); err != nil {
				t.Fatalf("remove contact between scenarios: %v", err)
			}
			if tc.ownerBlocksTarget {
				if _, err := s.UnblockUser(ctx, a.ID, b.ID); err != nil {
					t.Fatalf("owner unblock: %v", err)
				}
			}
			if tc.targetBlocksOwner {
				if _, err := s.UnblockUser(ctx, b.ID, a.ID); err != nil {
					t.Fatalf("target unblock: %v", err)
				}
			}
		})
	}
}

func TestContactsRejectInvalidPeersUniformlyAndDeleteAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := mustUser(t, s, "+15551970101")
	b := mustUser(t, s, "+15551970102")
	c := mustUser(t, s, "+15551970103")
	viewer := mustUser(t, s, "+15551970104")
	unknownID := c.ID + 100000

	invalid := []struct {
		name string
		peer tg.InputUserClass
	}{
		{name: "forged hash", peer: &tg.InputUser{UserID: b.ID, AccessHash: api.DeriveUserHash(a.ID, b.ID) + 1}},
		{name: "another viewer hash", peer: api.InputUser(viewer.ID, b.ID)},
		{name: "unknown id", peer: api.InputUser(a.ID, unknownID)},
		{name: "self", peer: &tg.InputUserSelf{}},
		{name: "from message", peer: &tg.InputUserFromMessage{Peer: &tg.InputPeerUser{UserID: b.ID}, MsgID: 1, UserID: b.ID}},
		{name: "empty", peer: &tg.InputUserEmpty{}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			_, err := api.AddContactForTest(s, a.ID, &tg.ContactsAddContactRequest{ID: tc.peer})
			if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
				t.Errorf("invalid peer error = %s, want PEER_ID_INVALID", msg)
			}
		})
	}
	if contacts, count, err := s.Contacts(ctx, a.ID); err != nil || count != 0 || len(contacts) != 0 {
		t.Fatalf("contacts after invalid adds = %+v total=%d err=%v, want empty", contacts, count, err)
	}

	for _, target := range []store.User{b, c} {
		if changed, err := s.AddContact(ctx, a.ID, target.ID); err != nil || !changed {
			t.Fatalf("seed contact %d: changed=%v err=%v", target.ID, changed, err)
		}
	}
	for _, edge := range [][2]int64{{a.ID, b.ID}, {b.ID, a.ID}} {
		if _, err := s.BlockUser(ctx, edge[0], edge[1]); err != nil {
			t.Fatalf("block %d -> %d: %v", edge[0], edge[1], err)
		}
	}
	_, err := api.DeleteContactsForTest(s, a.ID, &tg.ContactsDeleteContactsRequest{ID: []tg.InputUserClass{
		api.InputUser(a.ID, b.ID),
		&tg.InputUser{UserID: c.ID, AccessHash: api.DeriveUserHash(a.ID, c.ID) + 1},
	}})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("delete with one invalid peer = %s, want PEER_ID_INVALID", msg)
	}
	contacts, count, err := s.Contacts(ctx, a.ID)
	if err != nil || count != 2 || len(contacts) != 2 {
		t.Fatalf("contacts after rejected delete = %+v total=%d err=%v, want both retained", contacts, count, err)
	}

	tooMany := make([]tg.InputUserClass, maxPeerDialogsForContactsTest)
	for i := range tooMany {
		tooMany[i] = api.InputUser(a.ID, b.ID)
	}
	_, err = api.DeleteContactsForTest(s, a.ID, &tg.ContactsDeleteContactsRequest{ID: tooMany})
	if msg := rpcMessage(t, err); msg != "LIMIT_INVALID" {
		t.Fatalf("oversized delete = %s, want LIMIT_INVALID", msg)
	}
	contacts, count, err = s.Contacts(ctx, a.ID)
	if err != nil || count != 2 || len(contacts) != 2 {
		t.Fatalf("contacts after oversized delete = %+v total=%d err=%v, want both retained", contacts, count, err)
	}

	enc, err := api.DeleteContactsForTest(s, a.ID, &tg.ContactsDeleteContactsRequest{ID: []tg.InputUserClass{api.InputUser(a.ID, b.ID)}})
	if err != nil {
		t.Fatalf("delete one valid contact: %v", err)
	}
	assertEncodes(t, enc)
	contacts, count, err = s.Contacts(ctx, a.ID)
	if err != nil || count != 1 || len(contacts) != 1 || contacts[0].UserID != c.ID {
		t.Fatalf("contacts after valid delete = %+v total=%d err=%v, want only C", contacts, count, err)
	}
}

const maxPeerDialogsForContactsTest = 101

func TestContactsGetContactsAndIDsUseSortedOwnerScopedHash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := contactUser(t, s, "+15551970201", "ownerhash", "Owner Global", "Name")
	b := contactUser(t, s, "+15551970202", "targethash", "Target Global", "Name")
	c := contactUser(t, s, "+15551970203", "otherhash", "Other Global", "Name")
	if _, err := s.BlockUser(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("A blocks B: %v", err)
	}
	if _, err := s.BlockUser(ctx, c.ID, a.ID); err != nil {
		t.Fatalf("C blocks A: %v", err)
	}

	if changed, err := s.AddContact(ctx, a.ID, b.ID); err != nil || !changed {
		t.Fatalf("add B for A: changed=%v err=%v", changed, err)
	}
	firstHash := contactIDsHashForTest([]int64{b.ID})
	first, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: 0})
	if err != nil {
		t.Fatalf("get A contacts: %v", err)
	}
	firstFull, ok := first.(*tg.ContactsContacts)
	if !ok {
		t.Fatalf("first contacts response = %T, want *tg.ContactsContacts", first)
	}
	if firstFull.SavedCount != 1 || len(firstFull.Contacts) != 1 || firstFull.Contacts[0].UserID != b.ID || firstFull.Contacts[0].Mutual {
		t.Fatalf("first contacts = %+v, want B as one-sided", firstFull)
	}
	if len(firstFull.Users) != 1 {
		t.Fatalf("first users = %d, want one", len(firstFull.Users))
	}
	firstB, ok := firstFull.Users[0].(*tg.User)
	if !ok || firstB.ID != b.ID || firstB.AccessHash != api.DeriveUserHash(a.ID, b.ID) || !firstB.Contact || firstB.MutualContact || firstB.Phone != "" || firstB.FirstName != b.FirstName || firstB.LastName != b.LastName {
		t.Fatalf("first contact user = %#v, want B's global name, contact=true, mutual=false, no phone", firstFull.Users[0])
	}
	assertEncodes(t, first)

	idsReply, err := api.GetContactIDsForTest(s, a.ID, &tg.ContactsGetContactIDsRequest{Hash: 0})
	if err != nil {
		t.Fatalf("get contact ids: %v", err)
	}
	ids, ok := idsReply.(*tg.IntVector)
	if !ok || !reflect.DeepEqual(ids.Elems, []int{int(b.ID)}) {
		t.Fatalf("contact ids = %#v, want [%d]", idsReply, b.ID)
	}
	assertEncodes(t, idsReply)

	unchanged, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: firstHash})
	if err != nil {
		t.Fatalf("get contacts with current hash: %v", err)
	}
	if _, ok := unchanged.(*tg.ContactsContactsNotModified); !ok {
		t.Fatalf("current contact hash response = %T, want not modified", unchanged)
	}
	assertEncodes(t, unchanged)
	unchangedIDs, err := api.GetContactIDsForTest(s, a.ID, &tg.ContactsGetContactIDsRequest{Hash: firstHash})
	if err != nil {
		t.Fatalf("get contact ids with current hash: %v", err)
	}
	unchangedIDsVector, ok := unchangedIDs.(*tg.IntVector)
	if !ok || len(unchangedIDsVector.Elems) != 0 {
		t.Fatalf("matching contact ids hash = %#v, want an empty int vector", unchangedIDs)
	}
	assertEncodes(t, unchangedIDs)

	// The other owner and an unrelated account see only their own empty lists.
	for _, ownerID := range []int64{b.ID, c.ID} {
		got, err := api.GetContactsForTest(s, ownerID, &tg.ContactsGetContactsRequest{Hash: 0})
		if err != nil {
			t.Fatalf("get contacts for %d: %v", ownerID, err)
		}
		full, ok := got.(*tg.ContactsContacts)
		if !ok || full.SavedCount != 0 || len(full.Contacts) != 0 || len(full.Users) != 0 {
			t.Errorf("contacts visible to %d = %#v, want empty", ownerID, got)
		}
	}

	if changed, err := s.AddContact(ctx, a.ID, c.ID); err != nil || !changed {
		t.Fatalf("add C for A: changed=%v err=%v", changed, err)
	}
	stale, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: firstHash})
	if err != nil {
		t.Fatalf("get contacts with stale hash: %v", err)
	}
	staleFull, ok := stale.(*tg.ContactsContacts)
	if !ok || staleFull.SavedCount != 2 || len(staleFull.Contacts) != 2 {
		t.Fatalf("stale contacts response = %#v, want full two-contact list", stale)
	}
	idsB := []int64{b.ID, c.ID}
	currentHash := contactIDsHashForTest(idsB)
	staleIDs, err := api.GetContactIDsForTest(s, a.ID, &tg.ContactsGetContactIDsRequest{Hash: firstHash})
	if err != nil {
		t.Fatalf("get ids with stale hash: %v", err)
	}
	staleVector, ok := staleIDs.(*tg.IntVector)
	if !ok || !reflect.DeepEqual(staleVector.Elems, []int{int(b.ID), int(c.ID)}) {
		t.Fatalf("stale ids response = %#v, want sorted B and C", staleIDs)
	}
	if currentHash == firstHash {
		t.Fatal("contact hash did not change when C was added")
	}

	mutualAdd, err := api.AddContactForTest(s, b.ID, &tg.ContactsAddContactRequest{ID: api.InputUser(b.ID, a.ID)})
	if err != nil {
		t.Fatalf("add A for B: %v", err)
	}
	assertContactAddReply(t, mutualAdd, a, b.ID, true)
	mutualReply, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: 0})
	if err != nil {
		t.Fatalf("get contacts after reciprocal add: %v", err)
	}
	mutual, ok := mutualReply.(*tg.ContactsContacts)
	if !ok || len(mutual.Contacts) != 2 || !mutual.Contacts[0].Mutual || mutual.Contacts[1].Mutual {
		t.Fatalf("mutual contacts = %#v, want only B mutual", mutualReply)
	}
	mutualB, ok := mutual.Users[0].(*tg.User)
	if !ok || !mutualB.Contact || !mutualB.MutualContact {
		t.Fatalf("mutual B user = %#v, want contact and mutual_contact", mutual.Users[0])
	}

	deleted, err := api.DeleteContactsForTest(s, a.ID, &tg.ContactsDeleteContactsRequest{ID: []tg.InputUserClass{api.InputUser(a.ID, b.ID)}})
	if err != nil {
		t.Fatalf("remove A -> B: %v", err)
	}
	assertEncodes(t, deleted)
	afterDelete, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: currentHash})
	if err != nil {
		t.Fatalf("get contacts after delete: %v", err)
	}
	afterDeleteFull, ok := afterDelete.(*tg.ContactsContacts)
	if !ok || afterDeleteFull.SavedCount != 1 || len(afterDeleteFull.Contacts) != 1 || afterDeleteFull.Contacts[0].UserID != c.ID {
		t.Fatalf("contacts after delete = %#v, want only C", afterDelete)
	}
	deletedIDs, err := api.GetContactIDsForTest(s, a.ID, &tg.ContactsGetContactIDsRequest{Hash: currentHash})
	if err != nil {
		t.Fatalf("get ids after delete with stale hash: %v", err)
	}
	deletedIDsVector, ok := deletedIDs.(*tg.IntVector)
	if !ok || !reflect.DeepEqual(deletedIDsVector.Elems, []int{int(c.ID)}) {
		t.Fatalf("contact ids after delete = %#v, want only C", deletedIDs)
	}
	deletedHash := contactIDsHashForTest([]int64{c.ID})
	unchangedAfterDelete, err := api.GetContactIDsForTest(s, a.ID, &tg.ContactsGetContactIDsRequest{Hash: deletedHash})
	if err != nil {
		t.Fatalf("get ids after delete with current hash: %v", err)
	}
	unchangedAfterDeleteVector, ok := unchangedAfterDelete.(*tg.IntVector)
	if !ok || len(unchangedAfterDeleteVector.Elems) != 0 {
		t.Fatalf("matching hash after delete = %#v, want empty vector", unchangedAfterDelete)
	}
	bAfterDelete, err := api.GetContactsForTest(s, b.ID, &tg.ContactsGetContactsRequest{Hash: 0})
	if err != nil {
		t.Fatalf("B contacts after A deletes edge: %v", err)
	}
	bFull, ok := bAfterDelete.(*tg.ContactsContacts)
	if !ok || len(bFull.Contacts) != 1 || bFull.Contacts[0].Mutual {
		t.Fatalf("B contact state after A deletes edge = %#v, want A one-sided", bAfterDelete)
	}
	if bUser, ok := bFull.Users[0].(*tg.User); !ok || !bUser.Contact || bUser.MutualContact || bUser.FirstName != a.FirstName || bUser.FirstName == "Private label" {
		t.Fatalf("B sees A as %#v, want global name and one-sided contact", bFull.Users[0])
	}

	if changed, err := s.AddContact(ctx, a.ID, b.ID); err != nil || !changed {
		t.Fatalf("restore A -> B edge: changed=%v err=%v", changed, err)
	}
	if changed, err := s.RemoveContact(ctx, b.ID, a.ID); err != nil || !changed {
		t.Fatalf("remove B -> A edge: changed=%v err=%v", changed, err)
	}
	afterReverseDelete, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{Hash: 0})
	if err != nil {
		t.Fatalf("get contacts after reverse delete: %v", err)
	}
	reverseDeleted, ok := afterReverseDelete.(*tg.ContactsContacts)
	if !ok || len(reverseDeleted.Contacts) != 2 || reverseDeleted.Contacts[0].Mutual {
		t.Fatalf("A contacts after B deletes reverse edge = %#v, want B no longer mutual", afterReverseDelete)
	}
}

func TestContactsConcurrentReciprocalAddsConverge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := contactUser(t, s, "+15551970211", "reciprocalowner", "Owner", "A")
	b := contactUser(t, s, "+15551970212", "reciprocalpeer", "Peer", "B")

	start := make(chan struct{})
	type addResult struct {
		owner, target store.User
		response      bin.Encoder
		err           error
	}
	results := make(chan addResult, 2)
	for _, pair := range [][2]store.User{{a, b}, {b, a}} {
		go func() {
			<-start
			response, err := api.AddContactForTest(s, pair[0].ID, &tg.ContactsAddContactRequest{
				ID: api.InputUser(pair[0].ID, pair[1].ID),
			})
			results <- addResult{owner: pair[0], target: pair[1], response: response, err: err}
		}()
	}
	close(start)

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent add %d -> %d: %v", result.owner.ID, result.target.ID, result.err)
		}
		assertEncodes(t, result.response)
		updates, ok := result.response.(*tg.Updates)
		if !ok || len(updates.Updates) != 1 || len(updates.Chats) != 0 || len(updates.Users) != 1 {
			t.Fatalf("concurrent add reply = %#v, want one peer-settings update and one user", result.response)
		}
		settingsUpdate, ok := updates.Updates[0].(*tg.UpdatePeerSettings)
		if !ok {
			t.Fatalf("concurrent add update = %T, want *tg.UpdatePeerSettings", updates.Updates[0])
		}
		settingsPeer, ok := settingsUpdate.Peer.(*tg.PeerUser)
		if !ok || settingsPeer.UserID != result.target.ID {
			t.Fatalf("concurrent add settings peer = %#v, want user %d", settingsUpdate.Peer, result.target.ID)
		}
		user, ok := updates.Users[0].(*tg.User)
		if !ok || user.ID != result.target.ID || user.AccessHash != api.DeriveUserHash(result.owner.ID, result.target.ID) || !user.Contact || user.Phone != "" {
			t.Fatalf("concurrent add peer = %#v, want safe contact user", updates.Users[0])
		}
		if user.MutualContact {
			mutual, err := s.IsContact(ctx, result.target.ID, result.owner.ID)
			if err != nil || !mutual {
				t.Fatalf("concurrent add reports mutual=%v, reverse edge=%v err=%v", user.MutualContact, mutual, err)
			}
		}
	}

	for _, pair := range [][2]store.User{{a, b}, {b, a}} {
		response, err := api.GetContactsForTest(s, pair[0].ID, &tg.ContactsGetContactsRequest{})
		if err != nil {
			t.Fatalf("get contacts for %d after reciprocal adds: %v", pair[0].ID, err)
		}
		contacts, ok := response.(*tg.ContactsContacts)
		if !ok || contacts.SavedCount != 1 || len(contacts.Contacts) != 1 || contacts.Contacts[0].UserID != pair[1].ID || !contacts.Contacts[0].Mutual {
			t.Fatalf("contacts for %d after reciprocal adds = %#v, want peer %d mutual", pair[0].ID, response, pair[1].ID)
		}
		user, ok := contacts.Users[0].(*tg.User)
		if !ok || !user.Contact || !user.MutualContact {
			t.Fatalf("contact user for %d after reciprocal adds = %#v, want contact and mutual", pair[0].ID, contacts.Users[0])
		}
	}
}

func contactIDsHashForTest(ids []int64) int64 {
	ordered := append([]int64(nil), ids...)
	slices.Sort(ordered)
	var hash uint32
	for _, id := range ordered {
		hash = hash*20261 + uint32(id) //nolint:gosec // Telegram's contact hash consumes ids as uint32 values.
	}
	return int64(hash & 0x7fffffff)
}

func TestContactSearchAddAndCreateChatFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := contactUser(t, s, "+15551970301", "flowowner", "Flow Owner", "Name")
	b := contactUser(t, s, "+15551970302", "bravoexact", "Bravo", "Target")

	searched, err := api.ContactsSearchForTest(s, a.ID, &tg.ContactsSearchRequest{Q: "@bravoexact", Limit: 10})
	if err != nil {
		t.Fatalf("exact username search: %v", err)
	}
	found, ok := searched.(*tg.ContactsFound)
	if !ok || len(found.Results) != 1 || len(found.Users) != 1 {
		t.Fatalf("exact username search result = %#v, want one user", searched)
	}
	peer, ok := found.Results[0].(*tg.PeerUser)
	if !ok || peer.UserID != b.ID {
		t.Fatalf("exact username result peer = %#v, want B", found.Results[0])
	}
	searchUser, ok := found.Users[0].(*tg.User)
	if !ok || searchUser.ID != b.ID || searchUser.AccessHash != api.DeriveUserHash(a.ID, b.ID) {
		t.Fatalf("exact username result user = %#v, want B with A's peer hash", found.Users[0])
	}

	prefix, err := api.ContactsSearchForTest(s, a.ID, &tg.ContactsSearchRequest{Q: "bravo", Limit: 10})
	if err != nil {
		t.Fatalf("prefix search: %v", err)
	}
	if result, ok := prefix.(*tg.ContactsFound); !ok || len(result.Results) != 0 {
		t.Fatalf("prefix search result = %#v, want no global user result", prefix)
	}

	added, err := api.AddContactForTest(s, a.ID, &tg.ContactsAddContactRequest{ID: api.InputUser(a.ID, b.ID), FirstName: "Private label"})
	if err != nil {
		t.Fatalf("add contact: %v", err)
	}
	assertContactAddReply(t, added, b, a.ID, false)
	listed, err := api.GetContactsForTest(s, a.ID, &tg.ContactsGetContactsRequest{})
	if err != nil {
		t.Fatalf("get contacts: %v", err)
	}
	contactList, ok := listed.(*tg.ContactsContacts)
	if !ok || contactList.SavedCount != 1 || len(contactList.Contacts) != 1 || contactList.Contacts[0].UserID != b.ID {
		t.Fatalf("contact picker result = %#v, want B", listed)
	}

	created, err := api.CreateChatForTest(s, a.ID, &tg.MessagesCreateChatRequest{Users: []tg.InputUserClass{api.InputUser(a.ID, b.ID)}, Title: "Contacts group"})
	if err != nil {
		t.Fatalf("create group with contact: %v", err)
	}
	assertEncodes(t, created)
	createdUsers, ok := created.(*tg.MessagesInvitedUsers)
	if !ok || len(createdUsers.MissingInvitees) != 0 {
		t.Fatalf("create group result = %#v, want B invited", created)
	}
	groupUpdates, ok := createdUsers.Updates.(*tg.Updates)
	if !ok || len(groupUpdates.Chats) != 1 {
		t.Fatalf("create group updates = %#v, want one chat", createdUsers.Updates)
	}
	groupChat, ok := groupUpdates.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("create group chat = %T, want *tg.Chat", groupUpdates.Chats[0])
	}
	participants, err := s.Participants(ctx, groupChat.ID)
	if err != nil {
		t.Fatalf("group participants: %v", err)
	}
	if len(participants) != 2 || participants[0].UserID == participants[1].UserID {
		t.Fatalf("group participants = %+v, want A and B", participants)
	}
	bState, err := s.State(ctx, b.ID)
	if err != nil || bState.Pts == 0 {
		t.Fatalf("B update state after group create = %+v err=%v, want delivered service event", bState, err)
	}
	bEvents, err := s.EventsWindow(ctx, b.ID, 0, bState.Pts, 100)
	if err != nil || len(bEvents) == 0 {
		t.Fatalf("B events after group create = %d err=%v, want service event", len(bEvents), err)
	}
}

func TestContactDoesNotGrantMessageOrProfileAuthorityAndCannotBypassBlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a := contactUser(t, s, "+15551970401", "authorityowner", "Owner", "Global")
	b := contactUser(t, s, "+15551970402", "authoritytarget", "Target", "Global")

	if _, err := s.BlockUser(ctx, b.ID, a.ID); err != nil {
		t.Fatalf("B blocks A: %v", err)
	}
	if _, err := s.AddContact(ctx, a.ID, b.ID); err != nil {
		t.Fatalf("A adds B despite block: %v", err)
	}
	if blocked, err := s.IsBlocked(ctx, b.ID, a.ID); err != nil || !blocked {
		t.Fatalf("B -> A block after add = %v err=%v, want retained", blocked, err)
	}

	_, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerUser{UserID: b.ID, AccessHash: 1},
		Message:  "contact is not authorization",
		RandomID: 70401,
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("message authorized by contact status: %s, want PEER_ID_INVALID", msg)
	}
	_, err = api.CreateChatForTest(s, a.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{&tg.InputUser{UserID: b.ID, AccessHash: 1}},
		Title: "Invalid peer",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("invitation authorized by contact status: %s, want PEER_ID_INVALID", msg)
	}

	users, err := api.LoadUsersForTest(s, []int64{a.ID, b.ID}, b.ID)
	if err != nil {
		t.Fatalf("load A for B: %v", err)
	}
	if user, ok := loadUsersWire(t, users, a.ID).(*tg.UserEmpty); !ok || user.ID != a.ID {
		t.Fatalf("B profile access after one-sided contact = %#v, want userEmpty", loadUsersWire(t, users, a.ID))
	}

	// A can still add a blocked invitee to the contact list, but the existing
	// group authorization contract keeps that invitee out of a new group.
	d := mustUser(t, s, "+15551970404")
	created, err := api.CreateChatForTest(s, a.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{api.InputUser(a.ID, b.ID), api.InputUser(a.ID, d.ID)},
		Title: "Blocked contact",
	})
	if err != nil {
		t.Fatalf("create group with blocked contact: %v", err)
	}
	invited, ok := created.(*tg.MessagesInvitedUsers)
	if !ok || len(invited.MissingInvitees) != 1 || invited.MissingInvitees[0].UserID != b.ID {
		t.Fatalf("blocked contact invite result = %#v, want B uniformly excluded", created)
	}
	groupUpdates, ok := invited.Updates.(*tg.Updates)
	if !ok || len(groupUpdates.Chats) != 1 {
		t.Fatalf("blocked group updates = %#v, want one chat", invited.Updates)
	}
	chat, ok := groupUpdates.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("blocked group chat = %T, want *tg.Chat", groupUpdates.Chats[0])
	}
	participants, err := s.Participants(ctx, chat.ID)
	if err != nil {
		t.Fatalf("group participants: %v", err)
	}
	for _, member := range participants {
		if member.UserID == b.ID {
			t.Fatalf("blocked contact B was added to group: %+v", participants)
		}
	}
	if contacts, total, err := s.Contacts(ctx, a.ID); err != nil || total != 1 || len(contacts) != 1 || contacts[0].UserID != b.ID {
		t.Fatalf("contact list after refused invitation = %+v total=%d err=%v, want B retained", contacts, total, err)
	}
}
