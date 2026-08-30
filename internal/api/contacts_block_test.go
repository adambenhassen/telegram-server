package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestContactsBlockUnblockAndGetBlockedAreDirectedAndIdempotent(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	blocker := chatUser(t, s, 201)
	blocked := chatUser(t, s, 202)

	request := &tg.ContactsBlockRequest{ID: api.InputPeerUser(blocker.ID, blocked.ID)}
	enc, err := api.BlockForTest(s, blocker.ID, request)
	if err != nil {
		t.Fatalf("block: %v", err)
	}
	if _, ok := enc.(*tg.BoolTrue); !ok {
		t.Fatalf("block result = %T, want *tg.BoolTrue", enc)
	}
	assertEncodes(t, enc)

	enc, err = api.BlockForTest(s, blocker.ID, request)
	if err != nil {
		t.Fatalf("duplicate block: %v", err)
	}
	assertEncodes(t, enc)

	enc, err = api.GetBlockedForTest(s, blocker.ID, &tg.ContactsGetBlockedRequest{Limit: 100})
	if err != nil {
		t.Fatalf("get blocked: %v", err)
	}
	full, ok := enc.(*tg.ContactsBlocked)
	if !ok {
		t.Fatalf("get blocked result = %T, want *tg.ContactsBlocked", enc)
	}
	if len(full.Blocked) != 1 || len(full.Users) != 1 {
		t.Fatalf("blocked response = %+v, want one peer and user", full)
	}
	peer, ok := full.Blocked[0].PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != blocked.ID {
		t.Fatalf("blocked peer = %#v, want user %d", full.Blocked[0].PeerID, blocked.ID)
	}
	user, ok := full.Users[0].(*tg.User)
	if !ok || user.ID != blocked.ID {
		t.Fatalf("blocked user = %#v, want user %d", full.Users[0], blocked.ID)
	}
	assertEncodes(t, enc)

	enc, err = api.GetBlockedForTest(s, blocked.ID, &tg.ContactsGetBlockedRequest{Limit: 100})
	if err != nil {
		t.Fatalf("reverse get blocked: %v", err)
	}
	reverse, ok := enc.(*tg.ContactsBlocked)
	if !ok || len(reverse.Blocked) != 0 || len(reverse.Users) != 0 {
		t.Fatalf("reverse blocked response = %#v, want empty full response", enc)
	}
	assertEncodes(t, enc)

	unblock := &tg.ContactsUnblockRequest{ID: api.InputPeerUser(blocker.ID, blocked.ID)}
	enc, err = api.UnblockForTest(s, blocker.ID, unblock)
	if err != nil {
		t.Fatalf("unblock: %v", err)
	}
	assertEncodes(t, enc)
	enc, err = api.UnblockForTest(s, blocker.ID, unblock)
	if err != nil {
		t.Fatalf("duplicate unblock: %v", err)
	}
	assertEncodes(t, enc)

	enc, err = api.GetBlockedForTest(s, blocker.ID, &tg.ContactsGetBlockedRequest{Limit: 100})
	if err != nil {
		t.Fatalf("get after unblock: %v", err)
	}
	full, ok = enc.(*tg.ContactsBlocked)
	if !ok || len(full.Blocked) != 0 || len(full.Users) != 0 {
		t.Fatalf("after unblock response = %#v, want empty full response", enc)
	}
}

func TestContactsBlockRejectsInvalidTargets(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	blocker := chatUser(t, s, 211)
	blocked := chatUser(t, s, 212)

	_, err := api.BlockForTest(s, 0, &tg.ContactsBlockRequest{ID: api.InputPeerUser(blocker.ID, blocked.ID)})
	wantRPC(t, err, "AUTH_KEY_UNREGISTERED")

	_, err = api.BlockForTest(s, blocker.ID, &tg.ContactsBlockRequest{ID: &tg.InputPeerUser{UserID: blocked.ID, AccessHash: 1}})
	wantRPC(t, err, "PEER_ID_INVALID")

	_, err = api.BlockForTest(s, blocker.ID, &tg.ContactsBlockRequest{ID: api.InputPeerUser(blocker.ID, blocker.ID)})
	wantRPC(t, err, "PEER_ID_INVALID")

	_, err = api.BlockForTest(s, blocker.ID, &tg.ContactsBlockRequest{ID: &tg.InputPeerChat{ChatID: 1}})
	wantRPC(t, err, "PEER_ID_INVALID")
}

func TestContactsGetBlockedPaginatesAndPreservesCount(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	blocker := chatUser(t, s, 231)
	first := chatUser(t, s, 232)
	second := chatUser(t, s, 233)
	for _, target := range []store.User{first, second} {
		if _, err := s.BlockUser(context.Background(), blocker.ID, target.ID); err != nil {
			t.Fatalf("block %d: %v", target.ID, err)
		}
	}

	enc, err := api.GetBlockedForTest(s, blocker.ID, &tg.ContactsGetBlockedRequest{Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	page, ok := enc.(*tg.ContactsBlockedSlice)
	if !ok {
		t.Fatalf("first page = %T, want *tg.ContactsBlockedSlice", enc)
	}
	if page.Count != 2 || len(page.Blocked) != 1 || len(page.Users) != 1 {
		t.Fatalf("first page = %+v, want count 2 and one row", page)
	}
	assertEncodes(t, enc)

	enc, err = api.GetBlockedForTest(s, blocker.ID, &tg.ContactsGetBlockedRequest{Offset: 100, Limit: 1})
	if err != nil {
		t.Fatalf("empty page: %v", err)
	}
	empty, ok := enc.(*tg.ContactsBlockedSlice)
	if !ok || empty.Count != 2 || len(empty.Blocked) != 0 || len(empty.Users) != 0 {
		t.Fatalf("empty page = %#v, want count 2 and no rows", enc)
	}
	assertEncodes(t, enc)
}

func TestBlockedTargetIsNotSmuggledIntoCreateChat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := chatUser(t, s, 221)
	blocked := chatUser(t, s, 222)
	member := chatUser(t, s, 223)
	if _, err := s.BlockUser(ctx, blocked.ID, creator.ID); err != nil {
		t.Fatalf("block: %v", err)
	}

	enc, err := api.CreateChatForTest(s, creator.ID, &tg.MessagesCreateChatRequest{
		Users: inputUsers(creator.ID, blocked.ID, member.ID),
		Title: "Filtered",
	})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	assertEncodes(t, enc)
	created, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("create result = %T, want *tg.MessagesInvitedUsers", enc)
	}
	if len(created.MissingInvitees) != 1 || created.MissingInvitees[0].UserID != blocked.ID {
		t.Fatalf("missing invitees = %+v, want blocked user %d", created.MissingInvitees, blocked.ID)
	}
	// The new chat is the only chat created in this isolated database. Find it
	// through the creator's dialogs and verify the blocked invitee was omitted.
	dialogs, err := s.Dialogs(ctx, creator.ID, 0, 100)
	if err != nil {
		t.Fatalf("creator dialogs: %v", err)
	}
	if len(dialogs) != 1 {
		t.Fatalf("creator dialogs = %+v, want one", dialogs)
	}
	participants, err := s.Participants(ctx, dialogs[0].PeerID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	if len(participants) != 2 || participants[0].UserID == blocked.ID || participants[1].UserID == blocked.ID {
		t.Fatalf("participants = %+v, want creator and unblocked member", participants)
	}
	if participants[0].UserID != creator.ID && participants[1].UserID != creator.ID {
		t.Fatalf("participants = %+v, missing creator %d", participants, creator.ID)
	}
}
