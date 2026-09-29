package e2e_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestContactsSearchAddGroupLive(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15551980001", "+15551980002"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)
	b, ok, err := st.UserByPhone(ctx, phoneB)
	if err != nil || !ok {
		t.Fatalf("B lookup: ok=%v err=%v", ok, err)
	}
	if err := st.ClaimUsername(ctx, b.ID, "bravoexact"); err != nil {
		t.Fatalf("claim B username: %v", err)
	}

	updatesA, updatesB := newUpdateCollector(), newUpdateCollector()
	clientA := createClient(addr.Port, key, dcID, updatesA, nil)
	clientB := createClient(addr.Port, key, dcID, updatesB, nil)
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA, codes), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB, codes), bID, bCmds) }()

	readID := func(ch <-chan int64, who string) int64 {
		t.Helper()
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout: %v", who, ctx.Err())
			return 0
		}
	}
	readID(aID, "A")
	bUserID := readID(bID, "B")

	var found *tg.ContactsFound
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		found, err = c.ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: "bravoexact", Limit: 10})
		return err
	})
	if found == nil || len(found.Results) != 1 || len(found.Users) != 1 {
		t.Fatalf("exact username search = %#v, want B", found)
	}
	peer, ok := found.Results[0].(*tg.PeerUser)
	if !ok || peer.UserID != bUserID {
		t.Fatalf("search peer = %#v, want B %d", found.Results[0], bUserID)
	}
	searchUser, ok := found.Users[0].(*tg.User)
	if !ok || searchUser.ID != bUserID || searchUser.AccessHash == 0 {
		t.Fatalf("search user = %#v, want B with usable peer settings", found.Users[0])
	}

	var added tg.UpdatesClass
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		added, err = c.ContactsAddContact(ctx, &tg.ContactsAddContactRequest{
			ID:        &tg.InputUser{UserID: bUserID, AccessHash: searchUser.AccessHash},
			FirstName: "Private label",
			Phone:     "+15550000000",
		})
		return err
	})
	addUpdates, ok := added.(*tg.Updates)
	if !ok || len(addUpdates.Users) != 1 {
		t.Fatalf("add reply = %#v, want Updates with B", added)
	}
	if len(addUpdates.Updates) != 1 {
		t.Fatalf("add reply updates = %#v, want one peer-settings update", addUpdates.Updates)
	}
	settingsUpdate, ok := addUpdates.Updates[0].(*tg.UpdatePeerSettings)
	if !ok {
		t.Fatalf("add reply update = %T, want *tg.UpdatePeerSettings", addUpdates.Updates[0])
	}
	settingsPeer, ok := settingsUpdate.Peer.(*tg.PeerUser)
	if !ok || settingsPeer.UserID != bUserID {
		t.Fatalf("add reply settings peer = %#v, want B %d", settingsUpdate.Peer, bUserID)
	}
	addedB, ok := addUpdates.Users[0].(*tg.User)
	if !ok || addedB.ID != bUserID || !addedB.Contact || addedB.Phone != "" || addedB.FirstName != "" {
		t.Fatalf("add reply user = %#v, want contact B without phone", addUpdates.Users[0])
	}
	if len(updatesB.serviceMsg) != 0 || len(updatesB.newMsg) != 0 {
		t.Fatalf("B received contact-add activity before group creation: service=%d messages=%d", len(updatesB.serviceMsg), len(updatesB.newMsg))
	}

	var contactResult tg.ContactsContactsClass
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		contactResult, err = c.ContactsGetContacts(ctx, 0)
		return err
	})
	contactList, ok := contactResult.(*tg.ContactsContacts)
	if !ok || contactList.SavedCount != 1 || len(contactList.Contacts) != 1 || contactList.Contacts[0].UserID != bUserID {
		t.Fatalf("contacts picker = %#v, want B", contactResult)
	}
	if len(contactList.Users) != 1 {
		t.Fatalf("contact users = %d, want one", len(contactList.Users))
	}
	listedB, ok := contactList.Users[0].(*tg.User)
	if !ok || listedB.ID != bUserID || listedB.AccessHash != searchUser.AccessHash || !listedB.Contact || listedB.Phone != "" {
		t.Fatalf("listed B = %#v, want contact without phone", contactList.Users[0])
	}
	var bContactResult tg.ContactsContactsClass
	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		bContactResult, err = c.ContactsGetContacts(ctx, 0)
		return err
	})
	bContactList, ok := bContactResult.(*tg.ContactsContacts)
	if !ok || bContactList.SavedCount != 0 || len(bContactList.Contacts) != 0 || len(bContactList.Users) != 0 {
		t.Fatalf("B sees A's one-sided contact edge: %#v", bContactResult)
	}

	var invited *tg.MessagesInvitedUsers
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		invited, err = c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Contact flow",
			Users: []tg.InputUserClass{&tg.InputUser{UserID: bUserID, AccessHash: searchUser.AccessHash}},
		})
		return err
	})
	if invited == nil || len(invited.MissingInvitees) != 0 {
		t.Fatalf("create chat result = %#v, want B invited", invited)
	}
	created, ok := invited.Updates.(*tg.Updates)
	if !ok || len(created.Chats) != 1 {
		t.Fatalf("create chat updates = %#v, want chat", invited.Updates)
	}
	var createdB *tg.User
	for _, user := range created.Users {
		if peer, ok := user.(*tg.User); ok && peer.ID == bUserID {
			createdB = peer
			break
		}
	}
	if createdB == nil || !createdB.Contact || createdB.MutualContact {
		t.Fatalf("create chat B = %#v, want contact=true mutual_contact=false", createdB)
	}
	chat, ok := created.Chats[0].(*tg.Chat)
	if !ok || chat.ParticipantsCount != 2 {
		t.Fatalf("created chat = %#v, want two participants", created.Chats[0])
	}

	service, err := updatesB.waitService(ctx, &tg.MessageActionChatCreate{})
	if err != nil {
		t.Fatalf("B wait for live group invite: %v", err)
	}
	action, ok := service.svc.Action.(*tg.MessageActionChatCreate)
	if !ok || action.Title != "Contact flow" {
		t.Fatalf("B live create action = %#v, want Contact flow", service.svc.Action)
	}
	if !hasChat(service.chats, chat.ID) {
		t.Fatalf("B live update chats = %v, want chat %d", service.chats, chat.ID)
	}

}
