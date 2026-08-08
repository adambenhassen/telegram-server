package e2e_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestContactsSearch proves contacts.search returns only dialog partners and
// rejects an empty query. Three users are created: A and B exchange a message
// (establishing a dialog), while C exists but has never communicated with A.
func TestContactsSearch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
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

	newClient := func() *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:         dcID,
			DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:   dcs.Plain(dcs.PlainOptions{}),
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	const phoneA = "+15551290001"
	const phoneB = "+15551290002"
	const phoneC = "+15551290003"

	// Log in all three users and capture their user IDs.
	logIn := func(phone string) int64 {
		var userID int64
		client := newClient()
		if err := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, flowFor(phone)); err != nil {
				return err
			}
			self, err := client.Self(ctx)
			if err != nil {
				return err
			}
			userID = self.ID
			return nil
		}); err != nil {
			t.Fatalf("login %s: %v", phone, err)
		}
		return userID
	}

	aUserID := logIn(phoneA)
	bUserID := logIn(phoneB)
	cUserID := logIn(phoneC)

	// Fetch B and C's first names so we can search for them.
	bUser, ok, err := st.UserByID(ctx, bUserID)
	if err != nil || !ok {
		t.Fatalf("load B user: ok=%v err=%v", ok, err)
	}
	cUser, ok, err := st.UserByID(ctx, cUserID)
	if err != nil || !ok {
		t.Fatalf("load C user: ok=%v err=%v", ok, err)
	}

	// A sends a message to B to establish a dialog.
	aClient := newClient()
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		_, err := aClient.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "hello B",
			RandomID: 1,
		})
		return err
	}); err != nil {
		t.Fatalf("A send to B: %v", err)
	}

	// contacts.search from A for B's first name — should return B.
	var foundB *tg.ContactsFound
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		var err error
		foundB, err = aClient.API().ContactsSearch(ctx, &tg.ContactsSearchRequest{
			Q:     bUser.FirstName,
			Limit: 10,
		})
		return err
	}); err != nil {
		t.Fatalf("A contacts.search(B): %v", err)
	}
	if foundB == nil {
		t.Fatal("contacts.search returned nil")
	}
	if len(foundB.MyResults) != 1 {
		t.Fatalf("A contacts.search(B) MyResults len = %d, want 1", len(foundB.MyResults))
	}
	peerB, ok := foundB.MyResults[0].(*tg.PeerUser)
	if !ok {
		t.Fatalf("MyResults[0] type = %T, want *tg.PeerUser", foundB.MyResults[0])
	}
	if peerB.UserID != bUserID {
		t.Fatalf("MyResults[0].UserID = %d, want %d", peerB.UserID, bUserID)
	}
	if len(foundB.Users) != 1 {
		t.Fatalf("Users len = %d, want 1", len(foundB.Users))
	}
	if foundBUser, ok := foundB.Users[0].(*tg.User); ok {
		if foundBUser.ID != bUserID {
			t.Fatalf("Users[0].ID = %d, want %d", foundBUser.ID, bUserID)
		}
	} else {
		t.Fatalf("Users[0] type = %T, want *tg.User", foundB.Users[0])
	}
	if len(foundB.Results) != 0 {
		t.Fatalf("Results len = %d, want 0 (global search out of scope)", len(foundB.Results))
	}

	// contacts.search from A for C's first name — should return nothing
	// (no dialog between A and C).
	var foundC *tg.ContactsFound
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		var err error
		foundC, err = aClient.API().ContactsSearch(ctx, &tg.ContactsSearchRequest{
			Q:     cUser.FirstName,
			Limit: 10,
		})
		return err
	}); err != nil {
		t.Fatalf("A contacts.search(C): %v", err)
	}
	if foundC == nil {
		t.Fatal("contacts.search returned nil for C")
	}
	if len(foundC.MyResults) != 0 {
		t.Fatalf("A contacts.search(C) MyResults len = %d, want 0 (no cross-dialog leak)", len(foundC.MyResults))
	}

	// contacts.search from A with empty query — should return SEARCH_QUERY_EMPTY.
	var searchEmptyErr error
	if err := aClient.Run(ctx, func(ctx context.Context) error {
		_, searchEmptyErr = aClient.API().ContactsSearch(ctx, &tg.ContactsSearchRequest{
			Q:     "",
			Limit: 10,
		})
		return nil
	}); err != nil {
		t.Fatalf("A contacts.search(\"\") run: %v", err)
	}
	if searchEmptyErr == nil {
		t.Fatal("contacts.search(\"\") should return error, got nil")
	}
}
