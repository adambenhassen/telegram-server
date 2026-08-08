package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"

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

	// Launch all three clients as interactive sessions so auth state persists.
	clientA, clientB, clientC := newClient(), newClient(), newClient()
	aCmds, bCmds, cCmds := make(chan command), make(chan command), make(chan command)
	aID, bID, cID := make(chan int64, 1), make(chan int64, 1), make(chan int64, 1)
	errA, errB, errC := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()
	go func() { errC <- runInteractive(ctx, clientC, flowFor(phoneC), cID, cCmds) }()

	var aUserID, bUserID, cUserID int64
	for i, ch := range []chan int64{aID, bID, cID} {
		select {
		case id := <-ch:
			switch i {
			case 0:
				aUserID = id
			case 1:
				bUserID = id
			case 2:
				cUserID = id
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("client login timeout (user %d)", i)
		}
	}

	// Set B and C's first names — CreateUser only sets phone, so first_name is empty.
	// The name_tsv column is GENERATED ALWAYS, so Postgres recomputes it automatically.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "UPDATE users SET first_name = $1 WHERE id = $2", "Bob", bUserID); err != nil {
		t.Fatalf("set B first name: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE users SET first_name = $1 WHERE id = $2", "Carol", cUserID); err != nil {
		t.Fatalf("set C first name: %v", err)
	}

	const searchB = "Bob"
	const searchC = "Carol"

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	// A sends a message to B to establish a dialog.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
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
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		foundB, err = c.ContactsSearch(ctx, &tg.ContactsSearchRequest{
			Q:     searchB,
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
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		var err error
		foundC, err = c.ContactsSearch(ctx, &tg.ContactsSearchRequest{
			Q:     searchC,
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
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, searchEmptyErr = c.ContactsSearch(ctx, &tg.ContactsSearchRequest{
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

	// Shut down clients.
	close(aCmds)
	close(bCmds)
	close(cCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
	if err := <-errC; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client C run: %v", err)
	}
}
