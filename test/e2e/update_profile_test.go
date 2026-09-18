package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestUpdateProfilePeerSeesNewName proves a signed-in user can rename themselves
// and a second client already observing them sees the new name without re-login.
func TestUpdateProfilePeerSeesNewName(t *testing.T) {
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

	const phoneA, phoneB = "+15550004201", "+15550004202"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB)

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
	}()

	login := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")

	// A sends B a message so B has A in their dialog list.
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "hello",
			RandomID: 50004201,
		})
		return err
	})

	req := &tg.AccountUpdateProfileRequest{}
	req.SetFirstName("Alicia")
	req.SetLastName("Renamed")
	execChat(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.AccountUpdateProfile(ctx, req)
		if err != nil {
			return err
		}
		u, ok := res.(*tg.User)
		if !ok {
			return errors.New("updateProfile: result is not *tg.User")
		}
		if u.FirstName != "Alicia" || u.LastName != "Renamed" {
			return errors.New("updateProfile: returned name mismatch")
		}
		return nil
	})

	execChat(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      20,
		})
		if err != nil {
			return err
		}
		var users []tg.UserClass
		switch d := res.(type) {
		case *tg.MessagesDialogs:
			users = d.Users
		case *tg.MessagesDialogsSlice:
			users = d.Users
		default:
			return errors.New("getDialogs: unexpected type")
		}
		for _, u := range users {
			user, ok := u.(*tg.User)
			if !ok || user.ID != aUserID {
				continue
			}
			if user.FirstName != "Alicia" || user.LastName != "Renamed" {
				return errors.New("getDialogs: A still has the old name")
			}
			return nil
		}
		return errors.New("getDialogs: A not in Users")
	})

	close(aCmds)
	close(bCmds)
	for _, ch := range []chan error{errA, errB} {
		if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}
}
