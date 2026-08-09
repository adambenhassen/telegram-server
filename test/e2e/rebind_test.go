package e2e_test

import (
	"context"
	"errors"
	"fmt"
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

// TestRebindStopsPushToPreviousUser proves a live socket stops receiving user
// A's updates once an auth.signIn on that same connection rebinds its auth key
// to a different user. Without the registry resync the socket keeps its place
// in A's bucket, so A's message bodies keep arriving encrypted under a key the
// new user now holds — and A cannot revoke it, since the key is no longer
// listed among A's authorizations.
//
// The 3s bound matters: a bound near the 30s read timeout would pass on the
// timeout closing the socket and would prove nothing about the resync.
func TestRebindStopsPushToPreviousUser(t *testing.T) {
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
	port := tcpPort(t, ln)
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
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

	// victim holds the connection under test; sender pushes messages at it;
	// taker signs in over the victim's own connection and takes its key.
	const phoneVictim, phoneSender, phoneTaker = "+15551290001", "+15551290002", "+15551290003"
	collVictim, collSender := newUpdateCollector(), newUpdateCollector()
	victim, sender := newClient(collVictim), newClient(collSender)

	victimCmds, senderCmds := make(chan command), make(chan command)
	victimID, senderID := make(chan int64, 1), make(chan int64, 1)
	errVictim, errSender := make(chan error, 1), make(chan error, 1)
	go func() { errVictim <- runInteractive(ctx, victim, flowFor(phoneVictim), victimID, victimCmds) }()
	go func() { errSender <- runInteractive(ctx, sender, flowFor(phoneSender), senderID, senderCmds) }()

	var victimUserID int64
	select {
	case victimUserID = <-victimID:
	case <-ctx.Done():
		t.Fatalf("victim login timeout: %v", ctx.Err())
	}
	var senderUserID int64
	select {
	case senderUserID = <-senderID:
	case <-ctx.Done():
		t.Fatalf("sender login timeout: %v", ctx.Err())
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-ctx.Done():
			t.Fatalf("command enqueue timeout: %v", ctx.Err())
		}
		return <-d
	}
	peerVictim := peerUser(senderUserID, victimUserID)
	sendToVictim := func(text string, randomID int64) {
		t.Helper()
		if err := exec(senderCmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peerVictim, Message: text, RandomID: randomID,
			})
			return err
		}); err != nil {
			t.Fatalf("send %q: %v", text, err)
		}
	}

	// Baseline: push to the victim's socket works before the rebind, so a later
	// silence is the resync and not a dead delivery path.
	sendToVictim("before rebind", 29001)
	if got := recvOrCtx(t, ctx, collVictim.newMsg, "victim updateNewMessage"); got.Message != "before rebind" {
		t.Fatalf("victim received %q, want %q", got.Message, "before rebind")
	}

	// The taker signs in with its own phone over the victim's connection, which
	// rebinds that connection's auth key from the victim to the taker.
	if err := exec(victimCmds, func(ctx context.Context, c *tg.Client) error {
		sent, err := c.AuthSendCode(ctx, &tg.AuthSendCodeRequest{
			PhoneNumber: phoneTaker, APIID: 1, APIHash: "hash", Settings: tg.CodeSettings{},
		})
		if err != nil {
			return err
		}
		code, ok := sent.(*tg.AuthSentCode)
		if !ok {
			return fmt.Errorf("sendCode returned %T", sent)
		}
		phoneCode, err := codes.wait(ctx, phoneTaker)
		if err != nil {
			return err
		}
		req := &tg.AuthSignInRequest{PhoneNumber: phoneTaker, PhoneCodeHash: code.PhoneCodeHash}
		req.SetPhoneCode(phoneCode)
		if _, err := c.AuthSignIn(ctx, req); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("taker sign-in over the victim connection: %v", err)
	}

	// The resync runs after the frame that follows the rebind, so two round
	// trips are needed for it to have provably happened: the first carries the
	// new binding, and the second cannot be answered until the first completed.
	for range 2 {
		if err := exec(victimCmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.UpdatesGetState(ctx)
			return err
		}); err != nil {
			t.Fatalf("post-rebind getState: %v", err)
		}
	}

	// A message for the victim must no longer reach the taken-over socket.
	sendToVictim("after rebind", 29002)
	select {
	case got := <-collVictim.newMsg:
		t.Fatalf("socket still received %q for the previous user after its key was rebound", got.Message)
	case <-time.After(3 * time.Second):
	}

	close(victimCmds)
	close(senderCmds)
	if err := <-errVictim; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("victim run: %v", err)
	}
	if err := <-errSender; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("sender run: %v", err)
	}
}
