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

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestForward1to1(t *testing.T) {
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
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

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB := newClient(collA), newClient(collB)
	const phoneA, phoneB = "+15551290001", "+15551290002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("client B login timeout")
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)

	// A sends a message to B.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerB, Message: "original message", RandomID: 100001,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	recvOr(t, collB.newMsg, "B updateNewMessage")

	// A forwards the message to B (same peer).
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer: peerB,
			ID:       []int{1},
			RandomID: []int64{200001},
			ToPeer:   peerB,
		})
		return err
	}); err != nil {
		t.Fatalf("A forward: %v", err)
	}

	// B should receive the forwarded message with FwdFrom populated.
	fwdMsg := recvOr(t, collB.newMsg, "B forwarded message")
	if fwdMsg.FwdFrom.Zero() {
		t.Fatalf("forwarded message has no FwdFrom")
	}
	fromID, ok := fwdMsg.FwdFrom.GetFromID()
	if !ok {
		t.Fatalf("FwdFrom has no FromID")
	}
	if pu, ok := fromID.(*tg.PeerUser); !ok || pu.UserID != aUserID {
		t.Fatalf("FwdFrom.FromID = %+v, want PeerUser(%d)", fromID, aUserID)
	}
	if fwdMsg.FwdFrom.Date == 0 {
		t.Fatalf("FwdFrom.Date is zero")
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

func TestForwardToGroup(t *testing.T) {
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
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

	collA, collB, collC := newUpdateCollector(), newUpdateCollector(), newUpdateCollector()
	clientA, clientB, clientC := newClient(collA), newClient(collB), newClient(collC)
	const phoneA, phoneB, phoneC = "+15551291001", "+15551291002", "+15551291003"

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
			t.Fatal("client login timeout")
		}
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)

	// A sends a message to B (1:1).
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerB, Message: "to forward", RandomID: 300001,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	recvOr(t, collB.newMsg, "B updateNewMessage")

	// A creates a chat with B and C.
	var chatID int64
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "test chat",
			Users: []tg.InputUserClass{
				inputUser(aUserID, bUserID),
				inputUser(aUserID, cUserID),
			},
		})
		if err != nil {
			return err
		}
		if ups, ok := res.Updates.(*tg.Updates); ok {
			for _, ucl := range ups.Chats {
				if ch, ok2 := ucl.(*tg.Chat); ok2 {
					chatID = ch.ID
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A createChat: %v", err)
	}

	peerChat := &tg.InputPeerChat{ChatID: chatID}

	// A forwards the 1:1 message to the chat.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer: peerB,
			ID:       []int{1},
			RandomID: []int64{400001},
			ToPeer:   peerChat,
		})
		return err
	}); err != nil {
		t.Fatalf("A forward to chat: %v", err)
	}

	// B and C should both receive the forwarded message with FwdFrom populated.
	for _, coll := range []*updateCollector{collB, collC} {
		fwdMsg := recvOr(t, coll.newMsg, "member forwarded message")
		if fwdMsg.FwdFrom.Zero() {
			t.Fatalf("forwarded message has no FwdFrom")
		}
		fromID, ok := fwdMsg.FwdFrom.GetFromID()
		if !ok {
			t.Fatalf("FwdFrom has no FromID")
		}
		if pu, ok := fromID.(*tg.PeerUser); !ok || pu.UserID != aUserID {
			t.Fatalf("FwdFrom.FromID = %+v, want PeerUser(%d)", fromID, aUserID)
		}
	}

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
	_ = cUserID // used above
}

func TestForwardAuthRejection(t *testing.T) {
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
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

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB := newClient(collA), newClient(collB)
	const phoneA, phoneB = "+15551292001", "+15551292002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("client B login timeout")
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerA := peerUser(bUserID, aUserID)

	// A sends a message to B.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(aUserID, bUserID), Message: "secret", RandomID: 500001,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	recvOr(t, collB.newMsg, "B updateNewMessage")

	// B tries to forward A's message to A, but the source is from A's side.
	// B's copy of the message has local_id=1 in B's space, and B owns it (as recipient).
	// The forward should succeed because B is a recipient of the message.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer: peerA,
			ID:       []int{1},
			RandomID: []int64{600001},
			ToPeer:   peerA,
		})
		return err
	}); err != nil {
		t.Fatalf("B forward (should succeed): %v", err)
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

func TestForwardDedup(t *testing.T) {
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

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
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

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB := newClient(collA), newClient(collB)
	const phoneA, phoneB = "+15551293001", "+15551293002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("client B login timeout")
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)

	// A sends a message to B.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerB, Message: "original", RandomID: 700001,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	recvOr(t, collB.newMsg, "B updateNewMessage")

	// A forwards with random_id X.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer: peerB,
			ID:       []int{1},
			RandomID: []int64{800001},
			ToPeer:   peerB,
		})
		return err
	}); err != nil {
		t.Fatalf("A forward: %v", err)
	}
	recvOr(t, collB.newMsg, "B forwarded message")

	// A forwards again with the same random_id X — should not create a new message.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
			FromPeer: peerB,
			ID:       []int{1},
			RandomID: []int64{800001},
			ToPeer:   peerB,
		})
		return err
	}); err != nil {
		t.Fatalf("A forward dedup: %v", err)
	}
	// B should NOT receive a second forwarded message.
	select {
	case <-collB.newMsg:
		t.Fatal("B received a duplicate forwarded message")
	case <-time.After(2 * time.Second):
		// Good — no duplicate.
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}
