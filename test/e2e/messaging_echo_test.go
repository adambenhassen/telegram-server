package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMessagingSenderSessionEchoSuppression(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	registry, stop := bootServerWithRegistry(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	newClient := func(collector *updateCollector, sess *session.StorageMemory) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
			UpdateHandler:  collector,
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

	const phoneA, phoneB, phoneC = "+15551282501", "+15551282502", "+15551282503"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB, phoneC)

	type runningClient struct {
		cmds chan command
		err  chan error
		id   int64
	}
	startClient := func(phone string, client *telegram.Client) *runningClient {
		run := &runningClient{cmds: make(chan command), err: make(chan error, 1)}
		ids := make(chan int64, 1)
		go func() { run.err <- runInteractive(ctx, client, flowFor(phone), ids, run.cmds) }()
		select {
		case run.id = <-ids:
		case <-ctx.Done():
			t.Fatalf("login %s timeout: %v", phone, ctx.Err())
		}
		return run
	}
	exec := func(run *runningClient, fn func(context.Context, *tg.Client) error) error {
		done := make(chan error, 1)
		select {
		case run.cmds <- command{fn: fn, done: done}:
		case <-ctx.Done():
			t.Fatalf("command enqueue timeout: %v", ctx.Err())
		}
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	stopClient := func(run *runningClient) {
		close(run.cmds)
		if err := <-run.err; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("client run: %v", err)
		}
	}

	sessB1, sessB2 := &session.StorageMemory{}, &session.StorageMemory{}
	collB1, collB2 := newUpdateCollector(), newUpdateCollector()
	b1 := startClient(phoneB, newClient(collB1, sessB1))
	b2 := startClient(phoneB, newClient(collB2, sessB2))
	collA, collC := newUpdateCollector(), newUpdateCollector()
	a := startClient(phoneA, newClient(collA, &session.StorageMemory{}))
	c := startClient(phoneC, newClient(collC, &session.StorageMemory{}))

	conns := registry.Conns(b1.id)
	if len(conns) != 2 {
		t.Fatalf("B live connections = %d, want 2", len(conns))
	}
	if conns[0].AuthKeyID() == 0 || conns[1].AuthKeyID() == 0 || conns[0].AuthKeyID() == conns[1].AuthKeyID() {
		t.Fatalf("B auth key ids = %d, %d; want distinct nonzero keys", conns[0].AuthKeyID(), conns[1].AuthKeyID())
	}

	if _, _, _, _, err := st.SendMessage(ctx, a.id, a.id, "A saved id seed", 82500, 0, 0); err != nil {
		t.Fatalf("seed A saved message: %v", err)
	}
	var warmup tg.UpdatesClass
	if err := exec(a, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a.id, b1.id), Message: "A to B warmup", RandomID: 82501,
		})
		warmup = res
		return err
	}); err != nil {
		t.Fatalf("A warmup send: %v", err)
	}
	warmupMessage, warmupPts, ok := outgoingMessage(t, warmup, "A to B warmup")
	if !ok {
		t.Fatal("A warmup RPC result omitted its outgoing message")
	}
	if warmupPts != 2 {
		t.Fatalf("A warmup RPC pts = %d, want 2 after its saved message", warmupPts)
	}
	b1Warmup := recvOrCtx(t, ctx, collB1.newMsg, "first B session incoming warmup")
	if b1Warmup.Message != "A to B warmup" || b1Warmup.Out {
		t.Fatalf("first B session warmup = %+v, want incoming A message", b1Warmup)
	}
	if pts := recvOrCtx(t, ctx, collB1.points, "first B session incoming warmup pts"); pts != 1 {
		t.Fatalf("first B session warmup pts = %d, want 1", pts)
	}
	b2Warmup := recvOrCtx(t, ctx, collB2.newMsg, "second B session incoming warmup")
	if b2Warmup.Message != "A to B warmup" || b2Warmup.Out {
		t.Fatalf("second B session warmup = %+v, want incoming A message", b2Warmup)
	}
	if pts := recvOrCtx(t, ctx, collB2.points, "second B session incoming warmup pts"); pts != 1 {
		t.Fatalf("second B session warmup pts = %d, want 1", pts)
	}
	if warmupMessage.ID == b2Warmup.ID {
		t.Fatalf("A and B warmup ids unexpectedly equal: A=%d B=%d", warmupMessage.ID, b2Warmup.ID)
	}
	if got := recvOrCtx(t, ctx, collA.newMsg, "A saved message event"); got.Message != "A saved id seed" || !got.Out {
		t.Fatalf("A saved message event = %+v", got)
	}
	if pts := recvOrCtx(t, ctx, collA.points, "A saved message pts"); pts != 1 {
		t.Fatalf("A saved message pts = %d, want 1", pts)
	}
	if got := takeMessage(t, collA.newMsg); got != nil {
		t.Fatalf("originating A auth key received its own send echo: %+v", got)
	}

	var sendResult tg.UpdatesClass
	if err := exec(b1, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(b1.id, a.id), Message: "sender echo target", RandomID: 82502,
		})
		sendResult = res
		return err
	}); err != nil {
		t.Fatalf("B send to A: %v", err)
	}
	senderMessage, senderPts, ok := outgoingMessage(t, sendResult, "sender echo target")
	if !ok {
		t.Fatal("sendMessage RPC result omitted its outgoing message")
	}
	if senderPts != 2 {
		t.Fatalf("sender RPC pts = %d, want 2 after A's message", senderPts)
	}
	secondSessionMessage := recvOrCtx(t, ctx, collB2.newMsg, "second B session outgoing update")
	if secondSessionMessage.Message != "sender echo target" || !secondSessionMessage.Out || secondSessionMessage.ID != senderMessage.ID {
		t.Fatalf("second B session update = %+v, want outgoing id %d", secondSessionMessage, senderMessage.ID)
	}
	if pts := recvOrCtx(t, ctx, collB2.points, "second B session outgoing pts"); pts != 2 {
		t.Fatalf("second B session outgoing pts = %d, want 2", pts)
	}
	recipientMessage := recvOrCtx(t, ctx, collA.newMsg, "A incoming update")
	if recipientMessage.Message != "sender echo target" || recipientMessage.Out {
		t.Fatalf("A incoming message = %+v", recipientMessage)
	}
	if recipientMessage.ID == senderMessage.ID {
		t.Fatalf("owner-local message ids unexpectedly equal: A=%d B=%d", recipientMessage.ID, senderMessage.ID)
	}
	if pts := recvOrCtx(t, ctx, collA.points, "A incoming pts"); pts != 3 {
		t.Fatalf("A incoming pts = %d, want 3", pts)
	}

	// The read uses A's owner-local id. B must receive the mirrored sender id,
	// and an oversized request must not move B's marker beyond that row.
	read := func(maxID int) error {
		return exec(a, func(ctx context.Context, api *tg.Client) error {
			_, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
				Peer: peerUser(a.id, b1.id), MaxID: maxID,
			})
			return err
		})
	}
	if err := read(recipientMessage.ID); err != nil {
		t.Fatalf("A readHistory through its local id: %v", err)
	}
	if got := recvOrCtx(t, ctx, collB2.readOutbox, "B mirrored read marker"); got != senderMessage.ID {
		t.Fatalf("B outbox marker = %d, want sender-local id %d", got, senderMessage.ID)
	}
	if err := read(int(1<<31 - 1)); err != nil {
		t.Fatalf("A oversized readHistory: %v", err)
	}
	if got := recvOrCtx(t, ctx, collB2.readOutbox, "B oversized read marker"); got != senderMessage.ID {
		t.Fatalf("oversized read advanced B outbox marker to %d, want %d", got, senderMessage.ID)
	}
	if got := takeMessage(t, collB1.newMsg); got != nil {
		t.Fatalf("originating B auth key received duplicate updateNewMessage: %+v", got)
	}

	checkHistory := func(run *runningClient, viewerID, peerID int64) {
		t.Helper()
		if err := exec(run, func(ctx context.Context, api *tg.Client) error {
			res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerUser(viewerID, peerID), Limit: 10})
			if err != nil {
				return err
			}
			history, ok := res.(*tg.MessagesMessages)
			if !ok {
				return fmt.Errorf("history response = %T", res)
			}
			if len(history.Messages) != 2 {
				return fmt.Errorf("history messages = %d, want 2", len(history.Messages))
			}
			found := map[string]bool{}
			for _, class := range history.Messages {
				msg, ok := class.(*tg.Message)
				if !ok {
					return fmt.Errorf("history message = %+v", class)
				}
				if msg.Message == "A to B warmup" || msg.Message == "sender echo target" {
					found[msg.Message] = true
				}
				if viewerID == a.id && msg.Message == "A to B warmup" && !msg.Out {
					return fmt.Errorf("A warmup history message is not outgoing: %+v", msg)
				}
				if viewerID == a.id && msg.Message == "sender echo target" && msg.Out {
					return fmt.Errorf("A target history message is outgoing: %+v", msg)
				}
				if viewerID == b1.id && msg.Message == "A to B warmup" && msg.Out {
					return fmt.Errorf("B warmup history message is outgoing: %+v", msg)
				}
				if viewerID == b1.id && msg.Message == "sender echo target" && !msg.Out {
					return fmt.Errorf("B target history message is incoming: %+v", msg)
				}
			}
			if !found["A to B warmup"] || !found["sender echo target"] {
				return fmt.Errorf("history messages missing round trip: %v", found)
			}
			return nil
		}); err != nil {
			t.Fatalf("history for %d with peer %d: %v", viewerID, peerID, err)
		}
	}
	checkHistory(a, a.id, b1.id)
	checkHistory(b2, b1.id, a.id)

	if got := takeMessage(t, collB1.newMsg); got != nil {
		t.Fatalf("originating B auth key later received a duplicate updateNewMessage: %+v", got)
	}
	if got := takeMessage(t, collC.newMsg); got != nil {
		t.Fatalf("C received an A/B message: %+v", got)
	}
	if got := takeReadOutbox(t, collC.readOutbox); got != nil {
		t.Fatalf("C received an A/B read marker: %d", *got)
	}

	stopClient(b1)
	var senderDifference tg.UpdatesDifferenceClass
	b1Reconnected := startClient(phoneB, newClient(collB1, sessB1))
	if err := exec(b1Reconnected, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		senderDifference = diff
		return err
	}); err != nil {
		t.Fatalf("B getDifference after reconnect: %v", err)
	}
	full, ok := senderDifference.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("B difference = %T, want *tg.UpdatesDifference", senderDifference)
	}
	if len(full.NewMessages) != 2 {
		t.Fatalf("B difference new messages = %d, want incoming warmup and sent message", len(full.NewMessages))
	}
	pts := []int{1, senderPts}
	foundDifferenceMessages := map[string]bool{}
	for _, class := range full.NewMessages {
		msg, ok := class.(*tg.Message)
		if !ok {
			t.Fatalf("B backfill message = %+v", class)
		}
		if msg.Message == "A to B warmup" && msg.Out {
			t.Fatalf("B backfill warmup unexpectedly outgoing: %+v", msg)
		}
		if msg.Message == "sender echo target" && !msg.Out {
			t.Fatalf("B backfill target unexpectedly incoming: %+v", msg)
		}
		foundDifferenceMessages[msg.Message] = true
		if msg.Message == "sender echo target" && msg.ID != senderMessage.ID {
			t.Fatalf("B backfill id = %d, want %d", msg.ID, senderMessage.ID)
		}
	}
	if !foundDifferenceMessages["A to B warmup"] || !foundDifferenceMessages["sender echo target"] {
		t.Fatalf("B difference messages = %v", foundDifferenceMessages)
	}
	if full.State.Pts != 4 {
		t.Fatalf("B backfill state pts = %d, want contiguous head 4", full.State.Pts)
	}
	readMarkers := 0
	for _, update := range full.OtherUpdates {
		if outbox, ok := update.(*tg.UpdateReadHistoryOutbox); ok {
			readMarkers++
			if outbox.MaxID != senderMessage.ID {
				t.Fatalf("B backfilled read marker = %d, want %d", outbox.MaxID, senderMessage.ID)
			}
			pts = append(pts, outbox.Pts)
		}
	}
	if readMarkers != 2 {
		t.Fatalf("B read outbox updates = %d, want 2", readMarkers)
	}
	if len(pts) != 4 || pts[0] != 1 || pts[1] != 2 || pts[2] != 3 || pts[3] != 4 {
		t.Fatalf("B backfill pts = %v, want [1 2 3 4]", pts)
	}

	if err := exec(c, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		if full, ok := diff.(*tg.UpdatesDifference); ok && (len(full.NewMessages) != 0 || len(full.OtherUpdates) != 0) {
			return fmt.Errorf("C difference exposed A/B events: messages=%d updates=%d", len(full.NewMessages), len(full.OtherUpdates))
		}
		if _, ok := diff.(*tg.UpdatesDifferenceEmpty); !ok {
			return fmt.Errorf("C difference = %T, want empty", diff)
		}
		return nil
	}); err != nil {
		t.Fatalf("C isolation difference: %v", err)
	}

	stopClient(b1Reconnected)
	stopClient(b2)
	stopClient(a)
	stopClient(c)
}

func outgoingMessage(t *testing.T, result tg.UpdatesClass, text string) (*tg.Message, int, bool) {
	t.Helper()
	updates, ok := result.(*tg.Updates)
	if !ok {
		return nil, 0, false
	}
	for _, update := range updates.Updates {
		message, ok := update.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		msg, ok := message.Message.(*tg.Message)
		if ok && msg.Message == text && msg.Out {
			return msg, message.Pts, true
		}
	}
	return nil, 0, false
}

func takeMessage(t *testing.T, updates <-chan *tg.Message) *tg.Message {
	t.Helper()
	select {
	case message := <-updates:
		return message
	default:
		return nil
	}
}

func takeReadOutbox(t *testing.T, updates <-chan int) *int {
	t.Helper()
	select {
	case marker := <-updates:
		return &marker
	default:
		return nil
	}
}
