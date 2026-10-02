package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func testSmokePeerDisconnect(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneA, phoneB = "+15551048001", "+15551048002"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB)
	a1 := newSmokeClient(t, f, "A1", phoneA)
	a2 := newSmokeClient(t, f, "A2", phoneA)
	b1 := newSmokeClient(t, f, "B1", phoneB)
	b2 := newSmokeClient(t, f, "B2", phoneB)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, a1.id, 2, "A sessions", a1.lifecycle)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, b1.id, 2, "B sessions", b1.lifecycle)

	beforeA, err := f.store.State(f.ctx, a1.id)
	if err != nil {
		t.Fatalf("A initial state: %v", err)
	}
	beforeB, err := f.store.State(f.ctx, b1.id)
	if err != nil {
		t.Fatalf("B initial state: %v", err)
	}

	const randomID int64 = 20951144001
	const messageText = "peer-disconnect rollback probe"
	messageLock := lockSmokeMessages(t, f.ctx, f.dsn)
	messageResult := make(chan error, 1)
	go func() {
		messageResult <- a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			_, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peerUser(a1.id, b1.id), Message: messageText, RandomID: randomID,
			})
			return err
		})
	}()
	waitForSmokeMessagesLock(t, f.ctx, messageLock.observer, true)
	a1.disconnectClient(t)
	waitForSmokeMessagesLock(t, f.ctx, messageLock.observer, false)
	messageLock.release(t)
	select {
	case err := <-messageResult:
		if err == nil {
			t.Fatal("disconnected send unexpectedly returned a successful result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected send command did not finish")
	}

	for _, ownerID := range []int64{a1.id, b1.id} {
		rows, err := f.store.History(f.ctx, ownerID, store.PeerTypeUser, func() int64 {
			if ownerID == a1.id {
				return b1.id
			}
			return a1.id
		}(), 0, 10)
		if err != nil {
			t.Fatalf("history after canceled send for %d: %v", ownerID, err)
		}
		if len(rows) != 0 {
			t.Fatalf("canceled send left %d message rows for owner %d", len(rows), ownerID)
		}
	}
	for userID, before := range map[int64]store.State{a1.id: beforeA, b1.id: beforeB} {
		after, err := f.store.State(f.ctx, userID)
		if err != nil {
			t.Fatalf("state after canceled send for %d: %v", userID, err)
		}
		if after != before {
			t.Fatalf("canceled send changed user %d state: before %+v, after %+v", userID, before, after)
		}
	}
	for label, updates := range map[string]<-chan *tg.Message{
		"A2 managed updates": a2.seen.newMsg,
		"A2 live pushes":     a2.push.newMsg,
		"B2 managed updates": b2.seen.newMsg,
		"B2 live pushes":     b2.push.newMsg,
	} {
		assertNoMessageFor(t, f.ctx, updates, label)
	}
	for _, client := range []*smokeClient{a2, b2} {
		if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			_, err := api.AccountGetAuthorizations(ctx)
			return err
		}); err != nil {
			t.Fatalf("remaining client %s authorization check: %v", client.label, err)
		}
	}
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true)

	const chatTitle = "peer-disconnect chat completion"
	chatLock := lockSmokeMessages(t, f.ctx, f.dsn)
	chatResult := make(chan error, 1)
	go func() {
		chatResult <- b1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			_, err := api.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
				Title: chatTitle,
				Users: []tg.InputUserClass{inputUser(b1.id, a1.id)},
			})
			return err
		})
	}()
	waitForSmokeMessagesLock(t, f.ctx, chatLock.observer, true)
	var chatID int64
	if err := chatLock.tx.QueryRow(f.ctx, `SELECT id FROM chats WHERE title = $1`, chatTitle).Scan(&chatID); err != nil {
		t.Fatalf("createChat first transaction did not commit before announcement: %v", err)
	}
	b1.disconnectClient(t)
	if got := smokeMessagesLockCount(t, f.ctx, chatLock.observer); got == 0 {
		t.Fatal("peer disconnect canceled the committed chat announcement while it was blocked")
	}
	assertSmokeMessagesStayBlocked(t, f.ctx, chatLock.observer, 300*time.Millisecond)
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true)
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, true)
	if err := b2.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		_, err := api.AccountGetAuthorizations(ctx)
		return err
	}); err != nil {
		t.Fatalf("remaining B session authorization check: %v", err)
	}
	chatLock.release(t)
	waitForSmokeMessagesLock(t, f.ctx, chatLock.observer, false)
	select {
	case err := <-chatResult:
		if err == nil {
			t.Fatal("disconnected createChat unexpectedly returned a successful result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected createChat command did not finish")
	}

	serviceCtx, cancelService := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancelService()
	service, err := a2.seen.waitService(serviceCtx, &tg.MessageActionChatCreate{})
	if err != nil {
		t.Fatalf("A did not receive the completed chat announcement: %v", err)
	}
	action, ok := service.svc.Action.(*tg.MessageActionChatCreate)
	if !ok || action.Title != chatTitle {
		t.Fatalf("chat-create announcement action = %#v, want title %q", service.svc.Action, chatTitle)
	}
	if !hasChat(service.chats, chatID) {
		t.Fatalf("chat-create update omitted chat %d", chatID)
	}
	if _, err := b2.seen.waitService(serviceCtx, &tg.MessageActionChatCreate{}); err != nil {
		t.Fatalf("B's remaining session did not receive the chat announcement: %v", err)
	}
	for _, userID := range []int64{a1.id, b1.id} {
		snapshot, err := f.store.ChatHistoryForMemberSnapshot(f.ctx, userID, chatID, 0, 0, 10)
		if err != nil {
			t.Fatalf("completed chat history for %d: %v", userID, err)
		}
		if len(snapshot.Messages) != 1 || snapshot.Messages[0].Action != store.ChatActionCreate || snapshot.Messages[0].Text != chatTitle {
			t.Fatalf("completed chat history for %d = %+v, want one create announcement", userID, snapshot.Messages)
		}
	}
	afterChatA, err := f.store.State(f.ctx, a1.id)
	if err != nil {
		t.Fatalf("A state after committed chat: %v", err)
	}
	afterChatB, err := f.store.State(f.ctx, b1.id)
	if err != nil {
		t.Fatalf("B state after committed chat: %v", err)
	}
	if afterChatA.Pts != beforeA.Pts+1 || afterChatB.Pts != beforeB.Pts+1 {
		t.Fatalf("chat completion pts = A:%d B:%d, want A:%d B:%d", afterChatA.Pts, afterChatB.Pts, beforeA.Pts+1, beforeB.Pts+1)
	}
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true)
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, true)
	a2.stopClient(t)
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, false)
	b2.stopClient(t)
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, false)
}

type smokeMessagesLock struct {
	blocker  *pgx.Conn
	tx       pgx.Tx
	observer *pgx.Conn
	released bool
}

func reportSmokeMessagesLockError(t *testing.T, action string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", action, err)
	}
}

func lockSmokeMessages(t *testing.T, ctx context.Context, dsn string) *smokeMessagesLock {
	t.Helper()
	blocker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect message lock: %v", err)
	}
	tx, err := blocker.Begin(ctx)
	if err != nil {
		reportSmokeMessagesLockError(t, "close message lock connection", blocker.Close(context.Background()))
		t.Fatalf("begin message lock: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE messages IN ACCESS EXCLUSIVE MODE`); err != nil {
		reportSmokeMessagesLockError(t, "rollback message lock transaction", tx.Rollback(context.Background()))
		reportSmokeMessagesLockError(t, "close message lock connection", blocker.Close(context.Background()))
		t.Fatalf("lock messages table: %v", err)
	}
	observer, err := pgx.Connect(ctx, dsn)
	if err != nil {
		reportSmokeMessagesLockError(t, "rollback message lock transaction", tx.Rollback(context.Background()))
		reportSmokeMessagesLockError(t, "close message lock connection", blocker.Close(context.Background()))
		t.Fatalf("connect message lock observer: %v", err)
	}
	lock := &smokeMessagesLock{blocker: blocker, tx: tx, observer: observer}
	t.Cleanup(func() {
		if !lock.released {
			reportSmokeMessagesLockError(t, "rollback message lock transaction", lock.tx.Rollback(context.Background()))
		}
		reportSmokeMessagesLockError(t, "close message lock connection", lock.blocker.Close(context.Background()))
		reportSmokeMessagesLockError(t, "close message lock observer", lock.observer.Close(context.Background()))
	})
	return lock
}

func (l *smokeMessagesLock) release(t *testing.T) {
	t.Helper()
	if l.released {
		return
	}
	if err := l.tx.Rollback(context.Background()); err != nil {
		t.Fatalf("release messages table lock: %v", err)
	}
	l.released = true
}

func waitForSmokeMessagesLock(t *testing.T, ctx context.Context, observer *pgx.Conn, wantBlocked bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		count := smokeMessagesLockCount(t, waitCtx, observer)
		if (count > 0) == wantBlocked {
			return
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			t.Fatalf("messages lock activity blocked=%t, want %t", count > 0, wantBlocked)
		}
	}
}

func smokeMessagesLockCount(t *testing.T, ctx context.Context, observer *pgx.Conn) int {
	t.Helper()
	var count int
	err := observer.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'active' AND wait_event_type = 'Lock'
		AND query ILIKE '%messages%' AND pid <> pg_backend_pid()`).Scan(&count)
	if err != nil {
		t.Fatalf("inspect blocked message operations: %v", err)
	}
	return count
}

func assertSmokeMessagesStayBlocked(t *testing.T, ctx context.Context, observer *pgx.Conn, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := smokeMessagesLockCount(t, ctx, observer); got == 0 {
			t.Fatal("committed chat announcement stopped waiting before the blocker released")
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return
		case <-ctx.Done():
			t.Fatalf("checking blocked chat announcement: %v", ctx.Err())
		}
	}
}

func waitForSmokeOnline(t *testing.T, ctx context.Context, st *store.Store, userID int64, want bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		user, ok, err := st.UserByID(waitCtx, userID)
		if err != nil {
			t.Fatalf("read online status for %d: %v", userID, err)
		}
		if ok && user.IsOnline == want {
			return
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			t.Fatalf("user %d online=%t, want %t", userID, ok && user.IsOnline, want)
		}
	}
}
