package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func testSmokePeerDisconnect(t *testing.T) {
	t.Helper()
	f := newSmokeFixtureWithLifecycle(t, config.RegistrationClosed, true)
	const phoneA, phoneB = "+15551048001", "+15551048002"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB)
	a1 := newSmokeClient(t, f, "A1", phoneA)
	a2 := newSmokeClient(t, f, "A2", phoneA)
	b1 := newSmokeClient(t, f, "B1", phoneB)
	b2 := newSmokeClient(t, f, "B2", phoneB)
	waitForDistinctAuthKeysWithCallsite(t, f.ctx, f.registry, a1.id, 2, "A sessions", a1.lifecycle, "peer-disconnect.distinct-a-keys")
	waitForDistinctAuthKeysWithCallsite(t, f.ctx, f.registry, b1.id, 2, "B sessions", b1.lifecycle, "peer-disconnect.distinct-b-keys")

	beforeA, err := f.store.State(f.ctx, a1.id)
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.initial-state-a] A initial state: %v", err)
	}
	beforeB, err := f.store.State(f.ctx, b1.id)
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.initial-state-b] B initial state: %v", err)
	}

	const randomID int64 = 20951144001
	const messageText = "peer-disconnect rollback probe"
	messageLock := lockSmokeOwner(t, f.ctx, f.dsn, min(a1.id, b1.id), "peer-disconnect.message-owner-lock")
	// The blocker targets writes only. Other connections must retain their
	// message reads while the sending transaction is held uncommitted.
	readCtx, cancelRead := context.WithTimeout(f.ctx, time.Second)
	_, err = f.store.History(readCtx, a2.id, store.PeerTypeUser, b1.id, 0, 10)
	cancelRead()
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.unrelated-message-read] owner write blocker prevented unrelated message reads: %v", err)
	}
	messageResult := make(chan error, 1)
	go func() {
		messageResult <- a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			_, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peerUser(a1.id, b1.id), Message: messageText, RandomID: randomID,
			})
			return err
		})
	}()
	waitForSmokeOwnerLock(t, f.ctx, messageLock, true, "peer-disconnect.message-lock-acquired")
	a1.disconnectClient(t, "peer-disconnect.send-client-disconnect")
	waitForSmokeOwnerLock(t, f.ctx, messageLock, false, "peer-disconnect.message-blocker-clear-confirm")
	messageLock.release(t, "peer-disconnect.message-lock-release")
	select {
	case err := <-messageResult:
		if err == nil {
			t.Fatal("[assert:peer-disconnect.send-canceled] disconnected send unexpectedly returned a successful result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("[assert:peer-disconnect.send-finished] disconnected send command did not finish")
	}

	for _, ownerID := range []int64{a1.id, b1.id} {
		rows, err := f.store.History(f.ctx, ownerID, store.PeerTypeUser, func() int64 {
			if ownerID == a1.id {
				return b1.id
			}
			return a1.id
		}(), 0, 10)
		if err != nil {
			t.Fatalf("[assert:peer-disconnect.canceled-send-history] history after canceled send for %d: %v", ownerID, err)
		}
		if len(rows) != 0 {
			t.Fatalf("[assert:peer-disconnect.canceled-send-empty-history] canceled send left %d message rows for owner %d", len(rows), ownerID)
		}
	}
	for userID, before := range map[int64]store.State{a1.id: beforeA, b1.id: beforeB} {
		after, err := f.store.State(f.ctx, userID)
		if err != nil {
			t.Fatalf("[assert:peer-disconnect.canceled-send-state-read] state after canceled send for %d: %v", userID, err)
		}
		if after != before {
			t.Fatalf("[assert:peer-disconnect.canceled-send-state-unchanged] canceled send changed user %d state: before %+v, after %+v", userID, before, after)
		}
	}
	for label, updates := range map[string]<-chan *tg.Message{
		"A2 managed updates": a2.seen.newMsg,
		"A2 live pushes":     a2.push.newMsg,
		"B2 managed updates": b2.seen.newMsg,
		"B2 live pushes":     b2.push.newMsg,
	} {
		assertNoMessageForWithCallsite(t, f.ctx, updates, label, "peer-disconnect.unexpected-update-drain")
	}
	for _, client := range []*smokeClient{a2, b2} {
		if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			_, err := api.AccountGetAuthorizations(ctx)
			return err
		}); err != nil {
			t.Fatalf("[assert:peer-disconnect.remaining-send-session-auth] remaining client %s authorization check: %v", client.label, err)
		}
	}
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true, "peer-disconnect.sender-remains-online-after-send")

	const chatTitle = "peer-disconnect chat completion"
	chatLock := lockSmokeOwner(t, f.ctx, f.dsn, min(a1.id, b1.id), "peer-disconnect.chat-owner-lock")
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
	waitForSmokeOwnerLock(t, f.ctx, chatLock, true, "peer-disconnect.chat-lock-acquired")
	var chatID int64
	if err := chatLock.tx.QueryRow(f.ctx, `SELECT id FROM chats WHERE title = $1`, chatTitle).Scan(&chatID); err != nil {
		t.Fatalf("[assert:peer-disconnect.chat-first-commit-observed] createChat first transaction did not commit before announcement: %v", err)
	}
	b1.disconnectClient(t, "peer-disconnect.chat-client-disconnect")
	if got := smokeOwnerLockCount(t, f.ctx, chatLock, "peer-disconnect.chat-announcement-lock-probe"); got == 0 {
		t.Fatal("[assert:peer-disconnect.chat-announcement-blocked-after-disconnect] peer disconnect canceled the committed chat announcement while it was blocked")
	}
	assertSmokeOwnerStaysBlocked(t, f.ctx, chatLock, 300*time.Millisecond, "peer-disconnect.chat-announcement-stays-blocked")
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true, "peer-disconnect.a-remains-online-during-chat")
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, true, "peer-disconnect.b-remains-online-during-chat")
	if err := b2.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		_, err := api.AccountGetAuthorizations(ctx)
		return err
	}); err != nil {
		t.Fatalf("[assert:peer-disconnect.remaining-chat-session-auth] remaining B session authorization check: %v", err)
	}
	chatLock.release(t, "peer-disconnect.chat-lock-release")
	waitForSmokeOwnerLock(t, f.ctx, chatLock, false, "peer-disconnect.chat-blocker-clear-confirm")
	select {
	case err := <-chatResult:
		if err == nil {
			t.Fatal("[assert:peer-disconnect.chat-command-canceled] disconnected createChat unexpectedly returned a successful result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("[assert:peer-disconnect.chat-command-finished] disconnected createChat command did not finish")
	}

	serviceCtx, cancelService := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancelService()
	service, err := a2.seen.waitService(serviceCtx, &tg.MessageActionChatCreate{})
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.a-receives-chat-announcement] A did not receive the completed chat announcement: %v", err)
	}
	action, ok := service.svc.Action.(*tg.MessageActionChatCreate)
	if !ok || action.Title != chatTitle {
		t.Fatalf("[assert:peer-disconnect.chat-announcement-action] chat-create announcement action = %#v, want title %q", service.svc.Action, chatTitle)
	}
	if !hasChat(service.chats, chatID) {
		t.Fatalf("[assert:peer-disconnect.chat-announcement-chat] chat-create update omitted chat %d", chatID)
	}
	if _, err := b2.seen.waitService(serviceCtx, &tg.MessageActionChatCreate{}); err != nil {
		t.Fatalf("[assert:peer-disconnect.b-receives-chat-announcement] B's remaining session did not receive the chat announcement: %v", err)
	}
	for _, userID := range []int64{a1.id, b1.id} {
		snapshot, err := f.store.ChatHistoryForMemberSnapshot(f.ctx, userID, chatID, 0, 0, 10)
		if err != nil {
			t.Fatalf("[assert:peer-disconnect.completed-chat-history-read] completed chat history for %d: %v", userID, err)
		}
		if len(snapshot.Messages) != 1 || snapshot.Messages[0].Action != store.ChatActionCreate || snapshot.Messages[0].Text != chatTitle {
			t.Fatalf("[assert:peer-disconnect.completed-chat-history-content] completed chat history for %d = %+v, want one create announcement", userID, snapshot.Messages)
		}
	}
	afterChatA, err := f.store.State(f.ctx, a1.id)
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.chat-state-a-read] A state after committed chat: %v", err)
	}
	afterChatB, err := f.store.State(f.ctx, b1.id)
	if err != nil {
		t.Fatalf("[assert:peer-disconnect.chat-state-b-read] B state after committed chat: %v", err)
	}
	if afterChatA.Pts != beforeA.Pts+1 || afterChatB.Pts != beforeB.Pts+1 {
		t.Fatalf("[assert:peer-disconnect.chat-completion-pts] chat completion pts = A:%d B:%d, want A:%d B:%d", afterChatA.Pts, afterChatB.Pts, beforeA.Pts+1, beforeB.Pts+1)
	}
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, true, "peer-disconnect.a-remains-online-after-chat")
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, true, "peer-disconnect.b-remains-online-after-chat")
	a2.stopClient(t)
	waitForSmokeOnline(t, f.ctx, f.store, a1.id, false, "peer-disconnect.a-offline-after-last-session")
	b2.stopClient(t)
	waitForSmokeOnline(t, f.ctx, f.store, b1.id, false, "peer-disconnect.b-offline-after-last-session")
}

type smokeOwnerLock struct {
	blocker  *pgx.Conn
	tx       pgx.Tx
	observer *pgx.Conn
	released bool
}

func reportSmokeOwnerLockError(t *testing.T, action, callsiteID string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("[assert:%s/peer-disconnect.owner-lock-cleanup-failure] %s: %v", callsiteID, action, err)
	}
}

func lockSmokeOwner(t *testing.T, ctx context.Context, dsn string, ownerID int64, callsiteID string) *smokeOwnerLock {
	t.Helper()
	blocker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-connect] connect owner lock: %v", callsiteID, err)
	}
	tx, err := blocker.Begin(ctx)
	if err != nil {
		reportSmokeOwnerLockError(t, "close owner lock connection", "peer-disconnect.owner-lock-close-after-begin", blocker.Close(context.Background()))
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-begin] begin owner lock: %v", callsiteID, err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ownerID); err != nil {
		reportSmokeOwnerLockError(t, "rollback owner lock transaction", "peer-disconnect.owner-lock-rollback-after-lock-error", tx.Rollback(context.Background()))
		reportSmokeOwnerLockError(t, "close owner lock connection", "peer-disconnect.owner-lock-close-after-lock-error", blocker.Close(context.Background()))
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-acquire] lock owner writes: %v", callsiteID, err)
	}
	observer, err := pgx.Connect(ctx, dsn)
	if err != nil {
		reportSmokeOwnerLockError(t, "rollback owner lock transaction", "peer-disconnect.owner-lock-rollback-after-observer-error", tx.Rollback(context.Background()))
		reportSmokeOwnerLockError(t, "close owner lock connection", "peer-disconnect.owner-lock-close-after-observer-error", blocker.Close(context.Background()))
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-observer-connect] connect owner lock observer: %v", callsiteID, err)
	}
	lock := &smokeOwnerLock{blocker: blocker, tx: tx, observer: observer}
	t.Cleanup(func() {
		if !lock.released {
			reportSmokeOwnerLockError(t, "rollback owner lock transaction", "peer-disconnect.owner-lock-cleanup-rollback", lock.tx.Rollback(context.Background()))
		}
		reportSmokeOwnerLockError(t, "close owner lock connection", "peer-disconnect.owner-lock-cleanup-blocker-close", lock.blocker.Close(context.Background()))
		reportSmokeOwnerLockError(t, "close owner lock observer", "peer-disconnect.owner-lock-cleanup-observer-close", lock.observer.Close(context.Background()))
	})
	return lock
}

func (l *smokeOwnerLock) release(t *testing.T, callsiteID string) {
	t.Helper()
	if l.released {
		return
	}
	if err := l.tx.Rollback(context.Background()); err != nil {
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-release] release owner write lock: %v", callsiteID, err)
	}
	l.released = true
}

func waitForSmokeOwnerLock(t *testing.T, ctx context.Context, lock *smokeOwnerLock, wantBlocked bool, callsiteID string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		count := smokeOwnerLockCount(t, waitCtx, lock, callsiteID)
		if (count > 0) == wantBlocked {
			return
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			t.Fatalf("[assert:%s/peer-disconnect.owner-lock-state] owner lock activity blocked=%t, want %t", callsiteID, count > 0, wantBlocked)
		}
	}
}

func smokeOwnerLockCount(t *testing.T, ctx context.Context, lock *smokeOwnerLock, callsiteID string) int {
	t.Helper()
	var count int
	err := lock.observer.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'active' AND wait_event_type = 'Lock'
		AND wait_event = 'advisory' AND $1::integer = ANY(pg_blocking_pids(pid))`,
		lock.blocker.PgConn().PID()).Scan(&count)
	if err != nil {
		t.Fatalf("[assert:%s/peer-disconnect.owner-lock-inspection] inspect blocked owner writes: %v", callsiteID, err)
	}
	return count
}

func assertSmokeOwnerStaysBlocked(t *testing.T, ctx context.Context, lock *smokeOwnerLock, duration time.Duration, callsiteID string) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := smokeOwnerLockCount(t, ctx, lock, "peer-disconnect.chat-announcement-hold-probe"); got == 0 {
			t.Fatalf("[assert:%s/peer-disconnect.chat-announcement-released-early] committed chat announcement stopped waiting before the blocker released", callsiteID)
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return
		case <-ctx.Done():
			t.Fatalf("[assert:%s/peer-disconnect.chat-announcement-hold-context] checking blocked chat announcement: %v", callsiteID, ctx.Err())
		}
	}
}

func waitForSmokeOnline(t *testing.T, ctx context.Context, st *store.Store, userID int64, want bool, callsiteID string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		user, ok, err := st.UserByID(waitCtx, userID)
		if err != nil {
			t.Fatalf("[assert:%s/peer-disconnect.online-status-read] read online status for %d: %v", callsiteID, userID, err)
		}
		if ok && user.IsOnline == want {
			return
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			t.Fatalf("[assert:%s/peer-disconnect.online-state-mismatch] user %d online=%t, want %t", callsiteID, userID, ok && user.IsOnline, want)
		}
	}
}
