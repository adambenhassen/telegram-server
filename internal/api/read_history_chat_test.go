package api_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestReadHistoryChatAdvancesAndReplaysReceipts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s,
		"+15551294001", "+15551294002", "+15551294003", "+15551294004",
	)
	listener := notificationListener(t, dsn)
	sender, reader, idleSender := users[0], users[2], users[3]
	other, err := s.CreateUser(ctx, "+15551294005")
	if err != nil {
		t.Fatalf("other user: %v", err)
	}

	// Make the reader's first group copy id differ from the sender's copy. The
	// sender's id 1 names this 1:1 message; the same fan-out is sender-local id 2.
	if _, _, _, _, err := s.SendMessage(ctx, sender.ID, other.ID, "1:1", 10, 0, 0); err != nil {
		t.Fatalf("seed sender 1:1: %v", err)
	}
	first := sendChatForReadHistory(t, s, chat.ID, sender.ID, 11)
	if first.LocalID != 2 {
		t.Fatalf("first sender local id = %d, want 2", first.LocalID)
	}
	readerFirst, ok, err := s.MessageByOwnerLocal(ctx, users[1].ID, 1)
	if err != nil || !ok || readerFirst.PeerType != store.PeerTypeChat {
		t.Fatalf("reader first copy = %+v ok=%v err=%v, want chat local id 1", readerFirst, ok, err)
	}

	// A current member reads the first message. Its receipt must use the sender's
	// local id, which is also what lets difference reconstruction name the chat.
	aBefore, err := s.State(ctx, sender.ID)
	if err != nil {
		t.Fatalf("sender state before first read: %v", err)
	}
	firstRead, err := api.ReadHistoryForTest(s, users[1].ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 1,
	})
	if err != nil {
		t.Fatalf("member readHistory: %v", err)
	}
	afterFirstRead, err := s.State(ctx, sender.ID)
	if err != nil {
		t.Fatalf("sender state after first read: %v", err)
	}
	firstUpdates, _, _, err := api.BuildUpdatesChatsForTest(s, sender.ID, aBefore.Pts)
	if err != nil {
		t.Fatalf("sender difference after first read: %v", err)
	}
	if len(firstUpdates) != 1 {
		t.Fatalf("sender read updates = %d, want 1: %v", len(firstUpdates), firstUpdates)
	}
	firstOut, ok := firstUpdates[0].(*tg.UpdateReadHistoryOutbox)
	if !ok {
		t.Fatalf("first sender update = %T, want UpdateReadHistoryOutbox", firstUpdates[0])
	}
	assertReadReceipt(t, firstOut.Peer, chat.ID, firstOut.MaxID, int(first.LocalID))
	if firstOut.Pts != afterFirstRead.Pts || firstOut.PtsCount != 1 {
		t.Fatalf("first sender update pts = %d/%d, want %d/1", firstOut.Pts, firstOut.PtsCount, afterFirstRead.Pts)
	}
	if _, ok := firstRead.(*tg.MessagesAffectedMessages); !ok {
		t.Fatalf("first read result = %T, want MessagesAffectedMessages", firstRead)
	}
	if got := waitForReadHistoryNotification(t, listener).Payload; got != strconv.FormatInt(users[1].ID, 10) {
		t.Fatalf("reader notification = %q, want %d", got, users[1].ID)
	}
	if got := waitForReadHistoryNotification(t, listener).Payload; got != strconv.FormatInt(sender.ID, 10) {
		t.Fatalf("sender notification = %q, want %d", got, sender.ID)
	}
	assertNoReadHistoryNotification(t, listener)

	// A second member sends a message the reader has not seen. The first
	// sender's marker is already at its message, so only the second sender gets
	// an outbox event for the next read. Two messages from that sender collapse
	// to its maximum local id in one receipt.
	second := sendChatForReadHistory(t, s, chat.ID, users[1].ID, 12)
	if second.LocalID != 2 {
		t.Fatalf("second sender local id = %d, want 2", second.LocalID)
	}
	third := sendChatForReadHistory(t, s, chat.ID, users[1].ID, 14)
	if third.LocalID != 3 {
		t.Fatalf("third sender local id = %d, want 3", third.LocalID)
	}
	if _, _, _, _, err := s.SendMessage(ctx, other.ID, reader.ID, "unrelated", 13, 0, 0); err != nil {
		t.Fatalf("seed reader's unrelated dialog: %v", err)
	}

	readerBefore, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("reader state before read: %v", err)
	}
	firstSenderBefore, err := s.State(ctx, sender.ID)
	if err != nil {
		t.Fatalf("first sender state before read: %v", err)
	}
	secondSenderBefore, err := s.State(ctx, users[1].ID)
	if err != nil {
		t.Fatalf("second sender state before read: %v", err)
	}
	idleBefore, err := s.State(ctx, idleSender.ID)
	if err != nil {
		t.Fatalf("idle sender state before read: %v", err)
	}

	groupBefore := apiDialog(t, s, reader.ID, store.PeerTypeChat, chat.ID)
	if groupBefore.TopMessage != 3 || groupBefore.ReadInboxMaxID != 0 || groupBefore.UnreadCount != 3 {
		t.Fatalf("reader group dialog before read = %+v, want top=3 inbox=0 unread=3", groupBefore)
	}
	unrelatedBefore := apiDialog(t, s, reader.ID, store.PeerTypeUser, other.ID)
	if unrelatedBefore.TopMessage != 4 {
		t.Fatalf("unrelated dialog top = %d, want 4", unrelatedBefore.TopMessage)
	}

	enc, err := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 99,
	})
	if err != nil {
		t.Fatalf("readHistory: %v", err)
	}
	affected, ok := enc.(*tg.MessagesAffectedMessages)
	if !ok {
		t.Fatalf("read result = %T, want MessagesAffectedMessages", enc)
	}
	if affected.Pts != readerBefore.Pts+1 || affected.PtsCount != 1 {
		t.Fatalf("read result pts = %d/%d, want %d/1", affected.Pts, affected.PtsCount, readerBefore.Pts+1)
	}

	groupAfter := apiDialog(t, s, reader.ID, store.PeerTypeChat, chat.ID)
	if groupAfter.TopMessage != groupBefore.TopMessage || groupAfter.ReadInboxMaxID != 3 || groupAfter.UnreadCount != 0 {
		t.Fatalf("reader group dialog after read = %+v, want top=3 inbox=3 unread=0", groupAfter)
	}
	reconnected, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reconnected.Close(); err != nil {
			t.Errorf("close reconnected store: %v", err)
		}
	})
	dialogsEnc, err := api.GetDialogsForTest(reconnected, reader.ID)
	if err != nil {
		t.Fatalf("reconnected getDialogs: %v", err)
	}
	dialogsReply, ok := dialogsEnc.(*tg.MessagesDialogs)
	if !ok {
		t.Fatalf("reconnected dialogs = %T, want MessagesDialogs", dialogsEnc)
	}
	var recoveredGroup bool
	for _, entry := range dialogsReply.Dialogs {
		dialog, ok := entry.(*tg.Dialog)
		if !ok {
			continue
		}
		peer, ok := dialog.Peer.(*tg.PeerChat)
		if ok && peer.ChatID == chat.ID {
			recoveredGroup = true
			if dialog.TopMessage != 3 || dialog.ReadInboxMaxID != 3 || dialog.UnreadCount != 0 {
				t.Fatalf("reconnected group dialog = %+v, want top=3 inbox=3 unread=0", dialog)
			}
		}
	}
	if !recoveredGroup {
		t.Fatalf("reconnected getDialogs omitted chat %d", chat.ID)
	}
	recoveredUpdates, _, _, err := api.BuildUpdatesChatsForTest(reconnected, reader.ID, readerBefore.Pts)
	if err != nil {
		t.Fatalf("reconnected difference: %v", err)
	}
	if len(recoveredUpdates) != 1 {
		t.Fatalf("reconnected updates = %d, want read receipt", len(recoveredUpdates))
	}
	if _, ok := recoveredUpdates[0].(*tg.UpdateReadHistoryInbox); !ok {
		t.Fatalf("reconnected update = %T, want UpdateReadHistoryInbox", recoveredUpdates[0])
	}
	unrelatedAfter := apiDialog(t, s, reader.ID, store.PeerTypeUser, other.ID)
	if unrelatedAfter.TopMessage != unrelatedBefore.TopMessage || unrelatedAfter.ReadInboxMaxID != 0 {
		t.Fatalf("unrelated dialog changed: before=%+v after=%+v", unrelatedBefore, unrelatedAfter)
	}

	readerUpdates, _, _, err := api.BuildUpdatesChatsForTest(s, reader.ID, readerBefore.Pts)
	if err != nil {
		t.Fatalf("reader difference: %v", err)
	}
	if len(readerUpdates) != 1 {
		t.Fatalf("reader difference updates = %d, want 1: %v", len(readerUpdates), readerUpdates)
	}
	readIn, ok := readerUpdates[0].(*tg.UpdateReadHistoryInbox)
	if !ok {
		t.Fatalf("reader update = %T, want UpdateReadHistoryInbox", readerUpdates[0])
	}
	assertReadReceipt(t, readIn.Peer, chat.ID, readIn.MaxID, 3)
	if readIn.Pts != affected.Pts || readIn.PtsCount != affected.PtsCount {
		t.Fatalf("reader update pts = %d/%d, want %d/%d", readIn.Pts, readIn.PtsCount, affected.Pts, affected.PtsCount)
	}
	if got := waitForReadHistoryNotification(t, listener).Payload; got != strconv.FormatInt(reader.ID, 10) {
		t.Fatalf("reader notification = %q, want %d", got, reader.ID)
	}

	secondSenderUpdates, _, _, err := api.BuildUpdatesChatsForTest(s, users[1].ID, secondSenderBefore.Pts)
	if err != nil {
		t.Fatalf("second sender difference: %v", err)
	}
	if len(secondSenderUpdates) != 1 {
		t.Fatalf("second sender updates = %d, want 1: %v", len(secondSenderUpdates), secondSenderUpdates)
	}
	secondOut, ok := secondSenderUpdates[0].(*tg.UpdateReadHistoryOutbox)
	if !ok {
		t.Fatalf("second sender update = %T, want UpdateReadHistoryOutbox", secondSenderUpdates[0])
	}
	assertReadReceipt(t, secondOut.Peer, chat.ID, secondOut.MaxID, int(third.LocalID))
	if got := waitForReadHistoryNotification(t, listener).Payload; got != strconv.FormatInt(users[1].ID, 10) {
		t.Fatalf("second sender notification = %q, want %d", got, users[1].ID)
	}

	firstSenderAfter, err := s.State(ctx, sender.ID)
	if err != nil {
		t.Fatalf("first sender state after read: %v", err)
	}
	if firstSenderAfter.Pts != firstSenderBefore.Pts {
		t.Fatalf("unchanged sender pts = %d, want %d", firstSenderAfter.Pts, firstSenderBefore.Pts)
	}
	firstSenderUpdates, _, _, err := api.BuildUpdatesChatsForTest(s, sender.ID, firstSenderBefore.Pts)
	if err != nil {
		t.Fatalf("first sender difference: %v", err)
	}
	if len(firstSenderUpdates) != 0 {
		t.Fatalf("unchanged sender updates = %v, want none", firstSenderUpdates)
	}
	idleAfter, err := s.State(ctx, idleSender.ID)
	if err != nil {
		t.Fatalf("idle sender state after read: %v", err)
	}
	if idleAfter.Pts != idleBefore.Pts {
		t.Fatalf("idle sender pts = %d, want %d", idleAfter.Pts, idleBefore.Pts)
	}
	idleUpdates, _, _, err := api.BuildUpdatesChatsForTest(s, idleSender.ID, idleBefore.Pts)
	if err != nil {
		t.Fatalf("idle sender difference: %v", err)
	}
	if len(idleUpdates) != 0 {
		t.Fatalf("idle sender updates = %v, want none", idleUpdates)
	}
	assertNoReadHistoryNotification(t, listener)

	// Repeating an older boundary is a strict no-op for every owner.
	readerAfter, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("reader state after read: %v", err)
	}
	enc, err = api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 2,
	})
	if err != nil {
		t.Fatalf("older readHistory: %v", err)
	}
	replay, ok := enc.(*tg.MessagesAffectedMessages)
	if !ok || replay.Pts != readerAfter.Pts || replay.PtsCount != 0 {
		t.Fatalf("older read result = %#v, want unchanged pts %d and pts_count 0", enc, readerAfter.Pts)
	}
	for _, u := range []store.User{reader, sender, users[1], idleSender} {
		before := readerAfter.Pts
		switch u.ID {
		case sender.ID:
			before = firstSenderAfter.Pts
		case users[1].ID:
			before = secondSenderBefore.Pts + 1
		case idleSender.ID:
			before = idleBefore.Pts
		}
		state, serr := s.State(ctx, u.ID)
		if serr != nil {
			t.Fatalf("state %d after replay: %v", u.ID, serr)
		}
		if state.Pts != before {
			t.Errorf("owner %d pts after replay = %d, want %d", u.ID, state.Pts, before)
		}
		updates, _, _, uerr := api.BuildUpdatesChatsForTest(s, u.ID, state.Pts)
		if uerr != nil {
			t.Fatalf("owner %d difference after replay: %v", u.ID, uerr)
		}
		if len(updates) != 0 {
			t.Errorf("owner %d got updates after replay: %v", u.ID, updates)
		}
	}
	assertNoReadHistoryNotification(t, listener)
}

func TestReadHistoryChatPreservesOutboxWhenOwnerIDMatchesChatID(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551294401", "+15551294402")
	reader, sender := users[0], users[1]
	if reader.ID != chat.ID {
		t.Fatalf("fixture user id %d != chat id %d", reader.ID, chat.ID)
	}
	_ = sendChatForReadHistory(t, s, chat.ID, sender.ID, 51)

	enc, err := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 1,
	})
	if err != nil {
		t.Fatalf("read group as same-numbered owner: %v", err)
	}
	if _, ok := enc.(*tg.MessagesAffectedMessages); !ok {
		t.Fatalf("read result = %T, want MessagesAffectedMessages", enc)
	}
	dialog := apiDialog(t, s, reader.ID, store.PeerTypeChat, chat.ID)
	if dialog.ReadInboxMaxID != 1 || dialog.ReadOutboxMaxID != 0 || dialog.UnreadCount != 0 {
		t.Fatalf("same-numbered chat dialog = %+v, want inbox=1 outbox=0 unread=0", dialog)
	}
}

func TestReadHistoryChatKeepsBoundAcrossConcurrentSend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551294501", "+15551294502", "+15551294503")
	reader, newSender := users[1], users[2]
	_ = sendChatForReadHistory(t, s, chat.ID, users[0].ID, 61)
	readerBefore, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("reader state: %v", err)
	}
	newSenderBefore, err := s.State(ctx, newSender.ID)
	if err != nil {
		t.Fatalf("new sender state: %v", err)
	}
	newSenderEventsBefore, err := s.EventsSince(ctx, newSender.ID, 0)
	if err != nil {
		t.Fatalf("new sender events before send: %v", err)
	}

	// Queue the send ahead of the read on the lowest owner lock. The read must
	// retain the bound it selected before either transaction acquired that lock.
	_, lockTx := lockOwnerForReadHistoryRace(t, dsn, users[0].ID)
	sendDone := make(chan error, 1)
	go func() {
		_, _, duplicate, sendErr := s.SendChatMessage(ctx, store.FanOut{
			ChatID: chat.ID, FromID: newSender.ID, Text: "later", RandomID: 62,
		})
		if sendErr == nil && duplicate {
			sendErr = errors.New("first concurrent send was marked duplicate")
		}
		sendDone <- sendErr
	}()
	waitForReadHistoryOwnerLocks(t, dsn, nil, 1)

	readDone := make(chan readHistoryCall, 1)
	go func() {
		enc, readErr := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 99,
		})
		readDone <- readHistoryCall{enc: enc, err: readErr}
	}()
	waitForReadHistoryOwnerLocks(t, dsn, readDone, 2)
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("release owner lock: %v", err)
	}
	if err := <-sendDone; err != nil {
		t.Fatalf("concurrent send: %v", err)
	}
	call := <-readDone
	if call.err != nil {
		t.Fatalf("reader readHistory: %v", call.err)
	}
	affected, ok := call.enc.(*tg.MessagesAffectedMessages)
	if !ok || affected.Pts != readerBefore.Pts+2 || affected.PtsCount != 1 {
		t.Fatalf("read result = %#v, want reader pts %d and count 1", call.enc, readerBefore.Pts+2)
	}
	readerDialog := apiDialog(t, s, reader.ID, store.PeerTypeChat, chat.ID)
	if readerDialog.TopMessage != 2 || readerDialog.ReadInboxMaxID != 1 || readerDialog.UnreadCount != 1 {
		t.Fatalf("reader dialog = %+v, want top=2 inbox=1 unread=1", readerDialog)
	}
	newSenderAfter := snapshotReadHistoryOwner(t, s, newSender.ID)
	if newSenderAfter.pts != newSenderBefore.Pts+1 || len(newSenderAfter.events) != len(newSenderEventsBefore)+1 {
		t.Fatalf("new sender state = %+v, want only its send event", newSenderAfter)
	}
	newSenderEvents, err := s.EventsSince(ctx, newSender.ID, newSenderBefore.Pts)
	if err != nil {
		t.Fatalf("new sender events: %v", err)
	}
	if len(newSenderEvents) != 1 || newSenderEvents[0].Type != store.EventNewMessage {
		t.Fatalf("new sender events after send = %+v, want only EventNewMessage", newSenderEvents)
	}
}

func TestReadHistoryChatRejectsUnknownAndNonMembersUniformly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551294101", "+15551294102")
	outsider, err := s.CreateUser(ctx, "+15551294103")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	removed, err := s.CreateUser(ctx, "+15551294104")
	if err != nil {
		t.Fatalf("removed user: %v", err)
	}
	if _, _, _, err := s.AddChatUser(ctx, chat.ID, removed.ID, users[0].ID); err != nil {
		t.Fatalf("add removed user: %v", err)
	}
	_ = sendChatForReadHistory(t, s, chat.ID, users[0].ID, 21)
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, removed.ID, users[0].ID); err != nil {
		t.Fatalf("remove user: %v", err)
	}

	listener := notificationListener(t, dsn)
	owners := []store.User{users[0], users[1], outsider, removed}
	before := make(map[int64]readHistoryOwnerState, len(owners))
	for _, owner := range owners {
		before[owner.ID] = snapshotReadHistoryOwner(t, s, owner.ID)
	}

	for _, tc := range []struct {
		name   string
		caller int64
		chatID int64
	}{
		{name: "absent chat", caller: outsider.ID, chatID: chat.ID + 10_000},
		{name: "non-member", caller: outsider.ID, chatID: chat.ID},
		{name: "removed reader", caller: removed.ID, chatID: chat.ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, readErr := api.ReadHistoryForTest(s, tc.caller, &tg.MessagesReadHistoryRequest{
				Peer: &tg.InputPeerChat{ChatID: tc.chatID}, MaxID: 100,
			})
			assertReadHistoryRPCError(t, readErr, 400, "PEER_ID_INVALID")
		})
	}

	for _, owner := range owners {
		assertReadHistoryOwnerState(t, s, owner.ID, before[owner.ID])
	}
	assertNoReadHistoryNotification(t, listener)
}

func TestReadHistoryChatRollsBackAfterLaterEventInsertFailure(t *testing.T) {
	t.Parallel()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551294601", "+15551294602", "+15551294603")
	reader, firstSender, failingSender := users[0], users[1], users[2]
	if firstSender.ID >= failingSender.ID {
		t.Fatalf("fixture sender ids = %d/%d, want target order", firstSender.ID, failingSender.ID)
	}
	_ = sendChatForReadHistory(t, s, chat.ID, firstSender.ID, 71)
	_ = sendChatForReadHistory(t, s, chat.ID, failingSender.ID, 72)
	listener := notificationListener(t, dsn)
	installReadEventFailure(t, dsn, failingSender.ID)

	owners := []store.User{reader, firstSender, failingSender}
	before := make(map[int64]readHistoryOwnerState, len(owners))
	for _, owner := range owners {
		before[owner.ID] = snapshotReadHistoryOwner(t, s, owner.ID)
	}

	_, err := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 99,
	})
	assertReadHistoryRPCError(t, err, 500, "INTERNAL")
	for _, owner := range owners {
		assertReadHistoryOwnerState(t, s, owner.ID, before[owner.ID])
	}
	assertNoReadHistoryNotification(t, listener)
}

func TestReadHistoryChatRechecksRemovedReaderAfterEarlyFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551294201", "+15551294202", "+15551294203")
	_ = sendChatForReadHistory(t, s, chat.ID, users[0].ID, 31)
	reader := users[1]
	before := snapshotReadHistoryOwner(t, s, reader.ID)
	listener := notificationListener(t, dsn)

	_, tx := lockChatForRemovalRace(t, dsn, chat.ID)
	result := make(chan readHistoryCall, 1)
	go func() {
		enc, err := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 1,
		})
		result <- readHistoryCall{enc: enc, err: err}
	}()
	waitForReadHistoryOwnerLock(t, dsn, result)

	if _, err := tx.Exec(ctx, `DELETE FROM chat_participants WHERE chat_id = $1 AND user_id = $2`, chat.ID, reader.ID); err != nil {
		t.Fatalf("remove reader participant: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit reader removal: %v", err)
	}
	call := <-result
	assertReadHistoryRPCError(t, call.err, 400, "PEER_ID_INVALID")
	assertReadHistoryOwnerState(t, s, reader.ID, before)
	assertNoReadHistoryNotification(t, listener)
}

func TestReadHistoryChatDoesNotReceiptToSenderRemovedAfterEarlyFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	users, chat := chatWith(t, s, "+15551294301", "+15551294302", "+15551294303")
	_ = sendChatForReadHistory(t, s, chat.ID, users[0].ID, 41)
	reader, sender := users[1], users[0]
	readerBefore := snapshotReadHistoryOwner(t, s, reader.ID)
	senderBefore := snapshotReadHistoryOwner(t, s, sender.ID)
	listener := notificationListener(t, dsn)

	_, tx := lockChatForRemovalRace(t, dsn, chat.ID)
	result := make(chan readHistoryCall, 1)
	go func() {
		enc, err := api.ReadHistoryForTest(s, reader.ID, &tg.MessagesReadHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chat.ID}, MaxID: 1,
		})
		result <- readHistoryCall{enc: enc, err: err}
	}()
	waitForReadHistoryOwnerLock(t, dsn, result)

	if _, err := tx.Exec(ctx, `DELETE FROM chat_participants WHERE chat_id = $1 AND user_id = $2`, chat.ID, sender.ID); err != nil {
		t.Fatalf("remove sender participant: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit sender removal: %v", err)
	}
	call := <-result
	if call.err != nil {
		t.Fatalf("current reader readHistory: %v", call.err)
	}
	affected, ok := call.enc.(*tg.MessagesAffectedMessages)
	if !ok || affected.Pts != readerBefore.pts+1 || affected.PtsCount != 1 {
		t.Fatalf("read result = %#v, want reader pts %d and count 1", call.enc, readerBefore.pts+1)
	}
	assertReadHistoryOwnerState(t, s, sender.ID, senderBefore)
	readerAfter := snapshotReadHistoryOwner(t, s, reader.ID)
	if readerAfter.pts != readerBefore.pts+1 || len(readerAfter.events) != len(readerBefore.events)+1 {
		t.Fatalf("reader state after read = %+v, want one inbox event after %+v", readerAfter, readerBefore)
	}
	if dialog := apiDialog(t, s, sender.ID, store.PeerTypeChat, chat.ID); dialog.ReadOutboxMaxID != 0 {
		t.Fatalf("removed sender read_outbox_max_id = %d, want 0", dialog.ReadOutboxMaxID)
	}
	notification := waitForReadHistoryNotification(t, listener)
	if notification.Payload != strconv.FormatInt(reader.ID, 10) {
		t.Fatalf("notification payload = %q, want reader %d only", notification.Payload, reader.ID)
	}
	assertNoReadHistoryNotification(t, listener)
}

func sendChatForReadHistory(t *testing.T, s *store.Store, chatID, senderID, randomID int64) store.Message {
	t.Helper()
	m, _, duplicate, err := s.SendChatMessage(context.Background(), store.FanOut{
		ChatID: chatID, FromID: senderID, Text: "group message", RandomID: randomID,
	})
	if err != nil || duplicate {
		t.Fatalf("send chat message: duplicate=%v err=%v", duplicate, err)
	}
	return m
}

func installReadEventFailure(t *testing.T, dsn string, ownerID int64) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect event failure injector: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanupCtx, `DROP TRIGGER IF EXISTS fail_group_read_event_for_sender ON message_events`); err != nil {
			t.Errorf("drop event failure trigger: %v", err)
		}
		if _, err := conn.Exec(cleanupCtx, `DROP FUNCTION IF EXISTS fail_group_read_event_for_sender()`); err != nil {
			t.Errorf("drop event failure function: %v", err)
		}
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close event failure injector: %v", err)
		}
	})

	functionSQL := fmt.Sprintf(`
CREATE FUNCTION fail_group_read_event_for_sender() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
  IF NEW.owner_id = %d AND NEW.type = %d THEN
    RAISE EXCEPTION 'injected group receipt event failure';
  END IF;
  RETURN NEW;
END;
$body$;`, ownerID, int16(store.EventReadOut))
	if _, err := conn.Exec(ctx, functionSQL); err != nil {
		t.Fatalf("create event failure function: %v", err)
	}
	if _, err := conn.Exec(ctx, `
CREATE TRIGGER fail_group_read_event_for_sender
BEFORE INSERT ON message_events
FOR EACH ROW EXECUTE FUNCTION fail_group_read_event_for_sender()`); err != nil {
		t.Fatalf("create event failure trigger: %v", err)
	}
}

func apiDialog(t *testing.T, s *store.Store, ownerID int64, peerType store.PeerType, peerID int64) store.Dialog {
	t.Helper()
	dialogs, err := s.Dialogs(context.Background(), ownerID, 0, 100)
	if err != nil {
		t.Fatalf("dialogs for %d: %v", ownerID, err)
	}
	for _, dialog := range dialogs {
		if dialog.PeerType == peerType && dialog.PeerID == peerID {
			return dialog
		}
	}
	t.Fatalf("no dialog for owner=%d peer_type=%d peer_id=%d in %+v", ownerID, peerType, peerID, dialogs)
	return store.Dialog{}
}

func assertReadReceipt(t *testing.T, peer tg.PeerClass, chatID int64, gotMax, wantMax int) {
	t.Helper()
	chatPeer, ok := peer.(*tg.PeerChat)
	if !ok || chatPeer.ChatID != chatID {
		t.Fatalf("read receipt peer = %#v, want PeerChat %d", peer, chatID)
	}
	if gotMax != wantMax {
		t.Fatalf("read receipt max_id = %d, want %d", gotMax, wantMax)
	}
}

type readHistoryOwnerState struct {
	pts     int
	events  []store.Event
	dialogs []store.Dialog
}

type readHistoryCall struct {
	enc bin.Encoder
	err error
}

func snapshotReadHistoryOwner(t *testing.T, s *store.Store, ownerID int64) readHistoryOwnerState {
	t.Helper()
	state, err := s.State(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("state %d: %v", ownerID, err)
	}
	events, err := s.EventsSince(context.Background(), ownerID, 0)
	if err != nil {
		t.Fatalf("events %d: %v", ownerID, err)
	}
	dialogs, err := s.Dialogs(context.Background(), ownerID, 0, 100)
	if err != nil {
		t.Fatalf("dialogs %d: %v", ownerID, err)
	}
	return readHistoryOwnerState{pts: state.Pts, events: events, dialogs: dialogs}
}

func assertReadHistoryOwnerState(t *testing.T, s *store.Store, ownerID int64, want readHistoryOwnerState) {
	t.Helper()
	got := snapshotReadHistoryOwner(t, s, ownerID)
	if got.pts != want.pts || !reflect.DeepEqual(got.events, want.events) || !reflect.DeepEqual(got.dialogs, want.dialogs) {
		t.Fatalf("owner %d state = %+v, want %+v", ownerID, got, want)
	}
}

func assertReadHistoryRPCError(t *testing.T, err error, code int, message string) {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != code || rpc.Message != message {
		t.Fatalf("RPC error = %v, want %d %s", err, code, message)
	}
}

func notificationListener(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("notification listener: %v", err)
	}
	if _, err := conn.Exec(context.Background(), "LISTEN "+store.ChannelUpdates); err != nil {
		t.Fatalf("listen for updates: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close notification listener: %v", err)
		}
	})
	return conn
}

func waitForReadHistoryNotification(t *testing.T, conn *pgx.Conn) *pgconn.Notification {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("wait for update notification: %v", err)
	}
	return notification
}

func assertNoReadHistoryNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if notification, err := conn.WaitForNotification(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected update notification %+v (err %v)", notification, err)
	}
}

func lockChatForRemovalRace(t *testing.T, dsn string, chatID int64) (*pgx.Conn, pgx.Tx) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect removal transaction: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin removal transaction: %v", err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rollback removal transaction: %v", err)
		}
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close removal transaction: %v", err)
		}
	})
	var lockedChatID int64
	if err := tx.QueryRow(ctx, "SELECT id FROM chats WHERE id = $1 FOR UPDATE", chatID).Scan(&lockedChatID); err != nil {
		t.Fatalf("lock chat row: %v", err)
	}
	rows, err := tx.Query(ctx, "SELECT user_id FROM chat_participants WHERE chat_id = $1 ORDER BY user_id", chatID)
	if err != nil {
		t.Fatalf("list participant locks: %v", err)
	}
	var ownerIDs []int64
	for rows.Next() {
		var ownerID int64
		if err := rows.Scan(&ownerID); err != nil {
			rows.Close()
			t.Fatalf("scan participant lock: %v", err)
		}
		ownerIDs = append(ownerIDs, ownerID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("participant locks: %v", err)
	}
	for _, ownerID := range ownerIDs {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", ownerID); err != nil {
			t.Fatalf("lock owner %d: %v", ownerID, err)
		}
	}
	return conn, tx
}

func waitForReadHistoryOwnerLock(t *testing.T, dsn string, result <-chan readHistoryCall) {
	t.Helper()
	waitForReadHistoryOwnerLocks(t, dsn, result, 1)
}

func waitForReadHistoryOwnerLocks(t *testing.T, dsn string, result <-chan readHistoryCall, minimum int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open lock observer: %v", err)
	}
	t.Cleanup(observer.Close)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		if err := observer.QueryRow(ctx, `
			SELECT count(*)::int
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND query LIKE '%pg_advisory_xact_lock%'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect advisory lock wait: %v", err)
		}
		if waiting >= minimum {
			return
		}
		select {
		case call := <-result:
			t.Fatalf("readHistory returned before waiting on owner locks: result=%#v err=%v", call.enc, call.err)
		case <-ctx.Done():
			t.Fatalf("readHistory saw %d advisory waiters, want %d", waiting, minimum)
		case <-ticker.C:
		}
	}
}

func lockOwnerForReadHistoryRace(t *testing.T, dsn string, ownerID int64) (*pgx.Conn, pgx.Tx) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect owner lock transaction: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin owner lock transaction: %v", err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rollback owner lock transaction: %v", err)
		}
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close owner lock transaction: %v", err)
		}
	})
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", ownerID); err != nil {
		t.Fatalf("lock owner %d: %v", ownerID, err)
	}
	return conn, tx
}
