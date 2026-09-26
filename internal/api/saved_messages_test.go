package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSavedMessagesSelfPeers(t *testing.T) {
	for _, tc := range []struct {
		name string
		peer func(int64) tg.InputPeerClass
	}{
		{name: "InputPeerSelf", peer: func(int64) tg.InputPeerClass { return &tg.InputPeerSelf{} }},
		{name: "InputPeerUser", peer: func(id int64) tg.InputPeerClass { return api.InputPeerUser(id, id) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := openStore(t)
			user, err := s.CreateUser(ctx, "+15551249011")
			if err != nil {
				t.Fatalf("create user: %v", err)
			}

			before, err := api.GetDialogsForTest(s, user.ID)
			if err != nil {
				t.Fatalf("empty dialogs: %v", err)
			}
			beforeDialogs, ok := before.(*tg.MessagesDialogs)
			if !ok || len(beforeDialogs.Dialogs) != 0 {
				t.Fatalf("initial dialogs = %T %+v, want an empty dialog list", before, before)
			}

			peer := tc.peer(user.ID)
			first, err := api.SendMessageForTest(s, user.ID, &tg.MessagesSendMessageRequest{
				Peer: peer, Message: "saved one", RandomID: 4911,
			})
			if err != nil {
				t.Fatalf("first self send: %v", err)
			}
			updates, ok := first.(*tg.Updates)
			if !ok {
				t.Fatalf("send result = %T, want *tg.Updates", first)
			}
			if len(updates.Updates) != 2 {
				t.Fatalf("send updates = %d, want message id plus one message", len(updates.Updates))
			}
			if id := msgID(t, first); id != 1 {
				t.Fatalf("self message id = %d, want first local id 1", id)
			}
			newMessages := 0
			for _, update := range updates.Updates {
				newMessage, ok := update.(*tg.UpdateNewMessage)
				if !ok {
					continue
				}
				newMessages++
				message, ok := newMessage.Message.(*tg.Message)
				if !ok {
					t.Fatalf("new message = %T, want *tg.Message", newMessage.Message)
				}
				peer, ok := message.PeerID.(*tg.PeerUser)
				from, fromOK := message.FromID.(*tg.PeerUser)
				if !ok || peer.UserID != user.ID || !fromOK || from.UserID != user.ID || !message.Out || message.Message != "saved one" {
					t.Fatalf("self update message = %+v, want outgoing self message", message)
				}
				if newMessage.Pts != 1 || newMessage.PtsCount != 1 {
					t.Fatalf("self update pts = %d/%d, want 1/1", newMessage.Pts, newMessage.PtsCount)
				}
			}
			if newMessages != 1 {
				t.Fatalf("new message updates = %d, want 1", newMessages)
			}

			history, err := api.GetHistoryForTest(s, user.ID, &tg.MessagesGetHistoryRequest{Peer: peer})
			if err != nil {
				t.Fatalf("self history: %v", err)
			}
			historyReply, ok := history.(*tg.MessagesMessages)
			if !ok || len(historyReply.Messages) != 1 {
				t.Fatalf("self history = %T %+v, want one message", history, history)
			}
			historyMessage, ok := historyReply.Messages[0].(*tg.Message)
			if !ok || historyMessage.ID != 1 || !historyMessage.Out || historyMessage.Message != "saved one" {
				t.Fatalf("history message = %#v, want outgoing saved one", historyReply.Messages[0])
			}

			dialogResult, err := api.GetDialogsForTest(s, user.ID)
			if err != nil {
				t.Fatalf("self dialogs: %v", err)
			}
			dialogReply, ok := dialogResult.(*tg.MessagesDialogs)
			if !ok || len(dialogReply.Dialogs) != 1 {
				t.Fatalf("self dialogs = %T %+v, want exactly one", dialogResult, dialogResult)
			}
			dialog, ok := dialogReply.Dialogs[0].(*tg.Dialog)
			if !ok {
				t.Fatalf("self dialog = %T, want *tg.Dialog", dialogReply.Dialogs[0])
			}
			dialogPeer, peerOK := dialog.Peer.(*tg.PeerUser)
			if !peerOK || dialogPeer.UserID != user.ID || dialog.TopMessage != 1 || dialog.UnreadCount != 0 {
				t.Fatalf("self dialog = %#v, want self peer, top message 1 and no unread messages", dialogReply.Dialogs[0])
			}

			difference, err := api.GetDifferenceForTest(s, user.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0})
			if err != nil {
				t.Fatalf("self difference: %v", err)
			}
			diff, ok := difference.(*tg.UpdatesDifference)
			if !ok || len(diff.NewMessages) != 1 {
				t.Fatalf("self difference = %T %+v, want one recovered message", difference, difference)
			}
			diffMessage, ok := diff.NewMessages[0].(*tg.Message)
			if !ok || diffMessage.ID != 1 || !diffMessage.Out || diffMessage.Message != "saved one" {
				t.Fatalf("difference message = %#v, want outgoing saved one", diff.NewMessages[0])
			}

			read, err := api.ReadHistoryForTest(s, user.ID, &tg.MessagesReadHistoryRequest{Peer: peer, MaxID: 1})
			if err != nil {
				t.Fatalf("read self history: %v", err)
			}
			if _, ok := read.(*tg.MessagesAffectedMessages); !ok {
				t.Fatalf("read result = %T, want *tg.MessagesAffectedMessages", read)
			}
			state, err := s.State(ctx, user.ID)
			if err != nil {
				t.Fatalf("state after read: %v", err)
			}
			if state.Pts != 2 {
				t.Fatalf("self pts after read = %d, want 2", state.Pts)
			}
			dialogState, err := s.Dialogs(ctx, user.ID, 0, 10)
			if err != nil {
				t.Fatalf("read dialog state: %v", err)
			}
			if len(dialogState) != 1 || dialogState[0].UnreadCount != 0 || dialogState[0].ReadInboxMaxID != 1 || dialogState[0].ReadOutboxMaxID != 1 {
				t.Fatalf("dialog state after read = %+v, want one read self dialog with both markers at 1", dialogState)
			}

			retry, err := api.SendMessageForTest(s, user.ID, &tg.MessagesSendMessageRequest{
				Peer: peer, Message: "saved one", RandomID: 4911,
			})
			if err != nil {
				t.Fatalf("retry self send: %v", err)
			}
			if id := msgID(t, retry); id != 1 {
				t.Fatalf("retry id = %d, want original id 1", id)
			}
			state, err = s.State(ctx, user.ID)
			if err != nil {
				t.Fatalf("state after retry: %v", err)
			}
			events, err := s.EventsSince(ctx, user.ID, 0)
			if err != nil {
				t.Fatalf("events after retry: %v", err)
			}
			rows, err := s.History(ctx, user.ID, store.PeerTypeUser, user.ID, 0, 20)
			if err != nil {
				t.Fatalf("stored self history after retry: %v", err)
			}
			if state.Pts != 2 || len(events) != 2 || len(rows) != 1 {
				t.Fatalf("retry changed self state: pts=%d events=%d rows=%d, want 2/2/1", state.Pts, len(events), len(rows))
			}
		})
	}
}

func TestSavedMessagesPeerAuthorizationIsSessionBound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551249021")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551249022")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if _, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "A secret", RandomID: 4921,
	}); err != nil {
		t.Fatalf("A self send: %v", err)
	}
	bSelf, err := api.SendMessageForTest(s, b.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "B secret", RandomID: 4921,
	})
	if err != nil {
		t.Fatalf("B self send with A's random id: %v", err)
	}
	if got := msgID(t, bSelf); got != 1 {
		t.Fatalf("B self message id = %d, want its first local id 1", got)
	}

	for _, tc := range []struct {
		name string
		hash int64
	}{
		{name: "A derived hash", hash: api.DeriveUserHash(a.ID, a.ID)},
		{name: "placeholder hash", hash: a.ID},
	} {
		peer := &tg.InputPeerUser{UserID: a.ID, AccessHash: tc.hash}
		t.Run(tc.name, func(t *testing.T) {
			_, err := api.SendMessageForTest(s, b.ID, &tg.MessagesSendMessageRequest{
				Peer: peer, Message: "forbidden", RandomID: 4922,
			})
			rpcError(t, err, "PEER_ID_INVALID")
			_, err = api.GetHistoryForTest(s, b.ID, &tg.MessagesGetHistoryRequest{Peer: peer})
			rpcError(t, err, "PEER_ID_INVALID")
			_, err = api.ReadHistoryForTest(s, b.ID, &tg.MessagesReadHistoryRequest{Peer: peer, MaxID: 1})
			rpcError(t, err, "PEER_ID_INVALID")
		})
	}

	bRows, err := s.History(ctx, b.ID, store.PeerTypeUser, a.ID, 0, 20)
	if err != nil {
		t.Fatalf("B history: %v", err)
	}
	bState, err := s.State(ctx, b.ID)
	if err != nil {
		t.Fatalf("B state: %v", err)
	}
	bEvents, err := s.EventsSince(ctx, b.ID, 0)
	if err != nil {
		t.Fatalf("B events: %v", err)
	}
	bDialogs, err := s.Dialogs(ctx, b.ID, 0, 20)
	if err != nil {
		t.Fatalf("B dialogs: %v", err)
	}
	if len(bRows) != 0 || bState.Pts != 1 || len(bEvents) != 1 || len(bDialogs) != 1 || bDialogs[0].PeerID != b.ID {
		t.Fatalf("B gained A's self dialog state: rows=%d pts=%d events=%d dialogs=%d", len(bRows), bState.Pts, len(bEvents), len(bDialogs))
	}
	bDifference, err := api.GetDifferenceForTest(s, b.ID, &tg.UpdatesGetDifferenceRequest{Pts: 0})
	if err != nil {
		t.Fatalf("B difference: %v", err)
	}
	if diff, ok := bDifference.(*tg.UpdatesDifference); ok {
		if len(diff.NewMessages) != 1 {
			t.Fatalf("B difference messages = %d, want only B's own message", len(diff.NewMessages))
		}
		message, ok := diff.NewMessages[0].(*tg.Message)
		if !ok {
			t.Fatalf("B difference message = %T, want *tg.Message", diff.NewMessages[0])
		}
		peer, peerOK := message.PeerID.(*tg.PeerUser)
		if !peerOK || peer.UserID != b.ID || message.Message != "B secret" {
			t.Fatalf("B difference contains A data: %#v", diff.NewMessages[0])
		}
	} else {
		t.Fatalf("B difference = %T, want B's own message only", bDifference)
	}
	aRows, err := s.History(ctx, a.ID, store.PeerTypeUser, a.ID, 0, 20)
	if err != nil {
		t.Fatalf("A self history: %v", err)
	}
	if len(aRows) != 1 || aRows[0].Text != "A secret" {
		t.Fatalf("A self history = %+v, want only A's own message", aRows)
	}
}

func TestInputPeerSelfRequiresAuthenticatedViewer(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.SendMessageForTest(s, 0, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "anonymous", RandomID: 4931,
	})
	rpcError(t, err, "AUTH_KEY_UNREGISTERED")
}

func TestSavedMessagesSelfSendUsesRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15551249031")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}
	peer := &tg.InputPeerSelf{}
	first, err := api.SendMessageForTestWithLimits(s, user.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "saved", RandomID: 4932,
	})
	if err != nil {
		t.Fatalf("first self send: %v", err)
	}
	if _, err = api.SendMessageForTestWithLimits(s, user.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "too many", RandomID: 4933,
	}); !isFloodWait(err) {
		t.Fatalf("second self send = %v, want FLOOD_WAIT", err)
	}
	retry, err := api.SendMessageForTestWithLimits(s, user.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "saved", RandomID: 4932,
	})
	if err != nil {
		t.Fatalf("retry self send: %v", err)
	}
	if msgID(t, first) != msgID(t, retry) {
		t.Fatalf("retry id = %d, want original id %d", msgID(t, retry), msgID(t, first))
	}
	rows, err := s.History(ctx, user.ID, store.PeerTypeUser, user.ID, 0, 20)
	if err != nil {
		t.Fatalf("self history: %v", err)
	}
	state, err := s.State(ctx, user.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if len(rows) != 1 || state.Pts != 1 {
		t.Fatalf("self state after limited send = rows %d pts %d, want 1/1", len(rows), state.Pts)
	}
}
