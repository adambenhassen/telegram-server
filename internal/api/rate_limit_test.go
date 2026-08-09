package api_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// isFloodWait reports whether err is a 420 FLOOD_WAIT error.
func isFloodWait(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 420
}

// TestSendMessageRateLimit proves that N+1 sends within the window are denied
// with FLOOD_WAIT and have no side effects (no message rows, no pts advance).
func TestSendMessageRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293001")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293002")
	if err != nil {
		t.Fatal(err)
	}

	// Use a small limit for testing: 3 sends per 10s.
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Sends 1-3 should pass.
	for i := range 3 {
		_, err := api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
			Peer: peerBob, Message: "msg", RandomID: int64(i + 1),
		})
		if err != nil {
			t.Fatalf("send %d: %v", i+1, err)
		}
	}

	// Send 4 should be denied with FLOOD_WAIT.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "blocked", RandomID: 4,
	})
	if !isFloodWait(err) {
		t.Fatalf("send 4: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: neither side should have a 4th message, and pts
	// should be unchanged from the 3 successful sends.
	aliceMsgs, err := s.History(ctx, alice.ID, store.PeerTypeUser, bob.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(aliceMsgs) != 3 {
		t.Fatalf("alice history = %d, want 3", len(aliceMsgs))
	}
	bobMsgs, err := s.History(ctx, bob.ID, store.PeerTypeUser, alice.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(bobMsgs) != 3 {
		t.Fatalf("bob history = %d, want 3", len(bobMsgs))
	}
	// Both pts should be 3 (one per successful send).
	aliceState, err := s.State(ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if aliceState.Pts != 3 {
		t.Fatalf("alice pts = %d, want 3", aliceState.Pts)
	}
	bobState, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bobState.Pts != 3 {
		t.Fatalf("bob pts = %d, want 3", bobState.Pts)
	}

	// A different account is unaffected.
	_, err = api.SendMessageForTestWithLimits(s, bob.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(bob.ID, alice.ID), Message: "bob sends", RandomID: 100,
	})
	if err != nil {
		t.Fatalf("bob send: %v", err)
	}
}

// TestSendMessageRateLimitWindowExpiry proves that after the window expires,
// the same account can send again.
func TestSendMessageRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293101")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293102")
	if err != nil {
		t.Fatal(err)
	}

	// Very short window: 2 sends per 500ms.
	cfg := store.RateLimitConfig{Limit: 2, Window: 500 * time.Millisecond}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Exhaust the limit.
	for i := range 2 {
		_, err := api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
			Peer: peerBob, Message: "msg", RandomID: int64(i + 1),
		})
		if err != nil {
			t.Fatalf("send %d: %v", i+1, err)
		}
	}

	// Denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "blocked", RandomID: 3,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Wait for window to expire.
	time.Sleep(600 * time.Millisecond)

	// Should be allowed again.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "after expiry", RandomID: 4,
	})
	if err != nil {
		t.Fatalf("post-expiry send: %v", err)
	}
}

// TestCreateChatRateLimit proves that N+1 creates within the window are denied.
func TestCreateChatRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293201")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293202")
	if err != nil {
		t.Fatal(err)
	}

	// 2 creates per 10s.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	// Creates 1-2 should pass.
	for i := range 2 {
		_, err := api.CreateChatForTestWithLimits(s, alice.ID, cfg, &tg.MessagesCreateChatRequest{
			Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
			Title: "Chat " + string(rune('A'+i)),
		})
		if err != nil {
			t.Fatalf("create %d: %v", i+1, err)
		}
	}

	// Create 3 should be denied.
	_, err = api.CreateChatForTestWithLimits(s, alice.ID, cfg, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
		Title: "Blocked",
	})
	if !isFloodWait(err) {
		t.Fatalf("create 3: expected FLOOD_WAIT, got %v", err)
	}
}

// TestAddChatUserRateLimit proves that N+1 member adds within the window are denied.
func TestAddChatUserRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293301")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293302")
	if err != nil {
		t.Fatal(err)
	}
	charlie, err := s.CreateUser(ctx, "+15551293303")
	if err != nil {
		t.Fatal(err)
	}

	// 2 adds per 10s.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	// Create a chat first (no limit on create in this test).
	enc, err := api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{},
		Title: "Test Chat",
	})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T", enc)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok || len(ups.Chats) != 1 {
		t.Fatalf("updates = %T", res.Updates)
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T", ups.Chats[0])
	}

	// Adds 1-2 should pass.
	for _, target := range []int64{bob.ID, charlie.ID} {
		_, err := api.AddChatUserForTestWithLimits(s, alice.ID, cfg, &tg.MessagesAddChatUserRequest{
			ChatID: chat.ID, UserID: api.InputUser(alice.ID, target),
		})
		if err != nil {
			t.Fatalf("add user: %v", err)
		}
	}

	// Add 3 should be denied.
	_, err = api.AddChatUserForTestWithLimits(s, alice.ID, cfg, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(alice.ID, bob.ID),
	})
	if !isFloodWait(err) {
		t.Fatalf("add 3: expected FLOOD_WAIT, got %v", err)
	}
}

// TestRateLimitDisabled proves that a zero limit disables enforcement.
func TestRateLimitDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293401")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293402")
	if err != nil {
		t.Fatal(err)
	}

	// Zero limit = disabled.
	cfg := store.RateLimitConfig{}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Many sends should all pass.
	for i := range 100 {
		_, err := api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
			Peer: peerBob, Message: "msg", RandomID: int64(i + 1),
		})
		if err != nil {
			t.Fatalf("send %d: %v", i+1, err)
		}
	}
}

// TestConcurrentColdKey proves that concurrent requests on a key being created
// for the first time are handled correctly (limit succeed, rest denied).
func TestConcurrentColdKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293701")
	if err != nil {
		t.Fatal(err)
	}

	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	const n = 10 // more than the limit

	type result struct {
		err    error
		denied bool
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	wg.Add(n)
	ready := make(chan struct{})

	for i := range n {
		go func() {
			defer wg.Done()
			<-ready
			rl, err := s.CheckRateLimit(ctx, alice.ID, "concurrent_cold", cfg)
			results[i] = result{err: err, denied: rl != nil}
		}()
	}

	close(ready)
	wg.Wait()

	var successes, denials, other int
	for _, r := range results {
		switch {
		case r.err != nil:
			other++
			t.Errorf("unexpected error: %v", r.err)
		case r.denied:
			denials++
		default:
			successes++
		}
	}

	if successes != cfg.Limit {
		t.Errorf("successes = %d, want %d", successes, cfg.Limit)
	}
	if denials != n-cfg.Limit {
		t.Errorf("denials = %d, want %d", denials, n-cfg.Limit)
	}
	if other != 0 {
		t.Errorf("unexpected errors = %d, want 0", other)
	}
}

// TestForwardRateLimit proves that forward draws from the shared message send budget.
func TestForwardRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293801")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293802")
	if err != nil {
		t.Fatal(err)
	}
	charlie, err := s.CreateUser(ctx, "+15551293803")
	if err != nil {
		t.Fatal(err)
	}

	// 2 sends per 10s — shared between send and forward.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	peerBob := api.InputPeerUser(alice.ID, bob.ID)
	peerCharlie := api.InputPeerUser(alice.ID, charlie.ID)

	// Send 1 message to bob.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "original", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Send 1 message to charlie.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerCharlie, Message: "original2", RandomID: 2,
	})
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	// Forward should be denied (shared budget exhausted).
	_, err = api.ForwardMessagesForTestWithLimits(s, alice.ID, cfg, &tg.MessagesForwardMessagesRequest{
		ToPeer:   peerCharlie,
		FromPeer: peerBob,
		ID:       []int{1},
		RandomID: []int64{999},
	})
	if !isFloodWait(err) {
		t.Fatalf("forward: expected FLOOD_WAIT, got %v", err)
	}
}

// TestSendMediaRateLimit proves that media send draws from the shared message send budget.
func TestSendMediaRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551293901")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551293902")
	if err != nil {
		t.Fatal(err)
	}

	// 1 send per 10s — very restrictive.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Plain text send.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "hello", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Media send should be denied (shared budget exhausted).
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.SendMediaForTestWithLimits(s, alice.ID, blobs, api.TestMaxUserStorageBytes, cfg, &tg.MessagesSendMediaRequest{
		Peer: peerBob, Message: "media", RandomID: 2,
		Media: &tg.InputMediaUploadedDocument{
			File:     &tg.InputFile{ID: 1, Parts: 1, Name: "test.txt"},
			MimeType: "text/plain",
		},
	})
	if !isFloodWait(err) {
		t.Fatalf("send media: expected FLOOD_WAIT, got %v", err)
	}
}

// TestRetryDoesNotRateLimit proves that a transport retry with an already-
// stored random_id returns the stored message, never FLOOD_WAIT. The rate
// limit check runs after the dedupe in store.SendMessage, so a retry is
// caught by the dedupe and never reaches the limiter.
func TestRetryDoesNotRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294001")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294002")
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 1: after the first send, the account is at its limit.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)

	// Send a message — consumes the only token.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "original", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Retry with the same random_id: should return the stored message,
	// not FLOOD_WAIT.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "retry", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("retry: expected success, got %v (FLOOD_WAIT on retry is a bug)", err)
	}

	// A NEW message with a different random_id should be denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "new", RandomID: 99,
	})
	if !isFloodWait(err) {
		t.Fatalf("new message: expected FLOOD_WAIT, got %v", err)
	}
}

// TestEncryptedSendRateLimit proves that encrypted sends draw from the shared
// message send budget and that a retry with the same random_id does not consume
// a token.
func TestEncryptedSendRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294101")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294102")
	if err != nil {
		t.Fatal(err)
	}

	// Create an active secret chat.
	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte{1}, []byte{2}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptSecretChat(ctx, chat.ID, bob.ID, []byte{3}, 42); err != nil {
		t.Fatal(err)
	}

	// Limit of 2 sends.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	encPeer := api.InputEncryptedChat(alice.ID, chat.ID)
	data := []byte("encrypted payload")

	// Send 1 — consumes token 1.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}

	// Retry same random_id — should succeed without consuming a token.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 1,
	})
	if err != nil {
		t.Fatalf("retry: expected success, got %v", err)
	}

	// Send 2 — consumes token 2.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 2,
	})
	if err != nil {
		t.Fatalf("send 2: %v", err)
	}

	// Send 3 — should be denied.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 3,
	})
	if !isFloodWait(err) {
		t.Fatalf("send 3: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: bob's qts should be unchanged (2 events = 2 qts bumps).
	bobStateAfter, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bobStateAfter.Qts != 2 {
		t.Fatalf("bob qts = %d, want 2 (denied encrypted send advanced qts)", bobStateAfter.Qts)
	}
}

// TestChannelPostRetryDoesNotRateLimit proves that a channel post retry with
// an already-stored random_id returns the stored post, never FLOOD_WAIT.
func TestChannelPostRetryDoesNotRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294201")
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel with alice as admin.
	channel, err := s.CreateChannel(ctx, alice.ID, "Test Channel", "", false)
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Post 1 — consumes the only token.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer:    api.InputPeerChannel(alice.ID, channel.ID),
		Message: "post", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// Retry same random_id — should succeed without consuming a token.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer:    api.InputPeerChannel(alice.ID, channel.ID),
		Message: "retry", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("retry: expected success, got %v", err)
	}

	// New post with different random_id — should be denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer:    api.InputPeerChannel(alice.ID, channel.ID),
		Message: "new", RandomID: 99,
	})
	if !isFloodWait(err) {
		t.Fatalf("new post: expected FLOOD_WAIT, got %v", err)
	}
}

// TestChatMediaSendsOneToken proves that a chat media send consumes exactly
// one rate limit token (not two). It sends one plain text to a 1:1 dialog
// (token 1), then a chat media (token 2), then verifies the third send is
// denied — proving the chat media path only spent one token.
func TestChatMediaSendsOneToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294301")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294302")
	if err != nil {
		t.Fatal(err)
	}

	// Create a chat and add bob.
	enc, err := api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
		Title: "Media Chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T", enc)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok || len(ups.Chats) != 1 {
		t.Fatalf("updates = %T", res.Updates)
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T", ups.Chats[0])
	}

	// Limit of 2.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)
	peerChat := api.InputPeerChat(alice.ID, chat.ID)

	// Plain text send to 1:1 — consumes token 1.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "hello", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Chat media send — should consume token 2 and succeed.
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Upload the file parts first.
	if _, err := api.SaveFilePartForTest(s, alice.ID, &tg.UploadSaveFilePartRequest{
		FileID: 1, FilePart: 0, Bytes: []byte("hello"),
	}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	_, err = api.SendMediaForTestWithLimits(s, alice.ID, blobs, api.TestMaxUserStorageBytes, cfg, &tg.MessagesSendMediaRequest{
		Peer: peerChat, Message: "media", RandomID: 2,
		Media: &tg.InputMediaUploadedDocument{
			File:     &tg.InputFile{ID: 1, Parts: 1, Name: "test.txt"},
			MimeType: "text/plain",
		},
	})
	if err != nil {
		t.Fatalf("chat media: expected success, got %v (chat media consuming 2 tokens is a bug)", err)
	}

	// Third send — should be denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "blocked", RandomID: 3,
	})
	if !isFloodWait(err) {
		t.Fatalf("third send: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: bob's 1:1 history should have 1 message (the first send),
	// not 2. The denied 1:1 send wrote nothing.
	bobMsgs, err := s.History(ctx, bob.ID, store.PeerTypeUser, alice.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(bobMsgs) != 1 {
		t.Fatalf("bob 1:1 history = %d, want 1 (denied send wrote row)", len(bobMsgs))
	}
}

// TestChatSendRateLimit proves that chat sends draw from the shared message
// send budget and that N+1 is denied with FLOOD_WAIT.
func TestChatSendRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294401")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294402")
	if err != nil {
		t.Fatal(err)
	}

	// Create a chat with bob.
	enc, err := api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
		Title: "Chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T", enc)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok || len(ups.Chats) != 1 {
		t.Fatalf("updates = %T", res.Updates)
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T", ups.Chats[0])
	}

	// Limit of 2.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerChat := api.InputPeerChat(alice.ID, chat.ID)

	// Chat send 1 — consumes token 1.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChat, Message: "chat msg 1", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("chat send 1: %v", err)
	}

	// Chat send 2 — consumes token 2.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChat, Message: "chat msg 2", RandomID: 2,
	})
	if err != nil {
		t.Fatalf("chat send 2: %v", err)
	}

	// Chat send 3 — should be denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChat, Message: "blocked", RandomID: 3,
	})
	if !isFloodWait(err) {
		t.Fatalf("chat send 3: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: bob's history should have 3 messages (1 create + 2 sends),
	// not 4. The denied chat send wrote nothing.
	bobMsgs, err := s.History(ctx, bob.ID, store.PeerTypeChat, chat.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(bobMsgs) != 3 {
		t.Fatalf("bob chat history = %d, want 3 (denied chat send wrote row)", len(bobMsgs))
	}
}

// TestChatSendRetryDoesNotRateLimit proves that a chat send retry with an
// already-stored random_id returns the stored message, never FLOOD_WAIT.
func TestChatSendRetryDoesNotRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294501")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294502")
	if err != nil {
		t.Fatal(err)
	}

	// Create a chat with bob.
	enc, err := api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
		Title: "Chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := enc.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("result type = %T", enc)
	}
	ups, ok := res.Updates.(*tg.Updates)
	if !ok || len(ups.Chats) != 1 {
		t.Fatalf("updates = %T", res.Updates)
	}
	chat, ok := ups.Chats[0].(*tg.Chat)
	if !ok {
		t.Fatalf("chat type = %T", ups.Chats[0])
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	peerChat := api.InputPeerChat(alice.ID, chat.ID)

	// Chat send — consumes the only token.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChat, Message: "original", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("chat send: %v", err)
	}

	// Retry same random_id — should succeed without consuming a token.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChat, Message: "retry", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("chat retry: expected success, got %v", err)
	}
}

// TestChannelPostRateLimit proves that channel posts draw from the shared
// message send budget and that N+1 is denied with FLOOD_WAIT.
func TestChannelPostRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294601")
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel with alice as admin.
	channel, err := s.CreateChannel(ctx, alice.ID, "Test Channel", "", false)
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 2.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerChannel := api.InputPeerChannel(alice.ID, channel.ID)

	// Post 1 — consumes token 1.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "post 1", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("post 1: %v", err)
	}

	// Post 2 — consumes token 2.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "post 2", RandomID: 2,
	})
	if err != nil {
		t.Fatalf("post 2: %v", err)
	}

	// Post 3 — should be denied.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "blocked", RandomID: 3,
	})
	if !isFloodWait(err) {
		t.Fatalf("post 3: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: only 2 channel messages should exist.
	history, err := s.ChannelHistory(ctx, channel.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("channel history = %d, want 2 (denied channel post wrote row)", len(history))
	}
}

// TestForwardRetryDoesNotRateLimit proves that a forward retry with all
// random_ids already stored returns the stored result, never FLOOD_WAIT.
func TestForwardRetryDoesNotRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294701")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294702")
	if err != nil {
		t.Fatal(err)
	}
	charlie, err := s.CreateUser(ctx, "+15551294703")
	if err != nil {
		t.Fatal(err)
	}

	// Limit of 2: send (1) + forward (1) = 2, retry should be free.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	peerBob := api.InputPeerUser(alice.ID, bob.ID)
	peerCharlie := api.InputPeerUser(alice.ID, charlie.ID)

	// Send a message to bob — consumes token 1.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerBob, Message: "original", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Forward to charlie — consumes token 2.
	_, err = api.ForwardMessagesForTestWithLimits(s, alice.ID, cfg, &tg.MessagesForwardMessagesRequest{
		ToPeer:   peerCharlie,
		FromPeer: peerBob,
		ID:       []int{1},
		RandomID: []int64{999},
	})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}

	// Retry same random_id — should succeed without consuming a token.
	_, err = api.ForwardMessagesForTestWithLimits(s, alice.ID, cfg, &tg.MessagesForwardMessagesRequest{
		ToPeer:   peerCharlie,
		FromPeer: peerBob,
		ID:       []int{1},
		RandomID: []int64{999},
	})
	if err != nil {
		t.Fatalf("forward retry: expected success, got %v", err)
	}

	// A NEW forward — should be denied (budget exhausted by send+forward).
	_, err = api.ForwardMessagesForTestWithLimits(s, alice.ID, cfg, &tg.MessagesForwardMessagesRequest{
		ToPeer:   peerCharlie,
		FromPeer: peerBob,
		ID:       []int{1},
		RandomID: []int64{998},
	})
	if !isFloodWait(err) {
		t.Fatalf("forward denied: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: charlie should have only 1 forwarded message.
	charlieMsgs, err := s.History(ctx, charlie.ID, store.PeerTypeUser, alice.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(charlieMsgs) != 1 {
		t.Fatalf("charlie history = %d, want 1 (denied forward wrote row)", len(charlieMsgs))
	}
}

// TestEncryptedSendDeniedNoSideEffects proves that a denied encrypted send
// writes no event row and advances no qts on the recipient.
func TestEncryptedSendDeniedNoSideEffects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551294801")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551294802")
	if err != nil {
		t.Fatal(err)
	}

	// Create an active secret chat.
	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte{1}, []byte{2}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptSecretChat(ctx, chat.ID, bob.ID, []byte{3}, 42); err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	encPeer := api.InputEncryptedChat(alice.ID, chat.ID)
	data := []byte("encrypted payload")

	// Send 1 — consumes the only token.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send 1: %v", err)
	}

	// Record bob's qts after the successful send.
	bobState, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatal(err)
	}
	qtsBefore := bobState.Qts

	// Send 2 — should be denied.
	_, err = api.SendEncryptedMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendEncryptedRequest{
		Peer: encPeer, Data: data, RandomID: 2,
	})
	if !isFloodWait(err) {
		t.Fatalf("send 2: expected FLOOD_WAIT, got %v", err)
	}

	// Bob's qts should be unchanged — the denied send wrote nothing.
	bobStateAfter, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bobStateAfter.Qts != qtsBefore {
		t.Fatalf("bob qts = %d, want %d (denied encrypted send advanced qts)", bobStateAfter.Qts, qtsBefore)
	}
}

// TestChannelNotifyFiresOncePerPost proves that a new channel post fires
// exactly one notification. The !dup guard on the notify prevents duplicate
// pushes when two concurrent posts share a random_id, but that race is
// already rare (the handler's read-only dedupe catches most duplicates
// before the store), so this test covers the reliable positive path.
func TestChannelNotifyFiresOncePerPost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551294901")
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel with alice as admin.
	channel, err := s.CreateChannel(ctx, alice.ID, "Test Channel", "", false)
	if err != nil {
		t.Fatal(err)
	}

	// Listen for channel post notifications.
	var notifyCount atomic.Int64
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) { notifyCount.Add(1) },
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() { _ = stop() }() //nolint:errcheck

	// waitForNotify polls until the counter reaches want or times out.
	waitForNotify := func(want int64) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for notifyCount.Load() < want {
			select {
			case <-ctx.Done():
				t.Fatalf("notify count = %d, want %d (timed out)", notifyCount.Load(), want)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	// No rate limit for this test.
	cfg := store.RateLimitConfig{}
	peerChannel := api.InputPeerChannel(alice.ID, channel.ID)

	// Post once — should fire exactly one notify.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "post", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// Wait for the notification.
	waitForNotify(1)

	// Post again with a different random_id — should fire a second notify.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "post2", RandomID: 43,
	})
	if err != nil {
		t.Fatalf("post2: %v", err)
	}

	// Wait for the second notification.
	waitForNotify(2)

	// Retry the first post — short-circuits at the dedupe lookup, no notify.
	_, err = api.SendMessageForTestWithLimits(s, alice.ID, cfg, &tg.MessagesSendMessageRequest{
		Peer: peerChannel, Message: "retry", RandomID: 42,
	})
	if err != nil {
		t.Fatalf("retry: expected success, got %v", err)
	}

	// Allow a brief window for a spurious notify, then assert count is still 2.
	time.Sleep(100 * time.Millisecond)
	if notifyCount.Load() != 2 {
		t.Fatalf("notify count after retry = %d, want 2 (retry fired notify)", notifyCount.Load())
	}
}

// TestCreateChannelRateLimit proves that N+1 creates within the window are
// denied with FLOOD_WAIT and have no side effects (no channel row, no
// participant row, no pts advance).
func TestCreateChannelRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551295001")
	if err != nil {
		t.Fatal(err)
	}

	// 2 creates per 10s.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	// Creates 1-2 should pass.
	for i := range 2 {
		_, err := api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
			Title: "Channel " + string(rune('A'+i)), About: "", Broadcast: true,
		})
		if err != nil {
			t.Fatalf("create %d: %v", i+1, err)
		}
	}

	// Record state before the denied create.
	aliceState, err := s.State(ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	ptsBefore := aliceState.Pts
	channelsBefore, err := s.ChannelsForUser(ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Create 3 should be denied.
	_, err = api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
		Title: "Blocked", About: "", Broadcast: true,
	})
	if !isFloodWait(err) {
		t.Fatalf("create 3: expected FLOOD_WAIT, got %v", err)
	}

	// No side effects: no new channel row, pts unchanged.
	channelsAfter, err := s.ChannelsForUser(ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(channelsAfter) != len(channelsBefore) {
		t.Fatalf("channel count = %d, want %d (denied create wrote channel row)", len(channelsAfter), len(channelsBefore))
	}
	aliceStateAfter, err := s.State(ctx, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if aliceStateAfter.Pts != ptsBefore {
		t.Fatalf("pts = %d, want %d (denied create advanced pts)", aliceStateAfter.Pts, ptsBefore)
	}
}

// TestCreateChannelRateLimitDisabled proves that a zero limit disables
// enforcement for the create_channel surface.
func TestCreateChannelRateLimitDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551295101")
	if err != nil {
		t.Fatal(err)
	}

	// Zero limit = disabled. Loop past the default (20) to prove enforcement is off.
	cfg := store.RateLimitConfig{}

	for i := range 25 {
		_, err := api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
			Title: "Channel " + string(rune('A'+i%26)), About: "", Broadcast: true,
		})
		if err != nil {
			t.Fatalf("create %d: %v", i+1, err)
		}
	}
}

// TestCreateChannelRateLimitWindowExpiry proves that after the window expires,
// the same account can create a channel again.
func TestCreateChannelRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551295201")
	if err != nil {
		t.Fatal(err)
	}

	// Very short window: 1 create per 500ms.
	cfg := store.RateLimitConfig{Limit: 1, Window: 500 * time.Millisecond}

	// Exhaust the limit.
	_, err = api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
		Title: "First", About: "", Broadcast: true,
	})
	if err != nil {
		t.Fatalf("create 1: %v", err)
	}

	// Denied.
	_, err = api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
		Title: "Blocked", About: "", Broadcast: true,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Wait for window to expire.
	time.Sleep(600 * time.Millisecond)

	// Should be allowed again.
	_, err = api.CreateChannelForTestWithLimits(s, alice.ID, cfg, &tg.ChannelsCreateChannelRequest{
		Title: "After Expiry", About: "", Broadcast: true,
	})
	if err != nil {
		t.Fatalf("post-expiry create: %v", err)
	}
}
