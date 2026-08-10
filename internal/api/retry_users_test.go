package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
)

// TestSendChatMessageRetryUsersAreTheSendTimeRecipients pins who a chat retry
// hydrates. The reply is answered from the stored message rather than from a
// second fan-out, so its user list follows that message: the sender it was
// stored for, and never a member who joined the chat after it was sent. A
// client that retries an old random_id is told about the message it already
// has, not about the chat's membership since.
func TestSendChatMessageRetryUsersAreTheSendTimeRecipients(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551299201", "+15551299202")
	peer := &tg.InputPeerChat{ChatID: chat.ID}

	first, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 201,
	})
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	sent := updatesOf(t, first).Users

	joiner, err := s.CreateUser(ctx, "+15551299203")
	if err != nil {
		t.Fatalf("joiner: %v", err)
	}
	if added, _, _, aerr := s.AddChatUser(ctx, chat.ID, joiner.ID, users[0].ID); aerr != nil || !added {
		t.Fatalf("add joiner: added=%v err=%v", added, aerr)
	}
	if hasUser(sent, joiner.ID) {
		t.Fatal("joiner was already in the first send's users — the test staged no join")
	}

	again, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 201,
	})
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)
	retry := updatesOf(t, again)
	got := userIDs(retry.Users)
	if hasUser(retry.Users, joiner.ID) {
		t.Errorf("retry users %v include the member who joined after the send (%d)", got, joiner.ID)
	}
	if len(got) != 1 || got[0] != users[0].ID {
		t.Errorf("retry users = %v, want just the sender %d", got, users[0].ID)
	}
	// The chat itself still comes back, so a client that dropped the reply can
	// still resolve the peer it is being answered about.
	if len(retry.Chats) != 1 {
		t.Errorf("retry chats = %d, want 1", len(retry.Chats))
	}
}
