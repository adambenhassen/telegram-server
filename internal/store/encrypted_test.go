package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestEncryptedReceivedQueue(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	alice, err := s.CreateUser(ctx, "+15551239100")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15551239101")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	if err := s.EnsureUpdateState(ctx, alice.ID); err != nil {
		t.Fatalf("ensure alice state: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure bob state: %v", err)
	}

	// Create secret chat (alice admin, bob participant).
	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte("ga"), []byte("hash"), 0)
	if err != nil {
		t.Fatalf("create secret chat: %v", err)
	}
	// Activate it.
	_, err = s.AcceptSecretChat(ctx, chat.ID, bob.ID, []byte("gb"), 0)
	if err != nil {
		t.Fatalf("accept secret chat: %v", err)
	}

	// alice sends a message to bob.
	_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
		RecipientID: bob.ID,
		ChatID:      chat.ID,
		RandomID:    100,
		Data:        []byte("msg1"),
	})
	if err != nil {
		t.Fatalf("send to bob: %v", err)
	}

	// bob sends a message to alice.
	_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
		RecipientID: alice.ID,
		ChatID:      chat.ID,
		RandomID:    200,
		Data:        []byte("msg2"),
	})
	if err != nil {
		t.Fatalf("send to alice: %v", err)
	}

	// bob calls receivedQueue with max_qts = 1 — should ack one message.
	ids, err := s.AcknowledgeEncryptedEvents(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if len(ids) != 1 || ids[0] != 100 {
		t.Fatalf("acknowledge = %+v, want [100]", ids)
	}

	// Verify bob's events are gone (atomic delete already happened).
	ids, err = s.AcknowledgeEncryptedEvents(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("acknowledge again: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("second acknowledge = %d, want 0", len(ids))
	}

	// Verify alice's events are untouched.
	ids, err = s.AcknowledgeEncryptedEvents(ctx, alice.ID, 1)
	if err != nil {
		t.Fatalf("alice acknowledge: %v", err)
	}
	if len(ids) != 1 || ids[0] != 200 {
		t.Fatalf("alice acknowledge = %+v, want [200]", ids)
	}
}

func TestEncryptedQtsClamp(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	alice, err := s.CreateUser(ctx, "+15551239102")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15551239103")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte("ga"), []byte("hash"), 0)
	if err != nil {
		t.Fatalf("create secret chat: %v", err)
	}

	// Current qts is 0.
	currentQts, err := s.GetQts(ctx, bob.ID)
	if err != nil {
		t.Fatalf("get qts: %v", err)
	}
	if currentQts != 0 {
		t.Fatalf("initial qts = %d, want 0", currentQts)
	}

	// Bump qts to 2 via sends.
	for i := range 2 {
		_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
			RecipientID: bob.ID,
			ChatID:      chat.ID,
			RandomID:    int64(i + 1),
			Data:        []byte("data"),
		})
		if err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// Clamp: max_qts = 10, current = 2, so effective = 2.
	clamped := int64(10)
	currentQts, err = s.GetQts(ctx, bob.ID)
	if err != nil {
		t.Fatalf("get qts: %v", err)
	}
	if clamped > currentQts {
		clamped = currentQts
	}
	if clamped != 2 {
		t.Fatalf("clamped qts = %d, want 2", clamped)
	}

	// Acknowledge with clamped qts — returns both random_ids.
	ids, err := s.AcknowledgeEncryptedEvents(ctx, bob.ID, clamped)
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("acknowledge = %+v, want 2 ids", ids)
	}
}

func TestEncryptedEventsMonotonic(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	alice, err := s.CreateUser(ctx, "+15551239104")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15551239105")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	chat, _, err := s.CreateSecretChatRequest(ctx, alice.ID, bob.ID, []byte("ga"), []byte("hash"), 0)
	if err != nil {
		t.Fatalf("create secret chat: %v", err)
	}

	// Insert two events at qts 1 and 2.
	_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
		RecipientID: bob.ID,
		ChatID:      chat.ID,
		RandomID:    100,
		Data:        []byte("data"),
	})
	if err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	_, _, err = s.SendEncryptedMessage(ctx, store.EncryptedSend{
		RecipientID: bob.ID,
		ChatID:      chat.ID,
		RandomID:    200,
		Data:        []byte("data"),
	})
	if err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	// receivedQueue with max_qts = 1 — ack only first.
	ids, err := s.AcknowledgeEncryptedEvents(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("acknowledge 1: %v", err)
	}
	if len(ids) != 1 || ids[0] != 100 {
		t.Fatalf("acknowledge = %+v, want [100]", ids)
	}

	// Second call with max_qts = 1 — empty (already acked).
	ids, err = s.AcknowledgeEncryptedEvents(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("acknowledge 1 again: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("second call with same max_qts = %d, want 0", len(ids))
	}

	// Second call with max_qts = 2 — ack second message.
	ids, err = s.AcknowledgeEncryptedEvents(ctx, bob.ID, 2)
	if err != nil {
		t.Fatalf("acknowledge 2: %v", err)
	}
	if len(ids) != 1 || ids[0] != 200 {
		t.Fatalf("acknowledge = %+v, want [200]", ids)
	}

	// All acked.
	ids, err = s.AcknowledgeEncryptedEvents(ctx, bob.ID, 2)
	if err != nil {
		t.Fatalf("acknowledge all: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("all events acked = %d, want 0", len(ids))
	}
}
