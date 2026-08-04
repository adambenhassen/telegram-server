package store_test

import (
	"context"
	"testing"
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

	// Create encrypted chat between alice and bob.
	if err := s.CreateEncryptedChat(ctx, 1, 0xabcd, alice.ID, bob.ID); err != nil {
		t.Fatalf("create encrypted chat: %v", err)
	}

	// Verify chat lookup.
	ec, err := s.GetEncryptedChat(ctx, 1)
	if err != nil {
		t.Fatalf("get encrypted chat: %v", err)
	}
	if ec.ID != 1 || ec.User1ID != alice.ID || ec.User2ID != bob.ID {
		t.Fatalf("encrypted chat = %+v", ec)
	}

	// alice sends a message to bob: bump bob's qts, insert event.
	qts1, err := s.BumpQts(ctx, bob.ID)
	if err != nil {
		t.Fatalf("bump qts: %v", err)
	}
	if qts1 != 1 {
		t.Fatalf("qts after first bump = %d, want 1", qts1)
	}

	if err := s.InsertEncryptedEvent(ctx, bob.ID, qts1, 100, []byte("msg1")); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	// bob sends a second message to alice.
	qts2, err := s.BumpQts(ctx, alice.ID)
	if err != nil {
		t.Fatalf("bump qts for alice: %v", err)
	}
	if err := s.InsertEncryptedEvent(ctx, alice.ID, qts2, 200, []byte("msg2")); err != nil {
		t.Fatalf("insert event for alice: %v", err)
	}

	// bob calls receivedQueue with max_qts = 1 — should ack one message.
	events, err := s.EncryptedEventsUpTo(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("events up to: %v", err)
	}
	if len(events) != 1 || events[0].RandomID != 100 {
		t.Fatalf("events = %+v, want one event with random_id 100", events)
	}

	if err := s.DeleteEncryptedEventsUpTo(ctx, bob.ID, 1); err != nil {
		t.Fatalf("delete events: %v", err)
	}

	// Verify bob's events are gone.
	events, err = s.EncryptedEventsUpTo(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("events after delete: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events after delete = %d, want 0", len(events))
	}

	// Verify alice's events are untouched.
	events, err = s.EncryptedEventsUpTo(ctx, alice.ID, 1)
	if err != nil {
		t.Fatalf("alice events: %v", err)
	}
	if len(events) != 1 || events[0].RandomID != 200 {
		t.Fatalf("alice events = %+v, want one event with random_id 200", events)
	}
}

func TestEncryptedQtsClamp(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	bob, err := s.CreateUser(ctx, "+15551239102")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	// Current qts is 0.
	currentQts, err := s.GetQts(ctx, bob.ID)
	if err != nil {
		t.Fatalf("get qts: %v", err)
	}
	if currentQts != 0 {
		t.Fatalf("initial qts = %d, want 0", currentQts)
	}

	// Bump qts to 2.
	for i := range 2 {
		qts, err := s.BumpQts(ctx, bob.ID)
		if err != nil {
			t.Fatalf("bump qts: %v", err)
		}
		if qts != int64(i+1) {
			t.Fatalf("qts after bump %d = %d, want %d", i+1, qts, i+1)
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

	// receivedQueue with lower max_qts (monotonic watermark) returns empty.
	events, err := s.EncryptedEventsUpTo(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("events up to 1: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events with no inserts = %d, want 0", len(events))
	}
}

func TestEncryptedEventsMonotonic(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	bob, err := s.CreateUser(ctx, "+15551239103")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	if err := s.EnsureUpdateState(ctx, bob.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	// Insert two events at qts 1 and 2.
	for i := int64(1); i <= 2; i++ {
		qts, err := s.BumpQts(ctx, bob.ID)
		if err != nil {
			t.Fatalf("bump qts: %v", err)
		}
		if err := s.InsertEncryptedEvent(ctx, bob.ID, qts, i*100, []byte("data")); err != nil {
			t.Fatalf("insert event: %v", err)
		}
	}

	// receivedQueue with max_qts = 1 — ack only first.
	events, err := s.EncryptedEventsUpTo(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("events up to 1: %v", err)
	}
	if len(events) != 1 || events[0].RandomID != 100 {
		t.Fatalf("events = %+v, want one event with random_id 100", events)
	}

	if err := s.DeleteEncryptedEventsUpTo(ctx, bob.ID, 1); err != nil {
		t.Fatalf("delete events: %v", err)
	}

	// Second call with max_qts = 1 — empty (already acked).
	events, err = s.EncryptedEventsUpTo(ctx, bob.ID, 1)
	if err != nil {
		t.Fatalf("events up to 1 (second): %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("second call with same max_qts = %d, want 0", len(events))
	}

	// Second call with max_qts = 2 — ack second message.
	events, err = s.EncryptedEventsUpTo(ctx, bob.ID, 2)
	if err != nil {
		t.Fatalf("events up to 2: %v", err)
	}
	if len(events) != 1 || events[0].RandomID != 200 {
		t.Fatalf("events = %+v, want one event with random_id 200", events)
	}

	if err := s.DeleteEncryptedEventsUpTo(ctx, bob.ID, 2); err != nil {
		t.Fatalf("delete events: %v", err)
	}

	// All acked.
	events, err = s.EncryptedEventsUpTo(ctx, bob.ID, 2)
	if err != nil {
		t.Fatalf("events after full ack: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("all events acked = %d, want 0", len(events))
	}
}
