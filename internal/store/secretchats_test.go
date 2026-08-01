package store_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// gaFor builds a distinct 256-byte g_a-shaped value. The store does not
// validate group membership — that is the handler's boundary — so any distinct
// bytes are enough here.
func gaFor(tag int) []byte {
	b := make([]byte, 256)
	b[0] = 0x7f
	copy(b[240:], strconv.Itoa(tag))
	return b
}

// TestCreateSecretChatRequestCapIsAtomic proves the advisory lock in
// CreateSecretChatRequest serialises concurrent requests from one account, so
// exactly MaxOutstandingSecretChats rows are created.
//
// Without the lock the count and the insert race: every goroutine reads the same
// pre-insert count and commits past the cap, which is precisely the durable-row
// amplification the cap exists to stop.
func TestCreateSecretChatRequestCapIsAtomic(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	admin, err := s.CreateUser(ctx, "+15559980001")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	participant, err := s.CreateUser(ctx, "+15559980002")
	if err != nil {
		t.Fatalf("participant: %v", err)
	}

	const n = store.MaxOutstandingSecretChats + 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	ready := make(chan struct{})
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-ready
			ga := gaFor(i)
			hash := sha256.Sum256(ga)
			_, errs[i] = s.CreateSecretChatRequest(ctx, admin.ID, participant.ID, ga, hash[:])
		}(i)
	}
	close(ready)
	wg.Wait()

	var created, capped int
	for _, err := range errs {
		switch {
		case err == nil:
			created++
		case errors.Is(err, store.ErrSecretChatsTooMany):
			capped++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if created != store.MaxOutstandingSecretChats {
		t.Errorf("created = %d, want %d", created, store.MaxOutstandingSecretChats)
	}
	if capped != n-store.MaxOutstandingSecretChats {
		t.Errorf("capped = %d, want %d", capped, n-store.MaxOutstandingSecretChats)
	}
}

// TestAcceptSecretChatIsSingleWinner proves the guarded UPDATE, not a prior
// read, is what makes a replayed accept fail: under concurrency exactly one
// caller may write a fingerprint, and the row keeps that one.
func TestAcceptSecretChatIsSingleWinner(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	admin, err := s.CreateUser(ctx, "+15559981001")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	participant, err := s.CreateUser(ctx, "+15559981002")
	if err != nil {
		t.Fatalf("participant: %v", err)
	}
	ga := gaFor(1)
	hash := sha256.Sum256(ga)
	chat, err := s.CreateSecretChatRequest(ctx, admin.ID, participant.ID, ga, hash[:])
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	ready := make(chan struct{})
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			<-ready
			_, errs[i] = s.AcceptSecretChat(ctx, chat.ID, participant.ID, gaFor(100+i), int64(i+1))
		}(i)
	}
	close(ready)
	wg.Wait()

	var accepted int
	for _, err := range errs {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, store.ErrSecretChatStale):
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want exactly 1", accepted)
	}

	// The initiator may not accept its own request, at any time.
	if _, err := s.AcceptSecretChat(ctx, chat.ID, admin.ID, gaFor(9), 42); !errors.Is(err, store.ErrSecretChatStale) {
		t.Errorf("initiator accept = %v, want ErrSecretChatStale", err)
	}
}

// TestDiscardSecretChatIsTerminal pins that discard works from both live states
// and that a repeat reports the stale guard rather than moving the row again.
func TestDiscardSecretChatIsTerminal(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	admin, err := s.CreateUser(ctx, "+15559982001")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	participant, err := s.CreateUser(ctx, "+15559982002")
	if err != nil {
		t.Fatalf("participant: %v", err)
	}

	for _, accept := range []bool{false, true} {
		ga := gaFor(2)
		hash := sha256.Sum256(ga)
		chat, err := s.CreateSecretChatRequest(ctx, admin.ID, participant.ID, ga, hash[:])
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if accept {
			if _, err := s.AcceptSecretChat(ctx, chat.ID, participant.ID, gaFor(3), 5); err != nil {
				t.Fatalf("accept: %v", err)
			}
		}
		got, err := s.DiscardSecretChat(ctx, chat.ID)
		if err != nil {
			t.Fatalf("discard (accepted=%v): %v", accept, err)
		}
		if got.State != store.SecretChatDiscarded {
			t.Fatalf("state = %q, want %q", got.State, store.SecretChatDiscarded)
		}
		if _, err := s.DiscardSecretChat(ctx, chat.ID); !errors.Is(err, store.ErrSecretChatStale) {
			t.Errorf("second discard = %v, want ErrSecretChatStale", err)
		}
		// Terminal means terminal: no accept revives it.
		if _, err := s.AcceptSecretChat(ctx, chat.ID, participant.ID, gaFor(4), 6); !errors.Is(err, store.ErrSecretChatStale) {
			t.Errorf("accept after discard = %v, want ErrSecretChatStale", err)
		}
	}
}

// Ids come from a sequence and are never reused, including across a discard.
func TestSecretChatIDsAreNeverReused(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	admin, err := s.CreateUser(ctx, "+15559983001")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	participant, err := s.CreateUser(ctx, "+15559983002")
	if err != nil {
		t.Fatalf("participant: %v", err)
	}

	seen := map[int32]bool{}
	for range 3 {
		ga := gaFor(5)
		hash := sha256.Sum256(ga)
		chat, err := s.CreateSecretChatRequest(ctx, admin.ID, participant.ID, ga, hash[:])
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if seen[chat.ID] {
			t.Fatalf("chat id %d reused", chat.ID)
		}
		seen[chat.ID] = true
		if _, err := s.DiscardSecretChat(ctx, chat.ID); err != nil {
			t.Fatalf("discard: %v", err)
		}
	}
}
