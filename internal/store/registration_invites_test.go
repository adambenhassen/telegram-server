package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestIssueInviteStoresOnlyDigestAndCanonicalizesHandle(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	invite, secret, err := s.IssueInvite(ctx, "Alice")
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}
	secretBytes, err := hex.DecodeString(secret)
	if err != nil {
		t.Fatalf("secret is not hex: %v", err)
	}
	if len(secretBytes) < 16 {
		t.Fatalf("secret entropy = %d bytes, want at least 16", len(secretBytes))
	}
	if invite.Handle != "alice" {
		t.Errorf("handle = %q, want alice", invite.Handle)
	}
	if invite.State != store.InviteIssued {
		t.Errorf("state = %q, want issued", invite.State)
	}
	if !invite.ExpiresAt.After(invite.IssuedAt) {
		t.Fatalf("expiry %s is not after issue time %s", invite.ExpiresAt, invite.IssuedAt)
	}
	if lifetime := invite.ExpiresAt.Sub(invite.IssuedAt); lifetime < store.DefaultInviteLifetime-2*time.Second || lifetime > store.DefaultInviteLifetime+2*time.Second {
		t.Errorf("lifetime = %s, want about %s", lifetime, store.DefaultInviteLifetime)
	}

	var storedDigest []byte
	var storedHandle, state string
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT secret_digest, handle, state FROM registration_invites WHERE id = $1`, invite.ID,
	).Scan(&storedDigest, &storedHandle, &state); err != nil {
		t.Fatalf("read stored invite: %v", err)
	}
	wantDigest := sha256.Sum256([]byte(secret))
	if !bytes.Equal(storedDigest, wantDigest[:]) {
		t.Errorf("stored digest = %x, want SHA-256 digest", storedDigest)
	}
	if bytes.Equal(storedDigest, []byte(secret)) {
		t.Error("stored invite contains the returned secret instead of a digest")
	}
	if storedHandle != "alice" || state != string(store.InviteIssued) {
		t.Errorf("stored invite = handle %q state %q, want alice/issued", storedHandle, state)
	}

	if err := s.VerifyInvite(ctx, "ALICE", secret); err != nil {
		t.Fatalf("verify issued invite: %v", err)
	}
	listed, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatalf("list invites: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != invite.ID {
		t.Fatalf("listed invites = %+v, want invite %d", listed, invite.ID)
	}
	if rendered := fmt.Sprintf("%+v", listed[0]); strings.Contains(rendered, secret) {
		t.Error("listed invite contains the secret")
	}
}

func TestIssueInviteRejectsLiveDuplicateAndAllowsExpiredReplacement(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	first, _, err := s.IssueInvite(ctx, "Alice", time.Hour)
	if err != nil {
		t.Fatalf("first invite: %v", err)
	}
	if _, _, err := s.IssueInvite(ctx, "alice", time.Hour); !errors.Is(err, store.ErrInviteLive) {
		t.Fatalf("live duplicate: err = %v, want ErrInviteLive", err)
	}

	if _, err := store.StorePool(s).Exec(ctx,
		`UPDATE registration_invites
		    SET issued_at = clock_timestamp() - interval '2 seconds',
		        expires_at = clock_timestamp() - interval '1 second'
		  WHERE id = $1`, first.ID,
	); err != nil {
		t.Fatalf("expire invite: %v", err)
	}
	second, _, err := s.IssueInvite(ctx, "ALICE", time.Hour)
	if err != nil {
		t.Fatalf("replacement invite: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("replacement reused invite id %d", second.ID)
	}

	listed, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatalf("list invites: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed invites = %d, want 2", len(listed))
	}
	for _, got := range listed {
		switch got.ID {
		case first.ID:
			if got.State != store.InviteExpired {
				t.Errorf("old invite state = %q, want expired", got.State)
			}
		case second.ID:
			if got.State != store.InviteIssued {
				t.Errorf("replacement state = %q, want issued", got.State)
			}
		default:
			t.Errorf("unexpected listed invite %d", got.ID)
		}
	}

	if _, _, err := s.IssueInvite(ctx, "bob", -time.Second); !errors.Is(err, store.ErrInviteLifetimeInvalid) {
		t.Fatalf("negative lifetime: err = %v, want ErrInviteLifetimeInvalid", err)
	}
	if _, _, err := s.IssueInvite(ctx, "bob", store.MaxInviteLifetime+time.Second); !errors.Is(err, store.ErrInviteLifetimeInvalid) {
		t.Fatalf("overlong lifetime: err = %v, want ErrInviteLifetimeInvalid", err)
	}
	if _, _, err := s.IssueInvite(ctx, "max", store.MaxInviteLifetime); err != nil {
		t.Fatalf("maximum lifetime: %v", err)
	}
}

func TestConcurrentIssueInviteHasOneLiveRow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const attempts = 10
	results := make([]error, attempts)
	ready := make(chan struct{})
	var readyWG sync.WaitGroup
	var wg sync.WaitGroup
	readyWG.Add(attempts)
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			readyWG.Done()
			<-ready
			_, _, results[i] = s.IssueInvite(ctx, "Alice", time.Hour)
		}(i)
	}
	readyWG.Wait()
	close(ready)
	wg.Wait()

	var issued, liveErrors int
	for _, err := range results {
		switch {
		case err == nil:
			issued++
		case errors.Is(err, store.ErrInviteLive):
			liveErrors++
		default:
			t.Errorf("unexpected issue error: %v", err)
		}
	}
	if issued != 1 || liveErrors != attempts-1 {
		t.Fatalf("issue results: issued=%d live errors=%d, want 1/%d", issued, liveErrors, attempts-1)
	}

	var liveRows int
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT count(*) FROM registration_invites
		  WHERE handle = 'alice' AND state = 'issued' AND expires_at > clock_timestamp()`,
	).Scan(&liveRows); err != nil {
		t.Fatalf("count live invites: %v", err)
	}
	if liveRows != 1 {
		t.Fatalf("live invite rows = %d, want 1", liveRows)
	}
}

func TestVerifyInviteReturnsOneNegativeForEveryUnusableState(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	type caseInvite struct {
		name   string
		handle string
		secret string
		setup  func(store.RegistrationInvite)
	}
	cases := make([]caseInvite, 0, 4)

	wrong, wrongSecret, err := s.IssueInvite(ctx, "wrong")
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, caseInvite{name: "wrong secret", handle: wrong.Handle, secret: wrongSecret + "x"})

	expired, expiredSecret, err := s.IssueInvite(ctx, "expired")
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, caseInvite{
		name: "expired", handle: expired.Handle, secret: expiredSecret,
		setup: func(inv store.RegistrationInvite) {
			if _, err := store.StorePool(s).Exec(ctx,
				`UPDATE registration_invites
				    SET issued_at = clock_timestamp() - interval '2 seconds',
				        expires_at = clock_timestamp() - interval '1 second'
				  WHERE id = $1`, inv.ID,
			); err != nil {
				t.Fatalf("expire %s invite: %v", inv.Handle, err)
			}
		},
	})

	revoked, revokedSecret, err := s.IssueInvite(ctx, "revoked")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeInvite(ctx, revoked.ID); err != nil {
		t.Fatalf("revoke invite: %v", err)
	}
	cases = append(cases, caseInvite{name: "revoked", handle: revoked.Handle, secret: revokedSecret})

	consumed, consumedSecret, err := s.IssueInvite(ctx, "consumed")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeInvite(ctx, tx, consumed.Handle, consumedSecret); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			t.Errorf("rollback failed consume: %v", rollbackErr)
		}
		t.Fatalf("consume invite: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	cases = append(cases, caseInvite{name: "consumed", handle: consumed.Handle, secret: consumedSecret})

	for _, tc := range cases {
		if tc.setup != nil {
			tc.setup(map[string]store.RegistrationInvite{
				"expired":  expired,
				"revoked":  revoked,
				"consumed": consumed,
				"wrong":    wrong,
			}[tc.name])
		}
		if got := s.VerifyInvite(ctx, tc.handle, tc.secret); !errors.Is(got, store.ErrInviteInvalid) {
			t.Errorf("%s: err = %v, want ErrInviteInvalid", tc.name, got)
		}
	}

	if got := s.VerifyInvite(ctx, "never-issued", "anything"); !errors.Is(got, store.ErrInviteInvalid) {
		t.Errorf("missing invite: err = %v, want ErrInviteInvalid", got)
	}

	listed, err := s.ListInvites(ctx)
	if err != nil {
		t.Fatalf("list terminal invite states: %v", err)
	}
	states := make(map[string]store.InviteState, len(listed))
	for _, invite := range listed {
		states[invite.Handle] = invite.State
	}
	for handle, want := range map[string]store.InviteState{
		"wrong":    store.InviteIssued,
		"expired":  store.InviteExpired,
		"revoked":  store.InviteRevoked,
		"consumed": store.InviteConsumed,
	} {
		if states[handle] != want {
			t.Errorf("listed %s state = %q, want %q", handle, states[handle], want)
		}
	}
}

func TestConsumeInviteRollbackLeavesInviteLive(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	invite, secret, err := s.IssueInvite(ctx, "rollback")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeInvite(ctx, tx, invite.Handle, secret); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			t.Errorf("rollback failed consume: %v", rollbackErr)
		}
		t.Fatalf("consume in caller transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyInvite(ctx, invite.Handle, secret); err != nil {
		t.Fatalf("verify after rollback: %v", err)
	}

	tx, err = store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeInvite(ctx, tx, invite.Handle, secret); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			t.Errorf("rollback failed consume: %v", rollbackErr)
		}
		t.Fatalf("consume after rollback: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyInvite(ctx, invite.Handle, secret); !errors.Is(err, store.ErrInviteInvalid) {
		t.Fatalf("verify after committed consume: err = %v, want ErrInviteInvalid", err)
	}
}

func TestConcurrentConsumeInviteHasExactlyOneWinner(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	invite, secret, err := s.IssueInvite(ctx, "racing")
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 10
	results := make([]error, attempts)
	ready := make(chan struct{})
	var readyWG sync.WaitGroup
	var wg sync.WaitGroup
	readyWG.Add(attempts)
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			readyWG.Done()
			<-ready
			tx, err := store.StorePool(s).Begin(ctx)
			if err != nil {
				results[i] = err
				return
			}
			results[i] = s.ConsumeInvite(ctx, tx, invite.Handle, secret)
			if results[i] == nil {
				if err := tx.Commit(ctx); err != nil {
					results[i] = err
				}
			} else if err := tx.Rollback(ctx); err != nil {
				results[i] = err
			}
		}(i)
	}
	readyWG.Wait()
	close(ready)
	wg.Wait()

	var successes, invalids int
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, store.ErrInviteInvalid):
			invalids++
		default:
			t.Errorf("unexpected consume error: %v", err)
		}
	}
	if successes != 1 || invalids != attempts-1 {
		t.Fatalf("consume results: successes=%d invalids=%d, want 1/%d", successes, invalids, attempts-1)
	}
	if err := s.VerifyInvite(ctx, invite.Handle, secret); !errors.Is(err, store.ErrInviteInvalid) {
		t.Fatalf("verify after race: err = %v, want ErrInviteInvalid", err)
	}
}

func TestRevokeAndConsumeInviteRowOrderChoosesOneWinner(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	invite, secret, err := s.IssueInvite(ctx, "ordered")
	if err != nil {
		t.Fatal(err)
	}

	blocker, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }() //nolint:errcheck // cleanup after test
	var lockedID int64
	if err := blocker.QueryRow(ctx,
		`SELECT id FROM registration_invites WHERE id = $1 FOR UPDATE`, invite.ID,
	).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	if lockedID != invite.ID {
		t.Fatalf("locked invite id = %d, want %d", lockedID, invite.ID)
	}

	revokeDone := make(chan error, 1)
	go func() { revokeDone <- s.RevokeInvite(ctx, invite.ID) }()
	if err := store.WaitForLockWaiters(ctx, s, 1); err != nil {
		t.Fatal(err)
	}

	consumeDone := make(chan error, 1)
	go func() {
		tx, err := store.StorePool(s).Begin(ctx)
		if err != nil {
			consumeDone <- err
			return
		}
		err = s.ConsumeInvite(ctx, tx, invite.Handle, secret)
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				err = rollbackErr
			}
		}
		consumeDone <- err
	}()
	if err := store.WaitForLockWaiters(ctx, s, 2); err != nil {
		t.Fatal(err)
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-revokeDone; err != nil {
		t.Fatalf("revoke winner: %v", err)
	}
	if err := <-consumeDone; !errors.Is(err, store.ErrInviteInvalid) {
		t.Fatalf("consume loser: err = %v, want ErrInviteInvalid", err)
	}
	if err := s.VerifyInvite(ctx, invite.Handle, secret); !errors.Is(err, store.ErrInviteInvalid) {
		t.Fatalf("verify after revoke race: err = %v, want ErrInviteInvalid", err)
	}
}
