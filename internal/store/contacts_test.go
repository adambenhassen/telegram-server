package store_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestContactsAreDirectedIdempotentAndMutual(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551900001")
	b := mustUser(t, s, "+15551900002")
	c := mustUser(t, s, "+15551900003")

	changed, err := s.AddContact(ctx, a.ID, c.ID)
	if err != nil || !changed {
		t.Fatalf("add C for A: changed=%v err=%v", changed, err)
	}
	changed, err = s.AddContact(ctx, a.ID, b.ID)
	if err != nil || !changed {
		t.Fatalf("add B for A: changed=%v err=%v", changed, err)
	}
	changed, err = s.AddContact(ctx, a.ID, b.ID)
	if err != nil || changed {
		t.Fatalf("duplicate add: changed=%v err=%v, want false/nil", changed, err)
	}

	contacts, total, err := s.Contacts(ctx, a.ID)
	if err != nil {
		t.Fatalf("A contacts: %v", err)
	}
	if total != 2 || len(contacts) != 2 {
		t.Fatalf("A contacts = %+v total=%d, want two", contacts, total)
	}
	if contacts[0].UserID != b.ID || contacts[0].Mutual || contacts[1].UserID != c.ID || contacts[1].Mutual {
		t.Fatalf("A contacts = %+v, want ordered B then C with no mutual edges", contacts)
	}
	for _, ownerID := range []int64{b.ID, c.ID} {
		got, count, err := s.Contacts(ctx, ownerID)
		if err != nil || len(got) != 0 || count != 0 {
			t.Fatalf("contacts for owner %d = %+v total=%d err=%v, want empty", ownerID, got, count, err)
		}
	}

	changed, err = s.AddContact(ctx, b.ID, a.ID)
	if err != nil || !changed {
		t.Fatalf("add A for B: changed=%v err=%v", changed, err)
	}
	for _, ownerID := range []int64{a.ID, b.ID} {
		got, count, err := s.Contacts(ctx, ownerID)
		if err != nil {
			t.Fatalf("mutual contacts for owner %d: %v", ownerID, err)
		}
		if count != len(got) {
			t.Fatalf("mutual contact count for owner %d = %d, rows=%d", ownerID, count, len(got))
		}
		var found bool
		for _, contact := range got {
			if contact.UserID == map[int64]int64{a.ID: b.ID, b.ID: a.ID}[ownerID] {
				found = true
				if !contact.Mutual {
					t.Errorf("reciprocal edge for owner %d is not mutual", ownerID)
				}
			}
		}
		if !found {
			t.Errorf("owner %d is missing its reciprocal contact", ownerID)
		}
	}
}

func TestConcurrentReciprocalContactAddsConverge(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551900101")
	b := mustUser(t, s, "+15551900102")

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, edge := range [][2]int64{{a.ID, b.ID}, {b.ID, a.ID}} {
		wg.Add(1)
		go func(ownerID, contactID int64) {
			defer wg.Done()
			<-start
			changed, err := s.AddContact(ctx, ownerID, contactID)
			if err == nil && !changed {
				err = errors.New("first concurrent add was reported as a duplicate")
			}
			results <- err
		}(edge[0], edge[1])
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent reciprocal add: %v", err)
		}
	}

	for ownerID, contactID := range map[int64]int64{a.ID: b.ID, b.ID: a.ID} {
		contacts, total, err := s.Contacts(ctx, ownerID)
		if err != nil || total != 1 || len(contacts) != 1 {
			t.Fatalf("contacts for owner %d = %+v total=%d err=%v, want one", ownerID, contacts, total, err)
		}
		if contacts[0].UserID != contactID || !contacts[0].Mutual {
			t.Errorf("contact for owner %d = %+v, want mutual %d", ownerID, contacts[0], contactID)
		}
	}
}

func TestRemovingContactLeavesOtherRelationshipsUnchanged(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551900201")
	b := mustUser(t, s, "+15551900202")
	chat := chatWith(t, s, a, b)
	message := send(t, s, a, b, "conversation remains", 190201)
	for _, edge := range [][2]int64{{a.ID, b.ID}, {b.ID, a.ID}} {
		if _, err := s.BlockUser(ctx, edge[0], edge[1]); err != nil {
			t.Fatalf("block %d -> %d: %v", edge[0], edge[1], err)
		}
	}
	for _, edge := range [][2]int64{{a.ID, b.ID}, {b.ID, a.ID}} {
		changed, err := s.AddContact(ctx, edge[0], edge[1])
		if err != nil || !changed {
			t.Fatalf("add contact %d -> %d: changed=%v err=%v", edge[0], edge[1], changed, err)
		}
	}

	wantParticipants := participantIDs(t, s, chat.ID)
	wantVersion := chatVersion(t, s, chat.ID)
	wantPts := map[int64]int{a.ID: ptsOf(t, s, a.ID), b.ID: ptsOf(t, s, b.ID)}
	wantEvents := map[int64][]store.Event{
		a.ID: eventsOf(t, s, a.ID, 0),
		b.ID: eventsOf(t, s, b.ID, 0),
	}

	changed, err := s.RemoveContact(ctx, a.ID, b.ID)
	if err != nil || !changed {
		t.Fatalf("remove A -> B: changed=%v err=%v", changed, err)
	}
	changed, err = s.RemoveContact(ctx, a.ID, b.ID)
	if err != nil || changed {
		t.Fatalf("repeat remove A -> B: changed=%v err=%v, want false/nil", changed, err)
	}

	contactsA, countA, err := s.Contacts(ctx, a.ID)
	if err != nil || len(contactsA) != 0 || countA != 0 {
		t.Fatalf("A contacts after remove = %+v total=%d err=%v, want empty", contactsA, countA, err)
	}
	contactsB, countB, err := s.Contacts(ctx, b.ID)
	if err != nil || countB != 1 || len(contactsB) != 1 || contactsB[0].UserID != a.ID || contactsB[0].Mutual {
		t.Fatalf("B contacts after remove = %+v total=%d err=%v, want only A and not mutual", contactsB, countB, err)
	}
	if got := participantIDs(t, s, chat.ID); !reflect.DeepEqual(got, wantParticipants) {
		t.Errorf("chat participants after contact removal = %v, want %v", got, wantParticipants)
	}
	if got := chatVersion(t, s, chat.ID); got != wantVersion {
		t.Errorf("chat version after contact removal = %d, want %d", got, wantVersion)
	}
	for _, edge := range [][2]int64{{a.ID, b.ID}, {b.ID, a.ID}} {
		blocked, err := s.IsBlocked(ctx, edge[0], edge[1])
		if err != nil || !blocked {
			t.Errorf("block %d -> %d after contact removal = %v, err=%v", edge[0], edge[1], blocked, err)
		}
	}
	for ownerID, want := range wantPts {
		if got := ptsOf(t, s, ownerID); got != want {
			t.Errorf("owner %d pts after contact mutation = %d, want %d", ownerID, got, want)
		}
		if got := eventsOf(t, s, ownerID, 0); !reflect.DeepEqual(got, wantEvents[ownerID]) {
			t.Errorf("owner %d events changed after contact mutation: got %d, want %d", ownerID, len(got), len(wantEvents[ownerID]))
		}
	}
	got, ok, err := s.MessageByOwnerLocal(ctx, a.ID, message.LocalID)
	if err != nil || !ok || got.LocalID != message.LocalID || got.Text != message.Text {
		t.Errorf("conversation message after contact removal = %+v, ok=%v err=%v", got, ok, err)
	}
}

func TestContactsRejectSelfAndMissingUsers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551900301")
	b := mustUser(t, s, "+15551900302")

	if _, err := s.AddContact(ctx, a.ID, a.ID); !errors.Is(err, store.ErrInvalidContact) {
		t.Errorf("self add error = %v, want ErrInvalidContact", err)
	}
	if _, err := s.RemoveContact(ctx, a.ID, a.ID); !errors.Is(err, store.ErrInvalidContact) {
		t.Errorf("self remove error = %v, want ErrInvalidContact", err)
	}
	if _, err := s.AddContact(ctx, a.ID, b.ID+1000); err == nil {
		t.Error("add to a missing user succeeded")
	}
	if _, err := s.AddContact(ctx, a.ID+1000, b.ID); err == nil {
		t.Error("add for a missing owner succeeded")
	}
	contacts, total, err := s.Contacts(ctx, a.ID)
	if err != nil || len(contacts) != 0 || total != 0 {
		t.Fatalf("contacts after rejected adds = %+v total=%d err=%v, want empty", contacts, total, err)
	}
}

func TestConcurrentContactAddsRespectOwnerCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	owner := mustUser(t, s, "+15551900401")
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close test connection: %v", err)
		}
	})

	rows, err := conn.Query(ctx, `
WITH created AS (
    INSERT INTO users (phone)
    SELECT 'contact-cap-' || n FROM generate_series(1, 5015) AS n
    RETURNING id
)
INSERT INTO update_state (user_id)
SELECT id FROM created
RETURNING user_id`)
	if err != nil {
		t.Fatalf("create cap test users: %v", err)
	}
	userIDs := make([]int64, 0, 5015)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			rows.Close()
			t.Fatalf("scan cap test user: %v", err)
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("read cap test users: %v", err)
	}
	rows.Close()
	if len(userIDs) != 5015 {
		t.Fatalf("created %d cap test users, want 5015", len(userIDs))
	}
	slices.Sort(userIDs)
	_, err = conn.Exec(ctx, `
INSERT INTO user_contacts (owner_id, contact_id)
SELECT $1, contact_id FROM unnest($2::bigint[]) AS seeded(contact_id)`, owner.ID, userIDs[:4999])
	if err != nil {
		t.Fatalf("seed contacts at 4,999: %v", err)
	}

	type result struct {
		contactID int64
		changed   bool
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for _, contactID := range userIDs[4999:] {
		wg.Add(1)
		go func(contactID int64) {
			defer wg.Done()
			<-start
			changed, err := s.AddContact(ctx, owner.ID, contactID)
			results <- result{contactID: contactID, changed: changed, err: err}
		}(contactID)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner int64
	var denied int64
	for result := range results {
		if result.err == nil && result.changed {
			if winner != 0 {
				t.Fatalf("more than one concurrent add crossed the cap: %d and %d", winner, result.contactID)
			}
			winner = result.contactID
			continue
		}
		if !errors.Is(result.err, store.ErrContactLimit) {
			t.Fatalf("concurrent add %d = changed %v, err %v, want ErrContactLimit", result.contactID, result.changed, result.err)
		}
		if denied == 0 {
			denied = result.contactID
		}
	}
	if winner == 0 || denied == 0 {
		t.Fatalf("concurrent results have winner %d and denied contact %d, want one of each", winner, denied)
	}

	changed, err := s.AddContact(ctx, owner.ID, winner)
	if err != nil || changed {
		t.Fatalf("duplicate add at cap = changed %v, err %v, want false/nil", changed, err)
	}
	if _, err := s.AddContact(ctx, owner.ID, denied); !errors.Is(err, store.ErrContactLimit) {
		t.Fatalf("new add at cap error = %v, want ErrContactLimit", err)
	}
	contacts, total, err := s.Contacts(ctx, owner.ID)
	if err != nil || total != 5000 || len(contacts) != 5000 {
		t.Fatalf("contact count after concurrent cap adds = rows %d total %d err %v, want 5,000", len(contacts), total, err)
	}
	for i := 1; i < len(contacts); i++ {
		if contacts[i-1].UserID >= contacts[i].UserID {
			t.Fatalf("contacts are not strictly ordered at %d: %d then %d", i, contacts[i-1].UserID, contacts[i].UserID)
		}
	}
}
