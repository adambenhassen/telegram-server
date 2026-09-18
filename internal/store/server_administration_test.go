package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestServerAdministrationStartsOpenOnEmptyDatabase(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	var rows int
	var closed bool
	var administrator *int64
	if err := store.StorePool(s).QueryRow(ctx, `
		SELECT count(*),
		       bool_and(election_closed),
		       max(administrator_user_id)
		FROM server_administration
	`).Scan(&rows, &closed, &administrator); err != nil {
		t.Fatalf("read server administration: %v", err)
	}
	if rows != 1 {
		t.Fatalf("server administration rows = %d, want 1", rows)
	}
	if closed {
		t.Fatal("empty database election is closed")
	}
	if administrator != nil {
		t.Fatalf("empty database administrator = %d, want nil", *administrator)
	}
}

func TestCreateUserElectsOnlyTheFirstNewUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	owner, err := s.CreateUser(ctx, "+15550000001")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	if ok, err := s.IsServerAdministrator(ctx, owner.ID); err != nil {
		t.Fatalf("check owner administrator: %v", err)
	} else if !ok {
		t.Fatal("first new user is not the server administrator")
	}

	member, err := s.CreateUser(ctx, "+15550000002")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	if ok, err := s.IsServerAdministrator(ctx, owner.ID); err != nil {
		t.Fatalf("recheck owner administrator: %v", err)
	} else if !ok {
		t.Fatal("second creation moved the administrator grant")
	}
	if ok, err := s.IsServerAdministrator(ctx, member.ID); err != nil {
		t.Fatalf("check member administrator: %v", err)
	} else if ok {
		t.Fatal("second new user received the administrator grant")
	}

	duplicate, err := s.CreateUser(ctx, "+15550000001")
	if err != nil {
		t.Fatalf("idempotent lookup: %v", err)
	}
	if duplicate.ID != owner.ID {
		t.Fatalf("idempotent lookup user id = %d, want %d", duplicate.ID, owner.ID)
	}
	if ok, err := s.IsServerAdministrator(ctx, duplicate.ID); err != nil {
		t.Fatalf("check idempotent lookup administrator: %v", err)
	} else if !ok {
		t.Fatal("idempotent lookup changed the administrator grant")
	}
}

func TestConcurrentCreateUserElectsExactlyOneAdministrator(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const attempts = 12
	users := make([]store.User, attempts)
	errs := make([]error, attempts)
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
			users[i], errs[i] = s.CreateUser(ctx, fmt.Sprintf("+15550000%04d", i+10))
		}(i)
	}
	readyWG.Wait()
	close(ready)
	wg.Wait()

	var administrators int
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT count(*) FROM server_administration WHERE administrator_user_id IS NOT NULL`,
	).Scan(&administrators); err != nil {
		t.Fatalf("count administrators: %v", err)
	}
	if administrators != 1 {
		t.Fatalf("administrator rows = %d, want 1", administrators)
	}
	var winners int
	for i, err := range errs {
		if err != nil {
			t.Fatalf("create user %d: %v", i, err)
		}
		ok, err := s.IsServerAdministrator(ctx, users[i].ID)
		if err != nil {
			t.Fatalf("check user %d administrator: %v", i, err)
		}
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("administrator winners = %d, want 1", winners)
	}
}

func TestCreateUserFailsClosedWhenAdministrationSingletonIsMissing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	if _, err := store.StorePool(s).Exec(ctx, `DELETE FROM server_administration`); err != nil {
		t.Fatalf("delete administration singleton: %v", err)
	}
	if _, err := s.CreateUser(ctx, "+15550000099"); err == nil {
		t.Fatal("create user with missing administration singleton succeeded")
	}

	var users int
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT count(*) FROM users WHERE phone = '15550000099'`,
	).Scan(&users); err != nil {
		t.Fatalf("count rolled-back user: %v", err)
	}
	if users != 0 {
		t.Fatalf("rolled-back user rows = %d, want 0", users)
	}
}

func TestCreateUserFailsClosedWhenAdministrationSingletonIsDuplicated(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	if _, err := store.StorePool(s).Exec(ctx,
		`ALTER TABLE server_administration DROP CONSTRAINT server_administration_pkey`,
	); err != nil {
		t.Fatalf("drop singleton key: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx, `
		INSERT INTO server_administration (singleton_id, election_closed)
		VALUES (1, FALSE)
	`); err != nil {
		t.Fatalf("duplicate singleton: %v", err)
	}
	if _, err := s.CreateUser(ctx, "+15550000100"); err == nil {
		t.Fatal("create user with duplicated administration singleton succeeded")
	}

	var users int
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT count(*) FROM users WHERE phone = '15550000100'`,
	).Scan(&users); err != nil {
		t.Fatalf("count rolled-back duplicate user: %v", err)
	}
	if users != 0 {
		t.Fatalf("rolled-back duplicate user rows = %d, want 0", users)
	}
}

func TestCreateUserFailsClosedWhenAdministrationStateIsInvalid(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	owner, err := s.CreateUser(ctx, "+15550000101")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx,
		`ALTER TABLE server_administration DROP CONSTRAINT server_administration_check`,
	); err != nil {
		t.Fatalf("drop singleton state check: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx,
		`UPDATE server_administration SET election_closed = FALSE, administrator_user_id = $1 WHERE singleton_id = 1`,
		owner.ID,
	); err != nil {
		t.Fatalf("corrupt singleton state: %v", err)
	}
	if administrator, err := s.IsServerAdministrator(ctx, owner.ID); err != nil {
		t.Fatalf("check invalid singleton authority: %v", err)
	} else if administrator {
		t.Fatal("invalid singleton granted administrator authority")
	}
	if _, err := s.CreateUser(ctx, "+15550000102"); err == nil {
		t.Fatal("create user with invalid administration state succeeded")
	}

	var users int
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT count(*) FROM users WHERE phone = '15550000102'`,
	).Scan(&users); err != nil {
		t.Fatalf("count rolled-back invalid-state user: %v", err)
	}
	if users != 0 {
		t.Fatalf("rolled-back invalid-state user rows = %d, want 0", users)
	}
}

func TestIsServerAdministratorRejectsUnpersistedIDs(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	for _, userID := range []int64{0, -1, 999999999} {
		ok, err := s.IsServerAdministrator(ctx, userID)
		if err != nil {
			t.Fatalf("check user %d: %v", userID, err)
		}
		if ok {
			t.Fatalf("unpersisted user %d is an administrator", userID)
		}
	}
}
