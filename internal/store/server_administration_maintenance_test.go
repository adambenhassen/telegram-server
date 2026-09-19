package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestAssignOperatorServerAdministrator(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	operatorID := seedOperator(t, s)

	if err := s.AssignOperatorServerAdministrator(ctx); err != nil {
		t.Fatalf("assign operator server administrator: %v", err)
	}

	var administratorID *int64
	if err := store.StorePool(s).QueryRow(ctx,
		`SELECT administrator_user_id FROM server_administration WHERE singleton_id = 1`,
	).Scan(&administratorID); err != nil {
		t.Fatalf("read administrator: %v", err)
	}
	if administratorID == nil {
		t.Fatal("administrator is nil")
	}
	if *administratorID != operatorID {
		t.Fatalf("administrator = %d, want operator %d", *administratorID, operatorID)
	}
}

func TestAssignOperatorServerAdministratorIsIdempotent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	operatorID := seedOperator(t, s)

	if err := s.AssignOperatorServerAdministrator(ctx); err != nil {
		t.Fatalf("first assignment: %v", err)
	}
	before := maintenanceState(t, s)
	if err := s.AssignOperatorServerAdministrator(ctx); err != nil {
		t.Fatalf("idempotent assignment: %v", err)
	}
	if after := maintenanceState(t, s); after != before {
		t.Fatalf("idempotent assignment changed state:\nbefore: %s\nafter:  %s", before, after)
	}
	if ok, err := s.IsServerAdministrator(ctx, operatorID); err != nil || !ok {
		t.Fatalf("operator administrator after retry: ok=%v err=%v", ok, err)
	}
}

func TestAssignOperatorServerAdministratorRejectsInvalidAccountState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*testing.T, *store.Store, int64)
	}{
		{
			name: "extra user",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					INSERT INTO users (phone, login_mode)
					VALUES ('15550000999', 'phone')
				`); err != nil {
					t.Fatalf("insert extra user: %v", err)
				}
			},
		},
		{
			name: "phone mode",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE users SET phone = '15550000998', login_mode = 'phone' WHERE id = $1
				`, operatorID); err != nil {
					t.Fatalf("make phone-mode account: %v", err)
				}
			},
		},
		{
			name: "missing verifier",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(),
					`DELETE FROM user_passwords WHERE user_id = $1`, operatorID); err != nil {
					t.Fatalf("delete verifier: %v", err)
				}
			},
		},
		{
			name: "unreadable verifier",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE user_passwords SET verifier = decode('00', 'hex') WHERE user_id = $1
				`, operatorID); err != nil {
					t.Fatalf("corrupt verifier: %v", err)
				}
			},
		},
		{
			name: "denormalized username mismatch",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE users SET username = 'other' WHERE id = $1
				`, operatorID); err != nil {
					t.Fatalf("mismatch denormalized username: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := open(t)
			operatorID := seedOperator(t, s)
			tc.mutate(t, s, operatorID)
			before := maintenanceState(t, s)
			if err := s.AssignOperatorServerAdministrator(context.Background()); err == nil {
				t.Fatal("invalid account state was accepted")
			}
			if after := maintenanceState(t, s); after != before {
				t.Fatalf("rejected account state changed durable state:\nbefore: %s\nafter:  %s", before, after)
			}
		})
	}
}

func TestAssignOperatorServerAdministratorRejectsUsernameAmbiguity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*testing.T, *store.Store, int64)
	}{
		{
			name: "channel owner",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE usernames SET owner_type = 'channel', owner_id = $1 WHERE handle = 'operator'
				`, operatorID); err != nil {
					t.Fatalf("make channel owner: %v", err)
				}
			},
		},
		{
			name: "dangling owner",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE usernames SET owner_id = 999999999 WHERE handle = 'operator'
				`); err != nil {
					t.Fatalf("make dangling owner: %v", err)
				}
			},
		},
		{
			name: "missing operator row",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					DELETE FROM usernames WHERE handle = 'operator'
				`); err != nil {
					t.Fatalf("delete operator username: %v", err)
				}
			},
		},
		{
			name: "duplicate rows",
			mutate: func(t *testing.T, s *store.Store, operatorID int64) {
				t.Helper()
				ctx := context.Background()
				if _, err := store.StorePool(s).Exec(ctx,
					`ALTER TABLE usernames DROP CONSTRAINT usernames_pkey`); err != nil {
					t.Fatalf("drop username key: %v", err)
				}
				if _, err := store.StorePool(s).Exec(ctx, `
					INSERT INTO usernames (handle, owner_type, owner_id)
					VALUES ('operator', 'user', $1)
				`, operatorID); err != nil {
					t.Fatalf("duplicate username row: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := open(t)
			operatorID := seedOperator(t, s)
			tc.mutate(t, s, operatorID)
			before := maintenanceState(t, s)
			if err := s.AssignOperatorServerAdministrator(context.Background()); err == nil {
				t.Fatal("invalid username state was accepted")
			}
			if after := maintenanceState(t, s); after != before {
				t.Fatalf("rejected username state changed durable state:\nbefore: %s\nafter:  %s", before, after)
			}
		})
	}
}

func TestAssignOperatorServerAdministratorRejectsVerifierAmbiguity(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	operatorID := seedOperator(t, s)

	var salt1, salt2, verifier []byte
	if err := store.StorePool(s).QueryRow(ctx, `
		SELECT salt1, salt2, verifier FROM user_passwords WHERE user_id = $1
	`, operatorID).Scan(&salt1, &salt2, &verifier); err != nil {
		t.Fatalf("read verifier fixture: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx,
		`ALTER TABLE user_passwords DROP CONSTRAINT user_passwords_pkey`); err != nil {
		t.Fatalf("drop verifier key: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx, `
		INSERT INTO user_passwords (user_id, salt1, salt2, verifier)
		VALUES ($1, $2, $3, $4)
	`, operatorID, salt1, salt2, verifier); err != nil {
		t.Fatalf("duplicate verifier row: %v", err)
	}

	before := maintenanceState(t, s)
	if err := s.AssignOperatorServerAdministrator(ctx); err == nil {
		t.Fatal("duplicate verifier rows were accepted")
	}
	if after := maintenanceState(t, s); after != before {
		t.Fatalf("rejected verifier state changed durable state:\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestAssignOperatorServerAdministratorRejectsElectionState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*testing.T, *store.Store, int64)
	}{
		{
			name: "missing singleton",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(),
					`DELETE FROM server_administration`); err != nil {
					t.Fatalf("delete singleton: %v", err)
				}
			},
		},
		{
			name: "duplicate singleton",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				ctx := context.Background()
				if _, err := store.StorePool(s).Exec(ctx,
					`ALTER TABLE server_administration DROP CONSTRAINT server_administration_pkey`); err != nil {
					t.Fatalf("drop singleton key: %v", err)
				}
				if _, err := store.StorePool(s).Exec(ctx, `
					INSERT INTO server_administration (singleton_id, election_closed)
					VALUES (1, TRUE)
				`); err != nil {
					t.Fatalf("duplicate singleton: %v", err)
				}
			},
		},
		{
			name: "open election",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				if _, err := store.StorePool(s).Exec(context.Background(), `
					UPDATE server_administration SET election_closed = FALSE
					WHERE singleton_id = 1
				`); err != nil {
					t.Fatalf("open election: %v", err)
				}
			},
		},
		{
			name: "assigned to another user",
			mutate: func(t *testing.T, s *store.Store, _ int64) {
				t.Helper()
				ctx := context.Background()
				if _, err := store.StorePool(s).Exec(ctx,
					`ALTER TABLE server_administration DROP CONSTRAINT server_administration_administrator_user_id_fkey`); err != nil {
					t.Fatalf("drop administrator foreign key: %v", err)
				}
				if _, err := store.StorePool(s).Exec(ctx, `
					UPDATE server_administration
					SET administrator_user_id = 999999999
					WHERE singleton_id = 1
				`); err != nil {
					t.Fatalf("assign other administrator: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := open(t)
			operatorID := seedOperator(t, s)
			tc.mutate(t, s, operatorID)
			before := maintenanceState(t, s)
			if err := s.AssignOperatorServerAdministrator(context.Background()); err == nil {
				t.Fatal("invalid election state was accepted")
			}
			if after := maintenanceState(t, s); after != before {
				t.Fatalf("rejected election state changed durable state:\nbefore: %s\nafter:  %s", before, after)
			}
		})
	}
}

func TestConcurrentAssignOperatorServerAdministrator(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	operatorID := seedOperator(t, s)

	const attempts = 8
	start := make(chan struct{})
	errs := make([]error, attempts)
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = s.AssignOperatorServerAdministrator(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent assignment %d: %v", i, err)
		}
	}
	var count int
	if err := store.StorePool(s).QueryRow(ctx, `
		SELECT count(*) FROM server_administration
		WHERE administrator_user_id IS NOT NULL
	`).Scan(&count); err != nil {
		t.Fatalf("count administrators: %v", err)
	}
	if count != 1 {
		t.Fatalf("administrator assignments = %d, want 1", count)
	}
	if ok, err := s.IsServerAdministrator(ctx, operatorID); err != nil || !ok {
		t.Fatalf("operator administrator after concurrent assignment: ok=%v err=%v", ok, err)
	}
}

func TestAssignOperatorServerAdministratorSerializesConcurrentUserCreation(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seedOperator(t, s)

	// Hold the update_state relation so CreateUsernameUser has inserted its
	// user, but cannot reach the election row yet. The maintenance transaction
	// must wait on that transaction's users-table lock before it can decide that
	// exactly one user exists.
	blocker, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatalf("begin update-state blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }() //nolint:errcheck // cleanup after explicit release
	if _, err := blocker.Exec(ctx, `LOCK TABLE update_state IN SHARE MODE`); err != nil {
		t.Fatalf("lock update_state: %v", err)
	}

	createDone := make(chan error, 1)
	go func() {
		_, err := s.CreateUsernameUser(ctx, "racer", "Racer", "")
		createDone <- err
	}()
	waitForMaintenanceLocks := func(n int) error {
		waitCtx, cancelWait := context.WithTimeout(ctx, 3*time.Second)
		defer cancelWait()
		return store.WaitForLockWaiters(waitCtx, s, n)
	}
	if err := waitForMaintenanceLocks(1); err != nil {
		_ = blocker.Rollback(context.Background()) //nolint:errcheck // release before reporting the setup failure
		t.Fatalf("CreateUsernameUser did not reach its blocked election step: %v", err)
	}

	assignDone := make(chan error, 1)
	go func() { assignDone <- s.AssignOperatorServerAdministrator(ctx) }()
	if err := waitForMaintenanceLocks(2); err != nil {
		_ = blocker.Rollback(context.Background()) //nolint:errcheck // release before collecting goroutines
		createErr := <-createDone
		assignErr := <-assignDone
		t.Fatalf("maintenance did not wait for the in-flight user insert: %v (create=%v assign=%v)", err, createErr, assignErr)
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release update-state blocker: %v", err)
	}
	if err := <-createDone; err != nil {
		t.Fatalf("concurrent user creation: %v", err)
	}
	if err := <-assignDone; !errors.Is(err, store.ErrOperatorAdministrationInvalid) {
		t.Fatalf("assignment with concurrent user creation: err=%v, want ErrOperatorAdministrationInvalid", err)
	}

	var users int
	var administratorID *int64
	if err := store.StorePool(s).QueryRow(ctx, `
		SELECT (SELECT count(*) FROM users), administrator_user_id
		FROM server_administration
		WHERE singleton_id = 1
	`).Scan(&users, &administratorID); err != nil {
		t.Fatalf("read post-race state: %v", err)
	}
	if users != 2 {
		t.Fatalf("users after rejected assignment = %d, want 2", users)
	}
	if administratorID != nil {
		t.Fatalf("administrator after rejected assignment = %d, want nil", *administratorID)
	}
}

func maintenanceState(t *testing.T, s *store.Store) string {
	t.Helper()
	ctx := context.Background()
	var users, usernames, passwords, administration string
	queries := []struct {
		name string
		out  *string
		sql  string
	}{
		{
			name: "users",
			out:  &users,
			sql:  `SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY x.id)::text, '[]') FROM (SELECT id, phone, username, login_mode FROM users) x`,
		},
		{
			name: "usernames",
			out:  &usernames,
			sql:  `SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY x.handle, x.owner_type, x.owner_id)::text, '[]') FROM (SELECT handle, owner_type, owner_id FROM usernames) x`,
		},
		{
			name: "passwords",
			out:  &passwords,
			sql:  `SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY x.user_id)::text, '[]') FROM (SELECT user_id, salt1, salt2, verifier FROM user_passwords) x`,
		},
		{
			name: "administration",
			out:  &administration,
			sql:  `SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY x.singleton_id)::text, '[]') FROM (SELECT singleton_id, election_closed, administrator_user_id FROM server_administration) x`,
		},
	}
	for _, query := range queries {
		if err := store.StorePool(s).QueryRow(ctx, query.sql).Scan(query.out); err != nil {
			t.Fatalf("snapshot %s: %v", query.name, err)
		}
	}
	return fmt.Sprintf("users=%s usernames=%s passwords=%s administration=%s", users, usernames, passwords, administration)
}

func seedOperator(t *testing.T, s *store.Store) int64 {
	t.Helper()
	ctx := context.Background()
	operator, err := s.CreateUsernameUser(ctx, "operator", "Operator", "")
	if err != nil {
		t.Fatalf("create operator fixture: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx, `
		INSERT INTO usernames (handle, owner_type, owner_id)
		VALUES ('operator', 'user', $1)
	`, operator.ID); err != nil {
		t.Fatalf("claim operator fixture: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx,
		`UPDATE users SET username = 'operator' WHERE id = $1`, operator.ID); err != nil {
		t.Fatalf("set operator fixture username: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   operator.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: []byte("readable verifier"),
	}); err != nil {
		t.Fatalf("set operator fixture verifier: %v", err)
	}
	if _, err := store.StorePool(s).Exec(ctx, `
		UPDATE server_administration
		SET election_closed = TRUE, administrator_user_id = NULL
		WHERE singleton_id = 1
	`); err != nil {
		t.Fatalf("close unassigned election fixture: %v", err)
	}
	return operator.ID
}
