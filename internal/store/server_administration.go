package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrServerAdministrationInvalid is returned when the durable singleton is
// missing, duplicated, or contains an invalid state. Account creation must
// fail closed rather than infer or repair authority.
var ErrServerAdministrationInvalid = errors.New("server administration singleton invalid")

// ErrOperatorAdministrationInvalid is returned when the one-shot operator
// promotion preflight does not find the exact deployed account and election
// state it is allowed to change.
var ErrOperatorAdministrationInvalid = errors.New("operator server administration preflight invalid")

const operatorAccountHandle = "operator"

// IsServerAdministrator reports whether userID is the persisted server
// administrator. It is the one server-side authority decision future
// privileged MTProto methods should use after the request's auth-key binding
// and provisional-session gates have passed.
func (s *Store) IsServerAdministrator(ctx context.Context, userID int64) (bool, error) {
	if userID <= 0 {
		return false, nil
	}
	administrator, err := s.q.IsServerAdministrator(ctx, &userID)
	if err != nil {
		return false, fmt.Errorf("check server administrator: %w", err)
	}
	return administrator, nil
}

// AssignOperatorServerAdministrator promotes the already-deployed operator
// account to the durable server administrator. It is deliberately a
// parameterless, local-maintenance operation: the account, username, and
// election state are all verified from the database before the sole authority
// field is changed.
func (s *Store) AssignOperatorServerAdministrator(ctx context.Context) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin operator administration: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) && err == nil {
			err = fmt.Errorf("rollback operator administration: %w", rollbackErr)
		}
	}()

	// Account insertion takes ROW EXCLUSIVE before it locks the election row.
	// Take the compatible users-first lock so this transaction and the existing
	// insert-then-singleton order cannot deadlock or cross the transition.
	if _, err := tx.Exec(ctx, "LOCK TABLE users IN SHARE MODE"); err != nil {
		return fmt.Errorf("lock users relation: %w", err)
	}

	qtx := s.q.WithTx(tx)
	administration, err := qtx.LockServerAdministration(ctx)
	if err != nil {
		return fmt.Errorf("lock server administration: %w", err)
	}
	if len(administration) != 1 || administration[0].SingletonID != 1 || !administration[0].ElectionClosed {
		return fmt.Errorf("%w: singleton is not exactly one closed row", ErrOperatorAdministrationInvalid)
	}
	assignedID := administration[0].AdministratorUserID

	operatorID, err := lockOperatorAccount(ctx, tx)
	if err != nil {
		return err
	}
	if err := lockOperatorUsername(ctx, tx, operatorID); err != nil {
		return err
	}
	if err := s.lockReadableOperatorVerifier(ctx, tx, operatorID); err != nil {
		return err
	}

	if assignedID != nil {
		if *assignedID != operatorID {
			return fmt.Errorf("%w: administrator is assigned to user %d", ErrOperatorAdministrationInvalid, *assignedID)
		}
		return nil
	}

	tag, err := tx.Exec(ctx, `
		UPDATE server_administration
		SET administrator_user_id = $1
		WHERE singleton_id = 1
		  AND election_closed = TRUE
		  AND administrator_user_id IS NULL
	`, operatorID)
	if err != nil {
		return fmt.Errorf("assign operator server administrator: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: assignment changed %d rows", ErrOperatorAdministrationInvalid, tag.RowsAffected())
	}

	assigned, err := qtx.LockServerAdministration(ctx)
	if err != nil {
		return fmt.Errorf("re-read server administration: %w", err)
	}
	if len(assigned) != 1 || assigned[0].SingletonID != 1 || !assigned[0].ElectionClosed || assigned[0].AdministratorUserID == nil || *assigned[0].AdministratorUserID != operatorID {
		return fmt.Errorf("%w: assignment invariant failed", ErrOperatorAdministrationInvalid)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit operator server administrator: %w", err)
	}
	return nil
}

func lockOperatorAccount(ctx context.Context, tx pgx.Tx) (int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, phone, username, login_mode
		FROM users
		ORDER BY id
		FOR UPDATE
	`)
	if err != nil {
		return 0, fmt.Errorf("lock users: %w", err)
	}
	defer rows.Close()

	var (
		operatorID     int64
		phone          *string
		username       *string
		loginMode      string
		userCount      int
		accountReadErr error
	)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id, &phone, &username, &loginMode); err != nil {
			accountReadErr = err
			break
		}
		userCount++
		operatorID = id
		if userCount != 1 || phone != nil || username == nil || *username != operatorAccountHandle || loginMode != "username" {
			return 0, fmt.Errorf("%w: users does not contain exactly the canonical operator account", ErrOperatorAdministrationInvalid)
		}
	}
	if accountReadErr != nil {
		return 0, fmt.Errorf("scan operator account: %w", accountReadErr)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read operator account: %w", err)
	}
	if userCount != 1 {
		return 0, fmt.Errorf("%w: users count = %d, want 1", ErrOperatorAdministrationInvalid, userCount)
	}
	return operatorID, nil
}

func lockOperatorUsername(ctx context.Context, tx pgx.Tx, operatorID int64) error {
	rows, err := tx.Query(ctx, `
		SELECT handle, owner_type, owner_id
		FROM usernames
		WHERE lower(handle) = $1
		ORDER BY handle, owner_type, owner_id
		FOR UPDATE
	`, operatorAccountHandle)
	if err != nil {
		return fmt.Errorf("lock operator username: %w", err)
	}
	defer rows.Close()

	var (
		count     int
		handle    string
		ownerType string
		ownerID   int64
	)
	for rows.Next() {
		if err := rows.Scan(&handle, &ownerType, &ownerID); err != nil {
			return fmt.Errorf("scan operator username: %w", err)
		}
		count++
		if count != 1 || handle != operatorAccountHandle || ownerType != "user" || ownerID != operatorID {
			return fmt.Errorf("%w: operator username is not an exact user-owned row", ErrOperatorAdministrationInvalid)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read operator username: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("%w: operator username rows = %d, want 1", ErrOperatorAdministrationInvalid, count)
	}
	return nil
}

func (s *Store) lockReadableOperatorVerifier(ctx context.Context, tx pgx.Tx, operatorID int64) error {
	rows, err := tx.Query(ctx, `
		SELECT user_id, verifier
		FROM user_passwords
		WHERE user_id = $1
		ORDER BY user_id
		FOR UPDATE
	`, operatorID)
	if err != nil {
		return fmt.Errorf("lock operator verifier: %w", err)
	}
	defer rows.Close()

	var (
		count     int
		userID    int64
		encrypted []byte
	)
	for rows.Next() {
		if err := rows.Scan(&userID, &encrypted); err != nil {
			return fmt.Errorf("scan operator verifier: %w", err)
		}
		count++
		if count != 1 || userID != operatorID {
			return fmt.Errorf("%w: operator verifier row is not exact", ErrOperatorAdministrationInvalid)
		}
		verifier, err := s.cipher.Open(encrypted)
		if err != nil {
			return fmt.Errorf("%w: operator verifier is unreadable", ErrOperatorAdministrationInvalid)
		}
		if len(verifier) == 0 {
			clear(verifier)
			return fmt.Errorf("%w: operator verifier is empty", ErrOperatorAdministrationInvalid)
		}
		clear(verifier)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read operator verifier: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("%w: operator verifier rows = %d, want 1", ErrOperatorAdministrationInvalid, count)
	}
	return nil
}

// electServerAdministrator validates and transitions the singleton inside the
// caller's account-creation transaction. The row lock makes the integrity
// check and the conditional update one database-owned decision while still
// allowing a transaction that rolls back to leave the election open.
func (s *Store) electServerAdministrator(ctx context.Context, qtx *db.Queries, userID int64) error {
	rows, err := qtx.LockServerAdministration(ctx)
	if err != nil {
		return fmt.Errorf("lock server administration: %w", err)
	}
	if len(rows) != 1 {
		return ErrServerAdministrationInvalid
	}
	row := rows[0]
	if row.SingletonID != 1 || (!row.ElectionClosed && row.AdministratorUserID != nil) {
		return ErrServerAdministrationInvalid
	}
	if row.ElectionClosed {
		return nil
	}

	elected, err := qtx.ElectServerAdministrator(ctx, &userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrServerAdministrationInvalid
	}
	if err != nil {
		return fmt.Errorf("elect server administrator: %w", err)
	}
	if elected.AdministratorUserID == nil || *elected.AdministratorUserID != userID || !elected.ElectionClosed {
		return ErrServerAdministrationInvalid
	}
	return nil
}
