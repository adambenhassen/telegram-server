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
