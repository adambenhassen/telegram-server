package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// AdmitUsername consumes the invite secret stored in the username's phone-code
// row and creates the provisional account in the same transaction. Every
// account-side write is rolled back when any later step fails, including the
// invite consumption.
func (s *Store) AdmitUsername(ctx context.Context, handle, phoneCodeHash string, authKeyID int64, firstName, lastName string) (User, error) {
	handle = strings.ToLower(handle)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, fmt.Errorf("admit username: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	codeRow, err := qtx.GetCodeByHashAndPhone(ctx, db.GetCodeByHashAndPhoneParams{
		CodeHash: phoneCodeHash,
		Phone:    handle,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, ErrCodeInvalid
	case err != nil:
		return User{}, fmt.Errorf("admit username: check code hash: %w", err)
	}
	if err := validateCodeHash(codeRow); err != nil {
		return User{}, err
	}

	if err := s.ConsumeInvite(ctx, tx, handle, codeRow.Code); err != nil {
		return User{}, err
	}

	u, err := qtx.CreateUsernameUser(ctx, db.CreateUsernameUserParams{
		FirstName: firstName,
		LastName:  lastName,
	})
	if err != nil {
		return User{}, fmt.Errorf("admit username: create user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("admit username: ensure update state: %w", err)
	}

	_, err = qtx.ClaimUsername(ctx, db.ClaimUsernameParams{
		Handle:    handle,
		OwnerType: "user",
		OwnerID:   u.ID,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return User{}, ErrUsernameOccupied
		}
		return User{}, fmt.Errorf("admit username: claim username: %w", err)
	}
	if _, err := qtx.SetUsername(ctx, db.SetUsernameParams{ID: u.ID, Username: &handle}); err != nil {
		return User{}, fmt.Errorf("admit username: set username: %w", err)
	}

	if _, err := qtx.LockUnboundAuthKey(ctx, authKeyID); errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrAuthKeyNotFound
	} else if err != nil {
		return User{}, fmt.Errorf("admit username: lock auth key: %w", err)
	}
	rows, err := qtx.BindAuthKeyUser(ctx, db.BindAuthKeyUserParams{ID: authKeyID, UserID: &u.ID})
	if err != nil {
		return User{}, fmt.Errorf("admit username: bind auth key: %w", err)
	}
	if rows == 0 {
		return User{}, ErrAuthKeyNotFound
	}

	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("admit username: commit: %w", err)
	}
	return UserFromCreateUsernameUser(u), nil
}
