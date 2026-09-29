package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrInvalidContact is returned when a contact edge has a non-positive id or
// points from a user to themself.
var ErrInvalidContact = errors.New("invalid contact")

// ErrContactLimit is returned when an owner already has MaxContactsPerUser
// contacts and tries to add a new one.
var ErrContactLimit = errors.New("contact limit reached")

// MaxContactsPerUser is the maximum number of directed contacts one account
// may store.
const MaxContactsPerUser = 5000

// Contact is one owner-scoped contact edge. Mutual is derived from the
// recipient's reverse edge when the list is read; it is never stored.
type Contact struct {
	UserID int64
	Mutual bool
}

func validContactIDs(ownerID, contactID int64) bool {
	return ownerID > 0 && contactID > 0 && ownerID != contactID
}

// AddContact adds contactID to ownerID's directed contact list. The existing
// owner advisory lock serializes list-cap checks and mutations for that owner.
func (s *Store) AddContact(ctx context.Context, ownerID, contactID int64) (changed bool, err error) {
	if !validContactIDs(ownerID, contactID) {
		return false, ErrInvalidContact
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err := lockOwners(ctx, tx, ownerID); err != nil {
		return false, err
	}
	qtx := s.q.WithTx(tx)
	exists, err := qtx.ContactExists(ctx, db.ContactExistsParams{
		OwnerID:   ownerID,
		ContactID: contactID,
	})
	if err != nil {
		return false, fmt.Errorf("check contact: %w", err)
	}
	if exists {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit: %w", err)
		}
		return false, nil
	}
	count, err := qtx.CountContacts(ctx, ownerID)
	if err != nil {
		return false, fmt.Errorf("count contacts: %w", err)
	}
	if count >= MaxContactsPerUser {
		return false, ErrContactLimit
	}
	n, err := qtx.InsertContact(ctx, db.InsertContactParams{
		OwnerID:   ownerID,
		ContactID: contactID,
	})
	if err != nil {
		return false, fmt.Errorf("insert contact: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return n > 0, nil
}

// RemoveContact deletes only ownerID's directed edge to contactID. An absent
// edge is a successful no-op.
func (s *Store) RemoveContact(ctx context.Context, ownerID, contactID int64) (changed bool, err error) {
	if !validContactIDs(ownerID, contactID) {
		return false, ErrInvalidContact
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err := lockOwners(ctx, tx, ownerID); err != nil {
		return false, err
	}
	n, err := s.q.WithTx(tx).DeleteContact(ctx, db.DeleteContactParams{
		OwnerID:   ownerID,
		ContactID: contactID,
	})
	if err != nil {
		return false, fmt.Errorf("delete contact: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return n > 0, nil
}

// RemoveContacts deletes an owner's selected contact edges in one transaction.
// The entire id set is validated before the transaction can mutate anything.
func (s *Store) RemoveContacts(ctx context.Context, ownerID int64, contactIDs []int64) (changed bool, err error) {
	if ownerID <= 0 {
		return false, ErrInvalidContact
	}
	for _, contactID := range contactIDs {
		if !validContactIDs(ownerID, contactID) {
			return false, ErrInvalidContact
		}
	}
	if len(contactIDs) == 0 {
		return false, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err := lockOwners(ctx, tx, ownerID); err != nil {
		return false, err
	}
	n, err := s.q.WithTx(tx).DeleteContacts(ctx, db.DeleteContactsParams{
		OwnerID:    ownerID,
		ContactIds: contactIDs,
	})
	if err != nil {
		return false, fmt.Errorf("delete contacts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return n > 0, nil
}

// IsContact reports whether ownerID has a directed edge to contactID.
func (s *Store) IsContact(ctx context.Context, ownerID, contactID int64) (bool, error) {
	if !validContactIDs(ownerID, contactID) {
		return false, ErrInvalidContact
	}
	exists, err := s.q.ContactExists(ctx, db.ContactExistsParams{
		OwnerID:   ownerID,
		ContactID: contactID,
	})
	if err != nil {
		return false, fmt.Errorf("check contact: %w", err)
	}
	return exists, nil
}

// Contacts returns an owner's contacts in contact-id order and the same
// statement's total. Mutual state is derived from each contact's reverse edge.
func (s *Store) Contacts(ctx context.Context, ownerID int64) ([]Contact, int, error) {
	rows, err := s.q.ListContacts(ctx, ownerID)
	if err != nil {
		return nil, 0, fmt.Errorf("list contacts: %w", err)
	}
	contacts := make([]Contact, len(rows))
	var total int
	for i, row := range rows {
		contacts[i] = Contact{UserID: row.ContactID, Mutual: row.Mutual}
		if i == 0 {
			total = int(row.Total)
		}
	}
	return contacts, total, nil
}
