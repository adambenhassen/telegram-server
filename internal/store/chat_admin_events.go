package store

import (
	"context"
	"fmt"

	"github.com/teagramhq/teagram-server/internal/store/db"
)

type ChatAdminEvent struct {
	ID      int64
	ChatID  int64
	UserID  int64
	IsAdmin bool
	Version int
}

// ChatAdminSnapshot is the current role and chat version for a participant
// whose admin state the requesting member was entitled to observe.
type ChatAdminSnapshot struct {
	EventID int64
	ChatID  int64
	UserID  int64
	IsAdmin bool
	Version int
}

func (s *Store) ChatAdminEventsByIDs(ctx context.Context, ids []int64) ([]ChatAdminEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.ChatAdminEventsByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("chat admin events by ids: %w", err)
	}
	events := make([]ChatAdminEvent, len(rows))
	for i, row := range rows {
		events[i] = ChatAdminEvent{
			ID:      row.ID,
			ChatID:  row.ChatID,
			UserID:  row.UserID,
			IsAdmin: row.IsAdmin,
			Version: int(row.Version),
		}
	}
	return events, nil
}

// ChatAdminEventRecipientsByEvent lists the currently pending recipients for
// an event who are still members, for best-effort live delivery.
func (s *Store) ChatAdminEventRecipientsByEvent(ctx context.Context, eventID int64) ([]int64, error) {
	rows, err := s.q.ChatAdminEventRecipientsByEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("chat admin event recipients: %w", err)
	}
	return rows, nil
}

// ChatAdminSnapshotsForMember returns pending durable current role state for
// participants whose admin state the member may observe. It is independent of
// pts so getDifference can repair a missed push.
func (s *Store) ChatAdminSnapshotsForMember(ctx context.Context, ownerID int64) ([]ChatAdminSnapshot, error) {
	rows, err := s.q.ChatAdminSnapshotsForMember(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("chat admin snapshots for member: %w", err)
	}
	snapshots := make([]ChatAdminSnapshot, len(rows))
	for i, row := range rows {
		snapshots[i] = ChatAdminSnapshot{
			EventID: row.EventID,
			ChatID:  row.ChatID,
			UserID:  row.UserID,
			IsAdmin: row.IsAdmin,
			Version: int(row.Version),
		}
	}
	return snapshots, nil
}

// DeleteChatAdminStateMarkersByEventIDs consumes the exact role snapshots
// delivered to ownerID. A newer role change has a different event ID and is
// therefore left pending.
func (s *Store) DeleteChatAdminStateMarkersByEventIDs(ctx context.Context, ownerID int64, eventIDs []int64) error {
	if len(eventIDs) == 0 {
		return nil
	}
	if err := s.q.DeleteChatAdminStateMarkersByEventIDs(ctx, db.DeleteChatAdminStateMarkersByEventIDsParams{
		OwnerID:  ownerID,
		EventIds: eventIDs,
	}); err != nil {
		return fmt.Errorf("delete chat admin state markers: %w", err)
	}
	return nil
}
