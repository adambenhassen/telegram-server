package store

import (
	"context"
	"fmt"
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

// ChatAdminEventRecipientsByEvent lists the event's original recipients who
// are still members of the chat, for best-effort live delivery.
func (s *Store) ChatAdminEventRecipientsByEvent(ctx context.Context, eventID int64) ([]int64, error) {
	rows, err := s.q.ChatAdminEventRecipientsByEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("chat admin event recipients: %w", err)
	}
	return rows, nil
}

// ChatAdminSnapshotsForMember returns durable current role state for every
// participant whose admin state the member was entitled to observe. It is
// intentionally independent of pts so getDifference can repair a missed push.
func (s *Store) ChatAdminSnapshotsForMember(ctx context.Context, ownerID int64) ([]ChatAdminSnapshot, error) {
	rows, err := s.q.ChatAdminSnapshotsForMember(ctx, ownerID)
	if err != nil {
		return nil, fmt.Errorf("chat admin snapshots for member: %w", err)
	}
	snapshots := make([]ChatAdminSnapshot, len(rows))
	for i, row := range rows {
		snapshots[i] = ChatAdminSnapshot{
			ChatID:  row.ChatID,
			UserID:  row.UserID,
			IsAdmin: row.IsAdmin,
			Version: int(row.Version),
		}
	}
	return snapshots, nil
}
