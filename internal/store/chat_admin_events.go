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
