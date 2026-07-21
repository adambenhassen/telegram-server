package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// EventType classifies a persisted update in the per-owner event log. Values
// match the message_events.type column.
type EventType int

const (
	EventNewMessage EventType = 1
	EventEdit       EventType = 2
	EventDelete     EventType = 3
	EventReadIn     EventType = 4
	EventReadOut    EventType = 5
)

// State is a user's current update sequence, mirroring updates.State on the
// wire. Date is unix seconds; UnreadCount is summed across the user's dialogs.
type State struct {
	Pts         int
	Seq         int
	Date        int
	UnreadCount int
}

// Event is one entry from the per-owner ordered event log.
type Event struct {
	Pts     int
	Type    EventType
	LocalID int64
}

// EnsureUpdateState creates the user's update_state row if absent (idempotent).
func (s *Store) EnsureUpdateState(ctx context.Context, userID int64) error {
	if err := s.q.EnsureUpdateState(ctx, userID); err != nil {
		return fmt.Errorf("ensure update state: %w", err)
	}
	return nil
}

// State returns the user's current pts/seq/date and total unread count. A user
// with no update_state row yet (fresh account that never participated in a send)
// reports the zero state (pts 0), so getState/getDifference work immediately.
func (s *Store) State(ctx context.Context, userID int64) (State, error) {
	row, err := s.q.GetState(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("get state: %w", err)
	}
	var unread int
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(unread_count), 0) FROM dialogs WHERE owner_id = $1`,
		userID,
	).Scan(&unread); err != nil {
		return State{}, fmt.Errorf("sum unread: %w", err)
	}
	return State{
		Pts:         int(row.Pts),
		Seq:         int(row.Seq),
		Date:        int(row.Date.Time.Unix()),
		UnreadCount: unread,
	}, nil
}

// EventsSince returns the user's events with pts strictly greater than fromPts,
// ordered ascending by pts. It is the raw input to update hydration
// (getDifference and real-time push).
func (s *Store) EventsSince(ctx context.Context, userID int64, fromPts int) ([]Event, error) {
	rows, err := s.q.EventsSince(ctx, db.EventsSinceParams{OwnerID: userID, Pts: int64(fromPts)})
	if err != nil {
		return nil, fmt.Errorf("events since: %w", err)
	}
	events := make([]Event, len(rows))
	for i, r := range rows {
		events[i] = Event{Pts: int(r.Pts), Type: EventType(r.Type), LocalID: r.LocalID}
	}
	return events, nil
}

// EventsWindow returns the user's events in (fromPts, toPts] ordered ascending,
// at most limit of them. Bounding by toPts (a previously-read state pts) keeps
// the difference from advertising a pts past an event it did not return.
func (s *Store) EventsWindow(ctx context.Context, userID int64, fromPts, toPts, limit int) ([]Event, error) {
	rows, err := s.q.EventsWindow(ctx, db.EventsWindowParams{
		OwnerID: userID,
		FromPts: int64(fromPts),
		ToPts:   int64(toPts),
		Lim:     int32(limit), //nolint:gosec // limit is a small server-set cap
	})
	if err != nil {
		return nil, fmt.Errorf("events window: %w", err)
	}
	events := make([]Event, len(rows))
	for i, r := range rows {
		events[i] = Event{Pts: int(r.Pts), Type: EventType(r.Type), LocalID: r.LocalID}
	}
	return events, nil
}
