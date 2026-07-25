package store

import (
	"context"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Reconnect pacing, exported so the flapping and idle-recovery cases can be
// asserted on the rule itself instead of on a 30-second wall clock.
func NextBackoff(prev, uptime time.Duration) time.Duration { return nextBackoff(prev, uptime) }

const (
	ListenerBackoffMin = listenerBackoffMin
	ListenerBackoffMax = listenerBackoffMax
	ListenerStableFor  = listenerStableFor
)

// InsertTestEvent bumps the owner's pts and appends one event at the new pts,
// letting update-state tests exercise EventsSince without a full message send.
func InsertTestEvent(ctx context.Context, s *Store, ownerID int64, typ EventType, localID int64) error {
	pts, err := s.q.BumpPtsOnly(ctx, ownerID)
	if err != nil {
		return err
	}
	return s.q.InsertEvent(ctx, db.InsertEventParams{
		OwnerID: ownerID,
		Pts:     pts,
		Type:    int16(typ), //nolint:gosec // event types are small constants (1-5)
		LocalID: localID,
	})
}
