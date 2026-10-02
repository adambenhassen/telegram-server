package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

func TestNotifyHasFiniteBudgetWhenPoolIsSaturated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	s, err := Open(context.Background(), dsn+separator+"pool_max_conns=2", pgtest.EncKey(), WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	connections := make([]*pgxpool.Conn, 0, 2)
	for range 2 {
		conn, err := s.pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("saturate store pool: %v", err)
		}
		connections = append(connections, conn)
	}
	t.Cleanup(func() {
		for _, conn := range connections {
			conn.Release()
		}
	})

	callerCtx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	started := time.Now()
	err = s.Notify(callerCtx, ChannelUpdates, "7")
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("notify error = %v, want a bounded deadline while the pool is saturated", err)
	}
	if elapsed > 6*time.Second {
		t.Fatalf("notify waited %s with a 5s budget, want it to finish within 6s", elapsed)
	}
}

func TestNotifyFanoutSharesOneBudgetWhenPoolIsSaturated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	s, err := Open(context.Background(), dsn+separator+"pool_max_conns=2", pgtest.EncKey(), WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	connections := make([]*pgxpool.Conn, 0, 2)
	for range 2 {
		conn, err := s.pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("saturate store pool: %v", err)
		}
		connections = append(connections, conn)
	}
	t.Cleanup(func() {
		for _, conn := range connections {
			conn.Release()
		}
	})

	rpcCtx, cancelRPC := context.WithCancel(context.Background())
	cancelRPC()
	notifyCtx, cancelNotify := context.WithTimeout(context.WithoutCancel(rpcCtx), 5*time.Second)
	defer cancelNotify()

	// Channel-invite notifications use this payload and share this context
	// across the target loop.
	payloads := []string{
		ChannelMembershipPayload(7, 42),
		ChannelMembershipPayload(8, 42),
		ChannelMembershipPayload(9, 42),
	}
	started := time.Now()
	for _, payload := range payloads {
		if err := s.Notify(notifyCtx, ChannelUpdates, payload); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("notify %s error = %v, want the shared fan-out deadline", payload, err)
		}
	}
	if elapsed := time.Since(started); elapsed > 6*time.Second {
		t.Fatalf("three-member fan-out waited %s with one 5s budget, want it to finish within 6s", elapsed)
	}
}
