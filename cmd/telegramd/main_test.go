package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type commandRemoveProbe struct {
	blob.Store

	removed chan struct{}
	once    sync.Once
}

func (p *commandRemoveProbe) Remove(ctx context.Context, key string) error {
	p.once.Do(func() { close(p.removed) })
	return p.Store.Remove(ctx, key)
}

// The destructive off-switch is decided in cmd/telegramd, not in the Store.
// Calling the command-layer pass directly proves that an aged assembled temp
// remains untouched when the deployment is in its default reporting mode.
func TestMediaErasureDestructiveOffSwitchAtCommandBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	probe := &commandRemoveProbe{Store: l, removed: make(chan struct{})}
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(probe))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	u, err := s.CreateUser(ctx, "+15559301001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	f, err := s.AllocateFile(ctx, u.ID, 1, "application/octet-stream", "upload.bin", 1<<20)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	key := blob.Key(f.ID) + blob.TempSuffix
	if _, err := l.Put(ctx, key, bytes.NewReader([]byte("pending"))); err != nil {
		t.Fatalf("put temp: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(l.RootDir(), filepath.FromSlash(key)), old, old); err != nil {
		t.Fatalf("age temp: %v", err)
	}

	cfg := config.Config{
		MediaErasureDestructive: false,
		MediaErasureMinAge:      time.Hour,
		BlobScanTempMinAge:      time.Nanosecond,
	}
	log := slog.New(slog.DiscardHandler)
	sweepMediaErasurePass(ctx, s, cfg, log)

	select {
	case <-probe.removed:
		t.Fatal("reporting mode reached the blob backend's Remove")
	default:
	}
	if _, err := l.ReadAt(ctx, key, 0, 1); err != nil {
		t.Fatalf("reporting mode removed aged temp: %v", err)
	}
}
