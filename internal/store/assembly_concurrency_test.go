package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestAssemblyConcurrencyLeavesPoolHeadroom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t) + "&pool_max_conns=4"
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := s.AssemblyConcurrencyLimit(); got != 1 {
		t.Fatalf("assembly concurrency limit = %d, want 1 from pool_max_conns=4 and headroom=%d", got, store.AssemblyConnectionHeadroom)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	assemblyUser, err := s.CreateUser(ctx, "+15559100301")
	if err != nil {
		t.Fatalf("assembly user: %v", err)
	}
	secondAssemblyUser, err := s.CreateUser(ctx, "+15559100302")
	if err != nil {
		t.Fatalf("second assembly user: %v", err)
	}
	sender, err := s.CreateUser(ctx, "+15559100303")
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	recipient, err := s.CreateUser(ctx, "+15559100304")
	if err != nil {
		t.Fatalf("recipient: %v", err)
	}

	// Prepare one live stored file for the download path and one auth key before
	// the assembly connection is held.
	file := allocate(t, s, sender.ID, 1)
	if err := s.MarkFileStored(ctx, file.ID); err != nil {
		t.Fatalf("mark fixture stored: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, sender.ID, recipient.ID, "fixture", 7001, file.ID, 0); err != nil {
		t.Fatalf("send fixture: %v", err)
	}
	const authKeyID = int64(7002)
	if err := s.SaveAuthKey(ctx, authKeyID, []byte("fixture auth key")); err != nil {
		t.Fatalf("save auth key: %v", err)
	}

	const largeAssemblySize = int64(10 << 20)
	firstReady := make(chan struct{})
	firstRelease := make(chan struct{})
	secondReady := make(chan struct{})
	secondRelease := make(chan struct{})
	var firstOnce, secondOnce sync.Once

	firstDone := make(chan error, 1)
	go func() {
		_, err := s.AllocateAndCompleteFile(ctx, assemblyUser.ID, largeAssemblySize, "application/octet-stream", "large-1.bin", bigQuota, func(store.File) error {
			firstOnce.Do(func() { close(firstReady) })
			<-firstRelease
			return nil
		})
		firstDone <- err
	}()
	select {
	case <-firstReady:
	case <-time.After(2 * time.Second):
		t.Fatal("first assembly did not reach its held Put")
	}

	secondDone := make(chan error, 1)
	secondCtx, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()
	go func() {
		_, err := s.AllocateAndCompleteFile(secondCtx, secondAssemblyUser.ID, largeAssemblySize, "application/octet-stream", "large-2.bin", bigQuota, func(store.File) error {
			secondOnce.Do(func() { close(secondReady) })
			<-secondRelease
			return nil
		})
		secondDone <- err
	}()

	// With MaxConns=4, one connection is reserved for each of the three
	// ordinary paths below. A second assembly must therefore wait before it
	// acquires a session connection or reaches Put.
	unbounded := false
	select {
	case <-secondReady:
		unbounded = true
	case <-time.After(2 * time.Second):
	}
	if unbounded {
		close(firstRelease)
		close(secondRelease)
		if err := <-firstDone; err != nil {
			t.Errorf("first assembly cleanup: %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Errorf("second assembly cleanup: %v", err)
		}
		t.Fatal("second assembly reached Put while the one allowed assembly slot was held")
	}

	operationCtx, cancelOperations := context.WithTimeout(ctx, 3*time.Second)
	defer cancelOperations()
	results := make(chan error, 3)
	go func() {
		_, _, _, _, err := s.SendMessage(operationCtx, sender.ID, recipient.ID, "headroom send", 7003, 0, 0)
		results <- err
	}()
	go func() {
		_, err := s.FileForDownload(operationCtx, file.ID, file.AccessHash, recipient.ID)
		results <- err
	}()
	go func() {
		_, _, err := s.AuthKeyByID(operationCtx, authKeyID)
		results <- err
	}()

	var operationErr error
	for range 3 {
		select {
		case err := <-results:
			if err != nil && operationErr == nil {
				operationErr = err
			}
		case <-operationCtx.Done():
			operationErr = operationCtx.Err()
		}
	}

	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Errorf("first assembly: %v", err)
	}
	select {
	case <-secondReady:
	case <-time.After(2 * time.Second):
		t.Error("second assembly did not acquire the released slot")
	}
	close(secondRelease)
	if err := <-secondDone; err != nil {
		t.Errorf("second assembly: %v", err)
	}
	if operationErr != nil {
		t.Fatalf("ordinary path while assembly slot held: %v", operationErr)
	}
}

func TestOpenRefusesPoolWithoutAssemblyHeadroom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t) + fmt.Sprintf("&pool_max_conns=%d", store.AssemblyConnectionHeadroom)
	_, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err == nil {
		t.Fatal("Open succeeded without the reserved assembly headroom")
	}
	if got := err.Error(); got != "store: pool max connections 3 must exceed assembly headroom 3" {
		t.Fatalf("Open error = %q, want explicit headroom failure", got)
	}
}
