package blobscan_test

import (
	"context"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/blobscan"
)

type storeWithoutTree struct{ blob.Store }

func TestScanStoreUsesConfiguredBackendEnumeration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	gone := f.stored(payload)
	f.dropRow(gone.ID)
	rep, err := blobscan.ScanStore(context.Background(), f.blobs, f.store, past())
	if err != nil {
		t.Fatalf("scan store: %v", err)
	}
	if !has(rep.Orphans, blob.Key(gone.ID)) {
		t.Fatalf("configured backend enumeration missed orphan %s: %+v", blob.Key(gone.ID), rep)
	}
}

func TestScanStoreRejectsBackendWithoutFullEnumeration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	_, err := blobscan.ScanStore(context.Background(), storeWithoutTree{Store: f.blobs}, f.store, past())
	if err == nil {
		t.Fatal("scan store accepted a backend without full-tree enumeration")
	}
	if !strings.Contains(err.Error(), "full-tree enumeration") {
		t.Fatalf("scan store error = %q, want enumeration diagnostic", err)
	}
}
