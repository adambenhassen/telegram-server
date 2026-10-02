package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func openDialogFilterStore(t *testing.T, dsn string) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open dialog filter store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() }) //nolint:errcheck // test cleanup
	return s
}

func TestDialogFilterQuotaSerializesConcurrentCreates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091001")
	const attempts = 11
	errList := make([]error, attempts)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errList[i] = s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{
				ID:     i + 2,
				Title:  fmt.Sprintf("Folder %d", i),
				Groups: true,
			})
		}(i)
	}
	wg.Wait()
	succeeded, limited := 0, 0
	for _, err := range errList {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, store.ErrDialogFilterLimit):
			limited++
		default:
			t.Fatalf("create folder error: %v", err)
		}
	}
	if succeeded != 10 || limited != 1 {
		t.Fatalf("concurrent creates = %d succeeded, %d limited, want 10/1", succeeded, limited)
	}
	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read folders: %v", err)
	}
	if len(snapshot.Filters) != 10 || snapshot.ChangedAt == nil {
		t.Fatalf("folder count = %d, change marker = %v, want 10 and marker", len(snapshot.Filters), snapshot.ChangedAt)
	}
}

func TestDialogFiltersSeedUnreadOnceAndKeepDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091006")

	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("first get dialog filters: %v", err)
	}
	if len(snapshot.Filters) != 1 {
		t.Fatalf("first get returned %d folders, want Unread", len(snapshot.Filters))
	}
	unread := snapshot.Filters[0]
	if unread.ID != 2 || unread.Title != "Unread" || !unread.Contacts || !unread.NonContacts || !unread.Groups || !unread.Broadcasts || !unread.Bots || !unread.ExcludeRead {
		t.Fatalf("first folder = %+v, want the all-types Unread folder", unread)
	}
	if len(snapshot.Order) != 2 || snapshot.Order[0] != 0 || snapshot.Order[1] != 2 {
		t.Fatalf("first folder order = %v, want All chats then Unread", snapshot.Order)
	}

	if _, err := s.DeleteDialogFilter(ctx, owner.ID, 2); err != nil {
		t.Fatalf("delete Unread: %v", err)
	}
	snapshot, err = s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("get dialog filters after deleting Unread: %v", err)
	}
	if len(snapshot.Filters) != 0 || len(snapshot.Order) != 1 || snapshot.Order[0] != 0 {
		t.Fatalf("folders after deleting Unread = %#v, order %v; want only All chats", snapshot.Filters, snapshot.Order)
	}
}

func TestConcurrentFirstDialogFilterReadsSeedOneUnreadFolder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091007")
	const attempts = 8
	snapshots := make([]store.DialogFilterSnapshot, attempts)
	errList := make([]error, attempts)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			snapshots[i], errList[i] = s.DialogFilters(ctx, owner.ID)
		}(i)
	}
	wg.Wait()
	for i, err := range errList {
		if err != nil {
			t.Fatalf("concurrent get %d: %v", i, err)
		}
		if len(snapshots[i].Filters) != 1 || snapshots[i].Filters[0].ID != 2 || snapshots[i].Filters[0].Title != "Unread" {
			t.Fatalf("concurrent get %d returned %#v, want one Unread folder", i, snapshots[i].Filters)
		}
	}
	final, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("final get dialog filters: %v", err)
	}
	if len(final.Filters) != 1 || final.Filters[0].Title != "Unread" {
		t.Fatalf("persisted folders = %#v, want exactly one Unread folder", final.Filters)
	}
}

func TestDialogFilterMarkerSurvivesLastDeleteAndRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner := mustUser(t, s, "+15551091002")
	if _, err := s.DialogFilters(ctx, owner.ID); err != nil {
		t.Fatalf("seed defaults before testing last-folder delete: %v", err)
	}
	if err := s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{ID: 2, Title: "Only", Groups: true}); err != nil {
		t.Fatalf("save folder: %v", err)
	}
	if _, err := s.DeleteDialogFilter(ctx, owner.ID, 2); err != nil {
		t.Fatalf("delete last folder: %v", err)
	}
	marker, found, err := s.DialogFilterChangeAt(ctx, owner.ID)
	if err != nil || !found {
		t.Fatalf("read last-delete marker: found %v, err %v", found, err)
	}
	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after last delete: %v", err)
	}
	if len(snapshot.Filters) != 0 || len(snapshot.Order) != 1 || snapshot.Order[0] != 0 || snapshot.ChangedAt == nil {
		t.Fatalf("last-delete snapshot = filters %#v, order %#v, marker %v", snapshot.Filters, snapshot.Order, snapshot.ChangedAt)
	}
	if _, err := s.DeleteDialogFilter(ctx, owner.ID, 2); err != nil {
		t.Fatalf("repeat absent delete: %v", err)
	}
	afterAbsent, found, err := s.DialogFilterChangeAt(ctx, owner.ID)
	if err != nil || !found || !afterAbsent.Equal(marker) {
		t.Fatalf("absent delete changed marker from %s to %s (found %v, err %v)", marker, afterAbsent, found, err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	reopened := openDialogFilterStore(t, dsn)
	got, err := reopened.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if len(got.Filters) != 0 || got.ChangedAt == nil || !got.ChangedAt.Equal(marker) {
		t.Fatalf("restarted state = filters %#v, marker %v; want durable last-delete marker %s", got.Filters, got.ChangedAt, marker)
	}
}

func TestDialogFilterDefinitionSurvivesStoreRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner := mustUser(t, s, "+15551091004")
	member := mustUser(t, s, "+15551091005")
	chat, err := s.CreateChat(ctx, owner.ID, "persistent folder group", []int64{member.ID})
	if err != nil {
		t.Fatalf("create folder group: %v", err)
	}
	color := 3
	want := store.DialogFilter{
		ID:             2,
		Title:          "Work",
		Entities:       []store.DialogFilterEntity{{Offset: 0, Length: 4, DocumentID: 987}},
		Emoticon:       "📌",
		Color:          &color,
		Contacts:       true,
		Groups:         true,
		ExcludeMuted:   true,
		ExcludeRead:    true,
		TitleNoanimate: true,
		PinnedPeers:    []store.DialogFilterPeer{{Type: store.PeerTypeChat, ID: chat.ID}},
		IncludePeers:   []store.DialogFilterPeer{{Type: store.PeerTypeUser, ID: member.ID}},
		ExcludePeers:   []store.DialogFilterPeer{{Type: store.PeerTypeChannel, ID: 81}},
	}
	if err := s.SaveDialogFilter(ctx, owner.ID, want); err != nil {
		t.Fatalf("save persistent folder: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}
	reopened := openDialogFilterStore(t, dsn)
	got, err := reopened.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if len(got.Filters) != 1 {
		t.Fatalf("restarted folders = %d, want one", len(got.Filters))
	}
	folder := got.Filters[0]
	if folder.ID != want.ID || folder.Title != want.Title || folder.Emoticon != want.Emoticon || folder.Color == nil || *folder.Color != color || !folder.Contacts || !folder.Groups || !folder.ExcludeMuted || !folder.ExcludeRead || !folder.TitleNoanimate {
		t.Fatalf("restarted folder definition = %+v", folder)
	}
	if len(folder.Entities) != 1 || folder.Entities[0] != want.Entities[0] || len(folder.PinnedPeers) != 1 || folder.PinnedPeers[0] != want.PinnedPeers[0] || len(folder.IncludePeers) != 1 || folder.IncludePeers[0] != want.IncludePeers[0] || len(folder.ExcludePeers) != 1 || folder.ExcludePeers[0] != want.ExcludePeers[0] {
		t.Fatalf("restarted folder children = entities %#v, pinned %#v, include %#v, exclude %#v", folder.Entities, folder.PinnedPeers, folder.IncludePeers, folder.ExcludePeers)
	}
}

func TestDialogFilterInvalidChildRollsBackDefinitionAndMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091003")
	baseline, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("seed defaults before invalid save: %v", err)
	}
	if len(baseline.Filters) != 1 || baseline.Filters[0].Title != "Unread" || baseline.ChangedAt == nil {
		t.Fatalf("baseline folders = %#v, marker %v; want seeded Unread", baseline.Filters, baseline.ChangedAt)
	}
	marker, found, err := s.DialogFilterChangeAt(ctx, owner.ID)
	if err != nil || !found || !marker.Equal(*baseline.ChangedAt) {
		t.Fatalf("read baseline marker: %s, found %v, err %v", marker, found, err)
	}
	err = s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{
		ID:     3,
		Title:  "Rollback",
		Groups: true,
		Entities: []store.DialogFilterEntity{{
			Offset: 0, Length: 1, DocumentID: 0,
		}},
	})
	if err == nil {
		t.Fatal("invalid entity unexpectedly committed")
	}
	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after failed save: %v", err)
	}
	if len(snapshot.Filters) != 1 || snapshot.Filters[0].ID != 2 || snapshot.Filters[0].Title != "Unread" || snapshot.ChangedAt == nil || !snapshot.ChangedAt.Equal(marker) {
		t.Fatalf("failed save left filters %#v and marker %v", snapshot.Filters, snapshot.ChangedAt)
	}
	if after, found, err := s.DialogFilterChangeAt(ctx, owner.ID); err != nil || !found || !after.Equal(marker) {
		t.Fatalf("failed save changed committed marker from %s to %s (found %v, err %v)", marker, after, found, err)
	}
}
