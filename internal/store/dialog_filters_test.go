package store_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

func TestDialogFiltersSeedDefaultsOnceAndKeepDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091006")

	seeded, err := s.SeedDefaultDialogFilters(ctx, owner.ID)
	if err != nil || !seeded {
		t.Fatalf("seed default dialog filters: seeded %v, err %v", seeded, err)
	}
	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read default dialog filters: %v", err)
	}
	if len(snapshot.Filters) != 4 {
		t.Fatalf("seed returned %d folders, want Personal, Groups, Channels and Unread", len(snapshot.Filters))
	}
	wantTitles := []string{"Personal", "Groups", "Channels", "Unread"}
	for i, want := range wantTitles {
		if snapshot.Filters[i].ID != i+2 || snapshot.Filters[i].Title != want {
			t.Fatalf("default %d = %+v, want ID %d %s", i, snapshot.Filters[i], i+2, want)
		}
		if len(snapshot.Filters[i].PinnedPeers)+len(snapshot.Filters[i].IncludePeers)+len(snapshot.Filters[i].ExcludePeers)+len(snapshot.Filters[i].Entities) != 0 {
			t.Fatalf("default %s contains owner-specific data: %+v", want, snapshot.Filters[i])
		}
	}
	if len(snapshot.Order) != 5 || snapshot.Order[0] != 0 || snapshot.Order[1] != 2 || snapshot.Order[2] != 3 || snapshot.Order[3] != 4 || snapshot.Order[4] != 5 {
		t.Fatalf("default folder order = %v, want All chats then Personal, Groups, Channels, Unread", snapshot.Order)
	}

	if _, err := s.DeleteDialogFilter(ctx, owner.ID, 5); err != nil {
		t.Fatalf("delete Unread: %v", err)
	}
	beforeRepeat, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after deleting Unread: %v", err)
	}
	seeded, err = s.SeedDefaultDialogFilters(ctx, owner.ID)
	if err != nil || seeded {
		t.Fatalf("repeat seed after deletion: seeded %v, err %v; want no re-seed", seeded, err)
	}
	afterRepeat, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read after repeat seed: %v", err)
	}
	if len(afterRepeat.Filters) != 3 || afterRepeat.ChangedAt == nil || beforeRepeat.ChangedAt == nil || !afterRepeat.ChangedAt.Equal(*beforeRepeat.ChangedAt) {
		t.Fatalf("repeat seed changed deleted defaults or marker: filters=%#v before=%v after=%v", afterRepeat.Filters, beforeRepeat.ChangedAt, afterRepeat.ChangedAt)
	}
}

func TestConcurrentFirstDialogFilterReadsSeedDefaultsOnceAndNotifyOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner := mustUser(t, s, "+15551091007")
	listener, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect notification listener: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(context.Background()); err != nil {
			t.Errorf("close notification listener: %v", err)
		}
	})
	if _, err := listener.Exec(ctx, "LISTEN tg_dialog_filters"); err != nil {
		t.Fatalf("listen for dialog filter notification: %v", err)
	}
	const attempts = 8
	seeded := make([]bool, attempts)
	errList := make([]error, attempts)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seeded[i], errList[i] = s.SeedDefaultDialogFilters(ctx, owner.ID)
		}(i)
	}
	wg.Wait()
	initializations := 0
	for i, err := range errList {
		if err != nil {
			t.Fatalf("concurrent seed %d: %v", i, err)
		}
		if seeded[i] {
			initializations++
		}
	}
	if initializations != 1 {
		t.Fatalf("concurrent seed initialized %d times, want once", initializations)
	}
	final, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("final get dialog filters: %v", err)
	}
	if len(final.Filters) != 4 {
		t.Fatalf("persisted folders = %#v, want exactly four defaults", final.Filters)
	}
	var notifications []*pgconn.Notification
	notifyCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	for {
		notification, err := listener.WaitForNotification(notifyCtx)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatalf("wait for dialog filter notification: %v", err)
		}
		notifications = append(notifications, notification)
	}
	if len(notifications) != 1 || notifications[0].Payload != strconv.FormatInt(owner.ID, 10) {
		t.Fatalf("seed notifications = %#v, want one owner-id-only notification for %d", notifications, owner.ID)
	}
}

func TestDialogFilterSeedPreservesExistingFoldersAndCapacity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openDialogFilterStore(t, pgtest.DSN(t))
	owner := mustUser(t, s, "+15551091009")
	member := mustUser(t, s, "+15551091010")
	if err := s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{
		ID: 2, Title: "Folder 1", Groups: true,
		IncludePeers: []store.DialogFilterPeer{{Type: store.PeerTypeUser, ID: member.ID}},
	}); err != nil {
		t.Fatalf("save existing id 2 folder: %v", err)
	}
	if err := s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{ID: 5, Title: "unread", ExcludeMuted: true}); err != nil {
		t.Fatalf("save existing duplicate-title folder: %v", err)
	}
	if _, err := s.UpdateDialogFilterOrder(ctx, owner.ID, []int{0, 5, 2}); err != nil {
		t.Fatalf("set existing folder order: %v", err)
	}
	seeded, err := s.SeedDefaultDialogFilters(ctx, owner.ID)
	if err != nil || !seeded {
		t.Fatalf("seed beside existing folders: seeded %v, err %v", seeded, err)
	}
	snapshot, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read seeded folders: %v", err)
	}
	if len(snapshot.Filters) != 5 {
		t.Fatalf("seeded %d folders, want 2 existing plus 3 missing defaults", len(snapshot.Filters))
	}
	if snapshot.Filters[0].ID != 2 || snapshot.Filters[0].Title != "Folder 1" || !snapshot.Filters[0].Groups || len(snapshot.Filters[0].IncludePeers) != 1 || snapshot.Filters[0].IncludePeers[0].ID != member.ID {
		t.Fatalf("existing id 2 folder changed: %+v", snapshot.Filters[0])
	}
	if snapshot.Filters[1].ID != 3 || snapshot.Filters[1].Title != "Personal" || snapshot.Filters[2].ID != 4 || snapshot.Filters[2].Title != "Groups" || snapshot.Filters[3].ID != 5 || snapshot.Filters[3].Title != "unread" || !snapshot.Filters[3].ExcludeMuted || snapshot.Filters[4].ID != 6 || snapshot.Filters[4].Title != "Channels" {
		t.Fatalf("seeded folders or ids = %+v, want missing defaults appended in canonical order without duplicate Unread", snapshot.Filters)
	}
	if len(snapshot.Order) != 6 || snapshot.Order[0] != 0 || snapshot.Order[1] != 5 || snapshot.Order[2] != 2 || snapshot.Order[3] != 3 || snapshot.Order[4] != 4 || snapshot.Order[5] != 6 {
		t.Fatalf("order after seeding = %v, want existing order followed by missing defaults", snapshot.Order)
	}

	fullOwner := mustUser(t, s, "+15551091011")
	for i := range 9 {
		if err := s.SaveDialogFilter(ctx, fullOwner.ID, store.DialogFilter{ID: i + 2, Title: fmt.Sprintf("Folder %d", i), Groups: true}); err != nil {
			t.Fatalf("save folder %d for nine-folder account: %v", i, err)
		}
	}
	if seeded, err := s.SeedDefaultDialogFilters(ctx, fullOwner.ID); err != nil || !seeded {
		t.Fatalf("seed nine-folder account: seeded %v, err %v", seeded, err)
	}
	partial, err := s.DialogFilters(ctx, fullOwner.ID)
	if err != nil {
		t.Fatalf("read nine-folder account: %v", err)
	}
	if len(partial.Filters) != 10 || partial.Filters[9].Title != "Personal" {
		t.Fatalf("nine-folder account received %#v; want only Personal appended", partial.Filters)
	}
	if _, err := s.DeleteDialogFilter(ctx, fullOwner.ID, 2); err != nil {
		t.Fatalf("delete a folder after initialization: %v", err)
	}
	if seeded, err := s.SeedDefaultDialogFilters(ctx, fullOwner.ID); err != nil || seeded {
		t.Fatalf("repeat seed after making room: seeded %v, err %v; want marker to prevent backfill", seeded, err)
	}
	partial, err = s.DialogFilters(ctx, fullOwner.ID)
	if err != nil || len(partial.Filters) != 9 {
		t.Fatalf("folders after deletion and repeat seed = %d, err %v; want 9", len(partial.Filters), err)
	}

	capacityOwner := mustUser(t, s, "+15551091013")
	for i := range 10 {
		if err := s.SaveDialogFilter(ctx, capacityOwner.ID, store.DialogFilter{ID: i + 2, Title: fmt.Sprintf("Existing %d", i), Groups: true}); err != nil {
			t.Fatalf("save folder %d for full account: %v", i, err)
		}
	}
	if seeded, err := s.SeedDefaultDialogFilters(ctx, capacityOwner.ID); err != nil || !seeded {
		t.Fatalf("seed full account: seeded %v, err %v", seeded, err)
	}
	full, err := s.DialogFilters(ctx, capacityOwner.ID)
	if err != nil || len(full.Filters) != 10 {
		t.Fatalf("full account folders = %d, err %v; want unchanged capacity of 10", len(full.Filters), err)
	}
	if seeded, err := s.DialogFilterDefaultsSeeded(ctx, capacityOwner.ID); err != nil || !seeded {
		t.Fatalf("full account was not durably marked seeded: seeded %v, err %v", seeded, err)
	}
}

func TestDialogFilterSeedRollbackLeavesNoState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner := mustUser(t, s, "+15551091012")
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for seed failure trigger: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close seed failure connection: %v", err)
		}
	})
	_, err = conn.Exec(ctx, `
CREATE FUNCTION fail_dialog_filter_seed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'forced seed insert failure';
END $$;`)
	if err != nil {
		t.Fatalf("install seed failure function: %v", err)
	}
	_, err = conn.Exec(ctx, `CREATE TRIGGER fail_dialog_filter_seed BEFORE INSERT ON user_dialog_filters
FOR EACH ROW EXECUTE FUNCTION fail_dialog_filter_seed();`)
	if err != nil {
		t.Fatalf("install seed failure trigger: %v", err)
	}
	if seeded, err := s.SeedDefaultDialogFilters(ctx, owner.ID); err == nil || seeded {
		t.Fatalf("seed with forced insert failure = seeded %v, err %v; want rollback", seeded, err)
	}
	var stateCount, folderCount int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM user_dialog_filter_state WHERE owner_id = $1`, owner.ID).Scan(&stateCount); err != nil {
		t.Fatalf("read state rows after rollback: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM user_dialog_filters WHERE owner_id = $1`, owner.ID).Scan(&folderCount); err != nil {
		t.Fatalf("read folders after rollback: %v", err)
	}
	if stateCount != 0 || folderCount != 0 {
		t.Fatalf("failed seed left %d state rows and %d folders, want neither", stateCount, folderCount)
	}
}

func TestDialogFilterMarkerSurvivesLastDeleteAndRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDialogFilterStore(t, dsn)
	owner := mustUser(t, s, "+15551091002")
	if _, err := s.SeedDefaultDialogFilters(ctx, owner.ID); err != nil {
		t.Fatalf("seed defaults before testing last-folder delete: %v", err)
	}
	for id := 2; id <= 5; id++ {
		if deleted, err := s.DeleteDialogFilter(ctx, owner.ID, id); err != nil || !deleted {
			t.Fatalf("delete default folder %d: deleted %v, err %v", id, deleted, err)
		}
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
	if seeded, err := reopened.SeedDefaultDialogFilters(ctx, owner.ID); err != nil || seeded {
		t.Fatalf("restart re-seeded deleted defaults: seeded %v, err %v", seeded, err)
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
	if _, err := s.SeedDefaultDialogFilters(ctx, owner.ID); err != nil {
		t.Fatalf("seed defaults before invalid save: %v", err)
	}
	baseline, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read seeded defaults before invalid save: %v", err)
	}
	if len(baseline.Filters) != 4 || baseline.Filters[0].Title != "Personal" || baseline.ChangedAt == nil {
		t.Fatalf("baseline folders = %#v, marker %v; want four seeded defaults", baseline.Filters, baseline.ChangedAt)
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
	if len(snapshot.Filters) != 4 || snapshot.Filters[0].ID != 2 || snapshot.Filters[0].Title != "Personal" || snapshot.ChangedAt == nil || !snapshot.ChangedAt.Equal(marker) {
		t.Fatalf("failed save left filters %#v and marker %v", snapshot.Filters, snapshot.ChangedAt)
	}
	if after, found, err := s.DialogFilterChangeAt(ctx, owner.ID); err != nil || !found || !after.Equal(marker) {
		t.Fatalf("failed save changed committed marker from %s to %s (found %v, err %v)", marker, after, found, err)
	}
}
