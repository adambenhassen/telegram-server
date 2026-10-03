package catalog_test

import (
	"testing"

	"github.com/teagramhq/teagram-server/internal/catalog"
)

type Pack = catalog.Pack
type Change = catalog.Change

func TestSnapshotDifferenceUsesLatestChangePerKey(t *testing.T) {
	t.Parallel()

	pack := Pack{
		Pack:          PackTDesktop,
		LanguageCode:  LanguageEnglish,
		Version:       4,
		OldestVersion: 1,
		Entries: []Entry{
			{Key: "added", Value: "new"},
			{Key: "kept", Value: "same"},
		},
		Changes: []Change{
			{Version: 2, Entry: Entry{Key: "edited", Value: "old"}},
			{Version: 3, Deleted: true, Entry: Entry{Key: "removed"}},
			{Version: 4, Entry: Entry{Key: "edited", Value: "new"}},
			{Version: 4, Entry: Entry{Key: "added", Value: "new"}},
		},
	}

	diff := pack.Difference(1)
	if diff.Version != 4 || len(diff.Entries) != 3 {
		t.Fatalf("difference = %+v, want version 4 and three latest changes", diff)
	}
	if diff.Entries[0].Key != "added" || diff.Entries[1].Key != "edited" || diff.Entries[2].Key != "removed" {
		t.Fatalf("difference keys = %+v, want sorted added/edited/removed", diff.Entries)
	}
	if !diff.Entries[2].Deleted {
		t.Fatalf("removed entry = %+v, want tombstone", diff.Entries[2])
	}

	future := pack.Difference(9)
	if future.FromVersion != 9 || future.Version != 9 || len(future.Entries) != 0 {
		t.Fatalf("future difference = %+v, want empty version 9", future)
	}
}

func TestSnapshotDifferenceFallsBackToFullPackBeforeRetainedHistory(t *testing.T) {
	t.Parallel()

	pack := Pack{
		Pack:          PackTDesktop,
		LanguageCode:  LanguageEnglish,
		Version:       5,
		OldestVersion: 3,
		Entries:       []Entry{{Key: "current", Value: "value"}},
		Changes:       []Change{{Version: 3, Entry: Entry{Key: "current", Value: "value"}}},
	}
	diff := pack.Difference(1)
	if diff.Version != 5 || len(diff.Entries) != 1 || diff.Entries[0].Key != "current" {
		t.Fatalf("full difference = %+v, want current pack", diff)
	}
}

func TestSnapshotSelectedEntriesPreservesRequestedOrderAndOmitsMissing(t *testing.T) {
	t.Parallel()

	pack := Pack{Entries: []Entry{{Key: "a", Value: "A"}, {Key: "b", Value: "B"}}}
	got := pack.Select([]string{"b", "missing", "a"})
	if len(got) != 2 || got[0].Key != "b" || got[1].Key != "a" {
		t.Fatalf("selected = %+v, want b/a", got)
	}
}
