package catalog_test

import (
	"strings"
	"testing"

	"github.com/teagramhq/teagram-server/internal/catalog"
)

func TestSuggestedLanguageCodeUsesEnglishOnlySnapshot(t *testing.T) {
	t.Parallel()

	snapshot := &catalog.Snapshot{Packs: []catalog.Pack{{
		Pack:         catalog.PackTDesktop,
		LanguageCode: catalog.LanguageEnglish,
	}}}
	for _, hint := range []string{"en", "en-US", "en_GB", "de-DE", "unsupported", "", strings.Repeat("a", 33), "en!", "é"} {
		t.Run(hint, func(t *testing.T) {
			t.Parallel()
			if got := catalog.SuggestedLanguageCode(snapshot, hint); got != catalog.LanguageEnglish {
				t.Errorf("SuggestedLanguageCode(%q) = %q, want %q", hint, got, catalog.LanguageEnglish)
			}
		})
	}
}

func TestSuggestedLanguageCodeUsesExactThenBaseAvailableMatch(t *testing.T) {
	t.Parallel()

	snapshot := &catalog.Snapshot{Packs: []catalog.Pack{
		{Pack: catalog.PackTDesktop, LanguageCode: catalog.LanguageEnglish},
		{Pack: catalog.PackTDesktop, LanguageCode: "de-DE"},
		{Pack: "other", LanguageCode: "fr-CA"},
	}}
	for _, test := range []struct {
		hint string
		want string
	}{
		{hint: "de-DE", want: "de-DE"},
		{hint: "DE_de", want: "de-DE"},
		{hint: "de-AT", want: "de-DE"},
		{hint: "fr-CA", want: catalog.LanguageEnglish},
		{hint: "fr", want: catalog.LanguageEnglish},
	} {
		t.Run(test.hint, func(t *testing.T) {
			t.Parallel()
			if got := catalog.SuggestedLanguageCode(snapshot, test.hint); got != test.want {
				t.Errorf("SuggestedLanguageCode(%q) = %q, want %q", test.hint, got, test.want)
			}
		})
	}
}
