package api

import (
	"errors"
	"strings"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/catalog"
)

func TestLangpackResponsesUsePreparedEnglishCatalog(t *testing.T) {
	t.Parallel()

	snapshot := langpackTestSnapshot()
	service := newLangpackService(snapshot, 2)

	languages, err := service.getLanguages(catalog.PackTDesktop)
	if err != nil {
		t.Fatal(err)
	}
	var languageVector tg.LangPackLanguageVector
	decodeLangpackResponse(t, languages, &languageVector)
	if len(languageVector.Elems) != 1 {
		t.Fatalf("languages = %#v, want the single published English pack", languageVector.Elems)
	}
	language := languageVector.Elems[0]
	if language.Name != "English" || language.NativeName != "English" || language.LangCode != catalog.LanguageEnglish || language.PluralCode != catalog.LanguageEnglish ||
		language.StringsCount != 3 || language.TranslatedCount != 3 || !language.Official || language.Rtl || language.Beta || language.BaseLangCode != "" || language.TranslationsURL != "" {
		t.Fatalf("English metadata = %+v", language)
	}
	if again, err := service.getLanguages(catalog.PackTDesktop); err != nil || again != languages {
		t.Fatalf("languages response was not reused: same=%t err=%v", again == languages, err)
	}

	full, err := service.getLangPack(catalog.PackTDesktop, catalog.LanguageEnglish)
	if err != nil {
		t.Fatal(err)
	}
	var fullDifference tg.LangPackDifference
	decodeLangpackResponse(t, full, &fullDifference)
	if fullDifference.FromVersion != 0 || fullDifference.Version != 4 || len(fullDifference.Strings) != 3 {
		t.Fatalf("full pack = %+v, want version 4 with three current strings", full)
	}
	if again, err := service.getLangPack(catalog.PackTDesktop, catalog.LanguageEnglish); err != nil || again != full {
		t.Fatalf("full pack response was not reused: same=%t err=%v", again == full, err)
	}

	selected, err := service.getStrings(catalog.PackTDesktop, catalog.LanguageEnglish, []string{"HELLO", "HELLO", strings.Repeat("x", catalog.MaxKeyBytes+1), "PLURAL", "MISSING"})
	if err != nil {
		t.Fatal(err)
	}
	var selectedVector tg.LangPackStringClassVector
	decodeLangpackResponse(t, selected, &selectedVector)
	if len(selectedVector.Elems) != 2 {
		t.Fatalf("selected strings = %d, want one each for HELLO and PLURAL", len(selectedVector.Elems))
	}
	first, ok := selectedVector.Elems[0].(*tg.LangPackString)
	if !ok || first.Key != "HELLO" || first.Value != "Hello" {
		t.Fatalf("first selected string = %#v", selectedVector.Elems[0])
	}
	plural, ok := selectedVector.Elems[1].(*tg.LangPackStringPluralized)
	if !ok || plural.Key != "PLURAL" || plural.OneValue != "one item" || plural.OtherValue != "many items" {
		t.Fatalf("plural response = %#v", selectedVector.Elems[1])
	}
	if one, ok := plural.GetOneValue(); !ok || one != "one item" {
		t.Fatalf("plural one = %q, %t", one, ok)
	}

	nearest := service.nearestDC()
	if nearest.ThisDC != 2 || nearest.NearestDC != 2 || nearest.Country != "" {
		t.Fatalf("nearest DC = %+v", nearest)
	}
}

func TestLangpackResponsesBoundInputAndHideUnsupportedValues(t *testing.T) {
	t.Parallel()
	service := newLangpackService(langpackTestSnapshot(), 4)

	if _, err := service.getLanguages("other"); !hasRPCError(err, "LANG_PACK_INVALID") {
		t.Fatalf("unsupported pack error = %v", err)
	}
	if _, err := service.getLangPack(catalog.PackTDesktop, strings.Repeat("e", 33)); !hasRPCError(err, "LANGUAGE_INVALID") {
		t.Fatalf("overlong language error = %v", err)
	}
	if _, err := service.getLangPack(catalog.PackTDesktop, "de"); !hasRPCError(err, "LANG_CODE_NOT_SUPPORTED") {
		t.Fatalf("unsupported language error = %v", err)
	}
	if _, err := service.getStrings("private-pack-name", "private-language-code", []string{"private-key"}); !hasRPCError(err, "LANG_PACK_INVALID") {
		t.Fatalf("invalid pack error = %v", err)
	} else if strings.Contains(err.Error(), "private-") {
		t.Fatalf("error echoed unsupported input: %v", err)
	}

	tooMany := make([]string, 1025)
	if _, err := service.getStrings(catalog.PackTDesktop, catalog.LanguageEnglish, tooMany); !hasRPCError(err, "INPUT_REQUEST_INVALID") {
		t.Fatalf("1025-key error = %v", err)
	}
}

func TestLangpackDifferenceUsesBoundedCatalogHistory(t *testing.T) {
	t.Parallel()
	service := newLangpackService(langpackTestSnapshot(), 2)

	difference, err := service.getDifference(catalog.PackTDesktop, catalog.LanguageEnglish, 2)
	if err != nil {
		t.Fatal(err)
	}
	var response tg.LangPackDifference
	decodeLangpackResponse(t, difference, &response)
	if response.FromVersion != 2 || response.Version != 4 || len(response.Strings) != 2 {
		t.Fatalf("difference = %+v", response)
	}
	changed, ok := response.Strings[0].(*tg.LangPackString)
	if !ok || changed.Key != "NEW" || changed.Value != "new value" {
		t.Fatalf("first change = %#v, want NEW value", response.Strings[0])
	}
	deleted, ok := response.Strings[1].(*tg.LangPackStringDeleted)
	if !ok || deleted.Key != "REMOVED" {
		t.Fatalf("second change = %#v, want REMOVED tombstone", response.Strings[1])
	}

	for _, fromVersion := range []int{-2147483648, -1} {
		full, err := service.getDifference(catalog.PackTDesktop, catalog.LanguageEnglish, fromVersion)
		if err != nil {
			t.Fatal(err)
		}
		var fullResponse tg.LangPackDifference
		decodeLangpackResponse(t, full, &fullResponse)
		if fullResponse.FromVersion != fromVersion || fullResponse.Version != 4 || len(fullResponse.Strings) != 3 {
			t.Fatalf("difference from %d = %+v, want one current full pack", fromVersion, fullResponse)
		}
		for _, entry := range fullResponse.Strings {
			if _, ok := entry.(*tg.LangPackStringDeleted); ok {
				t.Fatalf("full response from %d included a tombstone: %#v", fromVersion, entry)
			}
		}
	}

	for _, fromVersion := range []int{4, 9} {
		future, err := service.getDifference(catalog.PackTDesktop, catalog.LanguageEnglish, fromVersion)
		if err != nil {
			t.Fatal(err)
		}
		var futureResponse tg.LangPackDifference
		decodeLangpackResponse(t, future, &futureResponse)
		if futureResponse.FromVersion != fromVersion || futureResponse.Version != fromVersion || len(futureResponse.Strings) != 0 {
			t.Fatalf("difference from future version %d = %+v", fromVersion, futureResponse)
		}
	}
}

func langpackTestSnapshot() *catalog.Snapshot {
	one := "one item"
	return &catalog.Snapshot{Packs: []catalog.Pack{{
		Pack:          catalog.PackTDesktop,
		LanguageCode:  catalog.LanguageEnglish,
		Name:          "English",
		NativeName:    "English",
		PluralCode:    catalog.LanguageEnglish,
		Version:       4,
		OldestVersion: 2,
		Entries: []catalog.Entry{
			{Key: "HELLO", Value: "Hello"},
			{Key: "PLURAL", Plural: &catalog.Plural{One: &one, Other: "many items"}},
			{Key: "NEW", Value: "new value"},
		},
		Changes: []catalog.Change{
			{Version: 2, Entry: catalog.Entry{Key: "HELLO", Value: "Hello"}},
			{Version: 3, Entry: catalog.Entry{Key: "REMOVED", Deleted: true}, Deleted: true},
			{Version: 4, Entry: catalog.Entry{Key: "NEW", Value: "new value"}},
		},
	}}}
}

func hasRPCError(err error, message string) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Message == message
}

func decodeLangpackResponse(t *testing.T, encoded bin.Encoder, decoded bin.Decoder) {
	t.Helper()
	var buffer bin.Buffer
	if err := encoded.Encode(&buffer); err != nil {
		t.Fatalf("encode langpack response: %v", err)
	}
	reader := bin.Buffer{Buf: buffer.Copy()}
	if err := decoded.Decode(&reader); err != nil {
		t.Fatalf("decode langpack response: %v", err)
	}
}
