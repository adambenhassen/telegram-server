package catalog_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/teagramhq/teagram-server/internal/catalog"
)

type Artifact = catalog.Artifact
type Source = catalog.Source
type Entry = catalog.Entry

const (
	PackTDesktop          = catalog.PackTDesktop
	LanguageEnglish       = catalog.LanguageEnglish
	MaxKeyBytes           = catalog.MaxKeyBytes
	MaxValueBytes         = catalog.MaxValueBytes
	MaxEncodedPackBytes   = catalog.MaxEncodedPackBytes
	ArtifactSchemaVersion = catalog.ArtifactSchemaVersion
)

var (
	BuildEnglish           = catalog.BuildEnglish
	ParseArtifact          = catalog.ParseArtifact
	ErrInvalidUTF8         = catalog.ErrInvalidUTF8
	ErrNULByte             = catalog.ErrNULByte
	ErrMalformedEntry      = catalog.ErrMalformedEntry
	ErrDuplicateKey        = catalog.ErrDuplicateKey
	ErrMalformedPlural     = catalog.ErrMalformedPlural
	ErrEnglishOnly         = catalog.ErrEnglishOnly
	ErrKeyTooLong          = catalog.ErrKeyTooLong
	ErrValueTooLong        = catalog.ErrValueTooLong
	ErrChecksumMismatch    = catalog.ErrChecksumMismatch
	ErrEncodedPackTooLarge = catalog.ErrEncodedPackTooLarge
)

func TestBuildEnglishIsDeterministicAndPreservesPlurals(t *testing.T) {
	t.Parallel()

	const source = `
// The source order is intentionally not canonical.
"lng_plural#other" = "{count} files";
"lng_plain" = "Plain value";
"lng_plural#one" = "{count} file";
`
	sum := sha256.Sum256([]byte(source))
	artifact, err := BuildEnglish([]byte(source), Source{
		URL:      "https://github.com/telegramdesktop/tdesktop/blob/abc/Telegram/Resources/langs/lang.strings",
		Revision: "abc",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("BuildEnglish: %v", err)
	}
	if artifact.Pack != PackTDesktop || artifact.LanguageCode != LanguageEnglish {
		t.Fatalf("artifact identity = %q/%q, want %q/%q", artifact.Pack, artifact.LanguageCode, PackTDesktop, LanguageEnglish)
	}
	if len(artifact.Entries) != 2 || artifact.Entries[0].Key != "lng_plain" || artifact.Entries[1].Key != "lng_plural" {
		t.Fatalf("entries = %+v, want canonical key order", artifact.Entries)
	}
	if artifact.Entries[1].Plural == nil || artifact.Entries[1].Plural.One == nil || artifact.Entries[1].Plural.Other != "{count} files" {
		t.Fatalf("plural = %+v, want one/other forms", artifact.Entries[1].Plural)
	}
	if artifact.Source.Notice == "" || artifact.Attribution == "" {
		t.Fatalf("artifact omitted source attribution: %+v", artifact)
	}

	canonical, err := artifact.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes: %v", err)
	}
	canonicalAgain, err := artifact.CanonicalBytes()
	if err != nil {
		t.Fatalf("CanonicalBytes second call: %v", err)
	}
	if !bytes.Equal(canonical, canonicalAgain) {
		t.Fatalf("canonical bytes changed between calls")
	}
	if _, err := ParseArtifact(canonical); err != nil {
		t.Fatalf("ParseArtifact(canonical): %v", err)
	}
}

func TestBuildEnglishAcceptsUpstreamBlockHeader(t *testing.T) {
	t.Parallel()
	// Header and first entries from tdesktop revision 33261535a0e747f125e0ed25486f01e556330677.
	raw, err := os.ReadFile("testdata/tdesktop_header.strings")
	if err != nil {
		t.Fatalf("read upstream header fixture: %v", err)
	}
	sum := sha256.Sum256(raw)
	checksum := hex.EncodeToString(sum[:])
	artifact, err := BuildEnglish(raw, Source{
		URL: "https://example.test/tdesktop/lang.strings", Revision: "revision", SHA256: checksum,
	})
	if err != nil {
		t.Fatalf("build unchanged source with upstream header: %v", err)
	}
	if artifact.Source.SHA256 != checksum || len(artifact.Entries) != 2 ||
		artifact.Entries[0].Key != "lng_language_name" || artifact.Entries[0].Value != "English" ||
		artifact.Entries[1].Key != "lng_switch_to_this" || artifact.Entries[1].Value != "Continue in English" {
		t.Fatalf("artifact = %+v, want both English entries and original checksum", artifact)
	}
}

func TestBuildEnglishBlockCommentsPreserveQuotedMarkers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  string
		key  string
		want string
	}{
		{name: "inline", raw: `/* before */"key"/* key */ = /* value */"value";/* after */`, want: "value"},
		{name: "multiline", raw: "/* comment\n\"ignored\" = \"hidden\";\n*/\n\"key\" = \"value\";", want: "value"},
		{name: "markers in value", raw: `"key" = "https://example.test/*literal*/ // literal"; // /* not a block`, want: "https://example.test/*literal*/ // literal"},
		{name: "markers in key", raw: `"key/*literal*/ // literal" = "value";`, key: "key/*literal*/ // literal", want: "value"},
		{name: "escaped quote", raw: `"key" = "Escaped \"quote\" /* literal */ // literal";`, want: `Escaped "quote" /* literal */ // literal`},
		{name: "line comment inside block", raw: "/* // still a block */\n\"key\" = \"value\";", want: "value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum := sha256.Sum256([]byte(tc.raw))
			artifact, err := BuildEnglish([]byte(tc.raw), Source{
				URL: "https://example.test/tdesktop/lang.strings", Revision: "revision", SHA256: hex.EncodeToString(sum[:]),
			})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			key := tc.key
			if key == "" {
				key = "key"
			}
			if len(artifact.Entries) != 1 || artifact.Entries[0].Key != key || artifact.Entries[0].Value != tc.want {
				t.Fatalf("entries = %+v, want key with value %q", artifact.Entries, tc.want)
			}
		})
	}
}

func TestBuildEnglishPreservesEntryBoundariesAcrossBlockComments(t *testing.T) {
	t.Parallel()
	raw := []byte("\"first\" = \"one\";/* comment\ncomment */\"second\" = \"two\";")
	sum := sha256.Sum256(raw)
	artifact, err := BuildEnglish(raw, Source{
		URL: "https://example.test/tdesktop/lang.strings", Revision: "revision", SHA256: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(artifact.Entries) != 2 || artifact.Entries[0].Key != "first" || artifact.Entries[0].Value != "one" ||
		artifact.Entries[1].Key != "second" || artifact.Entries[1].Value != "two" {
		t.Fatalf("entries = %+v, want both entries separated by the block's newline", artifact.Entries)
	}
}

func TestBuildEnglishRejectsUnterminatedBlockComment(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"/* PRIVATE_TRANSLATION", "\"key\" = \"value\";\n/* PRIVATE_TRANSLATION"} {
		sum := sha256.Sum256([]byte(raw))
		_, err := BuildEnglish([]byte(raw), Source{
			URL: "https://example.test/tdesktop/lang.strings", Revision: "revision", SHA256: hex.EncodeToString(sum[:]),
		})
		if !errors.Is(err, ErrMalformedEntry) {
			t.Fatalf("error = %v, want malformed entry", err)
		}
		if strings.Contains(err.Error(), "PRIVATE_TRANSLATION") {
			t.Fatalf("diagnostic contains source text: %v", err)
		}
	}
}

func TestParseArtifactRejectsChangedSourceNoticeAndAttribution(t *testing.T) {
	t.Parallel()
	base := validTestArtifact(t)
	for _, tc := range []struct {
		name   string
		mutate func(*Artifact)
	}{
		{name: "source notice", mutate: func(a *Artifact) { a.Source.Notice = "changed source notice" }},
		{name: "attribution", mutate: func(a *Artifact) { a.Attribution = "changed attribution" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := base
			tc.mutate(&artifact)
			data, err := json.Marshal(artifact)
			if err != nil {
				t.Fatalf("marshal artifact: %v", err)
			}
			if _, err := ParseArtifact(data); err == nil {
				t.Fatal("ParseArtifact accepted modified provenance")
			}
		})
	}
}

func TestBuildEnglishRejectsInvalidInputsWithValueFreeDiagnostics(t *testing.T) {
	t.Parallel()

	valid := func(raw []byte) Source {
		sum := sha256.Sum256(raw)
		return Source{
			URL:      "https://example.test/tdesktop/lang.strings",
			Revision: "revision",
			SHA256:   hex.EncodeToString(sum[:]),
		}
	}
	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "invalid utf8", raw: []byte{0xff}, want: ErrInvalidUTF8},
		{name: "nul", raw: []byte{'"', 'k', '"', ' ', '=', ' ', '"', 0, '"', ';'}, want: ErrNULByte},
		{name: "malformed entry", raw: []byte(`"key" = "value"`), want: ErrMalformedEntry},
		{name: "duplicate key", raw: []byte("\"key\" = \"one\";\n\"key\" = \"two\";"), want: ErrDuplicateKey},
		{name: "malformed plural", raw: []byte(`"key#one" = "one";`), want: ErrMalformedPlural},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildEnglish(tc.raw, valid(tc.raw))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if strings.Contains(err.Error(), "one") || strings.Contains(err.Error(), "two") || strings.Contains(err.Error(), "value") {
				t.Fatalf("diagnostic contains source text: %q", err)
			}
		})
	}
}

func TestArtifactValidationRejectsNonEnglishAndOversizedFields(t *testing.T) {
	t.Parallel()
	base := validTestArtifact(t)
	cases := []struct {
		name   string
		mutate func(*Artifact)
		want   error
	}{
		{name: "non english", mutate: func(a *Artifact) { a.LanguageCode = "de" }, want: ErrEnglishOnly},
		{name: "long key", mutate: func(a *Artifact) { a.Entries[0].Key = strings.Repeat("k", MaxKeyBytes+1) }, want: ErrKeyTooLong},
		{name: "long value", mutate: func(a *Artifact) { a.Entries[0].Value = strings.Repeat("v", MaxValueBytes+1) }, want: ErrValueTooLong},
		{name: "invalid source checksum", mutate: func(a *Artifact) { a.Source.SHA256 = "not-a-sha" }, want: ErrChecksumMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := base
			a.Entries = append([]Entry(nil), base.Entries...)
			tc.mutate(&a)
			if err := a.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestArtifactRejectsEncodedPackAboveCeiling(t *testing.T) {
	t.Parallel()

	raw := []byte(`"key" = "value";`)
	sum := sha256.Sum256(raw)
	artifact, err := BuildEnglish(raw, Source{
		URL:      "https://example.test/lang.strings",
		Revision: "revision",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("BuildEnglish: %v", err)
	}
	for i := range 300 {
		artifact.Entries = append(artifact.Entries, Entry{Key: "key" + strconv.Itoa(i), Value: strings.Repeat("x", MaxValueBytes)})
	}
	sort.Slice(artifact.Entries, func(i, j int) bool { return artifact.Entries[i].Key < artifact.Entries[j].Key })
	if err := artifact.Validate(); !errors.Is(err, ErrEncodedPackTooLarge) {
		t.Fatalf("Validate = %v, want %v", err, ErrEncodedPackTooLarge)
	}
}

func TestArtifactRejectsPluralSyntaxInCanonicalKeys(t *testing.T) {
	t.Parallel()
	artifact := validTestArtifact(t)
	artifact.Entries[0].Key = "key#one"
	if err := artifact.Validate(); !errors.Is(err, ErrMalformedPlural) {
		t.Fatalf("Validate = %v, want %v", err, ErrMalformedPlural)
	}
}

func validTestArtifact(t *testing.T) Artifact {
	t.Helper()
	raw := []byte(`"key" = "value";`)
	sum := sha256.Sum256(raw)
	artifact, err := BuildEnglish(raw, Source{
		URL:      "https://example.test/tdesktop/lang.strings",
		Revision: "revision",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("BuildEnglish: %v", err)
	}
	return artifact
}
