// Package catalog defines the reviewed, immutable language-catalog artifact
// and the English tdesktop source importer. It has no database or network
// dependencies so the build step remains an offline operation.
package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	PackTDesktop    = "tdesktop"
	LanguageEnglish = "en"

	MaxKeyBytes           = 128
	MaxValueBytes         = 16 * 1024
	MaxEncodedPackBytes   = 4 * 1024 * 1024
	ArtifactSchemaVersion = 1
)

var (
	ErrInvalidUTF8         = errors.New("catalog: invalid UTF-8")
	ErrNULByte             = errors.New("catalog: NUL byte")
	ErrMalformedEntry      = errors.New("catalog: malformed entry")
	ErrDuplicateKey        = errors.New("catalog: duplicate key")
	ErrMalformedPlural     = errors.New("catalog: malformed plural")
	ErrEnglishOnly         = errors.New("catalog: only English catalogs are supported")
	ErrKeyTooLong          = errors.New("catalog: key too long")
	ErrValueTooLong        = errors.New("catalog: value too long")
	ErrChecksumMismatch    = errors.New("catalog: checksum mismatch")
	ErrEncodedPackTooLarge = errors.New("catalog: encoded pack too large")
	ErrMalformedArtifact   = errors.New("catalog: malformed artifact")
	ErrInvalidMetadata     = errors.New("catalog: invalid metadata")
)

// Source records the exact upstream input used to create an artifact. The
// SHA256 is the checksum of the supplied lang.strings bytes, not a fetched
// value, and the importer never dereferences URL.
type Source struct {
	URL      string `json:"url"`
	Revision string `json:"revision"`
	SHA256   string `json:"sha256"`
	Notice   string `json:"notice"`
}

// Plural contains the Telegram langPackStringPluralized forms. Other is
// required by the MTProto constructor; the remaining forms are optional.
type Plural struct {
	Zero  *string `json:"zero"`
	One   *string `json:"one"`
	Two   *string `json:"two"`
	Few   *string `json:"few"`
	Many  *string `json:"many"`
	Other string  `json:"other"`
}

// Entry is either an ordinary string (Plural nil) or a pluralized string.
// Value is intentionally not a pointer: an empty ordinary translation is a
// valid value and remains distinguishable from a plural entry by Plural.
type Entry struct {
	Key     string  `json:"key"`
	Value   string  `json:"value"`
	Plural  *Plural `json:"plural,omitempty"`
	Deleted bool    `json:"deleted,omitempty"`
}

// Artifact is the canonical, reviewable publication input. Entries are
// sorted by key before encoding.
type Artifact struct {
	SchemaVersion int     `json:"schema_version"`
	Pack          string  `json:"pack"`
	LanguageCode  string  `json:"language_code"`
	Name          string  `json:"name"`
	NativeName    string  `json:"native_name"`
	PluralCode    string  `json:"plural_code"`
	Source        Source  `json:"source"`
	Attribution   string  `json:"attribution"`
	Entries       []Entry `json:"entries"`
}

const sourceNotice = "This artifact contains source strings from Telegram Desktop, licensed under GPLv3 with the OpenSSL exception. See https://github.com/telegramdesktop/tdesktop/blob/master/LEGAL."

const sourceAttribution = "Telegram Desktop, https://github.com/telegramdesktop/tdesktop"

// BuildEnglish parses one supplied tdesktop lang.strings file and creates the
// English artifact without network access.
func BuildEnglish(raw []byte, source Source) (Artifact, error) {
	if !utf8.Valid(raw) {
		return Artifact{}, ErrInvalidUTF8
	}
	if bytes.IndexByte(raw, 0) >= 0 {
		return Artifact{}, ErrNULByte
	}
	actual := sha256.Sum256(raw)
	if !validSHA256(source.SHA256) || !strings.EqualFold(hex.EncodeToString(actual[:]), source.SHA256) {
		return Artifact{}, ErrChecksumMismatch
	}
	entries, err := parseSource(raw)
	if err != nil {
		return Artifact{}, err
	}
	artifact := Artifact{
		SchemaVersion: ArtifactSchemaVersion,
		Pack:          PackTDesktop,
		LanguageCode:  LanguageEnglish,
		Name:          "English",
		NativeName:    "English",
		PluralCode:    LanguageEnglish,
		Source: Source{
			URL:      source.URL,
			Revision: source.Revision,
			SHA256:   strings.ToLower(source.SHA256),
			Notice:   sourceNotice,
		},
		Attribution: sourceAttribution,
		Entries:     entries,
	}
	if err := artifact.Validate(); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

// ParseArtifact decodes and validates a canonical artifact. It does not
// access the filesystem or the network.
func ParseArtifact(data []byte) (Artifact, error) {
	if !utf8.Valid(data) {
		return Artifact{}, ErrInvalidUTF8
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return Artifact{}, ErrNULByte
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var artifact Artifact
	if err := decoder.Decode(&artifact); err != nil {
		return Artifact{}, ErrMalformedArtifact
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Artifact{}, ErrMalformedArtifact
	}
	if err := artifact.Validate(); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

// CanonicalBytes returns the stable JSON representation used for review
// manifests. Struct field order is fixed and entries are required to already
// be sorted by Validate.
func (a Artifact) CanonicalBytes() ([]byte, error) {
	return json.Marshal(a)
}

// ManifestSHA256 returns the checksum of the canonical artifact bytes.
func (a Artifact) ManifestSHA256() (string, error) {
	b, err := a.CanonicalBytes()
	if err != nil {
		return "", ErrMalformedArtifact
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ContentSHA256 returns the checksum of the effective catalog content. Source
// provenance and attribution are intentionally excluded, so changing a
// citation does not create a new served version.
func (a Artifact) ContentSHA256() (string, error) {
	content := struct {
		Pack         string  `json:"pack"`
		LanguageCode string  `json:"language_code"`
		Name         string  `json:"name"`
		NativeName   string  `json:"native_name"`
		PluralCode   string  `json:"plural_code"`
		Entries      []Entry `json:"entries"`
	}{
		Pack: a.Pack, LanguageCode: a.LanguageCode, Name: a.Name,
		NativeName: a.NativeName, PluralCode: a.PluralCode, Entries: a.Entries,
	}
	b, err := json.Marshal(content)
	if err != nil {
		return "", ErrMalformedArtifact
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Validate checks all artifact and transport bounds without including any
// source key or translation value in its diagnostics.
func (a Artifact) Validate() error {
	if a.SchemaVersion != ArtifactSchemaVersion || a.Pack != PackTDesktop || a.LanguageCode != LanguageEnglish {
		return ErrEnglishOnly
	}
	if a.Name == "" || a.NativeName == "" || a.PluralCode == "" || !validMetadata(a.Name) || !validMetadata(a.NativeName) || !validMetadata(a.PluralCode) {
		return ErrInvalidMetadata
	}
	if a.Source.URL == "" || a.Source.Revision == "" || !validSourceURL(a.Source.URL) || a.Source.Notice != sourceNotice || a.Attribution != sourceAttribution {
		return ErrInvalidMetadata
	}
	if !validSHA256(a.Source.SHA256) {
		return ErrChecksumMismatch
	}
	seen := make(map[string]struct{}, len(a.Entries))
	for i := range a.Entries {
		entry := a.Entries[i]
		if entry.Deleted {
			return ErrMalformedEntry
		}
		if entry.Key == "" || !utf8.ValidString(entry.Key) || strings.IndexByte(entry.Key, 0) >= 0 {
			return ErrMalformedEntry
		}
		if len(entry.Key) > MaxKeyBytes {
			return ErrKeyTooLong
		}
		if strings.ContainsRune(entry.Key, '#') {
			return ErrMalformedPlural
		}
		if _, ok := seen[entry.Key]; ok {
			return ErrDuplicateKey
		}
		seen[entry.Key] = struct{}{}
		if entry.Plural == nil {
			if err := validateValue(entry.Value); err != nil {
				return err
			}
			continue
		}
		if entry.Value != "" || entry.Plural.Other == "" && entry.Plural.One == nil && entry.Plural.Two == nil && entry.Plural.Few == nil && entry.Plural.Many == nil && entry.Plural.Zero == nil {
			return ErrMalformedPlural
		}
		if err := validatePlural(*entry.Plural); err != nil {
			return err
		}
		for _, value := range []*string{entry.Plural.Zero, entry.Plural.One, entry.Plural.Two, entry.Plural.Few, entry.Plural.Many} {
			if value != nil {
				if err := validateValue(*value); err != nil {
					return err
				}
			}
		}
		if err := validateValue(entry.Plural.Other); err != nil {
			return err
		}
	}
	if !sort.SliceIsSorted(a.Entries, func(i, j int) bool { return a.Entries[i].Key < a.Entries[j].Key }) {
		return ErrMalformedArtifact
	}
	b, err := a.CanonicalBytes()
	if err != nil {
		return ErrMalformedArtifact
	}
	if len(b) > MaxEncodedPackBytes {
		return ErrEncodedPackTooLarge
	}
	return nil
}

func validMetadata(value string) bool {
	return utf8.ValidString(value) && strings.IndexByte(value, 0) < 0 && len(value) <= 64
}

func validSourceURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "https" && u.Host != ""
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateValue(value string) error {
	if !utf8.ValidString(value) {
		return ErrInvalidUTF8
	}
	if strings.IndexByte(value, 0) >= 0 {
		return ErrNULByte
	}
	if len(value) > MaxValueBytes {
		return ErrValueTooLong
	}
	return nil
}

func validatePlural(plural Plural) error {
	if plural.Other == "" {
		return ErrMalformedPlural
	}
	return nil
}

type sourcePlural struct {
	values [5]*string
	other  *string
}

func parseSource(raw []byte) ([]Entry, error) {
	source, err := stripSourceComments(string(raw))
	if err != nil {
		return nil, err
	}
	entries := make(map[string]*Entry)
	plurals := make(map[string]*sourcePlural)
	for line := range strings.SplitSeq(source, "\n") {
		line = strings.TrimSuffix(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		key, value, err := parseLine(line)
		if err != nil {
			return nil, err
		}
		if err := validateValue(value); err != nil {
			return nil, err
		}
		if base, category, found := strings.CutLast(key, "#"); found {
			if base == "" || !isPluralCategory(category) {
				return nil, ErrMalformedPlural
			}
			if plain, ok := entries[base]; ok && plain.Plural == nil {
				_ = plain
				return nil, ErrDuplicateKey
			}
			plural := plurals[base]
			if plural == nil {
				plural = &sourcePlural{}
				plurals[base] = plural
			}
			var slot **string
			switch category {
			case "zero":
				slot = &plural.values[0]
			case "one":
				slot = &plural.values[1]
			case "two":
				slot = &plural.values[2]
			case "few":
				slot = &plural.values[3]
			case "many":
				slot = &plural.values[4]
			case "other":
				slot = &plural.other
			}
			if *slot != nil {
				return nil, ErrDuplicateKey
			}
			copyValue := value
			*slot = &copyValue
			continue
		}
		if _, ok := entries[key]; ok || plurals[key] != nil {
			return nil, ErrDuplicateKey
		}
		copyValue := value
		entries[key] = &Entry{Key: key, Value: copyValue}
	}
	for key, plural := range plurals {
		if plural.other == nil {
			return nil, ErrMalformedPlural
		}
		entry := &Entry{Key: key, Plural: &Plural{Other: *plural.other}}
		entry.Plural.Zero = plural.values[0]
		entry.Plural.One = plural.values[1]
		entry.Plural.Two = plural.values[2]
		entry.Plural.Few = plural.values[3]
		entry.Plural.Many = plural.values[4]
		entries[key] = entry
	}
	result := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, *entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func stripSourceComments(source string) (string, error) {
	var result strings.Builder
	for pos := 0; pos < len(source); {
		switch {
		case source[pos] == '"':
			_, next, err := parseQuoted(source, pos)
			if err != nil {
				return "", err
			}
			result.WriteString(source[pos:next])
			pos = next
		case strings.HasPrefix(source[pos:], "//"):
			next := strings.IndexByte(source[pos:], '\n')
			if next < 0 {
				return result.String(), nil
			}
			pos += next
		case strings.HasPrefix(source[pos:], "/*"):
			end := strings.Index(source[pos+2:], "*/")
			if end < 0 {
				return "", ErrMalformedEntry
			}
			end += pos + 4
			result.WriteByte(' ')
			// Keep line boundaries so separate entries cannot become one line.
			for _, char := range source[pos:end] {
				if char == '\n' {
					result.WriteByte('\n')
				}
			}
			pos = end
		default:
			result.WriteByte(source[pos])
			pos++
		}
	}
	return result.String(), nil
}

func isPluralCategory(value string) bool {
	switch value {
	case "zero", "one", "two", "few", "many", "other":
		return true
	default:
		return false
	}
}

func parseLine(line string) (string, string, error) {
	pos := 0
	key, next, err := parseQuoted(line, pos)
	if err != nil {
		return "", "", err
	}
	pos = skipSpace(line, next)
	if pos >= len(line) || line[pos] != '=' {
		return "", "", ErrMalformedEntry
	}
	pos = skipSpace(line, pos+1)
	value, next, err := parseQuoted(line, pos)
	if err != nil {
		return "", "", err
	}
	pos = skipSpace(line, next)
	if pos >= len(line) || line[pos] != ';' {
		return "", "", ErrMalformedEntry
	}
	pos = skipSpace(line, pos+1)
	if pos < len(line) && !strings.HasPrefix(line[pos:], "//") {
		return "", "", ErrMalformedEntry
	}
	if key == "" {
		return "", "", ErrMalformedEntry
	}
	return key, value, nil
}

func parseQuoted(line string, pos int) (string, int, error) {
	if pos >= len(line) || line[pos] != '"' {
		return "", 0, ErrMalformedEntry
	}
	for i := pos + 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '"':
			value, err := strconv.Unquote(line[pos : i+1])
			if err != nil {
				return "", 0, ErrMalformedEntry
			}
			return value, i + 1, nil
		}
	}
	return "", 0, ErrMalformedEntry
}

func skipSpace(value string, pos int) int {
	for pos < len(value) {
		switch value[pos] {
		case ' ', '\t':
			pos++
		default:
			return pos
		}
	}
	return pos
}
