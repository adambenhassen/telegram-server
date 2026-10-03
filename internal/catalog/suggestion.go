package catalog

import "strings"

// SuggestedLanguageCode returns an available tdesktop language code for a
// bounded system locale hint, or English when no available pack matches.
func SuggestedLanguageCode(snapshot *Snapshot, hint string) string {
	normalizedHint, ok := normalizeLocaleCode(hint)
	if !ok || snapshot == nil {
		return LanguageEnglish
	}

	base, _, _ := strings.Cut(normalizedHint, "-")
	baseMatch := ""
	for _, pack := range snapshot.Packs {
		if pack.Pack != PackTDesktop {
			continue
		}
		normalizedCode, valid := normalizeLocaleCode(pack.LanguageCode)
		if !valid {
			continue
		}
		if normalizedCode == normalizedHint {
			return pack.LanguageCode
		}
		packBase, _, _ := strings.Cut(normalizedCode, "-")
		if packBase == base && (baseMatch == "" || pack.LanguageCode < baseMatch) {
			baseMatch = pack.LanguageCode
		}
	}
	if baseMatch != "" {
		return baseMatch
	}
	return LanguageEnglish
}

func normalizeLocaleCode(code string) (string, bool) {
	if len(code) == 0 || len(code) > 32 {
		return "", false
	}

	normalized := make([]byte, len(code))
	for i := range len(code) {
		char := code[i]
		switch {
		case char >= 'A' && char <= 'Z':
			normalized[i] = char + ('a' - 'A')
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9':
			normalized[i] = char
		case char == '-':
			normalized[i] = '-'
		case char == '_':
			normalized[i] = '-'
		default:
			return "", false
		}
	}
	return string(normalized), true
}
