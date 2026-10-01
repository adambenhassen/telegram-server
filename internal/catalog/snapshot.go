package catalog

import "sort"

// Change is one immutable effective change. Deleted entries are tombstones
// and carry only their key.
type Change struct {
	Version int64
	Entry   Entry
	Deleted bool
}

// Pack is the read-only in-memory representation of one published language.
// Entries are the current non-deleted values; Changes retains the bounded
// history needed for differences.
type Pack struct {
	Pack          string
	LanguageCode  string
	Name          string
	NativeName    string
	PluralCode    string
	Version       int64
	OldestVersion int64
	Entries       []Entry
	Changes       []Change
}

// Snapshot is an immutable set of validated packs. Callers must treat its
// slices as read-only after publication.
type Snapshot struct {
	Packs []Pack
}

// Pack returns the requested pack or nil.
func (s *Snapshot) Pack(pack, languageCode string) *Pack {
	if s == nil {
		return nil
	}
	for i := range s.Packs {
		if s.Packs[i].Pack == pack && s.Packs[i].LanguageCode == languageCode {
			return &s.Packs[i]
		}
	}
	return nil
}

// Select returns current values for keys in request order. Missing and
// deleted keys are omitted, matching langpack.getStrings.
func (p Pack) Select(keys []string) []Entry {
	byKey := make(map[string]Entry, len(p.Entries))
	for _, entry := range p.Entries {
		byKey[entry.Key] = entry
	}
	result := make([]Entry, 0, len(keys))
	for _, key := range keys {
		if entry, ok := byKey[key]; ok {
			result = append(result, entry)
		}
	}
	return result
}

// Difference returns the latest effective state for every key changed after
// fromVersion. If the requested version predates retained history, it returns
// a full current pack. A future request version is echoed without moving
// backwards to the snapshot's current version.
func (p Pack) Difference(fromVersion int64) Difference {
	if fromVersion >= p.Version {
		return Difference{FromVersion: fromVersion, Version: fromVersion}
	}
	if p.OldestVersion > 0 && fromVersion < p.OldestVersion-1 {
		return Difference{FromVersion: fromVersion, Version: p.Version, Entries: cloneEntries(p.Entries)}
	}
	latest := make(map[string]Change)
	for _, change := range p.Changes {
		if change.Version <= fromVersion {
			continue
		}
		previous, ok := latest[change.Entry.Key]
		if !ok || change.Version > previous.Version {
			latest[change.Entry.Key] = change
		}
	}
	entries := make([]Entry, 0, len(latest))
	for _, change := range latest {
		entry := change.Entry
		entry.Deleted = change.Deleted
		if change.Deleted {
			entry.Value = ""
			entry.Plural = nil
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return Difference{FromVersion: fromVersion, Version: p.Version, Entries: entries}
}

// Difference is the wire-independent form used by the Langpack handlers.
type Difference struct {
	FromVersion int64
	Version     int64
	Entries     []Entry
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	copy(result, entries)
	for i := range result {
		if entries[i].Plural != nil {
			plural := *entries[i].Plural
			result[i].Plural = &plural
		}
	}
	return result
}
