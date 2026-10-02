package api

import (
	"errors"
	"sort"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/catalog"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

const maxLangpackKeys = 1024

type langpackService struct {
	languages  *cachedTL
	packs      map[string]preparedLangPack
	nearest    tg.NearestDC
	prepareErr error
}

type preparedLangPack struct {
	pack  catalog.Pack
	full  *cachedTL
	byKey map[string]catalog.Entry
}

type cachedTL struct {
	bytes []byte
}

func (c *cachedTL) Encode(b *bin.Buffer) error {
	if c == nil {
		return errInternal
	}
	b.Put(c.bytes)
	return nil
}

// newLangpackService prepares the immutable RPC view from one validated
// snapshot. Request handlers use only this view and the configured DC id.
func newLangpackService(snapshot *catalog.Snapshot, dcID int) *langpackService {
	service := &langpackService{
		packs:   make(map[string]preparedLangPack),
		nearest: tg.NearestDC{ThisDC: dcID, NearestDC: dcID},
	}
	if snapshot == nil {
		service.languages, service.prepareErr = cacheTL(&tg.LangPackLanguageVector{Elems: []tg.LangPackLanguage{}})
		return service
	}

	packs := make([]catalog.Pack, 0, len(snapshot.Packs))
	for _, pack := range snapshot.Packs {
		if pack.Pack == catalog.PackTDesktop {
			packs = append(packs, pack)
		}
	}
	sort.Slice(packs, func(i, j int) bool { return packs[i].LanguageCode < packs[j].LanguageCode })

	languages := make([]tg.LangPackLanguage, 0, len(packs))
	for _, pack := range packs {
		byKey := make(map[string]catalog.Entry, len(pack.Entries))
		fullStrings := make([]tg.LangPackStringClass, 0, len(pack.Entries))
		for _, entry := range pack.Entries {
			wire := langpackString(entry)
			byKey[entry.Key] = entry
			fullStrings = append(fullStrings, wire)
		}
		full, err := cacheTL(&tg.LangPackDifference{
			LangCode:    pack.LanguageCode,
			FromVersion: 0,
			Version:     int(pack.Version),
			Strings:     fullStrings,
		})
		if err != nil {
			service.prepareErr = err
			return service
		}
		service.packs[pack.LanguageCode] = preparedLangPack{
			pack:  pack,
			full:  full,
			byKey: byKey,
		}
		languages = append(languages, tg.LangPackLanguage{
			Official:        true,
			Name:            pack.Name,
			NativeName:      pack.NativeName,
			LangCode:        pack.LanguageCode,
			PluralCode:      pack.PluralCode,
			StringsCount:    len(pack.Entries),
			TranslatedCount: len(pack.Entries),
		})
	}
	service.languages, service.prepareErr = cacheTL(&tg.LangPackLanguageVector{Elems: languages})
	return service
}

func cacheTL(encoder bin.Encoder) (*cachedTL, error) {
	var buffer bin.Buffer
	if err := encoder.Encode(&buffer); err != nil {
		return nil, err
	}
	return &cachedTL{bytes: buffer.Copy()}, nil
}

func (s *langpackService) getLanguages(langPack string) (bin.Encoder, error) {
	if err := validateLangPack(langPack); err != nil {
		return nil, err
	}
	if s.prepareErr != nil {
		return nil, errLangPackInvalid
	}
	return s.languages, nil
}

func (s *langpackService) getLangPack(langPack, langCode string) (bin.Encoder, error) {
	pack, err := s.resolve(langPack, langCode)
	if err != nil {
		return nil, err
	}
	return pack.full, nil
}

func (s *langpackService) getStrings(langPack, langCode string, keys []string) (bin.Encoder, error) {
	if len(keys) > maxLangpackKeys {
		return nil, errInputRequestInvalid
	}
	pack, err := s.resolve(langPack, langCode)
	if err != nil {
		return nil, err
	}

	selected := make([]tg.LangPackStringClass, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if len(key) > catalog.MaxKeyBytes {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if value, ok := pack.byKey[key]; ok {
			selected = append(selected, langpackString(value))
		}
	}
	return &tg.LangPackStringClassVector{Elems: selected}, nil
}

func (s *langpackService) getDifference(langPack, langCode string, fromVersion int) (bin.Encoder, error) {
	pack, err := s.resolve(langPack, langCode)
	if err != nil {
		return nil, err
	}
	difference := pack.pack.Difference(int64(fromVersion))
	strings := make([]tg.LangPackStringClass, 0, len(difference.Entries))
	for _, entry := range difference.Entries {
		strings = append(strings, langpackString(entry))
	}
	return &tg.LangPackDifference{
		LangCode:    pack.pack.LanguageCode,
		FromVersion: int(difference.FromVersion),
		Version:     int(difference.Version),
		Strings:     strings,
	}, nil
}

func (s *langpackService) nearestDC() *tg.NearestDC {
	return &s.nearest
}

func (s *langpackService) resolve(langPack, langCode string) (preparedLangPack, error) {
	if s.prepareErr != nil {
		return preparedLangPack{}, errLangPackInvalid
	}
	if err := validateLangPack(langPack); err != nil {
		return preparedLangPack{}, err
	}
	if len(langCode) > 32 {
		return preparedLangPack{}, errLanguageInvalid
	}
	pack, ok := s.packs[langCode]
	if !ok {
		return preparedLangPack{}, errLangCodeNotSupported
	}
	return pack, nil
}

func validateLangPack(langPack string) error {
	if langPack != catalog.PackTDesktop {
		return errLangPackInvalid
	}
	return nil
}

func langpackString(entry catalog.Entry) tg.LangPackStringClass {
	if entry.Deleted {
		return &tg.LangPackStringDeleted{Key: entry.Key}
	}
	if entry.Plural == nil {
		return &tg.LangPackString{Key: entry.Key, Value: entry.Value}
	}
	plural := &tg.LangPackStringPluralized{Key: entry.Key, OtherValue: entry.Plural.Other}
	if entry.Plural.Zero != nil {
		plural.SetZeroValue(*entry.Plural.Zero)
	}
	if entry.Plural.One != nil {
		plural.SetOneValue(*entry.Plural.One)
	}
	if entry.Plural.Two != nil {
		plural.SetTwoValue(*entry.Plural.Two)
	}
	if entry.Plural.Few != nil {
		plural.SetFewValue(*entry.Plural.Few)
	}
	if entry.Plural.Many != nil {
		plural.SetManyValue(*entry.Plural.Many)
	}
	return plural
}

func (h *handlers) handleLangpackGetLanguages(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.LangpackGetLanguagesRequest
	if err := req.Decode(r.Buf); err != nil || r.Buf.Len() != 0 {
		return nil, errInputRequestInvalid
	}
	return h.langpack.getLanguages(req.LangPack)
}

func (h *handlers) handleLangpackGetLangPack(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.LangpackGetLangPackRequest
	if err := req.Decode(r.Buf); err != nil || r.Buf.Len() != 0 {
		return nil, errInputRequestInvalid
	}
	return h.langpack.getLangPack(req.LangPack, req.LangCode)
}

func (h *handlers) handleLangpackGetStrings(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.LangpackGetStringsRequest
	if err := req.Decode(r.Buf); err != nil || r.Buf.Len() != 0 {
		return nil, errInputRequestInvalid
	}
	return h.langpack.getStrings(req.LangPack, req.LangCode, req.Keys)
}

func (h *handlers) handleLangpackGetDifference(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.LangpackGetDifferenceRequest
	if err := req.Decode(r.Buf); err != nil || r.Buf.Len() != 0 {
		return nil, errInputRequestInvalid
	}
	return h.langpack.getDifference(req.LangPack, req.LangCode, req.FromVersion)
}

func (h *handlers) handleHelpGetNearestDC(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.HelpGetNearestDCRequest
	if err := req.Decode(r.Buf); err != nil || r.Buf.Len() != 0 {
		return nil, errInputRequestInvalid
	}
	return h.langpack.nearestDC(), nil
}

func registerLangpack(d *mtproto.Dispatcher, id uint32, name string, fn methodFunc) {
	d.HandleFuncNamed(id, name, func(c *mtproto.Conn, req *mtproto.Request) error {
		if provisionalBlocked(id, req) {
			return c.SendErr(req, errAuthKeyUnreg)
		}
		switch c.ChargeLangpack() {
		case mtproto.UnimplementedClose:
			return errMethodNotImplBurst
		case mtproto.UnimplementedFloodWait:
			return c.SendErr(req, errMethodNotImplFlood)
		}
		res, err := fn(req)
		if err != nil {
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return c.SendErr(req, rpc)
		}
		return c.SendResult(req, res)
	})
}
