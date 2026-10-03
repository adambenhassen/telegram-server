package api_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/teagramhq/teagram-server/internal/api"
)

func TestGetConfigSuggestionOnlyChangesPerResponseCopy(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	getConfig, template := api.GetConfigSeqWithSystemLangCodeForTest(7, "127.0.0.1", 443, func() time.Time { return now })
	cfg, err := getConfig("en-US")
	if err != nil {
		t.Fatalf("help.getConfig: %v", err)
	}
	if cfg.SuggestedLangCode != "en" {
		t.Errorf("suggested_lang_code = %q, want English fallback", cfg.SuggestedLangCode)
	}
	if cfg.ThisDC != 7 || cfg.Date != int(now.Unix()) || cfg.Expires != int(now.Add(api.ConfigTTL).Unix()) {
		t.Errorf("help.getConfig time/DC fields changed: dc=%d date=%d expires=%d", cfg.ThisDC, cfg.Date, cfg.Expires)
	}
	if cfg.LangPackVersion != 12 || cfg.BaseLangPackVersion != 13 {
		t.Errorf("help.getConfig language versions = %d/%d, want unchanged 12/13", cfg.LangPackVersion, cfg.BaseLangPackVersion)
	}
	shared := template()
	if shared.SuggestedLangCode != "shared" || shared.LangPackVersion != 12 || shared.BaseLangPackVersion != 13 || shared.ThisDC != 7 {
		t.Errorf("shared help.getConfig template mutated: %+v", shared)
	}
}

func TestGetConfigConcurrentSuggestionsDoNotMutateTemplate(t *testing.T) {
	t.Parallel()

	getConfig, template := api.GetConfigSeqWithSystemLangCodeForTest(7, "127.0.0.1", 443, func() time.Time { return time.Unix(1_800_000_000, 0) })
	const requestCount = 20
	hints := []string{"en", "en-US", "en_GB", "de-DE", "unsupported"}
	var wg sync.WaitGroup
	errs := make(chan error, requestCount)
	for i := range requestCount {
		wg.Add(1)
		go func(hint string) {
			defer wg.Done()
			cfg, err := getConfig(hint)
			if err != nil {
				errs <- err
				return
			}
			if cfg.SuggestedLangCode != "en" {
				errs <- fmt.Errorf("suggested_lang_code for %q = %q, want English fallback", hint, cfg.SuggestedLangCode)
			}
		}(hints[i%len(hints)])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	shared := template()
	if shared.SuggestedLangCode != "shared" || shared.LangPackVersion != 12 || shared.BaseLangPackVersion != 13 {
		t.Errorf("shared help.getConfig template changed after concurrent requests: %+v", shared)
	}
}
