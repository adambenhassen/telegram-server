package rsakey_test

import (
	"path/filepath"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/rsakey"
)

func TestLoadOrGenerateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.pem")

	first, err := rsakey.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	second, err := rsakey.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if first.N.Cmp(second.N) != 0 {
		t.Error("key not persisted: modulus differs across loads")
	}
	if rsakey.Fingerprint(&first.PublicKey) == 0 {
		t.Error("fingerprint is zero")
	}
}
