package rsakey_test

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/rsakey"
)

// TestKeyIDIsSHA256OfSubjectPublicKeyInfo pins the identity to the digest of
// the DER SubjectPublicKeyInfo encoding, not of any other key representation.
func TestKeyIDIsSHA256OfSubjectPublicKeyInfo(t *testing.T) {
	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal SubjectPublicKeyInfo: %v", err)
	}
	sum := sha256.Sum256(der)
	want := hex.EncodeToString(sum[:])

	got, err := rsakey.KeyID(&key.PublicKey)
	if err != nil {
		t.Fatalf("KeyID: %v", err)
	}
	if strings.ReplaceAll(got, "-", "") != want {
		t.Errorf("KeyID = %q, want SHA-256 of SubjectPublicKeyInfo %q", got, want)
	}
}

// TestKeyIDGroupedHex pins the human-comparable format: 16 groups of 4 hex
// chars, dash-separated. MAIN-314 renders this exact format on the client.
func TestKeyIDGroupedHex(t *testing.T) {
	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	got, err := rsakey.KeyID(&key.PublicKey)
	if err != nil {
		t.Fatalf("KeyID: %v", err)
	}
	if len(got) != 64+15 {
		t.Fatalf("KeyID length = %d, want 79 (64 hex chars + 15 dashes): %q", len(got), got)
	}
	groups := strings.Split(got, "-")
	if len(groups) != 16 {
		t.Fatalf("KeyID has %d dash-separated groups, want 16: %q", len(groups), got)
	}
	for _, g := range groups {
		if len(g) != 4 {
			t.Fatalf("KeyID group %q is %d chars, want 4", g, len(g))
		}
		if _, err := hex.DecodeString(g); err != nil {
			t.Fatalf("KeyID group %q is not hex: %v", g, err)
		}
	}
}

// TestKeyIDStableAcrossLoads pins that the identity is a pure function of the
// public key: reloading the same file yields the same value.
func TestKeyIDStableAcrossLoads(t *testing.T) {
	path := t.TempDir() + "/key.pem"
	first, err := rsakey.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	second, err := rsakey.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	firstID, err := rsakey.KeyID(&first.PublicKey)
	if err != nil {
		t.Fatalf("KeyID: %v", err)
	}
	secondID, err := rsakey.KeyID(&second.PublicKey)
	if err != nil {
		t.Fatalf("KeyID: %v", err)
	}
	if firstID != secondID {
		t.Error("KeyID differs across loads of the same key")
	}
}
