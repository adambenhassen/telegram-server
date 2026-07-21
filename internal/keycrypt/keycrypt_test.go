package keycrypt_test

import (
	"bytes"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
)

func testKey(b byte) []byte { return bytes.Repeat([]byte{b}, keycrypt.KeyLen) }

func TestSealOpenRoundTrip(t *testing.T) {
	c, err := keycrypt.New(testKey(0x2a))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	plaintext := bytes.Repeat([]byte{0x11, 0x22, 0x33, 0x44}, 64) // 256-byte auth key
	blob, err := c.Seal(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(blob, plaintext) {
		t.Fatal("plaintext appears verbatim in sealed blob")
	}
	got, err := c.Open(blob)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatal("round-trip mismatch")
	}
}

func TestSealFreshNonce(t *testing.T) {
	pt := []byte("same plaintext")
	a, c := mustSeal(t, testKey(0x2a), pt)
	b, err := c.Seal(pt)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two seals of identical plaintext produced identical blobs (nonce reuse)")
	}
}

func TestOpenRejectsWrongKey(t *testing.T) {
	blob, _ := mustSeal(t, testKey(0x2a), []byte("secret"))
	other, err := keycrypt.New(testKey(0x99))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := other.Open(blob); err == nil {
		t.Fatal("open with wrong key must fail")
	}
}

func TestOpenRejectsTamper(t *testing.T) {
	blob, c := mustSeal(t, testKey(0x2a), []byte("secret"))
	blob[len(blob)-1] ^= 0xFF // flip a ciphertext/tag bit
	if _, err := c.Open(blob); err == nil {
		t.Fatal("open of tampered blob must fail")
	}
}

func TestNewRejectsBadKeyLen(t *testing.T) {
	if _, err := keycrypt.New(make([]byte, 16)); err == nil {
		t.Fatal("expected error for 16-byte key")
	}
}

func mustSeal(t *testing.T, key, pt []byte) ([]byte, *keycrypt.Cipher) {
	t.Helper()
	c, err := keycrypt.New(key)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	blob, err := c.Seal(pt)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	return blob, c
}
