// Package keycrypt seals MTProto auth-key material for storage at rest using
// AES-256-GCM. Stored blobs are self-describing: a version byte lets the format
// (algorithm, key rotation) evolve without ambiguity when decrypting old rows.
package keycrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

const (
	// KeyLen is the required master-key length: 32 bytes selects AES-256.
	KeyLen = 32
	// version tags the on-disk blob format: version || nonce || ciphertext+tag.
	version = 0x01
	// nonceLen is the GCM standard 96-bit nonce.
	nonceLen = 12
)

// Cipher seals and opens auth-key blobs under a fixed master key.
type Cipher struct {
	aead cipher.AEAD
}

// New builds a Cipher from a 32-byte master key. It fails fast on a wrong-length
// key so a misconfigured server never starts with a weak or truncated key.
func New(key []byte) (*Cipher, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("keycrypt: key must be %d bytes, got %d", KeyLen, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("keycrypt: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("keycrypt: new gcm: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// Seal encrypts plaintext, returning version || nonce || ciphertext+tag. Each
// call draws a fresh random nonce, so identical plaintext yields distinct blobs.
func (c *Cipher) Seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("keycrypt: seal: nonce: %w", err)
	}
	out := make([]byte, 0, 1+nonceLen+len(plaintext)+c.aead.Overhead())
	out = append(out, version)
	out = append(out, nonce...)
	// Seal appends the ciphertext+tag onto out, after the version+nonce prefix.
	return c.aead.Seal(out, nonce, plaintext, nil), nil
}

// Open reverses Seal. It fails on a short blob, an unknown version, or a failed
// authentication tag (wrong key or tampered ciphertext), so a corrupt row never
// decrypts to garbage.
func (c *Cipher) Open(blob []byte) ([]byte, error) {
	if len(blob) < 1+nonceLen {
		return nil, errors.New("keycrypt: open: ciphertext too short")
	}
	if blob[0] != version {
		return nil, fmt.Errorf("keycrypt: open: unknown version %d", blob[0])
	}
	nonce := blob[1 : 1+nonceLen]
	ciphertext := blob[1+nonceLen:]
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("keycrypt: open: %w", err)
	}
	return plaintext, nil
}
