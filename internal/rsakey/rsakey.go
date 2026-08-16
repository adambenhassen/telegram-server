// Package rsakey loads or generates the server RSA key used in the MTProto
// auth-key exchange.
package rsakey

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gotd/td/crypto"
)

// LoadOrGenerate returns the RSA key at path, generating and persisting a new
// 2048-bit key (0600) when the file does not exist.
func LoadOrGenerate(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is the operator-configured server key file, not untrusted input.
	switch {
	case err == nil:
		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("no PEM block in %s", path)
		}
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse key: %w", err)
		}
		return key, nil
	case errors.Is(err, os.ErrNotExist):
		return generate(path)
	default:
		return nil, fmt.Errorf("read key: %w", err)
	}
}

func generate(path string) (*rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, crypto.RSAKeyBits)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, fmt.Errorf("write key: %w", err)
	}
	return key, nil
}

// Fingerprint returns the Telegram RSA fingerprint of the public key.
func Fingerprint(pub *rsa.PublicKey) int64 {
	return crypto.RSAFingerprint(pub)
}

// KeyID returns the SHA-256 of the DER SubjectPublicKeyInfo encoding of the
// public key, hex-encoded as 16 dash-separated groups of 4 characters
// (e.g. "a1b2c3d4-e5f6a7b8-..."). The grouping keeps the 64-character digest
// comparable by eye against the client UI; MAIN-314 renders the identical
// format.
func KeyID(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("marshal SubjectPublicKeyInfo: %w", err)
	}
	sum := sha256.Sum256(der)
	hexStr := hex.EncodeToString(sum[:])
	groups := make([]string, 0, 16)
	for i := 0; i < len(hexStr); i += 4 {
		groups = append(groups, hexStr[i:i+4])
	}
	return strings.Join(groups, "-"), nil
}
