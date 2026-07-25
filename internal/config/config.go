// Package config loads server configuration from environment variables.
package config

import (
	"encoding/hex"
	"errors"
	"os"
	"strconv"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
)

// Config holds server configuration.
type Config struct {
	ListenAddr  string
	PostgresDSN string
	RSAKeyPath  string
	// AuthKeyEncKey is the 32-byte master key that encrypts auth keys at rest.
	AuthKeyEncKey []byte
	DCID          int
	// LogLoginCodes opts into writing issued login codes to the log in
	// cleartext. Off by default: the log is readable by anyone with the
	// process output, and the code alone signs in any account that has no
	// 2FA cloud password — one with a password still needs the SRP step.
	LogLoginCodes bool
}

// Load reads configuration from environment variables, applying defaults.
func Load() (Config, error) {
	cfg := Config{
		ListenAddr:  envOr("TG_LISTEN_ADDR", ":2443"),
		PostgresDSN: os.Getenv("TG_POSTGRES_DSN"),
		RSAKeyPath:  envOr("TG_RSA_KEY_PATH", "server_key.pem"),
		DCID:        2,
	}
	if v := os.Getenv("TG_DC_ID"); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_DC_ID must be an integer")
		}
		cfg.DCID = id
	}
	if v := os.Getenv("TG_LOG_LOGIN_CODES"); v != "" {
		on, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, errors.New("TG_LOG_LOGIN_CODES must be a boolean")
		}
		cfg.LogLoginCodes = on
	}
	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("TG_POSTGRES_DSN is required")
	}
	encKey, err := decodeEncKey(os.Getenv("TG_AUTHKEY_ENC_KEY"))
	if err != nil {
		return Config{}, err
	}
	cfg.AuthKeyEncKey = encKey
	return cfg, nil
}

// decodeEncKey parses the hex-encoded auth-key encryption master key. It is
// required and must decode to exactly keycrypt.KeyLen bytes, so the server fails
// fast rather than starting without at-rest encryption or with a weak key.
func decodeEncKey(raw string) ([]byte, error) {
	if raw == "" {
		return nil, errors.New("TG_AUTHKEY_ENC_KEY is required (64 hex chars = 32 bytes)")
	}
	key, err := hex.DecodeString(raw)
	if err != nil {
		return nil, errors.New("TG_AUTHKEY_ENC_KEY must be valid hex")
	}
	if len(key) != keycrypt.KeyLen {
		return nil, errors.New("TG_AUTHKEY_ENC_KEY must be 64 hex chars (32 bytes)")
	}
	return key, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
