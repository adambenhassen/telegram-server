// Package config loads server configuration from environment variables.
package config

import (
	"errors"
	"os"
	"strconv"
)

// Config holds server configuration.
type Config struct {
	ListenAddr  string
	PostgresDSN string
	RSAKeyPath  string
	DCID        int
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
	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("TG_POSTGRES_DSN is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
