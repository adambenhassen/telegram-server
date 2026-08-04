// Package config loads server configuration from environment variables.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
)

// Config holds server configuration.
type Config struct {
	ListenAddr  string
	PostgresDSN string
	RSAKeyPath  string
	// AuthKeyEncKey is the 32-byte master key that encrypts auth keys at rest.
	//
	// Changing it is a total re-auth event: no stored auth key opens under a new
	// key, so every client re-handshakes. Peer access hashes are derived from
	// this material too (internal/peerhash) and carry no key epoch, which is
	// only safe while that stays true — before adding any dual-key rotation that
	// lets a session survive a key change, read the key rotation constraint in
	// the internal/peerhash package doc.
	AuthKeyEncKey []byte
	// AdvertiseHost and AdvertisePort are the address clients are told to
	// dial, which is not always the one the server binds: a listener on every
	// interface still has to name one address a client can reach it by.
	AdvertiseHost string
	AdvertisePort int
	DCID          int
	// LogLoginCodes opts into writing issued login codes to the log in
	// cleartext. Off by default: the log is readable by anyone with the
	// process output, and the code alone signs in any account that has no
	// 2FA cloud password — one with a password still needs the SRP step.
	LogLoginCodes bool
	// MaxFileBytes caps one uploaded file. A user's outstanding unassembled
	// bytes are capped at twice this, which is two concurrent max-size uploads.
	MaxFileBytes int64
	// BlobDir is where uploaded file bodies are stored. It must be outside the
	// repository and outside anything a future HTTP surface serves statically.
	BlobDir string
	// MaxUserStorageBytes caps the total size of one account's uploaded files.
	// M5 ships no blob deleter, so nothing decrements this: it is a lifetime
	// quota per account, not a live one, and it is the number that decides
	// whether one account can fill the disk.
	MaxUserStorageBytes int64
	// UploadPartTTL is how long an unassembled upload part is kept before the
	// sweeper deletes it. Short on purpose: a real client uploads and sends
	// within minutes, and the TTL is the term that makes worst-case retained
	// bytes finite at accounts x cap.
	UploadPartTTL time.Duration
}

// MaxFileBytesLimit is the ceiling on TG_MAX_FILE_BYTES. It is a bound on the
// arithmetic, not a product decision: the derived part count rounds up by the
// part size and the per-user cap doubles the per-file one, so a value near
// MaxInt64 overflows both and turns every save into a rejection. 1 TiB is far
// past any file a client can upload and leaves both terms in range.
const MaxFileBytesLimit int64 = 1 << 40

// Load reads configuration from environment variables, applying defaults. The
// logger is used only for the auth-key master key, which is the one value Load
// can create rather than read, and a generated one has to say so.
func Load(log *slog.Logger) (Config, error) {
	cfg := Config{
		ListenAddr:  envOr("TG_LISTEN_ADDR", ":2443"),
		PostgresDSN: os.Getenv("TG_POSTGRES_DSN"),
		RSAKeyPath:  envOr("TG_RSA_KEY_PATH", "server_key.pem"),
		DCID:        2,
		BlobDir:     envOr("TG_BLOB_DIR", "blobs"),

		MaxFileBytes:        100 << 20,
		MaxUserStorageBytes: 2 << 30,
		UploadPartTTL:       6 * time.Hour,
	}
	if v := os.Getenv("TG_DC_ID"); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_DC_ID must be an integer")
		}
		cfg.DCID = id
	}
	if v := os.Getenv("TG_MAX_FILE_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Config{}, errors.New("TG_MAX_FILE_BYTES must be an integer")
		}
		if n <= 0 {
			return Config{}, errors.New("TG_MAX_FILE_BYTES must be positive")
		}
		if n > MaxFileBytesLimit {
			return Config{}, errors.New("TG_MAX_FILE_BYTES must be at most 1099511627776")
		}
		cfg.MaxFileBytes = n
	}
	if v := os.Getenv("TG_MAX_USER_STORAGE_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Config{}, errors.New("TG_MAX_USER_STORAGE_BYTES must be an integer")
		}
		if n <= 0 {
			return Config{}, errors.New("TG_MAX_USER_STORAGE_BYTES must be positive")
		}
		cfg.MaxUserStorageBytes = n
	}
	if v := os.Getenv("TG_UPLOAD_PART_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_UPLOAD_PART_TTL must be a duration")
		}
		if d <= 0 {
			return Config{}, errors.New("TG_UPLOAD_PART_TTL must be positive")
		}
		cfg.UploadPartTTL = d
	}
	if v := os.Getenv("TG_LOG_LOGIN_CODES"); v != "" {
		on, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, errors.New("TG_LOG_LOGIN_CODES must be a boolean")
		}
		cfg.LogLoginCodes = on
	}
	advertiseHost, advertisePort, err := advertiseAddr(os.Getenv("TG_ADVERTISE_ADDR"), cfg.ListenAddr)
	if err != nil {
		return Config{}, err
	}
	cfg.AdvertiseHost, cfg.AdvertisePort = advertiseHost, advertisePort
	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("TG_POSTGRES_DSN is required")
	}
	encKey, err := loadEncKey(log)
	if err != nil {
		return Config{}, err
	}
	cfg.AuthKeyEncKey = encKey
	return cfg, nil
}

// advertiseAddr resolves the address clients are told to dial. An explicit
// TG_ADVERTISE_ADDR is used verbatim and must parse, since a wrong one is only
// visible as clients failing to reconnect; an empty one is derived from the
// listen address, which stays as loosely validated as it was — a bad one fails
// loudly at net.Listen.
func advertiseAddr(advertise, listen string) (string, int, error) {
	if advertise == "" {
		host, port := splitHostPort(listen)
		return host, port, nil
	}
	host, portStr, err := net.SplitHostPort(advertise)
	if err != nil {
		return "", 0, errors.New("TG_ADVERTISE_ADDR must be host:port")
	}
	if host == "" {
		return "", 0, errors.New("TG_ADVERTISE_ADDR must name a host clients can reach")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, errors.New("TG_ADVERTISE_ADDR port must be an integer")
	}
	if port < 1 || port > 65535 {
		return "", 0, errors.New("TG_ADVERTISE_ADDR port must be between 1 and 65535")
	}
	return host, port, nil
}

// splitHostPort derives an advertisable address from a listen address. A host
// that is empty or a wildcard binds every interface but names none, so it
// becomes loopback — the one address that is always reachable.
func splitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 2443
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 2443
	}
	return host, port
}

// loadEncKey resolves the auth-key master key from the two sources that can
// carry it. TG_AUTHKEY_ENC_KEY wins and is never written anywhere. Failing
// that, TG_AUTHKEY_ENC_KEY_FILE names a file the key is read from and, on a
// first boot where it does not exist yet, generated into — which is what lets
// the compose stack start with an unedited .env. With neither set the server
// refuses to boot, exactly as before: auto-generation is opt-in by naming a
// path, never a default baked into the binary.
func loadEncKey(log *slog.Logger) ([]byte, error) {
	if raw := os.Getenv("TG_AUTHKEY_ENC_KEY"); raw != "" {
		return decodeEncKey(raw, "TG_AUTHKEY_ENC_KEY")
	}
	path := os.Getenv("TG_AUTHKEY_ENC_KEY_FILE")
	if path == "" {
		return nil, errors.New("TG_AUTHKEY_ENC_KEY is required (64 hex chars = 32 bytes), or set TG_AUTHKEY_ENC_KEY_FILE to a path the key is kept in")
	}
	key, generated, err := encKeyFromFile(path)
	if err != nil {
		return nil, err
	}
	if generated {
		log.Warn("TG_AUTHKEY_ENC_KEY not set: generated a dev key", "path", path, "action", "do not use in production")
	} else {
		log.Info("auth-key master key loaded from file", "path", path)
	}
	return key, nil
}

// encKeyFromFile reads the master key at path, generating and persisting one
// when the file is absent. The create is O_EXCL rather than a write: two
// containers starting at once must not each write a key and leave one of them
// holding material the database was not sealed under, so the loser of the race
// reads back what the winner wrote instead of clobbering it.
func encKeyFromFile(path string) ([]byte, bool, error) {
	raw, err := os.ReadFile(path) // #nosec G304,G703 -- path is the operator-configured key file, not untrusted input.
	switch {
	case err == nil:
		key, err := decodeEncKey(strings.TrimSpace(string(raw)), path)
		return key, false, err
	case !os.IsNotExist(err):
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}

	buf := make([]byte, keycrypt.KeyLen)
	if _, err := rand.Read(buf); err != nil {
		return nil, false, fmt.Errorf("generate auth-key master key: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304,G703 -- same operator-configured path.
	if err != nil {
		if os.IsExist(err) {
			// Another start won the race between the read above and this
			// create. Its key is the real one.
			raw, err := os.ReadFile(path) // #nosec G304,G703 -- same operator-configured path.
			if err != nil {
				return nil, false, fmt.Errorf("read %s: %w", path, err)
			}
			key, err := decodeEncKey(strings.TrimSpace(string(raw)), path)
			return key, false, err
		}
		return nil, false, fmt.Errorf("create %s: %w", path, err)
	}
	_, writeErr := f.WriteString(hex.EncodeToString(buf))
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return nil, false, fmt.Errorf("write %s: %w", path, err)
	}
	return buf, true, nil
}

// decodeEncKey parses the hex-encoded auth-key encryption master key. It is
// required and must decode to exactly keycrypt.KeyLen bytes, so the server fails
// fast rather than starting without at-rest encryption or with a weak key. src
// names where the value came from — the env var or a file path — because the
// two failure modes are diagnosed in different places.
func decodeEncKey(raw, src string) ([]byte, error) {
	if raw == "" {
		return nil, fmt.Errorf("%s is empty: expected 64 hex chars (32 bytes)", src)
	}
	key, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be valid hex", src)
	}
	if len(key) != keycrypt.KeyLen {
		return nil, fmt.Errorf("%s must be 64 hex chars (32 bytes)", src)
	}
	return key, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
