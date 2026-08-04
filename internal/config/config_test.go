package config_test

import (
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
)

// validEncKey is 64 hex chars = 32 bytes, the required master-key length.
const validEncKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestLoadDefaults(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":2443" {
		t.Errorf("ListenAddr = %q, want :2443", cfg.ListenAddr)
	}
	if cfg.DCID != 2 {
		t.Errorf("DCID = %d, want 2", cfg.DCID)
	}
	if cfg.PostgresDSN != "postgres://localhost/tg" {
		t.Errorf("PostgresDSN = %q", cfg.PostgresDSN)
	}
	if len(cfg.AuthKeyEncKey) != 32 {
		t.Errorf("AuthKeyEncKey len = %d, want 32", len(cfg.AuthKeyEncKey))
	}
	if cfg.MaxFileBytes != 104857600 {
		t.Errorf("MaxFileBytes = %d, want 104857600", cfg.MaxFileBytes)
	}
	if cfg.UploadPartTTL != 6*time.Hour {
		t.Errorf("UploadPartTTL = %v, want 6h", cfg.UploadPartTTL)
	}
	if cfg.BlobDir != "blobs" {
		t.Errorf("BlobDir = %q, want blobs", cfg.BlobDir)
	}
	if cfg.MaxUserStorageBytes != 2<<30 {
		t.Errorf("MaxUserStorageBytes = %d, want %d", cfg.MaxUserStorageBytes, int64(2<<30))
	}
}

func TestLoadBlobDir(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_BLOB_DIR", "/var/lib/telegramd/blobs")
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BlobDir != "/var/lib/telegramd/blobs" {
		t.Errorf("BlobDir = %q", cfg.BlobDir)
	}
}

func TestLoadMaxUserStorageBytes(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    int64
		wantErr bool
	}{
		"unset":       {raw: "", want: 2 << 30},
		"override":    {raw: "1048576", want: 1048576},
		"not integer": {raw: "2GB", wantErr: true},
		"zero":        {raw: "0", wantErr: true},
		"negative":    {raw: "-1", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_MAX_USER_STORAGE_BYTES", tc.raw)
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Load: expected error, got nil")
				}
				if !strings.Contains(err.Error(), "TG_MAX_USER_STORAGE_BYTES") {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MaxUserStorageBytes != tc.want {
				t.Errorf("MaxUserStorageBytes = %d, want %d", cfg.MaxUserStorageBytes, tc.want)
			}
		})
	}
}

func TestLoadMaxFileBytes(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    int64
		wantErr bool
	}{
		"unset":       {raw: "", want: 104857600},
		"override":    {raw: "2097152", want: 2097152},
		"at limit":    {raw: "1099511627776", want: 1 << 40},
		"past limit":  {raw: "1099511627777", wantErr: true},
		"max int64":   {raw: "9223372036854775807", wantErr: true},
		"not integer": {raw: "10MB", wantErr: true},
		"zero":        {raw: "0", wantErr: true},
		"negative":    {raw: "-1", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_MAX_FILE_BYTES", tc.raw)
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got MaxFileBytes = %d", name, cfg.MaxFileBytes)
				}
				if !strings.Contains(err.Error(), "TG_MAX_FILE_BYTES") {
					t.Errorf("error %q does not name TG_MAX_FILE_BYTES", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MaxFileBytes != tc.want {
				t.Errorf("MaxFileBytes = %d, want %d", cfg.MaxFileBytes, tc.want)
			}
		})
	}
}

func TestLoadUploadPartTTL(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		"unset":        {raw: "", want: 6 * time.Hour},
		"override":     {raw: "30m", want: 30 * time.Minute},
		"not duration": {raw: "soon", wantErr: true},
		"zero":         {raw: "0s", wantErr: true},
		"negative":     {raw: "-1h", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_UPLOAD_PART_TTL", tc.raw)
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got UploadPartTTL = %v", name, cfg.UploadPartTTL)
				}
				if !strings.Contains(err.Error(), "TG_UPLOAD_PART_TTL") {
					t.Errorf("error %q does not name TG_UPLOAD_PART_TTL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.UploadPartTTL != tc.want {
				t.Errorf("UploadPartTTL = %v, want %v", cfg.UploadPartTTL, tc.want)
			}
		})
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	if _, err := config.Load(discardLog()); err == nil {
		t.Fatal("expected error when DSN missing")
	}
}

func TestLoadLogLoginCodes(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		raw     string
		want    bool
		wantErr bool
	}{
		"unset":       {raw: "", want: false},
		"true":        {raw: "true", want: true},
		"false":       {raw: "false", want: false},
		"not boolean": {raw: "maybe", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_LOG_LOGIN_CODES", tc.raw)
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got LogLoginCodes = %v", name, cfg.LogLoginCodes)
				}
				// The error must name the variable, so a typo is diagnosable
				// from the startup log alone.
				if !strings.Contains(err.Error(), "TG_LOG_LOGIN_CODES") {
					t.Errorf("error %q does not name TG_LOG_LOGIN_CODES", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.LogLoginCodes != tc.want {
				t.Errorf("LogLoginCodes = %v, want %v", cfg.LogLoginCodes, tc.want)
			}
		})
	}
}

func TestLoadAdvertiseAddr(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		listen    string
		advertise string
		wantHost  string
		wantPort  int
		wantErr   bool
	}{
		"derived from default listen addr": {wantHost: "127.0.0.1", wantPort: 2443},
		"derived from wildcard":            {listen: "0.0.0.0:2443", wantHost: "127.0.0.1", wantPort: 2443},
		"derived from ipv6 wildcard":       {listen: "[::]:2443", wantHost: "127.0.0.1", wantPort: 2443},
		"derived from explicit host":       {listen: "192.168.1.5:2443", wantHost: "192.168.1.5", wantPort: 2443},
		"set with derivable listen addr":   {listen: ":2443", advertise: "tg.example.com:2443", wantHost: "tg.example.com", wantPort: 2443},
		"set overrides listen addr":        {listen: "0.0.0.0:2443", advertise: "10.0.0.7:9999", wantHost: "10.0.0.7", wantPort: 9999},
		// Rule 3: an explicit value is used verbatim, wildcard included.
		"set to wildcard is verbatim": {listen: ":2443", advertise: "0.0.0.0:2443", wantHost: "0.0.0.0", wantPort: 2443},
		"not host port":               {advertise: "nope", wantErr: true},
		"port not an integer":         {advertise: "host:abc", wantErr: true},
		"empty host":                  {advertise: ":2443", wantErr: true},
		"port zero":                   {advertise: "host:0", wantErr: true},
		"port negative":               {advertise: "host:-1", wantErr: true},
		"port above range":            {advertise: "host:99999", wantErr: true},
		"highest valid port":          {advertise: "host:65535", wantHost: "host", wantPort: 65535},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_LISTEN_ADDR", tc.listen)
			t.Setenv("TG_ADVERTISE_ADDR", tc.advertise)
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got %s:%d", name, cfg.AdvertiseHost, cfg.AdvertisePort)
				}
				// The error must name the variable, so a typo is diagnosable
				// from the startup log alone.
				if !strings.Contains(err.Error(), "TG_ADVERTISE_ADDR") {
					t.Errorf("error %q does not name TG_ADVERTISE_ADDR", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.AdvertiseHost != tc.wantHost || cfg.AdvertisePort != tc.wantPort {
				t.Errorf("advertise = %s:%d, want %s:%d", cfg.AdvertiseHost, cfg.AdvertisePort, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestLoadEncKey(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	// Cleared explicitly: an inherited key-file path would make the empty case
	// below generate a key and succeed, and the failure would only reproduce on
	// the machine that has the variable set.
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	tests := map[string]string{
		"missing":      "",
		"not hex":      "zzzz",
		"wrong length": strings.Repeat("00", 16), // 16 bytes, want 32
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_AUTHKEY_ENC_KEY", key)
			if _, err := config.Load(discardLog()); err == nil {
				t.Fatalf("expected error for %s enc key", name)
			}
		})
	}
}

// keyFileEnv sets the minimum environment for a key-file load: no env key, a
// file path under t.TempDir().
func keyFileEnv(t *testing.T) string {
	t.Helper()
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	path := filepath.Join(t.TempDir(), "enc_key.hex")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", path)
	return path
}

func TestLoadEncKeyGeneratesFile(t *testing.T) {
	path := keyFileEnv(t)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.AuthKeyEncKey) != 32 {
		t.Fatalf("AuthKeyEncKey len = %d, want 32", len(cfg.AuthKeyEncKey))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("key file mode = %v, want 0600", perm)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if got := hex.EncodeToString(cfg.AuthKeyEncKey); got != string(raw) {
		t.Errorf("file contents %q do not match loaded key %q", raw, got)
	}
}

func TestLoadEncKeyReusesFile(t *testing.T) {
	path := keyFileEnv(t)
	first, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	second, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load (second): %v", err)
	}
	if hex.EncodeToString(first.AuthKeyEncKey) != hex.EncodeToString(second.AuthKeyEncKey) {
		t.Errorf("key changed across loads, sessions would not survive a restart")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat key file: %v", err)
	}
}

func TestLoadEncKeyEnvWins(t *testing.T) {
	path := keyFileEnv(t)
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := hex.EncodeToString(cfg.AuthKeyEncKey); got != validEncKey {
		t.Errorf("AuthKeyEncKey = %q, want the env value", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("key file was written even though the env key was set (err=%v)", err)
	}
}

func TestLoadEncKeyFileInvalid(t *testing.T) {
	for name, contents := range map[string]string{
		"not hex":   strings.Repeat("z", 64),
		"too short": "0011",
		"empty":     "",
	} {
		t.Run(name, func(t *testing.T) {
			path := keyFileEnv(t)
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatalf("write key file: %v", err)
			}
			if _, err := config.Load(discardLog()); err == nil {
				t.Fatalf("Load succeeded with key file %q", contents)
			}
		})
	}
}

func TestLoadEncKeyFileTrailingNewline(t *testing.T) {
	path := keyFileEnv(t)
	if err := os.WriteFile(path, []byte(validEncKey+"\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := hex.EncodeToString(cfg.AuthKeyEncKey); got != validEncKey {
		t.Errorf("AuthKeyEncKey = %q, want %q", got, validEncKey)
	}
}

func TestLoadEncKeyNeitherSet(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	if _, err := config.Load(discardLog()); err == nil {
		t.Fatal("Load succeeded with neither TG_AUTHKEY_ENC_KEY nor TG_AUTHKEY_ENC_KEY_FILE set")
	}
}

// TestLoadEncKeyConcurrentStarts is the replica case: several servers pointed at
// the same key file start at once. Exactly one may create it, and every other
// must end up holding that same key — a start that fails, or one that comes up
// under a different key, loses every session sealed under the winner's.
func TestLoadEncKeyConcurrentStarts(t *testing.T) {
	path := keyFileEnv(t)

	const starts = 64
	keys := make([]string, starts)
	errs := make([]error, starts)
	var wg sync.WaitGroup
	// Released together, so the create and the read of the loser overlap in the
	// window where the winner has created the file but not yet written it.
	start := make(chan struct{})
	for i := range starts {
		wg.Go(func() {
			<-start
			cfg, err := config.Load(discardLog())
			errs[i] = err
			keys[i] = hex.EncodeToString(cfg.AuthKeyEncKey)
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	for i, k := range keys {
		if k != keys[0] {
			t.Fatalf("start %d loaded key %q, start 0 loaded %q", i, k, keys[0])
		}
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if string(onDisk) != keys[0] {
		t.Errorf("file holds %q, servers loaded %q", onDisk, keys[0])
	}

	// Every start but one wrote a temp file it then lost the race to publish.
	// Those hold real master-key material under a name nothing reads, so they
	// must not survive the call that created them.
	leftover, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".enc_key-*"))
	if err != nil {
		t.Fatalf("glob temp key files: %v", err)
	}
	if len(leftover) != 0 {
		t.Errorf("%d temp key files left behind: %v", len(leftover), leftover)
	}
}
