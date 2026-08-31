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
	"github.com/adambenhassen/telegram-server/internal/mtproto"
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
	// Rate limit defaults.
	if cfg.RateLimits.MessageSend.Limit != 60 {
		t.Errorf("MessageSend limit = %d, want 60", cfg.RateLimits.MessageSend.Limit)
	}
	if cfg.RateLimits.MessageSend.Window != 60*time.Second {
		t.Errorf("MessageSend window = %v, want 60s", cfg.RateLimits.MessageSend.Window)
	}
	if cfg.RateLimits.CreateChat.Limit != 20 {
		t.Errorf("CreateChat limit = %d, want 20", cfg.RateLimits.CreateChat.Limit)
	}
	if cfg.RateLimits.CreateChat.Window != 24*time.Hour {
		t.Errorf("CreateChat window = %v, want 24h", cfg.RateLimits.CreateChat.Window)
	}
	if cfg.RateLimits.AddChatUser.Limit != 120 {
		t.Errorf("AddChatUser limit = %d, want 120", cfg.RateLimits.AddChatUser.Limit)
	}
	if cfg.RateLimits.AddChatUser.Window != 24*time.Hour {
		t.Errorf("AddChatUser window = %v, want 24h", cfg.RateLimits.AddChatUser.Window)
	}
	if cfg.RateLimits.CreateChannel.Limit != 20 {
		t.Errorf("CreateChannel limit = %d, want 20", cfg.RateLimits.CreateChannel.Limit)
	}
	if cfg.RateLimits.CreateChannel.Window != 24*time.Hour {
		t.Errorf("CreateChannel window = %v, want 24h", cfg.RateLimits.CreateChannel.Window)
	}
	if cfg.RateLimits.SearchMessages.Limit != 300 {
		t.Errorf("SearchMessages limit = %d, want 300", cfg.RateLimits.SearchMessages.Limit)
	}
	if cfg.RateLimits.SearchMessages.Window != time.Hour {
		t.Errorf("SearchMessages window = %v, want 1h", cfg.RateLimits.SearchMessages.Window)
	}
	if cfg.RateLimits.SearchContacts.Limit != 300 {
		t.Errorf("SearchContacts limit = %d, want 300", cfg.RateLimits.SearchContacts.Limit)
	}
	if cfg.RateLimits.SearchContacts.Window != time.Hour {
		t.Errorf("SearchContacts window = %v, want 1h", cfg.RateLimits.SearchContacts.Window)
	}
	if cfg.RateLimits.SearchGlobal.Limit != 300 {
		t.Errorf("SearchGlobal limit = %d, want 300", cfg.RateLimits.SearchGlobal.Limit)
	}
	if cfg.RateLimits.SearchGlobal.Window != time.Hour {
		t.Errorf("SearchGlobal window = %v, want 1h", cfg.RateLimits.SearchGlobal.Window)
	}
	if cfg.RateLimits.SaveFilePart.Limit != 600 {
		t.Errorf("SaveFilePart limit = %d, want 600", cfg.RateLimits.SaveFilePart.Limit)
	}
	if cfg.RateLimits.SaveFilePart.Window != 60*time.Second {
		t.Errorf("SaveFilePart window = %v, want 60s", cfg.RateLimits.SaveFilePart.Window)
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

// TestLoadPreAuthLimits covers the three bounds on an unauthenticated
// connection, and the one thing about them that is not like the other tunables:
// zero is a real setting here, and it turns a bound off. So a value that is not
// a number, and a negative one, have to fail the start by name — a bound quietly
// reading as "off" because of a typo is the outcome the bounds exist to prevent.
func TestLoadPreAuthLimits(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	defaults := mtproto.DefaultPreAuthLimits()
	tests := map[string]struct {
		env     string
		raw     string
		want    mtproto.PreAuthLimits
		wantErr bool
	}{
		"defaults":            {want: defaults},
		"conns override":      {env: "TG_MAX_PREAUTH_CONNS", raw: "16", want: withMaxConns(defaults, 16)},
		"conns off":           {env: "TG_MAX_PREAUTH_CONNS", raw: "0", want: withMaxConns(defaults, 0)},
		"conns not integer":   {env: "TG_MAX_PREAUTH_CONNS", raw: "many", wantErr: true},
		"conns negative":      {env: "TG_MAX_PREAUTH_CONNS", raw: "-1", wantErr: true},
		"per ip override":     {env: "TG_MAX_PREAUTH_CONNS_PER_IP", raw: "4", want: withMaxPerNet(defaults, 4)},
		"per ip off":          {env: "TG_MAX_PREAUTH_CONNS_PER_IP", raw: "0", want: withMaxPerNet(defaults, 0)},
		"per ip not integer":  {env: "TG_MAX_PREAUTH_CONNS_PER_IP", raw: "lots", wantErr: true},
		"per ip negative":     {env: "TG_MAX_PREAUTH_CONNS_PER_IP", raw: "-1", wantErr: true},
		"lifetime override":   {env: "TG_PREAUTH_LIFETIME", raw: "45s", want: withLifetime(defaults, 45*time.Second)},
		"lifetime off":        {env: "TG_PREAUTH_LIFETIME", raw: "0s", want: withLifetime(defaults, 0)},
		"lifetime not durat.": {env: "TG_PREAUTH_LIFETIME", raw: "soon", wantErr: true},
		"lifetime negative":   {env: "TG_PREAUTH_LIFETIME", raw: "-1m", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv(tc.env, tc.raw)
			}
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s=%q, got PreAuth = %+v", tc.env, tc.raw, cfg.PreAuth)
				}
				if !strings.Contains(err.Error(), tc.env) {
					t.Errorf("error %q does not name %s", err, tc.env)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.PreAuth != tc.want {
				t.Errorf("PreAuth = %+v, want %+v", cfg.PreAuth, tc.want)
			}
		})
	}
}

// TestLoadMaxConnsPerUnboundKey covers the bound that picks up where the
// pre-auth ones stop: what a key nobody has signed in on may hold. Zero is a
// real setting here too and turns it off, so a typo that would read as "off"
// has to fail the start by name.
func TestLoadMaxConnsPerUnboundKey(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	tests := map[string]struct {
		raw     string
		want    int
		wantErr bool
	}{
		"default":     {want: mtproto.DefaultMaxConnsPerUnboundKey},
		"override":    {raw: "3", want: 3},
		"off":         {raw: "0", want: 0},
		"not integer": {raw: "a few", wantErr: true},
		"negative":    {raw: "-1", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.raw != "" {
				t.Setenv("TG_MAX_CONNS_PER_UNBOUND_KEY", tc.raw)
			}
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got MaxConnsPerUnboundKey = %d", tc.raw, cfg.MaxConnsPerUnboundKey)
				}
				if !strings.Contains(err.Error(), "TG_MAX_CONNS_PER_UNBOUND_KEY") {
					t.Errorf("error %q does not name TG_MAX_CONNS_PER_UNBOUND_KEY", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MaxConnsPerUnboundKey != tc.want {
				t.Errorf("MaxConnsPerUnboundKey = %d, want %d", cfg.MaxConnsPerUnboundKey, tc.want)
			}
		})
	}
}

func withMaxConns(l mtproto.PreAuthLimits, n int) mtproto.PreAuthLimits {
	l.MaxConns = n
	return l
}

func withMaxPerNet(l mtproto.PreAuthLimits, n int) mtproto.PreAuthLimits {
	l.MaxConnsPerNet = n
	return l
}

func withLifetime(l mtproto.PreAuthLimits, d time.Duration) mtproto.PreAuthLimits {
	l.Lifetime = d
	return l
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

func TestLoadRateLimitEnv(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	// Override send limit.
	t.Setenv("TG_RATE_LIMIT_SEND", "10")
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.MessageSend.Limit != 10 {
		t.Errorf("MessageSend limit = %d, want 10", cfg.RateLimits.MessageSend.Limit)
	}

	// Override create chat limit.
	t.Setenv("TG_RATE_LIMIT_CREATE_CHAT", "5")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CreateChat.Limit != 5 {
		t.Errorf("CreateChat limit = %d, want 5", cfg.RateLimits.CreateChat.Limit)
	}

	// Override add chat user limit.
	t.Setenv("TG_RATE_LIMIT_ADD_CHAT_USER", "50")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.AddChatUser.Limit != 50 {
		t.Errorf("AddChatUser limit = %d, want 50", cfg.RateLimits.AddChatUser.Limit)
	}

	// Override create channel limit.
	t.Setenv("TG_RATE_LIMIT_CREATE_CHANNEL", "7")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CreateChannel.Limit != 7 {
		t.Errorf("CreateChannel limit = %d, want 7", cfg.RateLimits.CreateChannel.Limit)
	}

	// Override create channel window.
	t.Setenv("TG_RATE_LIMIT_CREATE_CHANNEL_WINDOW", "12h")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CreateChannel.Window != 12*time.Hour {
		t.Errorf("CreateChannel window = %v, want 12h", cfg.RateLimits.CreateChannel.Window)
	}

	// Override search limits and windows. Every surface is asserted with
	// distinct values so a name typo or a value landing on the wrong surface
	// fails here rather than shipping.
	t.Setenv("TG_RATE_LIMIT_SEARCH_MESSAGES", "11")
	t.Setenv("TG_RATE_LIMIT_SEARCH_MESSAGES_WINDOW", "30m")
	t.Setenv("TG_RATE_LIMIT_SEARCH_CONTACTS", "13")
	t.Setenv("TG_RATE_LIMIT_SEARCH_CONTACTS_WINDOW", "45m")
	t.Setenv("TG_RATE_LIMIT_SEARCH_GLOBAL", "19")
	t.Setenv("TG_RATE_LIMIT_SEARCH_GLOBAL_WINDOW", "20m")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.SearchMessages.Limit != 11 {
		t.Errorf("SearchMessages limit = %d, want 11", cfg.RateLimits.SearchMessages.Limit)
	}
	if cfg.RateLimits.SearchMessages.Window != 30*time.Minute {
		t.Errorf("SearchMessages window = %v, want 30m", cfg.RateLimits.SearchMessages.Window)
	}
	if cfg.RateLimits.SearchContacts.Limit != 13 {
		t.Errorf("SearchContacts limit = %d, want 13", cfg.RateLimits.SearchContacts.Limit)
	}
	if cfg.RateLimits.SearchContacts.Window != 45*time.Minute {
		t.Errorf("SearchContacts window = %v, want 45m", cfg.RateLimits.SearchContacts.Window)
	}
	if cfg.RateLimits.SearchGlobal.Limit != 19 {
		t.Errorf("SearchGlobal limit = %d, want 19", cfg.RateLimits.SearchGlobal.Limit)
	}
	if cfg.RateLimits.SearchGlobal.Window != 20*time.Minute {
		t.Errorf("SearchGlobal window = %v, want 20m", cfg.RateLimits.SearchGlobal.Window)
	}

	// Override the upload part limit and window.
	t.Setenv("TG_RATE_LIMIT_SAVE_FILE_PART", "17")
	t.Setenv("TG_RATE_LIMIT_SAVE_FILE_PART_WINDOW", "90s")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.SaveFilePart.Limit != 17 {
		t.Errorf("SaveFilePart limit = %d, want 17", cfg.RateLimits.SaveFilePart.Limit)
	}
	if cfg.RateLimits.SaveFilePart.Window != 90*time.Second {
		t.Errorf("SaveFilePart window = %v, want 90s", cfg.RateLimits.SaveFilePart.Window)
	}

	// Zero disables enforcement.
	t.Setenv("TG_RATE_LIMIT_SEND", "0")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.MessageSend.Limit != 0 {
		t.Errorf("MessageSend limit = %d, want 0 (disabled)", cfg.RateLimits.MessageSend.Limit)
	}

	// Invalid env var.
	t.Setenv("TG_RATE_LIMIT_SEND", "abc")
	_, err = config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error for invalid TG_RATE_LIMIT_SEND")
	}
	if !strings.Contains(err.Error(), "TG_RATE_LIMIT_SEND") {
		t.Errorf("error %q does not name TG_RATE_LIMIT_SEND", err)
	}
}

func TestLoadRegistrationMode(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	tests := map[string]struct {
		raw     string
		want    config.RegistrationMode
		wantErr bool
	}{
		"unset":        {want: config.RegistrationClosed},
		"empty string": {raw: "", want: config.RegistrationClosed},
		"closed":       {raw: "closed", want: config.RegistrationClosed},
		"invite":       {raw: "invite", want: config.RegistrationInvite},
		"open":         {raw: "open", wantErr: true},
		"invalid":      {raw: "unknown", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.raw != "" {
				t.Setenv("TG_REGISTRATION", tc.raw)
			}
			cfg, err := config.Load(discardLog())
			if tc.wantErr {
				if err != nil {
					// Load itself may not error for invalid values — validation
					// happens at ValidateRegistrationMode.
					return
				}
				verr := cfg.ValidateRegistrationMode()
				if verr == nil {
					t.Fatalf("ValidateRegistrationMode: expected error, got nil")
				}
				if !strings.Contains(verr.Error(), "TG_REGISTRATION") {
					t.Errorf("error %q does not name TG_REGISTRATION", verr)
				}
				for _, accepted := range []string{"closed", "invite"} {
					if !strings.Contains(verr.Error(), accepted) {
						t.Errorf("error %q does not name accepted mode %q", verr, accepted)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if verr := cfg.ValidateRegistrationMode(); verr != nil {
				t.Fatalf("ValidateRegistrationMode: %v", verr)
			}
			if cfg.RegistrationMode != tc.want {
				t.Errorf("RegistrationMode = %q, want %q", cfg.RegistrationMode, tc.want)
			}
		})
	}
}

func TestLoadNewRateLimits(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)

	// Defaults.
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CheckPassword.Limit != 5 {
		t.Errorf("CheckPassword limit = %d, want 5", cfg.RateLimits.CheckPassword.Limit)
	}
	if cfg.RateLimits.CheckPassword.Window != 10*time.Minute {
		t.Errorf("CheckPassword window = %v, want 10m", cfg.RateLimits.CheckPassword.Window)
	}
	if cfg.RateLimits.CheckPasswordIP.Limit != 10 {
		t.Errorf("CheckPasswordIP limit = %d, want 10", cfg.RateLimits.CheckPasswordIP.Limit)
	}
	if cfg.RateLimits.CheckPasswordIP.Window != time.Hour {
		t.Errorf("CheckPasswordIP window = %v, want 1h", cfg.RateLimits.CheckPasswordIP.Window)
	}
	if cfg.RateLimits.GetPasswordIP.Limit != 20 {
		t.Errorf("GetPasswordIP limit = %d, want 20", cfg.RateLimits.GetPasswordIP.Limit)
	}
	if cfg.RateLimits.GetPasswordIP.Window != time.Hour {
		t.Errorf("GetPasswordIP window = %v, want 1h", cfg.RateLimits.GetPasswordIP.Window)
	}
	if cfg.RateLimits.SignUpIP.Limit != 5 {
		t.Errorf("SignUpIP limit = %d, want 5", cfg.RateLimits.SignUpIP.Limit)
	}
	if cfg.RateLimits.SignUpIP.Window != time.Hour {
		t.Errorf("SignUpIP window = %v, want 1h", cfg.RateLimits.SignUpIP.Window)
	}

	// Override check_password.
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD", "3")
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD_WINDOW", "5m")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CheckPassword.Limit != 3 {
		t.Errorf("CheckPassword limit = %d, want 3", cfg.RateLimits.CheckPassword.Limit)
	}
	if cfg.RateLimits.CheckPassword.Window != 5*time.Minute {
		t.Errorf("CheckPassword window = %v, want 5m", cfg.RateLimits.CheckPassword.Window)
	}

	// Override check_password_ip.
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD_IP", "7")
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD_IP_WINDOW", "30m")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CheckPasswordIP.Limit != 7 {
		t.Errorf("CheckPasswordIP limit = %d, want 7", cfg.RateLimits.CheckPasswordIP.Limit)
	}
	if cfg.RateLimits.CheckPasswordIP.Window != 30*time.Minute {
		t.Errorf("CheckPasswordIP window = %v, want 30m", cfg.RateLimits.CheckPasswordIP.Window)
	}

	// Override get_password_ip.
	t.Setenv("TG_RATE_LIMIT_GET_PASSWORD_IP", "15")
	t.Setenv("TG_RATE_LIMIT_GET_PASSWORD_IP_WINDOW", "2h")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.GetPasswordIP.Limit != 15 {
		t.Errorf("GetPasswordIP limit = %d, want 15", cfg.RateLimits.GetPasswordIP.Limit)
	}
	if cfg.RateLimits.GetPasswordIP.Window != 2*time.Hour {
		t.Errorf("GetPasswordIP window = %v, want 2h", cfg.RateLimits.GetPasswordIP.Window)
	}

	// Override sign_up_ip.
	t.Setenv("TG_RATE_LIMIT_SIGN_UP_IP", "3")
	t.Setenv("TG_RATE_LIMIT_SIGN_UP_IP_WINDOW", "12h")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.SignUpIP.Limit != 3 {
		t.Errorf("SignUpIP limit = %d, want 3", cfg.RateLimits.SignUpIP.Limit)
	}
	if cfg.RateLimits.SignUpIP.Window != 12*time.Hour {
		t.Errorf("SignUpIP window = %v, want 12h", cfg.RateLimits.SignUpIP.Window)
	}

	// Zero disables.
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD", "0")
	cfg, err = config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimits.CheckPassword.Limit != 0 {
		t.Errorf("CheckPassword limit = %d, want 0 (disabled)", cfg.RateLimits.CheckPassword.Limit)
	}

	// Invalid value.
	t.Setenv("TG_RATE_LIMIT_CHECK_PASSWORD", "abc")
	_, err = config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error for invalid TG_RATE_LIMIT_CHECK_PASSWORD")
	}
	if !strings.Contains(err.Error(), "TG_RATE_LIMIT_CHECK_PASSWORD") {
		t.Errorf("error %q does not name TG_RATE_LIMIT_CHECK_PASSWORD", err)
	}
}

func TestLoad_Bootstrap_NoPassword(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_BOOTSTRAP_USERNAME", "operator")
	// No password source set.
	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error when bootstrap username is set but no password source")
	}
	if !strings.Contains(err.Error(), "TG_BOOTSTRAP_PASSWORD") {
		t.Errorf("error %q does not name expected vars", err)
	}
}

func TestLoad_Bootstrap_BothPasswords(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_BOOTSTRAP_USERNAME", "operator")
	t.Setenv("TG_BOOTSTRAP_PASSWORD", "secret")
	t.Setenv("TG_BOOTSTRAP_PASSWORD_FILE", "/tmp/pw")
	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("expected error when both password sources are set")
	}
	if !strings.Contains(err.Error(), "both set") {
		t.Errorf("error %q does not mention both set", err)
	}
}

func TestLoad_Bootstrap_PasswordEnv(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_BOOTSTRAP_USERNAME", "operator")
	t.Setenv("TG_BOOTSTRAP_PASSWORD", "secret")
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BootstrapUsername != "operator" {
		t.Errorf("BootstrapUsername = %q, want operator", cfg.BootstrapUsername)
	}
	if cfg.BootstrapPassword != "secret" {
		t.Errorf("BootstrapPassword = %q, want secret", cfg.BootstrapPassword)
	}
}

func TestLoad_Bootstrap_PasswordFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(tmp, []byte("file-secret"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	t.Setenv("TG_BOOTSTRAP_USERNAME", "operator")
	t.Setenv("TG_BOOTSTRAP_PASSWORD_FILE", tmp)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BootstrapUsername != "operator" {
		t.Errorf("BootstrapUsername = %q, want operator", cfg.BootstrapUsername)
	}
	if cfg.BootstrapPasswordFile != tmp {
		t.Errorf("BootstrapPasswordFile = %q, want %q", cfg.BootstrapPasswordFile, tmp)
	}
}

func TestLoad_Bootstrap_Disabled(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	// No bootstrap vars set.
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BootstrapUsername != "" {
		t.Errorf("BootstrapUsername = %q, want empty", cfg.BootstrapUsername)
	}
}

// The media erasure report's two knobs. The cutoff is a duration like any
// other. The interval defaults to zero, which is off — the report's scan is
// only affordable on a small media corpus — so the case that matters most is
// that a typo fails the start by name instead of reading as the default.
func TestLoadMediaErasureReport(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		minAge       string
		interval     string
		wantMinAge   time.Duration
		wantInterval time.Duration
		wantErrVar   string
	}{
		"unset":             {wantMinAge: 24 * time.Hour, wantInterval: 0},
		"override both":     {minAge: "72h", interval: "15m", wantMinAge: 72 * time.Hour, wantInterval: 15 * time.Minute},
		"report off":        {interval: "0s", wantMinAge: 24 * time.Hour, wantInterval: 0},
		"age not duration":  {minAge: "soon", wantErrVar: "TG_MEDIA_ERASURE_MIN_AGE"},
		"age zero":          {minAge: "0s", wantErrVar: "TG_MEDIA_ERASURE_MIN_AGE"},
		"age negative":      {minAge: "-1h", wantErrVar: "TG_MEDIA_ERASURE_MIN_AGE"},
		"interval typo":     {interval: "hourly", wantErrVar: "TG_MEDIA_ERASURE_REPORT_INTERVAL"},
		"interval negative": {interval: "-1h", wantErrVar: "TG_MEDIA_ERASURE_REPORT_INTERVAL"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_MEDIA_ERASURE_MIN_AGE", tc.minAge)
			t.Setenv("TG_MEDIA_ERASURE_REPORT_INTERVAL", tc.interval)
			cfg, err := config.Load(discardLog())
			if tc.wantErrVar != "" {
				if err == nil {
					t.Fatalf("expected error for %s, got min age %v interval %v",
						name, cfg.MediaErasureMinAge, cfg.MediaErasureReportInterval)
				}
				if !strings.Contains(err.Error(), tc.wantErrVar) {
					t.Errorf("error %q does not name %s", err, tc.wantErrVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MediaErasureMinAge != tc.wantMinAge {
				t.Errorf("MediaErasureMinAge = %v, want %v", cfg.MediaErasureMinAge, tc.wantMinAge)
			}
			if cfg.MediaErasureReportInterval != tc.wantInterval {
				t.Errorf("MediaErasureReportInterval = %v, want %v", cfg.MediaErasureReportInterval, tc.wantInterval)
			}
		})
	}
}

// The blob disk report's two knobs, which behave like the media erasure
// report's: a duration cutoff that must be positive, and an interval that
// defaults to zero, meaning off. The case that matters most is that a typo
// fails the start by name rather than reading as the default and leaving an
// operator believing a report is running.
func TestLoadBlobScanReport(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		tempMinAge   string
		interval     string
		wantMinAge   time.Duration
		wantInterval time.Duration
		wantErrVar   string
	}{
		"unset":             {wantMinAge: 24 * time.Hour, wantInterval: 0},
		"override both":     {tempMinAge: "48h", interval: "30m", wantMinAge: 48 * time.Hour, wantInterval: 30 * time.Minute},
		"report off":        {interval: "0s", wantMinAge: 24 * time.Hour, wantInterval: 0},
		"age not duration":  {tempMinAge: "a while", wantErrVar: "TG_BLOB_SCAN_TEMP_MIN_AGE"},
		"age zero":          {tempMinAge: "0s", wantErrVar: "TG_BLOB_SCAN_TEMP_MIN_AGE"},
		"age negative":      {tempMinAge: "-1h", wantErrVar: "TG_BLOB_SCAN_TEMP_MIN_AGE"},
		"interval typo":     {interval: "nightly", wantErrVar: "TG_BLOB_SCAN_REPORT_INTERVAL"},
		"interval negative": {interval: "-1h", wantErrVar: "TG_BLOB_SCAN_REPORT_INTERVAL"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_BLOB_SCAN_TEMP_MIN_AGE", tc.tempMinAge)
			t.Setenv("TG_BLOB_SCAN_REPORT_INTERVAL", tc.interval)
			cfg, err := config.Load(discardLog())
			if tc.wantErrVar != "" {
				if err == nil {
					t.Fatalf("expected error for %s, got temp min age %v interval %v",
						name, cfg.BlobScanTempMinAge, cfg.BlobScanReportInterval)
				}
				if !strings.Contains(err.Error(), tc.wantErrVar) {
					t.Errorf("error %q does not name %s", err, tc.wantErrVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.BlobScanTempMinAge != tc.wantMinAge {
				t.Errorf("BlobScanTempMinAge = %v, want %v", cfg.BlobScanTempMinAge, tc.wantMinAge)
			}
			if cfg.BlobScanReportInterval != tc.wantInterval {
				t.Errorf("BlobScanReportInterval = %v, want %v", cfg.BlobScanReportInterval, tc.wantInterval)
			}
		})
	}
}

// The erasure sweep's knobs. Two properties matter more than the parsing: the
// sweep is off and non-destructive with nothing set, and a fixed period is
// unconfigurable — a range with one end missing, or with both ends equal, is
// refused by name rather than quietly becoming a tick. The tick is what times
// another account's deletion to the second for a watching uploader.
func TestLoadMediaErasureSweep(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	tests := map[string]struct {
		min, max, destructive string
		wantMin, wantMax      time.Duration
		wantDestructive       bool
		wantErrVar            string
	}{
		"unset":       {},
		"range set":   {min: "30m", max: "90m", wantMin: 30 * time.Minute, wantMax: 90 * time.Minute},
		"destructive": {min: "30m", max: "90m", destructive: "true", wantMin: 30 * time.Minute, wantMax: 90 * time.Minute, wantDestructive: true},
		// Destruction asked for with nothing to run it is refused rather than
		// accepted and inert: an operator who set it is watching for the disk to
		// come back, and silence from a sweep that was never scheduled reads
		// exactly like a sweep that found nothing.
		"destructive alone":     {destructive: "true", wantErrVar: "TG_MEDIA_ERASURE_DESTRUCTIVE"},
		"destructive off alone": {destructive: "false"},
		"min only":              {min: "30m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MAX"},
		"max only":              {max: "90m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MIN"},
		"equal ends":            {min: "30m", max: "30m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MAX"},
		"inverted ends":         {min: "90m", max: "30m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MAX"},
		"min not duration":      {min: "often", max: "90m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MIN"},
		"max not duration":      {min: "30m", max: "often", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MAX"},
		"min negative":          {min: "-1h", max: "90m", wantErrVar: "TG_MEDIA_ERASURE_INTERVAL_MIN"},
		"destructive typo":      {destructive: "yes please", wantErrVar: "TG_MEDIA_ERASURE_DESTRUCTIVE"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TG_MEDIA_ERASURE_INTERVAL_MIN", tc.min)
			t.Setenv("TG_MEDIA_ERASURE_INTERVAL_MAX", tc.max)
			t.Setenv("TG_MEDIA_ERASURE_DESTRUCTIVE", tc.destructive)
			cfg, err := config.Load(discardLog())
			if tc.wantErrVar != "" {
				if err == nil {
					t.Fatalf("expected error for %s, got min %v max %v destructive %v",
						name, cfg.MediaErasureIntervalMin, cfg.MediaErasureIntervalMax, cfg.MediaErasureDestructive)
				}
				if !strings.Contains(err.Error(), tc.wantErrVar) {
					t.Errorf("error %q does not name %s", err, tc.wantErrVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.MediaErasureIntervalMin != tc.wantMin || cfg.MediaErasureIntervalMax != tc.wantMax {
				t.Errorf("interval range = [%v, %v], want [%v, %v]",
					cfg.MediaErasureIntervalMin, cfg.MediaErasureIntervalMax, tc.wantMin, tc.wantMax)
			}
			if cfg.MediaErasureDestructive != tc.wantDestructive {
				t.Errorf("MediaErasureDestructive = %v, want %v", cfg.MediaErasureDestructive, tc.wantDestructive)
			}
		})
	}
}

// The not-stored age cutoff defaults to a 24-hour floor while MAIN-338 remains
// open. The row interlock makes a live assembly safe even below that value; the
// floor is a deliberate crash-retention default, not the safety control.
func TestMediaErasureMinAgeDefaultsToAtLeastADay(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MediaErasureMinAge < 24*time.Hour {
		t.Errorf("MediaErasureMinAge default = %v, want at least 24h", cfg.MediaErasureMinAge)
	}
}
