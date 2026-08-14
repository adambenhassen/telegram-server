// Package config loads server configuration from environment variables.
package config

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// RegistrationMode controls whether new accounts can be created via auth.signUp.
type RegistrationMode string

const (
	RegistrationClosed RegistrationMode = "closed"
	RegistrationOpen   RegistrationMode = "open"
)

// Config holds server configuration.
type Config struct {
	ListenAddr string
	// AdminListenAddr is the address the admin HTTP server binds to.
	// Empty disables the admin server entirely.
	AdminListenAddr string
	// AdminTokenHash is the hex-encoded SHA-256 digest of the operator token.
	// Only set when AdminListenAddr is non-empty.
	AdminTokenHash string
	PostgresDSN    string
	RSAKeyPath     string
	// RegistrationMode controls whether auth.signUp is available.
	RegistrationMode RegistrationMode
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
	// RateLimits holds the per-surface rate-limit configurations. Zero limit
	// disables enforcement for that surface.
	RateLimits RateLimitsConfig
	// ClientAddrTrust names where the address a request is attributed to comes
	// from: the connection's own peer address, or a PROXY protocol v2 header.
	ClientAddrTrust ClientAddrTrust
	// ClientAddrProxies is the allowlist of balancer source addresses a PROXY
	// header is honoured from. It is the whole of the trust decision in
	// ClientAddrProxyV2 mode and is empty in every other mode.
	ClientAddrProxies []netip.Prefix
	// PreAuth bounds what connections that have not authenticated may hold:
	// concurrently in the process, concurrently per client network, and for how
	// long. Zero disables a bound, as it does for a rate limit.
	PreAuth mtproto.PreAuthLimits
	// MaxConnsPerUnboundKey bounds the concurrent connections one auth key with
	// nobody signed in on it may hold. It picks up where PreAuth stops counting
	// — at the first frame that decrypts under a server-issued key — and hands
	// over to the per-user connection cap at sign-in. Zero disables it.
	MaxConnsPerUnboundKey int
	// BootstrapUsername is the handle to create at startup. Empty disables
	// bootstrap. When set, exactly one of BootstrapPassword or
	// BootstrapPasswordFile must be configured.
	BootstrapUsername string
	// BootstrapPassword is the cleartext password for the bootstrap account.
	// Mutually exclusive with BootstrapPasswordFile.
	BootstrapPassword string
	// BootstrapPasswordFile is the path to a file containing the bootstrap
	// password. Mutually exclusive with BootstrapPassword.
	BootstrapPasswordFile string
}

// ClientAddrTrust names the source a client address is taken from.
type ClientAddrTrust string

// ClientAddrSocket attributes every request to the peer address of the socket
// it arrived on. It is the only source that cannot be influenced by the client:
// MTProto carries no address field, and any header a future one gained would be
// forgeable by whoever sends it.
const ClientAddrSocket ClientAddrTrust = "socket"

// ClientAddrProxyV2 attributes every request to the address a PROXY protocol v2
// header names, and only when that header arrives from an address in
// ClientAddrProxies. It is what makes per-IP limits mean anything behind an L4
// load balancer, where every peer address is the balancer's and socket keying
// collapses into one global bucket.
//
// The allowlist is the entire reason the header can be believed: it is
// client-supplied bytes, so a sender outside the allowlist is refused rather
// than credited with an address it chose. There is no third source.
const ClientAddrProxyV2 ClientAddrTrust = "proxy-v2"

// clientAddrTrustValues lists every supported value, for the startup error. An
// operator naming a mode this build does not implement must be told plainly
// rather than silently served socket addresses that all belong to the balancer.
var clientAddrTrustValues = []ClientAddrTrust{ClientAddrSocket, ClientAddrProxyV2}

// RateLimitsConfig holds the rate-limit parameters for each RPC surface.
type RateLimitsConfig struct {
	// MessageSend limits all client-visible message sends (1:1, chat, channel
	// post, media send, forward) to one shared budget per account.
	MessageSend store.RateLimitConfig
	// CreateChat limits messages.createChat per account.
	CreateChat store.RateLimitConfig
	// AddChatUser limits messages.addChatUser per account.
	AddChatUser store.RateLimitConfig
	// CreateChannel limits channels.createChannel per account.
	CreateChannel store.RateLimitConfig
	// SearchMessages limits messages.search per account.
	SearchMessages store.RateLimitConfig
	// SearchContacts limits contacts.search per account.
	SearchContacts store.RateLimitConfig
	// SearchGlobal limits messages.searchGlobal per account, on a budget of its
	// own: a cross-dialog search reads every dialog the caller is in, so one call
	// is not the same unit of work as a search inside a named peer.
	SearchGlobal store.RateLimitConfig
	// SaveFilePart limits upload.saveFilePart and upload.saveBigFilePart per
	// account, on one shared budget: both write the same rows, so a budget each
	// would let an account double its part rate by alternating between them.
	SaveFilePart store.RateLimitConfig
	// SendCodeIP limits auth.sendCode per client network. It is keyed on the
	// connection's address rather than an account because the surface is
	// unauthenticated: there is no account yet to hold a budget.
	SendCodeIP store.SendCodeIPLimits
	// SignInFailIP limits failed auth.signIn attempts per client network.
	// Keyed on the connection's address, not the identifier: only the
	// attacker's own IP budget is consumed, never the victim's.
	SignInFailIP store.RateLimitConfig
	// CheckPassword limits failed auth.checkPassword attempts per account.
	// Charged only on failed SRP proofs; a valid proof is never charged.
	CheckPassword store.RateLimitConfig
	// CheckPasswordIP limits failed auth.checkPassword attempts per client
	// network. Keyed on the connection's address. Charged only on failures.
	CheckPasswordIP store.RateLimitConfig
	// GetPasswordIP limits account.getPassword calls per client network, but
	// only for unauthenticated callers (pending state). A fully authorized
	// caller is not subject to this limit. The per-call 2048-bit modexp is
	// the cost being bounded.
	GetPasswordIP store.RateLimitConfig
	// SignUpIP limits auth.signUp calls per client network. Applied only when
	// TG_REGISTRATION=open; no-op in closed mode.
	SignUpIP store.RateLimitConfig
}

// DefaultRateLimits returns the shipped per-surface defaults: 60 sends per 60s,
// 20 chat creates per 24h, 120 member adds per 24h, 20 channel creates per 24h,
// 300 message searches per hour, 300 contacts searches per hour, 300 global
// searches per hour, 600 upload parts per 60s, per client network 10 sendCode
// calls per hour across at most 20 distinct phone numbers per 24h, 10 failed
// signIn attempts per hour per client network, 5 failed checkPassword attempts
// per 10 min per account, 10 failed checkPassword attempts per hour per client
// network, 20 getPassword calls per hour per client network (unauthenticated
// callers only), and 5 signUp calls per hour per client network.
// Zero disables enforcement for a surface.
//
// The upload number is the one derived rather than chosen: at the 512 KiB
// protocol part size it is roughly 300 MB/min, past any real client's upload
// rate, and it is what bounds how often an account can drive the per-user cap
// to rollover — the write amplification each rejected save still costs.
//
// Exported so a test can drive a surface at the numbers that actually ship
// instead of at ones it invented.
func DefaultRateLimits() RateLimitsConfig {
	return RateLimitsConfig{
		MessageSend:    store.RateLimitConfig{Limit: 60, Window: 60 * time.Second},
		CreateChat:     store.RateLimitConfig{Limit: 20, Window: 24 * time.Hour},
		AddChatUser:    store.RateLimitConfig{Limit: 120, Window: 24 * time.Hour},
		CreateChannel:  store.RateLimitConfig{Limit: 20, Window: 24 * time.Hour},
		SearchMessages: store.RateLimitConfig{Limit: 300, Window: time.Hour},
		SearchContacts: store.RateLimitConfig{Limit: 300, Window: time.Hour},
		SearchGlobal:   store.RateLimitConfig{Limit: 300, Window: time.Hour},
		SaveFilePart:   store.RateLimitConfig{Limit: 600, Window: 60 * time.Second},
		SendCodeIP: store.SendCodeIPLimits{
			Calls:  store.RateLimitConfig{Limit: 10, Window: time.Hour},
			Phones: store.RateLimitConfig{Limit: 20, Window: 24 * time.Hour},
		},
		SignInFailIP:    store.RateLimitConfig{Limit: 10, Window: time.Hour},
		CheckPassword:   store.RateLimitConfig{Limit: 5, Window: 10 * time.Minute},
		CheckPasswordIP: store.RateLimitConfig{Limit: 10, Window: time.Hour},
		GetPasswordIP:   store.RateLimitConfig{Limit: 20, Window: time.Hour},
		SignUpIP:        store.RateLimitConfig{Limit: 5, Window: time.Hour},
	}
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
		ListenAddr:      envOr("TG_LISTEN_ADDR", ":2443"),
		AdminListenAddr: os.Getenv("TG_ADMIN_LISTEN_ADDR"),
		PostgresDSN:     os.Getenv("TG_POSTGRES_DSN"),
		RSAKeyPath:      envOr("TG_RSA_KEY_PATH", "server_key.pem"),
		DCID:            2,
		BlobDir:         envOr("TG_BLOB_DIR", "blobs"),

		MaxFileBytes:        100 << 20,
		MaxUserStorageBytes: 2 << 30,
		UploadPartTTL:       6 * time.Hour,

		MaxConnsPerUnboundKey: mtproto.DefaultMaxConnsPerUnboundKey,

		RegistrationMode: RegistrationClosed,
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
	// TG_REGISTRATION resolves the registration mode.
	cfg.RegistrationMode = registrationMode(os.Getenv("TG_REGISTRATION"))
	// Zero or unset disables enforcement for a surface; the numbers and why they
	// are those numbers are on DefaultRateLimits.
	cfg.RateLimits = DefaultRateLimits()
	if v := os.Getenv("TG_RATE_LIMIT_SEND"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND must be an integer")
		}
		cfg.RateLimits.MessageSend.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEND_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND_WINDOW must be a duration")
		}
		cfg.RateLimits.MessageSend.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_CREATE_CHAT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CREATE_CHAT must be an integer")
		}
		cfg.RateLimits.CreateChat.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_CREATE_CHAT_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CREATE_CHAT_WINDOW must be a duration")
		}
		cfg.RateLimits.CreateChat.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_ADD_CHAT_USER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_ADD_CHAT_USER must be an integer")
		}
		cfg.RateLimits.AddChatUser.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_ADD_CHAT_USER_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_ADD_CHAT_USER_WINDOW must be a duration")
		}
		cfg.RateLimits.AddChatUser.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_CREATE_CHANNEL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CREATE_CHANNEL must be an integer")
		}
		cfg.RateLimits.CreateChannel.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_CREATE_CHANNEL_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CREATE_CHANNEL_WINDOW must be a duration")
		}
		cfg.RateLimits.CreateChannel.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_MESSAGES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_MESSAGES must be an integer")
		}
		cfg.RateLimits.SearchMessages.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_MESSAGES_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_MESSAGES_WINDOW must be a duration")
		}
		cfg.RateLimits.SearchMessages.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_CONTACTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_CONTACTS must be an integer")
		}
		cfg.RateLimits.SearchContacts.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_CONTACTS_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_CONTACTS_WINDOW must be a duration")
		}
		cfg.RateLimits.SearchContacts.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_GLOBAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_GLOBAL must be an integer")
		}
		cfg.RateLimits.SearchGlobal.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEARCH_GLOBAL_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEARCH_GLOBAL_WINDOW must be a duration")
		}
		cfg.RateLimits.SearchGlobal.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SAVE_FILE_PART"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SAVE_FILE_PART must be an integer")
		}
		cfg.RateLimits.SaveFilePart.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SAVE_FILE_PART_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SAVE_FILE_PART_WINDOW must be a duration")
		}
		cfg.RateLimits.SaveFilePart.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEND_CODE_IP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND_CODE_IP must be an integer")
		}
		cfg.RateLimits.SendCodeIP.Calls.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEND_CODE_IP_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND_CODE_IP_WINDOW must be a duration")
		}
		cfg.RateLimits.SendCodeIP.Calls.Window = d
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEND_CODE_IP_PHONES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND_CODE_IP_PHONES must be an integer")
		}
		cfg.RateLimits.SendCodeIP.Phones.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SEND_CODE_IP_PHONES_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SEND_CODE_IP_PHONES_WINDOW must be a duration")
		}
		cfg.RateLimits.SendCodeIP.Phones.Window = d
	}
	// CheckPassword per-account rate limit.
	if v := os.Getenv("TG_RATE_LIMIT_CHECK_PASSWORD"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CHECK_PASSWORD must be an integer")
		}
		cfg.RateLimits.CheckPassword.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_CHECK_PASSWORD_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CHECK_PASSWORD_WINDOW must be a duration")
		}
		cfg.RateLimits.CheckPassword.Window = d
	}
	// CheckPassword per-IP rate limit.
	if v := os.Getenv("TG_RATE_LIMIT_CHECK_PASSWORD_IP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CHECK_PASSWORD_IP must be an integer")
		}
		cfg.RateLimits.CheckPasswordIP.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_CHECK_PASSWORD_IP_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_CHECK_PASSWORD_IP_WINDOW must be a duration")
		}
		cfg.RateLimits.CheckPasswordIP.Window = d
	}
	// GetPassword per-IP rate limit (unauthenticated callers only).
	if v := os.Getenv("TG_RATE_LIMIT_GET_PASSWORD_IP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_GET_PASSWORD_IP must be an integer")
		}
		cfg.RateLimits.GetPasswordIP.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_GET_PASSWORD_IP_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_GET_PASSWORD_IP_WINDOW must be a duration")
		}
		cfg.RateLimits.GetPasswordIP.Window = d
	}
	// SignUp per-IP rate limit (open mode only).
	if v := os.Getenv("TG_RATE_LIMIT_SIGN_UP_IP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SIGN_UP_IP must be an integer")
		}
		cfg.RateLimits.SignUpIP.Limit = n
	}
	if v := os.Getenv("TG_RATE_LIMIT_SIGN_UP_IP_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, errors.New("TG_RATE_LIMIT_SIGN_UP_IP_WINDOW must be a duration")
		}
		cfg.RateLimits.SignUpIP.Window = d
	}
	preAuth, err := preAuthLimits()
	if err != nil {
		return Config{}, err
	}
	cfg.PreAuth = preAuth
	if v := os.Getenv("TG_MAX_CONNS_PER_UNBOUND_KEY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, errors.New("TG_MAX_CONNS_PER_UNBOUND_KEY must be an integer")
		}
		if n < 0 {
			return Config{}, errors.New("TG_MAX_CONNS_PER_UNBOUND_KEY must not be negative; 0 disables the cap")
		}
		cfg.MaxConnsPerUnboundKey = n
	}
	trust, err := clientAddrTrust(os.Getenv("TG_CLIENT_ADDR_TRUST"))
	if err != nil {
		return Config{}, err
	}
	cfg.ClientAddrTrust = trust
	proxies, err := clientAddrProxies(os.Getenv("TG_CLIENT_ADDR_PROXY_CIDRS"), trust)
	if err != nil {
		return Config{}, err
	}
	cfg.ClientAddrProxies = proxies
	advertiseHost, advertisePort, err := advertiseAddr(os.Getenv("TG_ADVERTISE_ADDR"), cfg.ListenAddr)
	if err != nil {
		return Config{}, err
	}
	cfg.AdvertiseHost, cfg.AdvertisePort = advertiseHost, advertisePort
	// Admin server requires both env vars or neither: a listener without auth
	// is a denial-of-service vector, and a hash with no listener is wasted work.
	adminErr := validateAdmin(cfg)
	if adminErr != nil {
		return Config{}, adminErr
	}
	cfg.AdminTokenHash = os.Getenv("TG_ADMIN_TOKEN_HASH")

	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("TG_POSTGRES_DSN is required")
	}
	encKey, err := loadEncKey(log)
	if err != nil {
		return Config{}, err
	}
	cfg.AuthKeyEncKey = encKey
	// Bootstrap: resolve username and password source before returning.
	cfg.BootstrapUsername = os.Getenv("TG_BOOTSTRAP_USERNAME")
	cfg.BootstrapPassword = os.Getenv("TG_BOOTSTRAP_PASSWORD")
	cfg.BootstrapPasswordFile = os.Getenv("TG_BOOTSTRAP_PASSWORD_FILE")
	if err := validateBootstrap(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// preAuthLimits resolves the bounds on what an unauthenticated connection may
// hold, starting from the shipped defaults.
//
// Zero is accepted and means the bound is off, matching every rate-limit
// surface: an operator taking one out has to be able to say so. Negative is not
// the same statement — it is a typo or a unit mistake — and a bound that silently
// became "off" because of one is the failure this whole surface exists to
// prevent, so it fails the start by name.
func preAuthLimits() (mtproto.PreAuthLimits, error) {
	limits := mtproto.DefaultPreAuthLimits()
	if v := os.Getenv("TG_MAX_PREAUTH_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return mtproto.PreAuthLimits{}, errors.New("TG_MAX_PREAUTH_CONNS must be an integer")
		}
		if n < 0 {
			return mtproto.PreAuthLimits{}, errors.New("TG_MAX_PREAUTH_CONNS must not be negative; 0 disables the cap")
		}
		limits.MaxConns = n
	}
	if v := os.Getenv("TG_MAX_PREAUTH_CONNS_PER_IP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return mtproto.PreAuthLimits{}, errors.New("TG_MAX_PREAUTH_CONNS_PER_IP must be an integer")
		}
		if n < 0 {
			return mtproto.PreAuthLimits{}, errors.New("TG_MAX_PREAUTH_CONNS_PER_IP must not be negative; 0 disables the cap")
		}
		limits.MaxConnsPerNet = n
	}
	if v := os.Getenv("TG_PREAUTH_LIFETIME"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return mtproto.PreAuthLimits{}, errors.New("TG_PREAUTH_LIFETIME must be a duration")
		}
		if d < 0 {
			return mtproto.PreAuthLimits{}, errors.New("TG_PREAUTH_LIFETIME must not be negative; 0 disables the ceiling")
		}
		limits.Lifetime = d
	}
	return limits, nil
}

// clientAddrTrust resolves the client-address source. An unset value is the
// socket, which is the only one implemented; anything else is refused by name
// rather than falling back, because a silent fallback to socket addresses
// behind a load balancer would key every client in the world to one bucket.
func clientAddrTrust(raw string) (ClientAddrTrust, error) {
	if raw == "" {
		return ClientAddrSocket, nil
	}
	for _, v := range clientAddrTrustValues {
		if ClientAddrTrust(raw) == v {
			return v, nil
		}
	}
	names := make([]string, len(clientAddrTrustValues))
	for i, v := range clientAddrTrustValues {
		names[i] = string(v)
	}
	return "", fmt.Errorf("TG_CLIENT_ADDR_TRUST must be one of: %s", strings.Join(names, ", "))
}

// clientAddrProxies resolves the balancer allowlist and refuses every way of
// getting it wrong, in both directions.
//
// In proxy-v2 mode an empty list cannot be defaulted: read as "trust nobody" it
// refuses every connection, read as "trust anybody" it lets any sender name any
// client address, and guessing between them is not the config loader's call. So
// the mode requires the list.
//
// The reverse mismatch fails just as hard. A list set while the mode is socket
// is an operator who believes they are behind a balancer and is in fact keying
// every client on the balancer's address — the collapsed global bucket this mode
// exists to prevent, and the one misconfiguration that looks fine at startup and
// locks out every login at once.
func clientAddrProxies(raw string, trust ClientAddrTrust) ([]netip.Prefix, error) {
	prefixes, err := parsePrefixes(raw)
	if err != nil {
		return nil, err
	}
	if trust != ClientAddrProxyV2 {
		if len(prefixes) > 0 {
			return nil, fmt.Errorf("TG_CLIENT_ADDR_PROXY_CIDRS is set while TG_CLIENT_ADDR_TRUST is %q: the allowlist is only read in %q mode, and %q keys every client on the balancer's own address", trust, ClientAddrProxyV2, trust)
		}
		return nil, nil
	}
	if len(prefixes) == 0 {
		return nil, fmt.Errorf("TG_CLIENT_ADDR_TRUST=%s requires TG_CLIENT_ADDR_PROXY_CIDRS to list the balancer addresses a PROXY header is accepted from", ClientAddrProxyV2)
	}
	return prefixes, nil
}

// mappedPrefixOffset is where the IPv4 part of an IPv4-mapped address starts:
// the first 96 bits are the ::ffff: wrapper.
const mappedPrefixOffset = 96

// normalizePrefix rewrites an IPv4-mapped prefix into the IPv4 one it means.
//
// It has to, because the two sides would otherwise never meet: the listener
// unmaps every peer address it reports, so a prefix left in mapped form is 128
// bits wide against a 32-bit address and matches nothing. The entry would start
// the server and then refuse every connection from the balancer it was written
// to name — an outage that looks like the limiter working, which is a worse
// failure than the default route, not a lesser one.
//
// A mapped prefix shorter than /96 covers more than the mapped range and has no
// IPv4 network to take, so it is refused rather than guessed at. Everything from
// /96 down takes its IPv4 meaning, which puts a disguised default route
// (::ffff:0.0.0.0/96) in front of the zero-length guard rather than past it.
func normalizePrefix(p netip.Prefix, entry string) (netip.Prefix, error) {
	if !p.Addr().Is4In6() {
		return p, nil
	}
	if p.Bits() < mappedPrefixOffset {
		return netip.Prefix{}, fmt.Errorf("TG_CLIENT_ADDR_PROXY_CIDRS entry %q is an IPv4-mapped prefix shorter than /%d, which names no IPv4 network: write it as an IPv4 CIDR", entry, mappedPrefixOffset)
	}
	return netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-mappedPrefixOffset), nil
}

// parsePrefixes reads a comma-separated allowlist. A bare address is its own
// single-host prefix, which is what an operator with one balancer writes.
// Anything that does not parse fails the start naming the entry: dropping it
// would leave a balancer whose connections are all refused, which reads as an
// outage rather than a typo.
func parsePrefixes(raw string) ([]netip.Prefix, error) {
	var prefixes []netip.Prefix
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if strings.Contains(entry, "/") {
			p, err := netip.ParsePrefix(entry)
			if err != nil {
				return nil, fmt.Errorf("TG_CLIENT_ADDR_PROXY_CIDRS entry %q is not a CIDR", entry)
			}
			// Before the guard below, so a default route written in mapped form
			// is caught by it rather than slipping past as a 96-bit prefix.
			if p, err = normalizePrefix(p, entry); err != nil {
				return nil, err
			}
			// A default route trusts every peer on the internet as a balancer,
			// which hands any client the right to name any source address: to
			// step out of its own bucket, or to pin an innocent address into
			// flood wait. That is the spoofable identity the whole mode exists
			// to prevent, and it is the one allowlist mistake that would
			// otherwise start cleanly, so it fails the start like the rest.
			if p.Bits() == 0 {
				return nil, fmt.Errorf("TG_CLIENT_ADDR_PROXY_CIDRS entry %q trusts every address: name the balancers, not a default route", entry)
			}
			prefixes = append(prefixes, p)
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return nil, fmt.Errorf("TG_CLIENT_ADDR_PROXY_CIDRS entry %q is not an address or CIDR", entry)
		}
		// Unmapped so a 4-in-6 spelling matches the same peer addresses the
		// listener reports, which are unmapped for the same reason.
		addr = addr.Unmap()
		prefixes = append(prefixes, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return prefixes, nil
}

// WarnClientAddrTrust states the operational assumption socket mode makes,
// once, at startup.
//
// It is not decoration. Per-IP limits in socket mode assume one peer address is
// one client, and the same collapse breaks that from either end. In front of
// the server, a proxy or an L4 load balancer makes every peer address the
// balancer's, so one bucket holds every client on earth and the per-IP cap
// becomes a global cap that locks everybody out at the same moment. In front of
// the clients, a carrier NAT puts thousands of mobile subscribers behind one
// IPv4 address, so the whole carrier spends a single key's budget and the 21st
// distinct number from it in a day is refused for up to a day.
//
// The balancer half has an answer now — ClientAddrProxyV2 keys on the address
// the balancer reports rather than its own — and this line is what tells an
// operator running without it where the support tickets will come from before
// they arrive. The carrier-NAT half remains an accepted risk in every mode.
// Silent in proxy-v2 mode, where the assumption it names is not the one being
// made, and silent when no per-IP limit is enabled, since nothing is then keyed
// on an address at all.
//
// Every limit keyed on a client address has to be in the condition below, or an
// operator turns one off and stops being warned about the other. The pre-auth
// connection cap is the harsher of the two to get wrong: the sendCode limits
// collapsing into one bucket refuses code requests, while a pre-auth cap
// collapsing into one bucket refuses handshakes server-wide, which is every
// client at once and looks like an outage rather than a limit.
func (c Config) WarnClientAddrTrust(log *slog.Logger) {
	perIPLimits := c.RateLimits.SendCodeIP.Enabled() || c.RateLimits.SignInFailIP.Enabled() ||
		c.RateLimits.CheckPasswordIP.Enabled() || c.RateLimits.GetPasswordIP.Enabled() ||
		c.RateLimits.SignUpIP.Enabled() || c.PreAuth.MaxConnsPerNet > 0
	if c.ClientAddrTrust != ClientAddrSocket || !perIPLimits {
		return
	}
	log.Warn("per-IP limits are keyed on each connection's own peer address",
		"trust", string(c.ClientAddrTrust),
		"assumes", "one peer address is one client, reaching this process directly",
		"risk", "behind a proxy or L4 load balancer every client shares one bucket: the per-IP cap becomes a global one, and the pre-auth connection cap becomes a server-wide ceiling on concurrent handshakes that refuses every client past it; behind a carrier NAT a whole mobile network shares one bucket and its subscribers are refused on each other's traffic")
}

// handshakeReadTimeout is the timeout gotd applies to each read inside key
// exchange (its DefaultTimeout). It is not this server's number to set, and it
// is the floor a pre-auth lifetime ceiling has to clear: a ceiling under it
// expires while a handshake is still legitimately waiting on one read.
const handshakeReadTimeout = 60 * time.Second

// WarnPreAuthLifetime states, once at startup, that the configured pre-auth
// ceiling is short enough to cut handshakes rather than holds.
//
// It is a warning and not a startup failure because the value is legitimate
// under load shedding, and because zero — the ceiling off entirely — is a
// supported setting that this must not appear to forbid. What it prevents is the
// silent case: an operator tuning the ceiling down to shed a flood, and getting
// intermittent handshake failures from slow clients that look like a network
// fault rather than the number they just set.
func (c Config) WarnPreAuthLifetime(log *slog.Logger) {
	if c.PreAuth.Lifetime <= 0 || c.PreAuth.Lifetime >= handshakeReadTimeout {
		return
	}
	log.Warn("TG_PREAUTH_LIFETIME is below the handshake's own read timeout",
		"lifetime", c.PreAuth.Lifetime,
		"handshake_read_timeout", handshakeReadTimeout,
		"risk", "a client still inside key exchange can be closed for being slow rather than for holding the connection, which reads as an intermittent connection failure")
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
// when the file is absent.
//
// The new key is written to a temporary file and linked into place, so the key
// file only ever becomes visible complete. Creating it with O_EXCL and writing
// afterwards looks equivalent and is not: it leaves a window where the path
// exists and is empty, and a second server starting inside that window reads
// nothing and fails to boot. os.Link is what closes it — it publishes the
// finished file in one step and fails with ErrExist rather than replacing a
// key another start already published, which a rename would do. Whoever loses
// that race adopts the winner's key, because a replica holding different key
// material cannot open any session the winner sealed.
func encKeyFromFile(path string) (key []byte, generated bool, err error) {
	key, err = readEncKeyFile(path)
	switch {
	case err == nil:
		return key, false, nil
	case !os.IsNotExist(err):
		return nil, false, err
	}

	buf := make([]byte, keycrypt.KeyLen)
	if _, err := rand.Read(buf); err != nil {
		return nil, false, fmt.Errorf("generate auth-key master key: %w", err)
	}
	// Same directory as the key file: os.Link cannot cross filesystems, and
	// TempDir would be a different one under the container's read-only root.
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".enc_key-*")
	if err != nil {
		return nil, false, fmt.Errorf("create temp key file in %s: %w", dir, err)
	}
	// Reported rather than dropped: what is left behind is master-key material
	// under a name nothing will ever read again, and a filesystem that cannot
	// unlink is worth failing the start over. Only the first start of a fresh
	// volume reaches here, and the next start reads the published file without
	// creating a temp at all, so this cannot become a crash loop.
	defer func() {
		if rmErr := os.Remove(tmp.Name()); rmErr != nil && !os.IsNotExist(rmErr) { // #nosec G703 -- os.CreateTemp's own name in the operator-configured directory.
			err = errors.Join(err, fmt.Errorf("remove temp key file: %w", rmErr))
		}
	}()

	_, writeErr := tmp.WriteString(hex.EncodeToString(buf))
	// Sync before the link, not after: the master key must be on the disk
	// before anything can be sealed under it. Postgres commits auth-key
	// ciphertext durably, so a key that is only in the page cache when the host
	// loses power comes back missing while the rows it opens come back intact —
	// every session undecryptable, which is the one failure this file exists to
	// prevent.
	if err := errors.Join(writeErr, tmp.Chmod(0o600), tmp.Sync(), tmp.Close()); err != nil {
		return nil, false, fmt.Errorf("write temp key file: %w", err)
	}
	if err := os.Link(tmp.Name(), path); err != nil {
		if os.IsExist(err) {
			// Another start published its key first. Its key is the real one,
			// and it is complete the moment the path exists.
			key, err := readEncKeyFile(path)
			return key, false, err
		}
		return nil, false, fmt.Errorf("create %s: %w", path, err)
	}
	// The file's bytes are durable, but the directory entry naming them is not
	// until the directory itself is synced.
	if err := syncDir(dir); err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

// syncDir flushes a directory's own entries, which is what makes a newly linked
// name survive a crash. Opened read-only: a directory cannot be opened for
// writing, and fsync does not need it.
func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304,G703 -- the directory of the operator-configured key file.
	if err != nil {
		return fmt.Errorf("open %s: %w", dir, err)
	}
	return errors.Join(d.Sync(), d.Close())
}

// readEncKeyFile reads and validates the key file. A NotExist error is returned
// unwrapped so the caller can tell "no key yet" from a real read failure.
func readEncKeyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path) // #nosec G304,G703 -- path is the operator-configured key file, not untrusted input.
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return decodeEncKey(strings.TrimSpace(string(raw)), path)
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

// adminHashRe matches a 64-character lowercase hex string (SHA-256 digest).
var adminHashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// validateAdmin checks that the admin server env vars are consistent.
//
// Both TG_ADMIN_LISTEN_ADDR and TG_ADMIN_TOKEN_HASH must be set together, or
// neither. A listener without a token hash is an unauthenticated admin surface;
// a token hash with no listener is wasted work. The hash must be a 64-char
// lowercase hex string (a SHA-256 digest).
func validateAdmin(cfg Config) error {
	listenAddr := cfg.AdminListenAddr
	tokenHash := os.Getenv("TG_ADMIN_TOKEN_HASH")

	switch {
	case listenAddr == "" && tokenHash == "":
		return nil // admin server disabled
	case listenAddr != "" && tokenHash != "":
		if !adminHashRe.MatchString(tokenHash) {
			return errors.New("TG_ADMIN_TOKEN_HASH must be a 64-character lowercase hex string (SHA-256 digest of the operator token)")
		}
		return nil
	case listenAddr != "" && tokenHash == "":
		return errors.New("TG_ADMIN_LISTEN_ADDR is set but TG_ADMIN_TOKEN_HASH is missing: both are required to start the admin HTTP server")
	default: // listenAddr == "" && tokenHash != ""
		return errors.New("TG_ADMIN_TOKEN_HASH is set but TG_ADMIN_LISTEN_ADDR is missing: both are required to start the admin HTTP server")
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// registrationMode resolves the registration mode from the env var. Unset or
// empty string produces the closed mode, so registration stays disabled by
// default until the operator opts in with a recognized value.
func registrationMode(raw string) RegistrationMode {
	switch {
	case raw == "":
		return RegistrationClosed
	case RegistrationMode(raw) == RegistrationClosed:
		return RegistrationClosed
	case RegistrationMode(raw) == RegistrationOpen:
		return RegistrationOpen
	}
	return RegistrationMode(raw)
}

// ValidateRegistrationMode checks that the registration mode is one of the
// recognized values. It is called after Load to fail the start when an
// operator names a mode this build does not implement.
func (c Config) ValidateRegistrationMode() error {
	switch c.RegistrationMode {
	case RegistrationClosed, RegistrationOpen:
		return nil
	}
	return fmt.Errorf("TG_REGISTRATION must be unset, %q, or %q; got %q", RegistrationClosed, RegistrationOpen, c.RegistrationMode)
}

// validateBootstrap checks that bootstrap env vars are consistent.
//
// When TG_BOOTSTRAP_USERNAME is set, exactly one of TG_BOOTSTRAP_PASSWORD or
// TG_BOOTSTRAP_PASSWORD_FILE must be set. Both set simultaneously is an error;
// neither set with a username is also an error.
func validateBootstrap(cfg Config) error {
	if cfg.BootstrapUsername == "" {
		return nil // bootstrap disabled
	}

	bothSet := cfg.BootstrapPassword != "" && cfg.BootstrapPasswordFile != ""
	noneSet := cfg.BootstrapPassword == "" && cfg.BootstrapPasswordFile == ""

	if bothSet {
		return errors.New("TG_BOOTSTRAP_PASSWORD and TG_BOOTSTRAP_PASSWORD_FILE are both set: use only one")
	}
	if noneSet {
		return errors.New("TG_BOOTSTRAP_USERNAME is set but no password source is configured: set TG_BOOTSTRAP_PASSWORD or TG_BOOTSTRAP_PASSWORD_FILE")
	}
	return nil
}

// BootstrapPasswordBytes returns the password for the bootstrap account.
// It reads from the file path when BootstrapPasswordFile is set, otherwise
// returns the in-memory password. The caller must zero the returned buffer
// after use.
func (c Config) BootstrapPasswordBytes() ([]byte, error) {
	if c.BootstrapPasswordFile != "" {
		data, err := os.ReadFile(c.BootstrapPasswordFile) // #nosec G304,G703 -- operator-configured path.
		if err != nil {
			return nil, fmt.Errorf("read bootstrap password file %s: %w", c.BootstrapPasswordFile, err)
		}
		return bytes.TrimSpace(data), nil
	}
	return []byte(c.BootstrapPassword), nil
}
