// Command telegramd runs the MTProto server.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/crypto/srp"
	"github.com/gotd/td/exchange"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// sweepInterval is how often the background sweep deletes expired login codes.
const sweepInterval = 5 * time.Minute

func run(log *slog.Logger) error {
	cfg, err := config.Load(log)
	if err != nil {
		return err
	}

	// Validate TG_REGISTRATION immediately after config load, before any
	// resource-intensive init (RSA/DB/blob). A bad value fails fast with a
	// message naming the variable.
	if err := cfg.ValidateRegistrationMode(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	key, err := rsakey.LoadOrGenerate(cfg.RSAKeyPath)
	if err != nil {
		return err
	}
	log.Info("server RSA key", "fingerprint", rsakey.Fingerprint(&key.PublicKey), "path", cfg.RSAKeyPath)

	st, err := store.Open(ctx, cfg.PostgresDSN, cfg.AuthKeyEncKey, store.WithLogger(log))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Error("store close", "err", cerr)
		}
	}()

	// Bootstrap: create the first username-mode operator account before the
	// port binds. Must succeed or fail before anything else starts.
	if cfg.BootstrapUsername != "" {
		if err := bootstrapAccount(ctx, st, cfg, log); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	}

	// Run the sweep under a cancelable child context and wait for it to exit
	// before the store pool closes, so shutdown never closes the pool out from
	// under an in-flight sweep. This defer is registered after st.Close so it
	// runs first (LIFO): cancel the sweep, wait it out, then Close runs.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	var sweepWG sync.WaitGroup
	sweepWG.Go(func() {
		sweepExpiredCodes(sweepCtx, st, log)
	})
	sweepWG.Go(func() {
		sweepExpiredUploadParts(sweepCtx, st, cfg.UploadPartTTL, log)
	})
	sweepWG.Go(func() {
		sweepExpiredRateLimits(sweepCtx, st, log)
	})
	sweepWG.Go(func() {
		sweepExpiredSendCodeIPLimits(sweepCtx, st, log)
	})
	sweepWG.Go(func() {
		sweepExpiredSignInFailLimits(sweepCtx, st, log)
	})
	sweepWG.Go(func() {
		sweepExpiredAdminSessions(sweepCtx, st, log)
	})
	defer func() {
		cancelSweep()
		sweepWG.Wait()
	}()

	blobs, err := blob.NewLocal(cfg.BlobDir)
	if err != nil {
		return err
	}

	// Derive the peer-hash subkey here, at process start, and hand only the
	// subkey to the RPC layer. cfg.AuthKeyEncKey itself must not travel past
	// store.Open: its reach today is storage, and this must not widen it.
	peerSubkey, err := peerhash.Subkey(cfg.AuthKeyEncKey)
	if err != nil {
		return err
	}
	peers, err := peerhash.New(peerSubkey)
	if err != nil {
		return err
	}

	tgcfg := api.DefaultConfig(cfg.DCID, cfg.AdvertiseHost, cfg.AdvertisePort)
	handler := api.New(st, cfg.DCID, tgcfg, log, cfg.LogLoginCodes, cfg.MaxFileBytes, blobs, cfg.MaxUserStorageBytes, peers, cfg.RateLimits, cfg.RegistrationMode)
	if cfg.LogLoginCodes {
		log.Warn("TG_LOG_LOGIN_CODES is on: login codes are written to the log in cleartext")
	}
	cfg.WarnClientAddrTrust(log)

	server := mtproto.New(exchange.PrivateKey{RSA: key}, cfg.DCID, mtproto.NewPgAuthKeyStore(st), handler, log)
	if err := trustClientAddr(server, cfg, log); err != nil {
		return err
	}
	if err := server.SetPreAuthLimits(cfg.PreAuth); err != nil {
		return err
	}
	if err := server.SetMaxConnsPerUnboundKey(cfg.MaxConnsPerUnboundKey); err != nil {
		return err
	}
	cfg.WarnPreAuthLifetime(log)

	// Connection lifecycle callback: when a session binds or closes, record the
	// status change and notify other replicas.
	server.OnStatusChange(func(ctx context.Context, userID int64, online bool) {
		if err := st.SetUserStatus(ctx, userID, online); err != nil {
			log.Error("set user status", "user_id", userID, "online", online, "err", err)
			return
		}
		if err := st.Notify(ctx, store.ChannelStatus, store.StatusPayload(userID, online)); err != nil {
			log.Error("notify status", "user_id", userID, "err", err)
		}
	})

	// Cross-replica real-time delivery: the listener wakes on NOTIFY and pushes
	// each user's pending updates to their live conns in this process. Drained
	// before the store pool closes (defer registered after st.Close, runs first).
	updater := api.NewUpdater(st, server.Registry(), log, peers)
	_, stopListener, err := store.StartListener(ctx, cfg.PostgresDSN, updater.Deliver, updater.DeliverTyping, updater.Evict, updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus, updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned, log)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := stopListener(); cerr != nil {
			log.Error("listener stop", "err", cerr)
		}
	}()

	// Start the admin HTTP server on a separate listener. All paths behind the
	// RequireAdmin stub return 401; handlers are wired after auth is complete.
	if cfg.AdminListenAddr != "" {
		adminSubMux := http.NewServeMux()
		adminHandler := http.NewServeMux()
		adminHandler.Handle("/admin/", admin.RequireAdmin(admin.AdminMiddlewareConfig{
			Store:     st,
			TokenHash: cfg.AdminTokenHash,
		})(adminSubMux))
		adminSrv := &http.Server{
			Addr:              cfg.AdminListenAddr,
			Handler:           adminHandler,
			ReadHeaderTimeout: 5 * time.Second,
			MaxHeaderBytes:    8192,
		}
		var adminLC net.ListenConfig
		adminLn, err := adminLC.Listen(ctx, "tcp", cfg.AdminListenAddr)
		if err != nil {
			return fmt.Errorf("admin listen: %w", err)
		}
		log.Info("admin server listening", "addr", cfg.AdminListenAddr)
		go func() {
			if err := adminSrv.Serve(adminLn); err != nil && err != http.ErrServerClosed {
				log.Error("admin server", "err", err)
			}
		}()
		defer func() {
			if err := adminSrv.Shutdown(context.Background()); err != nil {
				log.Error("admin server shutdown", "err", err)
			}
		}()
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	advertise := net.JoinHostPort(cfg.AdvertiseHost, strconv.Itoa(cfg.AdvertisePort))
	log.Info("listening", "addr", cfg.ListenAddr, "advertise", advertise, "dc", cfg.DCID)

	return server.Serve(ctx, ln)
}

// trustClientAddr applies the configured client-address source to the server.
//
// The switch is exhaustive on purpose and has no permissive default: config
// validation already rejects any other value, and a fallback to socket keying
// added here would be the silent collapse into one global bucket that the mode
// exists to prevent.
func trustClientAddr(server *mtproto.Server, cfg config.Config, log *slog.Logger) error {
	switch cfg.ClientAddrTrust {
	case config.ClientAddrSocket:
		return nil
	case config.ClientAddrProxyV2:
		log.Info("client addresses are read from PROXY protocol v2 headers",
			"trust", string(cfg.ClientAddrTrust),
			"balancers", cfg.ClientAddrProxies)
		server.TrustProxyV2Headers(cfg.ClientAddrProxies)
		return nil
	default:
		return fmt.Errorf("unsupported client address trust mode %q", cfg.ClientAddrTrust)
	}
}

// sweepExpiredCodes periodically deletes expired login codes until ctx is
// canceled.
//
// ponytail: naive full-table DELETE on a plain ticker. Fine at login-code
// volumes; if phone_codes gets hot, add an index on expires_at, partition by
// time, or move the sweep to an external cron/job scheduler.
func sweepExpiredCodes(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.DeleteExpiredCodes(ctx)
			if err != nil {
				log.Error("sweep expired codes", "err", err)
				continue
			}
			log.Info("swept expired codes", "deleted", n)
		}
	}
}

// sweepExpiredUploadParts periodically deletes unassembled upload parts older
// than ttl until ctx is canceled.
//
// The interval is derived from the TTL rather than being a constant: this sweep
// is what bounds worst-case retained bytes, and an interval no greater than a
// quarter of the TTL keeps the overshoot past expiry small relative to the
// window itself.
func sweepExpiredUploadParts(ctx context.Context, st *store.Store, ttl time.Duration, log *slog.Logger) {
	// Floored at a second: NewTicker panics on a non-positive interval, and the
	// config only rejects a non-positive TTL, so a tiny-but-valid TTL would
	// otherwise crash the server at boot rather than sweep aggressively.
	ticker := time.NewTicker(max(ttl/4, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// One sweep is as many bounded passes as it takes: the batch bounds
			// each statement, not how much a tick may retire, so a backlog left
			// by a long outage clears here rather than one batch per tick.
			n, err := st.SweepExpiredUploadParts(ctx, time.Now().Add(-ttl), store.ExpiredPartSweepBatch)
			if err != nil {
				log.Error("sweep expired upload parts", "deleted", n, "err", err)
				continue
			}
			log.Info("swept expired upload parts", "deleted", n)
		}
	}
}

// sweepExpiredRateLimits periodically deletes rate-limit rows whose per-row
// expiry deadline has passed. This is what bounds the rate_limits table: rows
// are only created by the limiter (not on every request) and only deleted by
// this sweep.
func sweepExpiredRateLimits(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.SweepExpiredRateLimits(ctx)
			if err != nil {
				log.Error("sweep expired rate limits", "err", err)
				continue
			}
			log.Info("swept expired rate limits", "deleted", n)
		}
	}
}

// sweepExpiredSendCodeIPLimits periodically deletes per-IP sendCode rows past
// their deadline. A network that keeps calling prunes its own rows on write;
// this is what clears the ones that go quiet, and it is what holds retention of
// the network-to-number rows to the limit window rather than forever.
func sweepExpiredSendCodeIPLimits(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.SweepExpiredSendCodeIPLimits(ctx)
			if err != nil {
				log.Error("sweep expired send code ip limits", "err", err)
				continue
			}
			log.Info("swept expired send code ip limits", "deleted", n)
		}
	}
}

// sweepExpiredSignInFailLimits periodically deletes per-IP signIn-failure rows
// past their deadline. Unlike sendCode IP limits there is no prune-on-write
// (signIn is not idempotent), so the sweep is the sole cleanup path.
func sweepExpiredSignInFailLimits(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.SweepExpiredSignInFailCalls(ctx)
			if err != nil {
				log.Error("sweep expired sign in fail limits", "err", err)
				continue
			}
			log.Info("swept expired sign in fail limits", "deleted", n)
		}
	}
}

// sweepExpiredAdminSessions periodically deletes admin session rows whose
// absolute expiry deadline has passed. This is what bounds the admin_sessions
// table: sessions are only created by the login handler and only deleted by
// this sweep.
func sweepExpiredAdminSessions(ctx context.Context, st *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.SweepExpiredAdminSessions(ctx)
			if err != nil {
				log.Error("sweep expired admin sessions", "err", err)
				continue
			}
			log.Info("swept expired admin sessions", "deleted", n)
		}
	}
}

// bootstrapAccount creates the first username-mode operator account at startup.
// It validates the username, computes the SRP verifier using gotd's crypto/srp
// package, and writes the user + username + verifier in a single transaction.
//
// The password buffer is zeroed after use.
func bootstrapAccount(ctx context.Context, st *store.Store, cfg config.Config, log *slog.Logger) error {
	// Validate the username format and reserved blocklist.
	handle := strings.ToLower(cfg.BootstrapUsername)
	if err := store.ValidateBootstrapUsername(handle); err != nil {
		return err
	}

	// Read the password.
	password, err := cfg.BootstrapPasswordBytes()
	if err != nil {
		return err
	}
	defer zeroBytes(password)

	// Warn about credential exposure.
	if cfg.BootstrapPasswordFile != "" {
		log.Warn("bootstrap password loaded from file", "path", cfg.BootstrapPasswordFile, "action", "ensure file is readable only by the server process (mode 0600)")
	} else {
		log.Warn("bootstrap password is in process environment", "risk", "/proc/self/environ retains the value for the life of the process; orchestrator manifests, docker inspect, and crash dumps can expose it")
	}

	// Generate KDF salts.
	salt1 := make([]byte, 32)
	if _, err := rand.Read(salt1); err != nil {
		return fmt.Errorf("generate salt1: %w", err)
	}
	salt2 := make([]byte, 32)
	if _, err := rand.Read(salt2); err != nil {
		return fmt.Errorf("generate salt2: %w", err)
	}

	// Compute the SRP verifier using gotd's crypto/srp package.
	srpClient := srp.NewSRP(rand.Reader)
	algo := srp.Input{
		Salt1: salt1,
		Salt2: salt2,
		G:     3,
		P:     srpPBytes(),
	}
	verifier, _, err := srpClient.NewHash(password, algo)
	if err != nil {
		return fmt.Errorf("compute verifier: %w", err)
	}

	// Call the store's single-transaction bootstrap.
	result, err := st.BootstrapAccount(ctx, store.BootstrapParams{
		Handle:    handle,
		FirstName: "Operator",
		LastName:  "",
		Salt1:     salt1,
		Salt2:     salt2,
		Verifier:  verifier,
	})
	if err != nil {
		return err
	}

	if result.Created {
		log.Info("bootstrap account created", "user_id", result.UserID, "username", handle)
	} else {
		log.Info("bootstrap account already exists (idempotent)", "user_id", result.UserID, "username", handle)
	}
	log.Info("bootstrap does not rotate passwords; to change the credential use the M16 dashboard or direct DB access")

	return nil
}

// zeroBytes overwrites b with zeros. It is used to clear sensitive buffers.
func zeroBytes(b []byte) {
	bytes.ReplaceAll(b, b, []byte{})
}

// srpPBytes returns the canonical 256-byte padded Telegram SRP prime.
// It is the same value used by the server-side SRP implementation.
func srpPBytes() []byte {
	hex := "" +
		"C71CAEB9C6B1C9048E6C522F70F13F73980D40238E3E21C14934D037563D930F" +
		"48198A0AA7C14058229493D22530F4DBFA336F6E0AC925139543AED44CCE7C37" +
		"20FD51F69458705AC68CD4FE6B6B13ABDC9746512969328454F18FAF8C595F64" +
		"2477FE96BB2A941D5BCD1D4AC8CC49880708FA9B378E3C4F3A9060BEE67CF9A4" +
		"A4A695811051907E162753B56B0F6B410DBA74D8A84B2A14B3144E0EF1284754" +
		"FD17ED950D5965B4B9DD46582DB1178D169C6BC465B0D6FF9CA3928FEF5B9AE4" +
		"E418FC15E83EBEA0F87FA9FF5EED70050DED2849F47BF959D956850CE929851F" +
		"0D8115F635B105EE2E4E15D04B2454BF6F4FADF034B10403119CD8E3B92FCC5B"
	b, _ := new(big.Int).SetString(hex, 16)
	out := make([]byte, 256)
	copy(out[256-len(b.Bytes()):], b.Bytes())
	return out
}
