// Command telegramd runs the MTProto server.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

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
	handler := api.New(st, cfg.DCID, tgcfg, log, cfg.LogLoginCodes, cfg.MaxFileBytes, blobs, cfg.MaxUserStorageBytes, peers, cfg.RateLimits)
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

	// Start the admin HTTP server on a separate listener. It serves read-only
	// operational metrics at GET /admin/metrics. Auth middleware can wrap the
	// handler without changing its shape.
	if cfg.AdminListenAddr != "" {
		adminMux := http.NewServeMux()
		adminMux.HandleFunc("/admin/metrics", admin.Handler(server.Registry(), st))
		adminSrv := &http.Server{
			Addr:              cfg.AdminListenAddr,
			Handler:           adminMux,
			ReadHeaderTimeout: 5 * time.Second,
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
