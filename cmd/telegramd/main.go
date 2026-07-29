// Command telegramd runs the MTProto server.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
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
	cfg, err := config.Load()
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

	st, err := store.Open(ctx, cfg.PostgresDSN, cfg.AuthKeyEncKey)
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
	defer func() {
		cancelSweep()
		sweepWG.Wait()
	}()

	tgcfg := api.DefaultConfig(cfg.DCID, cfg.AdvertiseHost, cfg.AdvertisePort)
	handler := api.New(st, cfg.DCID, tgcfg, log, cfg.LogLoginCodes, cfg.MaxFileBytes)
	if cfg.LogLoginCodes {
		log.Warn("TG_LOG_LOGIN_CODES is on: login codes are written to the log in cleartext")
	}

	server := mtproto.New(exchange.PrivateKey{RSA: key}, cfg.DCID, mtproto.NewPgAuthKeyStore(st), handler, log)

	// Cross-replica real-time delivery: the listener wakes on NOTIFY and pushes
	// each user's pending updates to their live conns in this process. Drained
	// before the store pool closes (defer registered after st.Close, runs first).
	updater := api.NewUpdater(st, server.Registry(), log)
	_, stopListener, err := store.StartListener(ctx, cfg.PostgresDSN, updater.Deliver, updater.DeliverTyping, updater.Evict, log)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := stopListener(); cerr != nil {
			log.Error("listener stop", "err", cerr)
		}
	}()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	advertise := net.JoinHostPort(cfg.AdvertiseHost, strconv.Itoa(cfg.AdvertisePort))
	log.Info("listening", "addr", cfg.ListenAddr, "advertise", advertise, "dc", cfg.DCID)

	return server.Serve(ctx, transport.Listen(ln))
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
			n, err := st.DeleteExpiredUploadParts(ctx, time.Now().Add(-ttl))
			if err != nil {
				log.Error("sweep expired upload parts", "err", err)
				continue
			}
			log.Info("swept expired upload parts", "deleted", n)
		}
	}
}
