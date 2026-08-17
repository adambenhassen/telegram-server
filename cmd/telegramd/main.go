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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gotd/td/exchange"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/blobscan"
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

// adminShutdownTimeout bounds how long the admin server waits for in-flight
// requests to finish before it gives up.
const adminShutdownTimeout = 5 * time.Second

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
	keyID, err := rsakey.KeyID(&key.PublicKey)
	if err != nil {
		return err
	}
	log.Info("server RSA key", "key_id", keyID, "fingerprint", rsakey.Fingerprint(&key.PublicKey), "path", cfg.RSAKeyPath)

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
	if cfg.MediaErasureReportInterval > 0 {
		sweepWG.Go(func() {
			reportMediaErasureCandidates(sweepCtx, st, cfg.MediaErasureMinAge, cfg.MediaErasureReportInterval, log)
		})
	}
	defer func() {
		cancelSweep()
		sweepWG.Wait()
	}()

	blobs, err := blob.NewLocal(cfg.BlobDir)
	if err != nil {
		return err
	}
	// Started here rather than with the sweeps above because it is the first
	// background pass that needs the blob store; sweepCtx and sweepWG already
	// cover it, so it is cancelled and waited for with the rest.
	if cfg.BlobScanReportInterval > 0 {
		sweepWG.Go(func() {
			reportBlobDisk(sweepCtx, blobs, st, cfg.BlobScanTempMinAge, cfg.BlobScanReportInterval, log)
		})
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

	// Start the admin HTTP server on a separate listener with login, logout,
	// CSRF protection, rate limiting, and security headers.
	if cfg.AdminListenAddr != "" {
		// Derive the admin origin from the listen address for CSRF checks.
		// When AdminListenAddr is a bare :port (e.g. ":2444"), substitute
		// localhost so the Origin header matches what a browser sends.
		adminHost, adminPort, err := net.SplitHostPort(cfg.AdminListenAddr)
		if err != nil {
			return fmt.Errorf("admin listen address: %w", err)
		}
		if adminHost == "" {
			adminHost = "localhost"
		}
		adminOrigin := "http://" + net.JoinHostPort(adminHost, adminPort)

		// One shared sampler feeds every dashboard stream; it idles while
		// nobody is connected.
		events := admin.NewBroadcaster(admin.BroadcasterConfig{
			Sample: admin.NewMetricsSampler(server.Registry(), st),
			Logger: log,
			Render: admin.DashboardFragmentRenderer,
		})
		var eventsWG sync.WaitGroup
		eventsCtx, stopEvents := context.WithCancel(ctx)
		defer stopEvents()
		eventsWG.Go(func() { events.Run(eventsCtx) })

		adminRouter := admin.AdminRouter(admin.LoginHandlerConfig{
			Store:       st,
			TokenHash:   cfg.AdminTokenHash,
			Logger:      log,
			AdminOrigin: adminOrigin,
			Events:      events,
		}, server.Registry())
		adminSrv := &http.Server{
			Addr:              cfg.AdminListenAddr,
			Handler:           adminRouter,
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
			// Stop the broadcaster first: it closes every open SSE stream, and
			// Shutdown waits on in-flight requests, which a live stream is.
			stopEvents()
			eventsWG.Wait()

			shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownTimeout)
			defer cancel()
			if err := adminSrv.Shutdown(shutdownCtx); err != nil {
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

// reportMediaErasureCandidates periodically counts what a media erase could
// reclaim and what is holding the rest back, until ctx is canceled.
//
// It is named a report rather than a sweep because it removes nothing: no row,
// no blob, no quota. The eraser is a later stage of M17 and lands with its own
// human decision to enable destruction; this pass exists so an operator can see
// the size of the reclaim before anything is allowed to perform it, and it is
// safe to leave running indefinitely.
//
// It logs aggregates and never a file's access hash. That value is the
// unguessable half of a download credential, so it must not reach log
// aggregation for every file a report names — which is why store's candidate
// type does not carry it at all rather than this line choosing not to print it.
//
// Off unless an operator sets an interval, and that default is why: the
// reference predicate is a SubPlan the planner cannot lift into a semi-join,
// and past roughly 300k media messages it stops being hashable and runs once
// per files row. See MediaErasureReportInterval for the measurement. An index
// on messages (file_id) is what makes it cheap, and that belongs with the
// ticket that decides how often a reclaim runs — it buys a write on every send,
// which is not a cost a report should be quietly incurring.
func reportMediaErasureCandidates(ctx context.Context, st *store.Store, minAge, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The cutoff is read per pass, not per batch: what a report holds
			// back is decided once, at its start, the same way a sweep's is.
			c, err := st.MediaErasureSummary(ctx, time.Now().Add(-minAge), store.ErasureScanBatch)
			if err != nil {
				log.Error("media erasure report", "err", err)
				continue
			}
			log.Info("media erasure candidates",
				"scanned", c.Scanned,
				"unreferenced", c.Unreferenced,
				"unreferenced_bytes", c.UnreferencedBytes,
				"unassembled", c.Unassembled,
				"unassembled_bytes", c.UnassembledBytes,
				"skipped_message_ref", c.SkippedMessageRef,
				"skipped_channel_ref", c.SkippedChannelRef,
				"skipped_too_new", c.SkippedTooNew)
		}
	}
}

// reportBlobDisk periodically classifies what is on the blob store against what
// the database accounts for, until ctx is canceled.
//
// A report, not a sweep: it removes nothing, and the pass it runs takes no lock
// and opens no transaction, so it is safe to leave running on a live server.
// The unaccounted bytes it finds are the completion mechanism a later stage
// needs — an eraser deletes a files row and unlinks afterwards, so a crash
// between the two leaves bytes nothing else will ever name — and an operator
// wants to see the size of that before anything is enabled to act on it.
//
// Paths the blob layout does not explain are logged one by one, at warn, and
// deliberately not counted alongside the reclaimable classes. Something is
// under the blob root that this server did not put there; that is a question
// for a person, and a pass that guessed at it would be how an unrelated file
// gets destroyed.
func reportBlobDisk(ctx context.Context, blobs *blob.Local, st *store.Store, tempMinAge, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The cutoff is read per pass, the same way a sweep's is: what a
			// report holds back is decided once, at its start.
			rep, err := blobscan.Scan(ctx, blobs, st, time.Now().Add(-tempMinAge))
			if err != nil {
				log.Error("blob disk report", "err", err)
				continue
			}
			log.Info("blob disk classification",
				"through_file_id", rep.Through,
				"walked", rep.Walked,
				"orphans", rep.Orphans.Count,
				"orphan_bytes", rep.Orphans.Bytes,
				"abandoned_temp", rep.Temps.Count,
				"abandoned_temp_bytes", rep.Temps.Bytes,
				"unexplained", rep.Unexplained.Count,
				"unexplained_bytes", rep.Unexplained.Bytes,
				"accounted", rep.Accounted,
				"above_snapshot", rep.AboveSnapshot,
				"temp_in_flight", rep.TempsInFlight)
			for _, p := range rep.Unexplained.Paths {
				log.Warn("path under the blob root the layout does not explain", "path", p.Key, "bytes", p.Size)
			}
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
// It validates the username, reads the password, and delegates to the store
// for the single-transaction bootstrap.
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
	defer clear(password)

	// Warn about credential exposure.
	if cfg.BootstrapPasswordFile != "" {
		log.Warn("bootstrap password loaded from file", "path", cfg.BootstrapPasswordFile, "action", "ensure file is readable only by the server process (mode 0600)")
	} else {
		log.Warn("bootstrap password is in process environment", "risk", "/proc/self/environ retains the value for the life of the process; orchestrator manifests, docker inspect, and crash dumps can expose it")
	}

	// Call the store's single-transaction bootstrap.
	result, err := st.BootstrapAccount(ctx, store.BootstrapParams{
		Handle:    handle,
		FirstName: "Operator",
		LastName:  "",
		Password:  password,
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
