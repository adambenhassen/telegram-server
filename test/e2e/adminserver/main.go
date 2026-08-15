//go:build e2e_admin_server

// Command e2e_admin_server runs a real admin HTTP server backed by a fresh
// Postgres database for the Playwright E2E suite (test/e2e/admin.spec.ts).
//
// Usage: TG_ADMIN_E2E_TOKEN=... go run -tags e2e_admin_server ./test/e2e/
//
// The process listens on 127.0.0.1:2444 and serves the admin router built by
// internal/admin.AdminRouter. The admin token hash is the SHA-256 of
// TG_ADMIN_E2E_TOKEN, matching what config.Load does with TG_ADMIN_TOKEN_HASH.
// A ready line "admin server ready" is printed to stdout when the listener is
// bound. The process exits on SIGINT/SIGTERM or when the DSN database is
// dropped out from under it.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const adminListenAddr = "127.0.0.1:2444"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	token := os.Getenv("TG_ADMIN_E2E_TOKEN")
	if token == "" {
		return os.ErrInvalid
	}
	sum := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(sum[:])

	dsn, dropDB, err := pgtest.DSNFor(os.Stderr)
	if err != nil {
		return err
	}
	defer dropDB()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dsn, make([]byte, 32))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Error("store close", "err", cerr)
		}
	}()

	// Background sweep, same cadence as the real server: the admin_sessions
	// table is bounded by this sweep, and the login flow is unaffected by it.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	defer cancelSweep()
	var sweepWG sync.WaitGroup
	sweepWG.Go(func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-sweepCtx.Done():
				return
			case <-ticker.C:
				if _, err := st.SweepExpiredAdminSessions(sweepCtx); err != nil {
					log.Error("sweep admin sessions", "err", err)
				}
			}
		}
	})
	defer sweepWG.Wait()

	registry := mtproto.NewSessionRegistry()
	router := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:       st,
		TokenHash:   tokenHash,
		Logger:      log,
		AdminOrigin: "http://" + adminListenAddr,
	}, registry)

	srv := &http.Server{
		Addr:              adminListenAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    8192,
	}
	ln, err := net.Listen("tcp", adminListenAddr)
	if err != nil {
		return err
	}
	log.Info("admin server listening", "addr", adminListenAddr)
	// The Playwright fixture waits for this exact line on stdout.
	if _, err := os.Stdout.WriteString("admin server ready\n"); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
