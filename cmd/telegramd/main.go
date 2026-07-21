// Command telegramd runs the MTProto server.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

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

	st, err := store.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Error("store close", "err", cerr)
		}
	}()

	host, port := splitHostPort(cfg.ListenAddr)
	tgcfg := api.DefaultConfig(cfg.DCID, host, port)
	handler := api.New(st, cfg.DCID, tgcfg, log)

	server := mtproto.New(exchange.PrivateKey{RSA: key}, cfg.DCID, mtproto.NewPgAuthKeyStore(st), handler, log)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	log.Info("listening", "addr", cfg.ListenAddr, "dc", cfg.DCID)

	return server.Serve(ctx, transport.Listen(ln))
}

func splitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 2443
	}
	if host == "" || strings.HasPrefix(addr, ":") {
		host = "127.0.0.1"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 2443
	}
	return host, port
}
