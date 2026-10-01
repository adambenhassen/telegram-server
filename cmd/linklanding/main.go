// Command linklanding serves the isolated Telegramd link landing page.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/adambenhassen/telegram-server/internal/linklanding"
)

const defaultPort = 8080

func main() {
	port := flag.Int("port", defaultPort, "loopback TCP port")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: linklanding [-port number]")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := serve(*port, logger); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("link landing server stopped", "err", err)
		os.Exit(1)
	}
}

func serve(port int, logger *slog.Logger) error {
	if port < 1 || port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", address)
	if err != nil {
		return fmt.Errorf("listen on loopback port %d: %w", port, err)
	}
	server := &http.Server{
		Addr:              address,
		Handler:           linklanding.NewHandler(logger),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    8192,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	return server.Serve(listener)
}
