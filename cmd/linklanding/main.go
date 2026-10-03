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

	"github.com/teagramhq/teagram-server/internal/linklanding"
)

const (
	defaultListenAddress = "127.0.0.1"
	defaultPort          = 8080
)

func main() {
	listenAddress := flag.String("listen-address", defaultListenAddress, "IP address to listen on")
	port := flag.Int("port", defaultPort, "loopback TCP port")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: linklanding [-listen-address ip] [-port number]")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := serve(*listenAddress, *port, logger); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("link landing server stopped", "err", err)
		os.Exit(1)
	}
}

func serve(listenHost string, port int, logger *slog.Logger) error {
	address, err := addressFor(listenHost, port)
	if err != nil {
		return err
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	server := &http.Server{
		Addr:                         address,
		Handler:                      linklanding.NewHandler(logger),
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            5 * time.Second,
		MaxHeaderBytes:               8192,
		ErrorLog:                     log.New(io.Discard, "", 0),
	}
	return server.Serve(listener)
}

func addressFor(host string, port int) (string, error) {
	if port < 1 || port > 65535 {
		return "", errors.New("port must be between 1 and 65535")
	}
	if net.ParseIP(host) == nil {
		return "", errors.New("listen address must be an IP address")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}
