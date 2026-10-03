// Command linkselector routes Web resources and Telegramd links to isolated services.
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

	"github.com/teagramhq/teagram-server/internal/linkselector"
)

const (
	defaultListenAddress = "127.0.0.1"
	defaultPort          = 8081
	defaultWebUpstream   = "http://web:8080"
	defaultLandingTarget = "http://linklanding:8082"
)

func main() {
	listenHost := flag.String("listen-address", defaultListenAddress, "IP address to listen on")
	port := flag.Int("port", defaultPort, "TCP port to listen on")
	webUpstream := flag.String("web-upstream", defaultWebUpstream, "Web service origin")
	landingUpstream := flag.String("landing-upstream", defaultLandingTarget, "landing service origin")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: linkselector [-listen-address ip] [-port number] [-web-upstream origin] [-landing-upstream origin]")
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler, err := linkselector.NewHandler(*webUpstream, *landingUpstream, logger)
	if err != nil {
		logger.Error("configure Web selector", "err", err)
		os.Exit(1)
	}
	address, err := addressFor(*listenHost, *port)
	if err != nil {
		logger.Error("configure Web selector listener", "err", err)
		os.Exit(1)
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", address)
	if err != nil {
		logger.Error("listen for Web selector", "err", err)
		os.Exit(1)
	}
	server := &http.Server{
		Addr:                         address,
		Handler:                      handler,
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            5 * time.Second,
		WriteTimeout:                 10 * time.Minute,
		MaxHeaderBytes:               8192,
		IdleTimeout:                  30 * time.Second,
		ErrorLog:                     log.New(io.Discard, "", 0),
	}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("Web selector stopped", "err", err)
		os.Exit(1)
	}
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
