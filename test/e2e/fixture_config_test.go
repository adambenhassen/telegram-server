package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/teagramhq/teagram-server/internal/api"
)

func fixtureConfigForListener(t *testing.T, dcID int, listener net.Listener) *tg.Config {
	t.Helper()
	if listener == nil {
		t.Fatal("fixture listener is required")
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("fixture listener address must be TCP")
	}
	if addr.IP == nil || !addr.IP.IsLoopback() || addr.Port < 1 || addr.Port > 65535 {
		t.Fatal("fixture listener must be bound to loopback with a non-zero port")
	}
	cfg := api.DefaultConfig(dcID, addr.IP.String(), addr.Port)
	cfg.MeURLPrefix = testPublicLinkPrefix
	return cfg
}

func TestFixtureConfigForListenerUsesBoundEndpoint(t *testing.T) {
	listener := mustListen(t, context.Background(), "127.0.0.1:0")
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener close failed (type=%T)", err)
		}
	})
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address type = %T", listener.Addr())
	}
	const dcID = 7
	cfg := fixtureConfigForListener(t, dcID, listener)
	if cfg.MeURLPrefix != testPublicLinkPrefix {
		t.Fatalf("fixture me_url_prefix = %q, want %q", cfg.MeURLPrefix, testPublicLinkPrefix)
	}
	if cfg.DCTxtDomainName != "" {
		t.Fatalf("fixture dc_txt_domain_name = %q, want empty", cfg.DCTxtDomainName)
	}
	if cfg.ThisDC != dcID || len(cfg.DCOptions) != 1 {
		t.Fatal("fixture config DC identity mismatch")
	}
	option := cfg.DCOptions[0]
	if option.ID != dcID || option.IPAddress != addr.IP.String() || option.Port != addr.Port || option.Port == 0 {
		t.Fatal("fixture config does not match its bound loopback listener")
	}
}
