package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/transport"
)

// TestLoginOverBothFramings is the end of the claim the transport sniff makes,
// measured where it matters: a real gotd client, driving a real login, over the
// framing a stock Telegram client uses — 64 opening bytes with the codec tag
// encrypted inside them — and then another over the plaintext framing this
// server already served, against the same running listener.
//
// Key exchange is the part that has to survive: it is the first thing on the
// connection and the first thing a misdetected codec breaks, because a frame
// read under the wrong codec fails before any handler sees it. A login
// completing means the exchange completed and the auth RPCs behind it were
// answered.
func TestLoginOverBothFramings(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv := bootAcceptServer(t, ctx, 0)

	// Abridged inside obfuscation, which is what Telegram Desktop sends.
	srv.loginWith(t, ctx, "+15551239110", dcs.Plain(dcs.PlainOptions{
		Protocol:   transport.Abridged,
		Obfuscated: true,
	}))
	// And the framing the rest of the suite drives, unchanged, on the port that
	// just served the other one.
	srv.loginWith(t, ctx, "+15551239111", dcs.Plain(dcs.PlainOptions{}))
}
