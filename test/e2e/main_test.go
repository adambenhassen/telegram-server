package e2e_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// TestMain pre-warms the shared Postgres container before any test runs, so the
// one-time cold start (image pull/boot) is not charged against a test's context
// deadline. Without this, the first e2e run on a fresh machine can exceed the
// per-test timeout while the container boots.
func TestMain(m *testing.M) {
	if err := pgtest.Prewarm(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest prewarm: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// peerUser builds an InputPeerUser with the derived access hash for a user peer.
// viewerID is the user sending the request; targetID is the peer being addressed.
func peerUser(viewerID, targetID int64) *tg.InputPeerUser {
	return &tg.InputPeerUser{
		UserID:     targetID,
		AccessHash: pgtest.PeerDeriver().Derive(viewerID, peerhash.KindUser, targetID),
	}
}

// inputUser builds an InputUser with the derived access hash.
func inputUser(viewerID, targetID int64) *tg.InputUser {
	return &tg.InputUser{
		UserID:     targetID,
		AccessHash: pgtest.PeerDeriver().Derive(viewerID, peerhash.KindUser, targetID),
	}
}
