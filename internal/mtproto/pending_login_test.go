package mtproto_test

import (
	"testing"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestPendingLoginMarkerIsConnectionLocal(t *testing.T) {
	t.Parallel()

	first := mtproto.NewTestConn(&fakeConn{}, testKey(t))
	second := mtproto.NewTestConn(&fakeConn{}, testKey(t))

	if first.PendingLogin() {
		t.Fatal("new connection has a pending-login marker")
	}
	if second.PendingLogin() {
		t.Fatal("a separate connection has a pending-login marker")
	}

	first.MarkPendingLogin()

	if !first.PendingLogin() {
		t.Fatal("marked connection has no pending-login marker")
	}
	if second.PendingLogin() {
		t.Fatal("pending-login marker crossed into a separate connection")
	}
}
