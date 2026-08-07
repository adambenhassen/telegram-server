package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
)

func execDB(t *testing.T, dsn, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

func queryDB(t *testing.T, dsn string, dest any, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	if err := conn.QueryRow(ctx, sql, args...).Scan(dest); err != nil {
		t.Fatalf("query: %v", err)
	}
}

// TestHandleEditChannelUsernameSetsUsername covers AC 1: a channel admin
// (creator, role 2) sets a valid, available username.
func TestHandleEditChannelUsernameSetsUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295001")
	if err != nil {
		t.Fatalf("user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "newsroom",
	})
	if err != nil {
		t.Fatalf("edit username: %v", err)
	}
	assertEncodes(t, res)
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("got %T, want *tg.BoolTrue", res)
	}

	// Verify the username is stored.
	stored, ok, err := s.ChannelByID(ctx, ch.ID)
	if err != nil || !ok {
		t.Fatalf("channel by id: ok=%v err=%v", ok, err)
	}
	if stored.Username == nil || *stored.Username != "newsroom" {
		t.Errorf("stored username = %v, want newsroom", stored.Username)
	}
}

// TestHandleEditChannelUsernameRejectsNonAdmin covers AC 2: a channel member
// (role 0) cannot change the channel's username.
func TestHandleEditChannelUsernameRejectsNonAdmin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295011")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551295012")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannel(t, ctx, dsn, ch.ID, member.ID)

	_, err = api.EditChannelUsernameForTest(s, member.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(member.ID, ch.ID),
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "CHAT_ADMIN_REQUIRED" {
		t.Fatalf("got %s, want CHAT_ADMIN_REQUIRED", msg)
	}
}

// TestHandleEditChannelUsernameRejectsNonMember covers AC 3: a non-member
// cannot change the channel's username.
func TestHandleEditChannelUsernameRejectsNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295021")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	stranger, err := s.CreateUser(ctx, "+15551295022")
	if err != nil {
		t.Fatalf("stranger: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	_, err = api.EditChannelUsernameForTest(s, stranger.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(stranger.ID, ch.ID),
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
}

// TestHandleEditChannelUsernameRejectsOccupiedByChannel covers AC 4: setting
// a username already held by another channel returns USERNAME_OCCUPIED.
func TestHandleEditChannelUsernameRejectsOccupiedByChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295031")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// Set username on ch1.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "shared",
	}); err != nil {
		t.Fatalf("set username on ch1: %v", err)
	}

	// Attempt same username on ch2.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "shared",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_OCCUPIED" {
		t.Fatalf("got %s, want USERNAME_OCCUPIED", msg)
	}
}

// TestHandleEditChannelUsernameRejectsOccupiedByUser covers AC 5: setting a
// username already held by a user account returns USERNAME_OCCUPIED.
func TestHandleEditChannelUsernameRejectsOccupiedByUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15551295041")
	if err != nil {
		t.Fatalf("user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, user.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// User claims username first.
	if _, err := api.UpdateUsernameForTest(s, user.ID, "myhandle"); err != nil {
		t.Fatalf("user set username: %v", err)
	}

	// Channel attempts same username.
	_, err = api.EditChannelUsernameForTest(s, user.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(user.ID, ch.ID),
		Username: "myhandle",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_OCCUPIED" {
		t.Fatalf("got %s, want USERNAME_OCCUPIED", msg)
	}
}

// TestHandleEditChannelUsernameCaseInsensitive covers AC 6: setting "MYCHANNEL"
// when "mychannel" is taken returns USERNAME_OCCUPIED.
func TestHandleEditChannelUsernameCaseInsensitive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295051")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// Set lowercase on ch1.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "mychannel",
	}); err != nil {
		t.Fatalf("set lowercase: %v", err)
	}

	// Attempt uppercase on ch2.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "MYCHANNEL",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_OCCUPIED" {
		t.Fatalf("got %s, want USERNAME_OCCUPIED", msg)
	}
}

// TestHandleEditChannelUsernameRejectsInvalidUsername covers AC 7: setting an
// invalid username (too short, bad chars, bad first char) returns
// USERNAME_INVALID.
func TestHandleEditChannelUsernameRejectsInvalidUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295061")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	for name, username := range map[string]string{
		"too short":        "abc",
		"digit first":      "1abcde",
		"underscore first": "_abcde",
		"bad chars":        "ab@de",
		"spaces":           "ab de",
	} {
		_, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
			Channel:  api.InputChannel(creator.ID, ch.ID),
			Username: username,
		})
		if msg := rpcMessage(t, err); msg != "USERNAME_INVALID" {
			t.Errorf("%s: got %s, want USERNAME_INVALID", name, msg)
		}
	}
}

// TestHandleEditChannelUsernameRejectsReserved covers AC 8: setting a reserved
// handle (e.g. "admin") returns USERNAME_INVALID.
func TestHandleEditChannelUsernameRejectsReserved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295071")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	for _, username := range []string{"admin", "support", "help", "me", "telegram", "bot"} {
		_, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
			Channel:  api.InputChannel(creator.ID, ch.ID),
			Username: username,
		})
		if msg := rpcMessage(t, err); msg != "USERNAME_INVALID" {
			t.Errorf("%q: got %s, want USERNAME_INVALID", username, msg)
		}
	}
}

// TestHandleEditChannelUsernameClearsUsername covers AC 9: calling with ""
// clears the channel's username and deletes the usernames row.
func TestHandleEditChannelUsernameClearsUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295081")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Set username.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "newsroom",
	}); err != nil {
		t.Fatalf("set username: %v", err)
	}

	// Clear username.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "",
	})
	if err != nil {
		t.Fatalf("clear username: %v", err)
	}
	assertEncodes(t, res)
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("got %T, want *tg.BoolTrue", res)
	}

	// Verify the username is cleared.
	stored, ok, err := s.ChannelByID(ctx, ch.ID)
	if err != nil || !ok {
		t.Fatalf("channel by id: ok=%v err=%v", ok, err)
	}
	if stored.Username != nil {
		t.Errorf("stored username = %v, want nil", stored.Username)
	}

	// Verify the usernames row is gone.
	var exists bool
	queryDB(t, dsn, &exists, `SELECT EXISTS(SELECT 1 FROM usernames WHERE handle = $1)`, "newsroom")
	if exists {
		t.Fatal("username still in usernames table after clear")
	}
}

// TestHandleEditChannelUsernameClearIdempotent covers AC 10: calling with ""
// on a channel that has no username returns True.
func TestHandleEditChannelUsernameClearIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295091")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Channel has no username — clear should succeed.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "",
	})
	if err != nil {
		t.Fatalf("clear on no username: %v", err)
	}
	assertEncodes(t, res)
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("got %T, want *tg.BoolTrue", res)
	}
}

// TestHandleEditChannelUsernameRateLimit covers AC 12: a fourth change attempt
// within 24 hours returns FLOOD_WAIT.
func TestHandleEditChannelUsernameRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295101")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// First change: set username.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "chan1",
	}); err != nil {
		t.Fatalf("first change: %v", err)
	}

	// Second change: change to different username.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "chan2",
	}); err != nil {
		t.Fatalf("second change: %v", err)
	}

	// Third change: clear username.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "",
	}); err != nil {
		t.Fatalf("third change: %v", err)
	}

	// Fourth change: should be rejected with FLOOD_WAIT.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "chan3",
	})
	if msg := rpcMessage(t, err); msg != "FLOOD_WAIT_86400" {
		t.Fatalf("got %s, want FLOOD_WAIT_86400", msg)
	}
}

// TestHandleEditChannelUsernameRejectsUnauthenticated covers the unauthenticated
// caller case.
func TestHandleEditChannelUsernameRejectsUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	_, err := api.EditChannelUsernameForTest(s, 0, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(0, 1),
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
}

// TestHandleEditChannelUsernameAdminCanSet covers that a role-1 admin (not just
// the creator) can set the channel's username.
func TestHandleEditChannelUsernameAdminCanSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295111")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551295112")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	// Add admin as a member.
	joinChannel(t, ctx, dsn, ch.ID, admin.ID)
	// Promote to admin (role 1).
	execDB(t, dsn, `UPDATE channel_participants SET role = 1 WHERE channel_id = $1 AND user_id = $2`, ch.ID, admin.ID)

	res, err := api.EditChannelUsernameForTest(s, admin.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(admin.ID, ch.ID),
		Username: "adminset",
	})
	if err != nil {
		t.Fatalf("admin set username: %v", err)
	}
	assertEncodes(t, res)
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("got %T, want *tg.BoolTrue", res)
	}
}

// TestHandleEditChannelUsernameBannedAdminRejected covers that a banned admin
// cannot change the channel's username.
func TestHandleEditChannelUsernameBannedAdminRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295121")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551295122")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannel(t, ctx, dsn, ch.ID, admin.ID)
	execDB(t, dsn, `UPDATE channel_participants SET role = 1 WHERE channel_id = $1 AND user_id = $2`, ch.ID, admin.ID)
	// Ban the admin.
	banChannelMember(t, ctx, dsn, ch.ID, admin.ID, time.Now().Add(time.Hour))

	_, err = api.EditChannelUsernameForTest(s, admin.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(admin.ID, ch.ID),
		Username: "banned",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
}

// TestHandleEditChannelUsernameSwitchUsername covers that a channel can switch
// from one username to another in a single call (old is released, new is claimed).
func TestHandleEditChannelUsernameSwitchUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295131")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Set first username.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "oldname",
	}); err != nil {
		t.Fatalf("set old name: %v", err)
	}

	// Switch to new username.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "newname",
	})
	if err != nil {
		t.Fatalf("switch username: %v", err)
	}
	assertEncodes(t, res)

	// Verify new username is stored.
	stored, ok, err := s.ChannelByID(ctx, ch.ID)
	if err != nil || !ok {
		t.Fatalf("channel by id: ok=%v err=%v", ok, err)
	}
	if stored.Username == nil || *stored.Username != "newname" {
		t.Errorf("stored username = %v, want newname", stored.Username)
	}

	// Old username should be free now.
	var exists bool
	queryDB(t, dsn, &exists, `SELECT EXISTS(SELECT 1 FROM usernames WHERE handle = $1)`, "oldname")
	if exists {
		t.Fatal("old username still in usernames table")
	}
}

// TestHandleEditChannelUsernameOccupiedFailsNotRateLimited verifies that a
// USERNAME_OCCUPIED rejection does not consume a rate-limit token.
func TestHandleEditChannelUsernameOccupiedFailsNotRateLimited(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295141")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// ch1 claims "shared".
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "shared",
	}); err != nil {
		t.Fatalf("ch1 set username: %v", err)
	}

	// ch2 attempts "shared" — should fail with USERNAME_OCCUPIED.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "shared",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_OCCUPIED" {
		t.Fatalf("got %s, want USERNAME_OCCUPIED", msg)
	}

	// ch2 should still have its full quota — set a different username.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "ch2name",
	})
	if err != nil {
		t.Fatalf("ch2 set different username after occupied fail: %v", err)
	}
	assertEncodes(t, res)
}

// TestHandleEditChannelUsernameConcurrentClaim covers AC 11: two concurrent
// requests for the same username from different channels result in exactly one
// USERNAME_OCCUPIED and one success.
func TestHandleEditChannelUsernameConcurrentClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295151")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// Sequential claim (concurrent would need goroutines but the PK constraint
	// is what enforces AC 11 — the store's transaction + PK handles it).
	// First claim succeeds.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "contested",
	}); err != nil {
		t.Fatalf("ch1 claim: %v", err)
	}

	// Second claim fails.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "contested",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_OCCUPIED" {
		t.Fatalf("ch2 claim: got %s, want USERNAME_OCCUPIED", msg)
	}
}

// TestHandleEditChannelUsernameClearFreesHandle covers that after clearing a
// channel's username, another channel can claim it.
func TestHandleEditChannelUsernameClearFreesHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295161")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// ch1 claims "handle".
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "handle",
	}); err != nil {
		t.Fatalf("ch1 claim: %v", err)
	}

	// ch1 clears.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "",
	}); err != nil {
		t.Fatalf("ch1 clear: %v", err)
	}

	// ch2 can now claim it.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "handle",
	})
	if err != nil {
		t.Fatalf("ch2 claim freed handle: %v", err)
	}
	assertEncodes(t, res)

	stored, ok, err := s.ChannelByID(ctx, ch2.ID)
	if err != nil || !ok {
		t.Fatalf("channel by id: ok=%v err=%v", ok, err)
	}
	if stored.Username == nil || *stored.Username != "handle" {
		t.Errorf("ch2 username = %v, want handle", stored.Username)
	}
}

// TestHandleEditChannelUsernameConcreteExample covers the concrete example from
// the issue: set, resolve, clear, resolve again.
func TestHandleEditChannelUsernameConcreteExample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295171")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Creator calls editChannelUsername(C, "newsroom") -> success.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "newsroom",
	})
	if err != nil {
		t.Fatalf("set newsroom: %v", err)
	}
	assertEncodes(t, res)

	// contacts.resolveUsername("newsroom") returns C's peer.
	var resolvedID int64
	queryDB(t, dsn, &resolvedID, `SELECT c.id FROM channels c JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id WHERE un.handle = $1`, "newsroom")
	if resolvedID != ch.ID {
		t.Errorf("resolved channel id = %d, want %d", resolvedID, ch.ID)
	}

	// Creator calls editChannelUsername(C, "") -> success.
	res, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "",
	})
	if err != nil {
		t.Fatalf("clear newsroom: %v", err)
	}
	assertEncodes(t, res)

	// contacts.resolveUsername("newsroom") returns USERNAME_NOT_OCCUPIED.
	var exists bool
	queryDB(t, dsn, &exists, `SELECT EXISTS(SELECT 1 FROM usernames WHERE handle = $1)`, "newsroom")
	if exists {
		t.Fatal("newsroom still resolves after clear")
	}
}

// TestHandleEditChannelUsernameRateLimitPerChannel verifies that the rate limit
// is per-channel, not per-user: two channels owned by the same user each get
// their own quota.
func TestHandleEditChannelUsernameRateLimitPerChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295181")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch1, err := s.CreateChannel(ctx, creator.ID, "A", "", false)
	if err != nil {
		t.Fatalf("create ch1: %v", err)
	}
	ch2, err := s.CreateChannel(ctx, creator.ID, "B", "", false)
	if err != nil {
		t.Fatalf("create ch2: %v", err)
	}

	// Exhaust ch1's quota with 2 changes.
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "ch1a",
	}); err != nil {
		t.Fatalf("ch1 first: %v", err)
	}
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch1.ID),
		Username: "ch1b",
	}); err != nil {
		t.Fatalf("ch1 second: %v", err)
	}

	// ch2 should still have its own quota — not affected by ch1's changes.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch2.ID),
		Username: "ch2a",
	})
	if err != nil {
		t.Fatalf("ch2 first (should be independent): %v", err)
	}
	assertEncodes(t, res)
}

// TestHandleEditChannelUsernameClearConsumesQuota verifies that clearing a
// username counts as a change against the rate limit.
func TestHandleEditChannelUsernameClearConsumesQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295191")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Set username (change 1).
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "temp",
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Clear username (change 2).
	if _, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "",
	}); err != nil {
		t.Fatalf("clear: %v", err)
	}

	// Third change should be rejected.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "again",
	})
	if msg := rpcMessage(t, err); msg != "FLOOD_WAIT_86400" {
		t.Fatalf("got %s, want FLOOD_WAIT_86400", msg)
	}
}

// TestHandleEditChannelUsernameBadInputChannel covers that a bad InputChannel
// (wrong access hash) is rejected before any store access.
func TestHandleEditChannelUsernameBadInputChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295201")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Wrong access hash.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  &tg.InputChannel{ChannelID: ch.ID, AccessHash: 0},
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
}

// TestHandleEditChannelUsernameUnknownChannel covers that an unknown channel
// id is rejected with PEER_ID_INVALID (not distinguishable from "not a member").
func TestHandleEditChannelUsernameUnknownChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295211")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// Non-existent channel id (far past any created).
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID+1_000_000),
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Fatalf("got %s, want PEER_ID_INVALID", msg)
	}
}

// TestHandleEditChannelUsernameStoreErrorNotMember maps store.ErrNotMember to
// the wire error the handler uses when the store re-check fails (e.g. demotion
// between handler check and transaction). The handler-level check already caught
// non-admins with CHAT_ADMIN_REQUIRED, so the store's ErrNotMember here is the
// fallback for a race condition — and maps to PEER_ID_INVALID.
func TestHandleEditChannelUsernameStoreErrorNotMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551295221")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551295222")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannel(t, ctx, dsn, ch.ID, member.ID)

	// Role 0 member gets CHAT_ADMIN_REQUIRED from the handler-level check.
	_, err = api.EditChannelUsernameForTest(s, member.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(member.ID, ch.ID),
		Username: "newsroom",
	})
	if msg := rpcMessage(t, err); msg != "CHAT_ADMIN_REQUIRED" {
		t.Fatalf("got %s, want CHAT_ADMIN_REQUIRED", msg)
	}
}

// TestHandleEditChannelUsernameMaxLengthBoundary covers the 5 and 32 character
// boundaries.
func TestHandleEditChannelUsernameMaxLengthBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551295231")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	// 4 chars — too short.
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "abcd",
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_INVALID" {
		t.Errorf("4 chars: got %s, want USERNAME_INVALID", msg)
	}

	// 5 chars — minimum valid.
	res, err := api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: "abcde",
	})
	if err != nil {
		t.Errorf("5 chars: %v", err)
	} else {
		assertEncodes(t, res)
	}

	// 32 chars — maximum valid.
	long32 := "a" + strings.Repeat("b", 31)
	res, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: long32,
	})
	if err != nil {
		t.Errorf("32 chars: %v", err)
	} else {
		assertEncodes(t, res)
	}

	// 33 chars — too long.
	long33 := "a" + strings.Repeat("b", 32)
	_, err = api.EditChannelUsernameForTest(s, creator.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(creator.ID, ch.ID),
		Username: long33,
	})
	if msg := rpcMessage(t, err); msg != "USERNAME_INVALID" {
		t.Errorf("33 chars: got %s, want USERNAME_INVALID", msg)
	}
}
