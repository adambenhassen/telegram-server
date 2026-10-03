package store_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/pgtest"
)

func TestNewGroupChannelPermissionStateDefaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	creator := mustUser(t, s, "+15551239901")
	chat := chatWith(t, s, creator)
	channel, err := s.CreateChannel(ctx, creator.ID, "Permissions", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	if !channel.Megagroup {
		t.Fatal("created channel is not a megagroup")
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	var chatRights []string
	if err := conn.QueryRow(ctx, `SELECT default_banned_rights FROM chats WHERE id = $1`, chat.ID).Scan(&chatRights); err != nil {
		t.Fatalf("read chat default rights: %v", err)
	}
	if len(chatRights) != 0 {
		t.Errorf("chat default rights = %v, want unrestricted", chatRights)
	}

	var channelRights []string
	var slowmodeSeconds int16
	if err := conn.QueryRow(ctx, `
		SELECT default_banned_rights, slowmode_seconds
		FROM channels WHERE id = $1
	`, channel.ID).Scan(&channelRights, &slowmodeSeconds); err != nil {
		t.Fatalf("read channel permission state: %v", err)
	}
	if len(channelRights) != 0 || slowmodeSeconds != 0 {
		t.Errorf("channel defaults = rights %v and slowmode %d, want unrestricted/0", channelRights, slowmodeSeconds)
	}

	var lastPostAt *string
	if err := conn.QueryRow(ctx, `
		SELECT last_post_at::text
		FROM channel_participants WHERE channel_id = $1 AND user_id = $2
	`, channel.ID, creator.ID).Scan(&lastPostAt); err != nil {
		t.Fatalf("read member last post: %v", err)
	}
	if lastPostAt != nil {
		t.Errorf("new member last_post_at = %s, want no prior post", *lastPostAt)
	}
}
