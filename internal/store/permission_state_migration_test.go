package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/teagramhq/teagram-server/internal/pgtest"
)

func TestGroupChannelPermissionStateMigrationPreservesDataAndChecksValues(t *testing.T) {
	ctx := context.Background()
	const (
		administrationMigration       = "20260913000039_server_administration.sql"
		permissionMigration           = "20260930000041_group_channel_permission_state.sql"
		permissionValidationMigration = "20260930000042_validate_group_channel_permission_state.sql"
		bulkRows                      = 50000
		bulkChannelID                 = int64(3000000000)
	)
	migrationsDir := filepath.Join("..", "..", "migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	readMigration := func(name string) string {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		return string(body)
	}

	admin, err := pgx.Connect(ctx, pgtest.AdminDSN())
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }() //nolint:errcheck // best-effort close

	dbName := "t_" + pgtest.RandomHex()
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+dbName); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		conn, err := pgx.Connect(cleanupCtx, pgtest.AdminDSN())
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(cleanupCtx) }() //nolint:errcheck // best-effort close
		if _, err := conn.Exec(cleanupCtx, `DROP DATABASE IF EXISTS `+dbName+` WITH (FORCE)`); err != nil {
			t.Logf("cleanup drop %s: %v", dbName, err)
		}
	})

	conn, err := pgx.Connect(ctx, pgtest.DSNFrom(dbName))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= administrationMigration {
			continue
		}
		if _, err := conn.Exec(ctx, readMigration(entry.Name())); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}

	var creatorID, memberID int64
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(&creatorID); err != nil {
		t.Fatalf("seed creator: %v", err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(&memberID); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	if _, err := conn.Exec(ctx, readMigration(administrationMigration)); err != nil {
		t.Fatalf("apply administration migration: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() <= administrationMigration || entry.Name() >= permissionMigration {
			continue
		}
		if _, err := conn.Exec(ctx, readMigration(entry.Name())); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}

	const existingChatVersion = 7
	const existingChannelVersion = 11
	var chatID int64
	if err := conn.QueryRow(ctx, `
		INSERT INTO chats (title, creator_id, version, date)
		VALUES ('legacy group', $1, $2, '2020-01-02T03:04:05Z') RETURNING id
	`, creatorID, existingChatVersion).Scan(&chatID); err != nil {
		t.Fatalf("seed chat: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO chat_participants (chat_id, user_id, inviter_id, date)
		VALUES ($1, $2, $2, '2020-01-02T03:04:05Z'), ($1, $3, $2, '2020-01-02T03:04:06Z')
	`, chatID, creatorID, memberID); err != nil {
		t.Fatalf("seed chat participants: %v", err)
	}
	const existingChannelID int64 = 2147483700
	if _, err := conn.Exec(ctx, `
		INSERT INTO channels (id, title, about, creator_id, megagroup, version, date, username, publicly_discoverable)
		VALUES ($1, 'legacy supergroup', 'legacy about', $2, true, $3, '2020-02-03T04:05:06Z', 'legacypermissions', true)
	`, existingChannelID, creatorID, existingChannelVersion); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO usernames (handle, owner_type, owner_id) VALUES ('legacypermissions', 'channel', $1)`, existingChannelID); err != nil {
		t.Fatalf("seed public username: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_state (channel_id, pts, next_local_id) VALUES ($1, 23, 81)`, existingChannelID); err != nil {
		t.Fatalf("seed channel state: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts, date)
		VALUES ($1, $2, 2, 23, '2020-02-03T04:05:06Z'),
		       ($1, $3, 0, 7, '2020-02-03T04:05:07Z')
	`, existingChannelID, creatorID, memberID); err != nil {
		t.Fatalf("seed channel participants: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE channel_participants SET banned_until = 'infinity'
		WHERE channel_id = $1 AND user_id = $2
	`, existingChannelID, memberID); err != nil {
		t.Fatalf("seed existing ban: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO messages (owner_id, local_id, peer_type, peer_id, from_id, date, message, out)
		VALUES ($1, 101, 2, $2, $1, '2020-02-04T05:06:07Z', 'legacy group message', true)
	`, creatorID, chatID); err != nil {
		t.Fatalf("seed group message: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO channel_messages (channel_id, local_id, from_id, date, message)
		VALUES ($1, 80, $2, '2020-02-05T06:07:08Z', 'legacy channel post')
	`, existingChannelID, creatorID); err != nil {
		t.Fatalf("seed channel message: %v", err)
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO chats (title, creator_id)
		SELECT 'bulk group', $1 FROM generate_series(1, $2)
	`, creatorID, bulkRows); err != nil {
		t.Fatalf("seed bulk chats: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO chat_participants (chat_id, user_id, inviter_id)
		SELECT id, $1, $1 FROM chats WHERE title = 'bulk group'
	`, creatorID); err != nil {
		t.Fatalf("seed bulk chat participants: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO channels (id, title, about, creator_id, megagroup)
		SELECT $1::bigint + n, 'bulk channel', '', $2, true FROM generate_series(1, $3) AS n
	`, bulkChannelID, creatorID, bulkRows); err != nil {
		t.Fatalf("seed bulk channels: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		SELECT id, $1, 0, 0 FROM channels WHERE title = 'bulk channel'
	`, creatorID); err != nil {
		t.Fatalf("seed bulk channel participants: %v", err)
	}

	started := time.Now()
	if _, err := conn.Exec(ctx, readMigration(permissionMigration)); err != nil {
		t.Fatalf("apply permission state migration: %v", err)
	}
	for _, constraintName := range []string{
		"chats_default_banned_rights_valid",
		"channels_default_banned_rights_valid",
		"channels_slowmode_seconds_valid",
	} {
		var validated bool
		if err := conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = $1`, constraintName).Scan(&validated); err != nil {
			t.Fatalf("read validation state for %s: %v", constraintName, err)
		}
		if validated {
			t.Fatalf("constraint %s validated in the column-addition migration; want validation in a later migration", constraintName)
		}
	}
	var bulkChatID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM chats WHERE title = 'bulk group' ORDER BY id LIMIT 1`).Scan(&bulkChatID); err != nil {
		t.Fatalf("read bulk chat id: %v", err)
	}
	testPermissionStateValidationAllowsConcurrentAccess(
		t,
		ctx,
		dbName,
		conn,
		readMigration(permissionValidationMigration),
		chatID,
		bulkChatID,
		existingChannelID,
		bulkChannelID+1,
		memberID,
	)
	for _, constraintName := range []string{
		"chats_default_banned_rights_valid",
		"channels_default_banned_rights_valid",
		"channels_slowmode_seconds_valid",
	} {
		var validated bool
		if err := conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = $1`, constraintName).Scan(&validated); err != nil {
			t.Fatalf("read validation state for %s: %v", constraintName, err)
		}
		if !validated {
			t.Fatalf("constraint %s remains unvalidated after validation migration", constraintName)
		}
	}
	t.Logf("permission state migration applied to %d chats, %d channels, and %d channel participants in %s", bulkRows+1, bulkRows+1, bulkRows+2, time.Since(started))

	var chatVersion int
	var chatRights []string
	if err := conn.QueryRow(ctx, `SELECT version, default_banned_rights FROM chats WHERE id = $1`, chatID).Scan(&chatVersion, &chatRights); err != nil {
		t.Fatalf("read existing chat permission state: %v", err)
	}
	if chatVersion != existingChatVersion || len(chatRights) != 0 {
		t.Fatalf("existing chat = version %d rights %v, want version %d and unrestricted", chatVersion, chatRights, existingChatVersion)
	}
	var channelVersion int
	var channelRights []string
	var slowmodeSeconds int16
	var username *string
	var publiclyDiscoverable bool
	if err := conn.QueryRow(ctx, `
		SELECT version, default_banned_rights, slowmode_seconds, username, publicly_discoverable
		FROM channels WHERE id = $1
	`, existingChannelID).Scan(&channelVersion, &channelRights, &slowmodeSeconds, &username, &publiclyDiscoverable); err != nil {
		t.Fatalf("read existing channel permission state: %v", err)
	}
	if channelVersion != existingChannelVersion || len(channelRights) != 0 || slowmodeSeconds != 0 || username == nil || *username != "legacypermissions" || !publiclyDiscoverable {
		t.Fatalf("existing channel = version %d rights %v slowmode %d username %v public %v", channelVersion, channelRights, slowmodeSeconds, username, publiclyDiscoverable)
	}

	var chatParticipants int
	var channelParticipants int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM chat_participants WHERE chat_id = $1`, chatID).Scan(&chatParticipants); err != nil {
		t.Fatalf("count chat participants: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_participants WHERE channel_id = $1`, existingChannelID).Scan(&channelParticipants); err != nil {
		t.Fatalf("count channel participants: %v", err)
	}
	if chatParticipants != 2 || channelParticipants != 2 {
		t.Fatalf("memberships after migration = chat %d channel %d, want 2 each", chatParticipants, channelParticipants)
	}
	var bannedUntil string
	var joinPts int64
	var lastPostAt *time.Time
	if err := conn.QueryRow(ctx, `
		SELECT banned_until::text, join_pts, last_post_at
		FROM channel_participants WHERE channel_id = $1 AND user_id = $2
	`, existingChannelID, memberID).Scan(&bannedUntil, &joinPts, &lastPostAt); err != nil {
		t.Fatalf("read existing member state: %v", err)
	}
	if bannedUntil != "infinity" || joinPts != 7 || lastPostAt != nil {
		t.Fatalf("existing member = ban %q join_pts %d last_post_at %v, want preserved ban/7/nil", bannedUntil, joinPts, lastPostAt)
	}

	var groupMessage, channelMessage string
	if err := conn.QueryRow(ctx, `SELECT message FROM messages WHERE owner_id = $1 AND local_id = 101`, creatorID).Scan(&groupMessage); err != nil {
		t.Fatalf("read existing group message: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT message FROM channel_messages WHERE channel_id = $1 AND local_id = 80`, existingChannelID).Scan(&channelMessage); err != nil {
		t.Fatalf("read existing channel message: %v", err)
	}
	if groupMessage != "legacy group message" || channelMessage != "legacy channel post" {
		t.Fatalf("messages after migration = %q and %q, want original content", groupMessage, channelMessage)
	}

	marker := time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)
	if _, err := conn.Exec(ctx, `UPDATE channel_participants SET last_post_at = $3 WHERE channel_id = $1 AND user_id = $2`, existingChannelID, creatorID, marker); err != nil {
		t.Fatalf("write member last post: %v", err)
	}
	var savedLastPostAt *time.Time
	if err := conn.QueryRow(ctx, `SELECT last_post_at FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, existingChannelID, creatorID).Scan(&savedLastPostAt); err != nil {
		t.Fatalf("read member last post: %v", err)
	}
	if savedLastPostAt == nil || !savedLastPostAt.Equal(marker) {
		t.Fatalf("saved last_post_at = %v, want %s", savedLastPostAt, marker)
	}

	allowedRights := []string{"send_messages", "send_polls"}
	if _, err := conn.Exec(ctx, `UPDATE chats SET default_banned_rights = $2 WHERE id = $1`, chatID, allowedRights); err != nil {
		t.Fatalf("store supported chat rights: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, existingChannelID, allowedRights); err != nil {
		t.Fatalf("store supported channel rights: %v", err)
	}
	for _, invalidRights := range [][]string{
		{"view_messages"},
		{"send_polls", "until_date"},
		{"send_reactions"},
		{"send_polls", ""},
	} {
		if _, err := conn.Exec(ctx, `UPDATE chats SET default_banned_rights = $2 WHERE id = $1`, chatID, invalidRights); !checkViolation(err) {
			t.Errorf("chat accepted invalid default rights %v: %v", invalidRights, err)
		}
		if _, err := conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, existingChannelID, invalidRights); !checkViolation(err) {
			t.Errorf("channel accepted invalid default rights %v: %v", invalidRights, err)
		}
	}
	var storedChatRights []string
	if err := conn.QueryRow(ctx, `SELECT default_banned_rights FROM chats WHERE id = $1`, chatID).Scan(&storedChatRights); err != nil {
		t.Fatalf("read rights after rejected writes: %v", err)
	}
	if len(storedChatRights) != len(allowedRights) || storedChatRights[0] != allowedRights[0] || storedChatRights[1] != allowedRights[1] {
		t.Fatalf("chat rights after rejected writes = %v, want %v", storedChatRights, allowedRights)
	}

	for _, seconds := range []int16{0, 10, 30, 60, 300, 900, 3600} {
		if _, err := conn.Exec(ctx, `UPDATE channels SET slowmode_seconds = $2 WHERE id = $1`, existingChannelID, seconds); err != nil {
			t.Errorf("store supported slowmode %d: %v", seconds, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE channels SET slowmode_seconds = 11 WHERE id = $1`, existingChannelID); !checkViolation(err) {
		t.Fatalf("slowmode 11 did not fail with a check violation: %v", err)
	}
	var storedSlowmode int16
	if err := conn.QueryRow(ctx, `SELECT slowmode_seconds FROM channels WHERE id = $1`, existingChannelID).Scan(&storedSlowmode); err != nil {
		t.Fatalf("read slowmode after rejected write: %v", err)
	}
	if storedSlowmode != 3600 {
		t.Fatalf("slowmode after rejected 11 write = %d, want unchanged 3600", storedSlowmode)
	}
}

func checkViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}

func testPermissionStateValidationAllowsConcurrentAccess(
	t *testing.T,
	ctx context.Context,
	dbName string,
	conn *pgx.Conn,
	validationMigration string,
	chatID int64,
	bulkChatID int64,
	channelID int64,
	bulkChannelID int64,
	memberID int64,
) {
	t.Helper()

	const (
		chatGateName    = "test_permission_state_chat_validation_gate"
		channelGateName = "test_permission_state_channel_validation_gate"
		chatLockKey     = int32(52041)
		channelLockKey  = int32(52042)
	)
	if _, err := conn.Exec(ctx, `
		CREATE FUNCTION test_permission_state_validation_gate(
			row_id BIGINT, gate_row_id BIGINT, lock_key INTEGER, constraint_name TEXT
		) RETURNS BOOLEAN
		LANGUAGE plpgsql VOLATILE AS $$
		BEGIN
			IF row_id = gate_row_id AND position(constraint_name IN current_query()) > 0 THEN
				PERFORM pg_advisory_lock(hashtext(current_database()), lock_key);
				PERFORM pg_advisory_unlock(hashtext(current_database()), lock_key);
			END IF;
			RETURN true;
		END;
		$$
	`); err != nil {
		t.Fatalf("create validation gate function: %v", err)
	}
	for _, constraint := range []struct {
		table string
		name  string
		id    int64
		key   int32
	}{
		{table: "chats", name: chatGateName, id: chatID, key: chatLockKey},
		{table: "channels", name: channelGateName, id: channelID, key: channelLockKey},
	} {
		statement := fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s CHECK (test_permission_state_validation_gate(id, %d, %d, '%s')) NOT VALID",
			constraint.table,
			constraint.name,
			constraint.id,
			constraint.key,
			constraint.name,
		)
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("add %s validation gate: %v", constraint.name, err)
		}
	}

	gateConn, err := pgx.Connect(ctx, pgtest.DSNFrom(dbName))
	if err != nil {
		t.Fatalf("connect validation gate: %v", err)
	}
	defer func() { _ = gateConn.Close(ctx) }() //nolint:errcheck // best-effort close releases session locks
	for _, key := range []int32{chatLockKey, channelLockKey} {
		if _, err := gateConn.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()), $1)`, key); err != nil {
			t.Fatalf("hold validation gate lock %d: %v", key, err)
		}
	}

	validationConn, err := pgx.Connect(ctx, pgtest.DSNFrom(dbName))
	if err != nil {
		t.Fatalf("connect validation session: %v", err)
	}
	defer func() { _ = validationConn.Close(ctx) }() //nolint:errcheck // best-effort close
	validationCtx, cancelValidation := context.WithTimeout(ctx, 30*time.Second)
	defer cancelValidation()
	validationPID := validationConn.PgConn().PID()
	validationDone := make(chan error, 1)
	go func() {
		statements := []string{
			"BEGIN",
			validationMigration,
			"ALTER TABLE chats VALIDATE CONSTRAINT " + chatGateName,
			"ALTER TABLE channels VALIDATE CONSTRAINT " + channelGateName,
			"COMMIT",
		}
		for _, statement := range statements {
			if _, err := validationConn.Exec(validationCtx, statement); err != nil {
				validationDone <- err
				return
			}
		}
		validationDone <- nil
	}()

	waitForPermissionStateValidationGate(t, ctx, conn, validationPID, chatGateName, validationDone)
	exercisePermissionStateReadsAndWrites(t, ctx, conn, chatID, bulkChatID, channelID, bulkChannelID, memberID)
	unlockPermissionStateValidationGate(t, ctx, gateConn, chatLockKey)

	waitForPermissionStateValidationGate(t, ctx, conn, validationPID, channelGateName, validationDone)
	exercisePermissionStateReadsAndWrites(t, ctx, conn, chatID, bulkChatID, channelID, bulkChannelID, memberID)
	unlockPermissionStateValidationGate(t, ctx, gateConn, channelLockKey)
	select {
	case err := <-validationDone:
		if err != nil {
			t.Fatalf("validate permission state constraints: %v", err)
		}
	case <-validationCtx.Done():
		t.Fatalf("validation transaction did not finish: %v", validationCtx.Err())
	}

	for _, constraint := range []struct {
		table string
		name  string
	}{
		{table: "chats", name: chatGateName},
		{table: "channels", name: channelGateName},
	} {
		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", constraint.table, constraint.name)); err != nil {
			t.Fatalf("drop %s validation gate: %v", constraint.name, err)
		}
	}
	if _, err := conn.Exec(ctx, `DROP FUNCTION test_permission_state_validation_gate(BIGINT, BIGINT, INTEGER, TEXT)`); err != nil {
		t.Fatalf("drop validation gate function: %v", err)
	}
}

func waitForPermissionStateValidationGate(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	validationPID uint32,
	constraintName string,
	validationDone <-chan error,
) {
	t.Helper()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := conn.QueryRow(waitCtx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE pid = $1 AND wait_event_type = 'Lock' AND wait_event = 'advisory'
				  AND position($2 IN query) > 0
			)
		`, validationPID, constraintName).Scan(&waiting); err != nil {
			t.Fatalf("check validation gate for %s: %v", constraintName, err)
		}
		if waiting {
			return
		}
		select {
		case err := <-validationDone:
			t.Fatalf("validation finished before reaching %s gate: %v", constraintName, err)
		case <-waitCtx.Done():
			t.Fatalf("validation did not reach %s gate: %v", constraintName, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func unlockPermissionStateValidationGate(t *testing.T, ctx context.Context, conn *pgx.Conn, key int32) {
	t.Helper()
	var unlocked bool
	if err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()), $1)`, key).Scan(&unlocked); err != nil {
		t.Fatalf("release validation gate lock %d: %v", key, err)
	}
	if !unlocked {
		t.Fatalf("validation gate lock %d was not held", key)
	}
}

func exercisePermissionStateReadsAndWrites(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	chatID int64,
	bulkChatID int64,
	channelID int64,
	bulkChannelID int64,
	memberID int64,
) {
	t.Helper()
	accessCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var title string
	if err := conn.QueryRow(accessCtx, `SELECT title FROM chats WHERE id = $1`, chatID).Scan(&title); err != nil {
		t.Fatalf("read chat during validation: %v", err)
	}
	tag, err := conn.Exec(accessCtx, `UPDATE chats SET version = version WHERE id = $1`, bulkChatID)
	if err != nil {
		t.Fatalf("write chat during validation: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("chat writes during validation affected %d rows, want 1", tag.RowsAffected())
	}
	if err := conn.QueryRow(accessCtx, `SELECT title FROM channels WHERE id = $1`, channelID).Scan(&title); err != nil {
		t.Fatalf("read channel during validation: %v", err)
	}
	tag, err = conn.Exec(accessCtx, `UPDATE channels SET version = version WHERE id = $1`, bulkChannelID)
	if err != nil {
		t.Fatalf("write channel during validation: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("channel writes during validation affected %d rows, want 1", tag.RowsAffected())
	}
	var lastPostAt *time.Time
	if err := conn.QueryRow(accessCtx, `
		SELECT last_post_at FROM channel_participants WHERE channel_id = $1 AND user_id = $2
	`, channelID, memberID).Scan(&lastPostAt); err != nil {
		t.Fatalf("read participant during validation: %v", err)
	}
	tag, err = conn.Exec(accessCtx, `
		UPDATE channel_participants SET last_post_at = last_post_at WHERE channel_id = $1 AND user_id = $2
	`, channelID, memberID)
	if err != nil {
		t.Fatalf("write participant during validation: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("participant writes during validation affected %d rows, want 1", tag.RowsAffected())
	}
}
