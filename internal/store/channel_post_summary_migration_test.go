package store_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestChannelPostSummaryMigrationIsAdditiveAndTransactional(t *testing.T) {
	ctx := context.Background()
	const migrationName = "20261002000049_channel_post_summaries.sql"
	migrationsDir := filepath.Join("..", "..", "migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	migrationBytes, err := os.ReadFile(filepath.Join(migrationsDir, migrationName))
	if err != nil {
		t.Fatalf("read summary migration: %v", err)
	}

	admin, err := pgx.Connect(ctx, pgtest.AdminDSN())
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer func() {
		if err := admin.Close(ctx); err != nil {
			t.Errorf("close admin: %v", err)
		}
	}()
	dbName := "t_" + pgtest.RandomHex()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create disposable database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		conn, err := pgx.Connect(cleanupCtx, pgtest.AdminDSN())
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(cleanupCtx) }() //nolint:errcheck // best-effort close
		if _, err := conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)"); err != nil {
			t.Logf("cleanup database %s: %v", dbName, err)
		}
	})

	conn, err := pgx.Connect(ctx, pgtest.DSNFrom(dbName))
	if err != nil {
		t.Fatalf("connect disposable database: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close disposable database: %v", err)
		}
	}()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= migrationName {
			continue
		}
		body, readErr := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if readErr != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), readErr)
		}
		if _, err = conn.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}
	if _, err = conn.Exec(ctx, `UPDATE server_administration SET election_closed = true WHERE singleton_id = 1`); err != nil {
		t.Fatalf("close empty server administrator election for legacy fixture: %v", err)
	}

	var creatorID, readerID, newMemberID int64
	for _, target := range []*int64{&creatorID, &readerID, &newMemberID} {
		if err = conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(target); err != nil {
			t.Fatalf("seed user: %v", err)
		}
	}
	const channelID int64 = 3000000001
	if _, err = conn.Exec(ctx, `INSERT INTO channels (id, title, creator_id) VALUES ($1, 'legacy', $2)`, channelID, creatorID); err != nil {
		t.Fatalf("seed legacy channel: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO channel_state (channel_id, pts, next_local_id) VALUES ($1, 2, 3)`, channelID); err != nil {
		t.Fatalf("seed legacy state: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		VALUES ($1, $2, 2, 0), ($1, $3, 0, 0)
	`, channelID, creatorID, readerID); err != nil {
		t.Fatalf("seed legacy participants: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_messages (channel_id, local_id, from_id, message, deleted)
		VALUES ($1, 1, $2, 'live legacy post', false), ($1, 2, $2, 'deleted legacy post', true)
	`, channelID, creatorID); err != nil {
		t.Fatalf("seed legacy posts: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_events (channel_id, pts, type, local_id)
		VALUES ($1, 1, 1, 1), ($1, 2, 1, 2)
	`, channelID); err != nil {
		t.Fatalf("seed legacy events: %v", err)
	}
	beforeMigration, err := migrationSourceFingerprints(ctx, conn, channelID)
	if err != nil {
		t.Fatalf("fingerprint legacy source before migration: %v", err)
	}

	failedTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed migration transaction: %v", err)
	}
	if _, err = failedTx.Exec(ctx, string(migrationBytes)); err != nil {
		t.Fatalf("execute summary migration in transaction: %v", err)
	}
	if _, err = failedTx.Exec(ctx, `SELECT 1 / 0`); err == nil {
		t.Fatal("forced migration failure succeeded")
	}
	if err = failedTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed migration transaction: %v", err)
	}
	var failedSchemaWasRolledBack bool
	if err = conn.QueryRow(ctx, `
		SELECT to_regclass('public.channel_read_state') IS NULL
		   AND to_regclass('public.channel_post_summary_state') IS NULL
		   AND to_regclass('public.channel_post_summaries') IS NULL
	`).Scan(&failedSchemaWasRolledBack); err != nil || !failedSchemaWasRolledBack {
		t.Fatalf("failed migration left partial schema: rolled_back=%v err=%v", failedSchemaWasRolledBack, err)
	}
	afterFailure, err := migrationSourceFingerprints(ctx, conn, channelID)
	if err != nil || beforeMigration != afterFailure {
		t.Fatalf("failed migration changed source: before %v after %v err %v", beforeMigration, afterFailure, err)
	}

	applyTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin successful migration: %v", err)
	}
	if _, err = applyTx.Exec(ctx, string(migrationBytes)); err != nil {
		_ = applyTx.Rollback(ctx) //nolint:errcheck // return the migration error below
		t.Fatalf("apply summary migration: %v", err)
	}
	if err = applyTx.Commit(ctx); err != nil {
		t.Fatalf("commit summary migration: %v", err)
	}
	afterMigration, err := migrationSourceFingerprints(ctx, conn, channelID)
	if err != nil || beforeMigration != afterMigration {
		t.Fatalf("migration changed source: before %v after %v err %v", beforeMigration, afterMigration, err)
	}
	var readinessRowCount, markerRowCount int64
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM channel_post_summary_state WHERE channel_id = $1`, channelID).Scan(&readinessRowCount); err != nil {
		t.Fatalf("read preexisting channel readiness rows: %v", err)
	}
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM channel_read_state WHERE channel_id = $1`, channelID).Scan(&markerRowCount); err != nil {
		t.Fatalf("read preexisting channel markers: %v", err)
	}
	if readinessRowCount != 0 || markerRowCount != 0 {
		t.Fatalf("migration backfilled readiness=%d markers=%d, want neither", readinessRowCount, markerRowCount)
	}

	// An old binary's raw post is allowed while a legacy channel is unready. The
	// explicit initializer then includes it, without changing its source rows.
	if _, err = conn.Exec(ctx, `UPDATE channel_state SET next_local_id = 6 WHERE channel_id = $1`, channelID); err != nil {
		t.Fatalf("advance legacy post top: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_messages (channel_id, local_id, from_id, message)
		VALUES ($1, 3, $2, 'old binary post')
	`, channelID, creatorID); err != nil {
		t.Fatalf("old-binary post: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
		VALUES ($1, $2, 0, 2)
	`, channelID, newMemberID); err != nil {
		t.Fatalf("old-binary join: %v", err)
	}
	var marker int64
	if err = conn.QueryRow(ctx, `SELECT read_max_id FROM channel_read_state WHERE channel_id = $1 AND user_id = $2`, channelID, newMemberID).Scan(&marker); err != nil || marker != 5 {
		t.Fatalf("new membership marker = %d err %v; want committed top 5", marker, err)
	}
	if _, err = conn.Exec(ctx, `
		INSERT INTO channel_post_summary_state (channel_id, version, ready) VALUES ($1, 1, false)
		ON CONFLICT (channel_id) DO UPDATE SET ready = false;
	`, channelID); err != nil {
		t.Fatalf("mark legacy channel not ready: %v", err)
	}
	beforeInitialization, err := migrationSourceFingerprints(ctx, conn, channelID)
	if err != nil {
		t.Fatalf("fingerprint source before initialization: %v", err)
	}
	initTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin legacy initialization: %v", err)
	}
	if _, err = initTx.Exec(ctx, `SELECT initialize_channel_post_summaries($1)`, channelID); err != nil {
		_ = initTx.Rollback(ctx) //nolint:errcheck // return the initialization error below
		t.Fatalf("initialize legacy channel: %v", err)
	}
	if err = initTx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy initialization: %v", err)
	}
	afterInitialization, err := migrationSourceFingerprints(ctx, conn, channelID)
	if err != nil || beforeInitialization != afterInitialization {
		t.Fatalf("initialization changed source: before %v after %v err %v", beforeInitialization, afterInitialization, err)
	}
	if err = conn.QueryRow(ctx, `SELECT read_max_id FROM channel_read_state WHERE channel_id = $1 AND user_id = $2`, channelID, newMemberID).Scan(&marker); err != nil || marker != 5 {
		t.Fatalf("initialization changed existing member marker to %d err %v", marker, err)
	}
	var rootCount int64
	if err = conn.QueryRow(ctx, `
		SELECT live_count FROM channel_post_summaries
		WHERE channel_id = $1 AND scope_kind = 0 AND author_id = 0 AND depth = 0 AND prefix = 0
	`, channelID).Scan(&rootCount); err != nil || rootCount != 2 {
		t.Fatalf("initialized live root = %d err %v; want 2", rootCount, err)
	}
	if _, err = conn.Exec(ctx, `UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = 3`, channelID); err != nil {
		t.Fatalf("old-binary live deletion: %v", err)
	}
	if err = conn.QueryRow(ctx, `
		SELECT live_count FROM channel_post_summaries
		WHERE channel_id = $1 AND scope_kind = 0 AND author_id = 0 AND depth = 0 AND prefix = 0
	`, channelID).Scan(&rootCount); err != nil || rootCount != 1 {
		t.Fatalf("root after old-binary delete = %d err %v; want 1", rootCount, err)
	}
	if _, err = conn.Exec(ctx, `DELETE FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channelID, newMemberID); err != nil {
		t.Fatalf("old-binary leave: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, 0, 2)`, channelID, newMemberID); err != nil {
		t.Fatalf("old-binary rejoin: %v", err)
	}
	if err = conn.QueryRow(ctx, `SELECT read_max_id FROM channel_read_state WHERE channel_id = $1 AND user_id = $2`, channelID, newMemberID).Scan(&marker); err != nil || marker != 5 {
		t.Fatalf("rejoined marker = %d err %v; want current top 5", marker, err)
	}
	if err = conn.QueryRow(ctx, `
		SELECT version = 1 AND ready FROM channel_post_summary_state WHERE channel_id = $1
	`, channelID).Scan(&failedSchemaWasRolledBack); err != nil || !failedSchemaWasRolledBack {
		t.Fatalf("legacy readiness after initialization = %v err %v; want ready", failedSchemaWasRolledBack, err)
	}

	const newChannelID int64 = 3000000002
	if _, err = conn.Exec(ctx, `INSERT INTO channels (id, title, creator_id) VALUES ($1, 'new', $2)`, newChannelID, creatorID); err != nil {
		t.Fatalf("create channel through old schema writes: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO channel_state (channel_id, pts, next_local_id) VALUES ($1, 0, 1)`, newChannelID); err != nil {
		t.Fatalf("create new channel state: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, 2, 0)`, newChannelID, creatorID); err != nil {
		t.Fatalf("create new channel member: %v", err)
	}
	if err = conn.QueryRow(ctx, `SELECT version = 1 AND ready FROM channel_post_summary_state WHERE channel_id = $1`, newChannelID).Scan(&failedSchemaWasRolledBack); err != nil || !failedSchemaWasRolledBack {
		t.Fatalf("new channel readiness = %v err %v; want ready", failedSchemaWasRolledBack, err)
	}

	// Old-binary writes above exercise the database boundary. Reopen the current
	// application store twice to verify rollback/roll-forward and process restart
	// leave the additive schema, marker, readiness, and counts intact.
	for restart := range 2 {
		appStore, openErr := store.Open(ctx, pgtest.DSNFrom(dbName), pgtest.EncKey(), store.WithoutBlobStore())
		if openErr != nil {
			t.Fatalf("open application store on restart %d: %v", restart, openErr)
		}
		if installed, installErr := store.ChannelPostSummarySchemaInstalled(ctx, appStore); installErr != nil || !installed {
			_ = appStore.Close() //nolint:errcheck // preserve the schema assertion below
			t.Fatalf("summary schema after restart %d: installed=%v err=%v", restart, installed, installErr)
		}
		status, statusErr := appStore.ChannelPostSummaryReadiness(ctx, channelID)
		if statusErr != nil || status != store.ChannelPostSummaryReady {
			_ = appStore.Close() //nolint:errcheck // preserve the readiness assertion below
			t.Fatalf("legacy summary readiness after restart %d: status=%v err=%v", restart, status, statusErr)
		}
		if marker, markerErr := appStore.ChannelReadMarker(ctx, channelID, newMemberID); markerErr != nil || marker != 5 {
			_ = appStore.Close() //nolint:errcheck // preserve the marker assertion below
			t.Fatalf("marker after restart %d: marker=%d err=%v; want 5", restart, marker, markerErr)
		}
		if count, countErr := appStore.ChannelPostUnreadCount(ctx, channelID, newMemberID, 0); countErr != nil || count != 1 {
			_ = appStore.Close() //nolint:errcheck // preserve the count assertion below
			t.Fatalf("unread count after restart %d: count=%d err=%v; want 1", restart, count, countErr)
		}
		if closeErr := appStore.Close(); closeErr != nil {
			t.Fatalf("close application store after restart %d: %v", restart, closeErr)
		}
	}
}

func migrationSourceFingerprints(ctx context.Context, conn *pgx.Conn, channelID int64) ([4]string, error) {
	var fingerprints [4]string
	err := conn.QueryRow(ctx, `
		SELECT
		    md5(COALESCE((SELECT string_agg(row_to_json(post)::text, E'\\n' ORDER BY post.local_id)
		                    FROM channel_messages AS post WHERE post.channel_id = $1), '')),
		    md5(COALESCE((SELECT string_agg(row_to_json(member)::text, E'\\n' ORDER BY member.user_id)
		                    FROM channel_participants AS member WHERE member.channel_id = $1), '')),
		    md5(COALESCE((SELECT row_to_json(state)::text FROM channel_state AS state WHERE state.channel_id = $1), '')),
		    md5(COALESCE((SELECT string_agg(row_to_json(event)::text, E'\\n' ORDER BY event.pts)
		                    FROM channel_events AS event WHERE event.channel_id = $1), ''))
	`, channelID).Scan(&fingerprints[0], &fingerprints[1], &fingerprints[2], &fingerprints[3])
	return fingerprints, err
}
