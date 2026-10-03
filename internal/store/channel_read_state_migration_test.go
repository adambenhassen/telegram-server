package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

const channelReadStateMigration = "20261003000051_channel_post_summaries.sql"

func TestChannelReadStateMigrationInitializesFreshSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	opened, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open freshly migrated store: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("close fresh store: %v", err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer closeChannelReadStateTestConn(t, ctx, conn)
	var exists bool
	var defaultValue string
	if err := conn.QueryRow(ctx, `
		SELECT to_regclass('public.channel_read_state') IS NOT NULL,
		       (SELECT column_default FROM information_schema.columns
		        WHERE table_schema = 'public' AND table_name = 'channel_read_state'
	                  AND column_name = 'read_max_id')`).Scan(&exists, &defaultValue); err != nil {
		t.Fatalf("read fresh channel read-state schema: %v", err)
	}
	if !exists || !strings.Contains(defaultValue, "0") {
		t.Fatalf("fresh read-state table/default = %v/%q, want table with zero default", exists, defaultValue)
	}
}

func TestChannelReadStateMigrationUpgradesPopulatedDatabaseAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := openPreReadStateDatabase(t, ctx)
	migrationBody, err := os.ReadFile(filepath.Join("..", "..", "migrations", channelReadStateMigration))
	if err != nil {
		t.Fatalf("read channel read-state migration: %v", err)
	}

	adminTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first-user setup: %v", err)
	}
	var creatorID int64
	if err := adminTx.QueryRow(ctx, `INSERT INTO users (phone) VALUES ('15550000983') RETURNING id`).Scan(&creatorID); err != nil {
		rollbackChannelReadStateTestTx(t, ctx, adminTx)
		t.Fatalf("insert populated database user: %v", err)
	}
	if _, err := adminTx.Exec(ctx, `UPDATE server_administration SET election_closed = true, administrator_user_id = $1 WHERE singleton_id = 1`, creatorID); err != nil {
		rollbackChannelReadStateTestTx(t, ctx, adminTx)
		t.Fatalf("close first-user election: %v", err)
	}
	if err := adminTx.Commit(ctx); err != nil {
		t.Fatalf("commit first-user setup: %v", err)
	}

	const channelID int64 = 9183001
	if _, err := conn.Exec(ctx, `INSERT INTO channels (id, title, creator_id, megagroup) VALUES ($1, 'preserved', $2, false)`, channelID, creatorID); err != nil {
		t.Fatalf("insert existing channel: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_state (channel_id, pts, next_local_id) VALUES ($1, 1, 2)`, channelID); err != nil {
		t.Fatalf("insert existing channel state: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, 2, 1)`, channelID, creatorID); err != nil {
		t.Fatalf("insert existing membership: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_messages (channel_id, local_id, from_id, message) VALUES ($1, 1, $2, 'kept post')`, channelID, creatorID); err != nil {
		t.Fatalf("insert existing channel post: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_events (channel_id, pts, type, local_id) VALUES ($1, 1, 1, 1)`, channelID); err != nil {
		t.Fatalf("insert existing channel event: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_post_markers (channel_id, user_id, last_post_at) VALUES ($1, $2, now())`, channelID, creatorID); err != nil {
		t.Fatalf("insert existing channel post marker: %v", err)
	}

	failedTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin failed migration transaction: %v", err)
	}
	if _, err := failedTx.Exec(ctx, string(migrationBody)); err != nil {
		rollbackChannelReadStateTestTx(t, ctx, failedTx)
		t.Fatalf("execute migration before injected failure: %v", err)
	}
	if _, err := failedTx.Exec(ctx, `SELECT 1 / 0`); err == nil {
		rollbackChannelReadStateTestTx(t, ctx, failedTx)
		t.Fatal("injected migration failure unexpectedly succeeded")
	}
	if err := failedTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed migration: %v", err)
	}
	assertReadStateMigrationRollback(t, ctx, conn, channelID, creatorID)

	upgradeTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin upgrade transaction: %v", err)
	}
	if _, err := upgradeTx.Exec(ctx, string(migrationBody)); err != nil {
		rollbackChannelReadStateTestTx(t, ctx, upgradeTx)
		t.Fatalf("apply channel read-state migration: %v", err)
	}
	if err := upgradeTx.Commit(ctx); err != nil {
		t.Fatalf("commit channel read-state migration: %v", err)
	}

	assertPreservedChannelData(t, ctx, conn, channelID, creatorID)
	var markerRows int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_read_state`).Scan(&markerRows); err != nil {
		t.Fatalf("count migrated markers: %v", err)
	}
	if markerRows != 0 {
		t.Fatalf("migration backfilled %d channel read markers, want none", markerRows)
	}

	// These are the participant projection and insert shape used by the old
	// server. The additive table leaves both operations valid after upgrade.
	if _, err := conn.Exec(ctx, `
		SELECT channel_id, user_id, role, banned_until, join_pts, date, last_post_at
		FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channelID, creatorID); err != nil {
		t.Fatalf("legacy participant projection after upgrade: %v", err)
	}
	var secondUserID int64
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ('15550000984') RETURNING id`).Scan(&secondUserID); err != nil {
		t.Fatalf("insert second legacy user: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, 0, 1)`, channelID, secondUserID); err != nil {
		t.Fatalf("legacy membership insert after upgrade: %v", err)
	}
	assertPreservedChannelData(t, ctx, conn, channelID, creatorID)

	opened, err := store.Open(ctx, conn.Config().ConnString(), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open new server against upgraded schema: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("close upgraded store: %v", err)
	}
}

func openPreReadStateDatabase(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	migrations, err := os.ReadDir(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("read migrations directory: %v", err)
	}
	admin, err := pgx.Connect(ctx, pgtest.AdminDSN())
	if err != nil {
		t.Fatalf("connect to admin database: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(context.Background()); err != nil {
			t.Logf("close admin database connection: %v", err)
		}
	})
	name := "t_" + pgtest.RandomHex()
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create pre-migration database: %v", err)
	}
	t.Cleanup(func() {
		cleanup, err := pgx.Connect(context.Background(), pgtest.AdminDSN())
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer func() {
			if err := cleanup.Close(context.Background()); err != nil {
				t.Logf("cleanup close: %v", err)
			}
		}()
		if _, err := cleanup.Exec(context.Background(), `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
			t.Logf("cleanup drop %s: %v", name, err)
		}
	})

	dsn := pgtest.DSNFrom(name)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to pre-migration database: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Logf("close pre-migration database: %v", err)
		}
	})
	for _, migration := range migrations {
		if migration.IsDir() || !strings.HasSuffix(migration.Name(), ".sql") || migration.Name() >= channelReadStateMigration {
			continue
		}
		body, err := os.ReadFile(filepath.Join("..", "..", "migrations", migration.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", migration.Name(), err)
		}
		if _, err := conn.Exec(ctx, string(body)); err != nil {
			t.Fatalf("apply pre-change migration %s: %v", migration.Name(), err)
		}
	}
	return conn
}

func rollbackChannelReadStateTestTx(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("rollback channel read-state migration test transaction: %v", err)
	}
}

func closeChannelReadStateTestConn(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if err := conn.Close(ctx); err != nil {
		t.Errorf("close channel read-state migration test connection: %v", err)
	}
}

func assertReadStateMigrationRollback(t *testing.T, ctx context.Context, conn *pgx.Conn, channelID, creatorID int64) {
	t.Helper()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.channel_read_state') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check rolled-back read-state table: %v", err)
	}
	if exists {
		t.Fatal("failed migration left channel_read_state behind")
	}
	assertPreservedChannelData(t, ctx, conn, channelID, creatorID)
}

func assertPreservedChannelData(t *testing.T, ctx context.Context, conn *pgx.Conn, channelID, creatorID int64) {
	t.Helper()
	var members, posts, events, pts, nextID, postMarkers int
	var message string
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_participants WHERE channel_id = $1 AND user_id = $2`, channelID, creatorID).Scan(&members); err != nil {
		t.Fatalf("count preserved membership: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*), max(message) FROM channel_messages WHERE channel_id = $1`, channelID).Scan(&posts, &message); err != nil {
		t.Fatalf("read preserved channel post: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_events WHERE channel_id = $1`, channelID).Scan(&events); err != nil {
		t.Fatalf("count preserved channel events: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT pts, next_local_id FROM channel_state WHERE channel_id = $1`, channelID).Scan(&pts, &nextID); err != nil {
		t.Fatalf("read preserved channel state: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_post_markers WHERE channel_id = $1 AND user_id = $2`, channelID, creatorID).Scan(&postMarkers); err != nil {
		t.Fatalf("count preserved post markers: %v", err)
	}
	if members != 1 || posts != 1 || message != "kept post" || events != 1 || pts != 1 || nextID != 2 || postMarkers != 1 {
		t.Fatalf("pre-change data = members %d posts %d/%q events %d pts/next %d/%d post-markers %d, want 1/1/kept post/1/1/2/1", members, posts, message, events, pts, nextID, postMarkers)
	}
}
