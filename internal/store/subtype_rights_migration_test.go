package store_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestFileSubtypeRightsMigrationPreservesLegacyAndStoresNewStates(t *testing.T) {
	ctx := context.Background()
	const (
		administrationMigration = "20260913000039_server_administration.sql"
		columnMigration         = "20260930000043_file_subtype_rights.sql"
		validationMigration     = "20260930000044_validate_file_subtype_rights.sql"
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

	var uploaderID, peerID, targetID, legacyFileID int64
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(&uploaderID); err != nil {
		t.Fatalf("seed uploader: %v", err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(&peerID); err != nil {
		t.Fatalf("seed source peer: %v", err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO users (phone) VALUES ($1) RETURNING id`, "+1555"+pgtest.RandomHex()).Scan(&targetID); err != nil {
		t.Fatalf("seed forward target: %v", err)
	}
	if err := conn.QueryRow(ctx, `
		INSERT INTO files (uploader_id, access_hash, size, mime_type, file_name, stored)
		VALUES ($1, 1, 7, 'application/octet-stream', 'legacy.bin', true)
		RETURNING id
	`, uploaderID).Scan(&legacyFileID); err != nil {
		t.Fatalf("seed legacy file: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO messages (owner_id, local_id, peer_type, peer_id, from_id, date, message, out, file_id)
		VALUES ($1, 1, 1, $2, $1, '2020-01-02T03:04:05Z', 'legacy media', true, $3)
	`, uploaderID, peerID, legacyFileID); err != nil {
		t.Fatalf("seed legacy media message: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO update_state (user_id, pts, next_local_id) VALUES ($1, 0, 2)`, uploaderID); err != nil {
		t.Fatalf("seed uploader update state: %v", err)
	}
	if _, err := conn.Exec(ctx, readMigration(administrationMigration)); err != nil {
		t.Fatalf("apply server administration migration: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() <= administrationMigration || entry.Name() >= columnMigration {
			continue
		}
		if _, err := conn.Exec(ctx, readMigration(entry.Name())); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}

	if _, err := conn.Exec(ctx, readMigration(columnMigration)); err != nil {
		t.Fatalf("apply subtype rights column migration: %v", err)
	}
	var validated bool
	if err := conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'files_subtype_rights_valid'`).Scan(&validated); err != nil {
		t.Fatalf("read subtype rights validation state: %v", err)
	}
	if validated {
		t.Fatal("subtype rights check validated with the column migration; want validation after its DDL lock is released")
	}

	var rightsNull bool
	var size int64
	var mimeType, fileName string
	var stored bool
	if err := conn.QueryRow(ctx, `
		SELECT subtype_rights IS NULL, size, mime_type, file_name, stored
		FROM files WHERE id = $1
	`, legacyFileID).Scan(&rightsNull, &size, &mimeType, &fileName, &stored); err != nil {
		t.Fatalf("read legacy file after column migration: %v", err)
	}
	if !rightsNull || size != 7 || mimeType != "application/octet-stream" || fileName != "legacy.bin" || !stored {
		t.Fatalf("legacy file after column migration = null %v, size %d, MIME %q, name %q, stored %v", rightsNull, size, mimeType, fileName, stored)
	}
	var message string
	var messageDate time.Time
	var messageFileID int64
	if err := conn.QueryRow(ctx, `SELECT message, date, file_id FROM messages WHERE owner_id = $1 AND local_id = 1`, uploaderID).Scan(&message, &messageDate, &messageFileID); err != nil {
		t.Fatalf("read legacy message after column migration: %v", err)
	}
	if message != "legacy media" || messageFileID != legacyFileID {
		t.Fatalf("legacy message after column migration = %q with file %d, want original data and file %d", message, messageFileID, legacyFileID)
	}

	if _, err := conn.Exec(ctx, readMigration(validationMigration)); err != nil {
		t.Fatalf("apply subtype rights validation migration: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'files_subtype_rights_valid'`).Scan(&validated); err != nil {
		t.Fatalf("read validated subtype rights constraint: %v", err)
	}
	if !validated {
		t.Fatal("subtype rights check remains unvalidated")
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() <= validationMigration {
			continue
		}
		if _, err := conn.Exec(ctx, readMigration(entry.Name())); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}

	blobs := testBlobs(t)
	s, err := store.Open(ctx, pgtest.DSNFrom(dbName), pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	put := func(body string) func(store.File) error {
		return func(file store.File) error {
			_, err := blobs.Put(ctx, blob.Key(file.ID), bytes.NewReader([]byte(body)))
			return err
		}
	}
	generic, err := s.AllocateAndCompleteFile(ctx, uploaderID, 7, "application/octet-stream", "generic.bin", bigQuota, []string{}, put("generic"))
	if err != nil {
		t.Fatalf("allocate known generic file: %v", err)
	}
	sticker, err := s.AllocateAndCompleteFile(ctx, uploaderID, 7, "application/octet-stream", "sticker.bin", bigQuota, []string{"send_stickers"}, put("sticker"))
	if err != nil {
		t.Fatalf("allocate classified sticker file: %v", err)
	}

	files, err := s.FilesByIDs(ctx, []int64{legacyFileID, generic.ID, sticker.ID})
	if err != nil {
		t.Fatalf("read stored files: %v", err)
	}
	if files[legacyFileID].SubtypeRights != nil {
		t.Fatalf("legacy subtype rights = %v, want unknown nil", files[legacyFileID].SubtypeRights)
	}
	if files[generic.ID].SubtypeRights == nil || len(files[generic.ID].SubtypeRights) != 0 {
		t.Fatalf("generic subtype rights = %v, want known empty set", files[generic.ID].SubtypeRights)
	}
	if len(files[sticker.ID].SubtypeRights) != 1 || files[sticker.ID].SubtypeRights[0] != "send_stickers" {
		t.Fatalf("sticker subtype rights = %v, want [send_stickers]", files[sticker.ID].SubtypeRights)
	}

	_, forwarded, err := s.ForwardMessages(ctx, uploaderID, store.PeerTypeUser, targetID, []store.ForwardSource{{
		FromID: uploaderID, Date: messageDate, Text: message, FileID: legacyFileID,
	}}, []int64{93001})
	if err != nil {
		t.Fatalf("forward legacy file: %v", err)
	}
	if len(forwarded) != 1 || forwarded[0].Message.FileID != legacyFileID {
		t.Fatalf("forwarded messages = %+v, want one referencing original file %d", forwarded, legacyFileID)
	}
	var forwardedFileID int64
	if err := conn.QueryRow(ctx, `SELECT file_id FROM messages WHERE owner_id = $1 AND random_id = $2`, uploaderID, int64(93001)).Scan(&forwardedFileID); err != nil {
		t.Fatalf("read forwarded message: %v", err)
	}
	if forwardedFileID != legacyFileID {
		t.Fatalf("forwarded file id = %d, want original %d", forwardedFileID, legacyFileID)
	}
	files, err = s.FilesByIDs(ctx, []int64{legacyFileID})
	if err != nil {
		t.Fatalf("read legacy file after forward: %v", err)
	}
	if files[legacyFileID].SubtypeRights != nil {
		t.Fatalf("legacy subtype rights after forward = %v, want unknown nil", files[legacyFileID].SubtypeRights)
	}

	if _, err := conn.Exec(ctx, `UPDATE files SET subtype_rights = ARRAY['not_a_restriction']::TEXT[] WHERE id = $1`, generic.ID); err == nil {
		t.Fatal("files accepted an unrecognized subtype right")
	}
}
