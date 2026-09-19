package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestRunCommandMaintenanceAssignOperator(t *testing.T) {
	dsn := pgtest.DSN(t)
	t.Setenv("TG_POSTGRES_DSN", dsn)
	t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("2a", 32))
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_REGISTRATION", "closed")
	rsaKeyPath := filepath.Join(t.TempDir(), "must-not-be-created.pem")
	blobDir := filepath.Join(t.TempDir(), "must-not-be-created")
	t.Setenv("TG_RSA_KEY_PATH", rsaKeyPath)
	t.Setenv("TG_BLOB_DIR", blobDir)

	seedMaintenanceOperator(t, dsn)

	var stdout, stderr bytes.Buffer
	if err := runCommand([]string{"maintenance", "assign-operator"}, slog.New(slog.DiscardHandler), &stdout, &stderr); err != nil {
		t.Fatalf("maintenance command: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if stderr.String() != "Operator server administrator assigned\n" {
		t.Fatalf("stderr = %q, want confirmation", stderr.String())
	}
	if _, err := os.Stat(rsaKeyPath); !os.IsNotExist(err) {
		t.Fatalf("maintenance command created RSA key, stat error = %v", err)
	}
	if _, err := os.Stat(blobDir); !os.IsNotExist(err) {
		t.Fatalf("maintenance command created blob directory, stat error = %v", err)
	}
}

func TestRunCommandMaintenanceAssignOperatorRequiresExactInvocation(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	for _, args := range [][]string{
		{"maintenance"},
		{"maintenance", "assign-operator", "1"},
		{"maintenance", "other"},
	} {
		if err := runCommand(args, slog.New(slog.DiscardHandler), &stdout, &stderr); err == nil {
			t.Fatalf("runCommand(%v) succeeded, want usage error", args)
		}
		stdout.Reset()
		stderr.Reset()
	}
}

func seedMaintenanceOperator(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("create fixture blob store: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open fixture store: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("close fixture store: %v", err)
		}
	}()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close fixture database: %v", err)
		}
	}()

	operator, err := s.CreateUsernameUser(ctx, "operator", "Operator", "")
	if err != nil {
		t.Fatalf("create operator fixture: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO usernames (handle, owner_type, owner_id)
		VALUES ('operator', 'user', $1)
	`, operator.ID); err != nil {
		t.Fatalf("claim operator fixture: %v", err)
	}
	if _, err := conn.Exec(ctx,
		`UPDATE users SET username = 'operator' WHERE id = $1`, operator.ID); err != nil {
		t.Fatalf("set operator fixture username: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   operator.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: []byte("readable verifier"),
	}); err != nil {
		t.Fatalf("set operator fixture verifier: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE server_administration
		SET election_closed = TRUE, administrator_user_id = NULL
		WHERE singleton_id = 1
	`); err != nil {
		t.Fatalf("close unassigned election fixture: %v", err)
	}
}
