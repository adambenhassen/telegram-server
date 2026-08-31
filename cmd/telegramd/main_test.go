package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestNewBlobStoreDefaultsToLocal(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "blobs")
	got, err := newBlobStore(context.Background(), config.Config{BlobDir: root}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	local, ok := got.(*blob.Local)
	if !ok {
		t.Fatalf("blob store type = %T, want *blob.Local", got)
	}
	if local.RootDir() != root {
		t.Fatalf("local root = %q, want %q", local.RootDir(), root)
	}
}

func TestNewBlobStoreChecksConfiguredS3AtStartup(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("startup request = %s %s, want S3 bucket listing", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	got, err := newBlobStore(context.Background(), config.Config{BlobS3: &blob.S3Config{
		Endpoint:          server.URL,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
	}}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	if _, ok := got.(*blob.S3); !ok {
		t.Fatalf("blob store type = %T, want *blob.S3", got)
	}
	if requests.Load() != 1 {
		t.Fatalf("startup requests = %d, want 1", requests.Load())
	}
}

func TestNewBlobStoreDoesNotFallBackWhenS3IsUnreachable(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL
	server.Close()
	root := filepath.Join(t.TempDir(), "blobs")
	_, err := newBlobStore(context.Background(), config.Config{BlobDir: root, BlobS3: &blob.S3Config{
		Endpoint:          endpoint,
		Bucket:            "test-bucket",
		Prefix:            "tenant/",
		AccessKeyID:       "access",
		SecretAccessKey:   "secret",
		AllowInsecureHTTP: true,
		MaxAttempts:       1,
		OperationTimeout:  100 * time.Millisecond,
	}}, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "startup") {
		t.Fatalf("new blob store error = %v, want startup failure", err)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("local fallback created %q, stat error = %v", root, statErr)
	}
}

type commandRemoveProbe struct {
	blob.Store

	removed chan struct{}
	once    sync.Once
}

func (p *commandRemoveProbe) Remove(ctx context.Context, key string) error {
	p.once.Do(func() { close(p.removed) })
	return p.Store.Remove(ctx, key)
}

// The destructive off-switch is decided in cmd/telegramd, not in the Store.
// Calling the command-layer pass directly proves that an aged assembled temp
// remains untouched when the deployment is in its default reporting mode.
func TestMediaErasureDestructiveOffSwitchAtCommandBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	probe := &commandRemoveProbe{Store: l, removed: make(chan struct{})}
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(probe))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	u, err := s.CreateUser(ctx, "+15559301001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	f, err := s.AllocateFile(ctx, u.ID, 1, "application/octet-stream", "upload.bin", 1<<20)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	key := blob.Key(f.ID) + blob.TempSuffix
	if _, err := l.Put(ctx, key, bytes.NewReader([]byte("pending"))); err != nil {
		t.Fatalf("put temp: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(l.RootDir(), filepath.FromSlash(key)), old, old); err != nil {
		t.Fatalf("age temp: %v", err)
	}

	cfg := config.Config{
		MediaErasureDestructive: false,
		MediaErasureMinAge:      time.Hour,
		BlobScanTempMinAge:      time.Nanosecond,
	}
	log := slog.New(slog.DiscardHandler)
	sweepMediaErasurePass(ctx, s, cfg, log)

	select {
	case <-probe.removed:
		t.Fatal("reporting mode reached the blob backend's Remove")
	default:
	}
	if _, err := l.ReadAt(ctx, key, 0, 1); err != nil {
		t.Fatalf("reporting mode removed aged temp: %v", err)
	}
}

// The temporary-file age is only the gate for the disk-side temporary class.
// A not-stored row still uses the media age, so a temp cutoff shortened for
// disk cleanup cannot erase a row that has not crossed the media cutoff.
func TestMediaErasureUsesSeparateTempAndRowCutoffs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	l, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(l))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	u, err := s.CreateUser(ctx, "+15559301002")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	f, err := s.AllocateFile(ctx, u.ID, 1, "application/octet-stream", "upload.bin", 1<<20)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to age row: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE files SET date = $2 WHERE id = $1`, f.ID, old); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after the failed setup query
		t.Fatalf("age file row: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close age connection: %v", err)
	}

	key := blob.Key(f.ID) + blob.TempSuffix
	if _, err := l.Put(ctx, key, bytes.NewReader([]byte("pending"))); err != nil {
		t.Fatalf("put temp: %v", err)
	}
	if err := os.Chtimes(filepath.Join(l.RootDir(), filepath.FromSlash(key)), old, old); err != nil {
		t.Fatalf("age temp: %v", err)
	}

	sweepMediaErasurePass(ctx, s, config.Config{
		MediaErasureDestructive: true,
		MediaErasureMinAge:      24 * time.Hour,
		BlobScanTempMinAge:      time.Hour,
	}, slog.New(slog.DiscardHandler))

	var rowExists bool
	conn, err = pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to inspect row: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM files WHERE id = $1)`, f.ID).Scan(&rowExists); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after the failed inspection query
		t.Fatalf("inspect file row: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close inspection connection: %v", err)
	}
	if !rowExists {
		t.Fatal("short temporary cutoff erased a not-stored row before the media cutoff")
	}
	if _, err := l.ReadAt(ctx, key, 0, 1); err != nil {
		t.Fatalf("short temporary cutoff removed the temporary: %v", err)
	}
}

func TestRunInviteCommandIssueAndList(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", pgtest.DSN(t))
	t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("00", 32))
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_REGISTRATION", "open")
	blobDir := filepath.Join(t.TempDir(), "blobs")
	rsaKeyPath := filepath.Join(t.TempDir(), "server-key.pem")
	t.Setenv("TG_BLOB_DIR", blobDir)
	t.Setenv("TG_RSA_KEY_PATH", rsaKeyPath)

	log := slog.New(slog.DiscardHandler)
	var stdout, stderr bytes.Buffer
	if err := runInviteCommand([]string{"issue", "Alice", "--lifetime", "48h"}, log, &stdout, &stderr); err != nil {
		t.Fatalf("issue command: %v", err)
	}
	secret := strings.TrimSpace(stdout.String())
	if len(secret) != 64 {
		t.Fatalf("secret = %q, want 64 characters", secret)
	}
	if strings.Trim(secret, "0123456789abcdef") != "" {
		t.Fatalf("secret = %q, want lowercase hex", secret)
	}
	if !strings.Contains(stderr.String(), "Invite issued: id=") || !strings.Contains(stderr.String(), "handle=alice") {
		t.Fatalf("issue metadata = %q, want id and canonical handle", stderr.String())
	}
	if strings.Contains(stderr.String(), secret) {
		t.Fatal("issue metadata contains the secret")
	}
	var id int64
	if _, err := fmt.Sscanf(stderr.String(), "Invite issued: id=%d", &id); err != nil {
		t.Fatalf("issue metadata = %q, parse id: %v", stderr.String(), err)
	}

	stdout.Reset()
	stderr.Reset()
	if err := runInviteCommand([]string{"list"}, log, &stdout, &stderr); err != nil {
		t.Fatalf("list command: %v", err)
	}
	if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
		t.Fatal("list output contains the secret")
	}
	for _, want := range []string{"ID", "HANDLE", "STATE", "ISSUED", "EXPIRES", "alice", "issued"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("list output = %q, missing %q", stdout.String(), want)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("list stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	err := runInviteCommand([]string{"issue", "alice"}, log, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "alice") {
		t.Fatalf("duplicate issue error = %v, want an error naming alice", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("duplicate issue stdout = %q, want empty", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := runInviteCommand([]string{"revoke", strconv.FormatInt(id, 10)}, log, &stdout, &stderr); err != nil {
		t.Fatalf("revoke command: %v", err)
	}
	if stdout.Len() != 0 || stderr.String() != fmt.Sprintf("Revoked: id=%d\n", id) {
		t.Fatalf("revoke output = stdout %q stderr %q", stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := runInviteCommand([]string{"list"}, log, &stdout, &stderr); err != nil {
		t.Fatalf("list after revoke: %v", err)
	}
	if !strings.Contains(stdout.String(), "revoked") {
		t.Fatalf("list after revoke = %q, missing revoked state", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("list after revoke stderr = %q, want empty", stderr.String())
	}

	if _, err := os.Stat(blobDir); !os.IsNotExist(err) {
		t.Fatalf("invite command initialized blob dir, stat error = %v", err)
	}
	if _, err := os.Stat(rsaKeyPath); !os.IsNotExist(err) {
		t.Fatalf("invite command initialized RSA key, stat error = %v", err)
	}
}

func TestParseInviteIssueArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []string
		handle   string
		lifetime time.Duration
		wantErr  bool
	}{
		{name: "default", args: []string{"Alice"}, handle: "Alice"},
		{name: "explicit", args: []string{"alice", "--lifetime", "48h"}, handle: "alice", lifetime: 48 * time.Hour},
		{name: "missing handle", wantErr: true},
		{name: "unknown option", args: []string{"alice", "--other", "48h"}, wantErr: true},
		{name: "invalid duration", args: []string{"alice", "--lifetime", "later"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handle, lifetime, err := parseInviteIssueArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("parse succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if handle != tc.handle || lifetime != tc.lifetime {
				t.Fatalf("parse = handle %q lifetime %s, want %q %s", handle, lifetime, tc.handle, tc.lifetime)
			}
		})
	}
}
