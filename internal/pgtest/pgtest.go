// Package pgtest provides a fast, isolated Postgres for tests: one reusable
// container for the whole suite, with each call to DSN returning a fresh
// database cloned from a schema template. Safe for t.Parallel().
package pgtest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Schema is the DDL applied to the template database. Task 2 sets this to
// store.Schema via internal/pgtest/schema.go. Empty means an empty template.
var Schema string

const (
	containerName   = "tg-test-pg"
	templatePrefix  = "tg_template_"
	advisoryLockKey = 0x7467 // "tg"
)

var (
	once         sync.Once
	adminDSN     string // connection string to the admin ("postgres") database
	templateName string // content-addressed template DB, resolved at setup
	errSetup     error
)

// schemaHash returns a short hex digest of the current Schema so the template
// is content-addressed: an empty Schema and store.Schema map to different
// template names, so changing Schema builds a new template instead of reusing
// a stale one from the reusable container.
func schemaHash() string {
	sum := sha256.Sum256([]byte(Schema))
	return hex.EncodeToString(sum[:])[:12]
}

func setup() {
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		testcontainers.WithReuseByName(containerName),
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithConfigFile(fastConfPath()),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		errSetup = fmt.Errorf("start container: %w", err)
		return
	}
	adminDSN, errSetup = container.ConnectionString(ctx, "sslmode=disable")
	if errSetup != nil {
		return
	}
	templateName = templatePrefix + schemaHash()
	errSetup = ensureTemplate(ctx)
}

// ensureTemplate builds the content-addressed template crash-safely: the schema
// is applied to a scratch "<name>_building" database that is only renamed to the
// canonical templateName once fully built. So the canonical name existing always
// means "complete", closing the partial-init-on-crash window. The whole
// check-build-rename is serialized by a pg advisory lock across parallel test
// binaries sharing the reusable container.
// ponytail: old-hash templates from earlier Schema versions are left behind;
// harmless in an ephemeral test container, GC them if they ever pile up.
func ensureTemplate(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("admin connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close of admin conn

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryLockKey) }() //nolint:errcheck // best-effort unlock; conn close releases it anyway

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1)`, templateName,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check template: %w", err)
	}
	if exists {
		return nil
	}

	// Identifiers below are built from a constant prefix + hex hash, never user
	// input; Postgres DDL cannot bind identifiers as parameters (no G201 risk).
	building := templateName + "_building"
	if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+building+` WITH (FORCE)`); err != nil {
		return fmt.Errorf("drop stale building: %w", err)
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+building); err != nil {
		return fmt.Errorf("create building: %w", err)
	}
	if Schema != "" {
		if err := applySchema(ctx, building); err != nil {
			return err
		}
	}
	if _, err := conn.Exec(ctx, `ALTER DATABASE `+building+` RENAME TO `+templateName); err != nil {
		return fmt.Errorf("promote template: %w", err)
	}
	return nil
}

func applySchema(ctx context.Context, db string) error {
	tconn, err := pgx.Connect(ctx, replaceDBName(adminDSN, db))
	if err != nil {
		return fmt.Errorf("template connect: %w", err)
	}
	defer func() { _ = tconn.Close(ctx) }() //nolint:errcheck // best-effort close of template conn
	if _, err := tconn.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// DSN clones a fresh database from the template and returns its connection
// string. The database is dropped on test cleanup. Safe for t.Parallel().
func DSN(tb testing.TB) string {
	tb.Helper()
	once.Do(setup)
	if errSetup != nil {
		tb.Fatalf("pgtest setup: %v", errSetup)
	}

	ctx := context.Background()
	name := "t_" + randName(tb)

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		tb.Fatalf("admin connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close of admin conn

	// G201 is not a risk here: name is internally-generated hex and templateName
	// is a constant prefix + hex hash; Postgres DDL cannot bind identifiers.
	if _, err := conn.Exec(ctx,
		fmt.Sprintf(`CREATE DATABASE %s TEMPLATE %s`, name, templateName),
	); err != nil {
		tb.Fatalf("clone db: %v", err)
	}
	tb.Cleanup(func() {
		dctx := context.Background()
		dconn, err := pgx.Connect(dctx, adminDSN)
		if err != nil {
			return
		}
		defer func() { _ = dconn.Close(dctx) }()                                              //nolint:errcheck // best-effort close of cleanup conn
		_, _ = dconn.Exec(dctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name)) //nolint:errcheck // best-effort drop on cleanup
	})

	return replaceDBName(adminDSN, name)
}

func randName(tb testing.TB) string {
	tb.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		tb.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}

// fastConfPath resolves the config file relative to THIS source file, not the
// caller's cwd — DSN is invoked from multiple test packages (internal/store,
// test/e2e), each with a different working directory.
func fastConfPath() string {
	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only need the file path
	return filepath.Join(filepath.Dir(thisFile), "testdata", "postgres-fast.conf")
}

func replaceDBName(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + db
	return u.String()
}
