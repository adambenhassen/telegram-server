// Package pgtest provides a fast, isolated Postgres for tests: one reusable
// container for the whole suite, with each call to DSN returning a fresh
// database cloned from a schema template. Safe for t.Parallel().
package pgtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnet "github.com/testcontainers/testcontainers-go/network"

	"github.com/adambenhassen/telegram-server/internal/peerhash"
)

// migration is one Atlas migration file: its name and SQL body.
type migration struct {
	name string
	sql  string
}

// migrations holds the ordered Atlas migrations applied to the template DB.
// Loaded once in setup from the repo's migrations/ directory.
var migrations []migration

const (
	containerName   = "tg-test-pg"
	templatePrefix  = "tg_template_"
	advisoryLockKey = 0x7467 // "tg"

	// readyTimeout bounds the wait for Postgres to accept connections, matching
	// testcontainers' own 60s startup deadline: long enough for a fresh
	// container's initdb-then-restart, short enough that a container which will
	// never answer fails the suite instead of hanging it.
	readyTimeout = 60 * time.Second
	// readyProbe bounds one connection attempt, readyInterval spaces them out.
	readyProbe    = 5 * time.Second
	readyInterval = 250 * time.Millisecond
)

var (
	once         sync.Once
	adminDSN     string // connection string to the admin ("postgres") database
	templateName string // content-addressed template DB, resolved at setup
	errSetup     error
)

// schemaHash returns a short hex digest of all migration bytes so the template
// is content-addressed: adding or changing a migration yields a new template
// name, so the reusable container rebuilds instead of serving a stale template.
func schemaHash(migs []migration) string {
	var buf []byte
	for _, m := range migs {
		buf = append(buf, m.sql...)
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])[:12]
}

func setup() {
	ctx := context.Background()
	migs, err := loadMigrations()
	if err != nil {
		errSetup = err
		return
	}
	migrations = migs
	container, err := postgres.Run(ctx, "postgres:16-alpine", containerOptions()...)
	if err != nil {
		errSetup = fmt.Errorf("start container: %w", err)
		return
	}
	// ponytail: bridge IP bypasses docker-proxy, which drops connections under load.
	inspect, err := container.Inspect(ctx)
	if err != nil {
		errSetup = fmt.Errorf("inspect container: %w", err)
		return
	}
	if bnet, ok := inspect.NetworkSettings.Networks["bridge"]; ok {
		adminDSN = fmt.Sprintf("postgres://postgres:postgres@%s/postgres?sslmode=disable", net.JoinHostPort(bnet.IPAddress.String(), "5432"))
	} else {
		// Reused container from before bridge attachment — fall back to published port.
		adminDSN, errSetup = container.ConnectionString(ctx, "sslmode=disable")
		if errSetup != nil {
			return
		}
	}
	if err := waitAccepting(ctx, adminDSN, readyTimeout); err != nil {
		errSetup = err
		return
	}
	templateName = templatePrefix + schemaHash(migrations)
	errSetup = ensureTemplate(ctx)
}

// containerOptions builds the create-or-reuse options for the shared container.
//
// Readiness is deliberately not a testcontainers wait strategy. The container is
// reused for days, and Docker's json-file log rotation eventually discards the
// "database system is ready to accept connections" startup lines that a log
// strategy counts, so a healthy Postgres starts failing every run with "matched
// 0 times, expected 2" — and reuse means it never boots again to write fresh
// ones. waitAccepting asks Postgres itself instead, which nothing can rotate
// away, and asks over the same DSN the tests use.
func containerOptions() []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithReuseByName(containerName),
		tcnet.WithBridgeNetwork(),
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithConfigFile(fastConfPath()),
	}
}

// waitAccepting polls dsn until Postgres answers a query, or timeout elapses.
// A fresh container is refusing connections through initdb and its restart; a
// container that never answers fails setup here with the last connection error.
func waitAccepting(ctx context.Context, dsn string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		err := probeAccepting(ctx, dsn)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres at %s not accepting connections within %s: %w", dsnHost(dsn), timeout, err)
		case <-time.After(readyInterval):
		}
	}
}

// probeAccepting runs one bounded connect-and-query against dsn.
func probeAccepting(ctx context.Context, dsn string) error {
	ctx, cancel := context.WithTimeout(ctx, readyProbe)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close of probe conn

	var one int
	if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	return nil
}

// dsnHost returns the host:port of a DSN for error messages, keeping the
// credentials out of them.
func dsnHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "unknown host"
	}
	return u.Host
}

// ensureTemplate builds the content-addressed template crash-safely: the schema
// is applied to a scratch "<name>_building" database that is only renamed to the
// canonical templateName once fully built. So the canonical name existing always
// means "complete", closing the partial-init-on-crash window. The whole
// check-build-rename is serialized by a pg advisory lock across parallel test
// binaries sharing the reusable container.
// ponytail: old-hash templates from earlier migration sets are left behind;
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
	if err := applyMigrations(ctx, building); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `ALTER DATABASE `+building+` RENAME TO `+templateName); err != nil {
		return fmt.Errorf("promote template: %w", err)
	}
	return nil
}

// applyMigrations execs each Atlas migration, in filename order, into db. The
// migrations are plain CREATE/ALTER statements, so no migration engine is
// needed — a sequential exec reproduces the full schema.
func applyMigrations(ctx context.Context, db string) error {
	tconn, err := pgx.Connect(ctx, replaceDBName(adminDSN, db))
	if err != nil {
		return fmt.Errorf("template connect: %w", err)
	}
	defer func() { _ = tconn.Close(ctx) }() //nolint:errcheck // best-effort close of template conn
	for _, m := range migrations {
		if _, err := tconn.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
	}
	return nil
}

// migrationsDir resolves the Atlas migrations directory relative to THIS source
// file (repo-root/migrations), not the caller's cwd — DSN runs from multiple
// test packages, each with a different working directory. pgtest lives at
// <repoRoot>/internal/pgtest, so migrations is two levels up.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only need the file path
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
}

// loadMigrations reads the *.sql migration files in filename order. os.ReadDir
// returns entries sorted by name, matching Atlas's lexical ordering; the
// atlas.sum checksum file is skipped — only SQL is applied.
func loadMigrations() ([]migration, error) {
	dir := migrationsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	var migs []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		// Path is repo-local: dir comes from runtime.Caller, name from a trusted
		// migrations/ listing — never user input (no G304 risk).
		b, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // trusted repo-local path
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		migs = append(migs, migration{name: e.Name(), sql: string(b)})
	}
	return migs, nil
}

// EncKey returns a fixed 32-byte auth-key encryption master key for tests. It is
// deterministic so a store reopened against the same database (restart tests)
// decrypts keys written by the prior instance.
func EncKey() []byte {
	return bytes.Repeat([]byte{0x2a}, 32)
}

// PeerDeriver returns the peer access-hash deriver an api.New under test takes,
// built from EncKey the same way the server builds it at process start. It is
// deterministic for the same reason EncKey is: a restart test must derive the
// same hashes the prior instance issued.
func PeerDeriver() *peerhash.Deriver {
	sub, err := peerhash.Subkey(EncKey())
	if err != nil {
		panic(err)
	}
	d, err := peerhash.New(sub)
	if err != nil {
		panic(err)
	}
	return d
}

// Prewarm triggers the one-time container setup (image pull and boot) outside of
// any test's deadline. Call it from TestMain so a cold container start is not
// charged against a test's context timeout, which otherwise flakes the first run
// on a fresh machine. Returns the setup error, if any.
func Prewarm() error {
	once.Do(setup)
	return errSetup
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
