// Command migrate applies and rolls back RAVEN SQL migrations. It uses only
// the standard library plus pgx — no external migration framework, so the
// mechanism stays inspectable end to end.
//
//	migrate up     # apply all pending migrations, in version order
//	migrate down   # roll back the most recently applied migration
//
// Configuration: DATABASE_URL (required) and MIGRATIONS_DIR (default
// ./migrations). Migration files are numbered globally across the whole
// platform: NNNNNN_name.up.sql / NNNNNN_name.down.sql.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v5"

	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// migrationFile is one parsed file name from the migrations directory.
type migrationFile struct {
	version int
	name    string
	path    string
}

var (
	upFileRe   = regexp.MustCompile(`^(\d+)_[^/]+\.up\.sql$`)
	downFileRe = regexp.MustCompile(`^(\d+)_[^/]+\.down\.sql$`)
)

func main() {
	log := logger.New("migrate", config.Get("LOG_LEVEL", "info"))

	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		fmt.Fprintln(os.Stderr, "usage: migrate up|down")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := config.Get("DATABASE_URL", "postgres://raven:raven@localhost:5432/raven?sslmode=disable")
	dir := config.Get("MIGRATIONS_DIR", "./migrations")

	var err error
	switch os.Args[1] {
	case "up":
		err = migrateUp(ctx, dsn, dir, log)
	case "down":
		err = migrateDown(ctx, dsn, dir, log)
	}
	if err != nil {
		log.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

// connect opens a single connection. Simple protocol mode is required: a
// migration file contains multiple statements, which the extended protocol
// (prepared statements) cannot execute in one call.
func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: parse DATABASE_URL: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("migrate: connect: %w", err)
	}
	return conn, nil
}

func ensureVersionTable(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    int PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}
	return nil
}

// appliedVersions returns the set of already-applied migration versions.
func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("migrate: read schema_migrations: %w", err)
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// listMigrations parses the migrations directory and returns the files
// matching re, sorted by version.
func listMigrations(dir string, re *regexp.Regexp) ([]migrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("migrate: read %s: %w", dir, err)
	}

	var files []migrationFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := re.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migrate: bad version in %s: %w", e.Name(), err)
		}
		files = append(files, migrationFile{
			version: version,
			name:    e.Name(),
			path:    filepath.Join(dir, e.Name()),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}

func migrateUp(ctx context.Context, dsn, dir string, log *slog.Logger) error {
	conn, err := connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := ensureVersionTable(ctx, conn); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}
	files, err := listMigrations(dir, upFileRe)
	if err != nil {
		return err
	}

	pending := 0
	for _, f := range files {
		if applied[f.version] {
			continue
		}
		pending++
		log.Info("applying migration", "version", f.version, "file", f.name)

		sql, err := os.ReadFile(f.path)
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", f.name, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("migrate %06d: begin: %w", f.version, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate %06d: apply: %w", f.version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, f.version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrate %06d: record version: %w", f.version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrate %06d: commit: %w", f.version, err)
		}
	}

	if pending == 0 {
		log.Info("schema is up to date")
	} else {
		log.Info("migrations applied", "count", pending)
	}
	return nil
}

func migrateDown(ctx context.Context, dsn, dir string, log *slog.Logger) error {
	conn, err := connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := ensureVersionTable(ctx, conn); err != nil {
		return err
	}

	var last int
	err = conn.QueryRow(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&last)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Info("nothing to roll back")
			return nil
		}
		return fmt.Errorf("migrate: read schema_migrations: %w", err)
	}

	downs, err := listMigrations(dir, downFileRe)
	if err != nil {
		return err
	}
	var target *migrationFile
	for i := range downs {
		if downs[i].version == last {
			target = &downs[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("migrate: no down file for version %06d in %s", last, dir)
	}

	log.Info("rolling back migration", "version", target.version, "file", target.name)
	sql, err := os.ReadFile(target.path)
	if err != nil {
		return fmt.Errorf("migrate: read %s: %w", target.name, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate %06d: begin: %w", target.version, err)
	}
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("migrate %06d: rollback: %w", target.version, err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM schema_migrations WHERE version = $1`, target.version); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("migrate %06d: record rollback: %w", target.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate %06d: commit: %w", target.version, err)
	}

	log.Info("migration rolled back", "version", target.version)
	return nil
}
