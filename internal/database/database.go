// Package database centralises Postgres access for every RAVEN service:
// connection pool construction with sane defaults, a readiness checker for
// the health registry, and a transaction helper that guarantees
// commit/rollback hygiene.
package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Pool defaults. Small on purpose: this is a learning platform, and a small
// pool surfaces contention bugs early instead of hiding them.
const (
	defaultMaxConns        = 10
	defaultHealthCheck     = 30 * time.Second
	defaultMaxConnLifetime = time.Hour
	defaultMaxConnIdleTime = 5 * time.Minute
	pingTimeout            = 5 * time.Second
)

// NewPool builds a pgxpool.Pool from a DATABASE_URL and verifies it with a
// ping before returning. The caller owns the pool and must Close it.
func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.E(errors.KindInvalid, "database_url_invalid",
			"DATABASE_URL could not be parsed", err)
	}
	cfg.MaxConns = defaultMaxConns
	cfg.HealthCheckPeriod = defaultHealthCheck
	cfg.MaxConnLifetime = defaultMaxConnLifetime
	cfg.MaxConnIdleTime = defaultMaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.E(errors.KindUnavailable, "database_pool_failed",
			"could not create the postgres connection pool", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, errors.E(errors.KindUnavailable, "database_unreachable",
			"postgres did not answer a ping", err)
	}
	return pool, nil
}

// Checker returns a health.Checker that pings Postgres. Register it in the
// service health registry so /ready reflects the database.
func Checker(pool *pgxpool.Pool) health.Checker {
	return func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			return fmt.Errorf("postgres ping: %w", err)
		}
		return nil
	}
}

// WithTx runs fn inside a transaction on pool. It commits when fn returns
// nil and rolls back otherwise (and on panic, via the deferred rollback).
// The deferred Rollback after a successful Commit is a documented no-op in
// pgx, so this stays panic-safe without extra bookkeeping.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.E(errors.KindUnavailable, "tx_begin_failed",
			"could not begin a database transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.E(errors.KindUnavailable, "tx_commit_failed",
			"could not commit the database transaction", err)
	}
	return nil
}
