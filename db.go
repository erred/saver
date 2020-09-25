package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
)

func ExecuteTx(ctx context.Context, pool *pgxpool.Pool, txOpts pgx.TxOptions, fn func(pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	return crdb.ExecuteInTx(ctx, pgxTxAdapter{tx}, func() error { return fn(tx) })
}

type pgxTxAdapter struct {
	pgx.Tx
}

func (tx pgxTxAdapter) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := tx.Tx.Exec(ctx, q, args...)
	return err
}

func (s *Server) dbSetup(ctx context.Context) error {
	// connection pool
	config, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		return fmt.Errorf("dbSetup parse dsn=%s: %w", s.dsn, err)
	}
	config.ConnConfig.Logger = zerologadapter.NewLogger(s.log)
	s.pool, err = pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("dbSetup connect dsn=%s: %w", s.dsn, err)
	}

	// ensure tables
	err = ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS http (
	timestamp	TIMESTAMP,
	remote		TEXT,
	user_agent	TEXT,
	referrer	TEXT,
	method		TEXT,
	domain		TEXT,
	path		TEXT,
)`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS beacon (
	timestamp	TIMESTAMP,
	remote		TEXT,
	user_agent	TEXT,
	referrer	TEXT
	duration_ms	INTEGER,
	src_page	TEXT,
	dst_page	TEXT,
)`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS csp (
	timestamp		TIMESTAMP,
	remote			TEXT,
	user_agent		TEXT,
	referrer		TEXT
	disposition		TEXT,
	blocked_uri		TEXT,
	source_file		TEXT,
	document_uri		TEXT,
	violated_directive	TEXT,
	effective_directive	TEXT,
	line_number		INTEGER,
	status_code		INTEGER,
)`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS repodefault (
	timestamp	TIMESTAMP,
	owner		TEXT,
	repo		TEXT,
)`)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("dbSetup ensure tables: %w", err)
	}

	return nil
}
