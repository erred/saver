package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	saver "go.seankhliao.com/apis/saver/v1"
	"go.seankhliao.com/usvc"
)

const (
	name = "go.seankhliao.com/saver"
)

func main() {
	os.Exit(usvc.Exec(context.Background(), &Server{}, os.Args))
}

type Server struct {
	dsn  string
	pool *pgxpool.Pool

	log    zerolog.Logger
	tracer trace.Tracer
}

func (s *Server) Flags(fs *flag.FlagSet) {
	fs.StringVar(&s.dsn, "db", "", "connection string for pgx")
}

func (s *Server) Setup(ctx context.Context, u *usvc.USVC) error {
	s.log = u.Logger
	s.tracer = global.Tracer(name)

	saver.RegisterSaverService(u.GRPCServer, &saver.SaverService{
		HTTP:        s.http,
		Beacon:      s.beacon,
		CSP:         s.csp,
		RepoDefault: s.repoDefault,
	})

	err := s.dbSetup(ctx)
	if err != nil {
		return fmt.Errorf("dbSetup: %w", err)
	}
	go func() {
		<-ctx.Done()
		s.pool.Close()
	}()
	return nil
}

func (s *Server) http(ctx context.Context, r *saver.HTTPRequest) (*saver.HTTPResponse, error) {
	err := ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO http VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			r.HttpRemote.Timestamp,
			r.HttpRemote.Remote,
			r.HttpRemote.UserAgent,
			r.HttpRemote.Referrer,
			r.Method,
			r.Domain,
			r.Path,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &saver.HTTPResponse{}, nil
}

func (s *Server) beacon(ctx context.Context, r *saver.BeaconRequest) (*saver.BeaconResponse, error) {
	err := ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO http VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			r.HttpRemote.Timestamp,
			r.HttpRemote.Remote,
			r.HttpRemote.UserAgent,
			r.HttpRemote.Referrer,
			r.DurationMs,
			r.SrcPage,
			r.DstPage,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &saver.BeaconResponse{}, nil
}

func (s *Server) csp(ctx context.Context, r *saver.CSPRequest) (*saver.CSPResponse, error) {
	err := ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO http VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			r.HttpRemote.Timestamp,
			r.HttpRemote.Remote,
			r.HttpRemote.UserAgent,
			r.HttpRemote.Referrer,
			r.Disposition,
			r.BlockedUri,
			r.SourceFile,
			r.DocumentUri,
			r.ViolatedDirective,
			r.EffectiveDirective,
			r.LineNumber,
			r.StatusCode,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &saver.CSPResponse{}, nil
}

func (s *Server) repoDefault(ctx context.Context, r *saver.RepoDefaultRequest) (*saver.RepoDefaultResponse, error) {
	err := ExecuteTx(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO http VALUES ($1, $2, $3)`,
			r.Timestamp,
			r.Owner,
			r.Repo,
		)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &saver.RepoDefaultResponse{}, nil
}
