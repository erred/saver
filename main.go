package main

import (
	"context"
	"flag"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/api/trace"
	saver "go.seankhliao.com/apis/saver/v1"
	"go.seankhliao.com/usvc"
)

const (
	name = "go.seankhliao.com/saver"
)

func main() {
	var s Server

	usvc.Run(context.Background(), name, &s, true)
}

type Server struct {
	dsn  string
	pool *pgxpool.Pool

	log    zerolog.Logger
	tracer trace.Tracer
}

func (s *Server) Flag(fs *flag.FlagSet) {
	fs.StringVar(&s.dsn, "db", "", "connection string for pgx")
}

func (s *Server) Register(c *usvc.Components) error {
	s.log = c.Log
	s.tracer = c.Tracer

	saver.RegisterSaverService(c.GRPC, &saver.SaverService{
		HTTP:        nil,
		Beacon:      nil,
		CSP:         nil,
		RepoDefault: nil,
	})

	return s.dbSetup(context.Background())
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.pool.Close()
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
