package bigdata

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"playground/internal/bigdata/pb"
	"playground/internal/play"
)

var (
	rows      = 1_000_000
	batchSize = 5000
	runs      = 1
	workers   = runtime.GOMAXPROCS(0)
)

type Session struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client pb.BigdataClient
	err    error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.srv != nil {
		s.srv.Stop()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Session) Fail(err error) {
	if s.err == nil && err != nil {
		s.err = err
	}
}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

func (s *Session) Exec(sql string, args ...any) {
	if s.err != nil {
		return
	}
	if _, err := s.pool.Exec(s.ctx, sql, args...); err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
	}
}

func (s *Session) Show(sql string, args ...any) {
	if s.err != nil {
		return
	}
	r, err := s.pool.Query(s.ctx, sql, args...)
	if err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
		return
	}
	defer r.Close()
	var headers []string
	for _, f := range r.FieldDescriptions() {
		headers = append(headers, f.Name)
	}
	var out [][]any
	for r.Next() {
		vals, err := r.Values()
		if err != nil {
			s.err = err
			return
		}
		out = append(out, vals)
	}
	if err := r.Err(); err != nil {
		s.err = err
		return
	}
	play.Table(headers, out)
}

var set = &play.Set[*Session]{
	Use:        "bigdata",
	Short:      "millions of rows through grpc into and out of postgres, three good ways each",
	DSNEnv:     "DATABASE_URL",
	DefaultDSN: "postgres://postgres:postgres@localhost:5432/playground",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(max(workers*2, 8))
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	// the default 64KB flow control window stalls a fast stream long before
	// the cpu does, so both ends get room for a few batches in flight
	lis := bufconn.Listen(4 << 20)
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.InitialWindowSize(1<<20),
		grpc.InitialConnWindowSize(16<<20),
	)
	pb.RegisterBigdataServer(srv, &server{pool: pool})
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)),
		grpc.WithInitialWindowSize(1<<20),
		grpc.WithInitialConnWindowSize(16<<20),
	)
	if err != nil {
		srv.Stop()
		pool.Close()
		return nil, err
	}

	return &Session{ctx: ctx, pool: pool, srv: srv, conn: conn, client: pb.NewBigdataClient(conn)}, nil
}

func Command() *cobra.Command {
	cmd := set.Command()
	cmd.PersistentFlags().Func("rows", "rows per scenario, e.g. 1m, 2m, 5m, 200k (default 1m)", func(v string) error {
		n, err := parseCount(v)
		rows = n
		return err
	})
	cmd.PersistentFlags().IntVar(&batchSize, "batch", batchSize, "rows per grpc message")
	cmd.PersistentFlags().IntVar(&runs, "runs", runs, "runs per approach, best is reported")
	return cmd
}

func parseCount(v string) (int, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	mult := 1
	switch {
	case strings.HasSuffix(v, "m"):
		mult, v = 1_000_000, strings.TrimSuffix(v, "m")
	case strings.HasSuffix(v, "k"):
		mult, v = 1_000, strings.TrimSuffix(v, "k")
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("bad row count %q", v)
	}
	return int(f * float64(mult)), nil
}
