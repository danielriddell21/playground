package mix

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"playground/internal/play"
)

type Session struct {
	ctx     context.Context
	dsn     string
	pool    *pgxpool.Pool
	kafka   *kgo.Client
	queries int
	err     error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Close() {
	if s.kafka != nil {
		s.kafka.Close()
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

func (s *Session) Table(headers []string, rows [][]any) {
	if s.err != nil {
		return
	}
	play.Table(headers, rows)
}

func (s *Session) PG() *pgxpool.Pool {
	if s.err != nil {
		return nil
	}
	if s.pool == nil {
		pool, err := pgxpool.New(s.ctx, s.dsn)
		if err != nil {
			s.err = err
			return nil
		}
		if err := pool.Ping(s.ctx); err != nil {
			pool.Close()
			s.err = err
			return nil
		}
		s.pool = pool
	}
	return s.pool
}

func (s *Session) Kafka(opts ...kgo.Opt) *kgo.Client {
	if s.err != nil {
		return nil
	}
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		s.err = err
		return nil
	}
	if s.kafka == nil {
		s.kafka = cl
	}
	return cl
}

func (s *Session) Exec(sql string, args ...any) {
	if s.err != nil {
		return
	}
	pool := s.PG()
	if pool == nil {
		return
	}
	s.queries++
	if _, err := pool.Exec(s.ctx, sql, args...); err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
	}
}

func (s *Session) Show(sql string, args ...any) {
	if s.err != nil {
		return
	}
	pool := s.PG()
	if pool == nil {
		return
	}
	s.queries++
	rows, err := pool.Query(s.ctx, sql, args...)
	if err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
		return
	}
	defer rows.Close()

	var headers []string
	for _, f := range rows.FieldDescriptions() {
		headers = append(headers, f.Name)
	}
	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			s.err = err
			return
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		s.err = err
		return
	}
	play.Table(headers, out)
}

func (s *Session) ResetQueries() { s.queries = 0 }

func (s *Session) Queries() int { return s.queries }

func (s *Session) CountQuery() { s.queries++ }

func (s *Session) AtLeast(major int) bool {
	if s.err != nil {
		return false
	}
	pool := s.PG()
	if pool == nil {
		return false
	}
	var num int
	if err := pool.QueryRow(s.ctx, `select current_setting('server_version_num')::int`).Scan(&num); err != nil {
		s.err = err
		return false
	}
	if num/10000 < major {
		fmt.Printf("skipped: needs postgres %d, server is %d.%d\n", major, num/10000, num%10000)
		return false
	}
	return true
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var set = &play.Set[*Session]{
	Use:        "mix",
	Short:      "two or more tools working together",
	DSNEnv:     "DATABASE_URL",
	DefaultDSN: "postgres://postgres:postgres@localhost:5432/playground",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	return &Session{ctx: ctx, dsn: dsn}, nil
}

func Command() *cobra.Command { return set.Command() }
