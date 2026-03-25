package sqlcplay

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"playground/internal/play"
	"playground/internal/sqlcplay/db"
)

type Session struct {
	ctx  context.Context
	dsn  string
	pool *pgxpool.Pool
	err  error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Close() {
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
		s.pool = pool
	}
	return s.pool
}

func (s *Session) Queries() *db.Queries {
	pool := s.PG()
	if pool == nil {
		return nil
	}
	return db.New(pool)
}

func (s *Session) Exec(sql string) {
	if s.err != nil {
		return
	}
	if pool := s.PG(); pool != nil {
		if _, err := pool.Exec(s.ctx, sql); err != nil {
			s.err = fmt.Errorf("%w\n%s", err, sql)
		}
	}
}

func (s *Session) File(path string) []string {
	body, err := os.ReadFile(filepath.Join(repoRoot(), path))
	if err != nil {
		s.Fail(err)
		return nil
	}
	return strings.Split(strings.TrimRight(string(body), "\n"), "\n")
}

func (s *Session) CLI(args ...string) (string, bool) {
	if s.err != nil {
		return "", false
	}
	bin, err := exec.LookPath("sqlc")
	if err != nil {
		s.Note("sqlc is not on PATH, install it with:")
		s.Note("  go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest")
		return "", false
	}
	cmd := exec.CommandContext(s.ctx, bin, args...)
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput()
	return string(out), err == nil
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

var set = &play.Set[*Session]{
	Use:        "sqlc",
	Short:      "write sql, get typed go",
	DSNEnv:     "DATABASE_URL",
	DefaultDSN: "postgres://postgres:postgres@localhost:5432/playground",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	return &Session{ctx: ctx, dsn: dsn}, nil
}

func Command() *cobra.Command { return set.Command() }
