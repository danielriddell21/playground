package melange

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
)

const schemaPath = "deploy/melange/schema.fga"

type Session struct {
	ctx  context.Context
	dsn  string
	pool *pgxpool.Pool
	err  error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) DSN() string { return s.dsn }

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

func (s *Session) Exec(sql string, args ...any) {
	if s.err != nil {
		return
	}
	if pool := s.PG(); pool != nil {
		if _, err := pool.Exec(s.ctx, sql, args...); err != nil {
			s.err = fmt.Errorf("%w\n%s", err, sql)
		}
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

func (s *Session) CLI(args ...string) (string, bool) {
	if s.err != nil {
		return "", false
	}
	bin, err := exec.LookPath("melange")
	if err != nil {
		s.Note("melange is not on PATH, install it with:")
		s.Note("  go install github.com/pthm/melange/cmd/melange@latest")
		return "", false
	}
	cmd := exec.CommandContext(s.ctx, bin, args...)
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		s.Note("melange %s failed:", strings.Join(args, " "))
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			s.Note("  %s", line)
		}
		return string(out), false
	}
	return string(out), true
}

func (s *Session) Echo(out string) {
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		s.Note("  %s", line)
	}
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
	Use:        "melange",
	Short:      "compile an openfga schema into postgres functions",
	DSNEnv:     "DATABASE_URL",
	DefaultDSN: "postgres://postgres:postgres@localhost:5432/playground",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	return &Session{ctx: ctx, dsn: dsn}, nil
}

func Command() *cobra.Command { return set.Command() }
