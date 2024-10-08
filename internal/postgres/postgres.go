package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"playground/internal/play"
)

type Session struct {
	ctx  context.Context
	pool *pgxpool.Pool
	conn *pgxpool.Conn
	err  error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Pool() *pgxpool.Pool { return s.pool }

func (s *Session) Fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Session) Close() {
	s.conn.Release()
	s.pool.Close()
}

func (s *Session) Exec(sql string, args ...any) {
	if s.err != nil {
		return
	}
	if _, err := s.conn.Exec(s.ctx, sql, args...); err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
	}
}

func (s *Session) Show(sql string, args ...any) {
	if s.err != nil {
		return
	}
	rows, err := s.conn.Query(s.ctx, sql, args...)
	if err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
		return
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	var headers []string
	for _, f := range fields {
		headers = append(headers, f.Name)
	}

	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			s.err = err
			return
		}
		cells := make([]any, len(vals))
		for i, v := range vals {
			cells[i] = s.render(fields[i].DataTypeOID, v)
		}
		out = append(out, cells)
	}
	if err := rows.Err(); err != nil {
		s.err = fmt.Errorf("%w\n%s", err, sql)
		return
	}
	play.Table(headers, out)
}

func (s *Session) render(oid uint32, v any) any {
	if v == nil {
		return nil
	}
	if buf, err := s.conn.Conn().TypeMap().Encode(oid, pgtype.TextFormatCode, v, nil); err == nil && buf != nil {
		return string(buf)
	}
	switch t := v.(type) {
	case pgtype.Range[any]:
		return rangeText(t)
	case pgtype.Multirange[pgtype.Range[any]]:
		parts := make([]string, len(t))
		for i, r := range t {
			parts[i] = rangeText(r)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return v
}

func rangeText(r pgtype.Range[any]) string {
	if !r.Valid {
		return "NULL"
	}
	lower, upper := "[", "]"
	if r.LowerType == pgtype.Exclusive {
		lower = "("
	}
	if r.UpperType == pgtype.Exclusive {
		upper = ")"
	}
	return lower + play.Cell(r.Lower) + "," + play.Cell(r.Upper) + upper
}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

var set = &play.Set[*Session]{
	Use:        "postgres",
	Short:      "postgres feature playgrounds",
	DSNEnv:     "DATABASE_URL",
	DefaultDSN: "postgres://postgres:postgres@localhost:5432/playground",
	Connect:    connect,
}

func connect(ctx context.Context, dsn string) (*Session, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Session{ctx: ctx, pool: pool, conn: conn}, nil
}

func Command() *cobra.Command { return set.Command() }
