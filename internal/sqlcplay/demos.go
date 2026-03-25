package sqlcplay

import (
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"playground/internal/sqlcplay/db"
)

func init() {
	set.Add("generate", "the sql you write, and the go it turns into", generate)
	set.Add("typed", "call the generated code against a real database", typed)
	set.Add("mistakes", "sqlc catches at build time what pgx finds at runtime", mistakes)
}

func generate(s *Session) {
	s.Note("you write a schema")
	for _, line := range s.File("internal/sqlcplay/sql/schema.sql") {
		s.Note("  %s", line)
	}

	s.Note("and named queries, in plain sql")
	for _, line := range s.File("internal/sqlcplay/sql/query.sql")[:12] {
		s.Note("  %s", line)
	}
	s.Note("  ...")

	s.Note("sqlc generate reads both and writes go")
	if out, ok := s.CLI("generate"); ok {
		if strings.TrimSpace(out) == "" {
			s.Note("  (no output, which is sqlc for success)")
		} else {
			s.Note("  %s", strings.TrimSpace(out))
		}
	} else {
		return
	}

	s.Note("the signatures it produced")
	var rows [][]any
	for _, line := range s.File("internal/sqlcplay/db/query.sql.go") {
		if strings.HasPrefix(line, "func (q *Queries)") {
			sig := strings.TrimPrefix(line, "func (q *Queries) ")
			sig = strings.TrimSuffix(sig, " {")
			name, rest, _ := strings.Cut(sig, "(")
			rows = append(rows, []any{name, "(" + rest})
		}
	}
	s.Table([]string{"method", "signature"}, rows)

	s.Note("nothing is reflected or guessed, the types come from the schema")
	s.Note("change a column and the next build fails, not the next deploy")
}

func typed(s *Session) {
	s.Exec(`drop table if exists books`)
	s.Exec(`drop table if exists authors`)
	for _, stmt := range strings.Split(strings.Join(s.File("internal/sqlcplay/sql/schema.sql"), "\n"), ";") {
		if strings.TrimSpace(stmt) != "" {
			s.Exec(stmt)
		}
	}

	q := s.Queries()
	if q == nil {
		return
	}

	s.Note("insert, and get a typed struct back rather than a row to scan")
	ada, err := q.CreateAuthor(s.Ctx(), db.CreateAuthorParams{Name: "ada", City: pgText("london")})
	if err != nil {
		s.Fail(err)
		return
	}
	grace, err := q.CreateAuthor(s.Ctx(), db.CreateAuthorParams{Name: "grace", City: pgText("new york")})
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"id", "name", "city"}, [][]any{
		{ada.ID, ada.Name, ada.City.String},
		{grace.ID, grace.Name, grace.City.String},
	})

	for _, b := range []db.AddBookParams{
		{AuthorID: ada.ID, Title: "notes on the engine", Year: 1843, Tags: []string{"maths"}},
		{AuthorID: ada.ID, Title: "on looms", Year: 1845, Tags: []string{"craft"}},
		{AuthorID: grace.ID, Title: "compiler design", Year: 1952, Tags: []string{"code"}},
	} {
		if _, err := q.AddBook(s.Ctx(), b); err != nil {
			s.Fail(err)
			return
		}
	}

	s.Note("a :many query returns a slice of a struct named after the query")
	books, err := q.BooksByAuthor(s.Ctx(), "ada")
	if err != nil {
		s.Fail(err)
		return
	}
	var rows [][]any
	for _, b := range books {
		rows = append(rows, []any{b.ID, b.Title, b.Year, b.Author})
	}
	s.Table([]string{"id", "title", "year", "author"}, rows)

	s.Note("joins and aggregates get their own row type too")
	counts, err := q.CountBooksPerCity(s.Ctx())
	if err != nil {
		s.Fail(err)
		return
	}
	rows = nil
	for _, c := range counts {
		rows = append(rows, []any{c.City.String, c.Books})
	}
	s.Table([]string{"city", "books"}, rows)

	s.Note("BooksByAuthorRow and CountBooksPerCityRow were both written by sqlc")
	s.Note("neither exists in the schema, they describe exactly what each query selects")
}

func mistakes(s *Session) {
	s.Note("the point of sqlc is where errors show up")
	s.Table([]string{"mistake", "with pgx", "with sqlc"}, [][]any{
		{"typo in a column name", "runtime error on that query", "sqlc generate fails"},
		{"wrong number of args", "runtime error", "does not compile"},
		{"scanning int into string", "runtime scan error", "does not compile"},
		{"column dropped from schema", "runtime error in production", "next build fails"},
		{"query returns extra column", "silently ignored", "row type changes, build fails"},
	})

	s.Note("try it: break a column name in internal/sqlcplay/sql/query.sql and run")
	s.Note("  sqlc generate")
	s.Note("it refuses, and points at the line")

	s.Note("the cost is that dynamic sql is awkward")
	s.Note("optional filters and sorts do not fit named queries, so those stay hand written")
	s.Note("most codebases end up with sqlc for the fixed queries and pgx for the rest")
}

func pgText(v string) pgtype.Text { return pgtype.Text{String: v, Valid: true} }
