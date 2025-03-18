package postgres

func init() {
	set.Add("fulltext", "tsvector, tsquery, ranking and highlighting", fulltext)
}

func fulltext(s *Session) {
	s.Exec(`drop table if exists articles`)
	s.Exec(`create table articles (
		id serial primary key,
		title text,
		body text,
		search tsvector generated always as (to_tsvector('english', title || ' ' || body)) stored
	)`)
	s.Exec(`insert into articles (title, body) values
		('Postgres indexes', 'A btree index speeds up equality and range lookups on a table.'),
		('Running vacuum', 'Autovacuum reclaims dead tuples and keeps table bloat under control.'),
		('Writing queries', 'Query planning picks an index or a sequential scan depending on statistics.')`)

	s.Exec(`create index articles_search_idx on articles using gin (search)`)

	s.Note("basic match with stemming")
	s.Show(`select title from articles where search @@ to_tsquery('english', 'index')`)
	s.Show(`select title from articles where search @@ to_tsquery('english', 'reclaim')`)

	s.Note("boolean and phrase queries")
	s.Show(`select title from articles where search @@ to_tsquery('english', 'index & scan')`)
	s.Show(`select title from articles where search @@ phraseto_tsquery('english', 'dead tuples')`)
	s.Show(`select title from articles where search @@ websearch_to_tsquery('english', '"table" -vacuum')`)

	s.Note("ranking")
	s.Show(`select title, round(ts_rank(search, q)::numeric, 4) as rank
		from articles, to_tsquery('english', 'table | index') q
		where search @@ q order by rank desc`)

	s.Note("highlighting and inspection")
	s.Show(`select ts_headline('english', body, to_tsquery('english', 'index')) as snippet
		from articles where title = 'Postgres indexes'`)
	s.Show(`select to_tsvector('english', 'the cats were running quickly') as stemmed`)
}
