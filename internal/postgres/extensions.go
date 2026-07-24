package postgres

func init() {
	set.Add("extensions", "pg_trgm fuzzy search, ltree trees, hstore, citext", extensions)
}

func extensions(s *Session) {
	for _, ext := range []string{"pg_trgm", "hstore", "ltree", "citext", "unaccent", "pgcrypto"} {
		s.Exec(`create extension if not exists "` + ext + `"`)
	}

	s.Note("pg_trgm fuzzy matching")
	s.Exec(`drop table if exists names`)
	s.Exec(`create table names (name text)`)
	s.Exec(`insert into names values ('postgresql'), ('postgres'), ('mysql'), ('sqlite')`)
	s.Exec(`create index names_trgm_idx on names using gin (name gin_trgm_ops)`)
	s.Show(`select name, round(similarity(name, 'postgres')::numeric, 3) as sim
		from names where name % 'postgres' order by sim desc`)
	s.Show(`select name from names order by name <-> 'postgrez' limit 2`)

	s.Note("hstore key value column")
	s.Show(`select 'a=>1, b=>2'::hstore as h,
			('a=>1, b=>2'::hstore)->'b' as b,
			akeys('a=>1, b=>2'::hstore) as keys,
			'a=>1'::hstore || 'c=>3'::hstore as merged`)

	s.Note("ltree hierarchical paths")
	s.Show(`select 'top.science.astronomy'::ltree as path,
			'top.science.astronomy'::ltree ~ 'top.*'::lquery as matches,
			subpath('top.science.astronomy'::ltree, 0, 2) as trimmed,
			nlevel('top.science.astronomy'::ltree) as depth`)

	s.Note("citext case insensitive text")
	s.Show(`select 'HELLO'::citext = 'hello'::citext as citext_equal, 'HELLO' = 'hello' as text_equal`)

	s.Note("unaccent")
	s.Show(`select unaccent('café crème brûlée') as folded`)

	s.Note("pgcrypto")
	s.Show(`select encode(digest('secret', 'sha256'), 'hex') as sha256,
			crypt('password', gen_salt('bf', 6)) is not null as hashed,
			encode(gen_random_bytes(8), 'hex') as random_bytes`)

}
