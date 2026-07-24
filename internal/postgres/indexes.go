package postgres

func init() {
	set.Add("indexes", "the index zoo: gin, brin, partial, expression and covering", indexes)
}

func indexes(s *Session) {
	s.Exec(`drop table if exists people`)
	s.Exec(`create table people (id serial primary key, name text, age int, city text, tags text[])`)
	s.Exec(`insert into people (name, age, city, tags)
		select 'person-' || g, (g % 80) + 18, (array['london','paris','berlin','tokyo'])[1 + g % 4],
			array[(array['a','b','c'])[1 + g % 3]]
		from generate_series(1, 200000) g`)
	s.Exec(`analyze people`)

	s.Note("no index yet, so it reads everything")
	s.Show(`explain (analyze, costs off, timing off, summary off) select * from people where age = 30`)

	s.Note("partial index, only the rows you care about")
	s.Exec(`create index people_seniors on people (age) where age > 90`)
	s.Show(`explain (costs off) select * from people where age > 95`)

	s.Note("expression index, so the function call is indexed")
	s.Exec(`create index people_lower_name on people (lower(name))`)
	s.Show(`explain (costs off) select * from people where lower(name) = 'person-42'`)

	s.Note("covering index answers from the index alone")
	s.Exec(`create index people_city_covering on people (city) include (age)`)
	s.Exec(`analyze people`)
	s.Show(`explain (analyze, costs off, timing off, summary off) select city, age from people where city = 'paris'`)

	s.Note("gin indexes the inside of arrays")
	s.Exec(`create index people_tags on people using gin (tags)`)
	s.Show(`explain (costs off) select * from people where tags @> '{b}'`)

	s.Note("brin is tiny because it only stores block ranges")
	s.Exec(`create index people_id_brin on people using brin (id)`)
	s.Exec(`create index people_id_btree on people (id)`)
	s.Show(`select indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as size
		from pg_indexes where tablename = 'people' and indexname like 'people_id%' order by indexname`)

	s.Note("same table, every index side by side")
	s.Show(`select indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as size
		from pg_indexes where tablename = 'people' order by pg_relation_size(indexname::regclass) desc`)
}
