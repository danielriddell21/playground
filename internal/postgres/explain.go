package postgres

func init() {
	set.Add("explain", "explain analyze, index types and planner stats", explainPlans)
}

func explainPlans(s *Session) {
	s.Exec(`drop table if exists people`)
	s.Exec(`create table people (id serial primary key, name text, age int, city text, profile jsonb)`)
	s.Exec(`insert into people (name, age, city, profile)
		select 'person-' || g, (g % 80) + 18, (array['london','paris','berlin','tokyo'])[1 + g % 4],
			jsonb_build_object('level', g % 5)
		from generate_series(1, 50000) g`)
	s.Exec(`analyze people`)

	s.Note("sequential scan before an index exists")
	s.Show(`explain (analyze, buffers, costs off, timing off, summary off) select * from people where age = 30`)

	s.Note("btree index")
	s.Exec(`create index people_age_idx on people (age)`)
	s.Exec(`analyze people`)
	s.Show(`explain (analyze, costs off, timing off, summary off) select * from people where age = 30`)

	s.Note("index only scan from a covering index")
	s.Exec(`create index people_city_age_idx on people (city) include (age)`)
	s.Exec(`analyze people`)
	s.Show(`explain (analyze, costs off, timing off, summary off) select city, age from people where city = 'paris'`)

	s.Note("partial index")
	s.Exec(`create index people_seniors_idx on people (age) where age > 90`)
	s.Show(`explain (costs off) select * from people where age > 95`)

	s.Note("expression index")
	s.Exec(`create index people_lower_name_idx on people (lower(name))`)
	s.Show(`explain (costs off) select * from people where lower(name) = 'person-42'`)

	s.Note("brin for naturally ordered data")
	s.Exec(`create index people_id_brin on people using brin (id)`)
	s.Show(`select indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as size
		from pg_indexes where tablename = 'people' order by indexname`)

	s.Note("planner statistics")
	s.Show(`select attname, n_distinct, most_common_vals from pg_stats where tablename = 'people' and attname in ('city', 'age') order by attname`)
	s.Show(`select relname, seq_scan, idx_scan, n_live_tup from pg_stat_user_tables where relname = 'people'`)

	s.Note("table and index sizes")
	s.Show(`select pg_size_pretty(pg_total_relation_size('people')) as total,
			pg_size_pretty(pg_relation_size('people')) as heap,
			pg_size_pretty(pg_indexes_size('people')) as indexes`)
}
