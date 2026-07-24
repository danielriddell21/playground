package postgres

func init() {
	set.Add("syntax", "postgres-only SQL: distinct on, on conflict, writable CTEs", syntax)
}

func syntax(s *Session) {
	s.Exec(`drop table if exists readings`)
	s.Exec(`create table readings (sensor text, taken_at timestamptz, value numeric)`)
	s.Exec(`insert into readings values
		('a', '2026-01-01 10:00+00', 5), ('a', '2026-01-01 11:00+00', 9), ('a', '2026-01-01 12:00+00', 7),
		('b', '2026-01-01 10:00+00', 2), ('b', '2026-01-01 12:00+00', 4)`)

	s.Note("distinct on gives the latest row per group with no window function")
	s.Show(`select distinct on (sensor) sensor, taken_at, value
		from readings order by sensor, taken_at desc`)

	s.Note("on conflict, upsert without a merge statement")
	s.Exec(`drop table if exists totals`)
	s.Exec(`create table totals (sensor text primary key, hits int)`)
	s.Show(`insert into totals (sensor, hits) values ('a', 1), ('b', 1)
		on conflict (sensor) do update set hits = totals.hits + 1 returning *`)
	s.Show(`insert into totals (sensor, hits) values ('a', 1), ('c', 1)
		on conflict (sensor) do update set hits = totals.hits + 1 returning *`)

	s.Note("a CTE that writes, then feeds the next statement")
	s.Exec(`drop table if exists archive`)
	s.Exec(`create table archive (sensor text, value numeric)`)
	s.Show(`with moved as (
			delete from readings where value < 5 returning sensor, value
		)
		insert into archive select sensor, value from moved returning *`)

	s.Note("filter beats a pile of case expressions")
	s.Show(`select sensor,
			count(*) as total,
			count(*) filter (where value > 5) as high,
			round(avg(value) filter (where value > 5), 1) as avg_high
		from readings group by sensor order by sensor`)

	s.Note("generate_series makes rows out of nothing")
	s.Show(`select d::date as day, coalesce(count(r.value), 0) as readings
		from generate_series('2026-01-01', '2026-01-03', interval '1 day') d
		left join readings r on r.taken_at::date = d::date
		group by d order by d`)

	s.Note("returning works on every write")
	s.Show(`update totals set hits = hits * 10 returning sensor, hits`)
}
