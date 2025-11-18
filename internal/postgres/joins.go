package postgres

func init() {
	set.Add("joins", "lateral joins, set operations and generate_series", joins)
}

func joins(s *Session) {
	s.Exec(`drop table if exists players`)
	s.Exec(`drop table if exists rounds`)
	s.Exec(`create table players (id int primary key, name text)`)
	s.Exec(`create table rounds (player_id int, played_on date, score int)`)
	s.Exec(`insert into players values (1, 'ada'), (2, 'linus'), (3, 'grace')`)
	s.Exec(`insert into rounds values
		(1, '2026-01-01', 70), (1, '2026-02-01', 65), (1, '2026-03-01', 72),
		(2, '2026-01-15', 80), (2, '2026-02-15', 78)`)

	s.Note("lateral gives each row its own subquery")
	s.Show(`select p.name, r.played_on, r.score
		from players p
		left join lateral (
			select played_on, score from rounds where player_id = p.id order by score limit 2
		) r on true
		order by p.name, r.score`)

	s.Note("lateral with a set returning function")
	s.Show(`select p.name, g.n from players p, lateral generate_series(1, p.id) g(n) order by p.name, g.n`)

	s.Note("set operations")
	s.Show(`select id from players union select player_id from rounds order by 1`)
	s.Show(`select id from players except select player_id from rounds order by 1`)
	s.Show(`select id from players intersect select player_id from rounds order by 1`)

	s.Note("generate_series for dates and gap filling")
	s.Show(`select d::date as day from generate_series('2026-01-01', '2026-01-05', interval '1 day') d`)
	s.Show(`select d::date as month, coalesce(count(r.score), 0) as rounds
		from generate_series('2026-01-01', '2026-03-01', interval '1 month') d
		left join rounds r on date_trunc('month', r.played_on) = d
		group by d order by d`)

	s.Note("join types side by side")
	s.Show(`select p.name, count(r.score) as rounds from players p left join rounds r on r.player_id = p.id group by p.name order by p.name`)
	s.Show(`select p.name from players p where exists (select 1 from rounds r where r.player_id = p.id) order by 1`)
	s.Show(`select p.name from players p where not exists (select 1 from rounds r where r.player_id = p.id) order by 1`)
}
