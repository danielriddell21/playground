package postgres

func init() {
	set.Add("grouping", "grouping sets, rollup, filter, distinct on and aggregates", grouping)
}

func grouping(s *Session) {
	s.Exec(`drop table if exists tickets`)
	s.Exec(`create table tickets (id serial primary key, team text, priority text, hours numeric, closed boolean)`)
	s.Exec(`insert into tickets (team, priority, hours, closed) values
		('core', 'high', 5, true), ('core', 'low', 2, false), ('core', 'high', 8, false),
		('web', 'high', 3, true), ('web', 'low', 1, true), ('web', 'low', 4, false)`)

	s.Note("rollup and cube")
	s.Show(`select coalesce(team, 'ALL') as team, coalesce(priority, 'ALL') as priority, sum(hours) as hours
		from tickets group by rollup (team, priority) order by team, priority`)
	s.Show(`select coalesce(team, 'ALL') as team, coalesce(priority, 'ALL') as priority, count(*) as n
		from tickets group by cube (team, priority) order by team, priority`)

	s.Note("explicit grouping sets with the grouping flag")
	s.Show(`select team, priority, sum(hours) as hours, grouping(team, priority) as level
		from tickets group by grouping sets ((team), (priority), ())
		order by level, team, priority`)

	s.Note("filter clause")
	s.Show(`select team,
			count(*) as total,
			count(*) filter (where closed) as closed,
			sum(hours) filter (where priority = 'high') as high_hours
		from tickets group by team order by team`)

	s.Note("distinct on")
	s.Show(`select distinct on (team) team, priority, hours from tickets order by team, hours desc`)

	s.Note("ordered set and statistical aggregates")
	s.Show(`select percentile_cont(0.5) within group (order by hours) as median,
			mode() within group (order by priority) as common_priority,
			round(stddev(hours), 2) as stddev,
			round(corr(hours, id)::numeric, 3) as correlation
		from tickets`)

	s.Note("string and json aggregates")
	s.Show(`select team, string_agg(priority, ',' order by hours desc) as priorities,
			json_agg(hours order by hours) as hours
		from tickets group by team order by team`)

	s.Note("having")
	s.Show(`select team, sum(hours) as hours from tickets group by team having sum(hours) > 8`)
}
