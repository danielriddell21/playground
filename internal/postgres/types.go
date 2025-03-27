package postgres

func init() {
	set.Add("types", "enums, domains, composite types, ranges and network types", types)
}

func types(s *Session) {
	s.Exec(`drop table if exists bookings`)
	s.Exec(`drop type if exists mood`)
	s.Exec(`drop type if exists address`)
	s.Exec(`drop domain if exists positive_int`)

	s.Note("enum")
	s.Exec(`create type mood as enum ('sad', 'ok', 'happy')`)
	s.Show(`select 'happy'::mood > 'ok'::mood as happier, enum_range(null::mood) as all_values`)

	s.Note("domain with a constraint")
	s.Exec(`create domain positive_int as int check (value > 0)`)
	s.Show(`select 5::positive_int as ok`)

	s.Note("composite type")
	s.Exec(`create type address as (street text, city text, postcode text)`)
	s.Show(`select (row('1 main st', 'london', 'e1 1aa')::address).city as city`)

	s.Note("ranges")
	s.Show(`select int4range(1, 10) as r,
			int4range(1, 10) @> 5 as contains,
			int4range(1, 10) && int4range(8, 20) as overlaps,
			upper(int4range(1, 10)) as upper_bound`)
	s.Show(`select tstzrange(now(), now() + interval '2 hours') as window,
			daterange('2026-01-01', '2026-02-01') - daterange('2026-01-15', '2026-02-01') as diff`)

	s.Note("multirange")
	s.Show(`select int4multirange(int4range(1,5), int4range(10,15)) as m,
			int4multirange(int4range(1,5), int4range(10,15)) @> 12 as contains`)

	s.Note("range column with an exclusion constraint")
	s.Exec(`create extension if not exists btree_gist`)
	s.Exec(`create table bookings (room text, during tstzrange, exclude using gist (room with =, during with &&))`)
	s.Exec(`insert into bookings values ('blue', tstzrange('2026-01-01 09:00+00', '2026-01-01 10:00+00'))`)
	s.Exec(`insert into bookings values ('blue', tstzrange('2026-01-01 10:00+00', '2026-01-01 11:00+00'))`)
	s.Show(`select room, during from bookings order by during`)

	s.Note("network, uuid and money")
	s.Show(`select '192.168.1.5'::inet << '192.168.1.0/24'::cidr as in_subnet,
			'08:00:2b:01:02:03'::macaddr as mac,
			gen_random_uuid() as id,
			12.50::money as price`)

	s.Note("bit strings and intervals")
	s.Show(`select B'1010' | B'0101' as bits, interval '90 minutes' as span, justify_interval(interval '90 minutes') as tidy`)
}
