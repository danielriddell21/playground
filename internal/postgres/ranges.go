package postgres

func init() {
	set.Add("ranges", "range and multirange types with exclusion constraints", ranges)
}

func ranges(s *Session) {
	s.Note("a range is one value")
	s.Show(`select int4range(1, 10) as r,
			int4range(1, 10) @> 5 as contains,
			int4range(1, 10) && int4range(8, 20) as overlaps,
			int4range(1, 10) * int4range(5, 20) as intersection`)

	s.Note("punching a hole in one range is not a range, so it fails")
	s.ExpectError(`select daterange('2026-01-01', '2026-03-01') - daterange('2026-01-15', '2026-02-01')`)

	s.Note("a multirange can hold the gap")
	s.Show(`select datemultirange(daterange('2026-01-01', '2026-03-01'))
			- datemultirange(daterange('2026-01-15', '2026-02-01')) as with_a_hole`)
	s.Show(`select int4multirange(int4range(1,5), int4range(10,15)) as m,
			int4multirange(int4range(1,5), int4range(10,15)) @> 12 as contains`)

	s.Note("the database refuses to double book")
	s.Exec(`create extension if not exists btree_gist`)
	s.Exec(`drop table if exists bookings`)
	s.Exec(`create table bookings (room text, during tstzrange, exclude using gist (room with =, during with &&))`)
	s.Exec(`insert into bookings values ('blue', tstzrange('2026-01-01 09:00+00', '2026-01-01 10:00+00'))`)
	s.Exec(`insert into bookings values ('blue', tstzrange('2026-01-01 10:00+00', '2026-01-01 11:00+00'))`)
	s.Exec(`insert into bookings values ('red',  tstzrange('2026-01-01 09:30+00', '2026-01-01 10:30+00'))`)
	s.Show(`select room, during from bookings order by room, during`)

	s.Note("this overlap is rejected, not stored")
	s.ExpectError(`insert into bookings values ('blue', tstzrange('2026-01-01 09:30+00', '2026-01-01 10:30+00'))`)
	s.Show(`select count(*) as still_only from bookings`)

	s.Note("network types get their own operators")
	s.Show(`select '192.168.1.5'::inet << '192.168.1.0/24'::cidr as in_subnet,
			set_masklen('192.168.1.5/32'::inet, 24) as with_mask,
			'192.168.1.0/24'::cidr >>= '192.168.1.5'::inet as contains`)
}
