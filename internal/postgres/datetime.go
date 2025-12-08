package postgres

func init() {
	set.Add("datetime", "timestamps, time zones, intervals and date maths", datetime)
}

func datetime(s *Session) {
	s.Note("now, clock and transaction time")
	s.Show(`select now() as transaction_time, clock_timestamp() as wall_clock, current_date as today`)

	s.Note("time zones")
	s.Show(`select current_setting('TimeZone') as server_zone,
			now() at time zone 'utc' as utc,
			now() at time zone 'asia/tokyo' as tokyo`)
	s.Show(`select name, utc_offset from pg_timezone_names where name in ('Europe/London', 'America/New_York', 'Asia/Tokyo') order by name`)

	s.Note("truncation and extraction")
	s.Show(`select date_trunc('month', timestamptz '2026-08-08 13:45:00+00') as month_start,
			extract(dow from date '2026-08-08') as day_of_week,
			extract(week from date '2026-08-08') as iso_week,
			to_char(date '2026-08-08', 'FMDay DD Month YYYY') as pretty`)

	s.Note("intervals")
	s.Show(`select date '2026-08-08' + interval '45 days' as later,
			age(date '2026-08-08', date '1990-03-01') as age,
			interval '1 day 3 hours' * 2 as doubled,
			extract(epoch from interval '2 hours 30 minutes') as seconds`)

	s.Note("date bins and series")
	s.Show(`select date_bin('15 minutes', timestamptz '2026-08-08 13:47:00+00', timestamptz '2026-08-08 00:00:00+00') as bucket`)
	s.Show(`select d::date as week_start from generate_series(date '2026-01-01', date '2026-02-15', interval '1 week') d`)

	s.Note("overlaps and ranges of time")
	s.Show(`select (date '2026-01-01', date '2026-02-01') overlaps (date '2026-01-15', date '2026-03-01') as overlaps`)

	s.Note("parsing and formatting")
	s.Show(`select to_timestamp('08/08/2026 14:30', 'DD/MM/YYYY HH24:MI') as parsed,
			to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SSOF') as iso`)

	s.Note("storing an event with a time zone")
	s.Exec(`drop table if exists meetings`)
	s.Exec(`create table meetings (id serial primary key, starts_at timestamptz, zone text)`)
	s.Exec(`insert into meetings (starts_at, zone) values ('2026-08-08 09:00+00', 'Europe/London'), ('2026-08-08 09:00+00', 'Asia/Tokyo')`)
	s.Show(`select zone, starts_at, starts_at at time zone zone as local_time from meetings order by id`)
}
