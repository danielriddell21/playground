package postgres

func init() {
	set.Add("temporal", "keys that cannot overlap in time, and slicing rows with FOR PORTION OF", temporal)
}

func temporal(s *Session) {
	if !s.AtLeast(19) {
		return
	}
	s.Exec(`create extension if not exists btree_gist`)

	s.Note("a primary key that cannot overlap in time")
	s.Exec(`drop table if exists prices`)
	s.Exec(`create table prices (
		product_id int,
		valid_at daterange,
		price numeric,
		primary key (product_id, valid_at without overlaps)
	)`)
	s.Exec(`insert into prices values (1, daterange('2026-01-01', '2026-06-01'), 10.00)`)
	s.Exec(`insert into prices values (1, daterange('2026-06-01', '2027-01-01'), 12.00)`)
	s.Show(`select product_id, valid_at, price from prices order by valid_at`)
	s.ExpectError(`insert into prices values (1, daterange('2026-03-01', '2026-09-01'), 99.00)`)

	s.Note("now update only a slice of time, and the rest survives")
	s.Exec(`drop table if exists coverage`)
	s.Exec(`create table coverage (
		policy_id int,
		valid_at daterange,
		amount numeric,
		primary key (policy_id, valid_at without overlaps)
	)`)
	s.Exec(`insert into coverage values (1, daterange('2026-01-01', '2027-01-01'), 100)`)
	s.Show(`select policy_id, valid_at, amount from coverage order by valid_at`)

	s.Exec(`update coverage for portion of valid_at from '2026-04-01' to '2026-07-01'
		set amount = 250 where policy_id = 1`)
	s.Note("one row became three, and only the middle one changed")
	s.Show(`select policy_id, valid_at, amount from coverage order by valid_at`)

	s.Exec(`delete from coverage for portion of valid_at from '2026-09-01' to '2026-11-01' where policy_id = 1`)
	s.Note("deleting a slice punches a hole the same way")
	s.Show(`select policy_id, valid_at, amount from coverage order by valid_at`)
}
