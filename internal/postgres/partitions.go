package postgres

func init() {
	set.Add("partitions", "range, list and hash partitioning with pruning", partitions)
}

func partitions(s *Session) {
	s.Exec(`drop table if exists measurements`)
	s.Exec(`drop table if exists customers`)
	s.Exec(`drop table if exists sessions`)

	s.Note("range partitioning by month")
	s.Exec(`create table measurements (id serial, taken_on date not null, value numeric) partition by range (taken_on)`)
	s.Exec(`create table measurements_2026_01 partition of measurements for values from ('2026-01-01') to ('2026-02-01')`)
	s.Exec(`create table measurements_2026_02 partition of measurements for values from ('2026-02-01') to ('2026-03-01')`)
	s.Exec(`create table measurements_default partition of measurements default`)
	s.Exec(`insert into measurements (taken_on, value) values
		('2026-01-05', 10), ('2026-01-20', 12), ('2026-02-11', 20), ('2026-07-01', 99)`)

	s.Show(`select tableoid::regclass as partition, count(*) as rows from measurements group by 1 order by 1`)

	s.Note("partition pruning in the plan")
	s.Show(`explain (costs off) select * from measurements where taken_on = '2026-01-05'`)

	s.Note("detaching and attaching")
	s.Exec(`alter table measurements detach partition measurements_2026_01`)
	s.Show(`select count(*) as rows_after_detach from measurements`)
	s.Exec(`alter table measurements attach partition measurements_2026_01 for values from ('2026-01-01') to ('2026-02-01')`)
	s.Show(`select count(*) as rows_after_attach from measurements`)

	s.Note("list partitioning")
	s.Exec(`create table customers (id serial, region text not null, name text) partition by list (region)`)
	s.Exec(`create table customers_uk partition of customers for values in ('uk')`)
	s.Exec(`create table customers_us partition of customers for values in ('us')`)
	s.Exec(`insert into customers (region, name) values ('uk', 'ada'), ('us', 'grace'), ('uk', 'alan')`)
	s.Show(`select tableoid::regclass as partition, count(*) from customers group by 1 order by 1`)

	s.Note("hash partitioning")
	s.Exec(`create table sessions (id int not null, token text) partition by hash (id)`)
	s.Exec(`create table sessions_0 partition of sessions for values with (modulus 3, remainder 0)`)
	s.Exec(`create table sessions_1 partition of sessions for values with (modulus 3, remainder 1)`)
	s.Exec(`create table sessions_2 partition of sessions for values with (modulus 3, remainder 2)`)
	s.Exec(`insert into sessions select g, 'tok-' || g from generate_series(1, 30) g`)
	s.Show(`select tableoid::regclass as partition, count(*) from sessions group by 1 order by 1`)

	s.Note("the partition tree")
	s.Show(`select relid::regclass as part, level from pg_partition_tree('measurements') order by level, 1`)
}
