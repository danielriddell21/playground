package postgres

func init() {
	set.Add("locks", "advisory locks, row locking and skip locked queues", locks)
}

func locks(s *Session) {
	s.Note("advisory locks")
	s.Show(`select pg_try_advisory_lock(42) as first_try`)
	s.Show(`select locktype, objid, granted from pg_locks where locktype = 'advisory'`)
	s.Show(`select pg_advisory_unlock(42) as released`)

	s.Exec(`drop table if exists jobs`)
	s.Exec(`create table jobs (id serial primary key, payload text, state text default 'queued')`)
	s.Exec(`insert into jobs (payload) values ('a'), ('b'), ('c'), ('d')`)

	s.Note("a queue with for update skip locked")
	s.Show(`with picked as (
			select id from jobs where state = 'queued' order by id limit 2 for update skip locked
		)
		update jobs set state = 'running' where id in (select id from picked) returning id, payload, state`)
	s.Show(`select id, payload, state from jobs order by id`)

	s.Note("nowait fails fast instead of waiting")
	s.Show(`select id from jobs where id = 1 for update nowait`)

	s.Note("share locks and explicit table locks")
	s.Exec(`begin`)
	s.Exec(`lock table jobs in share mode`)
	s.Show(`select mode, granted from pg_locks where relation = 'jobs'::regclass order by mode`)
	s.Exec(`commit`)

	s.Note("lock waits are visible in pg_stat_activity")
	s.Show(`select count(*) as waiting from pg_stat_activity where wait_event_type = 'Lock'`)
}
