package postgres

func init() {
	set.Add("queue", "a job queue built from SKIP LOCKED and advisory locks", queue)
}

func queue(s *Session) {
	s.Exec(`drop table if exists jobs`)
	s.Exec(`create table jobs (id serial primary key, payload text, state text default 'queued')`)
	s.Exec(`insert into jobs (payload) select 'job-' || g from generate_series(1, 6) g`)

	s.Note("worker one grabs two jobs and holds them")
	other, err := s.Pool().Acquire(s.Ctx())
	if err != nil {
		s.Fail(err)
		return
	}
	defer other.Release()

	if _, err := other.Exec(s.Ctx(), `begin`); err != nil {
		s.Fail(err)
		return
	}
	rows, err := other.Query(s.Ctx(), `select id from jobs where state = 'queued' order by id limit 2 for update skip locked`)
	if err != nil {
		s.Fail(err)
		return
	}
	var held []int32
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			s.Fail(err)
			return
		}
		held = append(held, id)
	}
	rows.Close()
	s.Note("  worker one holds %v", held)

	s.Note("worker two skips them instead of blocking")
	s.Show(`select id, payload from jobs where state = 'queued' order by id limit 2 for update skip locked`)

	if _, err := other.Exec(s.Ctx(), `rollback`); err != nil {
		s.Fail(err)
		return
	}

	s.Note("claim and return the work in one statement")
	s.Show(`with picked as (
			select id from jobs where state = 'queued' order by id limit 3 for update skip locked
		)
		update jobs set state = 'running' where id in (select id from picked) returning id, payload, state`)
	s.Show(`select state, count(*) from jobs group by state order by state`)

	s.Note("advisory locks are application locks, no rows involved")
	s.Show(`select pg_try_advisory_lock(42) as got_it`)
	s.Show(`select pg_try_advisory_lock(42) as same_session_again`)
	s.Show(`select pg_advisory_unlock(42) as released, pg_advisory_unlock(42) as released_again`)
}
