package postgres

func init() {
	set.Add("rls", "row level security policies", rls)
}

func rls(s *Session) {
	s.Exec(`drop table if exists notes`)
	s.Exec(`drop role if exists alice`)
	s.Exec(`drop role if exists bob`)

	s.Exec(`create role alice`)
	s.Exec(`create role bob`)

	s.Exec(`create table notes (id serial primary key, owner text not null default current_user, body text)`)
	s.Exec(`insert into notes (owner, body) values ('alice', 'alice private'), ('bob', 'bob private'), ('alice', 'alice again')`)
	s.Exec(`grant select, insert, update, delete on notes to alice, bob`)
	s.Exec(`grant usage, select on sequence notes_id_seq to alice, bob`)

	s.Note("without RLS everyone sees everything")
	s.Show(`select owner, body from notes order by id`)

	s.Exec(`alter table notes enable row level security`)
	s.Exec(`create policy owner_can_read on notes for select using (owner = current_user)`)
	s.Exec(`create policy owner_can_write on notes for insert with check (owner = current_user)`)

	s.Note("as alice")
	s.Exec(`set role alice`)
	s.Show(`select current_user as who`)
	s.Show(`select owner, body from notes order by id`)
	s.Exec(`insert into notes (body) values ('written by alice')`)
	s.Show(`select count(*) as alice_rows from notes`)
	s.Exec(`reset role`)

	s.Note("as bob")
	s.Exec(`set role bob`)
	s.Show(`select owner, body from notes order by id`)
	s.Exec(`reset role`)

	s.Note("policies in the catalog")
	s.Show(`select policyname, cmd, qual, with_check from pg_policies where tablename = 'notes' order by policyname`)

	s.Note("table owners bypass RLS unless forced")
	s.Exec(`alter table notes force row level security`)
	s.Show(`select count(*) as visible_to_owner from notes`)
	s.Exec(`alter table notes no force row level security`)
	s.Show(`select count(*) as visible_again from notes`)
}
