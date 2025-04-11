package postgres

func init() {
	set.Add("generated", "generated columns, identity columns and sequences", generated)
}

func generated(s *Session) {
	s.Exec(`drop table if exists items`)
	s.Exec(`drop sequence if exists ticket_seq`)

	s.Note("identity column and generated stored column")
	s.Exec(`create table items (
		id int generated always as identity primary key,
		name text not null,
		width numeric,
		height numeric,
		area numeric generated always as (width * height) stored,
		slug text generated always as (lower(replace(name, ' ', '-'))) stored
	)`)
	s.Exec(`insert into items (name, width, height) values ('Big Box', 3, 4), ('Small Tray', 1.5, 2)`)
	s.Show(`select id, name, slug, area from items order by id`)

	s.Note("sequences")
	s.Exec(`create sequence ticket_seq start 100 increment 5`)
	s.Show(`select nextval('ticket_seq') as a, nextval('ticket_seq') as b, currval('ticket_seq') as current`)
	s.Show(`select setval('ticket_seq', 500) as reset, nextval('ticket_seq') as next`)
	s.Show(`select last_value, is_called from ticket_seq`)
	s.Show(`select sequencename, start_value, increment_by, last_value from pg_sequences where sequencename = 'ticket_seq'`)

	s.Note("serial vs identity")
	s.Show(`select column_name, is_identity, is_generated, generation_expression
		from information_schema.columns where table_name = 'items' order by ordinal_position`)

	s.Note("default expressions")
	s.Exec(`drop table if exists events`)
	s.Exec(`create table events (
		id uuid default gen_random_uuid() primary key,
		created_at timestamp default localtimestamp,
		day date generated always as (created_at::date) stored
	)`)
	s.Exec(`insert into events default values`)
	s.Show(`select id, day from events`)
}
