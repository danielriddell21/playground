package postgres

func init() {
	set.Add("constraints", "keys, checks, cascades and partial unique indexes", constraints)
}

func constraints(s *Session) {
	s.Exec(`drop table if exists order_lines`)
	s.Exec(`drop table if exists shops`)

	s.Exec(`create table shops (
		id int primary key,
		name text not null unique,
		email text,
		rating int check (rating between 1 and 5),
		constraint email_shape check (email is null or email like '%@%')
	)`)
	s.Exec(`insert into shops values (1, 'corner', 'a@b.com', 4), (2, 'market', null, 5)`)

	s.Note("check constraints reject bad rows")
	s.Show(`select conname, pg_get_constraintdef(oid) as definition
		from pg_constraint where conrelid = 'shops'::regclass order by conname`)

	s.Note("foreign keys with cascade")
	s.Exec(`create table order_lines (
		id serial primary key,
		shop_id int not null references shops(id) on delete cascade,
		qty int not null default 1 check (qty > 0)
	)`)
	s.Exec(`insert into order_lines (shop_id, qty) values (1, 2), (1, 5), (2, 1)`)
	s.Show(`select count(*) as lines_before from order_lines`)
	s.Exec(`delete from shops where id = 1`)
	s.Show(`select count(*) as lines_after_cascade from order_lines`)

	s.Note("partial unique index")
	s.Exec(`drop table if exists drafts`)
	s.Exec(`create table drafts (id serial primary key, author text, published boolean default false)`)
	s.Exec(`create unique index one_draft_per_author on drafts (author) where not published`)
	s.Exec(`insert into drafts (author, published) values ('ada', false), ('ada', true), ('ada', true)`)
	s.Show(`select author, published, count(*) from drafts group by 1, 2 order by 2`)

	s.Note("not null, defaults and nulls not distinct")
	s.Exec(`drop table if exists tags`)
	s.Exec(`create table tags (label text, scope text, unique nulls not distinct (label, scope))`)
	s.Exec(`insert into tags values ('a', null)`)
	s.Show(`select * from tags`)

	s.Note("adding a constraint as not valid then validating")
	s.Exec(`alter table drafts add constraint author_present check (author is not null) not valid`)
	s.Exec(`alter table drafts validate constraint author_present`)
	s.Show(`select conname, convalidated from pg_constraint where conrelid = 'drafts'::regclass`)
}
