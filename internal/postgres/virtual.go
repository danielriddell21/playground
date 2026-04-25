package postgres

func init() {
	set.Add("virtual", "virtual generated columns, uuidv7 and returning old and new", virtual)
}

func virtual(s *Session) {
	if !s.AtLeast(18) {
		return
	}

	s.Note("virtual generated columns cost nothing to store")
	s.Exec(`drop table if exists boxes`)
	s.Exec(`create table boxes (
		id int generated always as identity primary key,
		width numeric,
		height numeric,
		area_virtual numeric generated always as (width * height) virtual,
		area_stored numeric generated always as (width * height) stored
	)`)
	s.Exec(`insert into boxes (width, height) values (3, 4), (2.5, 2)`)
	s.Show(`select id, width, height, area_virtual, area_stored from boxes order by id`)

	s.Note("RETURNING can hand back the before and after row")
	s.Exec(`drop table if exists balances`)
	s.Exec(`create table balances (id int primary key, amount numeric)`)
	s.Exec(`insert into balances values (1, 100), (2, 250)`)
	s.Show(`update balances set amount = amount * 1.1
		returning id, old.amount as before, new.amount as after`)
	s.Show(`update balances set amount = amount + 5
		returning with (old as o, new as n) id, o.amount as before, n.amount as after`)

	s.Note("uuidv7 is time ordered, so it indexes well")
	s.Show(`select uuidv7() as a, uuidv7() as b, uuidv4() as random`)
	s.Show(`select uuid_extract_version(uuidv7()) as version, uuid_extract_timestamp(uuidv7()) as created_at`)

	s.Note("new array and text helpers")
	s.Show(`select array_sort('{3,1,2}'::int[]) as sorted,
			array_reverse('{1,2,3}'::int[]) as reversed,
			casefold('Straße') as folded`)
}
