package postgres

func init() {
	set.Add("jsontable", "json_table, json_value, and merge_action on a merge", jsontable)
}

func jsontable(s *Session) {
	if !s.AtLeast(17) {
		return
	}

	s.Note("json_table turns a json document into a table")
	s.Show(`select * from json_table(
			'[{"id":1,"name":"ada","tags":["math"]},{"id":2,"name":"linus","tags":["git"]}]'::jsonb,
			'$[*]' columns (
				rn for ordinality,
				id int path '$.id',
				name text path '$.name',
				tag text path '$.tags[0]'
			)
		) t`)

	s.Note("json_value, json_query and json_exists")
	s.Show(`select json_value('{"score":91}'::jsonb, '$.score' returning int) as value,
			json_query('{"tags":["a","b"]}'::jsonb, '$.tags' returning jsonb) as query,
			json_exists('{"a":1}'::jsonb, '$.b') as exists_b`)
	s.Show(`select json_value('{"a":"x"}'::jsonb, '$.missing' returning text default 'fallback' on empty) as with_default`)

	s.Exec(`drop table if exists stock`)
	s.Exec(`drop table if exists shipment`)
	s.Exec(`create table stock (sku text primary key, qty int)`)
	s.Exec(`create table shipment (sku text, qty int)`)
	s.Exec(`insert into stock values ('a1', 5), ('b2', 9), ('c3', 1)`)
	s.Exec(`insert into shipment values ('a1', 3), ('d4', 7)`)

	s.Note("merge_action tells you what each row did")
	s.Show(`merge into stock s using shipment p on s.sku = p.sku
		when matched then update set qty = s.qty + p.qty
		when not matched then insert (sku, qty) values (p.sku, p.qty)
		returning merge_action() as action, s.sku, s.qty`)

	s.Note("and it can act on rows the source never mentioned")
	s.Show(`merge into stock s using shipment p on s.sku = p.sku
		when not matched by source then update set qty = 0
		returning merge_action() as action, s.sku, s.qty`)

	s.Note("AT LOCAL uses the session time zone")
	s.Show(`select timestamptz '2026-08-08 12:00:00+00' at local as local_time, current_setting('TimeZone') as zone`)

	s.Note("random now takes a range")
	s.Show(`select random(1, 6) as die, random(1.0, 2.0) as fraction`)
}
