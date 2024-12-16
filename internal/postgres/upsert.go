package postgres

func init() {
	set.Add("upsert", "on conflict, returning and merge", upsert)
}

func upsert(s *Session) {
	s.Exec(`drop table if exists inventory`)
	s.Exec(`create table inventory (sku text primary key, qty int not null, updated_at timestamptz default now())`)
	s.Exec(`insert into inventory (sku, qty) values ('a1', 10), ('b2', 5)`)

	s.Note("insert or update")
	s.Show(`insert into inventory (sku, qty) values ('a1', 3), ('c3', 7)
		on conflict (sku) do update set qty = inventory.qty + excluded.qty, updated_at = now()
		returning sku, qty`)

	s.Note("insert or ignore")
	s.Show(`insert into inventory (sku, qty) values ('a1', 999)
		on conflict (sku) do nothing
		returning sku, qty`)
	s.Show(`select sku, qty from inventory order by sku`)

	s.Note("conditional upsert")
	s.Show(`insert into inventory (sku, qty) values ('b2', 100)
		on conflict (sku) do update set qty = excluded.qty
		where inventory.qty < 10
		returning sku, qty`)

	s.Note("returning on update and delete")
	s.Show(`update inventory set qty = qty * 2 where qty < 20 returning sku, qty as doubled`)
	s.Show(`delete from inventory where sku = 'c3' returning sku`)

	s.Note("merge")
	s.Exec(`drop table if exists deliveries`)
	s.Exec(`create table deliveries (sku text, qty int)`)
	s.Exec(`insert into deliveries values ('a1', 4), ('d4', 12), ('b2', 0)`)
	s.Exec(`merge into inventory i using deliveries d on i.sku = d.sku
		when matched and d.qty = 0 then delete
		when matched then update set qty = i.qty + d.qty
		when not matched then insert (sku, qty) values (d.sku, d.qty)`)
	s.Show(`select sku, qty from inventory order by sku`)
}
